#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <cstring>
#include <ctime>
#include <functional>
#include <sstream>
#include <iomanip>
#include <csignal>
#include <cstdlib>
#include <cctype>
// 第三方库头文件
#include <sys/time.h>
#include <zlib.h>
#include <hiredis/hiredis.h>
#include <taos.h>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

// ==================== 配置管理 ====================

// 配置结构
struct Config {
    std::string rabbitmq_host = "localhost";
    int rabbitmq_port = 5672;
    std::string rabbitmq_user = "admin";
    std::string rabbitmq_password = "admin";
    std::string rabbitmq_vhost = "/";
    std::string queue_name = "stream2";
    
    int batch_size = 100;
    int worker_count = 4;
    int buffer_size = 10000;
    double flush_timeout = 1.0;
    bool verbose = true;
    int shutdown_timeout = 30;
    
    std::string redis_host = "localhost";
    int redis_port = 6379;
    int redis_db = 0;
    
    std::string volatile_pool_key = "stock:volatile_pool";
    int volatile_expire = 300;
    
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000;
    
    std::string tdengine_host = "localhost";
    int tdengine_port = 6030;
    std::string tdengine_user = "root";
    std::string tdengine_password = "taosdata";
    std::string tdengine_database = "market_data";
    
    // 新增背压控制配置
    size_t max_queue_memory = 100 * 1024 * 1024; // 100MB内存限制
    int flow_control_timeout_ms = 1000; // 入队超时时间
    int max_pending_batches = 5; // 最大待处理批次
    bool enable_message_ack = true; // 启用消息确认
};

// 单例配置管理器
class ConfigManager {
private:
    static std::unique_ptr<ConfigManager> instance_;
    static std::mutex mutex_;
    Config config_;

    ConfigManager() {
        loadFromEnvironment();
    }

    void loadFromEnvironment() {
        const char* env_workers = std::getenv("WORKER_COUNT");
        if (env_workers) {
            config_.worker_count = std::atoi(env_workers);
        }
        // 可以添加更多环境变量配置
    }

public:
    ConfigManager(const ConfigManager&) = delete;
    ConfigManager& operator=(const ConfigManager&) = delete;

    static ConfigManager& getInstance() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!instance_) {
            instance_ = std::unique_ptr<ConfigManager>(new ConfigManager());
        }
        return *instance_;
    }

    const Config& getConfig() const { return config_; }
    void updateConfig(const Config& newConfig) { config_ = newConfig; }
};

std::unique_ptr<ConfigManager> ConfigManager::instance_ = nullptr;
std::mutex ConfigManager::mutex_;

// ==================== 数据模型 ====================

// 股票数据结构
struct StockData {
    std::string symbol;
    std::string exchange;
    std::string market;
    long long timestamp = 0;
    double last_price = 0;
    double open = 0;
    double high = 0;
    double low = 0;
    double close = 0;
    double volume = 0;
    double amount = 0;
    double ask_prices[5] = {0};
    double bid_prices[5] = {0};
    double ask_volumes[5] = {0};
    double bid_volumes[5] = {0};
};

// 压缩类型枚举
enum CompressionType {
    COMPRESSION_NONE = 0,
    COMPRESSION_GZIP = 1,
    COMPRESSION_DEFLATE = 2
};

// ==================== 抽象接口 ====================

// 数据写入器接口
class IDataWriter {
public:
    virtual ~IDataWriter() = default;
    virtual bool connect() = 0;
    virtual void close() = 0;
    virtual bool writeBatch(const std::vector<StockData>& records) = 0;
};

// 异动检测器接口
class IVolatilityDetector {
public:
    virtual ~IVolatilityDetector() = default;
    virtual bool detectVolatility(const StockData& data) = 0;
    virtual void cleanupOldData() = 0;
};

// 消息处理器接口
class IMessageProcessor {
public:
    virtual ~IMessageProcessor() = default;
    virtual bool processMessage(const std::vector<char>& body, std::vector<StockData>& records) = 0;
};

// 消息消费者接口
class IMessageConsumer {
public:
    virtual ~IMessageConsumer() = default;
    virtual bool connect() = 0;
    virtual void disconnect() = 0;
    virtual void consume(std::function<void(std::vector<char>&&, std::function<void(bool)>)> callback) = 0;
    virtual void stop() = 0;
};

// ==================== 工具类 ====================

// 背压控制队列
class BackPressureQueue {
private:
    std::queue<std::vector<char>> queue_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::atomic<bool> shutdown_{false};
    std::atomic<size_t> current_memory_{0};
    size_t max_memory_;
    int timeout_ms_;
    
public:
    BackPressureQueue(size_t max_memory, int timeout_ms = 1000) 
        : max_memory_(max_memory), timeout_ms_(timeout_ms) {}
    
    bool push(std::vector<char>&& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (shutdown_) return false;
        
        size_t item_size = item.size();
        
        // 等待队列有足够空间
        if (!not_full_.wait_for(lock, std::chrono::milliseconds(timeout_ms_),
            [this, item_size]() { 
                return shutdown_ || (current_memory_ + item_size <= max_memory_); 
            })) {
            return false; // 超时，拒绝消息
        }
        
        if (shutdown_) return false;
        
        queue_.push(std::move(item));
        current_memory_ += item_size;
        not_empty_.notify_one();
        return true;
    }
    
    bool pop(std::vector<char>& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this]() { return !queue_.empty() || shutdown_; });
        
        if (shutdown_ && queue_.empty()) return false;
        
        item = std::move(queue_.front());
        queue_.pop();
        current_memory_ -= item.size();
        not_full_.notify_one();
        return true;
    }
    
    void shutdown() {
        shutdown_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
    
    size_t memory_usage() const {
        return current_memory_;
    }
    
    double memory_ratio() const {
        return static_cast<double>(current_memory_) / max_memory_;
    }
};

// 时间工具类
class TimeUtils {
public:
    static long long getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
    }
    
    static std::string formatTimestamp(long long timestamp) {
        auto time = std::chrono::system_clock::from_time_t(timestamp / 1000);
        auto ms = timestamp % 1000;
        
        auto tt = std::chrono::system_clock::to_time_t(time);
        std::tm tm = *std::localtime(&tt);
        
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << std::setfill('0') 
            << std::setw(3) << ms;
        return oss.str();
    }
};

// ==================== 具体实现类 ====================
#include "schema.pb.h"  // 包含生成的 protobuf 头文件

// 高效消息处理器 - 使用 protobuf 库
class EfficientMessageProcessor : public IMessageProcessor {
private:
    const Config& config_;
    
    // 消息头部结构
    struct MessageHeader {
        std::string proto_version;
        std::string compression;
        std::string batch_id;
        int32_t record_count;
        int32_t original_size;
        int32_t compressed_size;
        int64_t timestamp;
    };
    
public:
    EfficientMessageProcessor(const Config& config) : config_(config) {}
    
    bool processMessage(const std::vector<char>& body, std::vector<StockData>& records) override {
        try {
            std::cout<<"processMessage"<<std::endl;
            // 1. 解析消息格式（与Python版本相同）
            MessageHeader header;
            std::vector<char> proto_data;
            if (!parseMessageFormat(body, header, proto_data)) {
                std::cerr << "Failed to parse message format" << std::endl;
                return false;
            }
            
            if (config_.verbose) {
                std::cout << "Header: compression=" << header.compression 
                          << ", records=" << header.record_count 
                          << ", original_size=" << header.original_size
                          << ", compressed_size=" << header.compressed_size << std::endl;
            }
            
            // 2. 使用 protobuf 解析 DataRequest
            dataservice::DataRequest data_request;
            if (!data_request.ParseFromArray(proto_data.data(), proto_data.size())) {
                std::cerr << "Failed to parse DataRequest from protobuf" << std::endl;
                return false;
            }
            
            // 3. 解压缩数据
            std::vector<char> batch_bytes;
            if (header.compression == "GZIP" || header.compression == "ZLIB") {
                const std::string& compressed_data = data_request.compressed_data();
                if (!decompressZlib(compressed_data, batch_bytes)) {
                    std::cerr << "Failed to decompress data" << std::endl;
                    return false;
                }
            } else if (header.compression == "NONE" || header.compression.empty()) {
                const std::string& compressed_data = data_request.compressed_data();
                batch_bytes.assign(compressed_data.begin(), compressed_data.end());
            } else {
                std::cerr << "Unsupported compression: " << header.compression << std::endl;
                return false;
            }
            
            if (config_.verbose) {
                std::cout << "Decompressed data: " << batch_bytes.size() << " bytes" << std::endl;
            }
            
            // 4. 使用 protobuf 解析 DataBatch
            dataservice::DataBatch data_batch;
            if (!data_batch.ParseFromArray(batch_bytes.data(), batch_bytes.size())) {
                std::cerr << "Failed to parse DataBatch from protobuf" << std::endl;
                return false;
            }
            
            // 5. 转换记录到 StockData
            return convertDataBatchToStockData(data_batch, records);
            
        } catch (const std::exception& e) {
            std::cerr << "Error processing message: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    // 解析消息格式（与Python版本相同）
    bool parseMessageFormat(const std::vector<char>& body, MessageHeader& header, std::vector<char>& proto_data) {
        std::cout<<"解析消息格式"<<std::endl;

        if (body.size() < 4) {
            std::cerr << "Message too small: " << body.size() << " bytes" << std::endl;
            return false;
        }
        
        // 读取头部长度（大端）
        uint32_t header_len;
        std::memcpy(&header_len, body.data(), 4);
        header_len = ntohl(header_len);
        
        if (body.size() < 4 + header_len) {
            std::cerr << "Message too short for header: have " << body.size() 
                      << ", need " << (4 + header_len) << std::endl;
            return false;
        }
        
        // 解析 JSON 头部
        std::string header_json(body.data() + 4, header_len);
        if (!parseJsonHeader(header_json, header)) {
            std::cerr << "Failed to parse JSON header" << std::endl;
            return false;
        }
        
        // 剩余的是 protobuf 数据
        size_t proto_offset = 4 + header_len;
        proto_data.assign(body.begin() + proto_offset, body.end());
        
        return true;
    }
    
    // 解析JSON头部
    bool parseJsonHeader(const std::string& json_str, MessageHeader& header) {
        try {
            header.record_count = extractJsonFieldInt(json_str, "record_count");
            header.compression = extractJsonFieldString(json_str, "compression");
            header.original_size = extractJsonFieldInt(json_str, "original_size");
            header.compressed_size = extractJsonFieldInt(json_str, "compressed_size");
            header.timestamp = extractJsonFieldLong(json_str, "timestamp");
            header.batch_id = extractJsonFieldString(json_str, "batch_id");
            header.proto_version = extractJsonFieldString(json_str, "proto_version");
            
            if (header.record_count <= 0 || header.record_count > 100000) {
                std::cerr << "Invalid record count from header: " << header.record_count << std::endl;
                return false;
            }
            
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Failed to parse JSON header: " << e.what() << std::endl;
            return false;
        }
    }
    
    // 从JSON字符串中提取整数字段
    int32_t extractJsonFieldInt(const std::string& json, const std::string& field) {
        std::string search_pattern1 = "\"" + field + "\":";
        std::string search_pattern2 = field + ":";
        
        size_t pos = json.find(search_pattern1);
        if (pos == std::string::npos) {
            pos = json.find(search_pattern2);
        }
        
        if (pos == std::string::npos) {
            return 0;
        }
        
        pos += search_pattern1.length();
        
        while (pos < json.length() && std::isspace(json[pos])) {
            pos++;
        }
        
        if (pos >= json.length() || !std::isdigit(json[pos])) {
            return 0;
        }
        
        size_t end_pos = pos;
        while (end_pos < json.length() && (std::isdigit(json[end_pos]) || json[end_pos] == '-')) {
            end_pos++;
        }
        
        std::string num_str = json.substr(pos, end_pos - pos);
        return std::stoi(num_str);
    }
    
    // 从JSON字符串中提取长整数字段
    int64_t extractJsonFieldLong(const std::string& json, const std::string& field) {
        std::string search_pattern1 = "\"" + field + "\":";
        std::string search_pattern2 = field + ":";
        
        size_t pos = json.find(search_pattern1);
        if (pos == std::string::npos) {
            pos = json.find(search_pattern2);
        }
        
        if (pos == std::string::npos) {
            return 0;
        }
        
        pos += search_pattern1.length();
        
        while (pos < json.length() && std::isspace(json[pos])) {
            pos++;
        }
        
        if (pos >= json.length() || !std::isdigit(json[pos])) {
            return 0;
        }
        
        size_t end_pos = pos;
        while (end_pos < json.length() && (std::isdigit(json[end_pos]) || json[end_pos] == '-')) {
            end_pos++;
        }
        
        std::string num_str = json.substr(pos, end_pos - pos);
        return std::stoll(num_str);
    }
    
    // 从JSON字符串中提取字符串字段
    std::string extractJsonFieldString(const std::string& json, const std::string& field) {
        std::cout<<"extractJsonFieldString"<<std::endl;
        std::string search_pattern1 = "\"" + field + "\":";
        std::string search_pattern2 = field + ":";
        
        size_t pos = json.find(search_pattern1);
        if (pos == std::string::npos) {
            pos = json.find(search_pattern2);
        }
        
        if (pos == std::string::npos) {
            return "";
        }
        
        pos += search_pattern1.length();
        
        while (pos < json.length() && std::isspace(json[pos])) {
            pos++;
        }
        
        if (pos >= json.length() || json[pos] != '"') {
            return "";
        }
        
        pos++;
        
        size_t end_pos = json.find('"', pos);
        if (end_pos == std::string::npos) {
            return "";
        }
        
        return json.substr(pos, end_pos - pos);
    }
    
    // 解压缩 zlib 数据（与Python版本相同）
    bool decompressZlib(const std::string& compressed, std::vector<char>& decompressed) {
        std::cout<<"decompressZlib"<<std::endl;
        if (compressed.empty()) {
            return false;
        }
        
        z_stream strm;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        strm.avail_in = compressed.size();
        strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed.data()));
        
        // 使用自动检测窗口大小
        int ret = inflateInit2(&strm, MAX_WBITS | 32); // 自动检测GZIP/ZLIB
        if (ret != Z_OK) {
            std::cerr << "inflateInit2 failed: " << ret << std::endl;
            return false;
        }
        
        decompressed.reserve(compressed.size() * 4);
        const size_t CHUNK_SIZE = 65536;
        std::vector<char> buffer(CHUNK_SIZE);
        
        do {
            strm.avail_out = buffer.size();
            strm.next_out = reinterpret_cast<Bytef*>(buffer.data());
            
            ret = inflate(&strm, Z_NO_FLUSH);
            
            if (ret != Z_OK && ret != Z_STREAM_END) {
                std::cerr << "inflate failed: " << ret << " - " << (strm.msg ? strm.msg : "no message") << std::endl;
                inflateEnd(&strm);
                return false;
            }
            
            size_t have = buffer.size() - strm.avail_out;
            decompressed.insert(decompressed.end(), buffer.begin(), buffer.begin() + have);
            
        } while (ret != Z_STREAM_END);
        
        inflateEnd(&strm);
        return true;
    }
    
    // 将 DataBatch 转换为 StockData
    bool convertDataBatchToStockData(const dataservice::DataBatch& data_batch, std::vector<StockData>& records) {
        int record_count = data_batch.records_size();
        std::cout<<" DataBatch 转换为 StockData"<<std::endl;
        if (config_.verbose) {
            std::cout << "Converting DataBatch with " << record_count << " records" << std::endl;
        }
        
        // 添加空数据检查
        if (record_count == 0) {
            std::cout << "Warning: DataBatch contains 0 records" << std::endl;
            return false;
        }
        
        records.reserve(record_count);
        int successfully_converted = 0;
        
        for (int i = 0; i < record_count; i++) {
            const dataservice::DataRecord& proto_record = data_batch.records(i);
            StockData stock_data;
            
            // 转换字段
            stock_data.symbol = proto_record.symbol();
            stock_data.exchange = proto_record.exchange();
            stock_data.market = proto_record.market();
            stock_data.timestamp = proto_record.tss();
            stock_data.last_price = proto_record.lp();
            stock_data.open = proto_record.o();
            stock_data.high = proto_record.h();
            stock_data.low = proto_record.l();
            stock_data.close = proto_record.lc();
            stock_data.amount = proto_record.a();
            stock_data.volume = proto_record.v();
            
            // 五档买价
            stock_data.ask_prices[0] = proto_record.ap1();
            stock_data.ask_prices[1] = proto_record.ap2();
            stock_data.ask_prices[2] = proto_record.ap3();
            stock_data.ask_prices[3] = proto_record.ap4();
            stock_data.ask_prices[4] = proto_record.ap5();
            
            // 五档卖价
            stock_data.bid_prices[0] = proto_record.bp1();
            stock_data.bid_prices[1] = proto_record.bp2();
            stock_data.bid_prices[2] = proto_record.bp3();
            stock_data.bid_prices[3] = proto_record.bp4();
            stock_data.bid_prices[4] = proto_record.bp5();
            
            // 五档买量
            stock_data.ask_volumes[0] = proto_record.av1();
            stock_data.ask_volumes[1] = proto_record.av2();
            stock_data.ask_volumes[2] = proto_record.av3();
            stock_data.ask_volumes[3] = proto_record.av4();
            stock_data.ask_volumes[4] = proto_record.av5();
            
            // 五档卖量
            stock_data.bid_volumes[0] = proto_record.bv1();
            stock_data.bid_volumes[1] = proto_record.bv2();
            stock_data.bid_volumes[2] = proto_record.bv3();
            stock_data.bid_volumes[3] = proto_record.bv4();
            stock_data.bid_volumes[4] = proto_record.bv5();
            
            // 验证数据有效性
            if (!stock_data.symbol.empty() && stock_data.timestamp > 0) {
                records.push_back(std::move(stock_data));
                successfully_converted++;
            } else {
                if (config_.verbose) {
                    std::cout << "Skipping invalid record " << i << ": symbol='" << stock_data.symbol 
                              << "' timestamp=" << stock_data.timestamp << std::endl;
                }
            }
        }
        
        // 修改这里的输出，只在有记录时输出
        // if (!records.empty()) {
            std::cout << "First record symbol: " << records[0].symbol << std::endl;
        // }
        
        if (config_.verbose) {
            std::cout << "Successfully converted " << successfully_converted << " records" << std::endl;
        }
        
        return successfully_converted > 0;
    }
    
    // 网络字节序转换
    uint32_t ntohl(uint32_t netlong) {
        return ((netlong & 0xFF) << 24) |
               ((netlong & 0xFF00) << 8) |
               ((netlong & 0xFF0000) >> 8) |
               ((netlong & 0xFF000000) >> 24);
    }
};

// Redis客户端
class RedisClient {
private:
    redisContext* context_ = nullptr;
    std::string host_;
    int port_;
    int db_;
    
public:
    RedisClient(const std::string& host, int port, int db) 
        : host_(host), port_(port), db_(db) {}
    
    ~RedisClient() {
        disconnect();
    }
    
    bool connect() {
        context_ = redisConnect(host_.c_str(), port_);
        if (!context_ || context_->err) {
            std::cerr << "Redis connection error: " 
                      << (context_ ? context_->errstr : "can't allocate context") << std::endl;
            return false;
        }
        
        if (db_ > 0) {
            redisReply* reply = (redisReply*)redisCommand(context_, "SELECT %d", db_);
            if (!reply) {
                disconnect();
                return false;
            }
            freeReplyObject(reply);
        }
        
        return true;
    }
    
    void disconnect() {
        if (context_) {
            redisFree(context_);
            context_ = nullptr;
        }
    }
    
    bool zadd(const std::string& key, double score, const std::string& member) {
        if (!context_) return false;
        
        redisReply* reply = (redisReply*)redisCommand(context_, 
                                                    "ZADD %s %f %s", 
                                                    key.c_str(), score, member.c_str());
        if (!reply) return false;
        
        bool success = (reply->type != REDIS_REPLY_ERROR);
        freeReplyObject(reply);
        return success;
    }
    
    bool expire(const std::string& key, int seconds) {
        if (!context_) return false;
        
        redisReply* reply = (redisReply*)redisCommand(context_, 
                                                    "EXPIRE %s %d", 
                                                    key.c_str(), seconds);
        if (!reply) return false;
        
        bool success = (reply->type != REDIS_REPLY_ERROR);
        freeReplyObject(reply);
        return success;
    }
    
    bool zremrangebyscore(const std::string& key, double min, double max) {
        if (!context_) return false;
        
        redisReply* reply = (redisReply*)redisCommand(context_, 
                                                    "ZREMRANGEBYSCORE %s %f %f", 
                                                    key.c_str(), min, max);
        if (!reply) return false;
        
        bool success = (reply->type != REDIS_REPLY_ERROR);
        freeReplyObject(reply);
        return success;
    }
};

// 简单异动检测器
class SimpleVolatilityDetector : public IVolatilityDetector {
private:
    std::unique_ptr<RedisClient> redis_;
    const Config& config_;
    
public:
    SimpleVolatilityDetector(const Config& config) : config_(config) {
        redis_ = std::make_unique<RedisClient>(config.redis_host, 
                                             config.redis_port, 
                                             config.redis_db);
    }
    
    bool detectVolatility(const StockData& data) override {
        if (!redis_->connect()) {
            std::cerr << "Failed to connect to Redis for volatility detection" << std::endl;
            return false;
        }
        
        if (data.close <= 0) return false;
        
        double price_change = std::abs(data.last_price - data.close) / data.close;
        
        if (price_change >= config_.price_change_threshold && 
            data.amount >= config_.min_amount_threshold) {
            
            std::ostringstream oss;
            oss << "{\"symbol\":\"" << data.symbol 
                << "\",\"timestamp\":" << data.timestamp
                << ",\"price\":" << data.last_price
                << ",\"volume\":" << data.volume
                << ",\"amount\":" << data.amount
                << ",\"reason\":\"price_change\""
                << ",\"detect_time\":" << TimeUtils::getCurrentTimestamp()
                << ",\"is_trial_period\":true}";
            
            bool success = redis_->zadd(config_.volatile_pool_key, 
                                      TimeUtils::getCurrentTimestamp(), 
                                      oss.str());
            
            if (success) {
                redis_->expire(config_.volatile_pool_key, config_.volatile_expire);
                // if (config_.verbose) {
                //     std::cout << "Detected volatility for symbol: " << data.symbol << std::endl;
                // }
            }
            
            return success;
        }
        
        return false;
    }
    
    void cleanupOldData() override {
        if (!redis_->connect()) return;
        
        long long cutoff_time = TimeUtils::getCurrentTimestamp() - 3600000;
        redis_->zremrangebyscore(config_.volatile_pool_key, 0, cutoff_time);
    }
};

// TDengine连接
class TDengineConnection {
private:
    TAOS* conn_ = nullptr;
    std::string host_;
    std::string user_;
    std::string password_;
    std::string database_;
    uint16_t port_;
    
public:
    TDengineConnection(const std::string& host, const std::string& user, 
                      const std::string& password, const std::string& database, uint16_t port)
        : host_(host), user_(user), password_(password), database_(database), port_(port) {}
    
    ~TDengineConnection() {
        close();
    }
    
    bool connect() {
        if (conn_) return true;
        
        std::cout << "Connecting to TDengine at " << host_ << ":" << port_ << std::endl;
        conn_ = taos_connect(host_.c_str(), user_.c_str(), password_.c_str(), 
                           database_.c_str(), port_);
        
        if (!conn_) {
            const char* err_str = taos_errstr(NULL);
            std::cerr << "Failed to connect to TDengine: " << err_str << std::endl;
            return false;
        }
        
        std::cout << "Connected to TDengine successfully" << std::endl;
        return true;
    }
    
    void close() {
        if (conn_) {
            taos_close(conn_);
            conn_ = nullptr;
        }
    }
    
    TAOS* get() const { return conn_; }
    
    void execute(const std::string& sql) {
        if (!conn_) {
            throw std::runtime_error("Not connected to TDengine");
        }
        
        TAOS_RES* res = taos_query(conn_, sql.c_str());
        int code = taos_errno(res);
        if (code != 0) {
            const char* err_str = taos_errstr(res);
            std::string error_msg = "SQL execution failed: " + std::string(err_str);
            taos_free_result(res);
            throw std::runtime_error(error_msg);
        }
        taos_free_result(res);
    }
};

// TDengine批量写入器
class TDengineBatchWriter : public IDataWriter {
private:
    std::unique_ptr<TDengineConnection> connection_;
    const Config& config_;
    
public:
    TDengineBatchWriter(const Config& config) : config_(config) {
        connection_ = std::make_unique<TDengineConnection>(
            config.tdengine_host, config.tdengine_user, 
            config.tdengine_password, config.tdengine_database, 
            config.tdengine_port
        );
    }
    
    ~TDengineBatchWriter() {
        close();
    }
    
    bool connect() override {
        if (!connection_->connect()) {
            return false;
        }
        
        try {
            connection_->execute("CREATE DATABASE IF NOT EXISTS " + config_.tdengine_database);
            connection_->execute("USE " + config_.tdengine_database);
            
            const std::string create_stable_sql = 
                "CREATE STABLE IF NOT EXISTS stock_data ("
                "ts TIMESTAMP, lp FLOAT, o FLOAT, h FLOAT, l FLOAT, lc FLOAT, a FLOAT, "
                "v BIGINT, p BIGINT, "
                "ap1 FLOAT, ap2 FLOAT, ap3 FLOAT, ap4 FLOAT, ap5 FLOAT, "
                "bp1 FLOAT, bp2 FLOAT, bp3 FLOAT, bp4 FLOAT, bp5 FLOAT, "
                "av1 BIGINT, av2 BIGINT, av3 BIGINT, av4 BIGINT, av5 BIGINT, "
                "bv1 BIGINT, bv2 BIGINT, bv3 BIGINT, bv4 BIGINT, bv5 BIGINT"
                ") TAGS (symbol BINARY(20), exchange BINARY(10), market BINARY(10))";
            
            connection_->execute(create_stable_sql);
            std::cout << "Database and tables initialized successfully" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Failed to initialize database: " << e.what() << std::endl;
            return false;
        }
        
        return true;
    }
    
    void close() override {
        if (connection_) {
            connection_->close();
        }
    }
    
    bool writeBatch(const std::vector<StockData>& records) override {
        if (records.empty()) {
            return true;
        }
        
        if (!connection_->get() && !connect()) {
            std::cerr << "Failed to connect to TDengine for writing" << std::endl;
            return false;
        }
        
        std::unordered_map<std::string, std::vector<const StockData*>> grouped;
        for (const auto& record : records) {
            grouped[record.symbol].push_back(&record);
        }
        
        bool all_success = true;
        for (const auto& [symbol, symbol_records] : grouped) {
            if (!insertSymbolRecords(symbol_records, symbol)) {
                std::cerr << "Failed to insert records for symbol: " << symbol << std::endl;
                all_success = false;
            }
        }
        
        return all_success;
    }
    
private:
    std::string sanitizeSymbol(const std::string& symbol) {
        std::string sanitized;
        for (char c : symbol) {
            if (std::isalnum(c) || c == '_') {
                sanitized += c;
            } else {
                sanitized += '_';
            }
        }
        
        if (!sanitized.empty() && std::isdigit(sanitized[0])) {
            sanitized = "s_" + sanitized;
        }
        
        return sanitized;
    }
    
    bool createTableIfNotExists(const std::string& symbol, 
                               const std::string& exchange, 
                               const std::string& market) {
        std::string table_name = "t_" + sanitizeSymbol(symbol);
        
        std::string create_sql = 
            "CREATE TABLE IF NOT EXISTS " + table_name + 
            " USING stock_data TAGS ('" + symbol + "', '" + 
            exchange + "', '" + market + "')";
        
        try {
            connection_->execute(create_sql);
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to create table: " << e.what() << std::endl;
            return false;
        }
    }
    
    bool insertSymbolRecords(const std::vector<const StockData*>& records, const std::string& symbol) {
        if (records.empty()) return true;
        
        const StockData& first_record = *records[0];
        if (!createTableIfNotExists(symbol, first_record.exchange, first_record.market)) {
            return false;
        }
        
        std::string table_name = "t_" + sanitizeSymbol(symbol);
        std::ostringstream sql;
        sql << "INSERT INTO " << table_name << " VALUES ";
        
        for (size_t i = 0; i < records.size(); ++i) {
            const auto& record = *records[i];
            if (i > 0) sql << ", ";
            
            sql << "('" << TimeUtils::formatTimestamp(record.timestamp) << "', "
                << record.last_price << ", " << record.open << ", " 
                << record.high << ", " << record.low << ", " << record.close << ", "
                << record.amount << ", " << static_cast<long long>(record.volume) << ", 0, "
                << record.ask_prices[0] << ", " << record.ask_prices[1] << ", "
                << record.ask_prices[2] << ", " << record.ask_prices[3] << ", "
                << record.ask_prices[4] << ", " << record.bid_prices[0] << ", "
                << record.bid_prices[1] << ", " << record.bid_prices[2] << ", "
                << record.bid_prices[3] << ", " << record.bid_prices[4] << ", "
                << static_cast<long long>(record.ask_volumes[0]) << ", "
                << static_cast<long long>(record.ask_volumes[1]) << ", "
                << static_cast<long long>(record.ask_volumes[2]) << ", "
                << static_cast<long long>(record.ask_volumes[3]) << ", "
                << static_cast<long long>(record.ask_volumes[4]) << ", "
                << static_cast<long long>(record.bid_volumes[0]) << ", "
                << static_cast<long long>(record.bid_volumes[1]) << ", "
                << static_cast<long long>(record.bid_volumes[2]) << ", "
                << static_cast<long long>(record.bid_volumes[3]) << ", "
                << static_cast<long long>(record.bid_volumes[4]) << ")";
        }
        
        try {
            connection_->execute(sql.str());
            // if (config_.verbose) {
            //     std::cout << "Inserted " << records.size() << " records for symbol: " << symbol << std::endl;
            // }
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert records: " << e.what() << std::endl;
            return false;
        }
    }
};

// RabbitMQ消费者 - 支持消息确认
class RabbitMQConsumer : public IMessageConsumer {
private:
    const Config& config_;
    amqp_connection_state_t conn_ = nullptr;
    std::atomic<bool> running_{false};
    
    // 辅助函数：检查AMQP错误
    void checkAmqpError(amqp_rpc_reply_t reply, const std::string& context) {
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            std::string error_msg = context + " failed: ";
            
            switch (reply.reply_type) {
                case AMQP_RESPONSE_NONE:
                    error_msg += "missing RPC reply type";
                    break;
                case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                    error_msg += std::string("library exception: ") + amqp_error_string2(reply.library_error);
                    break;
                case AMQP_RESPONSE_SERVER_EXCEPTION:
                    if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
                        amqp_channel_close_t *close = (amqp_channel_close_t *)reply.reply.decoded;
                        std::string reply_text(static_cast<const char*>(close->reply_text.bytes), close->reply_text.len);
                        error_msg += std::string("channel closed by server: ") + 
                                   reply_text +
                                   " (code: " + std::to_string(close->reply_code) + ")";
                    } else if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
                        amqp_connection_close_t *close = (amqp_connection_close_t *)reply.reply.decoded;
                        std::string reply_text(static_cast<const char*>(close->reply_text.bytes), close->reply_text.len);
                        error_msg += std::string("connection closed by server: ") + 
                                   reply_text +
                                   " (code: " + std::to_string(close->reply_code) + ")";
                    } else {
                        error_msg += "unknown server exception";
                    }
                    break;
            }
            
            throw std::runtime_error(error_msg);
        }
    }
    
    // 尝试声明队列，如果失败则尝试使用被动声明（检查队列是否存在）
    bool declareQueue() {
        try {
            std::cout << "尝试声明队列: " << config_.queue_name << std::endl;
            
            // 首先尝试被动声明，检查队列是否存在
            amqp_queue_declare_ok_t* passive_declare = amqp_queue_declare(
                conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                1,  // passive: 只检查队列是否存在，不创建
                0, 0, 0, amqp_empty_table
            );
            
            amqp_rpc_reply_t passive_reply = amqp_get_rpc_reply(conn_);
            if (passive_reply.reply_type == AMQP_RESPONSE_NORMAL) {
                std::cout << "队列已存在，使用现有队列" << std::endl;
                return true;
            }
            
            // 如果队列不存在，则创建队列
            std::cout << "队列不存在，创建新队列" << std::endl;
            amqp_queue_declare_ok_t* declare_ok = amqp_queue_declare(
                conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                0,  // passive: 创建队列
                1,  // durable: 持久化
                0,  // exclusive: 非独占
                0,  // auto_delete: 不自动删除
                amqp_empty_table
            );
            
            amqp_rpc_reply_t declare_reply = amqp_get_rpc_reply(conn_);
            checkAmqpError(declare_reply, "Declare queue");
            
            std::cout << "队列创建成功: " << config_.queue_name << std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "队列声明失败: " << e.what() << std::endl;
            return false;
        }
    }
    
public:
    RabbitMQConsumer(const Config& config) : config_(config) {}
    
    ~RabbitMQConsumer() {
        disconnect();
    }
    
    bool connect() override {
        std::cout << "Connecting to RabbitMQ at " << config_.rabbitmq_host << ":" << config_.rabbitmq_port << std::endl;
        
        conn_ = amqp_new_connection();
        if (!conn_) {
            std::cerr << "Failed to create RabbitMQ connection" << std::endl;
            return false;
        }
        
        amqp_socket_t* socket = amqp_tcp_socket_new(conn_);
        if (!socket) {
            std::cerr << "Failed to create RabbitMQ socket" << std::endl;
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
            return false;
        }
        
        int status = amqp_socket_open(socket, config_.rabbitmq_host.c_str(), config_.rabbitmq_port);
        if (status != AMQP_STATUS_OK) {
            std::cerr << "Failed to open RabbitMQ socket: " << amqp_error_string2(status) << std::endl;
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
            return false;
        }
        
        amqp_rpc_reply_t login_reply = amqp_login(conn_, config_.rabbitmq_vhost.c_str(), 
                                                0, 131072, 0, AMQP_SASL_METHOD_PLAIN, 
                                                config_.rabbitmq_user.c_str(), config_.rabbitmq_password.c_str());
        
        checkAmqpError(login_reply, "Login");
        
        // 打开通道
        amqp_channel_open(conn_, 1);
        amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn_);
        checkAmqpError(channel_reply, "Open channel");
        
        std::cout << "Connected to RabbitMQ successfully" << std::endl;
        return true;
    }
    
    void disconnect() override {
        if (conn_) {
            try {
                // 关闭通道
                amqp_channel_close(conn_, 1, AMQP_REPLY_SUCCESS);
                // 关闭连接
                amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
            } catch (...) {
                // 忽略关闭时的异常
            }
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
        }
    }
    
    void consume(std::function<void(std::vector<char>&&, std::function<void(bool)>)> callback) override {
        std::cout << "进入消费" << std::endl;
        
        if (!conn_ && !connect()) {
            std::cerr << "Failed to connect to RabbitMQ" << std::endl;
            return;
        }
        
        std::cout << "进入消费 11111" << std::endl;
        
        try {
            // 声明或检查队列
            if (!declareQueue()) {
                std::cerr << "Failed to declare queue" << std::endl;
                return;
            }
            
            std::cout << "进入消费 2222 - 队列准备完成" << std::endl;
            
            // 设置QoS，限制未确认消息数量
            amqp_basic_qos(conn_, 1, 0, config_.max_pending_batches, 0);
            
            // 开始消费，手动确认
            amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                              amqp_empty_bytes, 0, 0, 1, amqp_empty_table);
            
            amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(conn_);
            checkAmqpError(consume_reply, "Start consuming");
            
            std::cout << "进入消费 333333 - 开始等待消息" << std::endl;
            
            running_ = true;
            int message_count = 0;
            auto start_time = std::chrono::steady_clock::now();
            
            while (running_) {
                amqp_envelope_t envelope;
                amqp_maybe_release_buffers(conn_);
                
                // 设置超时，避免无限等待
                struct timeval timeout;
                timeout.tv_sec = 1;  // 1秒超时
                timeout.tv_usec = 0;
                
                amqp_rpc_reply_t ret = amqp_consume_message(conn_, &envelope, &timeout, 0);
                
                if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
                    char* body_start = static_cast<char*>(envelope.message.body.bytes);
                    std::vector<char> body(body_start, body_start + envelope.message.body.len);
                    
                    std::cout << "Received message, size: " << body.size() << " bytes, delivery_tag: " << envelope.delivery_tag << std::endl;
                    
                    // 创建确认回调
                    auto ack_callback = [this, delivery_tag = envelope.delivery_tag](bool success) {
                        if (success) {
                            // 确认消息
                            amqp_basic_ack(conn_, 1, delivery_tag, 0);
                            if (config_.verbose) {
                                std::cout << "Message acknowledged: " << delivery_tag << std::endl;
                            }
                        } else {
                            // 拒绝消息并重新入队
                            amqp_basic_reject(conn_, 1, delivery_tag, true);
                            std::cerr << "Message rejected and requeued: " << delivery_tag << std::endl;
                        }
                    };
                    
                    callback(std::move(body), ack_callback);
                    
                    amqp_destroy_envelope(&envelope);
                    message_count++;
                    
                    if (config_.verbose) {
                        std::cout << "Successfully processed message " << message_count << std::endl;
                    }
                    
                } else if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && 
                          ret.library_error == AMQP_STATUS_TIMEOUT) {
                    // 超时是正常的，继续循环
                    continue;
                } else {
                    // 其他错误
                    if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
                        std::cout << "Library exception: " << amqp_error_string2(ret.library_error) << std::endl;
                    } else if (ret.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
                        std::cout << "Server exception occurred" << std::endl;
                    }
                    
                    if (running_) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    }
                    continue;
                }
                
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
                if (elapsed >= 10) {
                    double msg_rate = static_cast<double>(message_count) / elapsed;
                    std::cout << "Processed " << message_count << " messages (" 
                             << msg_rate << " msg/sec)" << std::endl;
                    start_time = now;
                    message_count = 0;
                }
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Exception in consume: " << e.what() << std::endl;
        }
        
        std::cout << "RabbitMQ consumer stopped" << std::endl;
    }
    
    void stop() override {
        running_ = false;
    }
};

// ==================== 工厂类 ====================

// 抽象工厂
class ComponentFactory {
public:
    virtual ~ComponentFactory() = default;
    virtual std::unique_ptr<IDataWriter> createDataWriter() = 0;
    virtual std::unique_ptr<IVolatilityDetector> createVolatilityDetector() = 0;
    virtual std::unique_ptr<IMessageProcessor> createMessageProcessor() = 0;
    virtual std::unique_ptr<IMessageConsumer> createMessageConsumer() = 0;
};

// 具体工厂
class StockDataFactory : public ComponentFactory {
private:
    const Config& config_;
    
public:
    StockDataFactory(const Config& config) : config_(config) {}
    
    std::unique_ptr<IDataWriter> createDataWriter() override {
        return std::make_unique<TDengineBatchWriter>(config_);
    }
    
    std::unique_ptr<IVolatilityDetector> createVolatilityDetector() override {
        return std::make_unique<SimpleVolatilityDetector>(config_);
    }
    
    std::unique_ptr<IMessageProcessor> createMessageProcessor() override {
        return std::make_unique<EfficientMessageProcessor>(config_);
    }
    
    std::unique_ptr<IMessageConsumer> createMessageConsumer() override {
        return std::make_unique<RabbitMQConsumer>(config_);
    }
};

// ==================== Worker和工作管道 ====================

// Worker类
class Worker {
private:
    const Config& config_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IVolatilityDetector> detector_;
    std::unique_ptr<IMessageProcessor> message_processor_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    int worker_id_;
    static std::atomic<int> next_worker_id_;
    
public:
    Worker(std::unique_ptr<IDataWriter> writer,
           std::unique_ptr<IVolatilityDetector> detector,
           std::unique_ptr<IMessageProcessor> processor,
           const Config& config)
        : config_(config), 
          writer_(std::move(writer)),
          detector_(std::move(detector)),
          message_processor_(std::move(processor)) {
        worker_id_ = next_worker_id_++;
    }
    
    ~Worker() {
        stop();
    }
    
    void start(BackPressureQueue& queue) {
        running_ = true;
        thread_ = std::thread([this, &queue]() { run(queue); });
    }
    
    void stop() {
        running_ = false;
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    
private:
    void run(BackPressureQueue& queue) {
        std::cout << "Worker " << worker_id_ << " started" << std::endl;
        
        // 连接TDengine
        if (!writer_->connect()) {
            std::cerr << "Worker " << worker_id_ << ": Failed to connect to TDengine" << std::endl;
            return;
        }
        std::cout << "Worker " << worker_id_ << ": TDengine connected successfully" << std::endl;
        
        std::vector<StockData> batch;
        auto last_flush = std::chrono::steady_clock::now();
        auto last_cleanup = std::chrono::steady_clock::now();
        
        int processed_messages = 0;
        int processed_records = 0;
        int failed_messages = 0;
        
        std::cout << "Worker " << worker_id_ << ": Starting message processing loop "<<running_ << std::endl;
        
        while (running_) {
            std::vector<char> message;

            if (!queue.pop(message)) {
                std::cout << "Worker " << worker_id_ << ": Queue shutdown, exiting" << std::endl;
                break;
            }
            
            // 添加消息接收日志
            if (config_.verbose) {
                std::cout << "Worker " << worker_id_ << ": Received message, size: " << message.size() << " bytes" << std::endl;
            }
            
            std::vector<StockData> records;
            if (message_processor_->processMessage(message, records)) {
                processed_messages++;
                processed_records += records.size();
                batch.insert(batch.end(), records.begin(), records.end());
                
                if (config_.verbose && !records.empty()) {
                    std::cout << "Worker " << worker_id_ << ": Successfully processed " << records.size() 
                             << " records from message. First symbol: " << records[0].symbol << std::endl;
                }
            } else {
                std::cerr << "Worker " << worker_id_ << ": Failed to process message" << std::endl;
                failed_messages++;
            }
            
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count();
            
            // 批次处理逻辑
            if (batch.size() >= config_.batch_size || elapsed >= config_.flush_timeout) {
                if (!batch.empty()) {
                    std::cout << "Worker " << worker_id_ << ": Flushing batch of " << batch.size() << " records" << std::endl;
                    
                    if (writer_->writeBatch(batch)) {
                        std::cout << "Worker " << worker_id_ << ": Successfully inserted " << batch.size() << " records to TDengine" << std::endl;
                        
                        // 异动检测
                        int volatility_detected = 0;
                        for (const auto& record : batch) {
                            if (detector_->detectVolatility(record)) {
                                volatility_detected++;
                            }
                        }
                        if (volatility_detected > 0) {
                            std::cout << "Worker " << worker_id_ << ": Detected volatility in " << volatility_detected << " records" << std::endl;
                        }
                    } else {
                        std::cerr << "Worker " << worker_id_ << ": Failed to write batch to TDengine" << std::endl;
                    }
                    batch.clear();
                }
                last_flush = now;
            }
            
            // 进度报告
            if (processed_messages % 10 == 0 && processed_messages > 0) {
                std::cout << "Worker " << worker_id_ << ": Progress - " << processed_messages 
                         << " messages, " << processed_records << " records, " 
                         << failed_messages << " failed" << std::endl;
            }
            
            // 清理旧数据
            if (std::chrono::duration_cast<std::chrono::minutes>(now - last_cleanup).count() >= 5) {
                detector_->cleanupOldData();
                last_cleanup = now;
            }
        }
        
        // 处理剩余批次
        if (!batch.empty()) {
            std::cout << "Worker " << worker_id_ << ": Processing final batch of " << batch.size() << " records" << std::endl;
            if (writer_->writeBatch(batch)) {
                std::cout << "Worker " << worker_id_ << ": Final batch inserted successfully" << std::endl;
            } else {
                std::cerr << "Worker " << worker_id_ << ": Failed to insert final batch" << std::endl;
            }
        }
        
        std::cout << "Worker " << worker_id_ << " finished. Total: " 
                  << processed_messages << " messages, " << processed_records << " records, "
                  << failed_messages << " failed" << std::endl;
        
        writer_->close();
    }
};

std::atomic<int> Worker::next_worker_id_{0};

// 工作管道
class ProcessingPipeline {
private:
    const Config& config_;
    std::unique_ptr<ComponentFactory> factory_;
    std::unique_ptr<IMessageConsumer> consumer_;
    std::vector<std::unique_ptr<Worker>> workers_;
    BackPressureQueue queue_;
    
public:
    ProcessingPipeline(std::unique_ptr<ComponentFactory> factory, const Config& config)
        : config_(config), factory_(std::move(factory)),
          queue_(config.max_queue_memory, config.flow_control_timeout_ms) {
        
        consumer_ = factory_->createMessageConsumer();
        
        for (int i = 0; i < config.worker_count; ++i) {
            workers_.push_back(std::make_unique<Worker>(
                factory_->createDataWriter(),
                factory_->createVolatilityDetector(),
                factory_->createMessageProcessor(),
                config_
            ));
        }
    }
    
    void start() {
        std::cout << "Starting processing pipeline with " 
                  << config_.worker_count << " workers" << std::endl;
        
        // 先启动Worker线程
        for (auto& worker : workers_) {
            worker->start(queue_);
        }
        
        // 等待Worker线程初始化完成
        std::cout << "Waiting for workers to initialize..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
        
        std::cout << "All workers started, starting RabbitMQ consumer..." << std::endl;
        
        // 再启动RabbitMQ消费者
        std::thread consumer_thread([this]() {
            std::cout << "RabbitMQ consumer thread started" << std::endl;
            consumer_->consume([this](std::vector<char>&& message, std::function<void(bool)> ack_callback) {
                bool pushed = queue_.push(std::move(message));
                
                if (pushed) {
                    // 消息成功入队，立即确认
                    if (config_.enable_message_ack) {
                        ack_callback(true);
                    }
                    if (config_.verbose) {
                        std::cout << "Message pushed to queue, current size: " << queue_.size() 
                                 << ", memory usage: " << (queue_.memory_ratio() * 100) << "%" << std::endl;
                    }
                } else {
                    // 队列满，拒绝消息（会重新入队）
                    std::cerr << "Queue full, rejecting message (memory usage: " 
                             << (queue_.memory_ratio() * 100) << "%)" << std::endl;
                    if (config_.enable_message_ack) {
                        ack_callback(false);
                    }
                }
            });
        });
        
        consumer_thread.detach();
        std::cout << "Processing pipeline fully started" << std::endl;
    }
    
    void stop() {
        std::cout << "Stopping processing pipeline..." << std::endl;
        consumer_->stop();
        queue_.shutdown();
        
        for (auto& worker : workers_) {
            worker->stop();
        }
    }
    
    size_t getQueueSize() const {
        return queue_.size();
    }
    
    double getMemoryUsageRatio() const {
        return queue_.memory_ratio();
    }
};

// ==================== 应用主类 ====================

class ConsumerApplication {
private:
    Config config_;
    std::unique_ptr<ProcessingPipeline> pipeline_;
    std::atomic<bool> shutdown_{false};
    static std::atomic<bool> signal_received_;
    
public:
    ConsumerApplication(const Config& config) : config_(config) {
        auto factory = std::make_unique<StockDataFactory>(config);
        pipeline_ = std::make_unique<ProcessingPipeline>(std::move(factory), config);
        
        // 设置信号处理
        setupSignalHandlers();
    }
    
    void run() {
        std::cout << "TDengine Consumer Application Starting..." << std::endl;
        
        pipeline_->start();
        
        waitForShutdown();
        
        shutdown();
        
        std::cout << "Application shutdown completed" << std::endl;
    }
    
private:
    static void signalHandler(int signal) {
        std::cout << "\nReceived shutdown signal (" << signal << ")" << std::endl;
        signal_received_ = true;
    }
    
    void setupSignalHandlers() {
        signal_received_ = false;
        signal(SIGINT, signalHandler);
        signal(SIGTERM, signalHandler);
    }
    
    void waitForShutdown() {
        std::cout << "Press Ctrl+C to stop..." << std::endl;
        
        auto last_report = std::chrono::steady_clock::now();
        
        while (!shutdown_ && !signal_received_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 更短的等待时间
            
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 30) {
                std::cout << "Queue status - Size: " << pipeline_->getQueueSize() 
                         << ", Memory usage: " << (pipeline_->getMemoryUsageRatio() * 100) << "%" << std::endl;
                last_report = now;
            }
        }
        
        // 如果收到信号，设置关闭标志
        if (signal_received_) {
            shutdown_ = true;
        }
    }
    
    void shutdown() {
        std::cout << "Initiating shutdown..." << std::endl;
        pipeline_->stop();
        
        // 等待一段时间让管道完全停止
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
};

// 静态成员定义
std::atomic<bool> ConsumerApplication::signal_received_{false};

// ==================== 主函数 ====================

int main() {
    try {
        auto& configManager = ConfigManager::getInstance();
        ConsumerApplication app(configManager.getConfig());
        app.run();
    } catch (const std::exception& e) {
        std::cerr << "Application error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}