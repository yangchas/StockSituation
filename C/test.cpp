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
    
    int batch_size = 2;  // 改为1，单条处理
    int worker_count = 1; // 只需要1个worker
    
    std::string redis_host = "localhost";
    int redis_port = 6379;
    int redis_db = 0;
    
    std::string volatile_pool_key = "stock:volatile_pool";
    int volatile_expire = 300;
    
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000;
    
    std::string tdengine_host = "chaos";
    int tdengine_port = 6030;
    std::string tdengine_user = "root";
    std::string tdengine_password = "taosdata";
    std::string tdengine_database = "market_data";
    
    // 串行处理相关配置
    int processing_delay_ms = 10;           // 每条消息处理后的延迟
    bool enable_rate_limiting = true;       // 启用速率限制
    bool enable_message_ack = true;         // 启用消息确认
    int max_pending_messages = 2;           // 最大待处理消息数
    int shutdown_timeout = 30;
    bool verbose = true;
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
            MessageHeader header;
            std::vector<char> proto_data;
            
            if (!parseMessageFormat(body, header, proto_data)) {
                std::cerr << "Failed to parse message format" << std::endl;
                return false;
            }
            
            // 使用 protobuf 解析 DataRequest
            dataservice::DataRequest data_request;
            if (!data_request.ParseFromArray(proto_data.data(), proto_data.size())) {
                std::cerr << "Failed to parse DataRequest from protobuf" << std::endl;
                // 释放内存
                std::vector<char>().swap(proto_data);
                return false;
            }
            
            // 解压缩数据
            std::vector<char> batch_bytes;
            {
                std::vector<char> decompressed;
                if (header.compression == "GZIP" || header.compression == "ZLIB") {
                    const std::string& compressed_data = data_request.compressed_data();
                    if (!decompressZlib(compressed_data, decompressed)) {
                        std::cerr << "Failed to decompress data" << std::endl;
                        data_request.Clear();
                        std::vector<char>().swap(proto_data);
                        return false;
                    }
                    batch_bytes = std::move(decompressed);
                } else if (header.compression == "NONE" || header.compression.empty()) {
                    const std::string& compressed_data = data_request.compressed_data();
                    batch_bytes.assign(compressed_data.begin(), compressed_data.end());
                } else {
                    std::cerr << "Unsupported compression: " << header.compression << std::endl;
                    data_request.Clear();
                    std::vector<char>().swap(proto_data);
                    return false;
                }
            } // decompressed 在这里离开作用域被自动销毁
            
            // 使用 protobuf 解析 DataBatch
            dataservice::DataBatch data_batch;
            if (!data_batch.ParseFromArray(batch_bytes.data(), batch_bytes.size())) {
                std::cerr << "Failed to parse DataBatch from protobuf" << std::endl;
                // 显式释放内存
                data_request.Clear();
                std::vector<char>().swap(batch_bytes);
                std::vector<char>().swap(proto_data);
                return false;
            }
            
            // 转换记录到 StockData
            bool result = convertDataBatchToStockData(data_batch, records);
            
            // 处理完成后立即释放大内存变量
            data_request.Clear();  // 释放protobuf内部内存
            data_batch.Clear();    // 释放protobuf内部内存
            std::vector<char>().swap(batch_bytes);  // 强制释放vector内存
            std::vector<char>().swap(proto_data);   // 强制释放vector内存
            
            return result;
            
        } catch (const std::exception& e) {
            std::cerr << "Error processing message: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    // 解析消息格式（与Python版本相同）
    bool parseMessageFormat(const std::vector<char>& body, MessageHeader& header, std::vector<char>& proto_data) {
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
        
        // 使用RAII确保资源释放
        struct ZlibGuard {
            z_stream* strm_;
            ZlibGuard(z_stream* strm) : strm_(strm) {}
            ~ZlibGuard() { inflateEnd(strm_); }
        } guard(&strm);
        
        const size_t CHUNK_SIZE = 65536;
        std::vector<char> buffer;
        buffer.reserve(CHUNK_SIZE);
        
        do {
            buffer.resize(CHUNK_SIZE);
            strm.avail_out = buffer.size();
            strm.next_out = reinterpret_cast<Bytef*>(buffer.data());
            
            ret = inflate(&strm, Z_NO_FLUSH);
            
            if (ret != Z_OK && ret != Z_STREAM_END) {
                std::cerr << "inflate failed: " << ret << " - " << (strm.msg ? strm.msg : "no message") << std::endl;
                return false;
            }
            
            size_t have = buffer.size() - strm.avail_out;
            decompressed.insert(decompressed.end(), buffer.begin(), buffer.begin() + have);
            
            // 及时释放buffer内存
            std::vector<char>().swap(buffer);
            buffer.reserve(CHUNK_SIZE); // 重新预留空间
            
        } while (ret != Z_STREAM_END);
        
        // 压缩完成后释放buffer
        std::vector<char>().swap(buffer);
        
        return true;
    }
    
    // 将 DataBatch 转换为 StockData
    bool convertDataBatchToStockData(const dataservice::DataBatch& data_batch, std::vector<StockData>& records) {
        int record_count = data_batch.records_size();
        
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
                          NULL, port_);
        
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
            if (config_.enable_rate_limiting) {
                std::this_thread::sleep_for(std::chrono::milliseconds(int(config_.processing_delay_ms*0.01)));
            }

        }
        
        try {
            connection_->execute(sql.str());
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert records: " << e.what() << " SQL: " << sql.str() << std::endl;
            return false;
        }
    }
};

// RabbitMQ消费者 - 串行处理版本
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
        if (!conn_ && !connect()) {
            std::cerr << "Failed to connect to RabbitMQ" << std::endl;
            return;
        }
        
        try {
            // 声明或检查队列
            if (!declareQueue()) {
                std::cerr << "Failed to declare queue" << std::endl;
                return;
            }
            
            // 设置QoS，每次只取一条消息
            amqp_basic_qos(conn_, 1, 0, config_.max_pending_messages, 0);
            
            // 开始消费，手动确认
            amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                              amqp_empty_bytes, 0, 0, 1, amqp_empty_table);
            
            amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(conn_);
            checkAmqpError(consume_reply, "Start consuming");
            
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
                    // 处理单条消息
                    std::vector<char> body;
                    body.resize(envelope.message.body.len);
                    std::memcpy(body.data(), envelope.message.body.bytes, envelope.message.body.len);
                    
                    // 创建确认回调
                    auto ack_callback = [this, delivery_tag = envelope.delivery_tag](bool success) {
                        if (success) {
                            // 确认消息
                            amqp_basic_ack(conn_, 1, delivery_tag, 0);
                        } else {
                            // 拒绝消息并重新入队
                            amqp_basic_reject(conn_, 1, delivery_tag, true);
                            std::cerr << "Message rejected and requeued: " << delivery_tag << std::endl;
                        }
                    };
                    
                    // 处理消息 - 这里会阻塞直到处理完成
                    callback(std::move(body), ack_callback);
                    
                    // 立即释放信封内存
                    amqp_destroy_envelope(&envelope);
                    
                    message_count++;
                    
                    // 处理完成后才继续下一条
                    if (config_.enable_rate_limiting) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing_delay_ms));
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
                
                // 进度报告
                auto now = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
                if (elapsed >= 30) {
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

// ==================== 工作管道 ====================

// 串行处理管道
class ProcessingPipeline {
private:
    const Config& config_;
    std::unique_ptr<ComponentFactory> factory_;
    std::unique_ptr<IMessageConsumer> consumer_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IVolatilityDetector> detector_;
    std::unique_ptr<IMessageProcessor> message_processor_;
    std::atomic<bool> running_{false};
    std::thread consumer_thread_;
    
public:
    ProcessingPipeline(std::unique_ptr<ComponentFactory> factory, const Config& config)
        : config_(config), factory_(std::move(factory)) {
        
        consumer_ = factory_->createMessageConsumer();
        writer_ = factory_->createDataWriter();
        detector_ = factory_->createVolatilityDetector();
        message_processor_ = factory_->createMessageProcessor();
    }
    
    void start() {
        std::cout << "Starting serial processing pipeline" << std::endl;
        
        // 先连接TDengine
        if (!writer_->connect()) {
            std::cerr << "Failed to connect to TDengine" << std::endl;
            return;
        }
        
        running_ = true;
        consumer_thread_ = std::thread([this]() {
            std::cout << "Starting RabbitMQ consumer with serial processing..." << std::endl;
            
            consumer_->consume([this](std::vector<char>&& message, std::function<void(bool)> ack_callback) {
                if (!running_) {
                    ack_callback(false); // 如果正在关闭，拒绝消息
                    return;
                }
                
                bool success = processSingleMessage(std::move(message));
                ack_callback(success);
            });
        });
        
        std::cout << "Serial processing pipeline started" << std::endl;
    }
    
    void stop() {
        std::cout << "Stopping processing pipeline..." << std::endl;
        running_ = false;
        consumer_->stop();
        
        if (consumer_thread_.joinable()) {
            consumer_thread_.join();
        }
        
        writer_->close();
    }
    
private:
    bool processSingleMessage(std::vector<char>&& message) {
        try {
            std::vector<StockData> records;
            
            // 解析消息
            if (!message_processor_->processMessage(message, records)) {
                std::cerr << "Failed to process message" << std::endl;
                return false;
            }
            
            // 立即释放消息内存
            std::vector<char>().swap(message);
            
            if (records.empty()) {
                std::cout << "No records extracted from message" << std::endl;
                return true; // 空记录不算失败
            }
            
            // 写入TDengine
            if (!writer_->writeBatch(records)) {
                std::cerr << "Failed to write records to TDengine" << std::endl;
                return false;
            }
            
            std::cout << "Successfully inserted " << records.size() << " records to TDengine" << std::endl;
            
            // 异动检测
            int volatility_detected = 0;
            for (const auto& record : records) {
                if (detector_->detectVolatility(record)) {
                    volatility_detected++;
                }
            }
            
            if (volatility_detected > 0) {
                std::cout << "Detected volatility in " << volatility_detected << " records" << std::endl;
            }
            
            // 定期清理旧数据（每处理100条消息清理一次）
            static int cleanup_counter = 0;
            if (++cleanup_counter >= 100) {
                detector_->cleanupOldData();
                cleanup_counter = 0;
            }
            
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Exception processing single message: " << e.what() << std::endl;
            return false;
        }
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
        
        while (!shutdown_ && !signal_received_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        
            // auto now = std::chrono::steady_clock::now();
            // if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 30) {
            //     std::cout << "Queue status - Size: " << pipeline_->getQueueSize() 
            //              << ", Memory usage: " << (pipeline_->getMemoryUsageRatio() * 100) << "%" << std::endl;
            //     last_report = now;
            // }
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