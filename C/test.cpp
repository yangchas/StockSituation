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
    
    // 单线程处理配置
    int messages_per_batch = 1;           // 每次处理2条消息
    int max_retry_count = 3;              // 最大重试次数
    int retry_delay_ms = 1000;            // 重试延迟
    bool verbose = false;
    int max_pending_messages = 50;        // 最大待处理消息数

    // 串行处理相关配置
    int processing_delay_ms = 400;           // 每条消息处理后的延迟
    bool enable_rate_limiting = true;       // 启用速率限制




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
        const char* env_messages_per_batch = std::getenv("MESSAGES_PER_BATCH");
        if (env_messages_per_batch) {
            config_.messages_per_batch = std::atoi(env_messages_per_batch);
        }
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

// 带确认的消息结构
struct PendingMessage {
    std::vector<char> data;
    uint64_t delivery_tag;
    
    PendingMessage(std::vector<char>&& d, uint64_t tag)
        : data(std::move(d)), delivery_tag(tag) {}
        
    PendingMessage(PendingMessage&& other) noexcept
        : data(std::move(other.data)), delivery_tag(other.delivery_tag) {}
        
    PendingMessage& operator=(PendingMessage&& other) noexcept {
        if (this != &other) {
            data = std::move(other.data);
            delivery_tag = other.delivery_tag;
        }
        return *this;
    }
    
    PendingMessage(const PendingMessage&) = delete;
    PendingMessage& operator=(const PendingMessage&) = delete;
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
    virtual bool consumeMessages(std::vector<PendingMessage>& messages, int count) = 0;
    virtual void ackMessage(uint64_t delivery_tag) = 0;
    virtual void rejectMessage(uint64_t delivery_tag, bool requeue) = 0;
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
#include "schema.pb.h"

// 高效消息处理器
class EfficientMessageProcessor : public IMessageProcessor {
private:
    const Config& config_;
    
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
                return false;
            }
            
            dataservice::DataRequest data_request;
            if (!data_request.ParseFromArray(proto_data.data(), proto_data.size())) {
                return false;
            }
            
            std::vector<char> batch_bytes;
            {
                std::vector<char> decompressed;
                if (header.compression == "GZIP" || header.compression == "ZLIB") {
                    const std::string& compressed_data = data_request.compressed_data();
                    if (!decompressZlib(compressed_data, decompressed)) {
                        return false;
                    }
                    batch_bytes = std::move(decompressed);
                } else if (header.compression == "NONE" || header.compression.empty()) {
                    const std::string& compressed_data = data_request.compressed_data();
                    batch_bytes.assign(compressed_data.begin(), compressed_data.end());
                } else {
                    return false;
                }
            }
            
            dataservice::DataBatch data_batch;
            if (!data_batch.ParseFromArray(batch_bytes.data(), batch_bytes.size())) {
                return false;
            }
            
            bool result = convertDataBatchToStockData(data_batch, records);
            
            return result;
            
        } catch (const std::exception& e) {
            std::cerr << "Error processing message: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    bool parseMessageFormat(const std::vector<char>& body, MessageHeader& header, std::vector<char>& proto_data) {
        if (body.size() < 4) {
            return false;
        }
        
        uint32_t header_len;
        std::memcpy(&header_len, body.data(), 4);
        header_len = ntohl(header_len);
        
        if (body.size() < 4 + header_len) {
            return false;
        }
        
        std::string header_json(body.data() + 4, header_len);
        if (!parseJsonHeader(header_json, header)) {
            return false;
        }
        
        size_t proto_offset = 4 + header_len;
        proto_data.assign(body.begin() + proto_offset, body.end());
        
        return true;
    }
    
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
                return false;
            }
            
            return true;
            
        } catch (const std::exception& e) {
            return false;
        }
    }
    
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
        
        int ret = inflateInit2(&strm, MAX_WBITS | 32);
        if (ret != Z_OK) {
            return false;
        }
        
        struct ZlibGuard {
            z_stream* strm_;
            ZlibGuard(z_stream* strm) : strm_(strm) {}
            ~ZlibGuard() { inflateEnd(strm_); }
        } guard(&strm);
        
        const size_t CHUNK_SIZE = 65536;
        std::vector<char> buffer(CHUNK_SIZE);
        
        do {
            strm.avail_out = buffer.size();
            strm.next_out = reinterpret_cast<Bytef*>(buffer.data());
            
            ret = inflate(&strm, Z_NO_FLUSH);
            
            if (ret != Z_OK && ret != Z_STREAM_END) {
                return false;
            }
            
            size_t have = buffer.size() - strm.avail_out;
            decompressed.insert(decompressed.end(), buffer.begin(), buffer.begin() + have);
            
        } while (ret != Z_STREAM_END);
        
        return true;
    }
    
    bool convertDataBatchToStockData(const dataservice::DataBatch& data_batch, std::vector<StockData>& records) {
        int record_count = data_batch.records_size();
        
        if (record_count == 0) {
            return false;
        }
        
        records.reserve(record_count);
        
        for (int i = 0; i < record_count; i++) {
            const dataservice::DataRecord& proto_record = data_batch.records(i);
            StockData stock_data;
            
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
            
            stock_data.ask_prices[0] = proto_record.ap1();
            stock_data.ask_prices[1] = proto_record.ap2();
            stock_data.ask_prices[2] = proto_record.ap3();
            stock_data.ask_prices[3] = proto_record.ap4();
            stock_data.ask_prices[4] = proto_record.ap5();
            
            stock_data.bid_prices[0] = proto_record.bp1();
            stock_data.bid_prices[1] = proto_record.bp2();
            stock_data.bid_prices[2] = proto_record.bp3();
            stock_data.bid_prices[3] = proto_record.bp4();
            stock_data.bid_prices[4] = proto_record.bp5();
            
            stock_data.ask_volumes[0] = proto_record.av1();
            stock_data.ask_volumes[1] = proto_record.av2();
            stock_data.ask_volumes[2] = proto_record.av3();
            stock_data.ask_volumes[3] = proto_record.av4();
            stock_data.ask_volumes[4] = proto_record.av5();
            
            stock_data.bid_volumes[0] = proto_record.bv1();
            stock_data.bid_volumes[1] = proto_record.bv2();
            stock_data.bid_volumes[2] = proto_record.bv3();
            stock_data.bid_volumes[3] = proto_record.bv4();
            stock_data.bid_volumes[4] = proto_record.bv5();
            
            if (!stock_data.symbol.empty() && stock_data.timestamp > 0) {
                records.push_back(std::move(stock_data));
            }
        }
        
        return !records.empty();
    }
    
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

// 简单异动检测器 - 修改版本
class SimpleVolatilityDetector : public IVolatilityDetector {
private:
    std::unique_ptr<RedisClient> redis_;
    const Config& config_;
    
    // 存储每个股票的上一个tick数据
    std::unordered_map<std::string, StockData> previous_ticks_;
    std::mutex data_mutex_;
    
    // 服务器时间延迟统计
    std::atomic<long long> max_server_timestamp_{0};
    std::atomic<long long> total_delay_{0};
    std::atomic<int> delay_count_{0};
    
public:
    SimpleVolatilityDetector(const Config& config) : config_(config) {
        redis_ = std::make_unique<RedisClient>(config.redis_host, 
                                             config.redis_port, 
                                             config.redis_db);
    }
    
    bool detectVolatility(const StockData& data) override {
        if (!redis_->connect()) {
            return false;
        }
        // std::cout<<"异动检测"<<std::endl;
        if (data.close <= 0) return false;
        
        // 更新服务器最大时间戳
        updateServerTimestamp(data.timestamp);
        
        // 获取上一个tick数据
        StockData prev_data;
        bool has_previous = getPreviousTick(data.symbol, prev_data);
        
        // 计算价格变化
        double price_change = std::abs(data.last_price - data.close) / data.close;
        
        // 计算瞬时成交量和成交金额变化
        double instant_volume = 0.0;
        double instant_amount = 0.0;
        double volume_ratio = 1.0;
        double amount_ratio = 1.0;
        
        if (has_previous && prev_data.volume > 0 && prev_data.amount > 0) {
            instant_volume = data.volume - prev_data.volume;
            instant_amount = data.amount - prev_data.amount;
            
            // 计算成交量比率（避免除零）
            if (prev_data.volume > 0) {
                volume_ratio = instant_volume / prev_data.volume;
            }
            if (prev_data.amount > 0) {
                amount_ratio = instant_amount / prev_data.amount;
            }
        }
        
        // 计算均价（避免除零）
        double avg_price = 0.0;
        if (data.volume > 0) {
            avg_price = data.amount * 0.01 / data.volume;
        }
        
        // 检测异动条件
        bool is_volatile = false;
        std::string reason;
        
        if (price_change >= config_.price_change_threshold && 
            data.amount >= config_.min_amount_threshold) {
            is_volatile = true;
            reason = "price_change";
        }
        else if (has_previous && volume_ratio >= config_.volume_ratio_threshold &&
                instant_amount >= config_.min_amount_threshold) {
            is_volatile = true;
            reason = "volume_surge";
        }
        else if (has_previous && amount_ratio >= config_.volume_ratio_threshold &&
                instant_amount >= config_.min_amount_threshold) {
            is_volatile = true;
            reason = "amount_surge";
        }
        
        if (is_volatile) {
            std::ostringstream oss;
            oss << "{\"symbol\":\"" << data.symbol 
                << "\",\"timestamp\":" << data.timestamp
                << ",\"price\":" << data.last_price
                << ",\"volume\":" << data.volume
                << ",\"amount\":" << data.amount
                << ",\"instant_volume\":" << instant_volume
                << ",\"instant_amount\":" << instant_amount
                << ",\"volume_ratio\":" << volume_ratio
                << ",\"amount_ratio\":" << amount_ratio
                << ",\"price_change\":" << price_change
                << ",\"reason\":\"" << reason << "\""
                << ",\"detect_time\":" << TimeUtils::getCurrentTimestamp()
                << ",\"is_trial_period\":true}";
            
            // std::cout << oss.str() << std::endl;
            
            bool success = redis_->zadd(config_.volatile_pool_key, 
                                      TimeUtils::getCurrentTimestamp(), 
                                      oss.str());
            
            if (success) {
                redis_->expire(config_.volatile_pool_key, config_.volatile_expire);
            }

            return success;
        }
        
        // 更新当前tick为上一个tick
        updatePreviousTick(data);
        
        return false;
    }
    
    // 获取服务器时间延迟
    long long getServerDelay() const {
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long max_server_time = max_server_timestamp_.load();
        
        if (max_server_time > 0) {
            return current_time - max_server_time;
        }
        return 0;
    }
    
    // 获取平均延迟
    double getAverageDelay() const {
        int count = delay_count_.load();
        long long total = total_delay_.load();
        
        if (count > 0) {
            return static_cast<double>(total) / count;
        }
        return 0.0;
    }
    
    // 重置延迟统计
    void resetDelayStats() {
        total_delay_.store(0);
        delay_count_.store(0);
    }
    
    void cleanupOldData() override {
        if (!redis_->connect()) return;
        
        long long cutoff_time = TimeUtils::getCurrentTimestamp() - 3600000;
        redis_->zremrangebyscore(config_.volatile_pool_key, 0, cutoff_time);
        
        // 清理过时的上一个tick数据（超过5分钟）
        cleanupOldPreviousTicks();
    }
    
private:
    void updateServerTimestamp(long long timestamp) {
        long long current_max = max_server_timestamp_.load();
        if (timestamp > current_max) {
            max_server_timestamp_.store(timestamp);
            // std::cout<<current_max<<std::endl;
            // 计算延迟并统计
            long long current_time = TimeUtils::getCurrentTimestamp();
            long long delay = current_time - timestamp;
            
            total_delay_.fetch_add(delay);
            delay_count_.fetch_add(1);
        }
    }
    
    bool getPreviousTick(const std::string& symbol, StockData& prev_data) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        auto it = previous_ticks_.find(symbol);
        if (it != previous_ticks_.end()) {
            prev_data = it->second;
            return true;
        }
        return false;
    }
    
    void updatePreviousTick(const StockData& current_data) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        previous_ticks_[current_data.symbol] = current_data;
    }
    
    void cleanupOldPreviousTicks() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long cutoff_time = current_time - 300000; // 5分钟
        
        auto it = previous_ticks_.begin();
        while (it != previous_ticks_.end()) {
            if (it->second.timestamp < cutoff_time) {
                it = previous_ticks_.erase(it);
            } else {
                ++it;
            }
        }
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
        
        conn_ = taos_connect(host_.c_str(), user_.c_str(), password_.c_str(), 
                           database_.c_str(), port_);
        
        if (!conn_) {
            const char* err_str = taos_errstr(NULL);
            std::cerr << "Failed to connect to TDengine: " << err_str << std::endl;
            return false;
        }
        
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
        if (sql.length()>50) {
            std::cout << "taos_query" <<sql.length()<<sql.substr(90,140)<< std::endl;
        }
        TAOS_RES* res = taos_query(conn_, sql.c_str());
        
        int code = taos_errno(res);
        // if (config_.verbose) {
            std::cout << "taos_query code:" <<code<< std::endl;
        // }
        if (code != 0) {
            const char* err_str = taos_errstr(res);
            std::string error_msg = "SQL execution failed: " + std::string(err_str);
            taos_free_result(res);
            throw std::runtime_error(error_msg);
        }
        taos_free_result(res);
        // if (config_.verbose) {
            std::cout << "sql execute END" << std::endl;
        // }
    }
};

// 优化的TDengine批量写入器
class OptimizedTDengineWriter : public IDataWriter {
private:
    std::unique_ptr<TDengineConnection> connection_;
    const Config& config_;
    
public:
    OptimizedTDengineWriter(const Config& config) : config_(config) {
        connection_ = std::make_unique<TDengineConnection>(
            config.tdengine_host, config.tdengine_user, 
            config.tdengine_password, config.tdengine_database, 
            config.tdengine_port
        );
    }
    
    ~OptimizedTDengineWriter() {
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
            return false;
        }
        if (config_.verbose) {
            std::cout << "into writeBatch" << std::endl;
        }
        if (!connection_->get() && !connect()) {
            std::cerr << "Failed to connect to TDengine for writing" << std::endl;
            return false;
        }
        
        try {
            return insertUsingSpecifiedFormat(records);
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert records: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    bool insertUsingSpecifiedFormat(const std::vector<StockData>& records) {
        return insertBatch(records);
    }
    
    bool insertBatch(const std::vector<StockData>& batch) {
        std::ostringstream sql;
        sql << "INSERT INTO stock_data(tbname, ts, symbol, exchange, market, "
            << "lp, o, h, l, lc, a, v, p, "
            << "ap1, ap2, ap3, ap4, ap5, "
            << "bp1, bp2, bp3, bp4, bp5, "
            << "av1, av2, av3, av4, av5, "
            << "bv1, bv2, bv3, bv4, bv5) VALUES ";
        
        for (size_t i = 0; i < batch.size(); ++i) {
            const auto& record = batch[i];
            if (i > 0) sql << ", ";
            
            std::string tbname = "t_s_" + sanitizeSymbol(record.symbol);
            std::string escaped_symbol = escapeSingleQuote(record.symbol);
            std::string escaped_exchange = escapeSingleQuote(record.exchange);
            std::string escaped_market = escapeSingleQuote(record.market);
            
            sql << "('" << tbname << "', '" 
                << TimeUtils::formatTimestamp(record.timestamp) << "', '"
                << escaped_symbol << "', '" << escaped_exchange << "', '" << escaped_market << "', "
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
        if (config_.verbose) {
            std::cout << "to sql" << std::endl;
        }
        try {
            connection_->execute(sql.str());
            if (config_.verbose) {
                std::cout << "end sql" << std::endl;
            }
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert batch: " << e.what() << std::endl;
            return false;
        }
    }
    
    std::string sanitizeSymbol(const std::string& symbol) {
        std::string sanitized;
        for (char c : symbol) {
            if (std::isalnum(c) || c == '_') {
                sanitized += c;
            } else {
                sanitized += '_';
            }
        }
        return sanitized;
    }
    
    std::string escapeSingleQuote(const std::string& str) {
        std::string escaped;
        for (char c : str) {
            if (c == '\'') {
                escaped += "''";
            } else {
                escaped += c;
            }
        }
        return escaped;
    }
};

// 基于多线程版本修复的单线程RabbitMQ消费者
class FixedRabbitMQConsumer : public IMessageConsumer {
private:
    const Config& config_;
    amqp_connection_state_t conn_ = nullptr;
    std::atomic<bool> running_{false};
    bool consumer_started_ = false;
    std::string consumer_tag_;
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
    
    bool declareQueue() {
        try {
            // 首先尝试被动声明，检查队列是否存在
            amqp_queue_declare_ok_t* passive_declare = amqp_queue_declare(
                conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                1, 0, 0, 0, amqp_empty_table
            );
            
            amqp_rpc_reply_t passive_reply = amqp_get_rpc_reply(conn_);
            if (passive_reply.reply_type == AMQP_RESPONSE_NORMAL) {
                std::cout << "队列已存在，使用现有队列" << std::endl;
                return true;
            }
            
            // 如果队列不存在，则创建队列
            amqp_queue_declare_ok_t* declare_ok = amqp_queue_declare(
                conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                0, 1, 0, 0, amqp_empty_table
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
    bool startConsumer() {
        try {
            if (consumer_started_) {
                return true;
            }
            
            std::cout << "启动消费者..." << std::endl;
            
            // 设置预取数量为1，一次只消费一条消息
            amqp_basic_qos(conn_, 1, 0, 1, 0);
            
            // 生成唯一的消费者标签
            consumer_tag_ = "single_consumer_" + std::to_string(TimeUtils::getCurrentTimestamp());

            // 开始消费，手动确认
            amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                              amqp_empty_bytes, 0, 0, 1, amqp_empty_table);
            amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(conn_);
            checkAmqpError(consume_reply, "Start consumer");
            
            consumer_started_ = true;
            std::cout << "消费者启动成功 "<< std::endl;
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "启动消费者失败: " << e.what() << std::endl;
            return false;
        }
    }
    
public:
    FixedRabbitMQConsumer(const Config& config) : config_(config) {}
    
    ~FixedRabbitMQConsumer() {
        disconnect();
    }
    
   bool connect() override {
        if (conn_) {
            return true;
        }
        
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
        
        amqp_channel_open(conn_, 1);
        amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn_);
        checkAmqpError(channel_reply, "Open channel");
        
        std::cout << "Connected to RabbitMQ successfully" << std::endl;
        
        // 声明并检查队列
        if (!declareQueue()) {
            return false;
        }
        
        // 启动消费者 - 在整个生命周期中只启动一次
        return startConsumer();
    }
    
    void disconnect() override {
        consumer_started_ = false;
        if (conn_) {
            try {
                // 取消消费者
                if (!consumer_tag_.empty()) {
                    amqp_basic_cancel(conn_, 1, amqp_cstring_bytes(consumer_tag_.c_str()));
                }
                amqp_channel_close(conn_, 1, AMQP_REPLY_SUCCESS);
                amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
            } catch (...) {
            }
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
        }
    }
    
     bool consumeMessages(std::vector<PendingMessage>& messages, int count) override {
        if (!conn_ && !connect()) {
            std::cerr << "Failed to connect to RabbitMQ" << std::endl;
            return false;
        }
        
        if (!consumer_started_ && !startConsumer()) {
            std::cerr << "Failed to start consumer" << std::endl;
            return false;
        }
        
        try {
            messages.clear();
            messages.reserve(1); // 每次只处理一条消息
            
            amqp_envelope_t envelope;
            amqp_maybe_release_buffers(conn_);
            
            struct timeval timeout;
            timeout.tv_sec = 1;  // 1秒超时
            timeout.tv_usec = 0;
            
            amqp_rpc_reply_t ret = amqp_consume_message(conn_, &envelope, &timeout, 0);
            
            if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
                // 安全地拷贝消息数据
                std::vector<char> body;
                if (envelope.message.body.len > 0) {
                    body.resize(envelope.message.body.len);
                    std::memcpy(body.data(), envelope.message.body.bytes, envelope.message.body.len);
                } else {
                    std::cerr << "警告: 收到空消息体" << std::endl;
                }
                
                messages.emplace_back(std::move(body), envelope.delivery_tag);
                
                // 安全地销毁envelope
                amqp_destroy_envelope(&envelope);
                
                if (config_.verbose && !body.empty()) {
                    std::cout << "收到单条消息，delivery_tag: " << envelope.delivery_tag 
                             << ", 大小: " << body.size() << " 字节" << std::endl;
                }
                
                return true;
                
            } else if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && 
                      ret.library_error == AMQP_STATUS_TIMEOUT) {
                // 超时，没有消息
                return false;
            } else {
                // 其他错误
                if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
                    std::cerr << "消费消息错误: " << amqp_error_string2(ret.library_error) << std::endl;
                } else if (ret.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
                    std::cerr << "服务器异常" << std::endl;
                }
                consumer_started_ = false; // 需要重新设置消费者
                return false;
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Exception in consumeMessages: " << e.what() << std::endl;
            consumer_started_ = false; // 需要重新设置消费者
            return false;
        }
    }
    
    void ackMessage(uint64_t delivery_tag) override {
        if (conn_) {
            amqp_basic_ack(conn_, 1, delivery_tag, 0);
            // if (config_.verbose) {
            //     std::cout << "确认消息: " << delivery_tag << std::endl;
            // }
        }
    }
    
    void rejectMessage(uint64_t delivery_tag, bool requeue) override {
        if (conn_) {
            amqp_basic_reject(conn_, 1, delivery_tag, requeue);
            if (config_.verbose) {
                std::cout << "拒绝消息: " << delivery_tag << "，重新入队: " << requeue << std::endl;
            }
        }
    }
    
    void stop() override {
        running_ = false;
    }
};

// ==================== 工厂类 ====================

class ComponentFactory {
public:
    virtual ~ComponentFactory() = default;
    virtual std::unique_ptr<IDataWriter> createDataWriter() = 0;
    virtual std::unique_ptr<IVolatilityDetector> createVolatilityDetector() = 0;
    virtual std::unique_ptr<IMessageProcessor> createMessageProcessor() = 0;
    virtual std::unique_ptr<IMessageConsumer> createMessageConsumer() = 0;
};

class StockDataFactory : public ComponentFactory {
private:
    const Config& config_;
    
public:
    StockDataFactory(const Config& config) : config_(config) {}
    
    std::unique_ptr<IDataWriter> createDataWriter() override {
        return std::make_unique<OptimizedTDengineWriter>(config_);
    }
    
    std::unique_ptr<IVolatilityDetector> createVolatilityDetector() override {
        return std::make_unique<SimpleVolatilityDetector>(config_);
    }
    
    std::unique_ptr<IMessageProcessor> createMessageProcessor() override {
        return std::make_unique<EfficientMessageProcessor>(config_);
    }
    
    std::unique_ptr<IMessageConsumer> createMessageConsumer() override {
        return std::make_unique<FixedRabbitMQConsumer>(config_);
    }
};

// ==================== 单线程处理器 ====================

class SingleThreadedProcessor {
private:
    const Config& config_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IVolatilityDetector> detector_;
    std::unique_ptr<IMessageProcessor> message_processor_;
    std::unique_ptr<IMessageConsumer> consumer_;
    std::atomic<bool> running_{false};
    // 添加延迟统计
    std::chrono::steady_clock::time_point last_delay_report_;
public:
    SingleThreadedProcessor(std::unique_ptr<IDataWriter> writer,
                          std::unique_ptr<IVolatilityDetector> detector,
                          std::unique_ptr<IMessageProcessor> processor,
                          std::unique_ptr<IMessageConsumer> consumer,
                          const Config& config)
        : config_(config), 
          writer_(std::move(writer)),
          detector_(std::move(detector)),
          message_processor_(std::move(processor)),
          consumer_(std::move(consumer)) {}
    
    ~SingleThreadedProcessor() {
        stop();
    }
    
    void start() {
        running_ = true;
        
        if (!writer_->connect()) {
            std::cerr << "Failed to connect to TDengine" << std::endl;
            return;
        }
        
        // 注意：这里不再预先连接RabbitMQ，而是在消费消息时连接
        
        std::cout << "Starting single-threaded processor..." << std::endl;
        std::cout << "Processing " << config_.messages_per_batch << " messages per batch" << std::endl;
        
        int total_messages = 0;
        int total_records = 0;
        int failed_messages = 0;
        int empty_cycles = 0;
        
        auto last_cleanup = std::chrono::steady_clock::now();
        auto last_report = std::chrono::steady_clock::now();
        last_delay_report_ = std::chrono::steady_clock::now();
        while (running_) {
            // 获取单条消息
            std::vector<PendingMessage> messages;
            //每条消息休息400ms
            if (config_.enable_rate_limiting) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(int(config_.processing_delay_ms)));
            }
            if (config_.verbose) {
                std::cout << "尝试获取单条消息..." << std::endl;
            }
            
            bool has_messages = consumer_->consumeMessages(messages, config_.messages_per_batch);
            
            if (!has_messages) {
                // 没有消息，等待一段时间再重试
                empty_cycles++;
                if (empty_cycles % 100 == 0) { // 每100次空循环报告一次
                    std::cout << "等待消息中... (" << empty_cycles << " 次空循环)" << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(10)); // 减少等待时间
                continue;
            }
            
            empty_cycles = 0; // 重置空循环计数
            
            if (config_.verbose && !messages.empty()) {
                std::cout << "收到单条消息，准备处理" << std::endl;
            }
            
            // 处理单条消息
            std::vector<StockData> all_records;
            std::vector<PendingMessage> valid_messages;
            
            for (auto& message : messages) {
                std::vector<StockData> records;
                 if (config_.verbose) {
                    std::cout << "processMessage 111" << std::endl;
                }
                if (message_processor_->processMessage(message.data, records)) {
                    all_records.insert(all_records.end(), 
                                     std::make_move_iterator(records.begin()),
                                     std::make_move_iterator(records.end()));
                    valid_messages.push_back(std::move(message));
                } else {
                    failed_messages++;
                    std::cerr << "消息处理失败，delivery_tag: " << message.delivery_tag << std::endl;
                    consumer_->rejectMessage(message.delivery_tag, true); // 处理失败，退回队列
                }
            }
            if (config_.verbose) {
                std::cout << "准备写入数据" << std::endl;
            }
            // 写入数据
            if (!all_records.empty()) {
                bool write_success = false;
                int retry_count = 0;
                
                // 重试机制
                while (retry_count < config_.max_retry_count && !write_success) {
                    write_success = writer_->writeBatch(all_records);
                    
                    if (!write_success) {
                        retry_count++;
                        std::cerr << "写入失败，重试 " << retry_count 
                                 << "/" << config_.max_retry_count << std::endl;
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(config_.retry_delay_ms));
                        if(retry_count>3){
                            std::cerr << "写入3次失败"<< std::endl;
                            break;
                        }
                    }
                }
                if (config_.verbose) {
                    std::cout << "存储成功，确认消息" << std::endl;
                }
                if (write_success) {
                    // 存储成功，确认消息
                    for (auto& message : valid_messages) {
                        consumer_->ackMessage(message.delivery_tag);
                    }
                    
                    total_messages += valid_messages.size();
                    total_records += all_records.size();
                    
                    if (config_.verbose) {
                        std::cout << "成功处理单条消息，包含 " << all_records.size() << " 条记录" << std::endl;
                    }
                    
                    // 异步异动检测
                    std::thread([this, batch_copy = all_records]() mutable {
                        for (const auto& record : batch_copy) {
                            detector_->detectVolatility(record);
                        }
                    }).detach();
                    
                } else {
                    // 存储失败，退回消息
                    std::cerr << "写入失败，已重试 " << config_.max_retry_count 
                             << " 次，退回消息" << std::endl;
                    for (auto& message : valid_messages) {
                        consumer_->rejectMessage(message.delivery_tag, true);
                    }
                    failed_messages += valid_messages.size();
                }
            }
            
            auto now = std::chrono::steady_clock::now();
            
            // // 定期清理
            if (std::chrono::duration_cast<std::chrono::minutes>(now - last_cleanup).count() >= 10) {
                detector_->cleanupOldData();
                last_cleanup = now;
            }
            
            // 进度报告
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 5) {
               // 获取延迟统计
                long long current_delay = dynamic_cast<SimpleVolatilityDetector*>(detector_.get())->getServerDelay();
                double avg_delay = dynamic_cast<SimpleVolatilityDetector*>(detector_.get())->getAverageDelay();
                
                std::cout << "进度: " << total_messages << " 条消息, " 
                         << total_records << " 条记录, " 
                         << failed_messages << " 条失败, 速度: " << total_messages/30 << " 每秒, "
                         << "服务器延迟: " << int(current_delay*0.001) << "s, "
                         << "平均延迟: " << int(avg_delay*0.001) << "s" << std::endl;
                
                // 重置统计
                last_report = now;
                total_messages = 0;
                total_records = 0;
                failed_messages = 0;
                dynamic_cast<SimpleVolatilityDetector*>(detector_.get())->resetDelayStats();
            }
        }
        
        std::cout << "处理器结束: " << total_messages << " 条消息, " 
                  << total_records << " 条记录, " << failed_messages << " 条失败" << std::endl;
        
        writer_->close();
        consumer_->disconnect();
    }
    
    void stop() {
        running_ = false;
        consumer_->stop();
    }
};

// ==================== 应用主类 ====================

class SingleThreadedConsumerApplication {
private:
    Config config_;
    std::unique_ptr<SingleThreadedProcessor> processor_;
    std::atomic<bool> shutdown_{false};
    static std::atomic<bool> signal_received_;
    
public:
    SingleThreadedConsumerApplication(const Config& config) : config_(config) {
        auto factory = std::make_unique<StockDataFactory>(config);
        processor_ = std::make_unique<SingleThreadedProcessor>(
            factory->createDataWriter(),
            factory->createVolatilityDetector(),
            factory->createMessageProcessor(),
            factory->createMessageConsumer(),
            config_
        );
        
        setupSignalHandlers();
    }
    
    void run() {
        std::cout << "Starting single-threaded application..." << std::endl;
        std::cout << "Messages per batch: " << config_.messages_per_batch << std::endl;
        
        // processor_->start();
        // 方案1：使用后台线程（推荐）
        std::thread processor_thread([this]() {
            processor_->start();
        });
        waitForShutdown();
        
        shutdown();
        // 等待处理器线程结束
        if (processor_thread.joinable()) {
            processor_thread.join();
        }
    
    // std::cout << "Application stopped gracefully" << std::endl;
        std::cout << "Application stopped" << std::endl;
    }
    
private:
    static void signalHandler(int signal) {
        std::cout << "\nReceived signal " << signal << std::endl;
        signal_received_ = true;
    }
    
    void setupSignalHandlers() {
        signal_received_ = false;
        signal(SIGINT, signalHandler);
        signal(SIGTERM, signalHandler);
    }
    
    void waitForShutdown() {
        std::cout << "Running... Press Ctrl+C to stop" << std::endl;
        
        while (!shutdown_ && !signal_received_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        if (signal_received_) {
            shutdown_ = true;
        }
    }
    
    void shutdown() {
        std::cout << "Shutting down..." << std::endl;
        processor_->stop();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
};

std::atomic<bool> SingleThreadedConsumerApplication::signal_received_{false};

// ==================== 主函数 ====================

int main() {
    try {
        auto& configManager = ConfigManager::getInstance();
        SingleThreadedConsumerApplication app(configManager.getConfig());
        app.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}