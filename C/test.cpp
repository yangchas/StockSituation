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
#include <sys/time.h>
#include <zlib.h>
#include <hiredis/hiredis.h>
#include <taos.h>

// 添加缺失的头文件
#include <sstream>
#include <iomanip>
#include <csignal>
#include <cstdlib>
#include <functional>

// 使用新的 RabbitMQ-C 头文件
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

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
    
    // Redis配置
    std::string redis_host = "localhost";
    int redis_port = 6379;
    int redis_db = 0;
    
    // 异动池配置
    std::string volatile_pool_key = "stock:volatile_pool";
    int volatile_expire = 300;
    
    // 异动检测阈值
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000;
    
    // TDengine配置
    std::string tdengine_host = "localhost";
    int tdengine_port = 6030;
    std::string tdengine_user = "root";
    std::string tdengine_password = "taosdata";
    std::string tdengine_database = "market_data";
};

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

// 抽象接口类
class IDataWriter {
public:
    virtual ~IDataWriter() = default;
    virtual bool connect() = 0;
    virtual void close() = 0;
    virtual bool writeBatch(const std::vector<StockData>& records) = 0;
};

class IVolatilityDetector {
public:
    virtual ~IVolatilityDetector() = default;
    virtual bool detectVolatility(const StockData& data) = 0;
    virtual void cleanupOldData() = 0;
};

// 高效的消息处理器 - 完全不依赖 Protobuf
class EfficientMessageProcessor {
private:
    Config config_;
    
public:
    EfficientMessageProcessor(const Config& config) : config_(config) {}
    
    // 解析消息 - 假设消息格式为简单的二进制格式
    bool processMessage(const std::vector<char>& body, std::vector<StockData>& records) {
        try {
            if (body.size() < 8) { // 至少需要头部信息
                if (config_.verbose) {
                    std::cerr << "Message too small: " << body.size() << " bytes" << std::endl;
                }
                return false;
            }
            
            // 解析消息头部
            const char* data = body.data();
            size_t offset = 0;
            
            // 假设头部格式: [4字节压缩类型][4字节记录数]
            int32_t compression_type;
            int32_t record_count;
            
            std::memcpy(&compression_type, data + offset, 4);
            offset += 4;
            std::memcpy(&record_count, data + offset, 4);
            offset += 4;
            
            // 网络字节序转换（如果需要）
            // compression_type = ntohl(compression_type);
            // record_count = ntohl(record_count);
            
            if (record_count <= 0 || record_count > 10000) {
                std::cerr << "Invalid record count: " << record_count << std::endl;
                return false;
            }
            
            // 获取压缩数据
            const char* compressed_data = data + offset;
            size_t compressed_size = body.size() - offset;
            
            std::vector<char> decompressed_data;
            const char* batch_data = nullptr;
            size_t batch_size = 0;
            
            // 处理压缩
            switch (compression_type) {
                case COMPRESSION_NONE:
                    batch_data = compressed_data;
                    batch_size = compressed_size;
                    break;
                    
                case COMPRESSION_GZIP:
                case COMPRESSION_DEFLATE:
                    if (!decompressData(compressed_data, compressed_size, decompressed_data, 
                                       compression_type == COMPRESSION_GZIP)) {
                        std::cerr << "Failed to decompress data" << std::endl;
                        return false;
                    }
                    batch_data = decompressed_data.data();
                    batch_size = decompressed_data.size();
                    break;
                    
                default:
                    std::cerr << "Unknown compression type: " << compression_type << std::endl;
                    return false;
            }
            
            // 解析数据批次
            return parseDataBatch(batch_data, batch_size, record_count, records);
            
        } catch (const std::exception& e) {
            std::cerr << "Error processing message: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    // 高效解压缩
    bool decompressData(const char* compressed, size_t compressed_size, 
                       std::vector<char>& decompressed, bool is_gzip) {
        if (compressed_size == 0) return false;
        
        z_stream strm;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        strm.avail_in = compressed_size;
        strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed));
        
        // 设置解压格式
        int window_bits = is_gzip ? (MAX_WBITS + 16) : MAX_WBITS;
        
        if (inflateInit2(&strm, window_bits) != Z_OK) {
            return false;
        }
        
        // 预分配缓冲区
        decompressed.reserve(compressed_size * 3);
        
        const size_t CHUNK_SIZE = 65536;
        std::vector<char> buffer(CHUNK_SIZE);
        int ret;
        
        do {
            strm.avail_out = buffer.size();
            strm.next_out = reinterpret_cast<Bytef*>(buffer.data());
            
            ret = inflate(&strm, Z_NO_FLUSH);
            
            if (ret != Z_OK && ret != Z_STREAM_END) {
                inflateEnd(&strm);
                return false;
            }
            
            size_t have = buffer.size() - strm.avail_out;
            decompressed.insert(decompressed.end(), buffer.begin(), buffer.begin() + have);
            
        } while (ret != Z_STREAM_END);
        
        inflateEnd(&strm);
        return true;
    }
    
    // 解析数据批次 - 假设为固定格式的二进制数据
    bool parseDataBatch(const char* batch_data, size_t batch_size, int record_count, 
                       std::vector<StockData>& records) {
        if (batch_size == 0 || record_count == 0) {
            return false;
        }
        
        // 假设每条记录固定大小（根据您的数据结构调整）
        const size_t RECORD_SIZE = 256; // 估算大小，根据实际调整
        
        // 检查数据大小是否足够
        if (batch_size < record_count * 50) { // 最小检查，每条记录至少50字节
            std::cerr << "Batch size too small for " << record_count << " records" << std::endl;
            return false;
        }
        
        records.reserve(record_count);
        size_t offset = 0;
        
        for (int i = 0; i < record_count && offset < batch_size; i++) {
            StockData data;
            
            // 解析符号（假设前20字节）
            if (offset + 20 > batch_size) break;
            data.symbol = std::string(batch_data + offset, 20);
            data.symbol.erase(data.symbol.find_last_not_of(' ') + 1); // 去除尾部空格
            offset += 20;
            
            // 解析时间戳（8字节）
            if (offset + 8 > batch_size) break;
            std::memcpy(&data.timestamp, batch_data + offset, 8);
            offset += 8;
            
            // 解析价格数据（各4字节）
            if (offset + 20 > batch_size) break;
            std::memcpy(&data.last_price, batch_data + offset, 4); offset += 4;
            std::memcpy(&data.open, batch_data + offset, 4); offset += 4;
            std::memcpy(&data.high, batch_data + offset, 4); offset += 4;
            std::memcpy(&data.low, batch_data + offset, 4); offset += 4;
            std::memcpy(&data.close, batch_data + offset, 4); offset += 4;
            
            // 解析成交量和成交额（各8字节）
            if (offset + 16 > batch_size) break;
            std::memcpy(&data.volume, batch_data + offset, 8); offset += 8;
            std::memcpy(&data.amount, batch_data + offset, 8); offset += 8;
            
            // 设置默认值
            data.exchange = "SH";
            data.market = "A";
            
            // 解析五档行情（如果数据中包含）
            // 买价
            for (int j = 0; j < 5 && offset + 4 <= batch_size; j++) {
                std::memcpy(&data.ask_prices[j], batch_data + offset, 4);
                offset += 4;
            }
            // 卖价
            for (int j = 0; j < 5 && offset + 4 <= batch_size; j++) {
                std::memcpy(&data.bid_prices[j], batch_data + offset, 4);
                offset += 4;
            }
            // 买量
            for (int j = 0; j < 5 && offset + 8 <= batch_size; j++) {
                std::memcpy(&data.ask_volumes[j], batch_data + offset, 8);
                offset += 8;
            }
            // 卖量
            for (int j = 0; j < 5 && offset + 8 <= batch_size; j++) {
                std::memcpy(&data.bid_volumes[j], batch_data + offset, 8);
                offset += 8;
            }
            
            // 如果数据有效，添加到结果
            if (!data.symbol.empty() && data.timestamp > 0) {
                records.push_back(std::move(data));
            }
        }
        
        if (config_.verbose && !records.empty()) {
            std::cout << "Processed " << records.size() << " records from binary message" << std::endl;
        }
        
        return !records.empty();
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

class SimpleVolatilityDetector : public IVolatilityDetector {
private:
    std::unique_ptr<RedisClient> redis_;
    Config config_;
    
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
                << ",\"detect_time\":" << getCurrentTimestamp()
                << ",\"is_trial_period\":true}";
            
            bool success = redis_->zadd(config_.volatile_pool_key, 
                                      getCurrentTimestamp(), 
                                      oss.str());
            
            if (success) {
                redis_->expire(config_.volatile_pool_key, config_.volatile_expire);
                if (config_.verbose) {
                    std::cout << "Detected volatility for symbol: " << data.symbol << std::endl;
                }
            }
            
            return success;
        }
        
        return false;
    }
    
    void cleanupOldData() override {
        if (!redis_->connect()) return;
        
        long long cutoff_time = getCurrentTimestamp() - 3600000;
        redis_->zremrangebyscore(config_.volatile_pool_key, 0, cutoff_time);
    }
    
private:
    long long getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
    }
};

// TDengine连接和写入器
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

class TDengineBatchWriter : public IDataWriter {
private:
    std::unique_ptr<TDengineConnection> connection_;
    Config config_;
    
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
    
    std::string formatTimestamp(long long timestamp) {
        auto time = std::chrono::system_clock::from_time_t(timestamp / 1000);
        auto ms = timestamp % 1000;
        
        auto tt = std::chrono::system_clock::to_time_t(time);
        std::tm tm = *std::localtime(&tt);
        
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "." << std::setfill('0') 
            << std::setw(3) << ms;
        return oss.str();
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
            
            sql << "('" << formatTimestamp(record.timestamp) << "', "
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
            if (config_.verbose) {
                std::cout << "Inserted " << records.size() << " records for symbol: " << symbol << std::endl;
            }
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert records: " << e.what() << std::endl;
            return false;
        }
    }
};

// 线程安全队列
class ThreadSafeQueue {
private:
    std::queue<std::vector<char>> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cond_;
    std::atomic<bool> shutdown_{false};
    
public:
    bool push(std::vector<char>&& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) return false;
        
        queue_.push(std::move(item));
        cond_.notify_one();
        return true;
    }
    
    bool pop(std::vector<char>& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this]() { return !queue_.empty() || shutdown_; });
        
        if (shutdown_ && queue_.empty()) return false;
        
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }
    
    void shutdown() {
        shutdown_ = true;
        cond_.notify_all();
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
};

// RabbitMQ消费者
class RabbitMQConsumer {
private:
    Config config_;
    amqp_connection_state_t conn_ = nullptr;
    std::atomic<bool> running_{false};
    
public:
    RabbitMQConsumer(const Config& config) : config_(config) {}
    
    ~RabbitMQConsumer() {
        disconnect();
    }
    
    bool connect() {
        conn_ = amqp_new_connection();
        if (!conn_) {
            std::cerr << "Failed to create RabbitMQ connection" << std::endl;
            return false;
        }
        
        amqp_socket_t* socket = amqp_tcp_socket_new(conn_);
        if (!socket) {
            std::cerr << "Failed to create RabbitMQ socket" << std::endl;
            amqp_destroy_connection(conn_);
            return false;
        }
        
        std::cout << "Connecting to RabbitMQ at " << config_.rabbitmq_host << ":" << config_.rabbitmq_port << std::endl;
        if (amqp_socket_open(socket, config_.rabbitmq_host.c_str(), config_.rabbitmq_port) != AMQP_STATUS_OK) {
            std::cerr << "Failed to open RabbitMQ socket" << std::endl;
            amqp_destroy_connection(conn_);
            return false;
        }
        
        amqp_rpc_reply_t login_reply = amqp_login(conn_, config_.rabbitmq_vhost.c_str(), 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, 
                  config_.rabbitmq_user.c_str(), config_.rabbitmq_password.c_str());
        
        if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
            std::cerr << "Failed to login to RabbitMQ" << std::endl;
            amqp_destroy_connection(conn_);
            return false;
        }
        
        amqp_channel_open(conn_, 1);
        
        amqp_queue_declare(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                          0, 1, 0, 0, amqp_empty_table);
        
        std::cout << "Connected to RabbitMQ successfully, queue: " << config_.queue_name << std::endl;
        return true;
    }
    
    void disconnect() {
        if (conn_) {
            amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
        }
    }
    
    void consume(ThreadSafeQueue& queue) {
    if (!conn_ && !connect()) {
        std::cerr << "Failed to connect to RabbitMQ" << std::endl;
        return;
    }
    
    // 检查队列状态
    amqp_queue_declare_ok_t* declare_ok = amqp_queue_declare(
        conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
        0, 1, 0, 0, amqp_empty_table
    );
    
    if (declare_ok == NULL) {
        std::cerr << "Failed to declare queue" << std::endl;
        return;
    }
    
    std::cout << "Queue '" << config_.queue_name << "' has " 
              << declare_ok->message_count << " messages ready and " 
              << declare_ok->consumer_count << " consumers" << std::endl;
    
    amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                      amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    
    running_ = true;
    int message_count = 0;
    auto start_time = std::chrono::steady_clock::now();
    
    std::cout << "Starting to consume messages from RabbitMQ..." << std::endl;
    
    while (running_) {
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn_);
        
        std::cout << "Waiting for message..." << std::endl;
        
        amqp_rpc_reply_t ret = amqp_consume_message(conn_, &envelope, nullptr, 0);
        
        if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
            // 成功收到消息
            char* body_start = static_cast<char*>(envelope.message.body.bytes);
            std::vector<char> body(body_start, body_start + envelope.message.body.len);
            
            std::cout << "Received message, size: " << body.size() << " bytes" << std::endl;
            
            if (!queue.push(std::move(body))) {
                std::cerr << "Failed to push message to queue" << std::endl;
                amqp_destroy_envelope(&envelope);
                break;
            }
            
            amqp_destroy_envelope(&envelope);
            message_count++;
            
            if (config_.verbose) {
                std::cout << "Successfully processed message " << message_count << std::endl;
            }
            
        } else {
            // 处理错误
            if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
                std::cout << "Library exception: " << amqp_error_string2(ret.library_error) << std::endl;
            } else if (ret.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
                std::cout << "Server exception: " << (ret.reply.decoded ? "decoded" : "not decoded") << std::endl;
            }
            
            if (running_) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
            continue;
        }
        
        // 定期报告
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
    
    std::cout << "RabbitMQ consumer stopped" << std::endl;
}
    
    void stop() {
        running_ = false;
    }
};

// Worker类使用高效的消息处理器
class Worker {
private:
    Config config_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IVolatilityDetector> detector_;
    std::unique_ptr<EfficientMessageProcessor> message_processor_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    int worker_id_;
    static std::atomic<int> next_worker_id_;
    
public:
    Worker(const Config& config) : config_(config) {
        worker_id_ = next_worker_id_++;
        writer_ = std::make_unique<TDengineBatchWriter>(config);
        detector_ = std::make_unique<SimpleVolatilityDetector>(config);
        message_processor_ = std::make_unique<EfficientMessageProcessor>(config);
    }
    
    ~Worker() {
        stop();
    }
    
    void start(ThreadSafeQueue& queue) {
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
    void run(ThreadSafeQueue& queue) {
        std::cout << "Worker " << worker_id_ << " started" << std::endl;
        
        if (!writer_->connect()) {
            std::cerr << "Worker " << worker_id_ << ": Failed to connect to TDengine" << std::endl;
            return;
        }
        
        std::vector<StockData> batch;
        auto last_flush = std::chrono::steady_clock::now();
        auto last_cleanup = std::chrono::steady_clock::now();
        
        int processed_messages = 0;
        int processed_records = 0;
        
        while (running_) {
            std::vector<char> message;
            if (!queue.pop(message)) {
                break;
            }
            
            std::vector<StockData> records;
            if (message_processor_->processMessage(message, records)) {
                processed_messages++;
                processed_records += records.size();
                batch.insert(batch.end(), records.begin(), records.end());
            } else {
                std::cerr << "Worker " << worker_id_ << ": Failed to process message" << std::endl;
            }
            
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count();
            
            if (batch.size() >= config_.batch_size || elapsed >= config_.flush_timeout) {
                if (!batch.empty()) {
                    if (writer_->writeBatch(batch)) {
                        for (const auto& record : batch) {
                            detector_->detectVolatility(record);
                        }
                        
                        if (config_.verbose) {
                            std::cout << "Worker " << worker_id_ << ": Inserted " << batch.size() << " records" << std::endl;
                        }
                    } else {
                        std::cerr << "Worker " << worker_id_ << ": Failed to write batch" << std::endl;
                    }
                    batch.clear();
                }
                last_flush = now;
            }
            
            if (processed_messages % 100 == 0 && processed_messages > 0) {
                std::cout << "Worker " << worker_id_ << ": Processed " << processed_messages 
                         << " messages, " << processed_records << " records" << std::endl;
            }
            
            if (std::chrono::duration_cast<std::chrono::minutes>(now - last_cleanup).count() >= 5) {
                detector_->cleanupOldData();
                last_cleanup = now;
            }
        }
        
        if (!batch.empty()) {
            std::cout << "Worker " << worker_id_ << ": Processing final batch of " << batch.size() << " records" << std::endl;
            writer_->writeBatch(batch);
        }
        
        std::cout << "Worker " << worker_id_ << " finished. Total: " 
                  << processed_messages << " messages, " << processed_records << " records" << std::endl;
        
        writer_->close();
    }
};

std::atomic<int> Worker::next_worker_id_{0};

class ConsumerApplication {
private:
    Config config_;
    std::unique_ptr<RabbitMQConsumer> rabbitmq_;
    std::vector<std::unique_ptr<Worker>> workers_;
    ThreadSafeQueue queue_;
    std::atomic<bool> shutdown_{false};
    
public:
    ConsumerApplication(const Config& config) : config_(config) {
        rabbitmq_ = std::make_unique<RabbitMQConsumer>(config);
        
        for (int i = 0; i < config.worker_count; ++i) {
            workers_.push_back(std::make_unique<Worker>(config));
        }
    }
    
    void run() {
        std::cout << "Starting TDengine consumer with " 
                  << config_.worker_count << " workers" << std::endl;
        
        for (auto& worker : workers_) {
            worker->start(queue_);
        }
        
        std::thread consumer_thread([this]() {
            rabbitmq_->consume(queue_);
        });
        
        waitForShutdown();
        
        shutdown();
        
        if (consumer_thread.joinable()) {
            consumer_thread.join();
        }
        
        std::cout << "Application shutdown completed" << std::endl;
    }
    
private:
    void waitForShutdown() {
        std::cout << "Press Ctrl+C to stop..." << std::endl;
        
        std::signal(SIGINT, [](int) { 
            std::cout << "\nReceived shutdown signal" << std::endl;
        });
        
        while (!shutdown_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            static auto last_report = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 30) {
                std::cout << "Queue size: " << queue_.size() << std::endl;
                last_report = now;
            }
        }
    }
    
    void shutdown() {
        std::cout << "Initiating shutdown..." << std::endl;
        shutdown_ = true;
        rabbitmq_->stop();
        queue_.shutdown();
        
        for (auto& worker : workers_) {
            worker->stop();
        }
    }
};

int main() {
    std::cout << "TDengine Consumer Application Starting..." << std::endl;
    
    Config config;
    
    const char* env_workers = std::getenv("WORKER_COUNT");
    if (env_workers) {
        config.worker_count = std::atoi(env_workers);
    }
    
    try {
        ConsumerApplication app(config);
        app.run();
    } catch (const std::exception& e) {
        std::cerr << "Application error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}