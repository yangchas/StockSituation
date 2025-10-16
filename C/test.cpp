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
    
    int batch_size = 500;
    int worker_count = 4;
    int buffer_size = 50000;
    double flush_timeout = 3.0;
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
        redis_->connect();
    }
    
    bool detectVolatility(const StockData& data) override {
        // 简化版异动检测：只检测价格变化
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
        long long cutoff_time = getCurrentTimestamp() - 3600000; // 1小时前
        redis_->zremrangebyscore(config_.volatile_pool_key, 0, cutoff_time);
    }
    
private:
    long long getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
    }
};

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
        
        // 创建数据库和超级表
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
        
        std::cout << "Connected to TDengine successfully" << std::endl;
        return true;
    }
    
    void close() override {
        if (connection_) {
            connection_->close();
        }
    }
    
    bool writeBatch(const std::vector<StockData>& records) override {
        if (records.empty()) return true;
        if (!connection_->get() && !connect()) return false;
        
        // 按符号分组
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
        if (!conn_) return false;
        
        amqp_socket_t* socket = amqp_tcp_socket_new(conn_);
        if (!socket) {
            amqp_destroy_connection(conn_);
            return false;
        }
        
        if (amqp_socket_open(socket, config_.rabbitmq_host.c_str(), config_.rabbitmq_port) != AMQP_STATUS_OK) {
            amqp_destroy_connection(conn_);
            return false;
        }
        
        amqp_login(conn_, config_.rabbitmq_vhost.c_str(), 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, 
                  config_.rabbitmq_user.c_str(), config_.rabbitmq_password.c_str());
        
        amqp_channel_open(conn_, 1);
        
        // 声明队列
        amqp_queue_declare(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                          0, 1, 0, 0, amqp_empty_table);
        
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
        
        amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                          amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        
        running_ = true;
        int message_count = 0;
        auto start_time = std::chrono::steady_clock::now();
        
        while (running_) {
            amqp_envelope_t envelope;
            amqp_maybe_release_buffers(conn_);
            
            amqp_rpc_reply_t ret = amqp_consume_message(conn_, &envelope, nullptr, 0);
            
            if (ret.reply_type != AMQP_RESPONSE_NORMAL) {
                if (running_) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                continue;
            }
            
            // 修复指针算术问题
            char* body_start = static_cast<char*>(envelope.message.body.bytes);
            std::vector<char> body(body_start, body_start + envelope.message.body.len);
            
            if (!queue.push(std::move(body))) {
                break;
            }
            
            amqp_destroy_envelope(&envelope);
            message_count++;
            
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
    }
    
    void stop() {
        running_ = false;
    }
};

class Worker {
private:
    Config config_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IVolatilityDetector> detector_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    
public:
    Worker(const Config& config) : config_(config) {
        writer_ = std::make_unique<TDengineBatchWriter>(config);
        detector_ = std::make_unique<SimpleVolatilityDetector>(config);
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
        if (!writer_->connect()) {
            std::cerr << "Failed to connect to TDengine" << std::endl;
            return;
        }
        
        std::vector<StockData> batch;
        auto last_flush = std::chrono::steady_clock::now();
        
        // 简化的消息处理 - 实际应该解析protobuf
        auto processMessage = [](const std::vector<char>& body, std::vector<StockData>& records) -> bool {
            // 这里应该解析protobuf消息
            // 为演示目的，我们创建一些模拟数据
            StockData data;
            data.symbol = "TEST";
            data.exchange = "SH";
            data.market = "A";
            data.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            data.last_price = 100.0 + (rand() % 100) / 10.0;
            data.open = 100.0;
            data.high = 105.0;
            data.low = 95.0;
            data.close = 100.0;
            data.volume = 1000000;
            data.amount = 100000000;
            
            for (int i = 0; i < 5; i++) {
                data.ask_prices[i] = data.last_price + i * 0.1;
                data.bid_prices[i] = data.last_price - i * 0.1;
                data.ask_volumes[i] = 1000 * (i + 1);
                data.bid_volumes[i] = 1000 * (i + 1);
            }
            
            records.push_back(data);
            return true;
        };
        
        while (running_) {
            std::vector<char> message;
            if (!queue.pop(message)) {
                break;
            }
            
            std::vector<StockData> records;
            if (processMessage(message, records)) {
                batch.insert(batch.end(), records.begin(), records.end());
            }
            
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_flush).count();
            
            if (batch.size() >= config_.batch_size || elapsed >= config_.flush_timeout) {
                if (!batch.empty()) {
                    if (writer_->writeBatch(batch)) {
                        // 异动检测
                        for (const auto& record : batch) {
                            detector_->detectVolatility(record);
                        }
                        
                        if (config_.verbose) {
                            std::cout << "Inserted " << batch.size() << " records" << std::endl;
                        }
                    }
                    batch.clear();
                }
                last_flush = now;
                
                // 定期清理异动池数据
                static auto last_cleanup = now;
                if (std::chrono::duration_cast<std::chrono::minutes>(now - last_cleanup).count() >= 5) {
                    detector_->cleanupOldData();
                    last_cleanup = now;
                }
            }
        }
        
        // 处理剩余数据
        if (!batch.empty()) {
            writer_->writeBatch(batch);
        }
        
        writer_->close();
    }
};

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
        
        // 创建工作线程
        for (int i = 0; i < config.worker_count; ++i) {
            workers_.push_back(std::make_unique<Worker>(config));
        }
    }
    
    void run() {
        std::cout << "Starting TDengine consumer with " 
                  << config_.worker_count << " workers" << std::endl;
        
        // 启动工作线程
        for (auto& worker : workers_) {
            worker->start(queue_);
        }
        
        // 启动RabbitMQ消费者
        std::thread consumer_thread([this]() {
            rabbitmq_->consume(queue_);
        });
        
        // 等待关闭信号
        waitForShutdown();
        
        // 停止服务
        shutdown();
        
        if (consumer_thread.joinable()) {
            consumer_thread.join();
        }
    }
    
private:
    void waitForShutdown() {
        std::cout << "Press Ctrl+C to stop..." << std::endl;
        
        // 设置信号处理
        std::signal(SIGINT, [](int) { });
        
        while (!shutdown_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    
    void shutdown() {
        shutdown_ = true;
        rabbitmq_->stop();
        queue_.shutdown();
        
        for (auto& worker : workers_) {
            worker->stop();
        }
        
        std::cout << "Application shutdown completed" << std::endl;
    }
};

// Main 函数
int main() {
    Config config;
    
    // 可以从环境变量加载配置
    const char* env_workers = std::getenv("WORKER_COUNT");
    if (env_workers) {
        config.worker_count = std::atoi(env_workers);
    }
    
    ConsumerApplication app(config);
    app.run();
    
    return 0;
}