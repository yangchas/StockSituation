#include "consumer.h"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <zlib.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <hiredis/hiredis.h>
#include <taos.h>

// 简化版protobuf解析（实际需要根据schema生成）
class ProtobufParser {
public:
    bool parseBatch(const std::vector<char>& data, std::vector<StockData>& records) {
        // 这里应该根据实际的protobuf schema进行解析
        // 为简化演示，我们假设数据已经过处理
        return true;
    }
};

class ZlibDecompressor {
public:
    bool decompress(const std::vector<char>& compressed, std::vector<char>& decompressed) {
        if (compressed.empty()) return false;
        
        z_stream strm;
        strm.zalloc = Z_NULL;
        strm.zfree = Z_NULL;
        strm.opaque = Z_NULL;
        strm.avail_in = compressed.size();
        strm.next_in = (Bytef*)compressed.data();
        
        if (inflateInit(&strm) != Z_OK) {
            return false;
        }
        
        std::vector<char> buffer(32768);
        int ret;
        
        do {
            strm.avail_out = buffer.size();
            strm.next_out = (Bytef*)buffer.data();
            ret = inflate(&strm, Z_NO_FLUSH);
            
            if (decompressed.size() < strm.total_out) {
                decompressed.insert(decompressed.end(), 
                                  buffer.begin(), 
                                  buffer.begin() + (strm.total_out - decompressed.size()));
            }
        } while (ret == Z_OK);
        
        inflateEnd(&strm);
        return ret == Z_STREAM_END;
    }
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

class TDengineWriter : public IDataWriter {
private:
    TAOS* conn_ = nullptr;
    Config config_;
    
public:
    TDengineWriter(const Config& config) : config_(config) {}
    
    ~TDengineWriter() {
        close();
    }
    
    bool connect() override {
        if (conn_) return true;
        
        conn_ = taos_connect(config_.tdengine_host.c_str(), 
                           config_.tdengine_user.c_str(), 
                           config_.tdengine_password.c_str(), 
                           config_.tdengine_database.c_str(), 
                           config_.tdengine_port);
        
        if (!conn_) {
            std::cerr << "Failed to connect to TDengine" << std::endl;
            return false;
        }
        
        // 使用数据库
        std::string use_db = "USE " + config_.tdengine_database;
        TAOS_RES* res = taos_query(conn_, use_db.c_str());
        if (taos_errno(res) != 0) {
            std::cerr << "Failed to use database: " << taos_errstr(res) << std::endl;
            taos_free_result(res);
            return false;
        }
        taos_free_result(res);
        
        return true;
    }
    
    void close() override {
        if (conn_) {
            taos_close(conn_);
            conn_ = nullptr;
        }
    }
    
    bool writeBatch(const std::vector<StockData>& records) override {
        if (records.empty()) return true;
        if (!conn_ && !connect()) return false;
        
        // 按符号分组
        std::unordered_map<std::string, std::vector<const StockData*>> grouped;
        for (const auto& record : records) {
            grouped[record.symbol].push_back(&record);
        }
        
        for (const auto& [symbol, symbol_records] : grouped) {
            if (!insertSymbolRecords(symbol, symbol_records)) {
                std::cerr << "Failed to insert records for symbol: " << symbol << std::endl;
                return false;
            }
        }
        
        return true;
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
        auto tm = *std::localtime(&tt);
        
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
            " USING market_data.stock_data TAGS ('" + symbol + "', '" + 
            exchange + "', '" + market + "')";
        
        TAOS_RES* res = taos_query(conn_, create_sql.c_str());
        if (taos_errno(res) != 0) {
            std::cerr << "Failed to create table: " << taos_errstr(res) << std::endl;
            taos_free_result(res);
            return false;
        }
        
        taos_free_result(res);
        return true;
    }
    
    bool insertSymbolRecords(const std::string& symbol, 
                           const std::vector<const StockData*>& records) {
        if (records.empty()) return true;
        
        const auto& first = *records[0];
        if (!createTableIfNotExists(symbol, first.exchange, first.market)) {
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
        
        TAOS_RES* res = taos_query(conn_, sql.str().c_str());
        if (taos_errno(res) != 0) {
            std::cerr << "Failed to insert records: " << taos_errstr(res) << std::endl;
            taos_free_result(res);
            return false;
        }
        
        taos_free_result(res);
        return true;
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
        double price_change = std::abs(data.last_price - data.close) / data.close;
        
        if (price_change >= config_.price_change_threshold && 
            data.amount >= config_.min_amount_threshold) {
            
            VolatilityResult result;
            result.symbol = data.symbol;
            result.timestamp = data.timestamp;
            result.price = data.last_price;
            result.volume = data.volume;
            result.amount = data.amount;
            result.reason = "price_change";
            result.detect_time = getCurrentTimestamp();
            result.is_trial_period = true;
            
            return addToVolatilePool(result);
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
    
    bool addToVolatilePool(const VolatilityResult& result) {
        std::ostringstream oss;
        oss << "{\"symbol\":\"" << result.symbol 
            << "\",\"timestamp\":" << result.timestamp
            << ",\"price\":" << result.price
            << ",\"volume\":" << result.volume
            << ",\"amount\":" << result.amount
            << ",\"reason\":\"" << result.reason << "\""
            << ",\"detect_time\":" << result.detect_time
            << ",\"is_trial_period\":" << (result.is_trial_period ? "true" : "false") << "}";
        
        bool success = redis_->zadd(config_.volatile_pool_key, 
                                  result.detect_time, 
                                  oss.str());
        
        if (success) {
            redis_->expire(config_.volatile_pool_key, config_.volatile_expire);
        }
        
        return success;
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
        
        // 解析URI（简化处理）
        std::string host = "localhost";
        int port = 5672;
        std::string user = "admin";
        std::string password = "admin";
        std::string vhost = "/";
        
        if (amqp_socket_open(socket, host.c_str(), port) != AMQP_STATUS_OK) {
            amqp_destroy_connection(conn_);
            return false;
        }
        
        amqp_login(conn_, vhost.c_str(), 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, 
                  user.c_str(), password.c_str());
        
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
            
            std::vector<char> body(envelope.message.body.bytes, 
                                 envelope.message.body.bytes + envelope.message.body.len);
            
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

class MessageProcessor : public IMessageProcessor {
private:
    ZlibDecompressor decompressor_;
    ProtobufParser parser_;
    
public:
    bool processMessage(const std::vector<char>& body, 
                       std::vector<StockData>& records) override {
        // 简化处理：直接解析protobuf
        // 实际应该先解析消息头，然后根据压缩标志解压
        return parser_.parseBatch(body, records);
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
        writer_ = std::make_unique<TDengineWriter>(config);
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
        writer_->connect();
        
        std::vector<StockData> batch;
        auto last_flush = std::chrono::steady_clock::now();
        MessageProcessor processor;
        
        while (running_) {
            std::vector<char> message;
            if (!queue.pop(message)) {
                break;
            }
            
            std::vector<StockData> records;
            if (processor.processMessage(message, records)) {
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
        // 简单实现：等待Ctrl+C
        std::cout << "Press Ctrl+C to stop..." << std::endl;
        
        // 设置信号处理（简化版）
        signal(SIGINT, [](int) {});
        
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

