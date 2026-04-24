#include "stock_analysis.h"

// ==================== Redis客户端 ====================
class RedisClient {
private:
    std::string host_;
    int port_;
    int db_;
    redisContext* redis_context_ = nullptr;
    std::mutex redis_mutex_;

public:
    RedisClient(const std::string& host, int port, int db = 0)
        : host_(host), port_(port), db_(db) {
    }

    ~RedisClient() {
        disconnect();
    }

    bool connect() {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        disconnect();
        
        redis_context_ = redisConnect(host_.c_str(), port_);
        if (redis_context_ == nullptr || redis_context_->err) {
            if (redis_context_) {
                if (global_logger) {
                    global_logger->error(std::string("Redis连接错误: ") + redis_context_->errstr);
                }
                redisFree(redis_context_);
                redis_context_ = nullptr;
            } else {
                if (global_logger) {
                    global_logger->error("Redis连接错误: 无法分配内存");
                }
            }
            return false;
        }
        
        // 选择数据库
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "SELECT %d", db_);
        if (reply) {
            if (reply->type == REDIS_REPLY_ERROR) {
                if (global_logger) {
                    global_logger->error(std::string("Redis选择数据库失败: ") + reply->str);
                }
                freeReplyObject(reply);
                disconnect();
                return false;
            }
            freeReplyObject(reply);
        }
        
        return true;
    }

    void disconnect() {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (redis_context_) {
            redisFree(redis_context_);
            redis_context_ = nullptr;
        }
    }

    bool set(const std::string& key, const std::string& value, int expire_seconds = 0) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return false;
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "SET %s %s", key.c_str(), value.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                freeReplyObject(reply);
            }
            return false;
        }
        freeReplyObject(reply);
        
        if (expire_seconds > 0) {
            reply = (redisReply*)redisCommand(redis_context_, "EXPIRE %s %d", key.c_str(), expire_seconds);
            if (reply) {
                freeReplyObject(reply);
            }
        }
        
        return true;
    }

    std::string get(const std::string& key) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return "";
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "GET %s", key.c_str());
        if (!reply) {
            return "";
        }
        
        std::string result;
        if (reply->type == REDIS_REPLY_STRING) {
            result = reply->str;
        }
        freeReplyObject(reply);
        return result;
    }

    bool hset(const std::string& key, const std::string& field, const std::string& value) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return false;
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                freeReplyObject(reply);
            }
            return false;
        }
        freeReplyObject(reply);
        return true;
    }

    std::string hget(const std::string& key, const std::string& field) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return "";
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "HGET %s %s", key.c_str(), field.c_str());
        if (!reply) {
            return "";
        }
        
        std::string result;
        if (reply->type == REDIS_REPLY_STRING) {
            result = reply->str;
        }
        freeReplyObject(reply);
        return result;
    }

    bool lpush(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return false;
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "LPUSH %s %s", key.c_str(), value.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                freeReplyObject(reply);
            }
            return false;
        }
        freeReplyObject(reply);
        return true;
    }

    bool sadd(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return false;
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "SADD %s %s", key.c_str(), value.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                freeReplyObject(reply);
            }
            return false;
        }
        freeReplyObject(reply);
        return true;
    }

    bool srem(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return false;
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "SREM %s %s", key.c_str(), value.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            if (reply) {
                freeReplyObject(reply);
            }
            return false;
        }
        freeReplyObject(reply);
        return true;
    }

    bool exists(const std::string& key) {
        std::lock_guard<std::mutex> lock(redis_mutex_);
        if (!redis_context_) {
            return false;
        }
        
        redisReply* reply = (redisReply*)redisCommand(redis_context_, "EXISTS %s", key.c_str());
        if (!reply) {
            return false;
        }
        
        bool result = (reply->type == REDIS_REPLY_INTEGER && reply->integer > 0);
        freeReplyObject(reply);
        return result;
    }
};

// ==================== TDengine连接 ====================
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
        : host_(host), user_(user), password_(password), database_(database), port_(port) {
    }

    ~TDengineConnection() {
        close();
    }

    bool connect() {
        if (conn_) {
            return true;
        }
        
        conn_ = taos_connect(host_.c_str(), user_.c_str(), password_.c_str(), database_.c_str(), port_);
        if (!conn_) {
            if (global_logger) {
                global_logger->error(std::string("TDengine连接失败: ") + taos_errstr(nullptr));
            }
            return false;
        }
        
        if (global_logger) {
            global_logger->info("TDengine连接成功");
        }
        return true;
    }

    void close() {
        if (conn_) {
            taos_close(conn_);
            conn_ = nullptr;
        }
    }

    bool isConnected() const {
        return conn_ != nullptr;
    }

    void execute(const std::string& sql) {
        if (!conn_) {
            if (global_logger) {
                global_logger->error("TDengine未连接，无法执行SQL");
            }
            return;
        }
        
        TAOS_RES* res = taos_query(conn_, sql.c_str());
        if (!res) {
            if (global_logger) {
                global_logger->error(std::string("TDengine执行SQL失败: ") + taos_errstr(conn_));
            }
            return;
        }
        
        int code = taos_errno(res);
        if (code != 0) {
            if (global_logger) {
                global_logger->error(std::string("TDengine执行SQL错误: ") + taos_errstr(res) + ", SQL: " + sql);
            }
        }
        
        taos_free_result(res);
    }
};

// ==================== TDengine数据写入器 ====================
class TDengineDataWriter : public IDataWriter {
private:
    std::unique_ptr<TDengineConnection> conn_;
    
public:
    TDengineDataWriter(const Config& config) {
        conn_ = std::make_unique<TDengineConnection>(
            config.tdengine_host, config.tdengine_user,
            config.tdengine_password, config.tdengine_database, config.tdengine_port
        );
    }
    
    bool connect() override {
        return conn_->connect();
    }
    
    void close() override {
        conn_->close();
    }
    
    bool writeBatch(const std::vector<StockData>& records) override {
        if (!conn_->isConnected()) {
            if (!conn_->connect()) {
                return false;
            }
        }
        
        if (records.empty()) {
            return true;
        }
        
        try {
            std::stringstream sql;
            sql << "INSERT INTO tick_data USING tick_data_tags TAGS('" 
                << records[0].symbol << "', '" << records[0].exchange << "', '" << records[0].market << "') VALUES ";
            
            for (size_t i = 0; i < records.size(); ++i) {
                if (i > 0) {
                    sql << ",";
                }
                
                sql << "(now, " 
                    << records[i].last_price << ", " 
                    << records[i].open << ", " 
                    << records[i].high << ", " 
                    << records[i].low << ", " 
                    << records[i].close << ", " 
                    << records[i].volume << ", " 
                    << records[i].amount << ", " 
                    << records[i].inst_vol << ", " 
                    << records[i].inst_amt << ", " 
                    << records[i].large_net << ")";
            }
            
            conn_->execute(sql.str());
            return true;
        } catch (const std::exception& e) {
            if (global_logger) {
                global_logger->error(std::string("TDengine写入错误: ") + e.what());
            }
            return false;
        }
    }
};

// ==================== RabbitMQ消费者 ====================
class FixedRabbitMQConsumer : public IMessageConsumer {
private:
    const Config& config_;
    amqp_connection_state_t conn_ = nullptr;
    std::atomic<bool> running_{false};
    bool consumer_started_ = false;
    std::string consumer_tag_;
    
    void checkAmqpError(amqp_rpc_reply_t reply, const std::string& context) {
        switch (reply.reply_type) {
            case AMQP_RESPONSE_NORMAL:
                return;
            case AMQP_RESPONSE_NONE:
                if (global_logger) {
                    global_logger->error(context + ": 无响应");
                }
                break;
            case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                if (global_logger) {
                    global_logger->error(context + ": 库异常: " + amqp_error_string2(reply.library_error));
                }
                break;
            case AMQP_RESPONSE_SERVER_EXCEPTION:
                switch (reply.reply.id) {
                    case AMQP_CONNECTION_CLOSE_METHOD:
                        if (global_logger) {
                            global_logger->error(context + ": 连接关闭: " + std::to_string(reply.reply.connection_close.reply_code) + ": " + std::string((char*)reply.reply.connection_close.reply_text.bytes));
                        }
                        break;
                    case AMQP_CHANNEL_CLOSE_METHOD:
                        if (global_logger) {
                            global_logger->error(context + ": 通道关闭: " + std::to_string(reply.reply.channel_close.reply_code) + ": " + std::string((char*)reply.reply.channel_close.reply_text.bytes));
                        }
                        break;
                    default:
                        if (global_logger) {
                            global_logger->error(context + ": 服务器异常: " + std::to_string(reply.reply.id));
                        }
                        break;
                }
                break;
        }
    }
    
    bool declareQueue() {
        amqp_rpc_reply_t reply = amqp_declare_queue(
            conn_, 1, 
            amqp_cstring_bytes(config_.queue_name.c_str()),
            0, 0, 0, 1, 
            amqp_empty_table
        );
        
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            checkAmqpError(reply, "声明队列失败");
            return false;
        }
        
        amqp_bytes_free(reply.reply.declare.ok.queue);
        return true;
    }
    
    bool startConsumer() {
        amqp_basic_consume(conn_, 1, 
                         amqp_cstring_bytes(config_.queue_name.c_str()),
                         amqp_empty_bytes, 0, 1, 0, 
                         amqp_empty_table);
        
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn_);
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            checkAmqpError(reply, "启动消费者失败");
            return false;
        }
        
        consumer_started_ = true;
        return true;
    }
    
public:
    FixedRabbitMQConsumer(const Config& config) : config_(config) {
    }
    
    ~FixedRabbitMQConsumer() {
        disconnect();
    }
    
    bool connect() override {
        if (conn_) {
            return true;
        }
        
        conn_ = amqp_new_connection();
        if (!conn_) {
            if (global_logger) {
                global_logger->error("创建RabbitMQ连接失败");
            }
            return false;
        }
        
        amqp_socket_t* socket = amqp_tcp_socket_new(conn_);
        if (!socket) {
            if (global_logger) {
                global_logger->error("创建TCP套接字失败");
            }
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
            return false;
        }
        
        int status = amqp_socket_open(socket, config_.rabbitmq_host.c_str(), config_.rabbitmq_port);
        if (status != AMQP_STATUS_OK) {
            if (global_logger) {
                global_logger->error("打开TCP套接字失败: " + std::string(amqp_error_string2(status)));
            }
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
            return false;
        }
        
        amqp_rpc_reply_t reply = amqp_login(
            conn_, config_.rabbitmq_vhost.c_str(),
            0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
            config_.rabbitmq_user.c_str(), config_.rabbitmq_password.c_str()
        );
        
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            checkAmqpError(reply, "登录RabbitMQ失败");
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
            return false;
        }
        
        amqp_channel_open(conn_, 1);
        reply = amqp_get_rpc_reply(conn_);
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            checkAmqpError(reply, "打开通道失败");
            disconnect();
            return false;
        }
        
        if (!declareQueue()) {
            disconnect();
            return false;
        }
        
        if (!startConsumer()) {
            disconnect();
            return false;
        }
        
        running_ = true;
        return true;
    }
    
    void disconnect() override {
        running_ = false;
        
        if (conn_) {
            if (consumer_started_) {
                amqp_basic_cancel(conn_, 1, amqp_cstring_bytes(consumer_tag_.c_str()));
                amqp_get_rpc_reply(conn_);
                consumer_started_ = false;
            }
            
            amqp_channel_close(conn_, 1, AMQP_REPLY_SUCCESS);
            amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
        }
    }
    
    bool consumeMessages(std::vector<PendingMessage>& messages, int count) override {
        if (!conn_ || !running_) {
            return false;
        }
        
        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn_);
        
        int received_count = 0;
        while (received_count < count && running_) {
            amqp_rpc_reply_t reply = amqp_consume_message(conn_, &envelope, nullptr, 0);
            
            if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
                checkAmqpError(reply, "接收消息失败");
                break;
            }
            
            // 保存consumer_tag（第一次收到消息时）
            if (consumer_tag_.empty()) {
                consumer_tag_ = std::string((char*)envelope.consumer_tag.bytes, envelope.consumer_tag.len);
            }
            
            // 创建消息
            std::vector<char> body((char*)envelope.message.body.bytes, 
                                  (char*)envelope.message.body.bytes + envelope.message.body.len);
            messages.emplace_back(std::move(body), (uint64_t)envelope.delivery_tag);
            
            received_count++;
            amqp_destroy_envelope(&envelope);
        }
        
        return received_count > 0;
    }
    
    void ackMessage(uint64_t delivery_tag) override {
        if (conn_) {
            amqp_basic_ack(conn_, 1, delivery_tag, 0);
        }
    }
    
    void rejectMessage(uint64_t delivery_tag, bool requeue) override {
        if (conn_) {
            amqp_basic_nack(conn_, 1, delivery_tag, 0, requeue);
        }
    }
    
    void stop() override {
        running_ = false;
    }
};

// ==================== 消息处理器 ====================
class SimpleMessageProcessor : public IMessageProcessor {
public:
    bool processMessage(const std::vector<char>& body, std::vector<StockData>& records) override {
        try {
            // 这里应该解析Protobuf消息
            // 简化实现，实际需要根据schema.proto的定义来解析
            StockData data;
            // 解析body到data中
            // ...
            records.push_back(data);
            return true;
        } catch (const std::exception& e) {
            if (global_logger) {
                global_logger->error(std::string("处理消息失败: ") + e.what());
            }
            return false;
        }
    }
};