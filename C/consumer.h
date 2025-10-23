#ifndef CONSUMER_H
#define CONSUMER_H

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <functional>

// 配置结构
struct Config {
    std::string rabbitmq_uri = "amqp://admin:admin@localhost:5672/";
    std::string queue_name = "stream2";
    std::string consumer_tag = "tdengine-consumer";
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

// 消息头结构
struct MessageHeader {
    std::string proto_version;
    std::string compression;
    std::string batch_id;
    int record_count = 0;
    int original_size = 0;
    int compressed_size = 0;
    long long timestamp = 0;
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

// 异动检测结果
struct VolatilityResult {
    std::string symbol;
    long long timestamp;
    double price;
    double volume;
    double amount;
    std::string reason;
    long long detect_time;
    bool is_trial_period;
};

// 抽象接口类
class IMessageProcessor {
public:
    virtual ~IMessageProcessor() = default;
    virtual bool processMessage(const std::vector<char>& body, 
                               std::vector<StockData>& records) = 0;
};

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

#endif