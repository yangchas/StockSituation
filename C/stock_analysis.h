#ifndef STOCK_ANALYSIS_H
#define STOCK_ANALYSIS_H

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
#include <fstream>
#include <unordered_map>
#include <deque>
#include <algorithm>
#include <cmath>

// 网络请求相关头文件
#include <curl/curl.h>

// 第三方库头文件
#include <sys/time.h>
#include <zlib.h>
#include <hiredis/hiredis.h>
#include <taos.h>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

// Protobuf schema
#include "schema.pb.h"

// 交易时间常量
#define MARKET_OPEN_TIME 92500    // 9:25:00
#define MARKET_CLOSE_TIME 150000  // 15:00:00
#define AUCTION_START_TIME 91500  // 9:15:00
#define AUCTION_TRIAL_END 92000   // 9:20:00
#define AUCTION_END_TIME 92500    // 9:25:00
#define AFTERNOON_OPEN_TIME 130000 // 13:00:00
#define AFTERNOON_CLOSE_TIME 150000 // 15:00:00
#define AUCTION_AFTERNOON_START 145700 // 14:57:00
#define AUCTION_AFTERNOON_END 150000   // 15:00:00

// 大单阈值常量
#define LARGE_ORDER_THRESHOLD_LOW 100000    // 10万元
#define LARGE_ORDER_THRESHOLD_MID 500000    // 50万元
#define LARGE_ORDER_THRESHOLD_HIGH 1000000  // 100万元

// 异动检测常量
#define VOLATILITY_CHANGE_THRESHOLD 0.02    // 2%
#define VOLATILITY_VOLUME_RATIO_THRESHOLD 3.0 // 3倍

// ==================== 数据模型 ====================
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
    int volatile_expire = 300;//redis过期时间
    // 时间窗口配置
    int minute1_window_ms = 60000; // 1分钟窗口
    int minute5_window_ms = 300000; // 5分钟窗口
    int max_history_ticks = 20*6; //6分钟 最大历史tick数量
    
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000;
    
    // 异动提醒阈值配置
    int volatility_threshold_ms = 10000; // 10秒内不重复提醒
    
    std::string tdengine_host = "chaos";
    int tdengine_port = 6030;
    std::string tdengine_user = "root";
    std::string tdengine_password = "taosdata";
    std::string tdengine_database = "market_data1";
    
    // 单线程处理配置
    int messages_per_batch = 1; // 每次处理1条消息
    int max_retry_count = 3; // 最大重试次数
    int retry_delay_ms = 1000; // 重试延迟
    bool verbose = true;
    // 串行处理相关配置
    int processing_delay_ms = 10; // 每条消息处理后的延迟
    bool enable_rate_limiting = true; // 启用速率限制
    int report_time = 10; //10s 报告输出间隔
    
    // 新增配置
    std::string log_file_path = "stock_analysis.log";
    int log_level = 2; // 0:DEBUG, 1:INFO, 2:WARNING, 3:ERROR
    bool enable_file_log = true;
    double large_order_threshold = 500000; // 50万元
    int volatility_tracking_minutes = 5;
};

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
    
    // 扩展字段
    double inst_vol = 0.0;
    double inst_amt = 0.0;
    double large_net = 0.0;
};

struct SimpleTickData {
    long long timestamp = 0;
    double last_price = 0;
    double volume = 0;
    double amount = 0;
    double large_net = 0.0; // 累计大单净额
    
    static SimpleTickData fromStockData(const StockData& data);
};

struct TimeWindowStats {
    double volume_1min = 0.0;
    double amount_1min = 0.0;
    double change_1min = 0.0;
    double change_5min = 0.0;
    double volume_5min = 0.0;
    double amount_5min = 0.0;
    double large_net_1min = 0.0;
    double large_net_5min = 0.0;
};

struct PendingMessage {
    std::vector<char> data;
    uint64_t delivery_tag;
    
    PendingMessage(std::vector<char>&& d, uint64_t tag);
    PendingMessage(PendingMessage&& other) noexcept;
    PendingMessage& operator=(PendingMessage&& other) noexcept;
    PendingMessage(const PendingMessage&) = delete;
    PendingMessage& operator=(const PendingMessage&) = delete;
};

struct AuctionMetrics {
    double price_change = 0.0;// 相对于前收盘价的涨跌幅
    double match_volume_ratio = 0.0;// 匹配量相对于近期均值的比值
    double net_large_order_flow = 0.0;// 大单净流入金额
    double withdrawal_impact = 0.0;// 撤单影响
    double cumulative_net_flow = 0.0;// 累计大单净流入
    double cumulative_price_change = 0.0;// 累计涨跌幅
    double bid_amount = 0.0; // 委买金额
    double ask_amount = 0.0;// 委卖金额
    bool is_limit_up = false;// 是否涨停
    bool is_limit_down = false;// 是否跌停
};

struct StockAuctionMetrics {
    std::deque<double> price_history;
    std::deque<double> bid_amount_history; // 优化: 只存bid_amount
    double auction_volume = 0.0;
    long long last_analysis_time = 0;
    double volatility_score = 0.0;
    long long added_time = 0;
    std::string volatility_level = "none";
    AuctionMetrics auction_metrics;
    SimpleTickData prev_tick_data;
    
    static const int HISTORY_SIZE = 20*15; // 优化: 减小到10
    
    StockAuctionMetrics();
};

struct MarketReport {
    std::vector<std::pair<std::string, double>> top_changes; // 涨跌幅前30
    std::vector<std::pair<std::string, double>> top_volumes; // 成交前20
    std::vector<std::pair<std::string, double>> top_net_inflows; // 净入前20
    std::vector<std::pair<std::string, double>> top_net_outflows; // 净出前20
    std::vector<std::pair<std::string, double>> withdrawal_stocks; // 撤单股 (9:20)
    std::vector<std::pair<std::string, double>> opportunities; // 机会 (9:24)
    std::vector<std::pair<std::string, double>> emotion_stocks; // 情绪票 (9:25)
    std::unordered_map<std::string, double> sector_strength; // 板块强度 (聚合mean change)
};

// ==================== 接口声明 ====================
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
    virtual bool detectVolatility(const StockData& data, double change, double bid_amount, double ask_amount) = 0;
};

class IMessageProcessor {
public:
    virtual ~IMessageProcessor() = default;
    virtual bool processMessage(const std::vector<char>& body, std::vector<StockData>& records) = 0;
};

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

class IExternalDataProvider {
public:
    virtual ~IExternalDataProvider() = default;
    virtual int getBoardCount(const std::string& symbol) = 0;
    virtual std::string getSector(const std::string& symbol) = 0;
};

// ==================== 类前向声明 ====================
class ConfigManager;
class TimeUtils;
class StockNameMapper;
class Logger;
class HttpRequest;
class IndicatorProvider;
class TDengineConnection;
class RedisClient;
class AuctionAnalyzer;
class VolatilityDetector;
class TickAnalysisEngine;
class PhaseDispatcher;
class FixedRabbitMQConsumer;
class EnhancedSingleThreadedProcessor;
class ComponentFactory;
class EnhancedStockDataFactory;
class EnhancedSingleThreadedConsumerApplication;
class DefaultExternalProvider;

#endif // STOCK_ANALYSIS_H