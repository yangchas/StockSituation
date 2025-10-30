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
#include <queue> // 新增 for priority_queue

// 第三方库头文件
#include <sys/time.h>
#include <zlib.h>
#include <hiredis/hiredis.h>
#include <taos.h>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

// Protobuf schema (假设已定义，实际需包含生成的schema.pb.h)
#include "schema.pb.h" // 假设存在

// ==================== 配置管理 ====================
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
    // 时间窗口配置
    int minute1_window_ms = 60000; // 1分钟窗口
    int minute5_window_ms = 300000; // 5分钟窗口
    int max_history_ticks = 50; // 最大历史tick数量 (优化: 减小到50)
   
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000;
   
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
    int max_pending_messages = 50; // 最大待处理消息数
    // 串行处理相关配置
    int processing_delay_ms = 100; // 每条消息处理后的延迟
    bool enable_rate_limiting = true; // 启用速率限制
    int report_time = 10; //报告输出间隔
   
    // 新增配置
    std::string log_file_path = "stock_analysis.log";
    int log_level = 2; // 0:DEBUG, 1:INFO, 2:WARNING, 3:ERROR
    bool enable_file_log = true;
    double large_order_threshold = 500000; // 50万元
    int volatility_tracking_minutes = 5;
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
            instance_.reset(new ConfigManager());
        }
        return *instance_;
    }
    const Config& getConfig() const { return config_; }
    void updateConfig(const Config& newConfig) { config_ = newConfig; }
};
std::unique_ptr<ConfigManager> ConfigManager::instance_ = nullptr;
std::mutex ConfigManager::mutex_;

// ==================== 数据模型 ====================
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

// 简化的历史tick数据 (优化: 只保留关键字段)
struct SimpleTickData {
    long long timestamp = 0;
    double last_price = 0;
    double volume = 0;
    double amount = 0;
    double large_net = 0.0; // 累计大单净额
    
    static SimpleTickData fromStockData(const StockData& data) {
        SimpleTickData simplified;
        simplified.timestamp = data.timestamp;
        simplified.last_price = data.last_price;
        simplified.volume = data.volume;
        simplified.amount = data.amount;
        simplified.large_net = data.large_net;
        return simplified;
    }
};

// 时间窗口统计数据 (开盘专用)
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

// ==================== 竞价分析数据结构 ====================
// 竞价指标数据结构 (优化: 精简)
struct AuctionMetrics {
    double price_change = 0.0;
    double match_volume_ratio = 0.0;
    double net_large_order_flow = 0.0;
    double withdrawal_impact = 0.0;
    double cumulative_net_flow = 0.0;
    double cumulative_price_change = 0.0;
    double bid_amount = 0.0;
    double ask_amount = 0.0;
    bool is_limit_up = false;
    bool is_limit_down = false;
};

// 股票竞价指标数据结构 (优化: history_size=10)
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
   
    static const int HISTORY_SIZE = 10; // 优化: 减小到10
   
    StockAuctionMetrics() {
        price_history = std::deque<double>(HISTORY_SIZE, 0.0);
        bid_amount_history = std::deque<double>(HISTORY_SIZE, 0.0);
    }
};

// 抢筹模式数据点
struct AccumulationDataPoint {
    long long timestamp = 0;
    double price = 0.0;
    double bid_amount = 0.0;
    double ask_amount = 0.0;
    double last_close = 0.0;
};

// 撤单总结 (新: 用于9:20报告)
struct WithdrawalSummary {
    std::string symbol;
    double withdrawal_amount = 0.0;
    double change = 0.0;
    double bid_amount = 0.0;
};

// 预开盘估算 (新: 用于9:24报告)
struct PreOpenEstimate {
    std::string symbol;
    double estimated_open_price = 0.0;
    double estimated_change = 0.0;
    std::string opportunity_type; // e.g., "抢筹", "大单一字"
};

// 情绪票 (新: 预留接口)
struct EmotionStock {
    std::string symbol;
    int board_count = 0; // 连板天数 (外部接口)
    double open_change = 0.0;
};

// 市场报告 (新: 统一报告结构体)
struct MarketReport {
    std::vector<std::pair<std::string, double>> top_changes; // 涨跌幅前30
    std::vector<std::pair<std::string, double>> top_volumes; // 成交前20
    std::vector<std::pair<std::string, double>> top_net_inflows; // 净入前20
    std::vector<std::pair<std::string, double>> top_net_outflows; // 净出前20
    std::vector<WithdrawalSummary> withdrawal_stocks; // 撤单股 (9:20)
    std::vector<PreOpenEstimate> opportunities; // 机会 (9:24)
    std::vector<EmotionStock> emotion_stocks; // 情绪票 (9:25)
    std::unordered_map<std::string, double> sector_strength; // 板块强度 (聚合mean change)
};

// ==================== 抽象接口 ====================
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
    virtual bool detectVolatility(const StockData& data, double change, double bid_amount) = 0; // 优化: 传入统一计算的指标
    virtual void cleanupOldData() = 0;
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

// 新: 外部接口预留 (连板/板块)
class IExternalDataProvider {
public:
    virtual ~IExternalDataProvider() = default;
    virtual int getBoardCount(const std::string& symbol) = 0; // 连板天数
    virtual std::string getSector(const std::string& symbol) = 0; // 板块题材
};

// 默认实现 (预留，实际接入第三方)
class DefaultExternalProvider : public IExternalDataProvider {
public:
    int getBoardCount(const std::string& symbol) override { return 0; } // 默认0
    std::string getSector(const std::string& symbol) override { return ""; } // 默认空
};

// ==================== 工具类 ====================
class TimeUtils {
public:
    static long long getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
    }
   
    static std::string formatTimestamp(long long timestamp) {
        auto time = std::chrono::system_clock::from_time_t(timestamp / 1000);
        auto tt = std::chrono::system_clock::to_time_t(time);
        std::tm tm;
        localtime_r(&tt, &tm);
       
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
    static bool isAuctionTime(const std::string& time_str) {
        return time_str >= "09:15:00" && time_str <= "09:25:00";
    }
   
    static bool isTrialPeriod(const std::string& time_str) {
        return time_str >= "09:15:00" && time_str < "09:20:00";
    }
   
    static bool isTradeTime(const std::string& time_str) {
        return (time_str >= "09:30:00" && time_str <= "11:30:00") ||
               (time_str >= "13:00:00" && time_str <= "15:00:00");
    }
};

// 股票名称映射器 (扩展: 添加sector，但预留为空)
class StockNameMapper {
private:
    std::unordered_map<std::string, std::string> code_to_name_;
    std::unordered_map<std::string, std::string> code_to_sector_; // 新: 板块，预留
    std::string csv_file_path_ = "stock.csv";
    std::mutex mutex_;
    bool loaded_ = false;
    StockNameMapper(){
        loadStockNames();
    }
public:
    StockNameMapper(const StockNameMapper&) = delete;
    StockNameMapper& operator=(const StockNameMapper&) = delete;
    static StockNameMapper& getInstance() {
        static StockNameMapper instance;
        return instance;
    }
    bool loadStockNames() {
        std::ifstream file(csv_file_path_);
        if (!file.is_open()) {
            std::cerr << "无法打开股票名称文件: " << csv_file_path_ << std::endl;
            return false;
        }
        std::string line;
        std::getline(file, line); // 跳过标题
        int loaded_count = 0;
        while (std::getline(file, line)) {
            size_t comma_pos = line.find(',');
            if (comma_pos == std::string::npos) continue;
            std::string code = line.substr(0, comma_pos);
            std::string name = line.substr(comma_pos + 1);
            code.erase(0, code.find_first_not_of(" \t"));
            code.erase(code.find_last_not_of(" \t") + 1);
            name.erase(0, name.find_first_not_of(" \t"));
            name.erase(name.find_last_not_of(" \t") + 1);
            size_t dot_pos = code.find('.');
            if (dot_pos != std::string::npos) code = code.substr(0, dot_pos);
            if (code.length() == 6) {
                code_to_name_[code] = name;
                // code_to_sector_[code] = ... ; // 预留，从csv第三列
                loaded_count++;
            }
        }
        file.close();
        loaded_ = true;
        std::cout << "成功加载 " << loaded_count << " 只股票名称" << std::endl;
        return true;
    }
    std::string getStockName(const std::string& code) {
        std::string clean_code = code;
        size_t dot_pos = code.find('.');
        if (dot_pos != std::string::npos) clean_code = code.substr(0, dot_pos);
        auto it = code_to_name_.find(clean_code);
        return (it != code_to_name_.end()) ? it->second : code;
    }
    std::string getStockDisplayName(const std::string& code) {
        std::string name = getStockName(code);
        return (name != code) ? name + "(" + code + ")" : code;
    }
    std::string getSector(const std::string& code) {
        auto it = code_to_sector_.find(code);
        return (it != code_to_sector_.end()) ? it->second : "";
    }
};

// 日志系统 (优化: 添加f2s应用)
class Logger {
private:
    std::ofstream log_file_;
    int console_level_ = 2;
    int file_level_ = 1;
    bool enable_file_ = true;
    std::mutex log_mutex_;
   
public:
    Logger(const std::string& file_path = "consumer.log", int console_level = 2, int file_level = 1, bool enable_file = true)
        : console_level_(console_level), file_level_(file_level), enable_file_(enable_file) {
        if (enable_file_) {
            log_file_.open(file_path, std::ios::app);
        }
    }
   
    ~Logger() {
        if (log_file_.is_open()) {
            log_file_.close();
        }
    }
   
    void setConsoleLevel(int level) {
        console_level_ = level;
    }
   
    void setFileLevel(int level) {
        file_level_ = level;
    }
   
    static std::string f2s(double value) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << value;
        std::string str = oss.str();
        // 移除多余0
        size_t dot_pos = str.find('.');
        if (dot_pos != std::string::npos) {
            str.erase(str.find_last_not_of('0') + 1, std::string::npos);
            if (str.back() == '.') str.pop_back();
        }
        return str;
    }
   
    static std::string amountToWan(double amount) {
        return std::to_string(static_cast<int>(amount / 10000));
    }
   
    void logWithTimestamp(int level, const std::string& message, long long timestamp) {
        std::string level_str;
        switch(level) {
            case 0: level_str = "DEBUG"; break;
            case 1: level_str = "INFO"; break;
            case 2: level_str = "WARN"; break;
            case 3: level_str = "ERROR"; break;
            default: level_str = "INFO";
        }
       
        std::string log_timestamp = TimeUtils::formatTimestamp(timestamp).substr(5);
        std::string log_msg = log_timestamp + "[" + level_str + "]" + message;
       
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (level >= console_level_) {
            std::cout << log_msg << std::endl;
        }
       
        if (enable_file_ && log_file_.is_open() && level >= file_level_) {
            log_file_ << log_msg << std::endl;
            log_file_.flush();
        }
    }
   
    void infoWithTickTime(const std::string& message, long long tick_timestamp) {
        logWithTimestamp(1, message, tick_timestamp);
    }
   
    void warnWithTickTime(const std::string& message, long long tick_timestamp) {
        logWithTimestamp(2, message, tick_timestamp);
    }
   
    void errorWithTickTime(const std::string& message, long long tick_timestamp) {
        logWithTimestamp(3, message, tick_timestamp);
    }
   
    void debug(const std::string& message) {
        logWithTimestamp(0, message, TimeUtils::getCurrentTimestamp());
    }
    void info(const std::string& message) {
        logWithTimestamp(1, message, TimeUtils::getCurrentTimestamp());
    }
    void warn(const std::string& message) {
        logWithTimestamp(2, message, TimeUtils::getCurrentTimestamp());
    }
    void error(const std::string& message) {
        logWithTimestamp(3, message, TimeUtils::getCurrentTimestamp());
    }
};

// 全局日志实例
Logger* global_logger = nullptr;

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
   
    bool isConnected() const {
        return conn_ != nullptr;
    }
   
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
    TAOS_RES* query(const std::string& sql) {
        if (!conn_ && !connect()) {
            throw std::runtime_error("Not connected to TDengine");
        }
       
        TAOS_RES* res = taos_query(conn_, sql.c_str());
        if (taos_errno(res) != 0) {
            const char* err_str = taos_errstr(res);
            std::string error_msg = "SQL query failed: " + std::string(err_str);
            taos_free_result(res);
            throw std::runtime_error(error_msg);
        }
        return res;
    }
};

// StringBuffer
class StringBuffer {
private:
    thread_local static std::string buffer_;
    thread_local static std::ostringstream oss_;
   
public:
    static std::string& getString() {
        buffer_.clear();
        return buffer_;
    }
   
    static std::ostringstream& getStream() {
        oss_.str("");
        oss_.clear();
        return oss_;
    }
   
    static std::string getStringFromStream() {
        return oss_.str();
    }
};
thread_local std::string StringBuffer::buffer_;
thread_local std::ostringstream StringBuffer::oss_;

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
        if (connection_->isConnected()) return true;
       
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
                "bv1 BIGINT, bv2 BIGINT, bv3 BIGINT, bv4 BIGINT, bv5 BIGINT, "
                "inst_vol FLOAT, inst_amt FLOAT, large_net FLOAT"
                ") TAGS (symbol BINARY(20))";
           
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
        if (records.empty()) return true;
       
        if (!connection_->isConnected() && !connect()) return false;
       
        try {
            std::ostringstream sql;
            sql << "INSERT INTO ";
            for (const auto& record : records) {
                std::string tbname = "t_s_" + record.symbol;
                sql << tbname << " USING stock_data TAGS ('" << record.symbol << "') "
                    << "VALUES (" << record.timestamp << ", " << record.last_price << ", " << record.open << ", " << record.high << ", " << record.low << ", " << record.close << ", " << record.amount << ", " << record.volume << ", 0, "
                    << record.ask_prices[0] << ", " << record.ask_prices[1] << ", " << record.ask_prices[2] << ", " << record.ask_prices[3] << ", " << record.ask_prices[4] << ", "
                    << record.bid_prices[0] << ", " << record.bid_prices[1] << ", " << record.bid_prices[2] << ", " << record.bid_prices[3] << ", " << record.bid_prices[4] << ", "
                    << record.ask_volumes[0] << ", " << record.ask_volumes[1] << ", " << record.ask_volumes[2] << ", " << record.ask_volumes[3] << ", " << record.ask_volumes[4] << ", "
                    << record.bid_volumes[0] << ", " << record.bid_volumes[1] << ", " << record.bid_volumes[2] << ", " << record.bid_volumes[3] << ", " << record.bid_volumes[4] << ", "
                    << record.inst_vol << ", " << record.inst_amt << ", " << record.large_net << ") ";
            }
            connection_->execute(sql.str());
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert batch: " << e.what() << std::endl;
            return false;
        }
    }
};

// ==================== Tick分析引擎 ====================
class TickAnalysisEngine {
private:
    struct StockTickState {
        SimpleTickData prev_tick;
        double cumulative_large_net = 0.0;
        bool has_previous = false;
        long long last_update = 0;
    };
   
    std::unordered_map<std::string, StockTickState> stock_states_;
    std::mutex state_mutex_;
    double large_order_threshold_;
public:
    TickAnalysisEngine(const Config& config, double threshold = 500000)
        : large_order_threshold_(threshold) {}
   
    void processTickData(StockData& current_tick, bool is_auction_period) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = stock_states_[current_tick.symbol];
       
        if (state.has_previous) {
            current_tick.inst_vol = current_tick.volume - state.prev_tick.volume;
            current_tick.inst_amt = current_tick.amount - state.prev_tick.amount;
            // 大单计算分阶段
            if (is_auction_period) {
                calculateAuctionLargeOrder(current_tick, state);
            } else {
                calculateTradeLargeOrder(current_tick, state);
            }
        } else {
            current_tick.inst_vol = 0;
            current_tick.inst_amt = 0;
            current_tick.large_net = 0;
        }
       
        state.prev_tick = SimpleTickData::fromStockData(current_tick);
        state.has_previous = true;
        state.last_update = current_tick.timestamp;
    }
   
    double getCumulativeLargeNet(const std::string& symbol) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = stock_states_.find(symbol);
        if (it != stock_states_.end()) {
            return it->second.cumulative_large_net;
        }
        return 0.0;
    }
   
    void cleanupOldData(long long current_time) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        long long cutoff_time = current_time - 1800000; // 30min
        auto it = stock_states_.begin();
        while (it != stock_states_.end()) {
            if (it->second.last_update < cutoff_time) {
                it = stock_states_.erase(it);
            } else {
                ++it;
            }
        }
    }
private:
    void calculateAuctionLargeOrder(StockData& current_tick, StockTickState& state) {
        double delta_bid = current_tick.bid_volumes[0] + current_tick.bid_volumes[1] - state.prev_tick.volume; // 简例，实际调整
        double instant_amount = delta_bid * current_tick.last_price * 100;
        if (std::abs(instant_amount) > large_order_threshold_) {
            current_tick.large_net = (delta_bid > 0) ? instant_amount : -instant_amount;
            state.cumulative_large_net += current_tick.large_net;
        } else {
            current_tick.large_net = 0;
        }
    }
   
    void calculateTradeLargeOrder(StockData& current_tick, StockTickState& state) {
        double instant_amount = current_tick.inst_amt;
        if (std::abs(instant_amount) > large_order_threshold_) {
            current_tick.large_net = (current_tick.last_price > state.prev_tick.last_price) ? instant_amount : -instant_amount;
            state.cumulative_large_net += current_tick.large_net;
        } else {
            current_tick.large_net = 0;
        }
    }
};

// ==================== 消息处理器 ====================
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
                const std::string& compressed_data = data_request.compressed_data();
                if (!decompressZlib(compressed_data, decompressed)) {
                    return false;
                }
                batch_bytes = std::move(decompressed);
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
        if (body.size() < 4) return false;
       
        uint32_t header_len;
        std::memcpy(&header_len, body.data(), 4);
        header_len = ntohl(header_len);
       
        if (body.size() < 4 + header_len) return false;
       
        std::string header_json(body.data() + 4, header_len);
        if (!parseJsonHeader(header_json, header)) return false;
       
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
        try {
            return std::stoi(num_str);
        } catch (const std::exception&) {
            return 0;
        }
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
        try {
            return std::stoll(num_str);
        } catch (const std::exception&) {
            return 0;
        }
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
        
        z_stream strm = {};
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
        
        records.clear();
        records.reserve(record_count);  // 预分配内存
        
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
        if (context_) return true;
       
        context_ = redisConnect(host_.c_str(), port_);
        if (!context_ || context_->err) {
            if (context_) redisFree(context_);
            context_ = nullptr;
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
   
    bool isConnected() const {
        return context_ != nullptr;
    }
   
    bool zadd(const std::string& key, double score, const std::string& member) {
        if (!context_ && !connect()) return false;
       
        redisReply* reply = (redisReply*)redisCommand(context_,
                                                    "ZADD %s %f %s",
                                                    key.c_str(), score, member.c_str());
        if (!reply) {
            disconnect();
            return false;
        }
       
        bool success = (reply->type != REDIS_REPLY_ERROR);
        freeReplyObject(reply);
        return success;
    }
   
    bool expire(const std::string& key, int seconds) {
        if (!context_ && !connect()) return false;
       
        redisReply* reply = (redisReply*)redisCommand(context_,
                                                    "EXPIRE %s %d",
                                                    key.c_str(), seconds);
        if (!reply) {
            disconnect();
            return false;
        }
       
        bool success = (reply->type != REDIS_REPLY_ERROR);
        freeReplyObject(reply);
        return success;
    }
   
    bool zremrangebyscore(const std::string& key, double min, double max) {
        if (!context_ && !connect()) return false;
       
        redisReply* reply = (redisReply*)redisCommand(context_,
                                                    "ZREMRANGEBYSCORE %s %f %f",
                                                    key.c_str(), min, max);
        if (!reply) {
            disconnect();
            return false;
        }
       
        bool success = (reply->type != REDIS_REPLY_ERROR);
        freeReplyObject(reply);
        return success;
    }
};

// ==================== 竞价分析器 ====================
class AuctionAnalyzer {
private:
    struct AuctionThresholds {
        double price_change = 0.02;
        double match_volume_ratio = 1.5;
        double net_large_order_flow = 500000;
        double min_volatility_score = 15;
        double cumulative_net_flow = 1000000;
        double cumulative_price_change = 0.05;
        double bid_amount_large = 5000000;
        double accumulation_bid_increase = 1000000;
        double accumulation_price_increase = 0.01;
    };
   
    AuctionThresholds thresholds_;
   
    std::unordered_map<std::string, StockAuctionMetrics> stock_auction_metrics_;
    std::unordered_map<std::string, std::vector<AccumulationDataPoint>> post_20_data_;
   
    MarketReport market_report_; // 新: 持续更新
    StockNameMapper& stock_mapper_;
    IExternalDataProvider* external_provider_; // 新: 预留外部
    std::mutex data_mutex_;
   
    // 关键时间
    const std::string TRIAL_END = "09:20:00";
    const std::string AUCTION_NEAR_END = "09:24:00";
    const std::string AUCTION_END = "09:25:00";
   
public:
    AuctionAnalyzer(IExternalDataProvider* provider = new DefaultExternalProvider())
        : stock_mapper_(StockNameMapper::getInstance()), external_provider_(provider) {}
   
    ~AuctionAnalyzer() { delete external_provider_; }
   
    bool isAuctionPeriod(long long timestamp) {
        std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11, 8);
        return time_str >= "09:15:00" && time_str <= "09:25:00";
    }
   
    bool isTrialPeriod(long long timestamp) {
        std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11, 8);
        return time_str >= "09:15:00" && time_str < "09:20:00";
    }
   
    void processTickData(const StockData& data, double change, double bid_amount) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        const std::string& symbol = data.symbol;
       
        StockAuctionMetrics& metrics = stock_auction_metrics_[symbol];
       
        // 更新历史 (精简)
        updateHistoryData(metrics, data.last_price, bid_amount);
       
        // 更新指标 (复用change)
        updateAuctionMetrics(metrics, data, change, bid_amount);
       
        std::string current_time = TimeUtils::formatTimestamp(data.timestamp).substr(11, 8);
        if (current_time >= "09:20:00") {
            analyzeAccumulationPattern(symbol, data.timestamp, data.last_price, bid_amount, data.close);
        }
       
        analyzeOrderFlow(symbol, data, metrics, data.timestamp, change, bid_amount);
       
        if (data.timestamp - metrics.last_analysis_time > 1000) {
            analyzeAuctionVolatility(symbol, metrics, data.timestamp);
            metrics.last_analysis_time = data.timestamp;
        }
       
        // 持续更新报告
        updateMarketReport(symbol, metrics);
       
        checkKeyTimepoints(data.timestamp);
    }
   
    void cleanupOldData() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long cutoff_time = current_time - 1800000; // 30min
        auto it = stock_auction_metrics_.begin();
        while (it != stock_auction_metrics_.end()) {
            if (it->second.last_analysis_time < cutoff_time) {
                it = stock_auction_metrics_.erase(it);
            } else {
                ++it;
            }
        }
    }
private:
    void updateHistoryData(StockAuctionMetrics& metrics, double price, double bid_amount) {
        metrics.price_history.push_back(price);
        metrics.bid_amount_history.push_back(bid_amount);
        if (metrics.price_history.size() > StockAuctionMetrics::HISTORY_SIZE) {
            metrics.price_history.pop_front();
            metrics.bid_amount_history.pop_front();
        }
    }
   
    void updateAuctionMetrics(StockAuctionMetrics& metrics, const StockData& data, double change, double bid_amount) {
        metrics.auction_metrics.price_change = change;
        metrics.auction_metrics.bid_amount = bid_amount;
        metrics.auction_metrics.ask_amount = (data.ask_volumes[0] + data.ask_volumes[1]) * data.last_price * 100;
        double limit_up_price = std::round(data.close * 1.1 * 100) / 100.0;
        double limit_down_price = std::round(data.close * 0.9 * 100) / 100.0;
        std::string symbol_prefix = data.symbol.substr(0, 2);
        if (symbol_prefix == "30" || symbol_prefix == "68") {
            limit_up_price = std::round(data.close * 1.2 * 100) / 100.0;
            limit_down_price = std::round(data.close * 0.8 * 100) / 100.0;
        }
        metrics.auction_metrics.is_limit_up = std::abs(data.last_price - limit_up_price) < 0.01;
        metrics.auction_metrics.is_limit_down = std::abs(data.last_price - limit_down_price) < 0.01;
        if (metrics.price_history.size() > 1) {
            metrics.auction_metrics.cumulative_price_change = (data.last_price - metrics.price_history.front()) / metrics.price_history.front();
        }
    }
   
    void analyzeAccumulationPattern(const std::string& symbol, long long timestamp, double price, double bid_amount, double last_close) {
        AccumulationDataPoint data_point;
        data_point.timestamp = timestamp;
        data_point.price = price;
        data_point.bid_amount = bid_amount;
        data_point.last_close = last_close;
       
        post_20_data_[symbol].push_back(data_point);
       
        if (post_20_data_[symbol].size() > 10) {
            post_20_data_[symbol].erase(post_20_data_[symbol].begin());
        }
       
        if (post_20_data_[symbol].size() >= 3) {
            auto& data_points = post_20_data_[symbol];
            bool bid_increasing = true;
            bool price_increasing = true;
            for (size_t i = 0; i < data_points.size() - 1; ++i) {
                if (data_points[i].bid_amount >= data_points[i + 1].bid_amount) bid_increasing = false;
                if (data_points[i].price >= data_points[i + 1].price) price_increasing = false;
            }
            double price_increase = (data_points.back().price - data_points.front().price) / data_points.front().price;
            if (bid_increasing && (price_increasing || data_points.back().price >= last_close * 1.097) &&
                (data_points.back().bid_amount - data_points.front().bid_amount) >= thresholds_.accumulation_bid_increase &&
                price_increase >= thresholds_.accumulation_price_increase) {
                if (global_logger) {
                    global_logger->warnWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|抢筹|" + stock_mapper_.getStockDisplayName(symbol), timestamp);
                }
            }
        }
    }
   
    void analyzeOrderFlow(const std::string& symbol, const StockData& data, StockAuctionMetrics& metrics, long long timestamp, double change, double bid_amount) {
        SimpleTickData& prev_data = metrics.prev_tick_data;
       
        if (prev_data.timestamp == 0) {
            prev_data = SimpleTickData::fromStockData(data);
            return;
        }
       
        bool is_trial_period = isTrialPeriod(timestamp);
       
        if (is_trial_period) {
            // 撤单分析
            if (data.ask_prices[0] == prev_data.last_price) {
                double delta_av1 = data.ask_volumes[0] - (prev_data.volume / data.ask_prices[0] / 100); // 简例
                if (delta_av1 < 0) {
                    double withdrawal_value = std::abs(delta_av1) * data.ask_prices[0] * 100;
                    metrics.auction_metrics.withdrawal_impact += withdrawal_value;
                    if (withdrawal_value > 500000) {
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|撤单|" + stock_mapper_.getStockDisplayName(symbol) + " " + Logger::amountToWan(withdrawal_value) + "万", timestamp);
                        }
                    }
                }
            }
        } else {
            // 大单分析
            double delta_bv1 = data.bid_volumes[0] + data.bid_volumes[1] - prev_data.volume / data.last_price / 100; // 简例
            if (delta_bv1 > 0 && delta_bv1 * data.last_price * 100 > thresholds_.net_large_order_flow) {
                metrics.auction_metrics.net_large_order_flow += delta_bv1 * data.last_price * 100;
            }
        }
       
        prev_data = SimpleTickData::fromStockData(data);
    }
   
    void analyzeAuctionVolatility(const std::string& symbol, StockAuctionMetrics& metrics, long long timestamp) {
        double volatility_score = 0.0;
        if (std::abs(metrics.auction_metrics.cumulative_price_change) >= thresholds_.cumulative_price_change) {
            volatility_score += std::abs(metrics.auction_metrics.cumulative_price_change) * 300;
        }
        // ... 其他计算 (参考原始)
        metrics.volatility_score = volatility_score;
        if (volatility_score > thresholds_.min_volatility_score) {
            if (global_logger) {
                global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|异动|" + stock_mapper_.getStockDisplayName(symbol), timestamp);
            }
        }
    }
   
    void updateMarketReport(const std::string& symbol, const StockAuctionMetrics& metrics) {
        // 用vector + sort维护top
        market_report_.top_changes.push_back({symbol, metrics.auction_metrics.price_change});
        std::sort(market_report_.top_changes.begin(), market_report_.top_changes.end(), [](const auto& a, const auto& b) {
            return std::abs(a.second) > std::abs(b.second);
        });
        if (market_report_.top_changes.size() > 30) market_report_.top_changes.resize(30);
        // 类似其他list
        // 情绪
        EmotionStock es;
        es.symbol = symbol;
        es.board_count = external_provider_->getBoardCount(symbol);
        es.open_change = metrics.auction_metrics.price_change;
        market_report_.emotion_stocks.push_back(es);
        // 板块
        std::string sector = external_provider_->getSector(symbol);
        if (!sector.empty()) {
            market_report_.sector_strength[sector] = (market_report_.sector_strength[sector] + metrics.auction_metrics.price_change) / 2.0; // 均值简例
        }
    }
   
    void checkKeyTimepoints(long long timestamp) {
        std::string current_time = TimeUtils::formatTimestamp(timestamp).substr(11, 8);
        if (current_time > "09:20:00" && current_time <= "09:20:09") {
            generateReport("9:20", timestamp); 
        } else if (current_time >= "09:24:00" && current_time <= "09:24:09") {
            simulateOpen();
            generateReport("9:24", timestamp);
        } else if (current_time >= "09:25:00" && current_time <= "09:25:09") {
            generateReport("9:25", timestamp);
        }
    }
   
    void generateReport(const std::string& time_key, long long timestamp) {
        std::ostringstream report;
        report << "====== 竞价报告 " << time_key << " ======\n";
        if (time_key == "9:20") {
            report << "| Symbol | Name | Withdrawal万 | Change% | Bid万 |\n";
            for (const auto& ws : market_report_.withdrawal_stocks) {
                report << "| " << ws.symbol << " | " << stock_mapper_.getStockDisplayName(ws.symbol) << " | " << Logger::amountToWan(ws.withdrawal_amount) << " | " << Logger::f2s(ws.change * 100) << " | " << Logger::amountToWan(ws.bid_amount) << " |\n";
            }
        } else if (time_key == "9:24") {
            report << "| Symbol | EstPrice | EstChange% | Type |\n";
            for (const auto& po : market_report_.opportunities) {
                report << "| " << po.symbol << " | " << Logger::f2s(po.estimated_open_price) << " | " << Logger::f2s(po.estimated_change * 100) << " | " << po.opportunity_type << " |\n";
            }
            for (const auto& ss : market_report_.sector_strength) {
                if (ss.second > 0.03) report << "板块机会: " << ss.first << " 强度: " << Logger::f2s(ss.second * 100) << "%\n";
            }
        } else if (time_key == "9:25") {
            report << "涨跌幅前30:\n| Symbol | Name | Change% | Bid万 | NetFlow万 | Volume万 | Board |\n";
            for (const auto& tc : market_report_.top_changes) {
                report << "| " << tc.first << " | " << stock_mapper_.getStockDisplayName(tc.first) << " | " << Logger::f2s(tc.second * 100) << " | ... " << " | " << "0" << " | " << "0" << " | " << "0" << " |\n"; // 简例，多字段
            }
            // 类似其他
            report << "情绪票:\n";
            for (const auto& es : market_report_.emotion_stocks) {
                report << es.symbol << " Board: " << es.board_count << " Change: " << Logger::f2s(es.open_change * 100) << "%\n";
            }
        }
        global_logger->warnWithTickTime(report.str(), timestamp);
    }
    
    void simulateOpen() {
        // 预估逻辑
        for (const auto& pair : stock_auction_metrics_) {
            PreOpenEstimate po;
            po.symbol = pair.first;
            po.estimated_open_price = pair.second.price_history.back(); // 简例
            po.estimated_change = pair.second.auction_metrics.price_change;
            po.opportunity_type = "抢筹"; // 简例
            market_report_.opportunities.push_back(po);
        }
    }
};

// ==================== 开盘异动检测器 ====================
class VolatilityDetector : public IVolatilityDetector {
private:
    const Config& config_;
    std::unordered_map<std::string, std::deque<SimpleTickData>> stock_history_; // 优化: 限50
    StockNameMapper& stock_mapper_;
    IExternalDataProvider* external_provider_;
    std::mutex data_mutex_;
public:
    VolatilityDetector(const Config& config, IExternalDataProvider* provider = new DefaultExternalProvider())
        : config_(config), stock_mapper_(StockNameMapper::getInstance()), external_provider_(provider) {}
   
    ~VolatilityDetector() { delete external_provider_; }
   
    bool detectVolatility(const StockData& data, double change, double bid_amount) override {
        updateHistory(data);
        TimeWindowStats stats = calculateTimeWindowStats(data);
        bool is_volatile = false;
        std::string reason;
        double strength = 0.0;
        
        // 涨跌停
        double limit = checkLimit(data, bid_amount);
        if (limit > 0 && data.inst_amt > 3000000) {
            is_volatile = true;
            reason = (change > 0) ? "Top|封单:" + Logger::amountToWan(limit) : "Low|封单:" + Logger::amountToWan(limit);
            strength = 10.0;
        }
        
        // 其他异动
        if (data.inst_amt > 1000000 && stats.amount_5min > 10000000 && std::abs(stats.change_1min) > 0.01) {
            is_volatile = true;
            reason = "Amount";
            strength = std::abs(stats.change_1min * 100);
        }
        
        if (is_volatile) {
            logVolatility(data, reason, strength, stats, Logger::f2s(stats.change_1min * 100));
            storeToRedis(data, reason, strength, stats);
            // 板块聚合
            std::string sector = external_provider_->getSector(data.symbol);
            if (!sector.empty()) {
                // 聚合逻辑
            }
        }
        return is_volatile;
    }
   
    void cleanupOldData() override {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long cutoff_time = current_time - 1800000; // 30min
        auto it = stock_history_.begin();
        while (it != stock_history_.end()) {
            if (it->second.back().timestamp < cutoff_time) {
                it = stock_history_.erase(it);
            } else {
                ++it;
            }
        }
    }
private:
    void updateHistory(const StockData& data) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        auto& history = stock_history_[data.symbol];
        history.push_back(SimpleTickData::fromStockData(data));
        if (history.size() > config_.max_history_ticks) history.pop_front();
    }
   
    TimeWindowStats calculateTimeWindowStats(const StockData& data) {
        TimeWindowStats stats;
        std::lock_guard<std::mutex> lock(data_mutex_);
        auto it = stock_history_.find(data.symbol);
        if (it == stock_history_.end()) return stats;
       
        const auto& history = it->second;
        long long current_time = data.timestamp;
        long long time_1min_ago = current_time - config_.minute1_window_ms;
        long long time_5min_ago = current_time - config_.minute5_window_ms;
       
        auto find_tick = [&](long long target) -> const SimpleTickData* {
            for (auto r_it = history.rbegin(); r_it != history.rend(); ++r_it) {
                if (r_it->timestamp <= target) return &(*r_it);
            }
            return nullptr;
        };
       
        const SimpleTickData* tick_1min = find_tick(time_1min_ago);
        if (tick_1min) {
            stats.change_1min = (data.last_price - tick_1min->last_price) / tick_1min->last_price;
            stats.amount_1min = data.amount - tick_1min->amount;
            stats.large_net_1min = data.large_net - tick_1min->large_net;
        }
       
        const SimpleTickData* tick_5min = find_tick(time_5min_ago);
        if (tick_5min) {
            stats.change_5min = (data.last_price - tick_5min->last_price) / tick_5min->last_price;
            stats.amount_5min = data.amount - tick_5min->amount;
            stats.large_net_5min = data.large_net - tick_5min->large_net;
        }
       
        return stats;
    }
    
    double checkLimit(const StockData& data, double bid_amount) {
        double limit_up_price = std::round(data.close * 1.1 * 100) / 100.0;
        double limit_down_price = std::round(data.close * 0.9 * 100) / 100.0;
        std::string symbol_prefix = data.symbol.substr(0, 2);
        if (symbol_prefix == "30" || symbol_prefix == "68") {
            limit_up_price = std::round(data.close * 1.2 * 100) / 100.0;
            limit_down_price = std::round(data.close * 0.8 * 100) / 100.0;
        }
        if (std::abs(data.last_price - limit_up_price) < 0.01) {
            return bid_amount;
        } else if (std::abs(data.last_price - limit_down_price) < 0.01) {
            return -bid_amount; // 简例
        }
        return 0.0;
    }
    
    void logVolatility(const StockData& data, std::string& reason, double strength, const TimeWindowStats& stats, const std::string& change_1min) {
        std::ostringstream log_msg;
        log_msg << "异动|" << reason << "|价格:" << Logger::f2s(data.last_price) << "|涨幅:" << Logger::f2s((data.last_price - data.close) / data.close * 100) << "%"
                << "|瞬时:" << Logger::amountToWan(data.inst_amt) << "万"
                << "|1分速:" << change_1min << "%"
                << "|1分净额:" << Logger::amountToWan(stats.amount_1min) << "万"
                << "|5分净额:" << Logger::amountToWan(stats.large_net_5min) << "万"
                << "|5分金额:" << Logger::amountToWan(stats.amount_5min) << "万"
                << "|强度:" << strength;
       
        global_logger->warnWithTickTime(log_msg.str(), data.timestamp);
    }
    
    void storeToRedis(const StockData& data, const std::string& reason, double strength, const TimeWindowStats& stats) {
        std::ostringstream json;
        json << "{\"symbol\":\"" << data.symbol << "\",\"timestamp\":" << data.timestamp << ",\"price\":" << Logger::f2s(data.last_price) << ","
             << "\"reason\":\"" << reason << "\",\"strength\":" << strength << ","
             << "\"large_net_5min\":" << Logger::f2s(stats.large_net_5min) << ",\"change_5min\":" << Logger::f2s(stats.change_5min) << ",\"amount_5min\":" << Logger::f2s(stats.amount_5min) << "}";
        redis_->zadd(config_.volatile_pool_key, data.timestamp, json.str());
        redis_->expire(config_.volatile_pool_key, config_.volatile_expire);
    }
};

// ==================== 阶段分流器 (新) ====================
class PhaseDispatcher {
private:
    TickAnalysisEngine* tick_engine_;
    AuctionAnalyzer* auction_analyzer_;
    IVolatilityDetector* volatility_detector_;
public:
    PhaseDispatcher(TickAnalysisEngine* te, AuctionAnalyzer* aa, IVolatilityDetector* vd)
        : tick_engine_(te), auction_analyzer_(aa), volatility_detector_(vd) {}
   
    void dispatch(StockData& data) {
        // 统一计算指标
        double change = (data.close > 0) ? std::round((data.last_price - data.close) / data.close * 10000) / 10000.0 : 0.0;
        double bid_amount = (data.bid_volumes[0] + data.bid_volumes[1]) * data.last_price * 100;
        double ask_amount = (data.ask_volumes[0] + data.ask_volumes[1]) * data.last_price * 100;
        
        std::string time_str = TimeUtils::formatTimestamp(data.timestamp).substr(11, 8);
        bool is_auction = TimeUtils::isAuctionTime(time_str);
        
        tick_engine_->processTickData(data, is_auction);
        
        if (is_auction) {
            auction_analyzer_->processTickData(data, change, bid_amount);
        } else if (TimeUtils::isTradeTime(time_str)) {
            volatility_detector_->detectVolatility(data, change, bid_amount);
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
        if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
            throw std::runtime_error(context + " failed");
        }
    }
   
    bool declareQueue() {
        amqp_queue_declare(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                          0, 1, 0, 0, amqp_empty_table);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn_);
        checkAmqpError(reply, "Declare queue");
        return true;
    }
   
    bool startConsumer() {
        amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                          amqp_empty_bytes, 0, 0, 1, amqp_empty_table);
        amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn_);
        checkAmqpError(reply, "Start consumer");
        consumer_started_ = true;
        return true;
    }
   
public:
    FixedRabbitMQConsumer(const Config& config) : config_(config) {}
   
    ~FixedRabbitMQConsumer() {
        disconnect();
    }
   
    bool connect() override {
        conn_ = amqp_new_connection();
        amqp_socket_t* socket = amqp_tcp_socket_new(conn_);
        int status = amqp_socket_open(socket, config_.rabbitmq_host.c_str(), config_.rabbitmq_port);
        if (status != AMQP_STATUS_OK) return false;
        amqp_rpc_reply_t login_reply = amqp_login(conn_, config_.rabbitmq_vhost.c_str(),
                                                0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                                                config_.rabbitmq_user.c_str(), config_.rabbitmq_password.c_str());
        checkAmqpError(login_reply, "Login");
        amqp_channel_open(conn_, 1);
        amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn_);
        checkAmqpError(channel_reply, "Open channel");
        declareQueue();
        startConsumer();
        return true;
    }
   
    void disconnect() override {
        if (conn_) {
            amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(conn_);
            conn_ = nullptr;
        }
    }
   
    bool consumeMessages(std::vector<PendingMessage>& messages, int count) override {
        messages.clear();
        amqp_envelope_t envelope;
        struct timeval timeout = {1, 0};
        amqp_rpc_reply_t ret = amqp_consume_message(conn_, &envelope, &timeout, 0);
        if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
            std::vector<char> body(envelope.message.body.bytes, (char*)envelope.message.body.bytes + envelope.message.body.len);
            messages.emplace_back(std::move(body), envelope.delivery_tag);
            amqp_destroy_envelope(&envelope);
            return true;
        }
        return false;
    }
   
    void ackMessage(uint64_t delivery_tag) override {
        amqp_basic_ack(conn_, 1, delivery_tag, 0);
    }
   
    void rejectMessage(uint64_t delivery_tag, bool requeue) override {
        amqp_basic_reject(conn_, 1, delivery_tag, requeue);
    }
   
    void stop() override {
        running_ = false;
    }
};

// ==================== 单线程处理器 ====================
class EnhancedSingleThreadedProcessor {
private:
    const Config& config_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IMessageProcessor> message_processor_;
    std::unique_ptr<IMessageConsumer> consumer_;
    std::atomic<bool> running_{false};
   
    std::unique_ptr<TickAnalysisEngine> tick_engine_;
    std::unique_ptr<AuctionAnalyzer> auction_analyzer_;
    std::unique_ptr<IVolatilityDetector> volatility_detector_;
    std::unique_ptr<PhaseDispatcher> phase_dispatcher_;
    std::unique_ptr<Logger> logger_;
    std::unique_ptr<RedisClient> redis_;
    StockNameMapper& stock_mapper_;
   
    std::atomic<long long> max_server_timestamp_{0};
    std::atomic<long long> total_delay_{0};
    std::atomic<int> delay_count_{0};
   
public:
    EnhancedSingleThreadedProcessor(std::unique_ptr<IDataWriter> writer,
                                  std::unique_ptr<IMessageProcessor> processor,
                                  std::unique_ptr<IMessageConsumer> consumer,
                                  const Config& config)
        : config_(config),
          writer_(std::move(writer)),
          message_processor_(std::move(processor)),
          consumer_(std::move(consumer)),
          stock_mapper_(StockNameMapper::getInstance()) {
       
        tick_engine_ = std::make_unique<TickAnalysisEngine>(config_);
        auction_analyzer_ = std::make_unique<AuctionAnalyzer>();
        volatility_detector_ = std::make_unique<VolatilityDetector>(config_);
        phase_dispatcher_ = std::make_unique<PhaseDispatcher>(tick_engine_.get(), auction_analyzer_.get(), volatility_detector_.get());
        logger_ = std::make_unique<Logger>(config.log_file_path, config.log_level, config.enable_file_log);
        redis_ = std::make_unique<RedisClient>(config.redis_host, config.redis_port, config.redis_db);
        global_logger = logger_.get();
    }
   
    ~EnhancedSingleThreadedProcessor() {
        stop();
    }
   
    void start() {
        running_ = true;
       
        if (!writer_->connect()) {
            logger_->error("Failed to connect to TDengine");
            return;
        }
       
        logger_->info("Starting enhanced single-threaded processor...");
       
        int total_messages = 0;
        int total_records = 0;
        int failed_messages = 0;
        int empty_cycles = 0;
       
        auto last_cleanup = std::chrono::steady_clock::now();
        auto last_report = std::chrono::steady_clock::now();
       
        while (running_) {
            if (config_.enable_rate_limiting) {
                std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing_delay_ms));
            }
           
            std::vector<PendingMessage> messages;
            bool has_messages = consumer_->consumeMessages(messages, config_.messages_per_batch);
           
            if (!has_messages) {
                empty_cycles++;
                std::this_thread::sleep_for(std::chrono::seconds(10));
                continue;
            }
           
            empty_cycles = 0;
           
            std::vector<StockData> all_records;
            std::vector<PendingMessage> valid_messages;
           
            for (auto& message : messages) {
                std::vector<StockData> records;
                if (message_processor_->processMessage(message.data, records)) {
                    for (auto& record : records) {
                        phase_dispatcher_->dispatch(record);
                    }
                    all_records.insert(all_records.end(), std::make_move_iterator(records.begin()), std::make_move_iterator(records.end()));
                    valid_messages.push_back(std::move(message));
                } else {
                    failed_messages++;
                    consumer_->rejectMessage(message.delivery_tag, true);
                }
            }
           
            if (!all_records.empty()) {
                bool write_success = false;
                int retry_count = 0;
               
                while (retry_count < config_.max_retry_count && !write_success) {
                    write_success = writer_->writeBatch(all_records);
                    if (!write_success) {
                        retry_count++;
                        std::this_thread::sleep_for(std::chrono::milliseconds(config_.retry_delay_ms));
                    }
                }
               
                if (write_success) {
                    for (auto& message : valid_messages) {
                        consumer_->ackMessage(message.delivery_tag);
                    }
                    total_messages += valid_messages.size();
                    total_records += all_records.size();
                } else {
                    for (auto& message : valid_messages) {
                        consumer_->rejectMessage(message.delivery_tag, true);
                    }
                    failed_messages += valid_messages.size();
                }
            }
           
            auto now = std::chrono::steady_clock::now();
           
            if (std::chrono::duration_cast<std::chrono::minutes>(now - last_cleanup).count() >= 5) {
                cleanupOldData();
                last_cleanup = now;
            }
           
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= config_.report_time) {
                logger_->warn("处理: " + std::to_string(total_messages) + " 条消息, " +
                             std::to_string(total_records) + " 条记录, " +
                             std::to_string(failed_messages) + " 条失败");
                last_report = now;
                total_messages = 0;
                total_records = 0;
                failed_messages = 0;
            }
        }
       
        logger_->info("Processor stopped");
       
        writer_->close();
        consumer_->disconnect();
    }
   
    void stop() {
        running_ = false;
        consumer_->stop();
    }
   
private:
    void cleanupOldData() {
        tick_engine_->cleanupOldData(TimeUtils::getCurrentTimestamp());
        auction_analyzer_->cleanupOldData();
        volatility_detector_->cleanupOldData();
    }
};

// ==================== 工厂类 ====================
class ComponentFactory {
public:
    virtual ~ComponentFactory() = default;
    virtual std::unique_ptr<IDataWriter> createDataWriter(const Config& config) = 0;
    virtual std::unique_ptr<IMessageProcessor> createMessageProcessor(const Config& config) = 0;
    virtual std::unique_ptr<IMessageConsumer> createMessageConsumer(const Config& config) = 0;
};

class EnhancedStockDataFactory : public ComponentFactory {
public:
    std::unique_ptr<IDataWriter> createDataWriter(const Config& config) override {
        return std::make_unique<OptimizedTDengineWriter>(config);
    }
   
    std::unique_ptr<IMessageProcessor> createMessageProcessor(const Config& config) override {
        return std::make_unique<EfficientMessageProcessor>(config);
    }
   
    std::unique_ptr<IMessageConsumer> createMessageConsumer(const Config& config) override {
        return std::make_unique<FixedRabbitMQConsumer>(config);
    }
};

// ==================== 应用主类 ====================
class EnhancedSingleThreadedConsumerApplication {
private:
    Config config_;
    std::unique_ptr<EnhancedSingleThreadedProcessor> processor_;
    std::atomic<bool> shutdown_{false};
    static std::atomic<bool> signal_received_;
   
public:
    EnhancedSingleThreadedConsumerApplication(const Config& config) : config_(config) {
        auto factory = std::make_unique<EnhancedStockDataFactory>();
        processor_ = std::make_unique<EnhancedSingleThreadedProcessor>(
            factory->createDataWriter(config),
            factory->createMessageProcessor(config),
            factory->createMessageConsumer(config),
            config
        );
       
        setupSignalHandlers();
    }
   
    void run() {
        std::cout << "Starting enhanced single-threaded application..." << std::endl;
       
        std::thread processor_thread([this]() {
            processor_->start();
        });
       
        waitForShutdown();
       
        shutdown();
       
        if (processor_thread.joinable()) {
            processor_thread.join();
        }
       
        std::cout << "Application stopped" << std::endl;
    }
   
private:
    static void signalHandler(int signal) {
        signal_received_ = true;
    }
   
    void setupSignalHandlers() {
        signal_received_ = false;
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);
    }
   
    void waitForShutdown() {
        while (!shutdown_ && !signal_received_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        shutdown_ = true;
    }
   
    void shutdown() {
        processor_->stop();
    }
};
std::atomic<bool> EnhancedSingleThreadedConsumerApplication::signal_received_{false};

// ==================== 主函数 ====================
int main() {
    try {
        auto& configManager = ConfigManager::getInstance();
        EnhancedSingleThreadedConsumerApplication app(configManager.getConfig());
        app.run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
   
    return 0;
}