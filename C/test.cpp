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
    // 时间窗口配置
    int minute1_window_ms = 60000;    // 1分钟窗口
    int minute5_window_ms = 300000;   // 5分钟窗口
    int max_history_ticks = 1000;     // 最大历史tick数量
    
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000;
    
    std::string tdengine_host = "chaos";
    int tdengine_port = 6030;
    std::string tdengine_user = "root";
    std::string tdengine_password = "taosdata";
    std::string tdengine_database = "market_data1";
    
    // 单线程处理配置
    int messages_per_batch = 1;           // 每次处理1条消息
    int max_retry_count = 3;              // 最大重试次数
    int retry_delay_ms = 1000;            // 重试延迟
    bool verbose = true;
    int max_pending_messages = 50;        // 最大待处理消息数

    // 串行处理相关配置
    int processing_delay_ms = 100;           // 每条消息处理后的延迟
    bool enable_rate_limiting = true;       // 启用速率限制
    int report_time = 10;                    //报告输出间隔
    
    // 新增配置
    std::string log_file_path = "stock_analysis.log";
    int log_level = 2;  // 0:DEBUG, 1:INFO, 2:WARNING, 3:ERROR
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
            instance_.reset(new ConfigManager());  // 使用reset而不是直接赋值
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
    
    // 新增扩展字段 - 使用简写
    double inst_vol = 0.0;        // 瞬时成交量
    double inst_amt = 0.0;        // 瞬时成交额  
    double large_net = 0.0;       // 大单净额(累计)
};

// 时间窗口统计数据
struct TimeWindowStats {
    double volume_1min = 0.0;//1分量
    double amount_1min = 0.0;//1分金额
    double change_1min = 0.0;//1分涨幅
    double change_5min = 0.0;//5分涨幅
    double volume_5min = 0.0;//5分量
    double amount_5min = 0.0;//5分金额
    double large_net_1min = 0.0;  // 1分钟大单净额
    double large_net_5min = 0.0;  // 5分钟大单净额
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

// 竞价指标数据结构
struct AuctionMetrics {
    double price_change = 0.0;           // 相对于前收盘价的涨跌幅
    double match_volume_ratio = 0.0;     // 匹配量相对于近期均值的比率
    double net_large_order_flow = 0.0;   // 大单净流入金额
    double withdrawal_impact = 0.0;      // 撤单影响
    double cumulative_net_flow = 0.0;    // 累计大单净流入
    double cumulative_price_change = 0.0; // 累计涨跌幅
    double bid_amount = 0.0;             // 委买金额
    bool is_limit_up = false;            // 是否涨停
};

// 股票竞价指标数据结构
struct StockAuctionMetrics {
    std::deque<double> price_history;
    std::deque<double> av1_history;  // 卖一量历史
    std::deque<double> bv1_history;  // 买一量历史
    double auction_volume = 0.0;
    double total_bid_amount = 0.0;
    long long last_analysis_time = 0;
    double volatility_score = 0.0;
    long long added_time = 0;
    std::string volatility_level = "none";
    AuctionMetrics auction_metrics;
    StockData prev_tick_data;
    
    // 历史数据大小限制
    static const int HISTORY_SIZE = 300;
    
    StockAuctionMetrics() {
        price_history = std::deque<double>(HISTORY_SIZE, 0.0);
        av1_history = std::deque<double>(HISTORY_SIZE, 0.0);
        bv1_history = std::deque<double>(HISTORY_SIZE, 0.0);
    }
};

// 抢筹模式数据点
struct AccumulationDataPoint {
    long long timestamp = 0;
    double price = 0.0;
    double bid_amount = 0.0;
    double last_close = 0.0;
};

// 竞价市场总结数据结构
struct AuctionMarketSummary {
    std::vector<std::pair<std::string, double>> top_gainers;
    std::vector<std::pair<std::string, double>> top_losers;
    std::vector<std::pair<std::string, double>> top_net_inflow;
    std::vector<std::pair<std::string, double>> top_net_outflow;
    std::vector<std::pair<std::string, double>> top_auction_volume;
    std::vector<std::string> limit_up_stocks;
    std::vector<std::string> accumulation_stocks;
    double total_net_flow = 0.0;
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
        std::tm tm;
        localtime_r(&tt, &tm);  // 使用线程安全版本
        
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");// << "." << std::setfill('0') 
            //<< std::setw(3) << ms;
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
    
    static int getSecond(const std::string& time_str) {
        if (time_str.length() >= 8) {
            return std::stoi(time_str.substr(6, 2));
        }
        return 0;
    }
};

// ==================== 股票名称映射器（使用静态局部变量） ====================
class StockNameMapper {
private:
    std::unordered_map<std::string, std::string> code_to_name_;
    std::string csv_file_path_="stock.csv";
    std::mutex mutex_;
    bool loaded_ = false;

    // 私有构造函数
    StockNameMapper(){
        loadStockNames();
    }

    // 防止拷贝和赋值
    StockNameMapper(const StockNameMapper&) = delete;
    StockNameMapper& operator=(const StockNameMapper&) = delete;

public:
    // 获取单例实例 - 使用静态局部变量（C++11保证线程安全）
    static StockNameMapper& getInstance() {
        static StockNameMapper instance;
        return instance;
    }
    
    // // 带配置的获取实例方法
    // static StockNameMapper& getInstance(const Config& config) {
    //     static StockNameMapper instance(config.stock_map_file);
    //     return instance;
    // }

    bool loadStockNames() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        std::ifstream file(csv_file_path_);
        if (!file.is_open()) {
            std::cerr << "无法打开股票名称文件: " << csv_file_path_ << std::endl;
            return false;
        }

        std::string line;
        // 跳过标题行
        std::getline(file, line);
        
        int loaded_count = 0;
        while (std::getline(file, line)) {
            // 解析CSV行
            size_t comma_pos = line.find(',');
            if (comma_pos == std::string::npos) {
                continue;
            }
            
            std::string code = line.substr(0, comma_pos);
            std::string name = line.substr(comma_pos + 1);
            
            // 清理字符串
            code.erase(0, code.find_first_not_of(" \t"));
            code.erase(code.find_last_not_of(" \t") + 1);
            name.erase(0, name.find_first_not_of(" \t"));
            name.erase(name.find_last_not_of(" \t") + 1);
            
            // 移除代码中的市场后缀，只保留6位数字
            size_t dot_pos = code.find('.');
            if (dot_pos != std::string::npos) {
                code = code.substr(0, dot_pos);
            }
            
            if (code.length() == 6) {
                code_to_name_[code] = name;
                loaded_count++;
            }
        }
        
        file.close();
        loaded_ = true;
        
        std::cout << "成功加载 " << loaded_count << " 只股票名称" << std::endl;
        return true;
    }

    std::string getStockName(const std::string& code) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 如果代码包含市场后缀，先移除
        std::string clean_code = code;
        size_t dot_pos = code.find('.');
        if (dot_pos != std::string::npos) {
            clean_code = code.substr(0, dot_pos);
        }
        
        auto it = code_to_name_.find(clean_code);
        if (it != code_to_name_.end()) {
            return it->second;
        }
        
        // 如果没有找到，返回原始代码
        return code;
    }

    std::string getStockDisplayName(const std::string& code) {
        std::string name = getStockName(code);
        if (name != code) {
            return name + "(" + code + ")";
        }
        return code;
    }

    bool isLoaded() const {
        return loaded_;
    }

    void reload() {
        loadStockNames();
    }
    
    // 获取已加载的股票数量
    size_t getLoadedCount() const {
        return code_to_name_.size();
    }
};

// ==================== 日志系统 ====================

class Logger {
private:
    std::ofstream log_file_;
    int console_level_ = 2;    // 命令行输出级别
    int file_level_ = 1;       // 文件输出级别  
    bool enable_file_ = true;
    std::mutex log_mutex_;
    
public:
    // 保持现有构造函数，添加文件级别参数（可选）
    Logger(const std::string& file_path="consumer.log", int console_level = 2, int file_level = 1, bool enable_file = true) 
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
    
    // 设置命令行输出级别
    void setConsoleLevel(int level) {
        console_level_ = level;
    }
    
    // 设置文件输出级别
    void setFileLevel(int level) {
        file_level_ = level;
    }
    
    // 核心日志方法 - 保持现有逻辑，但分别控制输出
    void logWithTimestamp(int level, const std::string& message, long long timestamp) {
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::string level_str;
        switch(level) {
            case 0: level_str = "DEBUG"; break;
            case 1: level_str = "INFO"; break;
            case 2: level_str = "WARN"; break;
            case 3: level_str = "ERROR"; break;
            default: level_str = "INFO";
        }
        
        // 使用传入的时间戳
        std::string log_timestamp = TimeUtils::formatTimestamp(timestamp).substr(5);
        std::string log_msg = log_timestamp + "[" + level_str + "]" + message;
        
        // 分别控制命令行和文件输出
        if (level >= console_level_) {
            std::cout << log_msg << std::endl;
        }
        
        if (enable_file_ && log_file_.is_open() && level >= file_level_) {
            log_file_ << log_msg << std::endl;
            log_file_.flush();
        }
    }
    
    // 保持您现有的三个主要方法完全不变
    void infoWithTickTime(const std::string& message, long long tick_timestamp) {
        logWithTimestamp(1, message, tick_timestamp);
    }
    
    void warnWithTickTime(const std::string& message, long long tick_timestamp) {
        logWithTimestamp(2, message, tick_timestamp);
    }
    
    void errorWithTickTime(const std::string& message, long long tick_timestamp) {
        logWithTimestamp(3, message, tick_timestamp);
    }
    
    // 可选：保持其他便捷方法
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


// 全局日志实例 - 改为原始指针
Logger* global_logger = nullptr;
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
    // 新增：执行查询并返回结果
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
        oss_.str("");  // 清空内容
        oss_.clear();  // 清除错误状态
        return oss_;
    }
    
    static std::string getStringFromStream() {
        return oss_.str();
    }
};

// 线程局部变量定义
thread_local std::string StringBuffer::buffer_;
thread_local std::ostringstream StringBuffer::oss_;
// ==================== 优化的TDengine批量写入器 ====================

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
        if (records.empty()) {
            return true;  // 空批次视为成功
        }
        
        if (!connection_->isConnected() && !connect()) {
            std::cerr << "Failed to connect to TDengine for writing" << std::endl;
            return false;
        }
        
        try {
            return insertBatch(records);
            // return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert records: " << e.what() << std::endl;
            return false;
        }
    }
    
private:
    bool insertBatch(const std::vector<StockData>& batch) {
        auto& sql = StringBuffer::getStream();
        sql << "INSERT INTO stock_data(tbname, ts, symbol, "
            << "lp, o, h, l, lc, a, v, p, "
            << "ap1, ap2, ap3, ap4, ap5, "
            << "bp1, bp2, bp3, bp4, bp5, "
            << "av1, av2, av3, av4, av5, "
            << "bv1, bv2, bv3, bv4, bv5, "
            << "inst_vol, inst_amt, large_net) VALUES ";
        
        for (size_t i = 0; i < batch.size(); ++i) {
            const auto& record = batch[i];
            if (i > 0) sql << ", ";
            
            std::string tbname = "t_s_" + sanitizeSymbol(record.symbol);
            std::string escaped_symbol = escapeSingleQuote(record.symbol);
            
            sql << "('" << tbname << "', '" 
                << TimeUtils::formatTimestamp(record.timestamp) << "', '"
                << escaped_symbol << "', "
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
                << static_cast<long long>(record.bid_volumes[4]) << ", "
                << record.inst_vol << ", " << record.inst_amt << ", " << record.large_net << ")";
        }
        
        try {
            connection_->execute(sql.str());
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to insert batch: " << e.what() << std::endl;
            return false;
        }
    }
    
    std::string sanitizeSymbol(const std::string& symbol) {
        auto& sanitized = StringBuffer::getString();
        // sanitized.reserve(symbol.size());
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
        auto& escaped = StringBuffer::getString();
        // std::string escaped;
        escaped.reserve(str.size() * 2);  // 预分配足够空间
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
// ==================== Tick分析引擎 ====================

class TickAnalysisEngine {
private:
    struct StockTickState {
        StockData prev_tick;
        double cumulative_large_net = 0.0; // 累计大单净额
        bool has_previous = false;
        long long last_update = 0;
        
        // 用于竞价阶段撤单分析
        double pre_20_bid_amount = 0.0;    // 20分前委买金额
        double pre_20_ask_amount = 0.0;    // 20分前委卖金额
    };
    
    std::unordered_map<std::string, StockTickState> stock_states_;
    std::mutex state_mutex_;
    double large_order_threshold_;
    StockNameMapper& stock_mapper_;
    // 移除 TDengineConnection 成员，改为在需要时创建连接
    const Config& config_;
     std::unique_ptr<TDengineConnection> tdengine_conn;
public:
    // 修改构造函数，接受 Config 引用
    TickAnalysisEngine(const Config& config, double threshold = 500000) 
        : config_(config), large_order_threshold_(threshold), stock_mapper_(StockNameMapper::getInstance())
    { 
    }
    
    // 处理tick数据，计算扩展字段
    void processTickData(StockData& current_tick, bool is_auction_period = false) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = stock_states_[current_tick.symbol];
        
        // if (!state.has_previous) {
        //     // 第一次处理该symbol，尝试从TDengine查询前一个tick
        //     if (tryLoadPreviousTickFromTDengine(current_tick, state)) {
        //         if (global_logger) {
        //             global_logger->warn("Loaded previous tick for " + current_tick.symbol + 
        //                                " from TDengine, time diff: " + 
        //                                std::to_string(current_tick.timestamp - state.prev_tick.timestamp) + "ms");
        //         }
        //     } else {
        //         if (global_logger) {
        //             global_logger->warn("No previous tick found for " + current_tick.symbol + 
        //                                ", using default values for inst_vol, inst_amt, large_net");
        //         }
        //     }
        // }
        
        if (state.has_previous) {
            // 计算瞬时成交量/额
            current_tick.inst_vol = current_tick.volume - state.prev_tick.volume;
            current_tick.inst_amt = current_tick.amount - state.prev_tick.amount;
            
            // 计算大单净额
            calculateLargeOrder(current_tick, state);
        } else {
            current_tick.inst_vol = 0;
            current_tick.inst_amt = 0;
            current_tick.large_net = 0;
            
        }
        
        // 竞价阶段特殊处理
        if (is_auction_period) {
            processAuctionMetrics(current_tick, state);
        }
        
        // 更新状态
        state.prev_tick = current_tick;
        state.has_previous = true;
        state.last_update = current_tick.timestamp;
    }
    
    // 获取累计大单净额
    double getCumulativeLargeNet(const std::string& symbol) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = stock_states_.find(symbol);
        if (it != stock_states_.end()) {
            return it->second.cumulative_large_net;
        }
        return 0.0;
    }
    
    // 清理过时数据
    void cleanupOldData(long long current_time) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        long long cutoff_time = current_time - 3600000; // 1小时前
        
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
    bool tryLoadPreviousTickFromTDengine(const StockData& current_tick, StockTickState& state) {
    try {
        // 直接创建TDengineConnection对象
        TDengineConnection tdengine_conn(
            config_.tdengine_host, config_.tdengine_user, 
            config_.tdengine_password, config_.tdengine_database, 
            config_.tdengine_port
        );
        
        if (!tdengine_conn.connect()) {
            if (global_logger) {
                global_logger->warn("TDengine connection failed, cannot load previous tick for " + current_tick.symbol);
            }
            return false;
        }
        
        // 计算时间范围：当前时间往前10秒
        long long start_time = current_tick.timestamp - 10000; // 10秒前
        std::string start_time_str = TimeUtils::formatTimestamp(start_time);
        std::string current_time_str = TimeUtils::formatTimestamp(current_tick.timestamp);
        
        // 构建SQL查询：查询所有symbol在时间范围内的最新tick，每个symbol取时间最大的
        std::string sql = 
            "SELECT symbol, ts, lp, o, h, l, lc, a, v, "
            "ap1, ap2, ap3, ap4, ap5, "
            "bp1, bp2, bp3, bp4, bp5, "
            "av1, av2, av3, av4, av5, "
            "bv1, bv2, bv3, bv4, bv5, "
            "inst_vol, inst_amt, large_net "
            "FROM stock_data "
            "WHERE ts >= '" + start_time_str + "' AND ts < '" + current_time_str + "' "
            "ORDER BY ts DESC";
        
        TAOS_RES* res = tdengine_conn.query(sql);
        
        // 检查查询结果是否为空
        if (!res) {
            if (global_logger) {
                global_logger->warn("TDengine query returned null result for " + current_tick.symbol);
            }
            return false;
        }
        
        int num_fields = taos_num_fields(res);
        if (num_fields <= 0) {
            if (global_logger) {
                global_logger->warn("TDengine query returned no fields for " + current_tick.symbol);
            }
            taos_free_result(res);
            return false;
        }
        
        std::cout << "获取到:" << num_fields << " sql:" << sql << std::endl;
        
        // 获取字段信息
        TAOS_FIELD* fields = taos_fetch_fields(res);
        if (!fields) {
            if (global_logger) {
                global_logger->warn("Failed to fetch fields from TDengine result for " + current_tick.symbol);
            }
            taos_free_result(res);
            return false;
        }
        
        // 处理查询结果：按照symbol分组，每个symbol取时间最大的记录
        std::unordered_map<std::string, StockData> latest_ticks;
        TAOS_ROW row;
        int row_count = 0;
        
        while ((row = taos_fetch_row(res))) {
            row_count++;
            
            // 检查row是否为空
            if (!row) {
                continue;
            }
            
            StockData tick = parseTDRowToStockData(row, num_fields, fields);
            if (tick.symbol.empty() || tick.timestamp == 0) {
                continue;
            }
            
            // 如果这个symbol还没有记录，或者当前记录时间更晚，则更新
            auto it = latest_ticks.find(tick.symbol);
            if (it == latest_ticks.end() || tick.timestamp > it->second.timestamp) {
                latest_ticks[tick.symbol] = tick;
            }
        }
        
        taos_free_result(res);
        
        if (global_logger) {
            global_logger->warn("TDengine query returned " + std::to_string(row_count) + " rows, " +
                               std::to_string(latest_ticks.size()) + " unique symbols for " + current_tick.symbol);
        }
        
        // 查找当前symbol的前一个tick
        auto it = latest_ticks.find(current_tick.symbol);
        if (it != latest_ticks.end()) {
            // 检查时间差是否在10秒内
            long long time_diff = current_tick.timestamp - it->second.timestamp;
            if (time_diff <= 10000) { // 10秒内
                state.prev_tick = it->second;
                state.has_previous = true;
                
                if (global_logger) {
                    global_logger->warn("Found previous tick for " + current_tick.symbol + 
                                      " at " + TimeUtils::formatTimestamp(state.prev_tick.timestamp) +
                                      ", time diff: " + std::to_string(time_diff) + "ms");
                }
                return true;
            } else {
                if (global_logger) {
                    global_logger->warn("Found previous tick for " + current_tick.symbol + 
                                       " but time diff too large: " + std::to_string(time_diff) + "ms");
                }
            }
        }
        
        if (global_logger) {
            global_logger->warn("No previous tick found in TDengine for " + current_tick.symbol + 
                               " within 10 seconds");
        }
        return false;
        
    } catch (const std::exception& e) {
        if (global_logger) {
            global_logger->error("Exception in tryLoadPreviousTickFromTDengine: " + std::string(e.what()));
        }
        return false;
    }
}

// 修改后的解析函数，添加字段信息参数
StockData parseTDRowToStockData(TAOS_ROW row, int num_fields, TAOS_FIELD* fields) {
    StockData data;
    
    if (!row || !fields) {
        return data;
    }
    
    for (int i = 0; i < num_fields; i++) {
        if (row[i] == NULL) continue;
        
        std::string field_name = fields[i].name;
        
        // 安全地解析每个字段
        try {
            if (field_name == "symbol") {
                data.symbol = std::string((char*)row[i]);
            }
            else if (field_name == "ts") {
                // 解析时间戳，TDengine返回的是Unix时间戳（毫秒）
                data.timestamp = *((int64_t*)row[i]);
            }
            else if (field_name == "lp") {
                data.last_price = *((double*)row[i]);
            }
            else if (field_name == "o") {
                data.open = *((double*)row[i]);
            }
            else if (field_name == "h") {
                data.high = *((double*)row[i]);
            }
            else if (field_name == "l") {
                data.low = *((double*)row[i]);
            }
            else if (field_name == "lc") {
                data.close = *((double*)row[i]);
            }
            else if (field_name == "a") {
                data.amount = *((double*)row[i]);
            }
            else if (field_name == "v") {
                data.volume = *((int64_t*)row[i]);
            }
            else if (field_name == "ap1") { data.ask_prices[0] = *((double*)row[i]); }
            else if (field_name == "ap2") { data.ask_prices[1] = *((double*)row[i]); }
            else if (field_name == "ap3") { data.ask_prices[2] = *((double*)row[i]); }
            else if (field_name == "ap4") { data.ask_prices[3] = *((double*)row[i]); }
            else if (field_name == "ap5") { data.ask_prices[4] = *((double*)row[i]); }
            else if (field_name == "bp1") { data.bid_prices[0] = *((double*)row[i]); }
            else if (field_name == "bp2") { data.bid_prices[1] = *((double*)row[i]); }
            else if (field_name == "bp3") { data.bid_prices[2] = *((double*)row[i]); }
            else if (field_name == "bp4") { data.bid_prices[3] = *((double*)row[i]); }
            else if (field_name == "bp5") { data.bid_prices[4] = *((double*)row[i]); }
            else if (field_name == "av1") { data.ask_volumes[0] = *((int64_t*)row[i]); }
            else if (field_name == "av2") { data.ask_volumes[1] = *((int64_t*)row[i]); }
            else if (field_name == "av3") { data.ask_volumes[2] = *((int64_t*)row[i]); }
            else if (field_name == "av4") { data.ask_volumes[3] = *((int64_t*)row[i]); }
            else if (field_name == "av5") { data.ask_volumes[4] = *((int64_t*)row[i]); }
            else if (field_name == "bv1") { data.bid_volumes[0] = *((int64_t*)row[i]); }
            else if (field_name == "bv2") { data.bid_volumes[1] = *((int64_t*)row[i]); }
            else if (field_name == "bv3") { data.bid_volumes[2] = *((int64_t*)row[i]); }
            else if (field_name == "bv4") { data.bid_volumes[3] = *((int64_t*)row[i]); }
            else if (field_name == "bv5") { data.bid_volumes[4] = *((int64_t*)row[i]); }
            else if (field_name == "inst_vol") { data.inst_vol = *((double*)row[i]); }
            else if (field_name == "inst_amt") { data.inst_amt = *((double*)row[i]); }
            else if (field_name == "large_net") { data.large_net = *((double*)row[i]); }
        } catch (const std::exception& e) {
            // 忽略单个字段解析错误，继续处理其他字段
            if (global_logger) {
                global_logger->warn("Failed to parse field " + field_name + ": " + std::string(e.what()));
            }
        }
    }
    
    return data;
}
    
    void calculateLargeOrder(StockData& current_tick, StockTickState& state) {
        double instant_amount = current_tick.inst_amt;
        
        if (std::abs(instant_amount) > large_order_threshold_) {
            // 判断大单方向
            if (current_tick.last_price > state.prev_tick.last_price) {
                current_tick.large_net = instant_amount; // 买入大单
            } else if (current_tick.last_price < state.prev_tick.last_price) {
                current_tick.large_net = -instant_amount; // 卖出大单
            } else {
                // 价格不变，根据买卖盘变化判断
                double bid_change = current_tick.bid_volumes[0] - state.prev_tick.bid_volumes[0];
                double ask_change = current_tick.ask_volumes[0] - state.prev_tick.ask_volumes[0];
                current_tick.large_net = (bid_change > ask_change) ? instant_amount : -instant_amount;
            }
            
            // 更新累计大单净额
            state.cumulative_large_net += current_tick.large_net;
            
            // 大单日志
            if (global_logger && std::abs(current_tick.large_net) > large_order_threshold_ * 4) {
                std::string direction = current_tick.large_net > 0 ? "买入" : "卖出";
                global_logger->infoWithTickTime("大单|" + stock_mapper_.getStockDisplayName(current_tick.symbol) 
                +"|瞬时:"+std::to_string(int(current_tick.inst_amt*0.0001))+"万"
                +"|涨幅:"+std::to_string(std::round((current_tick.last_price-current_tick.close)*10000/current_tick.close)/100)+ "|" + direction + "|" 
                + std::to_string(int(current_tick.large_net / 10000)) + "万元 |净额: "+std::to_string(int(state.cumulative_large_net / 10000)) + "万元",
                current_tick.timestamp);
            }   
        } else {
            current_tick.large_net = 0;
        }
    }
    
    void processAuctionMetrics(StockData& current_tick, StockTickState& state) {
        std::string time_str = TimeUtils::formatTimestamp(current_tick.timestamp).substr(11, 8);
        
        // 记录20分前的委买委卖金额
        if (time_str >= "09:15:00" && time_str < "09:20:00") {
            state.pre_20_bid_amount = (current_tick.bid_volumes[0] + current_tick.bid_volumes[1]) * current_tick.last_price * 100;
            state.pre_20_ask_amount = (current_tick.ask_volumes[0] + current_tick.ask_volumes[1]) * current_tick.last_price * 100;
        }
        
        // 20分时检查撤单情况
        if (time_str >= "09:20:00" && time_str <= "09:20:01") {
            double current_bid = (current_tick.bid_volumes[0] + current_tick.bid_volumes[1]) * current_tick.last_price * 100;
            double bid_withdrawal = state.pre_20_bid_amount - current_bid;
            
            if (bid_withdrawal > large_order_threshold_) {
                if (global_logger) {
                    global_logger->warnWithTickTime("撤单预警|" + current_tick.symbol + "|委买撤单:" + 
                                   std::to_string(int(bid_withdrawal / 10000)) + "万元",current_tick.timestamp);
                }
            }
        }
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
        if (context_) return true;  // 已经连接
        
        context_ = redisConnect(host_.c_str(), port_);
        if (!context_ || (context_->err)) {
            if (context_) {
                redisFree(context_);
                context_ = nullptr;
            }
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
            disconnect();  // 连接可能已断开
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
    // 配置参数
    struct AuctionThresholds {
        double price_change = 0.02;           // 涨跌幅超过2%
        double match_volume_ratio = 1.5;      // 匹配量是近期均值的1.5倍
        double net_large_order_flow = 500000; // 大单净流入50万元
        double min_volatility_score = 15;     // 最小异动分数阈值
        double cumulative_net_flow = 1000000; // 累计大单净流入100万元
        double cumulative_price_change = 0.05; // 累计涨跌幅5%
        double bid_amount_large = 5000000;    // 委买金额大单阈值(500万元)
        double accumulation_bid_increase = 1000000; // 抢筹模式委买金额增加阈值
        double accumulation_price_increase = 0.01;  // 抢筹模式价格上涨阈值
    };
    
    struct VolatilityLevels {
        double low = 30;
        double medium = 50;
        double high = 80;
    };
    
    AuctionThresholds thresholds_;
    VolatilityLevels volatility_levels_;
    
    // 数据存储
    std::unordered_map<std::string, StockAuctionMetrics> stock_auction_metrics_;
    std::unordered_map<std::string, std::vector<AccumulationDataPoint>> post_20_data_;
    
    std::vector<std::string> volatile_stocks_;
    std::vector<std::string> accumulation_stocks_;
    std::vector<std::string> limit_up_stocks_;
    
    AuctionMarketSummary market_summary_;
    std::string last_summary_time_;
    
    StockNameMapper& stock_mapper_;
    std::mutex data_mutex_;
    
    // 关键时间点
    const std::string TRIAL_END = "09:20:00";
    const std::string AUCTION_NEAR_END = "09:24:00";
    const std::string AUCTION_END = "09:25:00";
    int report_time = 0;//报告输出次数
    
public:
    AuctionAnalyzer():stock_mapper_(StockNameMapper::getInstance()) {
        
    }
    
    // 判断当前是否在竞价时间段
    bool isAuctionPeriod(long long timestamp) {
        std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11,19);
        return time_str >= "09:15:00" && time_str <= "09:25:00";
    }
    
    // 判断是否在试盘阶段（可撤单）
    bool isTrialPeriod(long long timestamp) {
        std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11,19);
        return time_str >= "09:15:00" && time_str < "09:20:00";
    }
    
    // 处理股票tick数据
    void processTickData(const StockData& data) {
        if (!isAuctionPeriod(data.timestamp)) {
            return;
        }
        
        std::lock_guard<std::mutex> lock(data_mutex_);
        const std::string& symbol = data.symbol;
        
        StockAuctionMetrics& metrics = stock_auction_metrics_[symbol];
        
        // 计算当前价格和涨跌幅
        double current_price = data.last_price;
        double last_close = data.close;
        
        // 获取卖一量和买一量
        double av1 = data.ask_volumes[0];
        double bv1 = data.bid_volumes[0];
        
        // 计算总委买金额（买一和买二）
        double total_bid_amount = (data.bid_volumes[0] + data.bid_volumes[1]) * current_price * 100;
        
        // 更新竞价成交量
        double auction_volume = std::max(av1, bv1) * current_price * 0.01;
        metrics.auction_volume = auction_volume;
        
        // 更新历史数据
        updateHistoryData(metrics, current_price, av1, bv1);
        
        // 更新竞价指标
        updateAuctionMetrics(metrics, data, current_price, last_close, total_bid_amount);
        // std::cout<<"检查20分后的抢筹模式"<<std::endl;
        // 检查20分后的抢筹模式
        std::string current_time = TimeUtils::formatTimestamp(data.timestamp);
        if (current_time >= "09:20:00") {
            analyzeAccumulationPattern(symbol, data.timestamp, current_price, total_bid_amount, last_close);
        }
        // std::cout<<"分析撤单和大单"<<std::endl;
        // 分析撤单和大单
        analyzeOrderFlow(symbol, data, metrics, data.timestamp);
        // std::cout<<"每秒进行一次完整的异动分析"<<std::endl;
        // 每秒进行一次完整的异动分析
        if (data.timestamp - metrics.last_analysis_time > 1000) {
            analyzeAuctionVolatility(symbol, metrics, data.timestamp);
            metrics.last_analysis_time = data.timestamp;
        }
        // std::cout<<"检查关键时间点: "<< data.symbol<<std::endl;
        // 检查关键时间点
        checkKeyTimepoints(data.timestamp);
    }
    
    // 获取大单阈值
    double getLargeOrderThreshold(double price) {
        if (price == 0) return 0;
        return 300000 / price / 100;
    }
    
    // 清理过时数据
    void cleanupOldData() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long cutoff_time = current_time - 600000; // 1小时前
        
        auto it = stock_auction_metrics_.begin();
        while (it != stock_auction_metrics_.end()) {
            if (it->second.last_analysis_time < cutoff_time) {
                it = stock_auction_metrics_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    // 获取市场总结
    AuctionMarketSummary getMarketSummary() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return market_summary_;
    }
    
    // 生成增强的竞价报告
    void generateEnhancedAuctionReport(const std::string& time_str, long long timestamp) {
        // std::cout<<"into generateEnhancedAuctionReport "<<std::endl;
        // std::lock_guard<std::mutex> lock(data_mutex_);
        // std::cout<<"计算市场统计 "<<std::endl;
        // 计算市场统计
        auto summary = calculateEnhancedMarketSummary();
        
        std::ostringstream report;
        report << "====== 竞价报告 " << time_str << " (Tick时间: " 
               << TimeUtils::formatTimestamp(timestamp) << ") ======\n";
        
        // 竞价强度
        double auction_strength = summary.total_stocks > 0 ? 
            (double)summary.high_open_count / summary.total_stocks : 0;
        report << "竞价强度: " << (auction_strength * 100) << "%\n";
        
        // 涨跌幅分布
        report << "高开>3%: " << summary.high_open_count << " 只, "
               << "低开<-3%: " << summary.low_open_count << " 只, "
               << "平开: " << (summary.total_stocks - summary.high_open_count - summary.low_open_count) << " 只\n";
        
        // 涨停股票统计
        report << "涨停数量: " << summary.limit_up_stocks.size() << "\n";
        if (!summary.limit_up_stocks.empty()) {
            report << "涨停股票详情:\n";
            for (const auto& symbol : summary.limit_up_stocks) {
                auto it = stock_auction_metrics_.find(symbol);
                if (it != stock_auction_metrics_.end()) {
                    const auto& metrics = it->second;
                    double bid_amount = metrics.auction_metrics.bid_amount;
                    double match_amount = metrics.auction_volume * 100; // 估算成交金额
                    report << "  " << stock_mapper_.getStockDisplayName(symbol) 
                           << " 涨幅:" << (metrics.auction_metrics.price_change * 100) << "%"
                           << " 封单:" << std::to_string(int(bid_amount / 10000)) << "万"
                           << " 成交:" << std::to_string(int(match_amount / 10000)) << "万"
                           << " 大单:" << std::to_string(int(metrics.auction_metrics.net_large_order_flow / 10000)) << "万\n";
                }
            }
        }
        
        // 大单净额排名
        report << "大单净额排名(前20):\n";
        int count = 0;
        for (const auto& item : summary.large_net_ranking) {
            if (count++ >= 20) break;
            report << "  " << stock_mapper_.getStockDisplayName(item.first) 
                   << " " << std::to_string(int(item.second / 10000)) << "万元\n";
        }
        
        // 成交额排名
        report << "成交额排名(前20):\n";
        count = 0;
        for (const auto& item : summary.amount_ranking) {
            if (count++ >= 20) break;
            report << "  " << stock_mapper_.getStockDisplayName(item.first) 
                   << " " << std::to_string(int(item.second / 10000)) << "万元\n";
        }
        
        // 输出到日志
        if (global_logger) {
            global_logger->warnWithTickTime(report.str(), timestamp);
        } else {
            std::cout << report.str() << std::endl;
        }
    }
    
private:
    struct EnhancedMarketSummary {
        int total_stocks = 0;
        int high_open_count = 0;  // 高开>3%
        int low_open_count = 0;   // 低开<-3%
        std::vector<std::string> limit_up_stocks;
        std::vector<std::pair<std::string, double>> large_net_ranking;
        std::vector<std::pair<std::string, double>> amount_ranking;
    };
    
    EnhancedMarketSummary calculateEnhancedMarketSummary() {
        EnhancedMarketSummary summary;
        std::vector<std::pair<std::string, double>> large_nets;
        std::vector<std::pair<std::string, double>> amounts;
        // std::cout<<"calculateEnhancedMarketSummary"<<std::endl;
        for (const auto& pair : stock_auction_metrics_) {
            const auto& symbol = pair.first;
            const auto& metrics = pair.second;
            
            summary.total_stocks++;
            
            // 计算涨跌幅
            double change = metrics.auction_metrics.price_change;
            if (change > 0.03) summary.high_open_count++;
            if (change < -0.03) summary.low_open_count++;
            
            // 涨停统计
            if (metrics.auction_metrics.is_limit_up) {
                summary.limit_up_stocks.push_back(symbol);
            }
            
            // 大单净额排名
            large_nets.emplace_back(symbol, metrics.auction_metrics.net_large_order_flow);
            
            // 成交额排名（使用竞价成交量估算）
            double amount = metrics.auction_volume * 100; // 估算
            amounts.emplace_back(symbol, amount);
        }
        // std::cout<<"for stock_auction_metrics_ END"<<std::endl;
        // 排序
        std::sort(large_nets.begin(), large_nets.end(), 
                 [](const auto& a, const auto& b) { return std::abs(a.second) > std::abs(b.second); });
        std::sort(amounts.begin(), amounts.end(), 
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        
        summary.large_net_ranking = std::move(large_nets);
        summary.amount_ranking = std::move(amounts);
        // std::cout<<"calculateEnhancedMarketSummary END"<<std::endl;
        return summary;
    }
    
    void updateHistoryData(StockAuctionMetrics& metrics, double price, double av1, double bv1) {
        metrics.price_history.push_back(price);
        metrics.av1_history.push_back(av1);
        metrics.bv1_history.push_back(bv1);
        
        // 限制历史数据大小
        if (metrics.price_history.size() > StockAuctionMetrics::HISTORY_SIZE) {
            metrics.price_history.pop_front();
        }
        if (metrics.av1_history.size() > StockAuctionMetrics::HISTORY_SIZE) {
            metrics.av1_history.pop_front();
        }
        if (metrics.bv1_history.size() > StockAuctionMetrics::HISTORY_SIZE) {
            metrics.bv1_history.pop_front();
        }
    }
    
    void updateAuctionMetrics(StockAuctionMetrics& metrics, const StockData& data, 
                             double current_price, double last_close, double total_bid_amount) {
        AuctionMetrics& auction_metrics = metrics.auction_metrics;
        
        if (last_close > 0) {
            auction_metrics.price_change = std::round((current_price - last_close)*10000 / last_close)*0.0001;
            
            // 检查是否涨停
            double limit_up_price = std::round(last_close * 1.1 * 100) / 100;
            std::string symbol_prefix = data.symbol.substr(0, 2);
            if (symbol_prefix == "30" || symbol_prefix == "68") {
                limit_up_price = std::round(last_close * 1.2 * 100) / 100;
            }
            
            auction_metrics.is_limit_up = std::abs(current_price - limit_up_price) < 0.01;
            
            if (auction_metrics.is_limit_up && 
                std::find(limit_up_stocks_.begin(), limit_up_stocks_.end(), data.symbol) == limit_up_stocks_.end() &&
                total_bid_amount > (data.ask_volumes[0] + data.ask_volumes[1]) * current_price * 100) {
                limit_up_stocks_.push_back(data.symbol);
                if (global_logger) {
                    global_logger->warnWithTickTime(TimeUtils::formatTimestamp(data.timestamp) + "|" 
                         + stock_mapper_.getStockDisplayName(data.symbol) 
                         + " 已涨停，涨停价: " + std::to_string(limit_up_price) 
                         + "，委买金额: " + std::to_string(int(total_bid_amount / 10000)) + "万元",
                         data.timestamp);
                }
            }
            
            // 更新累计涨跌幅
            if (metrics.price_history.size() > 1 && metrics.price_history.front() > 0) {
                double first_price = metrics.price_history.front();
                auction_metrics.cumulative_price_change = std::round((current_price - first_price)*10000 / first_price)*0.0001;
            }
        }
        
        auction_metrics.bid_amount = total_bid_amount;
    }
    
    void analyzeAccumulationPattern(const std::string& symbol, long long timestamp,
                                   double current_price, double bid_amount, double last_close) {
        // 记录数据点
        AccumulationDataPoint data_point;
        data_point.timestamp = timestamp;
        data_point.price = current_price;
        data_point.bid_amount = bid_amount;
        data_point.last_close = last_close;
        
        post_20_data_[symbol].push_back(data_point);
        
        // 限制数据点数量
        if (post_20_data_[symbol].size() > 10) {
            post_20_data_[symbol].erase(post_20_data_[symbol].begin());
        }
        
        // 分析抢筹模式（至少需要3个数据点）
        if (post_20_data_[symbol].size() >= 3) {
            auto& data_points = post_20_data_[symbol];
            std::vector<double> bid_amounts;
            std::vector<double> prices;
            
            for (const auto& point : data_points) {
                bid_amounts.push_back(point.bid_amount);
                prices.push_back(point.price);
            }
            
            // 检查委买金额是否持续增加
            bool bid_increasing = true;
            for (size_t i = 0; i < bid_amounts.size() - 1; ++i) {
                if (bid_amounts[i] >= bid_amounts[i + 1]) {
                    bid_increasing = false;
                    break;
                }
            }
            
            // 检查价格是否持续上涨
            bool price_increasing = true;
            for (size_t i = 0; i < prices.size() - 1; ++i) {
                if (prices[i] >= prices[i + 1]) {
                    price_increasing = false;
                    break;
                }
            }
            
            // 计算价格涨幅
            double price_increase = std::round((prices.back() - prices.front())*10000 / prices.front())/0.0001;
            
            // 判断是否为抢筹模式
            if (bid_increasing && 
                (price_increasing || prices.back() >= last_close * 1.097) &&
                (bid_amounts.back() - bid_amounts.front()) >= thresholds_.accumulation_bid_increase &&
                price_increase >= thresholds_.accumulation_price_increase &&
                std::find(accumulation_stocks_.begin(), accumulation_stocks_.end(), symbol) == accumulation_stocks_.end()) {
                
                accumulation_stocks_.push_back(symbol);
                if (global_logger) {
                    global_logger->warnWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                         + stock_mapper_.getStockDisplayName(symbol) + " 出现抢筹模式: "
                         + "委买增加 " + std::to_string(int((bid_amounts.back() - bid_amounts.front()) / 10000)) + "万, "
                         + "价格上涨 " + std::to_string(price_increase * 100) + "%",
                         timestamp);
                }
            }
        }
    }
    
    void analyzeOrderFlow(const std::string& symbol, const StockData& curr_data, 
                         StockAuctionMetrics& metrics, long long timestamp) {
        StockData& prev_data = metrics.prev_tick_data;
        
        // 如果是第一条数据，只存储不分析
        if (prev_data.timestamp == 0) {
            prev_data = curr_data;
            return;
        }
        
        bool is_trial_period = isTrialPeriod(timestamp);
        
        if (is_trial_period) {
            analyzeWithdrawals(symbol, curr_data, prev_data, metrics, timestamp);
        } else {
            analyzeLargeOrders(symbol, curr_data, prev_data, metrics, timestamp);
        }
        
        // 更新前一个tick数据
        prev_data = curr_data;
    }
    
    void analyzeWithdrawals(const std::string& symbol, const StockData& curr_data,
                           const StockData& prev_data, StockAuctionMetrics& metrics, 
                           long long timestamp) {
        // 检查卖单撤单：同一价格下，av1减少
        if (curr_data.ask_prices[0] == prev_data.ask_prices[0]) {
            double delta_av1 = curr_data.ask_volumes[0] - prev_data.ask_volumes[0];
            if (delta_av1 < 0) {
                double threshold = getLargeOrderThreshold(curr_data.ask_prices[0]);
                if (std::abs(delta_av1) >= threshold) {
                    double withdrawal_value = std::abs(delta_av1) * curr_data.ask_prices[0] * 100;
                    metrics.auction_metrics.withdrawal_impact += withdrawal_value;
                    
                    // 只记录高级别撤单
                    if (withdrawal_value > 10000 * 100) {  // 50万元以上
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + std::to_string(metrics.auction_metrics.price_change * 100) + "% "
                                     + "卖单撤单: " + std::to_string(std::abs(delta_av1)) + "股, "
                                     + "价格: " + std::to_string(curr_data.ask_prices[0]) + ", "
                                     + "金额: " + std::to_string(int(withdrawal_value * 0.0001)) + "万元", timestamp);
                        }
                    }
                }
            }
        }
        
        // 检查买单撤单：同一价格下，bv1减少
        if (curr_data.bid_prices[0] == prev_data.bid_prices[0]) {
            double delta_bv1 = curr_data.bid_volumes[0] - prev_data.bid_volumes[0];
            if (delta_bv1 < 0) {
                double threshold = getLargeOrderThreshold(curr_data.bid_prices[0]);
                if (std::abs(delta_bv1) >= threshold) {
                    double withdrawal_value = std::abs(delta_bv1) * curr_data.bid_prices[0] * 100;
                    metrics.auction_metrics.withdrawal_impact -= withdrawal_value;
                    
                    // 只记录高级别撤单
                    if (withdrawal_value > 100 * 10000) {  // 50万元以上
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + std::to_string(metrics.auction_metrics.price_change * 100) + "% "
                                     + "买单撤单: " + std::to_string(std::abs(delta_bv1)) + "股, "
                                     + "价格: " + std::to_string(curr_data.bid_prices[0]) + ", "
                                     + "金额: " + std::to_string(int(withdrawal_value * 0.0001)) + "万元",timestamp);
                        }
                    }
                }
            }
        }
    }
    
    void analyzeLargeOrders(const std::string& symbol, const StockData& curr_data,
                          const StockData& prev_data, StockAuctionMetrics& metrics,
                          long long timestamp) {
        // 计算匹配量变化
        double delta_av1 = curr_data.ask_volumes[0] - prev_data.ask_volumes[0];
        double delta_bv1 = curr_data.bid_volumes[0] - prev_data.bid_volumes[0];
        
        // 检查卖单大单：匹配量增加且价格下降
        if (delta_av1 > 0) {
            double threshold = getLargeOrderThreshold(curr_data.ask_prices[0]);
            if (delta_av1 >= threshold) {
                // 判断价格变化
                double price_change = curr_data.ask_prices[0] - prev_data.ask_prices[0];
                if (price_change < 0) {  // 价格下降，卖单大单
                    double order_value = delta_av1 * curr_data.ask_prices[0] * 100;
                    metrics.auction_metrics.net_large_order_flow -= order_value;
                    metrics.auction_metrics.cumulative_net_flow -= order_value;
                    
                    // 只记录高级别大单
                    if (order_value > 1000000 * 5) {  // 500万元以上
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|大单|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + std::to_string(metrics.auction_metrics.price_change * 100) + "% "
                                     + "卖单大单: " + std::to_string(delta_av1) + "股, "
                                     + "价格: " + std::to_string(curr_data.ask_prices[0]) + ", "
                                     + "金额: " + std::to_string(int(order_value * 0.0001)) + "万元",timestamp);
                        }
                    }
                }
            }
        }
        
        // 检查买单大单：匹配量增加且价格上升或不变
        if (delta_bv1 > 0) {
            double threshold = getLargeOrderThreshold(curr_data.bid_prices[0]);
            if (delta_bv1 >= threshold) {
                // 判断价格变化
                double price_change = curr_data.ask_prices[0] - prev_data.ask_prices[0];
                if (price_change >= 0) {  // 价格上升或不变，买单大单
                    double order_value = delta_bv1 * curr_data.bid_prices[0] * 100;
                    metrics.auction_metrics.net_large_order_flow += order_value;
                    metrics.auction_metrics.cumulative_net_flow += order_value;
                    
                    // 只记录高级别大单
                    if (order_value > 1000000 * 5) {  // 500万元以上
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + std::to_string(metrics.auction_metrics.price_change * 100) + "% "
                                     + "买单大单: " + std::to_string(delta_bv1) + "股, "
                                     + "价格: " + std::to_string(curr_data.bid_prices[0]) + ", "
                                     + "金额: " + std::to_string(int(order_value * 0.0001)) + "万元",timestamp);
                        }
                    }
                }
            }
        }
    }
    
    void analyzeAuctionVolatility(const std::string& symbol, StockAuctionMetrics& metrics, 
                                 long long timestamp) {
        AuctionMetrics& auction_metrics = metrics.auction_metrics;
        
        // 计算匹配量比率
        if (metrics.av1_history.size() > 5) {
            double recent_avg = 0.0;
            int count = 0;
            size_t start_idx = metrics.av1_history.size() > 5 ? metrics.av1_history.size() - 5 : 0;
            for (size_t i = start_idx; i < metrics.av1_history.size() - 1; ++i) {
                recent_avg += metrics.av1_history[i];
                count++;
            }
            if (count > 0) recent_avg /= count;
            
            if (recent_avg > 0) {
                double current_av1 = metrics.av1_history.back();
                auction_metrics.match_volume_ratio = current_av1 / recent_avg;
            }
        }
        
        // 计算异动分数
        double volatility_score = 0;
        
        // 累计涨跌幅异动
        if (std::abs(auction_metrics.cumulative_price_change) >= thresholds_.cumulative_price_change) {
            volatility_score += std::abs(auction_metrics.cumulative_price_change) * 300;
        }
        
        // 累计大单净流入异动
        if (std::abs(auction_metrics.cumulative_net_flow) >= thresholds_.cumulative_net_flow) {
            volatility_score += std::abs(auction_metrics.cumulative_net_flow) / 100000;
        }
        
        // 价格异动
        if (std::abs(auction_metrics.price_change) >= thresholds_.price_change) {
            volatility_score += std::abs(auction_metrics.price_change) * 50;
        }
        
        // 匹配量异动
        if (auction_metrics.match_volume_ratio >= thresholds_.match_volume_ratio) {
            volatility_score += auction_metrics.match_volume_ratio * 1;
        }
        
        // 大单净流入异动
        if (std::abs(auction_metrics.net_large_order_flow) >= thresholds_.net_large_order_flow) {
            volatility_score += std::abs(auction_metrics.net_large_order_flow) / 100000;
        }
        
        // 撤单影响
        if (std::abs(auction_metrics.withdrawal_impact) >= thresholds_.net_large_order_flow) {
            volatility_score += std::abs(auction_metrics.withdrawal_impact) / 100000;
        }
        
        // 更新异动分数
        metrics.volatility_score = volatility_score;
        
        // 确定异动级别
        if (volatility_score >= volatility_levels_.high) {
            metrics.volatility_level = "high";
        } else if (volatility_score >= volatility_levels_.medium) {
            metrics.volatility_level = "medium";
        } else if (volatility_score >= volatility_levels_.low) {
            metrics.volatility_level = "low";
        } else {
            metrics.volatility_level = "none";
        }
        
        // 决定是否加入异动池
        if (volatility_score >= thresholds_.min_volatility_score) {
            std::string reason = getVolatilityReason(auction_metrics, volatility_score);
            
            // 只对高级别异动输出日志
            if (metrics.volatility_level == "high") {
                bool is_trial_period = isTrialPeriod(timestamp);
                if (!is_trial_period || 
                    std::abs(auction_metrics.cumulative_net_flow) > 2000000 || 
                    std::abs(auction_metrics.cumulative_price_change) > 0.08) {
                    if (global_logger) {
                        global_logger->warnWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                 + "异动" + metrics.volatility_level + ": " 
                                 + stock_mapper_.getStockDisplayName(symbol) 
                                 + " - " + reason + "  涨幅：" 
                                 + std::to_string(metrics.auction_metrics.price_change * 100) + "%",timestamp);
                    }
                }
            }
            
            addToVolatilePool(symbol, reason);
        } else if (std::find(volatile_stocks_.begin(), volatile_stocks_.end(), symbol) != volatile_stocks_.end()) {
            // 检查是否仍然异动
            if (volatility_score < thresholds_.min_volatility_score * 0.7) {
                removeFromVolatilePool(symbol, "异动减弱");
            }
        }
    }
    
    std::string getVolatilityReason(const AuctionMetrics& metrics, double score) {
        std::vector<std::string> reasons;
        
        // 优先显示累计值
        if (std::abs(metrics.cumulative_net_flow) >= thresholds_.cumulative_net_flow) {
            std::string direction = metrics.cumulative_net_flow > 0 ? "累计流入" : "累计流出";
            char buffer[100];
            snprintf(buffer, sizeof(buffer), "累计%.1f万元%s", 
                    std::abs(metrics.cumulative_net_flow) / 10000, direction.c_str());
            reasons.push_back(buffer);
        }
        
        if (std::abs(metrics.cumulative_price_change) >= thresholds_.cumulative_price_change) {
            std::string direction = metrics.cumulative_price_change > 0 ? "累计上涨" : "累计下跌";
            char buffer[100];
            snprintf(buffer, sizeof(buffer), "%.2f%%%s", 
                    std::abs(metrics.cumulative_price_change) * 100, direction.c_str());
            reasons.push_back(buffer);
        }
        
        // 其次显示当前值
        if (std::abs(metrics.price_change) >= thresholds_.price_change) {
            std::string direction = metrics.price_change > 0 ? "上涨" : "下跌";
            char buffer[100];
            snprintf(buffer, sizeof(buffer), "%.2f%%%s", 
                    std::abs(metrics.price_change) * 100, direction.c_str());
            reasons.push_back(buffer);
        }
        
        if (metrics.match_volume_ratio >= thresholds_.match_volume_ratio) {
            char buffer[100];
            snprintf(buffer, sizeof(buffer), "匹配量%.1f倍", metrics.match_volume_ratio);
            reasons.push_back(buffer);
        }
        
        if (std::abs(metrics.net_large_order_flow) >= thresholds_.net_large_order_flow) {
            std::string direction = metrics.net_large_order_flow > 0 ? "流入" : "流出";
            char buffer[100];
            snprintf(buffer, sizeof(buffer), "大单%.1f万元%s", 
                    std::abs(metrics.net_large_order_flow) / 10000, direction.c_str());
            reasons.push_back(buffer);
        }
        
        if (std::abs(metrics.withdrawal_impact) >= thresholds_.net_large_order_flow) {
            std::string direction = metrics.withdrawal_impact > 0 ? "净撤单" : "净撤买";
            char buffer[100];
            snprintf(buffer, sizeof(buffer), "撤单%.1f万元%s", 
                    std::abs(metrics.withdrawal_impact) / 10000, direction.c_str());
            reasons.push_back(buffer);
        }
        
        char score_buffer[50];
        snprintf(score_buffer, sizeof(score_buffer), "异动分数%.1f: ", score);
        std::string result = score_buffer;
        
        for (size_t i = 0; i < reasons.size(); ++i) {
            if (i > 0) result += ", ";
            result += reasons[i];
        }
        
        return result;
    }
    
    void checkKeyTimepoints(long long timestamp) {
        std::string current_time = TimeUtils::formatTimestamp(timestamp).substr(11,8);
        int cut=std::stoi(current_time.substr(3, 2)) * 60 + std::stoi(current_time.substr(6, 2));
        
        // std::cout<<"当前时间："<<cut<<" min:"<< current_time.substr(3, 2)<<",sec: "<<current_time.substr(6, 2)
        // <<std::endl;
        // if (!last_summary_time_.empty())
        // {
        //    int lat=std::stoi(last_summary_time_.substr(3, 2)) * 60 + std::stoi(last_summary_time_.substr(6, 2));
        //    std::cout<<"| 最后报告时间"<<lat<<" sub:"<<last_summary_time_.substr(3, 2)<<last_summary_time_.substr(6, 2)<<std::endl;
        // }
        // std::cout<<"检查是否已经输出过总结"<<std::endl;
        // 检查是否已经输出过总结
        if (!last_summary_time_.empty() && 
            std::abs(
            (std::stoi(current_time.substr(3, 2)) * 60 + std::stoi(current_time.substr(6, 2))) -
            (std::stoi(last_summary_time_.substr(3, 2)) * 60 + std::stoi(last_summary_time_.substr(6, 2))))< 10
            ) {

            return;
        }
      
        // 试盘结束时间（9:20）
        if (current_time > "09:20:00" && current_time <= "09:20:09") {
            report_time++;
            if(report_time>2500){
                // std::cout<<"试盘结束时间（9"<<std::endl;
                generateEnhancedAuctionReport("试盘结束总结 " , timestamp);
                last_summary_time_ = current_time;
            }
            report_time=0;
        }
        // 竞价接近结束时间（9:24）
        else if (current_time >= "09:24:00" && current_time <= "09:24:09") {
            report_time++;
            if(report_time>2500){
                // std::cout<<"竞价接近结束时间（9"<<std::endl;
                generateEnhancedAuctionReport("竞价接近结束总结 " , timestamp);
                last_summary_time_ = current_time;
            }
            report_time=0;
        
        }
        // 竞价结束时间（9:25）
        else if (current_time == "09:25:00" && current_time <= "09:25:09") {
            report_time++;
            if(report_time>2500){
                // std::cout<<"竞价结束时间（9"<<std::endl;
                generateEnhancedAuctionReport("竞价结束总结 ", timestamp);
                last_summary_time_ = current_time;
                // // 竞价结束清理数据
                // cleanupAfterAuction();
            }
            report_time=0;
        }

    }
    
    void addToVolatilePool(const std::string& symbol, const std::string& reason) {
        if (std::find(volatile_stocks_.begin(), volatile_stocks_.end(), symbol) == volatile_stocks_.end()) {
            // 限制异动池大小
            if (volatile_stocks_.size() >= 2000) {
                // 移除分数最低的股票
                double min_score = std::numeric_limits<double>::max();
                auto min_it = volatile_stocks_.end();
                
                for (auto it = volatile_stocks_.begin(); it != volatile_stocks_.end(); ++it) {
                    if (stock_auction_metrics_[*it].volatility_score < min_score) {
                        min_score = stock_auction_metrics_[*it].volatility_score;
                        min_it = it;
                    }
                }
                
                if (min_it != volatile_stocks_.end()) {
                    removeFromVolatilePool(*min_it, "异动池已满");
                }
            }
            
            volatile_stocks_.push_back(symbol);
            stock_auction_metrics_[symbol].added_time = TimeUtils::getCurrentTimestamp();
        }
    }
    
    void removeFromVolatilePool(const std::string& symbol, const std::string& reason) {
        auto it = std::find(volatile_stocks_.begin(), volatile_stocks_.end(), symbol);
        if (it != volatile_stocks_.end()) {
            volatile_stocks_.erase(it);
        }
    }
    
    void cleanupAfterAuction() {
        volatile_stocks_.clear();
        stock_auction_metrics_.clear();
        accumulation_stocks_.clear();
        limit_up_stocks_.clear();
        post_20_data_.clear();
        last_summary_time_.clear();
        if (global_logger) {
            global_logger->info("竞价结束，清理异动检测数据");
        }
    }
};



// ==================== 基于多线程版本修复的单线程RabbitMQ消费者 ====================

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
                // 忽略析构函数中的异常
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
                    amqp_destroy_envelope(&envelope);
                    return false;
                }
                
                messages.emplace_back(std::move(body), envelope.delivery_tag);
                
                // 安全地销毁envelope
                amqp_destroy_envelope(&envelope);
                
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
        }
    }
    
    void rejectMessage(uint64_t delivery_tag, bool requeue) override {
        if (conn_) {
            amqp_basic_reject(conn_, 1, delivery_tag, requeue);
        }
    }
    
    void stop() override {
        running_ = false;
    }
};

// ==================== 增强的单线程处理器 ====================

class EnhancedSingleThreadedProcessor {
private:
    const Config& config_;
    std::unique_ptr<IDataWriter> writer_;
    std::unique_ptr<IMessageProcessor> message_processor_;
    std::unique_ptr<IMessageConsumer> consumer_;
    std::atomic<bool> running_{false};
    
    // 新增组件
    std::unique_ptr<TickAnalysisEngine> tick_engine_;
    std::unique_ptr<AuctionAnalyzer> auction_analyzer_;
    std::unique_ptr<Logger> logger_;
    std::unique_ptr<RedisClient> redis_;
    StockNameMapper& stock_mapper_;
    
    // 异动检测相关数据结构
    struct StockHistory {
        std::deque<StockData> ticks;
        StockData last_tick;
        bool is_limit_up = false;
        bool is_limit_down = false;
        long long last_limit_time = 0;
        double cumulative_large_net = 0.0;
    };
    
    std::unordered_map<std::string, StockHistory> stock_history_;
    std::mutex data_mutex_;
    
    // 服务器时间延迟统计
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
        
        // 初始化新增组件
        tick_engine_ = std::make_unique<TickAnalysisEngine>(config_);
        auction_analyzer_ = std::make_unique<AuctionAnalyzer>();
        logger_ = std::make_unique<Logger>(config.log_file_path, config.log_level, config.enable_file_log);
        redis_ = std::make_unique<RedisClient>(config.redis_host, config.redis_port, config.redis_db);
        
        
        // 设置全局日志 - 修复：使用原始指针赋值
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
        logger_->info("Processing " + std::to_string(config_.messages_per_batch) + " messages per batch");
        
        int total_messages = 0;
        int total_records = 0;
        int failed_messages = 0;
        int empty_cycles = 0;
        
        auto last_cleanup = std::chrono::steady_clock::now();
        auto last_report = std::chrono::steady_clock::now();
        
        while (running_) {
            // 获取单条消息
            std::vector<PendingMessage> messages;
            
            // 每条消息休息指定时间
            if (config_.enable_rate_limiting) {
                std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing_delay_ms));
            }
            
            bool has_messages = consumer_->consumeMessages(messages, config_.messages_per_batch);
            
            if (!has_messages) {
                // 没有消息，等待一段时间再重试
                empty_cycles++;
                if (empty_cycles % 100 == 0) {
                    logger_->debug("Waiting for messages... (" + std::to_string(empty_cycles) + " empty cycles)");
                }
                std::this_thread::sleep_for(std::chrono::seconds(10));
                continue;
            }
            
            empty_cycles = 0; // 重置空循环计数
            
            // 处理单条消息
            std::vector<StockData> all_records;
            std::vector<PendingMessage> valid_messages;
            
            for (auto& message : messages) {
                std::vector<StockData> records;
                if (message_processor_->processMessage(message.data, records)) {
                    // 处理扩展字段和异动检测
                    processEnhancedFields(records);
                    
                    all_records.insert(all_records.end(), 
                                     std::make_move_iterator(records.begin()),
                                     std::make_move_iterator(records.end()));
                    valid_messages.push_back(std::move(message));
                } else {
                    failed_messages++;
                    logger_->error("Message processing failed, delivery_tag: " + std::to_string(message.delivery_tag));
                    consumer_->rejectMessage(message.delivery_tag, true);
                }
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
                        logger_->warn("Write failed, retry " + std::to_string(retry_count) + 
                                     "/" + std::to_string(config_.max_retry_count));
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(config_.retry_delay_ms));
                    }
                }
                
                if (write_success) {
                    // 存储成功，确认消息
                    for (auto& message : valid_messages) {
                        consumer_->ackMessage(message.delivery_tag);
                    }
                    
                    total_messages += valid_messages.size();
                    total_records += all_records.size();
                    
                } else {
                    // 存储失败，退回消息
                    logger_->error("Write failed after " + std::to_string(config_.max_retry_count) + 
                                  " retries, rejecting messages");
                    for (auto& message : valid_messages) {
                        consumer_->rejectMessage(message.delivery_tag, true);
                    }
                    failed_messages += valid_messages.size();
                }
            }
            
            auto now = std::chrono::steady_clock::now();
            
            // 定期清理
            if (std::chrono::duration_cast<std::chrono::minutes>(now - last_cleanup).count() >= 10) {
                cleanupOldData();
                last_cleanup = now;
            }
            
            // 进度报告
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= config_.report_time) {
                // 获取延迟统计
                long long current_delay = getServerDelay();
                
                logger_->warn("处理: " + std::to_string(total_messages) + " 条消息, " +
                             std::to_string(total_records) + " 条记录, " +
                             std::to_string(failed_messages) + " 条失败，, " +
                             std::to_string(total_messages/config_.report_time) + " msg/s, " +
                            //  "延迟: " + (int(current_delay*0.001)>60*60*12?"historical":std::to_string(int(current_delay*0.001))) + "s");
                             "延迟: " + std::to_string(int(current_delay*0.001)) + "s");
                // 重置统计
                last_report = now;
                total_messages = 0;
                total_records = 0;
                failed_messages = 0;
                resetDelayStats();
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
    void processEnhancedFields(std::vector<StockData>& records) {
        for (auto& record : records) {
            if (record.close <= 0 || record.last_price > 1600) continue;
            if (record.ask_volumes[0] == 0 && record.bid_volumes[0] == 0) continue;
            
            // 更新服务器时间戳
            updateServerTimestamp(record.timestamp);
            
            std::string time_str = TimeUtils::formatTimestamp(record.timestamp).substr(11, 8);
            bool is_auction = TimeUtils::isAuctionTime(time_str);
            
            // 使用Tick分析引擎处理扩展字段
            tick_engine_->processTickData(record, is_auction);
            
            // 更新历史数据
            updateStockHistory(record);
            
            // 异动检测
            detectVolatility(record, is_auction);
            
            // 如果是竞价阶段，使用竞价分析器
            if (is_auction) {
                auction_analyzer_->processTickData(record);
                
                // // 生成竞价报告
                // if (time_str == "09:20:00" || time_str == "09:24:00" || time_str == "09:25:00") {
                //     auction_analyzer_->generateEnhancedAuctionReport(time_str, record.timestamp);
                // }
            }
        }
    }
    
    void detectVolatility(const StockData& data, bool is_auction_period) {
        if (!redis_->connect()) return;
        
        // 获取时间窗口统计数据
        TimeWindowStats stats = calculateTimeWindowStats(data);
        
        bool is_volatile = false;
        std::string reason;
        double strength = 0.0;
        // 涨幅
        double price_change = std::abs(data.last_price - data.close) / data.close;
        // 涨停检测
        if (checkLimitUp(data) && data.inst_amt>300*10000) {
            is_volatile = true;
            reason = "Top";
            strength = 10.0;
            if(price_change<0) reason = "Low";
        }

        //成交金额>100w 1分钟涨幅>1 5分钟涨幅>3 5分钟成交量>500w  量比>
        if (data.inst_amt>100*10000 && stats.amount_5min>1000*10000 && stats.change_1min>0.01)
        {
            is_volatile = true;
            reason = "Amount";
            strength = int(stats.change_1min*100);
        }
        if (std::abs(price_change)>0.02 && stats.change_1min>0.01 &&stats.change_5min>0.03)
        {
            if (!is_volatile){//} || isSignificantChange(data.symbol, "price_change", price_change)) {
                is_volatile = true;
                reason = "Price";
                strength = int((stats.change_5min+price_change)*50);
            }
        }
        // // 大单净额异动
        // if (std::abs(stats.large_net_5min) > config_.large_order_threshold * 10) {
        //     is_volatile = true;
        //     reason = "large_order_surge";
        //     strength = std::abs(stats.large_net_5min) / config_.large_order_threshold;
        // }
        
        // // 价格异动
        // if (std::abs(stats.change_5min) > 0.05) { // 5分钟涨跌幅超过5%
        //     is_volatile = true;
        //     reason = "price_surge";
        //     strength = std::abs(stats.change_5min) * 100;
        // }
        
        // // 成交额异动
        // if (stats.amount_5min > config_.min_amount_threshold * 5) {
        //     is_volatile = true;
        //     reason = "amount_surge";
        //     strength = stats.amount_5min / config_.min_amount_threshold;
        // }
        
        if (is_volatile) {
            logVolatility(data, reason, strength, stats,std::to_string(int(stats.change_1min*100)));
            storeVolatilityToRedis(data, reason, strength, stats);
        }
    }
    
    bool checkLimitUp(const StockData& data) {
        double limit_up_price = std::round(data.close * 1.1 * 100) / 100;
        double limit_down_price = std::round(data.close * 0.9 * 100) / 100;
        std::string symbol_prefix = data.symbol.substr(0, 2);
        if (symbol_prefix == "30" || symbol_prefix == "68") {
            limit_up_price = std::round(data.close * 1.2 * 100) / 100;
            limit_down_price = std::round(data.close * 0.8 * 100) / 100;
        }
        
        bool is_limit_up = std::abs(data.last_price - limit_up_price) < 0.01;
        bool is_limit_down = std::abs(data.last_price - limit_down_price) < 0.01;
        if (is_limit_up) {
            std::lock_guard<std::mutex> lock(data_mutex_);
            auto& history = stock_history_[data.symbol];
            if (!history.is_limit_up) {
                history.is_limit_up = true;
                history.last_limit_time = data.timestamp;
                
                // 计算封单金额
                double bid_amount = (data.bid_volumes[0] + data.bid_volumes[1]) * data.last_price * 100;
                
                logger_->infoWithTickTime("涨停|" + stock_mapper_.getStockDisplayName(data.symbol) + 
                    "|价格:" + std::to_string(data.last_price) + "|封单金额:" +
                             std::to_string(int(bid_amount / 10000))+ "万元|瞬时成交额: "+
                             std::to_string(int(data.inst_amt*0.0001))+"万",
                             data.timestamp);
            }
        }else if (is_limit_down)
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            auto& history = stock_history_[data.symbol];
            if (!history.is_limit_down) {
                history.is_limit_down = true;
                history.last_limit_time = data.timestamp;
                
                // 计算封单金额
                double ask_amount = (data.ask_volumes[0] + data.ask_volumes[1]) * data.last_price * 100;
                
                logger_->infoWithTickTime("跌停|" + stock_mapper_.getStockDisplayName(data.symbol) + 
                    "|价格:" + std::to_string(data.last_price) + "|封单金额:" +
                             std::to_string(int(ask_amount / 10000))+ "万元|瞬时成交额: "+
                             std::to_string(int(data.inst_amt*0.0001))+"万",
                             data.timestamp);
            }
        }
        
        
        return is_limit_up;
    }
    std::string getGradientColor(float value) {
       // 归一化到[-1,1]范围（对应-10%到+10%）
        float normalized = std::clamp(value / 10.0f, -1.0f, 1.0f);
    
        if (normalized < 0) {
            // 绿色区间：22(深绿) → 46(亮绿)
            int green_id = 22 + static_cast<int>(24 * (1 + normalized)); // -1→0 → 22→46 
            return "\033[38;5;" + std::to_string(green_id) + "m";
        } else {
            // 红黄区间：203(浅红) → 226(亮黄)
            int redyellow_id = 203 + static_cast<int>(23 * normalized); // 0→1 → 203→226 
            return "\033[38;5;" + std::to_string(redyellow_id) + "m";
        }
    }
    void logVolatility(const StockData& data, std::string& reason, 
                      double strength, const TimeWindowStats& stats, const std::string& change) {
        std::string display_name = stock_mapper_.getStockDisplayName(data.symbol);
         auto& log_msg = StringBuffer::getStream();
        if(reason=="Top")
        {
            reason= "\033[31m" + reason+"|" + display_name+ "\033[0m";
        }else if (reason=="Low")
        {
            reason= "\033[32m" + reason+"|" + display_name+ "\033[0m";
        }else{
            reason=reason +"|" + display_name ;
        }
        float price_change=(std::round((data.last_price - data.close) / data.close*10000) * 0.01);
        std::string display;
        display = "|涨幅:"+ std::to_string(price_change) + "%"
                + "|瞬时:" + std::to_string(int(data.inst_amt*0.0001))+"万"
                + "|1分速:" + change+"%";
        // price_change = price_change*0.09;
        
        if (price_change > 10) price_change=price_change*0.49;
        display = getGradientColor(price_change)+display+"\033[0m";

        // std::ostringstream log_msg;
        log_msg << "异动|" << reason 
                << "|价格:" << data.last_price << display
                // << "|涨幅:" << (std::round((data.last_price - data.close) / data.close*10000) * 0.01) << "%"
                // << "|瞬时:" <<std::to_string(int(data.inst_amt*0.0001))+"万"
                // << "|1分速:" << change<< "%"
                << "|1分净额:" << std::to_string(int(stats.amount_1min / 10000)) << "万"
                << "|5分净额:" << std::to_string(int(stats.large_net_5min / 10000)) << "万"
                << "|5分金额:" << std::to_string(int(stats.amount_5min / 10000)) << "万"
                << "|强度:" << strength;
        
        logger_->warnWithTickTime(log_msg.str(), data.timestamp);
    }
    
    // 修复：添加strength参数
    void storeVolatilityToRedis(const StockData& data, const std::string& reason, 
                               double strength, const TimeWindowStats& stats) {
        // auto& buffer = StringBuffer::getString();
        char buffer[2048];
        int len = snprintf(buffer, sizeof(buffer), 
            "{\"symbol\":\"%s\",\"timestamp\":%lld,\"price\":%.2f,"
            "\"reason\":\"%s\",\"strength\":%.2f,"
            "\"large_net_5min\":%.2f,\"change_5min\":%.4f,\"amount_5min\":%.2f}",
            data.symbol.c_str(), data.timestamp, data.last_price,
            reason.c_str(), strength,
            stats.large_net_5min, stats.change_5min, stats.amount_5min);
        
        if (len > 0 && len < static_cast<int>(sizeof(buffer))) {
            redis_->zadd(config_.volatile_pool_key, data.timestamp, std::string(buffer, len));
            redis_->expire(config_.volatile_pool_key, config_.volatile_expire);
        }
    }
    
    TimeWindowStats calculateTimeWindowStats(const StockData& current_data) {
        TimeWindowStats stats;
        std::lock_guard<std::mutex> lock(data_mutex_);
        
        auto it = stock_history_.find(current_data.symbol);
        if (it == stock_history_.end()) {
            return stats;
        }
        
        const auto& history = it->second;
        long long current_time = current_data.timestamp;
        
        // 计算1分钟和5分钟前的时间点
        long long time_1min_ago = current_time - config_.minute1_window_ms;
        long long time_5min_ago = current_time - config_.minute5_window_ms;
        
        // 查找时间窗口内的起始tick
        const StockData* tick_1min_ago = findTickAtTime(history.ticks, time_1min_ago);
        const StockData* tick_5min_ago = findTickAtTime(history.ticks, time_5min_ago);
        
        // 计算1分钟窗口统计数据
        if (tick_1min_ago) {
            stats.volume_1min = current_data.volume - tick_1min_ago->volume;
            stats.change_1min = (current_data.last_price - tick_1min_ago->last_price) / tick_1min_ago->last_price;
            stats.amount_1min = current_data.amount - tick_1min_ago->amount;
            stats.large_net_1min = tick_engine_->getCumulativeLargeNet(current_data.symbol) - 
                                  getCumulativeLargeNetAtTime(current_data.symbol, time_1min_ago);
        }
        
        // 计算5分钟窗口统计数据
        if (tick_5min_ago) {
            stats.volume_5min = current_data.volume - tick_5min_ago->volume;
            stats.change_5min = (current_data.last_price - tick_5min_ago->last_price) / tick_5min_ago->last_price;
            stats.amount_5min = current_data.amount - tick_5min_ago->amount;
            stats.large_net_5min = tick_engine_->getCumulativeLargeNet(current_data.symbol) - 
                                  getCumulativeLargeNetAtTime(current_data.symbol, time_5min_ago);
        }
        
        return stats;
    }
    
    double getCumulativeLargeNetAtTime(const std::string& symbol, long long timestamp) {
        // 简化实现，实际应该根据时间点计算
        return tick_engine_->getCumulativeLargeNet(symbol) * 0.8; // 估算值
    }
    
    void updateStockHistory(const StockData& data) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        auto& history = stock_history_[data.symbol];
        
        // 更新上一个tick
        history.last_tick = data;
        
        // 添加当前tick到历史队列
        history.ticks.push_back(data);
        
        // 限制历史数据大小
        if (history.ticks.size() > config_.max_history_ticks) {
            history.ticks.pop_front();
        }
        
        // 清理过时的tick数据
        cleanupOldTicks(history.ticks, data.timestamp);
    }
    
    const StockData* findTickAtTime(const std::deque<StockData>& ticks, long long target_time) {
        if (ticks.empty()) return nullptr;
        
        for (auto it = ticks.rbegin(); it != ticks.rend(); ++it) {
            if (it->timestamp <= target_time) {
                return &(*it);
            }
        }
        
        return &ticks.front();
    }
    
    void cleanupOldTicks(std::deque<StockData>& ticks, long long current_time) {
        long long cutoff_time = current_time - config_.minute5_window_ms - 60000;
        while (!ticks.empty() && ticks.front().timestamp < cutoff_time) {
            ticks.pop_front();
        }
    }
    
    void cleanupOldData() {
        if (!redis_->connect()) return;
        
        long long cutoff_time = TimeUtils::getCurrentTimestamp() - 3600000;
        redis_->zremrangebyscore(config_.volatile_pool_key, 0, cutoff_time);
        
        // 清理过时的历史数据
        cleanupOldHistoryData();
        tick_engine_->cleanupOldData(TimeUtils::getCurrentTimestamp());
        auction_analyzer_->cleanupOldData();
    }
    
    void cleanupOldHistoryData() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long cutoff_time = current_time - 3600000; // 1小时
        
        auto it = stock_history_.begin();
        while (it != stock_history_.end()) {
            if (it->second.last_tick.timestamp < cutoff_time) {
                it = stock_history_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    void updateServerTimestamp(long long timestamp) {
        long long current_max = max_server_timestamp_.load();
        if (timestamp > current_max) {
            max_server_timestamp_.store(timestamp);
            long long current_time = TimeUtils::getCurrentTimestamp();
            long long delay = current_time - timestamp;
            total_delay_.fetch_add(delay);
            delay_count_.fetch_add(1);
        }
    }
    
    long long getServerDelay() const {
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long max_server_time = max_server_timestamp_.load();
        return (max_server_time > 0) ? current_time - max_server_time : 0;
    }
    
    void resetDelayStats() {
        total_delay_.store(0);
        delay_count_.store(0);
    }
};

// ==================== 工厂类 ====================

class ComponentFactory {
public:
    virtual ~ComponentFactory() = default;
    virtual std::unique_ptr<IDataWriter> createDataWriter() = 0;
    virtual std::unique_ptr<IMessageProcessor> createMessageProcessor() = 0;
    virtual std::unique_ptr<IMessageConsumer> createMessageConsumer() = 0;
    virtual std::unique_ptr<EnhancedSingleThreadedProcessor> createEnhancedProcessor() = 0;
};

class EnhancedStockDataFactory : public ComponentFactory {
private:
    const Config& config_;
    
public:
    EnhancedStockDataFactory(const Config& config) : config_(config) {}
    
    std::unique_ptr<IDataWriter> createDataWriter() override {
        return std::make_unique<OptimizedTDengineWriter>(config_);
    }
    
    std::unique_ptr<IMessageProcessor> createMessageProcessor() override {
        return std::make_unique<EfficientMessageProcessor>(config_);
    }
    
    std::unique_ptr<IMessageConsumer> createMessageConsumer() override {
        return std::make_unique<FixedRabbitMQConsumer>(config_);
    }
    
    std::unique_ptr<EnhancedSingleThreadedProcessor> createEnhancedProcessor() override {
        return std::make_unique<EnhancedSingleThreadedProcessor>(
            createDataWriter(),
            createMessageProcessor(),
            createMessageConsumer(),
            config_
        );
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
        auto factory = std::make_unique<EnhancedStockDataFactory>(config);
        processor_ = factory->createEnhancedProcessor();
        
        setupSignalHandlers();
    }
    
    void run() {
        std::cout << "Starting enhanced single-threaded application..." << std::endl;
        std::cout << "Messages per batch: " << config_.messages_per_batch << std::endl;
        
        // 使用后台线程
        std::thread processor_thread([this]() {
            processor_->start();
        });
        
        waitForShutdown();
        
        shutdown();
        
        // 等待处理器线程结束
        if (processor_thread.joinable()) {
            processor_thread.join();
        }
        
        std::cout << "Application stopped" << std::endl;
    }
    
private:
    static void signalHandler(int signal) {
        std::cout << "\nReceived signal " << signal << std::endl;
        signal_received_ = true;
    }
    
    void setupSignalHandlers() {
        signal_received_ = false;
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);
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