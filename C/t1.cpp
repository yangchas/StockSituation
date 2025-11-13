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

// 网络请求相关头文件
#include <curl/curl.h>
// #include <nlohmann/json.hpp>

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
    int volatile_expire = 300;//redis过期时间
    // 时间窗口配置
    int minute1_window_ms = 60000; // 1分钟窗口
    int minute5_window_ms = 300000; // 5分钟窗口
    int max_history_ticks = 20*6; //6分钟 最大历史tick数量 (优化: 减小到60)
   
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
    // int max_pending_messages = 50; // 最大待处理消息数
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
   
    static const int HISTORY_SIZE = 20*15; // 优化: 减小到10
   
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
    virtual bool detectVolatility(const StockData& data, double change, double bid_amount,double ask_amount) = 0; // 优化: 传入统一计算的指标
    // virtual void cleanVolOldData() = 0;
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
        return time_str >= "09:15:00" && time_str <= "09:26:00";
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
       
        std::string log_timestamp = TimeUtils::formatTimestamp(timestamp).substr(11);//5
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

// ==================== 网络请求工具类 ====================
class HttpRequest {
private:
    static size_t writeCallback(void* contents, size_t size, size_t nmemb, std::string* response) {
        size_t total_size = size * nmemb;
        response->append(static_cast<char*>(contents), total_size);
        return total_size;
    }

public:
    static std::string get(const std::string& url, const std::unordered_map<std::string, std::string>& params = {}) {
        CURL* curl = curl_easy_init();
        if (!curl) return "";

        std::string full_url = url;
        if (!params.empty()) {
            full_url += "?";
            for (const auto& param : params) {
                full_url += param.first + "=" + param.second + "&";
            }
            full_url.pop_back();
        }

        std::string response_data;
        
        // 设置请求头
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded; charset=UTF-8");
        headers = curl_slist_append(headers, "User-Agent: Dalvik/2.1.0 (Linux; U; Android 5.1.1; VOG-AL10 Build/HUAWEIVOG-AL10)");
        headers = curl_slist_append(headers, "Connection: close");
        // headers = curl_slist_append(headers, "Accept-Encoding: gzip");
        
        curl_easy_setopt(curl, CURLOPT_URL, full_url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_data);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L); // 允许重定向

        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(headers); // 释放请求头
        curl_easy_cleanup(curl);
        // std::cout<<" 返回："<<res<<"请求："<<full_url<<std::endl;
        return (res == CURLE_OK) ? response_data : "";
    }
};

// ==================== 指标获取器 ====================
class IndicatorProvider {
private:
    std::string device_id_;
    std::string token_;
    std::string user_id_;

public:
    IndicatorProvider() 
        : device_id_("09802805ff9b57f33c8fd80bbfc53e40"),
          token_("830f48080d3b12e2dc1bdbd610b14601"),
          user_id_("1619653") {}

    // 获取个股DDE
    std::string getStockDDE(const std::string& stock_id = "300366") {
        std::unordered_map<std::string, std::string> params = {
            {"a", "GetKLineTodayDaDanNew"}, {"c", "StockLineData"}, {"PhoneOSNew", "1"},
            {"DeviceID", device_id_}, {"VerSion", "5.16.0.0"}, {"Token", token_},
            {"apiv", "w38"}, {"Type", "d"}, {"StockID", stock_id}, {"UserID", user_id_}
        };
        return HttpRequest::get("https://apphwhq.longhuvip.com/w1/api/index.php", params);
    }

    // 获取其他指标的方法可以在这里添加
    // std::string getOtherIndicator(const std::string& stock_id) { ... }
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
        std::deque<SimpleTickData> history;
        double cumulative_large_net = 0.0;
        // bool has_previous = false;
        long long last_update = 0;
    };
   
    std::unordered_map<std::string, StockTickState> stock_states_;
    std::mutex state_mutex_;
    double large_order_threshold_;
    const Config& config_;
public:
    TickAnalysisEngine(const Config& config, double threshold = 500000)
        : large_order_threshold_(threshold), config_(config)  {}
   
    void processTickData(StockData& current_tick, bool is_auction_period) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = stock_states_[current_tick.symbol];
       
        if (!state.history.empty()) {
            const auto& prev_tick = state.history.back();
            current_tick.inst_vol = current_tick.volume - prev_tick.volume;
            current_tick.inst_amt = current_tick.amount - prev_tick.amount;
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
       
        // state.prev_tick = SimpleTickData::fromStockData(current_tick);
        // 添加当前 tick 到 history
        state.history.push_back(SimpleTickData::fromStockData(current_tick));
        // state.has_previous = true;
        if (state.history.size() > config_.max_history_ticks) {
            state.history.pop_front();  // 删除最旧的，保持 <= 40
            state.history.shrink_to_fit(); 
        }
        state.last_update = current_tick.timestamp;
    }
   
    // 获取累计大单净额 后面需要接入API
    double getCumulativeLargeNet(const std::string& symbol) {
        // std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = stock_states_.find(symbol);
        if (it != stock_states_.end()) {
            return it->second.cumulative_large_net;
        }
        return 0.0;
    }
   
    // void cleanTickOldData() {
    //     if (stock_states_.empty()) {
    //         return;
    //     }
        
    //     long long max_timestamp = 0;
    //     long long min_timestamp = 0;
    //     int count_stock=0;
    //     for (const auto& pair : stock_states_) {
    //         if (pair.second.last_update > max_timestamp) {
    //             max_timestamp = pair.second.last_update;
    //         }
    //          if (pair.second.last_update < min_timestamp) {
    //             min_timestamp = pair.second.last_update;
    //         }
    //     }
        
    //     long long cutoff_time = max_timestamp - 1* 60 * 1000;
        
    //     auto it = stock_states_.begin();
    //     int count=0;
    //     while (it != stock_states_.end()) {
    //         count_stock+=1;
    //         if (it->second.last_update < cutoff_time) {
    //             count+=1;
    //             it = stock_states_.erase(it);
    //         } else {
    //             ++it;
    //         }
    //     }
    //     std::cout<<" cleanTickOldData 删除: "<<count<<" 最前:"<<min_timestamp<<" |最后"<<max_timestamp<<"||总共："<<count_stock<<std::endl;
    // }
private:
    void calculateAuctionLargeOrder(StockData& current_tick, StockTickState& state) {
        const auto& prev_tick = state.history.back();
        double delta_bid = current_tick.bid_volumes[0] + current_tick.bid_volumes[1] - prev_tick.volume; // 简例，实际调整
        double instant_amount = delta_bid * current_tick.last_price * 100;
        if (std::abs(instant_amount) > large_order_threshold_) {
            current_tick.large_net = (delta_bid > 0) ? instant_amount : -instant_amount;
            state.cumulative_large_net += current_tick.large_net;
        } else {
            current_tick.large_net = 0;
        }
    }
   
    void calculateTradeLargeOrder(StockData& current_tick, StockTickState& state) {
        const auto& prev_tick = state.history.back();
        double instant_amount = current_tick.inst_amt;
        if (std::abs(instant_amount) > large_order_threshold_) {
            current_tick.large_net = (current_tick.last_price > prev_tick.last_price) ? instant_amount : -instant_amount;
            state.cumulative_large_net += current_tick.large_net;
        } else {
            current_tick.large_net = 0;
        }
//         |德明利(001309)|Top|封单:3516|价格:247.14|10%|瞬时:13624万|1分速:0%|1分净额:15300万|5分净额:-15574万|5分金额:16306万|强度:10
// 13:09:24[WARN]异动|德明利(001309)|Top|封单:711|价格:247.14|10%|瞬时:2791万|1分速:0%|1分净额:18060万|5分净额:-18366万|5分金额:19093万|强度:10

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
        // body.clear();
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
        buffer.clear();
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
        double min_volatility_score = 15;  // 降低阈值
        double cumulative_net_flow = 1000000;
        double cumulative_price_change = 0.05;
        double bid_amount_large = 5000000;
        double accumulation_bid_increase = 5000000;
        double accumulation_price_increase = 0.01;
    };
    
    struct VolatilityLevels {
        double low = 30;
        double medium = 50;
        double high = 80;
    };
    
    AuctionThresholds thresholds_;
    VolatilityLevels volatility_levels_;
    
    std::unordered_map<std::string, StockAuctionMetrics> stock_auction_metrics_;
    std::unordered_map<std::string, std::vector<AccumulationDataPoint>> post_20_data_;
    
    std::vector<std::string> volatile_stocks_;
    std::vector<std::string> accumulation_stocks_;
    std::vector<std::string> limit_up_stocks_;
    std::vector<std::string> limit_down_stocks_;
    
    MarketReport market_report_;
    StockNameMapper& stock_mapper_;
    IExternalDataProvider* external_provider_;
    std::mutex data_mutex_;
    
    const std::string TRIAL_END = "09:20:00";
    const std::string AUCTION_NEAR_END = "09:24:00";
    const std::string AUCTION_END = "09:25:00";
    std::string last_summary_time_;
    static int report_time;
    
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
    
    void processTickData(const StockData& data, double change, double bid_amount, double ask_amount) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        const std::string& symbol = data.symbol;
        
        StockAuctionMetrics& metrics = stock_auction_metrics_[symbol];
        
        // 获取卖一量和买一量
        double av1 = data.ask_volumes[0];
        double bv1 = data.bid_volumes[0];
        
        // 更新历史数据（增加买卖量历史）
        updateHistoryData(metrics, data.last_price, av1, bv1, bid_amount);
        
        // 更新竞价指标
        updateAuctionMetrics(metrics, data, change, bid_amount, ask_amount);
        
        // 计算匹配量
        // double auction_volume = std::min(av1, bv1) * data.last_price * 0.01;
        double auction_volume = std::min(bid_amount, ask_amount);
        metrics.auction_volume = auction_volume;
        // std::cout<<data.symbol<<"| 匹配量："<<metrics.auction_volume<<"万 "<<av1<<" "<<bv1<<" "<<data.last_price<<std::endl;
        std::string current_time = TimeUtils::formatTimestamp(data.timestamp);
        
        if (current_time >= "09:20:00") {
            analyzeAccumulationPattern(symbol, data.timestamp, data.last_price, bid_amount, data.close);
        }
        
        // 分析撤单和大单
        analyzeOrderFlow(symbol, data, metrics, data.timestamp, change, bid_amount, ask_amount);
        
        // 每秒进行一次完整的异动分析
        if (data.timestamp - metrics.last_analysis_time > 1000) {
            analyzeAuctionVolatility(symbol, metrics, data.timestamp);
            metrics.last_analysis_time = data.timestamp;
        }
        
        // // 检查关键时间点 移到定时总结，不再每步检查
        // checkKeyTimepoints(data.timestamp);
    }
    
    void cleanAuctOldData() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long current_time = TimeUtils::getCurrentTimestamp();
        long long cutoff_time = current_time - 30*60*1000; // 30min
        
        auto it = stock_auction_metrics_.begin();
        while (it != stock_auction_metrics_.end()) {
            if (it->second.last_analysis_time < cutoff_time) {
                it = stock_auction_metrics_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // 获取开盘数据 - 通过外部接口补充真实数据并重新计算排名
    void getKaipan(){
        // std::lock_guard<std::mutex> lock(data_mutex_);
        
        // 创建指标获取器
        IndicatorProvider indicator_provider;
        // 这里需要设置实际的device_id, token, user_id
        // indicator_provider.setConfig("your_device_id", "your_token", "your_user_id");
        
        // 逐个获取活跃股票的DDE数据
        int updated_count = 0;
        for (const auto& pair : stock_auction_metrics_) {
            const std::string& symbol = pair.first;
            
            // 单个获取DDE数据
            std::string dde_data = indicator_provider.getStockDDE(symbol);
            // std::cout<<"获取大单 "<<symbol<<" "<<dde_data<<std::endl;
            if (!dde_data.empty()) {
                // 使用真实DDE数据更新竞价指标
                updateMetricsWithRealData(symbol, dde_data);
                updated_count++;
                
                // 添加适当延迟，避免请求过于频繁
                std::this_thread::sleep_for(std::chrono::milliseconds(60));
            }
        }
        
        if (global_logger) {
            global_logger->info("成功更新 " + std::to_string(updated_count) + " 只股票的DDE数据");
        }
        
        // // 生成基于真实数据的开盘分析报告
        // generateRealDataOpeningReport();
    }
    
    // 使用真实DDE数据更新单个股票的竞价指标
    void updateMetricsWithRealData(const std::string& symbol, const std::string& dde_data) {
        auto it = stock_auction_metrics_.find(symbol);
        if (it != stock_auction_metrics_.end()) {
            // 解析DDE数据
            double real_large_net = parseDDEData(dde_data);
            
            // 使用真实DDE数据更新竞价指标
            if (real_large_net != 0) {
                it->second.auction_metrics.net_large_order_flow = real_large_net;
                it->second.auction_metrics.cumulative_net_flow = real_large_net;
                
                if (global_logger) {
                    global_logger->info("更新股票 " + stock_mapper_.getStockDisplayName(symbol) + 
                                       " 的DDE数据: 大单净额 " + Logger::amountToWan(real_large_net) + "万元");
                }
            }
        }
    }
    
    // 解析DDE数据，提取大单净额
    double parseDDEData(const std::string& dde_data) {
        // 简单的JSON解析，提取DDJE字段
        // 数据格式示例: {"StockID":"002194","Date":["20251112"],"DDJE":[33786080],"Time":1762979994,"ttag":0.0024759999999999782,"errcode":"0"}
        
        size_t ddje_pos = dde_data.find("\"DDJE\":[");
        if (ddje_pos == std::string::npos) {
            return 0.0;
        }
        
        ddje_pos += 8; // 移动到数字开始位置
        size_t end_pos = dde_data.find("]", ddje_pos);
        if (end_pos == std::string::npos) {
            return 0.0;
        }
        
        std::string ddje_str = dde_data.substr(ddje_pos, end_pos - ddje_pos);
        try {
            double ddje_value = std::stod(ddje_str);
            return ddje_value; // 返回原始值，单位是元
        } catch (const std::exception& e) {
            return 0.0;
        }
    }
    
    // 增强的竞价报告生成（包含真实DDE数据）
    void generateEnhancedAuctionReport(const std::string& time_str, long long timestamp) {
        // std::lock_guard<std::mutex> lock(data_mutex_);
        
        // 计算市场统计
        auto summary = calculateEnhancedMarketSummary();
        
        std::ostringstream report;
        report << "====== 竞价报告 " << time_str << " (Tick时间: " 
               << TimeUtils::formatTimestamp(timestamp) << ") ======\n";
        
        // 竞价强度
        double auction_strength = summary.total_stocks > 0 ? 
            (double)summary.high_open_count / summary.total_stocks : 0;
        report << "竞价强度: " << Logger::f2s(auction_strength * 100) << "%\n";
        
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
                    // double match_amount = metrics.auction_volume * 0.01; // 估算成交金额
                    report << "  " << stock_mapper_.getStockDisplayName(symbol) 
                           << " 涨幅:" << Logger::f2s(metrics.auction_metrics.price_change * 100) << "%"
                           << " 封单:" << Logger::amountToWan(bid_amount) << "万"
                           << " 成交:" << Logger::amountToWan(metrics.auction_volume) << "万"
                           << " 大单:" << Logger::amountToWan(metrics.auction_metrics.net_large_order_flow) << "万\n";
                }
            }
        }
        
        // 跌停股票统计
        report << "跌停数量: " << summary.limit_down_stocks.size() << "\n";
        if (!summary.limit_down_stocks.empty()) {
            report << "跌停股票详情:\n";
            for (const auto& symbol : summary.limit_down_stocks) {
                auto it = stock_auction_metrics_.find(symbol);
                if (it != stock_auction_metrics_.end()) {
                    const auto& metrics = it->second;
                    double ask_amount = metrics.auction_metrics.ask_amount;
                    // double match_amount = metrics.auction_volume * 100; // 估算成交金额
                    report << "  " << stock_mapper_.getStockDisplayName(symbol) 
                           << " 跌幅:" << Logger::f2s(metrics.auction_metrics.price_change * 100) << "%"
                           << " 封单:" << Logger::amountToWan(ask_amount) << "万"
                           << " 成交:" << Logger::amountToWan(metrics.auction_volume) << "万"
                           << " 大单:" << Logger::amountToWan(metrics.auction_metrics.net_large_order_flow) << "万\n";
                }
            }
        }
        
        // 基于真实DDE数据的大单净额排名
        report << "====== 基于真实DDE数据的大单净额排名 ======\n";
        report << "大单净额排名(前20):\n";
        
        // 重新计算大单净额排名（基于真实DDE数据）
        std::vector<std::pair<std::string, double>> real_large_net_ranking;
        
        for (const auto& pair : stock_auction_metrics_) {
            const auto& symbol = pair.first;
            const auto& metrics = pair.second;
            
            // 使用真实DDE数据计算排名
            double real_net_flow = metrics.auction_metrics.net_large_order_flow;
            real_large_net_ranking.emplace_back(symbol, real_net_flow);
        }
        
        // 按大单净额排序
        std::sort(real_large_net_ranking.begin(), real_large_net_ranking.end(),
                 [](const auto& a, const auto& b) { return std::abs(a.second) > std::abs(b.second); });
        
        int count = 0;
        for (const auto& item : real_large_net_ranking) {
            if (count++ >= 20) break;
            auto it = stock_auction_metrics_.find(item.first);
            if (it != stock_auction_metrics_.end()) {
                const auto& metrics = it->second;
                report << "  " << stock_mapper_.getStockDisplayName(item.first) 
                       << " 大单净额:" << Logger::amountToWan(item.second) << "万元"
                       << " 涨幅:" << Logger::f2s(metrics.auction_metrics.price_change * 100) << "%"
                       << " 竞价强度:" << Logger::f2s(metrics.volatility_score) << "\n";
            }
        }
        
        // 成交额排名
        report << "成交额排名(前20):\n";
        count = 0;
        for (const auto& item : summary.amount_ranking) {
            if (count++ >= 20) break;
            report << "  " << stock_mapper_.getStockDisplayName(item.first) 
                   << " " << Logger::amountToWan(item.second) << "万元\n";
        }
        
        // 输出到日志
        if (global_logger) {
            global_logger->warnWithTickTime(report.str(), timestamp);
        }
    }
private:
    void updateHistoryData(StockAuctionMetrics& metrics, double price, double av1, double bv1, double bid_amount) {
        metrics.price_history.push_back(price);
        metrics.bid_amount_history.push_back(bid_amount);
        
        // 限制历史数据大小
        if (metrics.price_history.size() > StockAuctionMetrics::HISTORY_SIZE) {
            metrics.price_history.pop_front();
            metrics.bid_amount_history.pop_front();
        }
    }
    
    void updateAuctionMetrics(StockAuctionMetrics& metrics, const StockData& data, double change, double bid_amount, double ask_amount) {
        AuctionMetrics& auction_metrics = metrics.auction_metrics;
        
        auction_metrics.price_change = change;
        auction_metrics.bid_amount = bid_amount;
        auction_metrics.ask_amount = ask_amount;
        
        // 计算涨停跌停价格
        double limit_up_price = std::round(data.close * 1.1 * 100) / 100.0;
        double limit_down_price = std::round(data.close * 0.9 * 100) / 100.0;
        std::string symbol_prefix = data.symbol.substr(0, 2);
        if (symbol_prefix == "30" || symbol_prefix == "68") {
            limit_up_price = std::round(data.close * 1.2 * 100) / 100.0;
            limit_down_price = std::round(data.close * 0.8 * 100) / 100.0;
        }
        
        auction_metrics.is_limit_up = std::abs(data.last_price - limit_up_price) < 0.01;
        auction_metrics.is_limit_down = std::abs(data.last_price - limit_down_price) < 0.01;
        
        // 涨停股票记录
        if (auction_metrics.is_limit_up && 
            std::find(limit_up_stocks_.begin(), limit_up_stocks_.end(), data.symbol) == limit_up_stocks_.end() &&
            bid_amount > (data.ask_volumes[0] + data.ask_volumes[1]) * data.last_price * 100) {
            limit_up_stocks_.push_back(data.symbol);
            if (global_logger) {
                global_logger->warnWithTickTime(stock_mapper_.getStockDisplayName(data.symbol) 
                     + " 已涨停，涨停价: " + Logger::f2s(limit_up_price) 
                     + "，委买金额: " + Logger::amountToWan(bid_amount) + "万元"
                     + "，已成交:" + Logger::amountToWan(metrics.auction_volume) + "万元",
                     data.timestamp);
            }
        }
        
        // 跌停股票记录
        if (auction_metrics.is_limit_down && 
            std::find(limit_down_stocks_.begin(), limit_down_stocks_.end(), data.symbol) == limit_down_stocks_.end() &&
            bid_amount < (data.ask_volumes[0] + data.ask_volumes[1]) * data.last_price * 100) {
            limit_down_stocks_.push_back(data.symbol);
            if (global_logger) {
                global_logger->warnWithTickTime(stock_mapper_.getStockDisplayName(data.symbol) 
                     + " 已跌停，跌停价: " + Logger::f2s(limit_down_price) 
                     + "，委卖金额: " + Logger::amountToWan(ask_amount) + "万元"
                     + "，已成交:" + Logger::amountToWan(metrics.auction_volume) + "万元",
                     data.timestamp);
            }
        }
        
        // 更新累计涨跌幅
        if (metrics.price_history.size() > 1 && metrics.price_history.front() > 0) {
            double first_price = metrics.price_history.front();
            auction_metrics.cumulative_price_change = (data.last_price - first_price) / first_price;
        }
    }
    
    void analyzeAccumulationPattern(const std::string& symbol, long long timestamp,
                                   double current_price, double bid_amount, double last_close) {
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
            double price_increase = (prices.back() - prices.front()) / prices.front();
            
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
                         + "委买增加 " + Logger::amountToWan(bid_amounts.back() - bid_amounts.front()) + "万, "
                         + "价格上涨 " + Logger::f2s(price_increase * 100) + "%"
                         + "，已成交:" + Logger::amountToWan(metrics.auction_volume) + "万元",
                         timestamp);
                }
            }
        }
    }
    
    void analyzeOrderFlow(const std::string& symbol, const StockData& curr_data, 
                         StockAuctionMetrics& metrics, long long timestamp, 
                         double change, double bid_amount, double ·) {
        SimpleTickData& prev_data = metrics.prev_tick_data;
        
        // 如果是第一条数据，只存储不分析
        if (prev_data.timestamp == 0) {
            prev_data = SimpleTickData::fromStockData(curr_data);
            return;
        }
        
        bool is_trial_period = isTrialPeriod(timestamp);
        
        if (is_trial_period) {
            analyzeWithdrawals(symbol, curr_data, prev_data, metrics, timestamp);
        } else {
            analyzeLargeOrders(symbol, curr_data, prev_data, metrics, timestamp);
        }
        
        // 更新前一个tick数据
        prev_data = SimpleTickData::fromStockData(curr_data);
    }
    
    void analyzeWithdrawals(const std::string& symbol, const StockData& curr_data,
                           const SimpleTickData& prev_data, StockAuctionMetrics& metrics, 
                           long long timestamp) {
        // 检查卖单撤单：同一价格下，av1减少
        if (curr_data.ask_prices[0] == prev_data.last_price) {
            double delta_av1 = curr_data.ask_volumes[0] - (prev_data.volume / curr_data.ask_prices[0] / 100);
            if (delta_av1 < 0) {
                double threshold = getLargeOrderThreshold(curr_data.ask_prices[0]);
                if (std::abs(delta_av1) >= threshold) {
                    double withdrawal_value = std::abs(delta_av1) * curr_data.ask_prices[0] * 100;
                    metrics.auction_metrics.withdrawal_impact += withdrawal_value;
                    
                    // 只记录高级别撤单
                    if (withdrawal_value > 500000) {
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + Logger::f2s(metrics.auction_metrics.price_change * 100) + "% "
                                     + "卖单撤单: " + Logger::f2s(std::abs(delta_av1)) + "股, "
                                     + "价格: " + Logger::f2s(curr_data.ask_prices[0]) + ", "
                                     + "金额: " + Logger::amountToWan(withdrawal_value) + "万元", timestamp);
                        }
                    }
                }
            }
        }
        
        // 检查买单撤单：同一价格下，bv1减少
        if (curr_data.bid_prices[0] == prev_data.last_price) {
            double delta_bv1 = curr_data.bid_volumes[0] - (prev_data.volume / curr_data.bid_prices[0] / 100);
            if (delta_bv1 < 0) {
                double threshold = getLargeOrderThreshold(curr_data.bid_prices[0]);
                if (std::abs(delta_bv1) >= threshold) {
                    double withdrawal_value = std::abs(delta_bv1) * curr_data.bid_prices[0] * 100;
                    metrics.auction_metrics.withdrawal_impact -= withdrawal_value;
                    
                    // 只记录高级别撤单
                    if (withdrawal_value > 500000) {
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + Logger::f2s(metrics.auction_metrics.price_change * 100) + "% "
                                     + "买单撤单: " + Logger::f2s(std::abs(delta_bv1)) + "股, "
                                     + "价格: " + Logger::f2s(curr_data.bid_prices[0]) + ", "
                                     + "金额: " + Logger::amountToWan(withdrawal_value) + "万元", timestamp);
                        }
                    }
                }
            }
        }
    }
    
    void analyzeLargeOrders(const std::string& symbol, const StockData& curr_data,
                          const SimpleTickData& prev_data, StockAuctionMetrics& metrics,
                          long long timestamp) {
        // 计算匹配量变化
        double delta_av1 = curr_data.ask_volumes[0] - (prev_data.volume / curr_data.ask_prices[0] / 100);
        double delta_bv1 = curr_data.bid_volumes[0] - (prev_data.volume / curr_data.bid_prices[0] / 100);
        
        // 检查卖单大单：匹配量增加且价格下降
        if (delta_av1 > 0) {
            double threshold = getLargeOrderThreshold(curr_data.ask_prices[0]);
            if (delta_av1 >= threshold) {
                // 判断价格变化
                double price_change = curr_data.ask_prices[0] - prev_data.last_price;
                if (price_change < 0) {  // 价格下降，卖单大单
                    double order_value = delta_av1 * curr_data.ask_prices[0] * 100;
                    metrics.auction_metrics.net_large_order_flow -= order_value;
                    metrics.auction_metrics.cumulative_net_flow -= order_value;
                    
                    // 只记录高级别大单
                    if (order_value > 5000000) {
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|大单|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + Logger::f2s(metrics.auction_metrics.price_change * 100) + "% "
                                     + "卖单大单: " + Logger::f2s(delta_av1) + "股, "
                                     + "价格: " + Logger::f2s(curr_data.ask_prices[0]) + ", "
                                     + "金额: " + Logger::amountToWan(order_value) + "万元", timestamp);
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
                double price_change = curr_data.ask_prices[0] - prev_data.last_price;
                if (price_change >= 0) {  // 价格上升或不变，买单大单
                    double order_value = delta_bv1 * curr_data.bid_prices[0] * 100;
                    metrics.auction_metrics.net_large_order_flow += order_value;
                    metrics.auction_metrics.cumulative_net_flow += order_value;
                    
                    // 只记录高级别大单
                    if (order_value > 5000000) {
                        if (global_logger) {
                            global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                     + stock_mapper_.getStockDisplayName(symbol) 
                                     + " 涨幅：" + Logger::f2s(metrics.auction_metrics.price_change * 100) + "% "
                                     + "买单大单: " + Logger::f2s(delta_bv1) + "股, "
                                     + "价格: " + Logger::f2s(curr_data.bid_prices[0]) + ", "
                                     + "金额: " + Logger::amountToWan(order_value) + "万元", timestamp);
                        }
                    }
                }
            }
        }
    }
    
    double getLargeOrderThreshold(double price) {
        if (price == 0) return 0;
        return 500000 / price / 100;
    }
    
    void analyzeAuctionVolatility(const std::string& symbol, StockAuctionMetrics& metrics, 
                                 long long timestamp) {
        AuctionMetrics& auction_metrics = metrics.auction_metrics;
        
        // 计算匹配量比率（简化版）
        if (metrics.bid_amount_history.size() > 5) {
            double recent_avg = 0.0;
            int count = 0;
            size_t start_idx = metrics.bid_amount_history.size() > 5 ? metrics.bid_amount_history.size() - 5 : 0;
            for (size_t i = start_idx; i < metrics.bid_amount_history.size() - 1; ++i) {
                recent_avg += metrics.bid_amount_history[i];
                count++;
            }
            if (count > 0) recent_avg /= count;
            
            if (recent_avg > 0) {
                double current_bid = metrics.bid_amount_history.back();
                auction_metrics.match_volume_ratio = current_bid / recent_avg;
            }
        }
        
        // 计算异动分数（使用第一个代码的评分逻辑）
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
        
        // 确定异动级别（使用合理的阈值）
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
                        global_logger->infoWithTickTime(TimeUtils::formatTimestamp(timestamp) + "|"
                                 + "异动" + metrics.volatility_level + ": " 
                                 + stock_mapper_.getStockDisplayName(symbol) 
                                 + " - " + reason + "  涨幅：" 
                                 + Logger::f2s(metrics.auction_metrics.price_change * 100) + "%", timestamp);
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
            reasons.push_back("累计" + Logger::f2s(std::abs(metrics.cumulative_net_flow) / 10000) + "万元" + direction);
        }
        
        if (std::abs(metrics.cumulative_price_change) >= thresholds_.cumulative_price_change) {
            std::string direction = metrics.cumulative_price_change > 0 ? "累计上涨" : "累计下跌";
            reasons.push_back(Logger::f2s(std::abs(metrics.cumulative_price_change) * 100) + "%" + direction);
        }
        
        // 其次显示当前值
        if (std::abs(metrics.price_change) >= thresholds_.price_change) {
            std::string direction = metrics.price_change > 0 ? "上涨" : "下跌";
            reasons.push_back(Logger::f2s(std::abs(metrics.price_change) * 100) + "%" + direction);
        }
        
        if (metrics.match_volume_ratio >= thresholds_.match_volume_ratio) {
            reasons.push_back("匹配量" + Logger::f2s(metrics.match_volume_ratio) + "倍");
        }
        
        if (std::abs(metrics.net_large_order_flow) >= thresholds_.net_large_order_flow) {
            std::string direction = metrics.net_large_order_flow > 0 ? "流入" : "流出";
            reasons.push_back("大单" + Logger::f2s(std::abs(metrics.net_large_order_flow) / 10000) + "万元" + direction);
        }
        
        if (std::abs(metrics.withdrawal_impact) >= thresholds_.net_large_order_flow) {
            std::string direction = metrics.withdrawal_impact > 0 ? "净撤单" : "净撤买";
            reasons.push_back("撤单" + Logger::f2s(std::abs(metrics.withdrawal_impact) / 10000) + "万元" + direction);
        }
        
        std::string result = "异动分数" + Logger::f2s(score) + ": ";
        
        for (size_t i = 0; i < reasons.size(); ++i) {
            if (i > 0) result += ", ";
            result += reasons[i];
        }
        
        return result;
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
    
    void checkKeyTimepoints(long long timestamp) {
        std::string current_time = TimeUtils::formatTimestamp(timestamp).substr(11,8);
        int cut = std::stoi(current_time.substr(3, 2)) * 60 + std::stoi(current_time.substr(6, 2));
        
        if (!last_summary_time_.empty() && 
            std::abs(cut - (std::stoi(last_summary_time_.substr(3, 2)) * 60 + std::stoi(last_summary_time_.substr(6, 2)))) < 10) {
            return;
        }

        if (current_time > "09:20:00" && current_time <= "09:20:09") {
            report_time = report_time + 1;
            if(report_time < 2000) return;

            generateEnhancedAuctionReport("试盘结束总结", timestamp);
            last_summary_time_ = current_time;
            report_time = 0;
        } else if (current_time >= "09:24:00" && current_time <= "09:24:09") {
            report_time = report_time + 1;
            if(report_time < 4000) return;
            
            generateEnhancedAuctionReport("竞价接近结束总结", timestamp);
            last_summary_time_ = current_time;
            report_time = 0;
        } 
        // else if (current_time >= "09:25:00" && current_time <= "09:25:09") {
        //     report_time = report_time + 1;
            
        //     if(report_time < 3000) return;
            
        //     generateEnhancedAuctionReport("竞价结束总结", timestamp);
        //     last_summary_time_ = current_time;
        //     report_time = 0;
        // }
    }
        
    struct EnhancedMarketSummary {
        int total_stocks = 0;
        int high_open_count = 0;  // 高开>3%
        int low_open_count = 0;   // 低开<-3%
        std::vector<std::string> limit_up_stocks;
        std::vector<std::string> limit_down_stocks;
        std::vector<std::pair<std::string, double>> large_net_ranking;
        std::vector<std::pair<std::string, double>> amount_ranking;
    };
    
    EnhancedMarketSummary calculateEnhancedMarketSummary() {
        EnhancedMarketSummary summary;
        std::vector<std::pair<std::string, double>> large_nets;
        std::vector<std::pair<std::string, double>> amounts;
        
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
            
            // 跌停统计
            if (metrics.auction_metrics.is_limit_down) {
                summary.limit_down_stocks.push_back(symbol);
            }
            
            // 大单净额排名
            large_nets.emplace_back(symbol, metrics.auction_metrics.net_large_order_flow);
            
            // 成交额排名（使用竞价成交量估算）
            // double amount = metrics.auction_volume * 100; // 估算
            amounts.emplace_back(symbol, metrics.auction_volume);
        }
        
        // 排序
        std::sort(large_nets.begin(), large_nets.end(), 
                 [](const auto& a, const auto& b) { return std::abs(a.second) > std::abs(b.second); });
        std::sort(amounts.begin(), amounts.end(), 
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        
        summary.large_net_ranking = std::move(large_nets);
        summary.amount_ranking = std::move(amounts);
        
        return summary;
    }
};

int AuctionAnalyzer::report_time = 0;
// ==================== 开盘异动检测器 ====================
class VolatilityDetector : public IVolatilityDetector {
private:
    const Config& config_;
    std::unordered_map<std::string, std::deque<SimpleTickData>> stock_history_; // 优化: 限50
    StockNameMapper& stock_mapper_;
    IExternalDataProvider* external_provider_;
    std::mutex data_mutex_;
    std::unique_ptr<RedisClient> redis_; 
public:
    VolatilityDetector(const Config& config, IExternalDataProvider* provider = new DefaultExternalProvider())
        : config_(config), stock_mapper_(StockNameMapper::getInstance()), external_provider_(provider), 
        redis_(std::make_unique<RedisClient>(config.redis_host, config.redis_port, config.redis_db)){
        }
   
    ~VolatilityDetector() { delete external_provider_; }
   
    bool detectVolatility(const StockData& data, double change, double bid_amount,double ask_amount) override {
        updateHistory(data);
        TimeWindowStats stats = calculateTimeWindowStats(data);
        bool is_volatile = false;
        std::string reason;
        double strength = 0.0;
        
        // 涨跌停
        double limit = checkLimit(data, bid_amount,ask_amount);
        if (limit > 0 && data.inst_amt > 3000000) {
            is_volatile = true;
            reason = (change > 0) ? "Top|封单:" + Logger::amountToWan(limit) : "Low|封单:" + Logger::amountToWan(limit);
            strength = 10.0;
        }
        
        // 其他异动
        if (data.inst_amt > 2000000 && stats.amount_5min > 10000000 && std::abs(stats.change_1min) > 0.02) {
        //成交金额>100w 1分钟涨幅>1 5分钟涨幅>3 5分钟成交量>500w  量比>
        // if (data.inst_amt>100*10000 && stats.amount_5min>1000*10000 && stats.change_1min>0.01)
        // {
            is_volatile = true;
            reason = "Amount";
            strength = int(std::abs(stats.change_1min * 100));
        }
        if (std::abs(change)>0.02 && stats.change_1min>0.01 &&std::abs(stats.change_5min)>0.03)
        {
            if (!is_volatile){//} || isSignificantChange(data.symbol, "price_change", price_change)) {
                is_volatile = true;
                reason = "Price";
                strength = int((stats.change_5min+change)*50);
            }
        }
        if (is_volatile) {
            logVolatility(data, reason, strength, stats, Logger::f2s(change*100));
            storeToRedis(data, reason, strength, stats, Logger::f2s(change*100));
            // 板块聚合
            std::string sector = external_provider_->getSector(data.symbol);
            if (!sector.empty()) {
                // 聚合逻辑
            }
        }
        return is_volatile;
    }
   
    // void cleanVolOldData() override {
    //     std::lock_guard<std::mutex> lock(data_mutex_);
    //     if (stock_history_.empty()) {
    //         return;
    //     }
        
    //     long long max_timestamp = 0;
    //     for (const auto& pair : stock_history_) {
    //         if (pair.second.back().timestamp > max_timestamp) {
    //             max_timestamp = pair.second.back().timestamp;
    //         }
    //     }
    //     long long cutoff_time = max_timestamp - 1 * 60 * 1000;
    //     int count=0;
    //     auto it = stock_history_.begin();
    //     while (it != stock_history_.end()) {
    //         if (it->second.back().timestamp < cutoff_time) {
    //             count+=1;
    //             it = stock_history_.erase(it);
    //         } else {
    //             ++it;
    //         }
    //     }
    //      std::cout<<" cleanTickOldData "<<count<<std::endl;
    // }
private:
    void updateHistory(const StockData& data) {
        // std::lock_guard<std::mutex> lock(data_mutex_);
        auto& history = stock_history_[data.symbol];
        history.push_back(SimpleTickData::fromStockData(data));
        if (history.size() > config_.max_history_ticks) {
            history.pop_front();
            history.shrink_to_fit();
        }
    }
   
    TimeWindowStats calculateTimeWindowStats(const StockData& data) {
        TimeWindowStats stats;
        // std::lock_guard<std::mutex> lock(data_mutex_);
        auto it = stock_history_.find(data.symbol);
        if (it == stock_history_.end()) return stats;
    
        const auto& history = it->second;
        long long current_time = data.timestamp;
        long long time_1min_ago = current_time - config_.minute1_window_ms;
        long long time_5min_ago = current_time - config_.minute5_window_ms;
            
        // 初始化
        stats.large_net_1min = 0.0;
        stats.large_net_5min = 0.0;
        stats.amount_1min = 0.0;
        stats.amount_5min = 0.0;
        const SimpleTickData* tick_1min = nullptr;
        const SimpleTickData* tick_5min = nullptr;
        const SimpleTickData* oldest_tick = &history.front(); 
        
        // 单次遍历完成所有计算
        for (auto r_it = history.rbegin(); r_it != history.rend(); ++r_it) {
            const auto& tick = *r_it;
            
            // 如果在5分钟窗口内，累加大单净额到5分钟统计
            if (tick.timestamp >= time_5min_ago) {
                stats.large_net_5min += tick.large_net;
                
                // 如果在1分钟窗口内，累加大单净额到1分钟统计
                if (tick.timestamp >= time_1min_ago) {
                    stats.large_net_1min += tick.large_net;
                }
                
                // 记录1分钟和5分钟边界点的tick
                if (!tick_5min && tick.timestamp <= time_5min_ago) {
                    tick_5min = &tick;
                }
                if (!tick_1min && tick.timestamp <= time_1min_ago) {
                    tick_1min = &tick;
                }
            } else {
                break; // 超过5分钟窗口，提前退出
            }
        }
        if (!tick_1min) {
            tick_1min = oldest_tick;
        }
        if (!tick_5min) {
            tick_5min = oldest_tick;
        }
        // 使用找到的边界点tick计算价格变化和成交额
        if (tick_1min) {
            stats.change_1min = (data.last_price - tick_1min->last_price) / tick_1min->last_price;
            stats.amount_1min = data.amount - tick_1min->amount;
        }
    
        if (tick_5min) {
            stats.change_5min = (data.last_price - tick_5min->last_price) / tick_5min->last_price;
            stats.amount_5min = data.amount - tick_5min->amount;
        }
    
        return stats;
    }
    
    double checkLimit(const StockData& data, double bid_amount,double ask_amount) {
        double limit_up_price = std::round(data.close * 1.1 * 100) / 100.0;
        double limit_down_price = std::round(data.close * 0.9 * 100) / 100.0;
        std::string symbol_prefix = data.symbol.substr(0, 2);
        if (symbol_prefix == "30" || symbol_prefix == "68") {
            limit_up_price = std::round(data.close * 1.2 * 100) / 100.0;
            limit_down_price = std::round(data.close * 0.8 * 100) / 100.0;
        }
        if (std::abs(data.last_price - limit_up_price) < 0.01 &&ask_amount<1) {
            return bid_amount;
        } else if (std::abs(data.last_price - limit_down_price) < 0.01 &&bid_amount<0) {
            return ask_amount; // 简例
        }
        return 0.0;
    }
    
    void logVolatility(const StockData& data, std::string& reason, double strength, const TimeWindowStats& stats, const std::string& change) {
        std::ostringstream log_msg;
        std::string display_name = stock_mapper_.getStockDisplayName(data.symbol);
        log_msg << "异动|" << display_name <<"|"<<reason<< "|价格:" << Logger::f2s(data.last_price) 
                << "|" << change << "%"
                << "|瞬时:" << Logger::amountToWan(data.inst_amt) << "万"
                << "|1分速:" << Logger::f2s(stats.change_1min*100) << "%"
                << "|1分净额:" << Logger::amountToWan(stats.amount_1min) << "万"
                << "|5分净额:" << Logger::amountToWan(stats.large_net_5min) << "万"
                << "|5分金额:" << Logger::amountToWan(stats.amount_5min) << "万"
                << "|强度:" << strength;
       
        global_logger->warnWithTickTime(log_msg.str(), data.timestamp);
    }
    
    void storeToRedis(const StockData& data, const std::string& reason, double strength, const TimeWindowStats& stats, const std::string& change) {
        std::ostringstream json;
        json << "{\"symbol\":\"" << data.symbol << "\",\"timestamp\":" << data.timestamp << ",\"name\":\"" <<  stock_mapper_.getStockDisplayName(data.symbol) << "\",\"price\":" << Logger::f2s(data.last_price) << ","
             << "\"reason\":\"" << reason << "\",\"strength\":" << strength << ",\"change\":\"" << change<< "\",\"amount\":\"" << data.inst_amt<< "\","
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
        //过滤
        if (data.close <= 0 || (data.last_price > 700 && (data.symbol !="600519" || data.symbol != "688256"))) return;
        if (data.ask_volumes[0] == 0 && data.bid_volumes[0] == 0) return;
        // 统一计算指标
        double change = (data.close > 0) ? std::round((data.last_price - data.close) / data.close * 10000) / 10000.0 : 0.0;
        double bid_amount = (data.bid_volumes[0] + data.bid_volumes[1]) * data.last_price * 100;
        double ask_amount = (data.ask_volumes[0] + data.ask_volumes[1]) * data.last_price * 100;
        
        std::string time_str = TimeUtils::formatTimestamp(data.timestamp).substr(11, 8);
        bool is_auction = TimeUtils::isAuctionTime(time_str);
        
        tick_engine_->processTickData(data, is_auction);
        
        if (is_auction) {
            auction_analyzer_->processTickData(data, change, bid_amount, ask_amount);
        } else if (TimeUtils::isTradeTime(time_str)) {
            volatility_detector_->detectVolatility(data, change, bid_amount, ask_amount);
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
        try {
            // 首先尝试被动声明，检查队列是否存在
            amqp_queue_declare_ok_t* passive_declare = amqp_queue_declare(
                conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                1, 0, 0, 0, amqp_empty_table
            );
            
            amqp_rpc_reply_t passive_reply = amqp_get_rpc_reply(conn_);
            if (passive_reply.reply_type == AMQP_RESPONSE_NORMAL) {
                std::cout << "队列已存在，使用现有队列" << std::endl;
            }
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
            // 设置预取数量为1，一次只消费一条消息
            amqp_basic_qos(conn_, 1, 0, 1, 0);
            amqp_basic_consume(conn_, 1, amqp_cstring_bytes(config_.queue_name.c_str()),
                            amqp_empty_bytes, 0, 0, 1, amqp_empty_table);
            amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn_);
            checkAmqpError(reply, "Start consumer");
            consumer_started_ = true;
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
        // 声明并检查队列
        if (!declareQueue()) {
            return false;
        }
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
        if (!conn_ && !connect()) {
            std::cerr << "Failed to connect to RabbitMQ" << std::endl;
            return false;
        }
        
        // 单条消息消费，不需要向量操作
        // messages.clear();
        
        amqp_envelope_t envelope;
        struct timeval timeout = {1, 0}; // 100ms timeout
        amqp_rpc_reply_t ret = amqp_consume_message(conn_, &envelope, &timeout, 0);
        
        if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
            // 直接构造，避免不必要的拷贝
            messages.emplace_back(
                std::vector<char>(
                    static_cast<char*>(envelope.message.body.bytes),
                    static_cast<char*>(envelope.message.body.bytes) + envelope.message.body.len
                ),
                envelope.delivery_tag
            );
            amqp_destroy_envelope(&envelope);
            amqp_maybe_release_buffers_on_channel(conn_, 1);
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
         // 预分配向量，减少重复分配
        std::vector<PendingMessage> messages;
        std::vector<StockData> all_records;
        std::vector<PendingMessage> valid_messages;
        
        // 预留合理容量
        messages.reserve(config_.messages_per_batch);
        valid_messages.reserve(config_.messages_per_batch);
        bool auction_report_emitted_092510 = false;//是否竞价分析
        bool auction_report_emitted_092410 = false;//是否竞价分析
        bool auction_report_emitted_092003 = false;//是否竞价分析
        long long last_timestamp = 0;
        while (running_) {
            if (config_.enable_rate_limiting) {
                std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing_delay_ms));
            }
            // 清空向量但保留容量
            messages.clear();
            all_records.clear();
            valid_messages.clear();

            bool has_messages = consumer_->consumeMessages(messages, config_.messages_per_batch);
           
            if (!has_messages) {
                empty_cycles++;
                std::this_thread::sleep_for(std::chrono::seconds(10));
                continue;
            }
           
            empty_cycles = 0;
           
            for (auto& message : messages) {
                std::vector<StockData> records;
                if (message_processor_->processMessage(message.data, records)) {
                    // 更精确的reserve
                    size_t needed = all_records.size() + records.size();
                    if (all_records.capacity() < needed) {
                        all_records.reserve(needed);
                    }
                    if(records.size())
                        // 取第一个更新服务器时间戳
                        updateServerTimestamp(records[0].timestamp);
                    for (StockData& record : records) {
                        // if(record.symbol=="000833"||record.symbol=="002194"){
                            // auction_analyzer_->getKaipan();
                            phase_dispatcher_->dispatch(record);
                        // }
                        
                    }
                    // 使用移动语义
                    all_records.insert(all_records.end(),
                                    std::make_move_iterator(records.begin()),
                                    std::make_move_iterator(records.end()));
                    valid_messages.push_back(std::move(message));
                    records.clear();
                } else {
                    // 处理失败，拒绝消息并重新入队
                    failed_messages++;
                    consumer_->rejectMessage(message.delivery_tag, true);
                    // 手动释放消息数据
                    message.data.clear();
                    message.data.shrink_to_fit();
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
                last_timestamp = all_records.back().timestamp;
                std::string last_time_str = TimeUtils::formatTimestamp(last_timestamp);
                std::string time_part = last_time_str.substr(11, 8);
                if (!auction_report_emitted_092003 && "09:20:05"> time_part  && time_part >= "09:20:03")
                {
                    auction_analyzer_->generateEnhancedAuctionReport("试盘结束总结", last_timestamp);
                    auction_report_emitted_092003 = true;
                }else if (!auction_report_emitted_092410 && "09:24:13" > time_part && time_part>= "09:24:10")
                {
                    auction_analyzer_->generateEnhancedAuctionReport("竞价接近结束总结", last_timestamp);
                    auction_report_emitted_092410 = true;
                }
                // else if (!auction_report_emitted_092510 && "09:25:13" > time_part  && time_part >= "09:25:10")
                // {

                //     auction_analyzer_->generateEnhancedAuctionReport("竞价结束总结", last_timestamp);
                //     auction_report_emitted_092510 = true;
                // }
               
            }
           
            // 简化的时间检查：检查是否达到目标时间（9点25分10秒）
            if (!auction_report_emitted_092510) {
                auto now_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                std::tm* local_time = std::localtime(&now_time);
                
                if (local_time->tm_hour == 9 && local_time->tm_min == 25 && local_time->tm_sec >= 10) {
                    auction_analyzer_->getKaipan();
                    auction_analyzer_->generateEnhancedAuctionReport("竞价结束总结", last_timestamp);
                    auction_report_emitted_092510 = true;
                }
            }
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= config_.report_time) {
                long long current_delay = getServerDelay();
                logger_->warn("处理: " + std::to_string(total_messages) + " 条消息, " +
                             std::to_string(total_records) + " 条记录, " +
                             std::to_string(failed_messages) + " 条失败"+
                             std::to_string(total_messages/config_.report_time) + " msg/s, " +
                            //  "延迟: " + (int(current_delay*0.001)>60*60*12?"historical":std::to_string(int(current_delay*0.001))) + "s");
                             "延迟: " + std::to_string(int(current_delay*0.001)) + "s"+
                            "服务器时间："+ TimeUtils::formatTimestamp(max_server_timestamp_.load()).substr(11));
                last_report = now;
                total_messages = 0;
                total_records = 0;
                failed_messages = 0;
                // last_cleanup = now;
                //重置延时器
                resetDelayStats();
                // exit(0);
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
