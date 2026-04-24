#include "stock_analysis.h"

// ==================== 配置管理器 ====================
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

// ==================== 时间工具 ====================
class TimeUtils {
public:
    static long long getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }
    
    static std::string formatTimestamp(long long timestamp) {
        auto time_point = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp));
        auto local_time = std::chrono::system_clock::to_time_t(time_point);
        std::tm tm_info = *std::localtime(&local_time);
        
        std::stringstream ss;
        ss << std::put_time(&tm_info, "%Y-%m-%d %H:%M:%S");
        return ss.str();
    }
    
    static bool isAuctionTime(const std::string& time_str) {
        // 解析时间字符串 "HHMMSS"
        if (time_str.length() != 6) return false;
        int time = std::stoi(time_str);
        // 早盘竞价 9:15-9:25
        if (time >= 91500 && time <= 92500) return true;
        // 尾盘竞价 14:57-15:00
        if (time >= 145700 && time <= 150000) return true;
        return false;
    }
    
    static bool isTrialPeriod(const std::string& time_str) {
        // 试盘期 9:15-9:20
        if (time_str.length() != 6) return false;
        int time = std::stoi(time_str);
        return time >= 91500 && time <= 92000;
    }
    
    static bool isTradeTime(const std::string& time_str) {
        // 交易时间 9:30-11:30, 13:00-15:00
        if (time_str.length() != 6) return false;
        int time = std::stoi(time_str);
        return (time >= 93000 && time <= 113000) || (time >= 130000 && time <= 150000);
    }
};

// ==================== 股票名映射 ====================
class StockNameMapper {
private:
    std::unordered_map<std::string, std::string> code_to_name_;
    std::unordered_map<std::string, std::string> code_to_sector_; // 新: 板块，预留
    std::string csv_file_path_ = "stock.csv";
    std::mutex mutex_;
    bool loaded_ = false;
    StockNameMapper(){
        // 默认初始化一些股票数据
        addDefaultStocks();
    }
    
    void addDefaultStocks() {
        // 添加一些默认股票数据
        code_to_name_["000001"] = "平安银行";
        code_to_name_["000002"] = "万科A";
        code_to_name_["600000"] = "浦发银行";
        code_to_name_["600036"] = "招商银行";
        code_to_name_["000858"] = "五粮液";
        code_to_name_["000568"] = "泸州老窖";
        code_to_name_["600519"] = "贵州茅台";
        code_to_name_["002594"] = "比亚迪";
        code_to_name_["300750"] = "宁德时代";
        code_to_name_["600900"] = "长江电力";
    }
public:
    StockNameMapper(const StockNameMapper&) = delete;
    StockNameMapper& operator=(const StockNameMapper&) = delete;
    static StockNameMapper& getInstance() {
        static StockNameMapper instance;
        return instance;
    }
    bool loadStockNames() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (loaded_) return true;
        
        try {
            std::ifstream file(csv_file_path_);
            if (!file.is_open()) {
                // 文件不存在时使用默认数据
                loaded_ = true;
                return true;
            }
            
            std::string line;
            // 跳过表头
            if (std::getline(file, line)) {
                while (std::getline(file, line)) {
                    std::stringstream ss(line);
                    std::string code, name, sector;
                    
                    if (std::getline(ss, code, ',') && std::getline(ss, name, ',')) {
                        code_to_name_[code] = name;
                        if (std::getline(ss, sector, ',')) {
                            code_to_sector_[code] = sector;
                        }
                    }
                }
            }
            file.close();
            loaded_ = true;
            return true;
        } catch (const std::exception& e) {
            return false;
        }
    }
    
    std::string getStockName(const std::string& code) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = code_to_name_.find(code);
        if (it != code_to_name_.end()) {
            return it->second;
        }
        return code; // 默认返回代码
    }
    
    std::string getStockDisplayName(const std::string& code) {
        return getStockName(code) + "(" + code + ")";
    }
    
    std::string getSector(const std::string& code) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = code_to_sector_.find(code);
        if (it != code_to_sector_.end()) {
            return it->second;
        }
        return "未知";
    }
};

// ==================== 日志工具 ====================
class Logger {
private:
    std::ofstream log_file_;
    int console_level_ = 2;
    int file_level_ = 1;
    bool enable_file_ = true;
    std::mutex log_mutex_;
    
    const char* getLevelString(int level) {
        switch (level) {
            case 0: return "DEBUG";
            case 1: return "INFO";
            case 2: return "WARNING";
            case 3: return "ERROR";
            case 4: return "FATAL";
            default: return "UNKNOWN";
        }
    }
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
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << value;
        std::string str = ss.str();
        // 移除末尾的0
        size_t dot_pos = str.find('.');
        if (dot_pos != std::string::npos) {
            size_t last_non_zero = str.find_last_not_of('0');
            if (last_non_zero == dot_pos) {
                str.erase(dot_pos);
            } else {
                str.erase(last_non_zero + 1);
            }
        }
        return str;
    }
    
    static std::string amountToWan(double amount) {
        return f2s(amount / 10000) + "万";
    }
    
    void logWithTimestamp(int level, const std::string& message, long long timestamp) {
        std::lock_guard<std::mutex> lock(log_mutex_);
        
        std::string timestamp_str = timestamp > 0 ? TimeUtils::formatTimestamp(timestamp) : TimeUtils::formatTimestamp(TimeUtils::getCurrentTimestamp());
        std::string log_message = "[" + timestamp_str + "] [" + getLevelString(level) + "] " + message;
        
        // 输出到控制台
        if (level >= console_level_) {
            if (level >= 3) {
                std::cerr << log_message << std::endl;
            } else {
                std::cout << log_message << std::endl;
            }
        }
        
        // 输出到文件
        if (enable_file_ && log_file_.is_open() && level >= file_level_) {
            log_file_ << log_message << std::endl;
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
        logWithTimestamp(0, message, 0);
    }
    void info(const std::string& message) {
        logWithTimestamp(1, message, 0);
    }
    void warn(const std::string& message) {
        logWithTimestamp(2, message, 0);
    }
    void error(const std::string& message) {
        logWithTimestamp(3, message, 0);
    }
    void fatal(const std::string& message) {
        logWithTimestamp(4, message, 0);
    }
};

// 全局日志器实例
Logger* global_logger = nullptr;

// ==================== HTTP请求工具 ====================
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
        if (!curl) {
            if (global_logger) {
                global_logger->error("Failed to initialize curl");
            }
            return "";
        }
        
        // 构建带参数的URL
        std::string full_url = url;
        if (!params.empty()) {
            full_url += "?";
            bool first = true;
            for (const auto& [key, value] : params) {
                if (!first) {
                    full_url += "&";
                }
                full_url += key + "=" + value;
                first = false;
            }
        }
        
        std::string response;
        
        curl_easy_setopt(curl, CURLOPT_URL, full_url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L); // 10秒超时
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L); // 5秒连接超时
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L); // 跟随重定向
        
        // 禁用SSL验证（仅用于开发环境）
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        
        CURLcode res = curl_easy_perform(curl);
        
        if (res != CURLE_OK) {
            if (global_logger) {
                global_logger->error(std::string("CURL error: ") + curl_easy_strerror(res));
            }
        }
        
        curl_easy_cleanup(curl);
        return response;
    }
};

// ==================== 外部数据提供器 ====================
class DefaultExternalProvider : public IExternalDataProvider {
public:
    int getBoardCount(const std::string& symbol) override {
        // 默认返回0，可以扩展为从外部API获取
        return 0;
    }
    
    std::string getSector(const std::string& symbol) override {
        // 默认返回未知，可以扩展为从外部API获取
        return "未知";
    }
};