#include "stock_analysis.h"

// ==================== Tick分析引擎 ====================
class TickAnalysisEngine {
private:
    const Config& config_;
    StockNameMapper& stock_mapper_;
    std::mutex data_mutex_;
    std::unordered_map<std::string, std::deque<SimpleTickData>> stock_history_; // 股票历史tick数据
    
public:
    TickAnalysisEngine(const Config& config)
        : config_(config), stock_mapper_(StockNameMapper::getInstance()) {
    }
    
    void processTickData(const StockData& data) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        
        // 更新历史数据
        updateHistory(data);
        
        // 计算统计指标
        TimeWindowStats stats = calculateTimeWindowStats(data);
        
        // 分析大单
        analyzeLargeOrders(data);
        
        // 分析价格变动
        analyzePriceMovement(data, stats);
    }
    
private:
    void updateHistory(const StockData& data) {
        SimpleTickData simplified = SimpleTickData::fromStockData(data);
        
        auto& history = stock_history_[data.symbol];
        history.push_back(simplified);
        
        // 限制历史数据长度
        if (history.size() > config_.max_history_ticks) {
            history.pop_front();
        }
    }
    
    TimeWindowStats calculateTimeWindowStats(const StockData& data) {
        TimeWindowStats stats;
        auto& history = stock_history_[data.symbol];
        
        long long current_time = data.timestamp;
        
        for (auto it = history.rbegin(); it != history.rend(); ++it) {
            long long time_diff = current_time - it->timestamp;
            
            if (time_diff <= config_.minute1_window_ms) {
                stats.volume_1min += it->volume;
                stats.amount_1min += it->amount;
                stats.large_net_1min += it->large_net;
            }
            
            if (time_diff <= config_.minute5_window_ms) {
                stats.volume_5min += it->volume;
                stats.amount_5min += it->amount;
                stats.large_net_5min += it->large_net;
            }
        }
        
        // 计算涨跌幅
        if (!history.empty()) {
            double first_price = history.front().last_price;
            if (first_price > 0) {
                stats.change_1min = (data.last_price - first_price) / first_price;
            }
        }
        
        return stats;
    }
    
    void analyzeLargeOrders(const StockData& data) {
        // 大单分析逻辑
        if (data.inst_amt >= config_.large_order_threshold) {
            if (global_logger) {
                global_logger->info("大单成交: " + stock_mapper_.getStockDisplayName(data.symbol) + 
                                   " 价格: " + Logger::f2s(data.last_price) + 
                                   " 金额: " + Logger::amountToWan(data.inst_amt));
            }
        }
    }
    
    void analyzePriceMovement(const StockData& data, const TimeWindowStats& stats) {
        // 价格变动分析
        if (fabs(stats.change_1min) >= config_.price_change_threshold) {
            if (global_logger) {
                std::string direction = stats.change_1min > 0 ? "上涨" : "下跌";
                global_logger->info("价格异动: " + stock_mapper_.getStockDisplayName(data.symbol) + 
                                   " " + direction + ": " + Logger::f2s(fabs(stats.change_1min) * 100) + "%" + 
                                   " 成交量: " + Logger::amountToWan(stats.amount_1min));
            }
        }
    }
};

// ==================== 竞价分析器 ====================
class AuctionAnalyzer {
private:
    const Config& config_;
    std::unordered_map<std::string, StockAuctionMetrics> stock_auction_metrics_;
    MarketReport market_report_;
    StockNameMapper& stock_mapper_;
    IExternalDataProvider* external_provider_;
    std::unique_ptr<RedisClient> redis_;
    std::unique_ptr<IndicatorProvider> indicator_provider_;
    std::mutex data_mutex_;
    static int report_time;
    
public:
    AuctionAnalyzer(const Config& config, IExternalDataProvider* provider = new DefaultExternalProvider())
        : config_(config), stock_mapper_(StockNameMapper::getInstance()), external_provider_(provider),
          redis_(std::make_unique<RedisClient>(config.redis_host, config.redis_port, config.redis_db)),
          indicator_provider_(std::make_unique<IndicatorProvider>()) {
        report_time = config.report_time;
    }
    
    void processAuctionData(const StockData& data, double change, double bid_amount, double ask_amount) {
        std::lock_guard<std::mutex> lock(data_mutex_);
        
        // 获取或创建股票的竞价指标
        StockAuctionMetrics& metrics = stock_auction_metrics_[data.symbol];
        
        // 更新历史数据
        updateHistoryData(metrics, data.last_price, data.volume, 0.0, bid_amount);
        
        // 更新竞价指标
        updateAuctionMetrics(metrics, data, change, bid_amount, ask_amount);
        
        // 分析订单流
        analyzeOrderFlow(data.symbol, data, metrics, data.timestamp, change, bid_amount, ask_amount);
        
        // 分析大单
        analyzeLargeOrders(data.symbol, data, metrics.prev_tick_data, metrics, data.timestamp);
        
        // 分析撤单
        analyzeWithdrawals(data.symbol, data, metrics.prev_tick_data, metrics, data.timestamp);
        
        // 分析竞价波动性
        analyzeAuctionVolatility(data.symbol, metrics, data.timestamp);
        
        // 检查关键时间点
        checkKeyTimepoints(data.timestamp);
        
        // 保存前一个tick数据
        metrics.prev_tick_data = SimpleTickData::fromStockData(data);
    }
    
    void cleanAuctOldData() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        long long now = TimeUtils::getCurrentTimestamp();
        long long threshold = now - 30 * 60 * 1000; // 30分钟前的数据
        
        for (auto it = stock_auction_metrics_.begin(); it != stock_auction_metrics_.end();) {
            if (it->second.last_analysis_time < threshold) {
                it = stock_auction_metrics_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    void getKaipan() {
        // 生成开盘报告
        EnhancedMarketSummary summary = calculateEnhancedMarketSummary();
        
        if (global_logger) {
            global_logger->info("===== 开盘统计 ====" + 
                              "\n总股票数: " + std::to_string(summary.total_stocks) +
                              "\n高开股数: " + std::to_string(summary.high_open_count) +
                              "\n低开股数: " + std::to_string(summary.low_open_count) +
                              "\n涨停股数: " + std::to_string(summary.limit_up_stocks.size()) +
                              "\n跌停股数: " + std::to_string(summary.limit_down_stocks.size()));
        }
    }
    
    void updateMetricsWithRealData(const std::string& symbol, const std::string& dde_data) {
        // 使用真实DDE数据更新指标
        double large_net = parseDDEData(dde_data);
        if (large_net != 0.0) {
            if (stock_auction_metrics_.find(symbol) != stock_auction_metrics_.end()) {
                stock_auction_metrics_[symbol].auction_metrics.cumulative_net_flow = large_net;
            }
        }
    }
    
    double parseDDEData(const std::string& dde_data) {
        // 解析DDE数据，简化实现
        try {
            // 这里应该解析真实的DDE数据格式
            return 0.0;
        } catch (...) {
            return 0.0;
        }
    }
    
    void generateEnhancedAuctionReport(const std::string& time_str, long long timestamp) {
        // 生成增强的竞价报告
        std::string report = "===== 竞价报告 " + time_str + " =====\n";
        
        // 添加市场概览
        report += "\n市场概览:\n";
        report += "- 涨跌幅前5: ";
        for (int i = 0; i < std::min(5, (int)market_report_.top_changes.size()); ++i) {
            report += stock_mapper_.getStockDisplayName(market_report_.top_changes[i].first) + 
                     "(" + Logger::f2s(market_report_.top_changes[i].second * 100) + "%), ";
        }
        
        if (global_logger) {
            global_logger->info(report);
        }
    }
    
private:
    void updateHistoryData(StockAuctionMetrics& metrics, double price, double av1, double bv1, double bid_amount) {
        metrics.price_history.push_back(price);
        metrics.bid_amount_history.push_back(bid_amount);
        
        // 限制历史数据长度
        if (metrics.price_history.size() > metrics.HISTORY_SIZE) {
            metrics.price_history.pop_front();
        }
        if (metrics.bid_amount_history.size() > metrics.HISTORY_SIZE) {
            metrics.bid_amount_history.pop_front();
        }
    }
    
    void updateAuctionMetrics(StockAuctionMetrics& metrics, const StockData& data, double change, double bid_amount, double ask_amount) {
        AuctionMetrics& am = metrics.auction_metrics;
        am.price_change = change;
        am.bid_amount = bid_amount;
        am.ask_amount = ask_amount;
        
        // 检查是否涨停跌停
        am.is_limit_up = change >= 0.1; // 简化判断
        am.is_limit_down = change <= -0.1; // 简化判断
        
        // 计算累计净流入
        am.cumulative_net_flow += data.large_net;
    }
    
    void analyzeOrderFlow(const std::string& symbol, const StockData& curr_data, 
                         StockAuctionMetrics& metrics, long long timestamp, 
                         double change, double bid_amount, double ask_amount) {
        // 订单流分析
        if (bid_amount > ask_amount * 1.5) {
            // 买方力量强
            if (global_logger) {
                global_logger->info("买方力量强: " + stock_mapper_.getStockDisplayName(symbol) + 
                                   " 买盘: " + Logger::amountToWan(bid_amount) + 
                                   " 卖盘: " + Logger::amountToWan(ask_amount));
            }
        }
    }
    
    void analyzeWithdrawals(const std::string& symbol, const StockData& curr_data, 
                           const SimpleTickData& prev_data, StockAuctionMetrics& metrics, 
                           long long timestamp) {
        // 撤单分析
        std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11, 8);
        if (TimeUtils::isTrialPeriod(time_str.substr(0, 6))) {
            // 试盘期撤单分析
            if (metrics.auction_metrics.bid_amount < prev_data.amount * 0.8) {
                // 大幅撤单
                if (global_logger) {
                    global_logger->info("大幅撤单: " + stock_mapper_.getStockDisplayName(symbol) + 
                                       " 撤单前: " + Logger::amountToWan(prev_data.amount) + 
                                       " 撤单后: " + Logger::amountToWan(metrics.auction_metrics.bid_amount));
                }
            }
        }
    }
    
    void analyzeLargeOrders(const std::string& symbol, const StockData& curr_data, 
                          const SimpleTickData& prev_data, StockAuctionMetrics& metrics, 
                          long long timestamp) {
        // 大单分析
        double large_threshold = getLargeOrderThreshold(curr_data.last_price);
        
        // 计算即时成交金额
        double inst_amount = curr_data.inst_amt;
        
        if (inst_amount >= large_threshold) {
            std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11, 8);
            std::string period = TimeUtils::isTrialPeriod(time_str.substr(0, 6)) ? "试盘期" : "正式期";
            
            if (global_logger) {
                global_logger->info(period + "大单: " + stock_mapper_.getStockDisplayName(symbol) + 
                                   " 价格: " + Logger::f2s(curr_data.last_price) + 
                                   " 金额: " + Logger::amountToWan(inst_amount) + 
                                   " 涨跌幅: " + Logger::f2s(metrics.auction_metrics.price_change * 100) + "%");
            }
            
            // 特殊情况处理
            if (metrics.auction_metrics.is_limit_up) {
                // 涨停情况
                addToVolatilePool(symbol, "竞价涨停");
            } else if (metrics.auction_metrics.is_limit_down) {
                // 跌停情况
                addToVolatilePool(symbol, "竞价跌停");
            }
        }
    }
    
    double getLargeOrderThreshold(double price) {
        // 动态大单阈值
        if (price < 10) return LARGE_ORDER_THRESHOLD_LOW;
        if (price < 100) return LARGE_ORDER_THRESHOLD_MID;
        return LARGE_ORDER_THRESHOLD_HIGH;
    }
    
    void analyzeAuctionVolatility(const std::string& symbol, StockAuctionMetrics& metrics, 
                                 long long timestamp) {
        // 竞价波动性分析
        if (metrics.price_history.size() < 5) return;
        
        // 计算价格波动
        double price_range = metrics.price_history.back() - metrics.price_history.front();
        double price_volatility = fabs(price_range / metrics.price_history.front());
        
        metrics.volatility_score = price_volatility * 100;
        
        if (metrics.volatility_score > 5.0) {
            metrics.volatility_level = "high";
            addToVolatilePool(symbol, "竞价波动大");
        } else if (metrics.volatility_score > 2.0) {
            metrics.volatility_level = "medium";
        } else {
            metrics.volatility_level = "low";
        }
    }
    
    void addToVolatilePool(const std::string& symbol, const std::string& reason) {
        // 添加到异动池
        if (redis_) {
            redis_->sadd(config_.volatile_pool_key, symbol);
            redis_->set("stock:volatile:reason:" + symbol, reason, config_.volatile_expire);
        }
    }
    
    void removeFromVolatilePool(const std::string& symbol, const std::string& reason) {
        // 从异动池移除
        if (redis_) {
            redis_->srem(config_.volatile_pool_key, symbol);
        }
    }
    
    void checkKeyTimepoints(long long timestamp) {
        std::string time_str = TimeUtils::formatTimestamp(timestamp).substr(11, 8);
        
        // 9:20 撤单数据统计
        if (time_str == "09:20:00") {
            generateEnhancedAuctionReport("9:20", timestamp);
        }
        // 9:24 机会分析
        else if (time_str == "09:24:00") {
            generateEnhancedAuctionReport("9:24", timestamp);
        }
        // 9:25 开盘统计
        else if (time_str == "09:25:00") {
            getKaipan();
        }
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
        
        for (const auto& [symbol, metrics] : stock_auction_metrics_) {
            summary.total_stocks++;
            double change = metrics.auction_metrics.price_change;
            
            if (change >= 0.1) {
                summary.limit_up_stocks.push_back(symbol);
            } else if (change <= -0.1) {
                summary.limit_down_stocks.push_back(symbol);
            }
            
            if (change > 0.03) {
                summary.high_open_count++;
            } else if (change < -0.03) {
                summary.low_open_count++;
            }
            
            // 大单净额排名
            summary.large_net_ranking.emplace_back(symbol, metrics.auction_metrics.cumulative_net_flow);
            // 成交额排名
            summary.amount_ranking.emplace_back(symbol, metrics.auction_volume);
        }
        
        // 排序
        std::sort(summary.large_net_ranking.begin(), summary.large_net_ranking.end(), 
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        std::sort(summary.amount_ranking.begin(), summary.amount_ranking.end(), 
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        
        return summary;
    }
};

int AuctionAnalyzer::report_time = 0;

// ==================== 异动检测器 ====================
class VolatilityDetector : public IVolatilityDetector {
private:
    const Config& config_;
    std::unordered_map<std::string, std::deque<SimpleTickData>> stock_history_; // 优化: 限50
    StockNameMapper& stock_mapper_;
    IExternalDataProvider* external_provider_;
    std::mutex data_mutex_;
    std::unique_ptr<RedisClient> redis_; 
    std::unordered_map<std::string, long long> last_volatility_time_;
    
public:
    VolatilityDetector(const Config& config, IExternalDataProvider* provider = new DefaultExternalProvider())
        : config_(config), stock_mapper_(StockNameMapper::getInstance()), external_provider_(provider), 
        redis_(std::make_unique<RedisClient>(config.redis_host, config.redis_port, config.redis_db)){
        if (!redis_->connect()) {
            if (global_logger) {
                global_logger->error("Redis连接失败");
            }
        }
    }
    
    bool checkLastVolatilityTime(const std::string& symbol, long long current_time, int threshold_ms = 10000) {
        auto it = last_volatility_time_.find(symbol);
        if (it != last_volatility_time_.end()) {
            if (current_time - it->second < threshold_ms) {
                return false; // 10秒内不重复提醒
            }
        }
        last_volatility_time_[symbol] = current_time;
        return true;
    }
    
    bool detectVolatility(const StockData& data, double change, double bid_amount, double ask_amount) override {
        std::lock_guard<std::mutex> lock(data_mutex_);
        
        // 检查是否需要提醒
        if (!checkLastVolatilityTime(data.symbol, data.timestamp, config_.volatility_threshold_ms)) {
            return false;
        }
        
        // 更新历史数据
        updateHistory(data);
        
        // 计算时间窗口统计
        TimeWindowStats stats = calculateTimeWindowStats(data);
        
        // 检查涨跌幅
        if (fabs(change) >= config_.price_change_threshold) {
            std::string reason = change > 0 ? "大幅上涨" : "大幅下跌";
            logVolatility(data, reason, fabs(change), stats, change > 0 ? "up" : "down");
            storeToRedis(data, reason, fabs(change), stats, change > 0 ? "up" : "down");
            return true;
        }
        
        // 检查成交量放大
        if (stats.volume_1min > 0 && stats.volume_1min / stats.volume_5min > config_.volume_ratio_threshold) {
            std::string reason = "放量异动";
            logVolatility(data, reason, stats.volume_1min / stats.volume_5min, stats, "volume");
            storeToRedis(data, reason, stats.volume_1min / stats.volume_5min, stats, "volume");
            return true;
        }
        
        // 检查大单异动
        if (data.inst_amt >= config_.min_amount_threshold) {
            std::string reason = "大单一笔";
            logVolatility(data, reason, data.inst_amt / config_.min_amount_threshold, stats, "large_order");
            storeToRedis(data, reason, data.inst_amt / config_.min_amount_threshold, stats, "large_order");
            return true;
        }
        
        // 检查涨停跌停
        double limit_check = checkLimit(data, bid_amount, ask_amount);
        if (limit_check != 0) {
            std::string reason = limit_check > 0 ? "涨停" : "跌停";
            logVolatility(data, reason, 1.0, stats, limit_check > 0 ? "limit_up" : "limit_down");
            storeToRedis(data, reason, 1.0, stats, limit_check > 0 ? "limit_up" : "limit_down");
            return true;
        }
        
        return false;
    }
    
private:
    void updateHistory(const StockData& data) {
        SimpleTickData simplified = SimpleTickData::fromStockData(data);
        
        auto& history = stock_history_[data.symbol];
        history.push_back(simplified);
        
        // 限制历史数据长度
        if (history.size() > config_.max_history_ticks) {
            history.pop_front();
        }
    }
    
    TimeWindowStats calculateTimeWindowStats(const StockData& data) {
        TimeWindowStats stats;
        auto& history = stock_history_[data.symbol];
        
        long long current_time = data.timestamp;
        
        for (auto it = history.rbegin(); it != history.rend(); ++it) {
            long long time_diff = current_time - it->timestamp;
            
            if (time_diff <= config_.minute1_window_ms) {
                stats.volume_1min += it->volume;
                stats.amount_1min += it->amount;
                stats.large_net_1min += it->large_net;
            }
            
            if (time_diff <= config_.minute5_window_ms) {
                stats.volume_5min += it->volume;
                stats.amount_5min += it->amount;
                stats.large_net_5min += it->large_net;
            }
        }
        
        // 计算涨跌幅
        if (!history.empty()) {
            double first_price = history.front().last_price;
            if (first_price > 0) {
                stats.change_1min = (data.last_price - first_price) / first_price;
                stats.change_5min = (data.last_price - first_price) / first_price;
            }
        }
        
        return stats;
    }
    
    double checkLimit(const StockData& data, double bid_amount, double ask_amount) {
        // 简化的涨跌停检查
        if (bid_amount > 0 && ask_amount == 0) {
            return 1; // 涨停
        } else if (ask_amount > 0 && bid_amount == 0) {
            return -1; // 跌停
        }
        return 0;
    }
    
    void logVolatility(const StockData& data, std::string& reason, double strength, const TimeWindowStats& stats, const std::string& change) {
        if (!global_logger) return;
        
        std::string strength_str = strength > 2 ? "强烈" : (strength > 1.5 ? "明显" : "轻微");
        std::stringstream ss;
        ss << "异动提醒[" << strength_str << "]: " << stock_mapper_.getStockDisplayName(data.symbol) 
           << " 价格: " << Logger::f2s(data.last_price)
           << " 涨跌幅: " << Logger::f2s(data.last_price / data.close * 100 - 100) << "%"
           << " 原因: " << reason
           << " 1分钟量: " << Logger::amountToWan(stats.amount_1min);
        
        global_logger->warnWithTickTime(ss.str(), data.timestamp);
    }
    
    void storeToRedis(const StockData& data, const std::string& reason, double strength, const TimeWindowStats& stats, const std::string& change) {
        if (!redis_) return;
        
        std::string key = "stock:volatile:" + data.symbol;
        std::stringstream value;
        value << "{\"timestamp\":\"" << TimeUtils::formatTimestamp(data.timestamp)
              << "\",\"price\":" << data.last_price
              << ",\"change\":" << (data.last_price / data.close - 1)
              << ",\"reason\":\"" << reason
              << ",\"strength\":" << strength
              << ",\"amount_1min\":" << stats.amount_1min
              << ",\"type\":\"" << change << "\"}";
        
        redis_->set(key, value.str(), config_.volatile_expire);
        redis_->sadd(config_.volatile_pool_key, data.symbol);
    }
};

// ==================== 阶段分发器 ====================
class PhaseDispatcher {
private:
    TickAnalysisEngine* tick_engine_;
    AuctionAnalyzer* auction_analyzer_;
    IVolatilityDetector* volatility_detector_;
    
public:
    PhaseDispatcher(TickAnalysisEngine* te, AuctionAnalyzer* aa, IVolatilityDetector* vd)
        : tick_engine_(te), auction_analyzer_(aa), volatility_detector_(vd) {}
    
    void dispatch(StockData& data) {
        std::string time_str = TimeUtils::formatTimestamp(data.timestamp).substr(11, 8);
        
        // 计算涨跌幅
        double change = 0;
        if (data.close > 0) {
            change = (data.last_price - data.close) / data.close;
        }
        
        // 计算买卖盘金额
        double bid_amount = 0, ask_amount = 0;
        for (int i = 0; i < 5; ++i) {
            bid_amount += data.bid_prices[i] * data.bid_volumes[i];
            ask_amount += data.ask_prices[i] * data.ask_volumes[i];
        }
        
        // 根据时间阶段分发
        if (TimeUtils::isAuctionTime(time_str)) {
            // 竞价阶段
            auction_analyzer_->processAuctionData(data, change, bid_amount, ask_amount);
        } else if (TimeUtils::isTradeTime(time_str)) {
            // 连续交易阶段
            tick_engine_->processTickData(data);
            volatility_detector_->detectVolatility(data, change, bid_amount, ask_amount);
        }
    }
};