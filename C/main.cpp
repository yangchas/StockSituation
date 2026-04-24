#include "stock_analysis.h"
#include <iostream>

int main(int argc, char* argv[]) {
    try {
        // 初始化日志系统
        Logger::initialize("stock_analysis.log", Logger::LogLevel::INFO);
        
        if (global_logger) {
            global_logger->info("程序启动，初始化系统...");
        }
        
        // 初始化配置管理器
        ConfigManager& config_manager = ConfigManager::getInstance();
        if (!config_manager.initialize()) {
            if (global_logger) {
                global_logger->error("配置初始化失败");
            }
            return 1;
        }
        
        const Config& config = config_manager.getConfig();
        
        // 初始化股票名映射器
        StockNameMapper& stock_mapper = StockNameMapper::getInstance();
        if (!stock_mapper.initialize()) {
            if (global_logger) {
                global_logger->error("股票名映射初始化失败");
            }
            return 1;
        }
        
        // 初始化Redis连接
        std::unique_ptr<RedisClient> redis = std::make_unique<RedisClient>(config.redis_host, config.redis_port, config.redis_db);
        if (!redis->connect()) {
            if (global_logger) {
                global_logger->warn("Redis连接失败，部分功能可能不可用");
            }
        } else {
            if (global_logger) {
                global_logger->info("Redis连接成功");
            }
        }
        
        // 初始化TDengine连接
        std::unique_ptr<TDengineConnection> tdengine = std::make_unique<TDengineConnection>(
            config.tdengine_host, config.tdengine_port, config.tdengine_user, 
            config.tdengine_password, config.tdengine_database);
        
        if (!tdengine->connect()) {
            if (global_logger) {
                global_logger->warn("TDengine连接失败，数据写入功能不可用");
            }
        } else {
            if (global_logger) {
                global_logger->info("TDengine连接成功");
            }
        }
        
        // 创建数据写入器
        std::unique_ptr<IDataWriter> data_writer;
        if (tdengine) {
            data_writer = std::make_unique<TDengineDataWriter>(std::move(tdengine));
        }
        
        // 创建消息处理器
        std::unique_ptr<IMessageProcessor> message_processor = std::make_unique<SimpleMessageProcessor>();
        
        // 初始化分析引擎
        std::unique_ptr<TickAnalysisEngine> tick_engine = std::make_unique<TickAnalysisEngine>(config);
        std::unique_ptr<AuctionAnalyzer> auction_analyzer = std::make_unique<AuctionAnalyzer>(config);
        std::unique_ptr<IVolatilityDetector> volatility_detector = std::make_unique<VolatilityDetector>(config);
        
        // 创建阶段分发器
        PhaseDispatcher dispatcher(tick_engine.get(), auction_analyzer.get(), volatility_detector.get());
        
        // 创建RabbitMQ消费者
        std::unique_ptr<FixedRabbitMQConsumer> consumer = std::make_unique<FixedRabbitMQConsumer>(
            config.rabbitmq_uri, config.queue_name, config.consumer_tag, 
            [&dispatcher, &message_processor](const std::vector<char>& body) {
                std::vector<StockData> records;
                if (message_processor->processMessage(body, records)) {
                    for (auto& data : records) {
                        dispatcher.dispatch(data);
                    }
                }
            }
        );
        
        // 启动消费者
        if (!consumer->start()) {
            if (global_logger) {
                global_logger->error("启动消息消费者失败");
            }
            return 1;
        }
        
        if (global_logger) {
            global_logger->info("系统初始化完成，开始接收数据...");
        }
        
        // 等待用户输入退出
        std::cout << "按Enter键退出程序..." << std::endl;
        std::cin.ignore();
        
        // 关闭系统
        if (global_logger) {
            global_logger->info("正在关闭系统...");
        }
        
        consumer->stop();
        
        if (global_logger) {
            global_logger->info("系统已关闭");
        }
        
    } catch (const std::exception& e) {
        if (global_logger) {
            global_logger->error("程序异常: " + std::string(e.what()));
        } else {
            std::cerr << "程序异常: " << e.what() << std::endl;
        }
        return 1;
    }
    
    return 0;
}