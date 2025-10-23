import os
from datetime import time

# 日志配置
LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(name)s] - %(levelname)s - %(message)s"
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
        },
        "file": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/stock_system.log",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
        },
    },
    "loggers": {
        "": {
            "handlers": ["default", "file"],
            "level": "INFO",
            "propagate": True
        },
    }
}

# 运行模式配置
MODE_CONFIG = {
    "mode": "realtime",  # "realtime" 或 "backtest"
    "backtest_date": "2024-01-15",  # 回测日期
}

# TDengine配置
TDENGINE_CONFIG = {
    "host": os.environ.get("TDENGINE_HOST", "localhost"),
    "port": int(os.environ.get("TDENGINE_PORT", "6030")),
    "user": os.environ.get("TDENGINE_USER", "root"),
    "password": os.environ.get("TDENGINE_PASSWORD", "taosdata"),
    "database": "market_data"
}

# 性能配置
PERFORMANCE_CONFIG = {
    "queue_size": 10000,
    "batch_size": 500,
    "flush_interval": 1.0,
}

# 资源限制
RESOURCE_LIMITS = {
    "max_memory_percent": 80,
    "cleanup_interval": 300,
}

# 交易时间配置
TRADING_HOURS = [
    (time(9, 15), time(11, 30)),
    (time(13, 0), time(15, 0))
]

# Redis配置
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "decode_responses": True
}

# 异动检测配置
VOLATILITY_CONFIG = {
    "volatile_pool_key": "stock:volatile_pool",
    "volatile_expire": 300,  # 异动池数据过期时间(秒)
    "price_change_threshold": 0.02,  # 价格变化阈值2%
    "volume_ratio_threshold": 3.0,   # 量比阈值
    "min_amount_threshold": 1000000, # 最小成交额阈值100万
}

# 竞价分析配置
AUCTION_ANALYSIS_CONFIG = {
    "min_total_amount": 500000,      # 最低总金额50万
    "min_single_order": 100000,      # 最低单笔金额10万  
    "max_withdrawal_rate": 0.8,      # 最大撤单率80%
    "analysis_interval": 10,         # 分析间隔(秒)
}

# RabbitMQ配置
RABBITMQ_CONFIG = {
    "uri": "amqp://admin:admin@localhost:5672/",
    "queue_name": "stream2",
    "consumer_tag": "tdengine-consumer"
}