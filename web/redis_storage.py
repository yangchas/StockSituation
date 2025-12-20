import redis
import json
import time
import zlib
from typing import Dict, List, Optional, Any
import logging
import numpy as np
logger = logging.getLogger(__name__)

class RedisStorageManager:
    """
    高效Redis存储管理器 - 针对内存使用优化
    实现单例模式，确保整个应用只有一个Redis连接实例
    """
    
    _instance = None
    
    def __new__(cls, redis_url: str = "redis://localhost:6379", compression: bool = True):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # 初始化Redis连接和配置
            cls._instance.redis = redis.Redis.from_url(redis_url, decode_responses=True)
            cls._instance.compression = compression
            
            # Redis键前缀（精简）
            cls._instance.STOCK_PREFIX = "s:"      # 个股实时数据
            cls._instance.STOCK_INFO_PREFIX = "si:" # 个股基础信息  
            cls._instance.PLATE_METRICS_PREFIX = "pm:" # 板块指标
            cls._instance.PLATE_INFO_PREFIX = "pi:"   # 板块基础信息
            cls._instance.PLATE_HIERARCHY_PREFIX = "ph:" # 板块层级
            cls._instance.MAIN_PLATES_KEY = "main_plates" # 主板块列表
            cls._instance.CACHE_PREFIX = "cache:"  # 通用缓存数据
            
            # 数据过期时间（秒）
            cls._instance.STOCK_DATA_TTL = 300      # 5分钟
            cls._instance.PLATE_METRICS_TTL = 600   # 10分钟
            cls._instance.BASE_INFO_TTL = 86400 * 7 # 基础信息7天
            cls._instance.CACHE_TTL = 300           # 通用缓存5分钟
        return cls._instance
    
    def __init__(self, redis_url: str = "redis://localhost:6379", compression: bool = True):
        # 单例模式下，__init__会被调用多次，但我们已经在__new__中初始化了所有属性
        # 所以这里不需要做任何事情
        pass
    
    def _convert_numpy_types(self, obj):
        """将numpy类型转换为Python原生类型"""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        else:
            return obj

    def _compress_data(self, data: Any) -> str:
        """压缩数据"""
        # 先转换numpy类型为Python原生类型，确保可以JSON序列化
        converted_data = self._convert_numpy_types(data)
        
        if not self.compression:
            return json.dumps(converted_data, ensure_ascii=False)
        return zlib.compress(json.dumps(converted_data, ensure_ascii=False).encode()).hex()
    
    def _decompress_data(self, compressed_data: str) -> Any:
        """解压数据"""
        if isinstance(compressed_data, (dict, list)):
            # 数据已经是Python对象，直接返回
            return compressed_data
        if not self.compression:
            return json.loads(compressed_data)
        return json.loads(zlib.decompress(bytes.fromhex(compressed_data)))
    
    # ==================== 新增：个股高级指标获取方法 ====================
    
    def get_stock_advanced_indicators(self, symbol: str) -> Dict[str, Any]:
        """
        从Redis获取个股高级技术指标（包含1分钟涨速和2分钟成交额）
        
        Args:
            symbol: 股票代码
            
        Returns:
            Dict: 包含高级指标的字典
        """
        try:
            # 使用C++存储的格式
            key = f"stock:quote:{symbol}"
            stock_data = self.redis.hgetall(key)
            
            if not stock_data:
                return {}
            
            # 转换数据类型
            indicators = {}
            for field, value in stock_data.items():
                field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                
                # 根据字段名转换数据类型
                if field_str in ['price', 'change_pct', 'change_rate_1min']:
                    indicators[field_str] = float(value_str) if value_str else 0.0
                elif field_str in ['volume', 'amount', 'large_net', 'timestamp', 'market_cap', 'amount_2min']:
                    # amount_2min 可能是浮点数，但这里按整数处理，如果需要可单独处理
                    indicators[field_str] = float(value_str) if value_str and '.' in value_str else int(value_str) if value_str else 0
                else:
                    indicators[field_str] = value_str
            
            # 确保包含所有必需字段，提供默认值
            required_fields = {
                'change_rate_1min': 0.0,  # 1分钟涨速
                'amount_2min': 0,         # 2分钟成交额
                'price': 0.0,             # 当前价格
                'change_pct': 0.0,        # 涨跌幅
                'volume': 0,              # 成交量
                'amount': 0,              # 成交额
                'large_net': 0,           # 大单净额
                'timestamp': 0,           # 时间戳
                'name': f"股票{symbol}",   # 股票名称
                'market_cap': 0           # 市值
            }
            
            for field, default_value in required_fields.items():
                if field not in indicators:
                    indicators[field] = default_value
            
            logger.debug(f"✅ 获取个股高级指标: {symbol}, 1分钟涨速: {indicators.get('change_rate_1min', 0)}, 2分钟成交额: {indicators.get('amount_2min', 0)}")
            return indicators
            
        except Exception as e:
            logger.error(f"❌ 获取个股高级指标失败 {symbol}: {e}")
            return {}
    
    def get_limit_up_stocks(self) -> List[Dict[str, Any]]:
        """
        从Redis获取涨停票列表
        
        Returns:
            List[Dict]: 涨停票列表，包含股票代码、名称、价格等信息
        """
        try:
            # 从volatile_pool中获取所有异动票
            volatile_pool_key = "stock:volatile_pool"
            
            # 检查键是否存在
            if not self.redis.exists(volatile_pool_key):
                logger.warning("Redis键不存在: {volatile_pool_key}")
                return []
            
            # 获取所有异动票
            all_volatile_stocks = self.redis.zrange(volatile_pool_key, 0, -1)
            limit_up_stocks = []
            
            # 遍历所有异动票，筛选出涨停票
            for symbol in all_volatile_stocks:
                # 获取股票详细数据
                stock_data = self.get_stock_advanced_indicators(symbol)
                
                if not stock_data:
                    continue
                
                # 检查是否为涨停票（涨跌幅接近10%或20%）
                change_pct = stock_data.get('change_pct', 0.0)
                
                # 考虑10%和20%的涨停情况（创业板、科创板）
                if (9.8 <= change_pct <= 10.2) or (19.8 <= change_pct <= 20.2):
                    # 从Redis获取股票名称
                    stock_info_key = f"stock:quote:{symbol}"
                    stock_name = self.redis.hget(stock_info_key, 'name')
                    if stock_name:
                        stock_data['name'] = stock_name
                    
                    stock_data['symbol'] = symbol
                    limit_up_stocks.append(stock_data)
            
            logger.info(f"✅ 从Redis获取涨停票: {len(limit_up_stocks)}只")
            return limit_up_stocks
            
        except Exception as e:
            logger.error(f"❌ 获取涨停票失败: {e}")
            return []
    
    def get_first_limit_up_stocks(self) -> List[Dict[str, Any]]:
        """
        从Redis获取首板票列表（涨停但不是昨日涨停的个股）
        逻辑：获取今日所有涨停票，然后排除昨日涨停的股票，剩下的就是今日首板票
        
        Returns:
            List[Dict]: 首板票列表，包含股票代码、名称、价格等信息
        """
        try:
            from datetime import datetime, timedelta
            import datetime as dt
            
            # 步骤1: 获取今日所有涨停票
            # 先尝试从异动池获取
            volatile_pool_key = "stock:volatile_pool"
            today_limit_up_stocks = []
            
            if self.redis.exists(volatile_pool_key):
                # 从异动池获取涨停票
                all_volatile_stocks = self.redis.zrange(volatile_pool_key, 0, -1)
                
                for symbol in all_volatile_stocks:
                    # 获取股票详细数据
                    stock_data = self.get_stock_advanced_indicators(symbol)
                    
                    if not stock_data:
                        continue
                    
                    # 检查是否为涨停票（涨跌幅接近10%或20%）
                    change_pct = stock_data.get('change_pct', 0.0)
                    
                    # 考虑10%和20%的涨停情况（创业板、科创板）
                    if (9.8 <= change_pct <= 10.2) or (19.8 <= change_pct <= 20.2):
                        # 从Redis获取股票名称
                        stock_info_key = f"stock:quote:{symbol}"
                        stock_name = self.redis.hget(stock_info_key, 'name')
                        if stock_name:
                            stock_data['name'] = stock_name
                        
                        stock_data['symbol'] = symbol
                        today_limit_up_stocks.append(stock_data)
            else:
                # 异动池不存在时，直接返回空列表
                logger.warning(f"⚠️ Redis键不存在: {volatile_pool_key}")
                return []
            
            logger.info(f"📊 今日涨停票数量: {len(today_limit_up_stocks)}")
            
            # 步骤2: 获取昨日涨停的股票
            today = datetime.now().strftime('%Y-%m-%d')
            today_date = dt.datetime.strptime(today, '%Y-%m-%d')
            prev_date = today_date - timedelta(days=1)
            prev_day = prev_date.strftime('%Y-%m-%d')
            
            # 构造昨日涨停数据缓存键
            prev_limit_up_key = f"limit_up_{prev_day}"
            
            # 从Redis缓存获取昨日涨停数据
            prev_limit_up_data = self.get_data(prev_limit_up_key)
            
            # 提取昨日涨停的股票代码列表
            prev_limit_up_symbols = set()
            if prev_limit_up_data:
                for stock in prev_limit_up_data:
                    symbol = stock.get('code', '')
                    if symbol:
                        prev_limit_up_symbols.add(symbol)
            
            logger.info(f"📊 昨日涨停的股票数量: {len(prev_limit_up_symbols)}")
            
            # 步骤3: 筛选今日首板票（今日涨停但昨日未涨停的股票）
            first_limit_stocks = []
            for stock in today_limit_up_stocks:
                symbol = stock.get('symbol', '')
                if symbol and symbol not in prev_limit_up_symbols:
                    # 转换数据格式，确保与原有方法返回格式一致
                    formatted_stock = {
                        'symbol': symbol,
                        'name': stock.get('name', f"股票{symbol}"),
                        'price': stock.get('price', 0.0),
                        'change_pct': stock.get('change_pct', 0.0),
                        'amount': stock.get('amount', 0.0),
                        'timestamp': int(datetime.now().timestamp() * 1000),  # 当前时间戳
                        'plate': stock.get('plate', '涨停板块'),
                        'limit_time': stock.get('limit_time', '09:30:00')
                    }
                    first_limit_stocks.append(formatted_stock)
            
            logger.info(f"✅ 筛选出今日首板票: {len(first_limit_stocks)}只")
            return first_limit_stocks
            
        except Exception as e:
            logger.error(f"❌ 获取首板票失败: {e}")
            return []
    
    def batch_get_stocks_advanced_indicators(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个股票的高级指标
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            Dict: {symbol: 高级指标字典}
        """
        try:
            pipeline = self.redis.pipeline()
            
            # 批量获取
            for symbol in symbols:
                key = f"stock:quote:{symbol}"
                pipeline.hgetall(key)
            
            results = pipeline.execute()
            indicators_dict = {}
            
            for i, symbol in enumerate(symbols):
                stock_data = results[i]
                if stock_data:
                    # 转换数据类型
                    indicators = {}
                    for field, value in stock_data.items():
                        field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                        value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                        
                        if field_str in ['price', 'change_pct', 'change_rate_1min']:
                            indicators[field_str] = float(value_str) if value_str else 0.0
                        elif field_str in ['volume', 'amount', 'large_net', 'timestamp', 'market_cap', 'amount_2min']:
                            indicators[field_str] = float(value_str) if value_str and '.' in value_str else int(value_str) if value_str else 0
                        else:
                            indicators[field_str] = value_str
                    
                    # 确保包含高级指标字段
                    if 'change_rate_1min' not in indicators:
                        indicators['change_rate_1min'] = 0.0
                    if 'amount_2min' not in indicators:
                        indicators['amount_2min'] = 0
                    
                    indicators_dict[symbol] = indicators
            
            # logger.info(f"✅ 批量获取 {len(indicators_dict)}/{len(symbols)} 只股票的高级指标")
            return indicators_dict
            
        except Exception as e:
            logger.error(f"❌ 批量获取股票高级指标失败: {e}")
            return {}
    
    # ==================== 新增：板块高级指标计算方法 ====================
    
    def calculate_plate_advanced_indicators(self, plate_stocks: List[str]) -> Dict[str, Any]:
        """
        计算板块的高级技术指标（基于板块内个股的Redis数据）
        
        Args:
            plate_stocks: 板块成分股代码列表
            
        Returns:
            Dict: 板块高级指标
        """
        try:
            if not plate_stocks:
                return {}
            
            # 批量获取板块内所有股票的高级指标
            stocks_indicators = self.batch_get_stocks_advanced_indicators(plate_stocks)
            
            plate_change_rates = []
            plate_amounts_2min = []
            valid_stocks = 0
            
            for symbol, indicators in stocks_indicators.items():
                change_rate = indicators.get('change_rate_1min', 0)
                amount_2min = indicators.get('amount_2min', 0)
                
                # 只使用有效数据
                if change_rate is not None:
                    plate_change_rates.append(change_rate)
                    plate_amounts_2min.append(amount_2min)
                    valid_stocks += 1
            
            if not plate_change_rates:
                return {}
            
            # 计算板块整体指标
            
            avg_change_rate = np.mean(plate_change_rates)
            total_amount_2min = np.sum(plate_amounts_2min)
            
            # 计算涨跌家数
            rise_count = sum(1 for rate in plate_change_rates if rate > 0)
            fall_count = sum(1 for rate in plate_change_rates if rate < 0)
            
            result = {
                'avg_change_rate_1min': round(avg_change_rate, 4),  # 板块平均1分钟涨速
                'total_amount_2min': round(total_amount_2min, 2),   # 板块2分钟总成交额
                'rise_count': rise_count,                           # 上涨家数
                'fall_count': fall_count,                           # 下跌家数
                'flat_count': valid_stocks - rise_count - fall_count, # 平盘家数
                'total_stocks': valid_stocks,                       # 有效股票数量
                'update_time': time.time(),
                'data_source': 'redis_aggregated'
            }
            
            logger.debug(f"✅ 计算板块高级指标: 股票数{valid_stocks}, 平均涨速{avg_change_rate:.4f}, 总成交额{total_amount_2min:.2f}")
            return result
            
        except Exception as e:
            logger.error(f"❌ 计算板块高级指标失败: {e}")
            return {}
    
    # ==================== 原有的通用缓存方法 ====================
    
    def store_data(self, key: str, data: Any, expire_seconds: int = None) -> bool:
        """
        存储通用数据到缓存
        """
        try:
            if expire_seconds is None:
                expire_seconds = self.CACHE_TTL
                
            cache_key = f"{self.CACHE_PREFIX}{key}"
            compressed_data = self._compress_data(data)
            
            self.redis.setex(cache_key, expire_seconds, compressed_data)
            return True
            
        except Exception as e:
            logger.error(f"❌ 存储缓存数据失败 key={key}: {e}")
            return False
    
    def get_data(self, key: str) -> Any:
        """
        从缓存获取通用数据
        """
        try:
            cache_key = f"{self.CACHE_PREFIX}{key}"
            compressed_data = self.redis.get(cache_key)
            
            if compressed_data is None:
                return None
                
            return self._decompress_data(compressed_data)
            
        except Exception as e:
            logger.error(f"❌ 获取缓存数据失败 key={key}: {e}")
            return None
    
    def delete_data(self, key: str) -> bool:
        """
        删除缓存数据
        """
        try:
            cache_key = f"{self.CACHE_PREFIX}{key}"
            return bool(self.redis.delete(cache_key))
        except Exception as e:
            logger.error(f"❌ 删除缓存数据失败 key={key}: {e}")
            return False
    
    def exists_data(self, key: str) -> bool:
        """
        检查缓存数据是否存在
        """
        try:
            cache_key = f"{self.CACHE_PREFIX}{key}"
            return bool(self.redis.exists(cache_key))
        except Exception as e:
            logger.error(f"❌ 检查缓存数据失败 key={key}: {e}")
            return False
    
    def get_cache_keys(self, pattern: str = "*") -> List[str]:
        """
        获取匹配模式的缓存键列表
        """
        try:
            full_pattern = f"{self.CACHE_PREFIX}{pattern}"
            keys = list(self.redis.scan_iter(match=full_pattern))
            # 移除前缀返回
            return [key[len(self.CACHE_PREFIX):] for key in keys]
        except Exception as e:
            logger.error(f"❌ 获取缓存键列表失败 pattern={pattern}: {e}")
            return []
    
    def clear_cache_pattern(self, pattern: str = "*") -> int:
        """
        清除匹配模式的缓存数据
        """
        try:
            keys = self.get_cache_keys(pattern)
            if not keys:
                return 0
                
            # 为每个键添加前缀
            full_keys = [f"{self.CACHE_PREFIX}{key}" for key in keys]
            deleted_count = self.redis.delete(*full_keys)
            logger.info(f"🧹 清除缓存数据: {len(keys)} 个键")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ 清除缓存数据失败 pattern={pattern}: {e}")
            return 0
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        """
        try:
            cache_keys = self.get_cache_keys()
            memory_info = self.get_memory_info()
            
            return {
                "total_cache_keys": len(cache_keys),
                "cache_keys_sample": cache_keys[:10],  # 前10个作为样本
                "memory_usage": memory_info,
                "cache_prefix": self.CACHE_PREFIX
            }
        except Exception as e:
            logger.error(f"❌ 获取缓存统计信息失败: {e}")
            return {}
    
    # ==================== 原有的个股数据操作 ====================
    
    def start_pipeline(self):
        """开始批量操作"""
        return self.redis.pipeline()
    
    def execute_pipeline(self, pipeline):
        """执行批量操作"""
        return pipeline.execute()
    
    def update_stock_data(self, stock_id: str, data: Dict, plates: List[str] = None, 
                         pipeline: Optional[Any] = None) -> None:
        """
        更新个股数据和所属板块
        """
        redis_client = pipeline or self.redis
        
        # 精简的个股实时数据
        stock_data = {
            "p": float(data.get("price", 0)),           # 价格
            "c": float(data.get("change_pct", 0)),      # 涨跌幅
            "v": int(data.get("volume", 0)),            # 成交量
            "t": int(data.get("timestamp", time.time())) # 时间戳
        }
        
        # 存储个股实时数据
        stock_key = f"{self.STOCK_PREFIX}{stock_id}"
        redis_client.hset(stock_key, mapping=stock_data)
        redis_client.expire(stock_key, self.STOCK_DATA_TTL)
        
        # 存储个股基础信息（如果不频繁更新）
        if "name" in data or "market_cap" in data:
            stock_info = {}
            if "name" in data:
                stock_info["n"] = data["name"]  # 名称
            if "market_cap" in data:
                stock_info["m"] = int(data["market_cap"])  # 市值
                
            if stock_info:
                info_key = f"{self.STOCK_INFO_PREFIX}{stock_id}"
                redis_client.hset(info_key, mapping=stock_info)
                redis_client.expire(info_key, self.BASE_INFO_TTL)
        
        # 更新个股-板块关系
        if plates:
            plates_key = f"stock_plates:{stock_id}"
            redis_client.delete(plates_key)  # 先删除旧的
            if plates:
                redis_client.sadd(plates_key, *plates)
            redis_client.expire(plates_key, self.BASE_INFO_TTL)
    
    def batch_update_stocks(self, stock_updates: Dict[str, Dict]):
        """批量更新股票数据到Redis - 同时支持C++和Python访问"""
        try:
            pipeline = self.redis.pipeline()
            
            for stock_id, data in stock_updates.items():
                key = f"stock:quote:{stock_id}"
                
                # 存储到哈希表（供C++读取）
                pipeline.hset(key, "price", str(data.get('price', 0.0)))
                pipeline.hset(key, "change_pct", str(data.get('change_pct', 0.0)))
                pipeline.hset(key, "volume", str(data.get('volume', 0)))
                pipeline.hset(key, "large_net", str(data.get('large_net', 0)))
                pipeline.hset(key, "timestamp", str(data.get('timestamp', int(time.time()))))
                pipeline.hset(key, "name", data.get('name', f"股票{stock_id}"))
                pipeline.hset(key, "market_cap", str(data.get('market_cap', 0)))
                
                # 设置过期时间
                pipeline.expire(key, 300)  # 5分钟
            
            pipeline.execute()
            logger.info(f"💾 批量更新 {len(stock_updates)} 只股票数据到Redis")
            
        except Exception as e:
            logger.error(f"❌ 批量更新股票数据到Redis失败: {e}")
    
    def get_stock_data(self, stock_id: str) -> Dict:
        """从Redis获取股票数据 - 兼容C++和Python两种格式"""
        try:
            # 尝试从哈希表获取（C++格式）
            key = f"stock:quote:{stock_id}"
            stock_data = self.redis.hgetall(key)
            
            if stock_data:
                # 转换数据类型
                decoded_data = {}
                for field, value in stock_data.items():
                    field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                    value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                    
                    # 根据字段名转换数据类型
                    if field_str in ['price', 'change_pct']:
                        decoded_data[field_str] = float(value_str) if value_str else 0.0
                    elif field_str in ['volume', 'large_net', 'timestamp', 'market_cap']:
                        decoded_data[field_str] = int(value_str) if value_str else 0
                    else:
                        decoded_data[field_str] = value_str
                
                # 确保包含所有必需字段
                required_fields = ['price', 'change_pct', 'volume', 'large_net', 'timestamp', 'name']
                for field in required_fields:
                    if field not in decoded_data:
                        decoded_data[field] = 0.0 if field in ['price', 'change_pct'] else 0
                
                return decoded_data
            else:
                return None
                
        except Exception as e:
            logger.error(f"❌ 从Redis获取股票数据失败 {stock_id}: {e}")
            return None
    
    def get_stock_data1(self, stock_id: str) -> Optional[Dict]:
        """获取个股数据"""
        stock_key = f"{self.STOCK_PREFIX}{stock_id}"
        info_key = f"{self.STOCK_INFO_PREFIX}{stock_id}"
        
        # 获取实时数据
        stock_data = self.redis.hgetall(stock_key)
        if not stock_data:
            return None
        
        # 获取基础信息
        stock_info = self.redis.hgetall(info_key)
        
        # 合并数据并转换字段名
        result = {
            "code": stock_id,
            "price": float(stock_data.get("p", 0)),
            "change_pct": float(stock_data.get("c", 0)),
            "volume": int(stock_data.get("v", 0)),
            "timestamp": int(stock_data.get("t", 0))
        }
        
        if "n" in stock_info:
            result["name"] = stock_info["n"]
        if "m" in stock_info:
            result["market_cap"] = int(stock_info["m"])
        
        return result
    
    def get_stock_plates(self, stock_id: str) -> List[str]:
        """获取个股所属板块"""
        plates_key = f"stock_plates:{stock_id}"
        return list(self.redis.smembers(plates_key))
    
    # ==================== 原有的板块数据操作 ====================
    
    def update_plate_metrics(self, plate_id: str, metrics: Dict, pipeline: Optional[Any] = None) -> None:
        """
        更新板块指标
        """
        redis_client = pipeline or self.redis
        
        plate_metrics = {
            "c": float(metrics.get("change_pct", 0)),      # 涨跌幅
            "v": int(metrics.get("total_volume", 0)),      # 总成交额
            "r": int(metrics.get("rise_count", 0)),        # 上涨家数
            "f": int(metrics.get("fall_count", 0)),        # 下跌家数
            "s": int(metrics.get("stock_count", 0)),       # 成分股数量
            "t": int(metrics.get("timestamp", time.time())) # 时间戳
        }
        
        metrics_key = f"{self.PLATE_METRICS_PREFIX}{plate_id}"
        redis_client.hset(metrics_key, mapping=plate_metrics)
        redis_client.expire(metrics_key, self.PLATE_METRICS_TTL)
    
    def update_plate_info(self, plate_id: str, plate_info: Dict, pipeline: Optional[Any] = None) -> None:
        """更新板块基础信息"""
        redis_client = pipeline or self.redis
        
        info_data = {
            "n": plate_info.get("name", ""),           # 板块名称
            "t": plate_info.get("type", "sub"),        # 类型: main/sub
            "m": int(plate_info.get("market_cap", 0)), # 流通值
        }
        
        # 如果是子板块，记录父板块
        if "parent" in plate_info:
            info_data["p"] = plate_info["parent"]
        
        info_key = f"{self.PLATE_INFO_PREFIX}{plate_id}"
        redis_client.hset(info_key, mapping=info_data)
        redis_client.expire(info_key, self.BASE_INFO_TTL)
    
    def batch_update_plates(self, plate_metrics: Dict[str, Dict], plate_infos: Dict[str, Dict] = None) -> None:
        """批量更新板块数据"""
        pipeline = self.start_pipeline()
        
        # 更新板块指标
        for plate_id, metrics in plate_metrics.items():
            self.update_plate_metrics(plate_id, metrics, pipeline)
        
        # 更新板块基础信息
        if plate_infos:
            for plate_id, info in plate_infos.items():
                self.update_plate_info(plate_id, info, pipeline)
        
        self.execute_pipeline(pipeline)
        logger.info(f"✅ 批量更新 {len(plate_metrics)} 个板块数据到Redis")
    
    def get_plate_data(self, plate_id: str) -> Optional[Dict]:
        """获取板块完整数据"""
        metrics_key = f"{self.PLATE_METRICS_PREFIX}{plate_id}"
        info_key = f"{self.PLATE_INFO_PREFIX}{plate_id}"
        
        # 获取指标数据
        metrics_data = self.redis.hgetall(metrics_key)
        if not metrics_data:
            return None
        
        # 获取基础信息
        info_data = self.redis.hgetall(info_key)
        
        # 合并数据
        result = {
            "id": plate_id,
            "name": info_data.get("n", ""),
            "change_pct": float(metrics_data.get("c", 0)),
            "total_volume": int(metrics_data.get("v", 0)),
            "rise_count": int(metrics_data.get("r", 0)),
            "fall_count": int(metrics_data.get("f", 0)),
            "stock_count": int(metrics_data.get("s", 0)),
            "timestamp": int(metrics_data.get("t", 0)),
            "type": info_data.get("t", "sub"),
            "market_cap": int(info_data.get("m", 0))
        }
        
        # 如果是子板块，添加父板块信息
        if "p" in info_data:
            result["parent"] = info_data["p"]
        
        return result
    
    def get_plate_stocks(self, plate_id: str) -> List[Dict]:
        """获取板块成分股数据"""
        # 注意：这个方法需要遍历所有股票，性能较低
        # 在实际应用中，建议维护板块-股票的反向索引
        stocks = []
        pattern = f"{self.STOCK_PREFIX}*"
        
        for stock_key in self.redis.scan_iter(match=pattern):
            stock_id = stock_key[len(self.STOCK_PREFIX):]
            
            # 检查该股票是否属于该板块
            plates = self.get_stock_plates(stock_id)
            if plate_id in plates:
                stock_data = self.get_stock_data(stock_id)
                if stock_data:
                    stocks.append(stock_data)
        
        return stocks
    
    # ==================== 原有的板块层级关系操作 ====================
    
    def update_plate_hierarchy(self, main_plate_id: str, sub_plate_ids: List[str], 
                              pipeline: Optional[Any] = None) -> None:
        """更新板块层级关系"""
        redis_client = pipeline or self.redis
        
        hierarchy_key = f"{self.PLATE_HIERARCHY_PREFIX}{main_plate_id}"
        
        # 删除旧的并添加新的
        redis_client.delete(hierarchy_key)
        if sub_plate_ids:
            redis_client.rpush(hierarchy_key, *sub_plate_ids)
        
        # 添加到主板块集合
        redis_client.sadd(self.MAIN_PLATES_KEY, main_plate_id)
    
    def get_sub_plates(self, main_plate_id: str) -> List[Dict]:
        """获取子板块列表及数据"""
        hierarchy_key = f"{self.PLATE_HIERARCHY_PREFIX}{main_plate_id}"
        sub_plate_ids = self.redis.lrange(hierarchy_key, 0, -1)
        
        sub_plates = []
        for plate_id in sub_plate_ids:
            plate_data = self.get_plate_data(plate_id)
            if plate_data:
                sub_plates.append(plate_data)
        
        return sub_plates
    
    def get_main_plates(self) -> List[Dict]:
        """获取所有主板块数据"""
        main_plate_ids = self.redis.smembers(self.MAIN_PLATES_KEY)
        
        main_plates = []
        for plate_id in main_plate_ids:
            plate_data = self.get_plate_data(plate_id)
            if plate_data:
                main_plates.append(plate_data)
        
        return main_plates
    
    def initialize_plate_hierarchy(self, plate_hierarchy: Dict[str, List[str]]) -> None:
        """初始化板块层级关系"""
        pipeline = self.start_pipeline()
        
        for main_plate_id, sub_plate_ids in plate_hierarchy.items():
            self.update_plate_hierarchy(main_plate_id, sub_plate_ids, pipeline)
        
        self.execute_pipeline(pipeline)
        logger.info(f"✅ 初始化板块层级关系: {len(plate_hierarchy)} 个主板块")
    
    # ==================== 原有的工具方法 ====================
    
    def get_all_plate_metrics(self) -> List[Dict]:
        """获取所有板块指标（用于前端初始化）"""
        plates = []
        pattern = f"{self.PLATE_METRICS_PREFIX}*"
        
        for metrics_key in self.redis.scan_iter(match=pattern):
            plate_id = metrics_key[len(self.PLATE_METRICS_PREFIX):]
            plate_data = self.get_plate_data(plate_id)
            if plate_data:
                plates.append(plate_data)
        
        return plates
    
    def cleanup_old_data(self) -> None:
        """清理过期数据（可定期执行）"""
        # Redis会自动清理有过期时间的数据
        # 这里主要清理无过期时间的基础数据（如果需要）
        logger.info("🧹 执行Redis数据清理")
    
    def get_memory_info(self) -> Dict:
        """获取内存使用信息"""
        info = self.redis.info("memory")
        return {
            "used_memory": info.get("used_memory", 0),
            "used_memory_human": info.get("used_memory_human", "0"),
            "used_memory_peak": info.get("used_memory_peak", 0),
            "used_memory_peak_human": info.get("used_memory_peak_human", "0")
        }