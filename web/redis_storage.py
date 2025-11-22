import redis
import json
import time
import zlib
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class RedisStorageManager:
    """
    高效Redis存储管理器 - 针对内存使用优化
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", compression: bool = True):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.compression = compression
        
        # Redis键前缀（精简）
        self.STOCK_PREFIX = "s:"      # 个股实时数据
        self.STOCK_INFO_PREFIX = "si:" # 个股基础信息  
        self.PLATE_METRICS_PREFIX = "pm:" # 板块指标
        self.PLATE_INFO_PREFIX = "pi:"   # 板块基础信息
        self.PLATE_HIERARCHY_PREFIX = "ph:" # 板块层级
        self.MAIN_PLATES_KEY = "main_plates" # 主板块列表
        
        # 数据过期时间（秒）
        self.STOCK_DATA_TTL = 300      # 5分钟
        self.PLATE_METRICS_TTL = 600   # 10分钟
        self.BASE_INFO_TTL = 86400 * 7 # 基础信息7天
    
    def _compress_data(self, data: Dict) -> str:
        """压缩数据"""
        if not self.compression:
            return json.dumps(data, ensure_ascii=False)
        return zlib.compress(json.dumps(data, ensure_ascii=False).encode()).hex()
    
    def _decompress_data(self, compressed_data: str) -> Dict:
        """解压数据"""
        if not self.compression:
            return json.loads(compressed_data)
        return json.loads(zlib.decompress(bytes.fromhex(compressed_data)))
    
    def start_pipeline(self):
        """开始批量操作"""
        return self.redis.pipeline()
    
    def execute_pipeline(self, pipeline):
        """执行批量操作"""
        return pipeline.execute()
    
    # ==================== 个股数据操作 ====================
    
    def update_stock_data(self, stock_id: str, data: Dict, plates: List[str] = None, 
                         pipeline: Optional[Any] = None) -> None:
        """
        更新个股数据和所属板块
        
        Args:
            stock_id: 股票代码
            data: 股票数据 {p:价格, c:涨跌幅, v:成交量, t:时间戳}
            plates: 所属板块ID列表
            pipeline: Redis管道对象
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
    
    def batch_update_stocks(self, stock_updates: Dict[str, Dict]) -> None:
        """批量更新个股数据"""
        pipeline = self.start_pipeline()
        
        for stock_id, data in stock_updates.items():
            plates = data.get("plates")
            self.update_stock_data(stock_id, data, plates, pipeline)
        
        self.execute_pipeline(pipeline)
        logger.info(f"✅ 批量更新 {len(stock_updates)} 只股票数据到Redis")
    
    def get_stock_data(self, stock_id: str) -> Optional[Dict]:
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
    
    # ==================== 板块数据操作 ====================
    
    def update_plate_metrics(self, plate_id: str, metrics: Dict, pipeline: Optional[Any] = None) -> None:
        """
        更新板块指标
        
        Args:
            plate_id: 板块ID
            metrics: 板块指标 {c:涨跌幅, v:总成交额, r:上涨家数, f:下跌家数, s:成分股数量, t:时间戳}
            pipeline: Redis管道对象
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
        if(plate_id == "801128" or plate_id == "801709"):
            print("国产软件：",plate_id,metrics_data)
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
    
    # ==================== 板块层级关系操作 ====================
    
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
        print("获取子板块列表及数据",main_plate_id,sub_plate_ids)
        sub_plates = []
        for plate_id in sub_plate_ids:
            plate_data = self.get_plate_data(plate_id)
            if plate_data:
                sub_plates.append(plate_data)
        
        return sub_plates
    
    def get_main_plates(self) -> List[Dict]:
        """获取所有主板块数据"""
        main_plate_ids = self.redis.smembers(self.MAIN_PLATES_KEY)
        print("获取所有主板块数据 ",main_plate_ids)
        
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
    
    # ==================== 工具方法 ====================
    
    def get_all_plate_metrics(self) -> List[Dict]:
        """获取所有板块指标（用于前端初始化）"""
        plates = []
        pattern = f"{self.PLATE_METRICS_PREFIX}*"
        print("获取所有板块指标（用于前端初始化）",pattern)
        
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