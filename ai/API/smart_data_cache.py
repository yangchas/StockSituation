"""
三级缓存系统 SmartDataCache
内存缓存 → Redis缓存 → 文件存储
"""

import asyncio
import json
import logging
import time
import os
from collections import OrderedDict
from typing import Any, Callable, Optional, Dict
from datetime import datetime

import redis.asyncio as redis

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SmartDataCache')

class DataCategory:
    """数据分类和对应的TTL配置"""
    REAL_TIME_PRICE = 'real_time_price'  # 实时价格
    BIG_ORDER_NET_AMOUNT = 'big_order_net_amount'  # 大单净额
    LIMIT_UP_STATUS = 'limit_up_status'  # 涨停状态
    HOT_LIST = 'hot_list'  # 热榜数据
    LIMIT_UP_REASON = 'limit_up_reason'  # 涨停原因
    STOCK_GENE = 'stock_gene'  # 股票基因
    DEFAULT = 'default'  # 默认类型

# TTL配置（单位：秒）
TTL_CONFIG = {
    DataCategory.REAL_TIME_PRICE: 3,
    DataCategory.BIG_ORDER_NET_AMOUNT: 5,
    DataCategory.LIMIT_UP_STATUS: 10,
    DataCategory.HOT_LIST: 30,
    DataCategory.LIMIT_UP_REASON: 3600,  # 1小时
    DataCategory.STOCK_GENE: 86400,  # 1天
    DataCategory.DEFAULT: 300  # 默认5分钟
}

class LRUCache:
    """LRU内存缓存实现"""
    
    def __init__(self, capacity: int = 5000):
        self.capacity = capacity
        self.cache = OrderedDict()
        
    def get(self, key: str) -> Optional[Any]:
        """获取缓存数据"""
        if key not in self.cache:
            return None
        
        # 将访问的key移到末尾（最近使用）
        value = self.cache.pop(key)
        self.cache[key] = value
        return value
    
    def set(self, key: str, value: Any) -> None:
        """设置缓存数据"""
        if key in self.cache:
            # 如果key已存在，先删除再重新插入
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            # 如果达到容量限制，删除最久未使用的项
            self.cache.popitem(last=False)
        
        self.cache[key] = value
    
    def delete(self, key: str) -> bool:
        """删除缓存数据"""
        if key in self.cache:
            self.cache.pop(key)
            return True
        return False
    
    def clear(self) -> None:
        """清空缓存"""
        self.cache.clear()
    
    def size(self) -> int:
        """返回当前缓存大小"""
        return len(self.cache)

class FileStorage:
    """文件存储实现（替代TDengine）"""
    
    def __init__(self, storage_dir: str = 'cache_storage'):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
    
    def _get_file_path(self, key: str) -> str:
        """获取文件路径"""
        # 使用key的hash作为文件名，避免特殊字符问题
        import hashlib
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.storage_dir, f"{key_hash}.json")
    
    def save(self, key: str, data: Any) -> None:
        """保存数据到文件"""
        try:
            file_path = self._get_file_path(key)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'key': key,
                    'data': data,
                    'timestamp': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"文件存储失败: {e}")
    
    def load(self, key: str) -> Optional[Any]:
        """从文件加载数据"""
        try:
            file_path = self._get_file_path(key)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                    return cached_data['data']
        except Exception as e:
            logger.warning(f"文件加载失败: {e}")
        return None
    
    def delete(self, key: str) -> bool:
        """删除文件"""
        try:
            file_path = self._get_file_path(key)
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
        except Exception as e:
            logger.warning(f"文件删除失败: {e}")
        return False

class SmartDataCache:
    """三级缓存系统：内存(LRU)→Redis→文件存储"""
    
    def __init__(self, 
                 redis_host: str = 'localhost',
                 redis_port: int = 6379,
                 redis_db: int = 0,
                 redis_password: str = None,
                 storage_dir: str = 'cache_storage'):
        
        # 内存缓存
        self.memory_cache = LRUCache(capacity=5000)
        
        # Redis连接配置
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.redis_client = None
        
        # 文件存储
        self.file_storage = FileStorage(storage_dir)
        
        # 连接状态
        self.redis_connected = False
        
        # 缓存统计
        self.stats = {
            'memory_hits': 0,
            'redis_hits': 0,
            'file_hits': 0,
            'total_requests': 0,
            'errors': 0
        }
        
        logger.info("SmartDataCache初始化完成")
    
    async def init_connections(self) -> None:
        """初始化连接"""
        try:
            # 初始化Redis连接
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True
            )
            
            # 测试Redis连接
            await self.redis_client.ping()
            self.redis_connected = True
            logger.info("Redis连接成功")
                
        except Exception as e:
            logger.warning(f"Redis连接失败，将使用内存和文件缓存: {e}")
            self.redis_connected = False
    
    def _get_ttl(self, category: str) -> int:
        """根据分类获取TTL"""
        return TTL_CONFIG.get(category, TTL_CONFIG[DataCategory.DEFAULT])
    
    async def get(self, key: str, fetch_func: Optional[Callable] = None, 
                 force_fresh: bool = False, category: str = DataCategory.DEFAULT) -> Any:
        """
        获取数据，如果不存在则调用fetch_func
        
        Args:
            key: 缓存键
            fetch_func: 数据获取函数
            force_fresh: 是否强制刷新
            category: 数据类型分类
            
        Returns:
            Any: 缓存数据
        """
        start_time = time.time()
        self.stats['total_requests'] += 1
        
        # 如果强制刷新，直接跳过缓存
        if force_fresh:
            logger.info(f"强制刷新数据: {key}")
            if fetch_func:
                data = await self._fetch_and_cache(key, fetch_func, category)
                logger.info(f"数据获取完成，耗时: {time.time() - start_time:.3f}s")
                return data
            else:
                logger.warning(f"强制刷新但未提供fetch_func: {key}")
                return None
        
        # 1. 检查内存缓存
        memory_data = self.memory_cache.get(key)
        if memory_data is not None:
            logger.debug(f"内存缓存命中: {key}")
            self.stats['memory_hits'] += 1
            logger.info(f"数据获取完成，耗时: {time.time() - start_time:.3f}s")
            return memory_data
        
        # 2. 检查Redis缓存
        if self.redis_connected:
            try:
                redis_data = await self.redis_client.get(key)
                if redis_data is not None:
                    data = json.loads(redis_data)
                    # 写回内存缓存
                    self.memory_cache.set(key, data)
                    logger.debug(f"Redis缓存命中: {key}")
                    self.stats['redis_hits'] += 1
                    logger.info(f"数据获取完成，耗时: {time.time() - start_time:.3f}s")
                    return data
            except Exception as e:
                logger.warning(f"Redis读取失败: {e}")
        
        # 3. 检查文件存储
        file_data = self.file_storage.load(key)
        if file_data is not None:
            # 写回Redis和内存缓存
            if self.redis_connected:
                await self._set_to_redis(key, file_data, category)
            self.memory_cache.set(key, file_data)
            logger.debug(f"文件存储命中: {key}")
            self.stats['file_hits'] += 1
            logger.info(f"数据获取完成，耗时: {time.time() - start_time:.3f}s")
            return file_data
        
        # 4. 调用获取函数
        if fetch_func:
            data = await self._fetch_and_cache(key, fetch_func, category)
            logger.info(f"数据获取完成，耗时: {time.time() - start_time:.3f}s")
            return data
        else:
            logger.warning(f"数据不存在且未提供fetch_func: {key}")
            return None
    
    async def _fetch_and_cache(self, key: str, fetch_func: Callable, category: str) -> Any:
        """获取数据并缓存到各级存储"""
        try:
            # 调用获取函数
            if asyncio.iscoroutinefunction(fetch_func):
                data = await fetch_func()
            else:
                data = fetch_func()
            
            if data is not None:
                # 缓存到各级存储
                await self.set(key, data, category)
                return data
            else:
                logger.warning(f"fetch_func返回空数据: {key}")
                return None
        except Exception as e:
            logger.error(f"数据获取失败: {e}")
            self.stats['errors'] += 1
            raise
    
    async def set(self, key: str, data: Any, category: str = DataCategory.DEFAULT) -> None:
        """设置缓存数据"""
        try:
            # 1. 设置内存缓存
            self.memory_cache.set(key, data)
            
            # 2. 设置Redis缓存
            await self._set_to_redis(key, data, category)
            
            # 3. 持久化到文件存储
            self.file_storage.save(key, data)
            
            logger.debug(f"数据缓存成功: {key}")
        except Exception as e:
            logger.error(f"数据缓存失败: {e}")
            self.stats['errors'] += 1
    
    async def _set_to_redis(self, key: str, data: Any, category: str) -> None:
        """设置Redis缓存"""
        if self.redis_connected:
            try:
                ttl = self._get_ttl(category)
                serialized_data = json.dumps(data, ensure_ascii=False)
                await self.redis_client.setex(key, ttl, serialized_data)
            except Exception as e:
                logger.warning(f"Redis设置失败: {e}")
    
    async def invalidate(self, key: str) -> None:
        """使缓存失效"""
        try:
            # 1. 删除内存缓存
            self.memory_cache.delete(key)
            
            # 2. 删除Redis缓存
            if self.redis_connected:
                await self.redis_client.delete(key)
            
            # 3. 删除文件存储
            self.file_storage.delete(key)
            
            logger.info(f"缓存失效: {key}")
        except Exception as e:
            logger.error(f"缓存失效操作失败: {e}")
            self.stats['errors'] += 1
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        hit_rate = (
            (self.stats['memory_hits'] + self.stats['redis_hits'] + self.stats['file_hits']) / 
            self.stats['total_requests'] * 100
        ) if self.stats['total_requests'] > 0 else 0
        
        stats = {
            **self.stats,
            'memory_cache_size': self.memory_cache.size(),
            'memory_cache_capacity': self.memory_cache.capacity,
            'redis_connected': self.redis_connected,
            'hit_rate': f"{hit_rate:.2f}%",
            'timestamp': datetime.now().isoformat()
        }
        
        # 获取Redis信息
        if self.redis_connected:
            try:
                redis_info = await self.redis_client.info()
                stats['redis_used_memory'] = redis_info.get('used_memory', 0)
                stats['redis_connected_clients'] = redis_info.get('connected_clients', 0)
            except Exception as e:
                logger.warning(f"获取Redis统计信息失败: {e}")
        
        return stats
    
    async def close(self) -> None:
        """关闭连接"""
        try:
            if self.redis_connected and self.redis_client:
                await self.redis_client.close()
            
            logger.info("SmartDataCache连接已关闭")
        except Exception as e:
            logger.error(f"关闭连接失败: {e}")

# 使用示例
async def example_usage():
    """使用示例"""
    
    # 创建缓存实例
    cache = SmartDataCache()
    
    # 初始化连接
    await cache.init_connections()
    
    # 示例获取函数
    async def fetch_stock_price():
        # 模拟获取股票价格
        return {
            'code': '000001',
            'name': '平安银行',
            'price': 15.67,
            'change': 0.23,
            'change_percent': 1.49,
            'timestamp': datetime.now().isoformat()
        }
    
    # 获取数据（如果缓存不存在会自动调用fetch函数）
    stock_data = await cache.get(
        key='stock_000001',
        fetch_func=fetch_stock_price,
        category=DataCategory.REAL_TIME_PRICE
    )
    
    print(f"获取的股票数据: {stock_data}")
    
    # 获取统计信息
    stats = await cache.get_stats()
    print(f"缓存统计: {stats}")
    
    # 关闭连接
    await cache.close()

if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())