"""
Redis连接管理器
用于股票数据缓存和持久化
"""

import asyncio
import json
import logging
import time
from typing import Dict, Optional, Any
from datetime import datetime

import redis.asyncio as redis

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RedisManager')

class RedisManager:
    """Redis连接管理器"""
    
    def __init__(self, 
                 host: str = 'localhost',
                 port: int = 6379,
                 db: int = 0,
                 password: str = None,
                 max_connections: int = 10,
                 reconnect_interval: int = 5):
        """
        初始化Redis管理器
        
        Args:
            host: Redis主机地址
            port: Redis端口
            db: Redis数据库编号
            password: Redis密码
            max_connections: 最大连接数
            reconnect_interval: 重连间隔（秒）
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_connections = max_connections
        self.reconnect_interval = reconnect_interval
        
        # 连接池和客户端
        self.connection_pool = None
        self.client = None
        self.connected = False
        
        # 重连相关
        self.reconnect_task = None
        self.should_reconnect = True
        
        logger.info(f"RedisManager初始化完成: {host}:{port}/{db}")
    
    async def connect(self) -> bool:
        """
        连接到Redis服务器
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 创建连接池
            self.connection_pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                max_connections=self.max_connections,
                decode_responses=True
            )
            
            # 创建客户端
            self.client = redis.Redis(connection_pool=self.connection_pool)
            
            # 测试连接
            await self.client.ping()
            self.connected = True
            
            logger.info("Redis连接成功")
            return True
            
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            self.connected = False
            
            # 启动重连任务
            await self._start_reconnect()
            return False
    
    async def _start_reconnect(self) -> None:
        """启动自动重连机制"""
        if self.reconnect_task is None or self.reconnect_task.done():
            self.reconnect_task = asyncio.create_task(self._reconnect_loop())
    
    async def _reconnect_loop(self) -> None:
        """重连循环"""
        while self.should_reconnect and not self.connected:
            try:
                logger.info(f"尝试重新连接Redis...")
                
                # 关闭现有连接
                if self.client:
                    await self.client.close()
                
                # 重新连接
                success = await self.connect()
                
                if success:
                    logger.info("Redis重连成功")
                    break
                else:
                    logger.info(f"重连失败，{self.reconnect_interval}秒后重试...")
                    await asyncio.sleep(self.reconnect_interval)
                    
            except Exception as e:
                logger.error(f"重连过程中发生错误: {e}")
                await asyncio.sleep(self.reconnect_interval)
    
    def _format_stock_key(self, code: str, data_type: str) -> str:
        """
        格式化股票数据key
        
        Args:
            code: 股票代码
            data_type: 数据类型
            
        Returns:
            str: 格式化后的key
        """
        # 标准化代码格式（移除可能的交易所后缀）
        clean_code = code.replace('.SZ', '').replace('.SH', '')
        return f"stock:{clean_code}:{data_type}"
    
    async def get_stock_data(self, code: str, data_type: str) -> Optional[Dict[str, Any]]:
        """
        获取股票数据
        
        Args:
            code: 股票代码
            data_type: 数据类型
            
        Returns:
            Optional[Dict]: 股票数据，失败返回None
        """
        if not self.connected:
            logger.warning("Redis未连接，无法获取数据")
            return None
        
        try:
            key = self._format_stock_key(code, data_type)
            data_str = await self.client.get(key)
            
            if data_str:
                data = json.loads(data_str)
                logger.debug(f"获取股票数据成功: {key}")
                return data
            else:
                logger.debug(f"股票数据不存在: {key}")
                return None
                
        except Exception as e:
            logger.error(f"获取股票数据失败: {e}")
            return None
    
    async def set_stock_data(self, code: str, data_type: str, data: Dict[str, Any], ttl: int = 300) -> bool:
        """
        设置股票数据
        
        Args:
            code: 股票代码
            data_type: 数据类型
            data: 股票数据
            ttl: 过期时间（秒）
            
        Returns:
            bool: 设置是否成功
        """
        if not self.connected:
            logger.warning("Redis未连接，无法设置数据")
            return False
        
        try:
            key = self._format_stock_key(code, data_type)
            
            # 添加时间戳
            data_with_timestamp = {
                **data,
                'cache_time': datetime.now().isoformat(),
                'ttl': ttl
            }
            
            data_str = json.dumps(data_with_timestamp, ensure_ascii=False)
            
            if ttl > 0:
                await self.client.setex(key, ttl, data_str)
            else:
                await self.client.set(key, data_str)
            
            logger.debug(f"设置股票数据成功: {key}, TTL: {ttl}s")
            return True
            
        except Exception as e:
            logger.error(f"设置股票数据失败: {e}")
            return False
    
    async def delete_stock_data(self, code: str, data_type: str) -> bool:
        """
        删除股票数据
        
        Args:
            code: 股票代码
            data_type: 数据类型
            
        Returns:
            bool: 删除是否成功
        """
        if not self.connected:
            logger.warning("Redis未连接，无法删除数据")
            return False
        
        try:
            key = self._format_stock_key(code, data_type)
            result = await self.client.delete(key)
            
            if result > 0:
                logger.debug(f"删除股票数据成功: {key}")
                return True
            else:
                logger.debug(f"股票数据不存在: {key}")
                return False
                
        except Exception as e:
            logger.error(f"删除股票数据失败: {e}")
            return False
    
    async def get_multiple_stock_data(self, codes: list, data_type: str) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        批量获取股票数据
        
        Args:
            codes: 股票代码列表
            data_type: 数据类型
            
        Returns:
            Dict: 股票数据字典，key为股票代码
        """
        if not self.connected:
            logger.warning("Redis未连接，无法批量获取数据")
            return {code: None for code in codes}
        
        try:
            keys = [self._format_stock_key(code, data_type) for code in codes]
            data_strs = await self.client.mget(keys)
            
            result = {}
            for code, data_str in zip(codes, data_strs):
                if data_str:
                    result[code] = json.loads(data_str)
                else:
                    result[code] = None
            
            logger.debug(f"批量获取股票数据成功: {len(codes)}个股票")
            return result
            
        except Exception as e:
            logger.error(f"批量获取股票数据失败: {e}")
            return {code: None for code in codes}
    
    async def set_multiple_stock_data(self, data_dict: Dict[str, Dict[str, Any]], data_type: str, ttl: int = 300) -> bool:
        """
        批量设置股票数据
        
        Args:
            data_dict: 股票数据字典，key为股票代码
            data_type: 数据类型
            ttl: 过期时间（秒）
            
        Returns:
            bool: 设置是否成功
        """
        if not self.connected:
            logger.warning("Redis未连接，无法批量设置数据")
            return False
        
        try:
            pipeline = self.client.pipeline()
            
            for code, data in data_dict.items():
                key = self._format_stock_key(code, data_type)
                
                # 添加时间戳
                data_with_timestamp = {
                    **data,
                    'cache_time': datetime.now().isoformat(),
                    'ttl': ttl
                }
                
                data_str = json.dumps(data_with_timestamp, ensure_ascii=False)
                
                if ttl > 0:
                    pipeline.setex(key, ttl, data_str)
                else:
                    pipeline.set(key, data_str)
            
            await pipeline.execute()
            
            logger.debug(f"批量设置股票数据成功: {len(data_dict)}个股票")
            return True
            
        except Exception as e:
            logger.error(f"批量设置股票数据失败: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        获取Redis统计信息
        
        Returns:
            Dict: 统计信息
        """
        if not self.connected:
            return {
                'connected': False,
                'error': 'Redis未连接'
            }
        
        try:
            info = await self.client.info()
            
            stats = {
                'connected': True,
                'used_memory': info.get('used_memory', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'timestamp': datetime.now().isoformat()
            }
            
            # 计算命中率
            total_requests = stats['keyspace_hits'] + stats['keyspace_misses']
            if total_requests > 0:
                stats['hit_rate'] = f"{(stats['keyspace_hits'] / total_requests * 100):.2f}%"
            else:
                stats['hit_rate'] = "0.00%"
            
            return stats
            
        except Exception as e:
            logger.error(f"获取Redis统计信息失败: {e}")
            return {
                'connected': False,
                'error': str(e)
            }
    
    async def close(self) -> None:
        """关闭连接"""
        self.should_reconnect = False
        
        if self.reconnect_task and not self.reconnect_task.done():
            self.reconnect_task.cancel()
        
        if self.client:
            await self.client.close()
        
        self.connected = False
        logger.info("Redis连接已关闭")

# 使用示例
async def example_usage():
    """RedisManager使用示例"""
    
    # 创建Redis管理器
    redis_manager = RedisManager()
    
    # 连接Redis
    await redis_manager.connect()
    
    # 设置股票数据
    stock_data = {
        'code': '000001',
        'name': '平安银行',
        'price': 15.67,
        'change': 0.23,
        'change_percent': 1.49,
        'volume': 1000000,
        'amount': 15670000
    }
    
    await redis_manager.set_stock_data('000001', 'realtime', stock_data, ttl=300)
    
    # 获取股票数据
    data = await redis_manager.get_stock_data('000001', 'realtime')
    print(f"获取的股票数据: {data}")
    
    # 获取统计信息
    stats = await redis_manager.get_stats()
    print(f"Redis统计信息: {stats}")
    
    # 关闭连接
    await redis_manager.close()

if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())