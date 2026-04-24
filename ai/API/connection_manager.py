"""
综合连接管理器
整合Redis和TDengine，提供统一的股票数据缓存和持久化接口
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from redis_manager import RedisManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ConnectionManager')

class ConnectionManager:
    """综合连接管理器"""
    
    def __init__(self,
                 redis_config: Dict[str, Any] = None,
                 tdengine_config: Dict[str, Any] = None):
        """
        初始化连接管理器
        
        Args:
            redis_config: Redis配置
            tdengine_config: TDengine配置
        """
        # Redis配置
        redis_default = {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'password': None,
            'max_connections': 10,
            'reconnect_interval': 5
        }
        self.redis_config = {**redis_default, **(redis_config or {})}
        
        # TDengine配置
        tdengine_default = {
            'host': 'localhost',
            'port': 6030,
            'user': 'root',
            'password': 'taosdata',
            'database': 'stock_data',
            'keep_days': 365
        }
        self.tdengine_config = {**tdengine_default, **(tdengine_config or {})}
        
        # 管理器实例
        self.redis_manager = None
        self.tdengine_manager = None
        
        # 连接状态
        self.redis_connected = False
        self.tdengine_connected = False
        
        # TDengine可用性
        self.tdengine_available = self._check_tdengine_availability()
        
        logger.info("ConnectionManager初始化完成")
    
    def _check_tdengine_availability(self) -> bool:
        """检查TDengine客户端库是否可用"""
        try:
            import taos
            return True
        except ImportError:
            logger.warning("TDengine客户端库不可用，将使用模拟版本")
            return False
        except Exception as e:
            logger.warning(f"TDengine客户端库检查失败: {e}，将使用模拟版本")
            return False
    
    async def connect_all(self) -> Dict[str, bool]:
        """
        连接所有服务
        
        Returns:
            Dict: 连接状态
        """
        results = {}
        
        # 连接Redis
        try:
            self.redis_manager = RedisManager(**self.redis_config)
            self.redis_connected = await self.redis_manager.connect()
            results['redis'] = self.redis_connected
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            results['redis'] = False
        
        # 连接TDengine（根据可用性动态导入）
        try:
            if self.tdengine_available:
                from tdengine_manager import TDengineManager
                self.tdengine_manager = TDengineManager(**self.tdengine_config)
            else:
                from tdengine_mock import TDengineManagerMock
                self.tdengine_manager = TDengineManagerMock(**self.tdengine_config)
            
            self.tdengine_connected = self.tdengine_manager.connect()
            results['tdengine'] = self.tdengine_connected
            
            # 记录使用的版本
            if self.tdengine_available:
                logger.info("使用真实的TDengine连接管理器")
            else:
                logger.info("使用模拟的TDengine连接管理器")
                
        except Exception as e:
            logger.error(f"TDengine连接失败: {e}")
            results['tdengine'] = False
        
        logger.info(f"连接结果: Redis={results['redis']}, TDengine={results['tdengine']}")
        return results
    
    async def get_stock_data(self, code: str, data_type: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        获取股票数据（优先从缓存获取）
        
        Args:
            code: 股票代码
            data_type: 数据类型
            use_cache: 是否使用缓存
            
        Returns:
            Optional[Dict]: 股票数据
        """
        # 优先从Redis缓存获取
        if use_cache and self.redis_connected:
            cached_data = await self.redis_manager.get_stock_data(code, data_type)
            if cached_data:
                logger.debug(f"从Redis缓存获取数据: {code}:{data_type}")
                return cached_data
        
        # 如果缓存中没有，可以从TDengine获取历史数据
        if self.tdengine_connected and data_type == 'history':
            # 查询最近的历史数据
            end_time = datetime.now().isoformat()
            start_time = (datetime.now() - timedelta(days=1)).isoformat()
            
            history_data = await self.tdengine_manager.query_history(
                code, start_time, end_time, 'stock_prices', limit=1
            )
            
            if history_data:
                latest_data = history_data[0]
                logger.debug(f"从TDengine获取历史数据: {code}")
                
                # 将历史数据缓存到Redis
                if self.redis_connected:
                    await self.redis_manager.set_stock_data(
                        code, 'history', latest_data, ttl=3600
                    )
                
                return latest_data
        
        logger.debug(f"未找到股票数据: {code}:{data_type}")
        return None
    
    async def set_stock_data(self, code: str, data_type: str, data: Dict[str, Any], 
                            ttl: int = 300, persist: bool = True) -> bool:
        """
        设置股票数据
        
        Args:
            code: 股票代码
            data_type: 数据类型
            data: 股票数据
            ttl: 缓存过期时间（秒）
            persist: 是否持久化到TDengine
            
        Returns:
            bool: 设置是否成功
        """
        results = {}
        
        # 设置Redis缓存
        if self.redis_connected:
            redis_success = await self.redis_manager.set_stock_data(
                code, data_type, data, ttl
            )
            results['redis'] = redis_success
        
        # 持久化到TDengine
        if persist and self.tdengine_connected:
            if data_type == 'realtime':
                # 实时数据存储到股票价格表
                tdengine_data = {
                    'stock_code': code,
                    'price': data.get('price', 0),
                    'volume': data.get('volume', 0),
                    'turnover': data.get('amount', 0),
                    'pct_change': data.get('change_percent', 0),
                    'amplitude': data.get('amplitude', 0),
                    'tags': {
                        'data_type': 'realtime',
                        'source': 'cache'
                    }
                }
                tdengine_success = await self.tdengine_manager.store_history_data(
                    'stock_prices', tdengine_data
                )
                results['tdengine'] = tdengine_success
            elif data_type == 'status':
                # 状态数据存储到股票状态表
                tdengine_success = await self.tdengine_manager.store_history_data(
                    'stock_status', data
                )
                results['tdengine'] = tdengine_success
        
        # 返回整体结果
        success = any(results.values())
        logger.debug(f"设置股票数据结果: {results}")
        return success
    
    async def batch_get_stock_data(self, codes: List[str], data_type: str, 
                                  use_cache: bool = True) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        批量获取股票数据
        
        Args:
            codes: 股票代码列表
            data_type: 数据类型
            use_cache: 是否使用缓存
            
        Returns:
            Dict: 股票数据字典
        """
        result = {}
        
        # 优先从Redis批量获取
        if use_cache and self.redis_connected:
            cached_data = await self.redis_manager.get_multiple_stock_data(codes, data_type)
            
            # 处理缓存命中的数据
            for code, data in cached_data.items():
                if data:
                    result[code] = data
                    logger.debug(f"从Redis缓存获取数据: {code}:{data_type}")
        
        # 查找未命中的股票
        missing_codes = [code for code in codes if code not in result]
        
        if missing_codes and self.tdengine_connected and data_type == 'history':
            # 从TDengine批量查询历史数据
            for code in missing_codes:
                end_time = datetime.now().isoformat()
                start_time = (datetime.now() - timedelta(days=1)).isoformat()
                
                history_data = await self.tdengine_manager.query_history(
                    code, start_time, end_time, 'stock_prices', limit=1
                )
                
                if history_data:
                    latest_data = history_data[0]
                    result[code] = latest_data
                    logger.debug(f"从TDengine获取历史数据: {code}")
                    
                    # 缓存到Redis
                    if self.redis_connected:
                        await self.redis_manager.set_stock_data(
                            code, 'history', latest_data, ttl=3600
                        )
        
        # 确保所有股票都有结果
        for code in codes:
            if code not in result:
                result[code] = None
        
        logger.debug(f"批量获取完成: 总数={len(codes)}, 命中={len([v for v in result.values() if v])}")
        return result
    
    async def get_multiple_stock_data(self, codes: List[str], data_type: str = 'realtime', 
                                     use_cache: bool = True) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        批量获取股票数据（batch_get_stock_data的别名）
        
        Args:
            codes: 股票代码列表
            data_type: 数据类型
            use_cache: 是否使用缓存
            
        Returns:
            Dict: 股票数据字典
        """
        return await self.batch_get_stock_data(codes, data_type, use_cache)
    
    async def batch_set_stock_data(self, data_dict: Dict[str, Dict[str, Any]], 
                                  data_type: str, ttl: int = 300, persist: bool = True) -> bool:
        """
        批量设置股票数据
        
        Args:
            data_dict: 股票数据字典
            data_type: 数据类型
            ttl: 缓存过期时间
            persist: 是否持久化
            
        Returns:
            bool: 设置是否成功
        """
        results = {}
        
        # 批量设置Redis缓存
        if self.redis_connected:
            redis_success = await self.redis_manager.set_multiple_stock_data(
                data_dict, data_type, ttl
            )
            results['redis'] = redis_success
        
        # 批量持久化到TDengine
        if persist and self.tdengine_connected:
            tdengine_data_list = []
            
            for code, data in data_dict.items():
                if data_type == 'realtime':
                    tdengine_data = {
                        'stock_code': code,
                        'price': data.get('price', 0),
                        'volume': data.get('volume', 0),
                        'turnover': data.get('amount', 0),
                        'pct_change': data.get('change_percent', 0),
                        'amplitude': data.get('amplitude', 0),
                        'tags': {
                            'data_type': 'realtime',
                            'source': 'batch_cache'
                        }
                    }
                    tdengine_data_list.append(tdengine_data)
                elif data_type == 'status':
                    tdengine_data_list.append(data)
            
            if tdengine_data_list:
                table = 'stock_prices' if data_type == 'realtime' else 'stock_status'
                tdengine_success = await self.tdengine_manager.store_batch_data(
                    table, tdengine_data_list
                )
                results['tdengine'] = tdengine_success
        
        # 返回整体结果
        success = any(results.values())
        logger.info(f"批量设置完成: 数量={len(data_dict)}, 结果={results}")
        return success
    
    async def set_multiple_stock_data(self, data_dict: Dict[str, Dict[str, Any]], 
                                     data_type: str = 'realtime', ttl: int = 300, 
                                     persist: bool = True) -> bool:
        """
        批量设置股票数据（batch_set_stock_data的别名）
        
        Args:
            data_dict: 股票数据字典
            data_type: 数据类型
            ttl: 缓存过期时间
            persist: 是否持久化
            
        Returns:
            bool: 设置是否成功
        """
        return await self.batch_set_stock_data(data_dict, data_type, ttl, persist)
    
    async def store_history_data(self, table: str, data: Dict[str, Any]) -> bool:
        """
        存储历史数据
        
        Args:
            table: 表名
            data: 历史数据
            
        Returns:
            bool: 存储是否成功
        """
        if not self.tdengine_connected:
            logger.warning("TDengine未连接，无法存储历史数据")
            return False
        
        try:
            success = await self.tdengine_manager.store_history_data(table, data)
            
            if success:
                logger.info(f"历史数据存储成功: {table} - {data.get('stock_code', 'unknown')}")
            else:
                logger.error(f"历史数据存储失败: {table}")
            
            return success
            
        except Exception as e:
            logger.error(f"存储历史数据失败: {e}")
            return False
    
    async def query_history(self, stock_code: str, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """
        查询历史数据
        
        Args:
            stock_code: 股票代码
            start_time: 开始时间
            end_time: 结束时间
            
        Returns:
            List[Dict]: 历史数据列表
        """
        if not self.tdengine_connected:
            logger.warning("TDengine未连接，无法查询历史数据")
            return []
        
        try:
            results = await self.tdengine_manager.query_history(stock_code, start_time, end_time)
            logger.info(f"查询历史数据: {stock_code} -> {len(results)} 条记录")
            return results
            
        except Exception as e:
            logger.error(f"查询历史数据失败: {e}")
            return []
    
    async def store_limit_up_event(self, code: str, limit_time: datetime, 
                                  reason_type: str, reason_detail: str) -> bool:
        """
        存储涨停事件
        
        Args:
            code: 股票代码
            limit_time: 涨停时间
            reason_type: 原因类型
            reason_detail: 原因详情
            
        Returns:
            bool: 存储是否成功
        """
        if not self.tdengine_connected:
            logger.warning("TDengine未连接，无法存储涨停事件")
            return False
        
        try:
            limit_data = {
                'stock_code': code,
                'limit_time': limit_time.isoformat(),
                'reason_type': reason_type,
                'reason_detail': reason_detail
            }
            
            success = await self.tdengine_manager.store_history_data(
                'limit_up_history', limit_data
            )
            
            if success:
                logger.info(f"涨停事件存储成功: {code} at {limit_time}")
            else:
                logger.error(f"涨停事件存储失败: {code}")
            
            return success
            
        except Exception as e:
            logger.error(f"存储涨停事件失败: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        Returns:
            Dict: 统计信息
        """
        stats = {
            'timestamp': datetime.now().isoformat(),
            'redis_connected': self.redis_connected,
            'tdengine_connected': self.tdengine_connected
        }
        
        # Redis统计
        if self.redis_connected:
            redis_stats = await self.redis_manager.get_stats()
            stats['redis'] = redis_stats
        
        # TDengine统计
        if self.tdengine_connected:
            tdengine_stats = {}
            for table in ['stock_prices', 'limit_up_history', 'stock_status']:
                table_stats = await self.tdengine_manager.get_table_stats(table)
                tdengine_stats[table] = table_stats
            stats['tdengine'] = tdengine_stats
        
        return stats
    
    async def health_check(self) -> Dict[str, bool]:
        """
        健康检查
        
        Returns:
            Dict: 健康状态
        """
        health = {}
        
        # Redis健康检查
        if self.redis_connected:
            try:
                await self.redis_manager.get_stock_data('test', 'health')
                health['redis'] = True
            except Exception as e:
                logger.error(f"Redis健康检查失败: {e}")
                health['redis'] = False
        else:
            health['redis'] = False
        
        # TDengine健康检查
        if self.tdengine_connected:
            try:
                stats = await self.tdengine_manager.get_table_stats('stock_prices')
                health['tdengine'] = 'error' not in stats
            except Exception as e:
                logger.error(f"TDengine健康检查失败: {e}")
                health['tdengine'] = False
        else:
            health['tdengine'] = False
        
        return health
    
    async def close_all(self) -> None:
        """关闭所有连接"""
        if self.redis_manager:
            await self.redis_manager.close()
        
        if self.tdengine_manager:
            self.tdengine_manager.close()
        
        self.redis_connected = False
        self.tdengine_connected = False
        
        logger.info("所有连接已关闭")

# 使用示例
async def example_usage():
    """ConnectionManager使用示例"""
    
    # 创建连接管理器
    manager = ConnectionManager()
    
    # 连接所有服务
    connection_results = await manager.connect_all()
    print(f"连接结果: {connection_results}")
    
    # 设置股票数据
    stock_data = {
        'code': '000001',
        'name': '平安银行',
        'price': 15.8,
        'change': 0.5,
        'change_percent': 3.27,
        'volume': 1000000,
        'turnover': 15800000,
        'timestamp': datetime.now().isoformat()
    }
    
    set_result = await manager.set_stock_data('000001', 'realtime', stock_data)
    print(f"设置股票数据结果: {set_result}")
    
    # 获取股票数据
    data = await manager.get_stock_data('000001', 'realtime')
    print(f"获取的股票数据: {data}")
    
    # 测试批量设置股票数据
    multiple_stocks = {
        '000001': {
            'code': '000001',
            'name': '平安银行',
            'price': 15.8,
            'change': 0.5,
            'change_percent': 3.27
        },
        '000002': {
            'code': '000002',
            'name': '万科A',
            'price': 18.5,
            'change': -0.2,
            'change_percent': -1.07
        }
    }
    
    set_multiple_result = await manager.batch_set_stock_data(multiple_stocks, 'realtime')
    print(f"批量设置股票数据结果: {set_multiple_result}")
    
    batch_result = await manager.batch_get_stock_data(['000001', '000002'], 'realtime')
    print(f"批量获取结果: {batch_result}")
    
    # 存储涨停事件
    await manager.store_limit_up_event(
        '000001', 
        datetime.now(), 
        '市场热点', 
        '银行板块整体上涨'
    )
    
    # 获取统计信息
    stats = await manager.get_stats()
    print(f"统计信息: {stats}")
    
    # 健康检查
    health = await manager.health_check()
    print(f"健康状态: {health}")
    
    # 关闭连接
    await manager.close_all()

if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())