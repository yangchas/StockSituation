"""
TDengine连接管理器模拟版本
用于在没有安装TDengine客户端库的情况下测试连接管理器逻辑
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger('TDengineMock')

class TDengineManagerMock:
    """TDengine连接管理器模拟类"""
    
    def __init__(self, host: str = "localhost", port: int = 6030, 
                 user: str = "root", password: str = "taosdata", 
                 database: str = "stock_data", keep_days: int = 365):
        """初始化TDengine连接管理器"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.keep_days = keep_days
        self.connection = None
        self.connected = False
        self.logger = logging.getLogger(f'TDengineManagerMock.{id(self)}')
        
        # 模拟数据存储
        self.mock_data = {
            'stock_prices': [],
            'limit_up_history': [],
            'stock_status': []
        }
        
        self.logger.info(f"TDengineManagerMock初始化完成: {host}:{port}/{database}, keep_days={keep_days}")
    
    def connect(self) -> bool:
        """连接TDengine数据库"""
        try:
            self.logger.info("模拟连接TDengine数据库...")
            
            # 模拟连接过程
            self.connected = True
            self.connection = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'database': self.database
            }
            
            # 模拟创建数据库和表结构
            self._create_database_and_tables()
            
            self.logger.info("TDengine连接成功（模拟）")
            return True
            
        except Exception as e:
            self.logger.error(f"TDengine连接失败: {e}")
            self.connected = False
            return False
    
    def _create_database_and_tables(self):
        """模拟创建数据库和表结构"""
        self.logger.info("模拟创建数据库和表结构...")
        
        # 模拟表结构创建
        table_definitions = {
            'stock_prices': [
                'ts TIMESTAMP',
                'stock_code NCHAR(10)', 
                'price FLOAT',
                'volume BIGINT',
                'turnover FLOAT',
                'pct_change FLOAT',
                'amplitude FLOAT',
                'tags JSON'
            ],
            'limit_up_history': [
                'ts TIMESTAMP',
                'stock_code NCHAR(10)',
                'limit_time TIMESTAMP', 
                'reason_type NCHAR(50)',
                'reason_detail NCHAR(200)'
            ],
            'stock_status': [
                'ts TIMESTAMP',
                'stock_code NCHAR(10)',
                'status NCHAR(20)',
                'reason NCHAR(100)'
            ]
        }
        
        for table_name, columns in table_definitions.items():
            self.logger.info(f"模拟创建表 {table_name}: {', '.join(columns)}")
    
    async def store_history_data(self, table: str, data: Dict[str, Any]) -> bool:
        """存储历史数据（模拟）"""
        if not self.connected:
            self.logger.warning("TDengine未连接")
            return False
        
        try:
            # 添加时间戳
            data_with_ts = data.copy()
            if 'ts' not in data_with_ts:
                data_with_ts['ts'] = datetime.now()
            
            # 模拟数据存储
            if table in self.mock_data:
                self.mock_data[table].append(data_with_ts)
                self.logger.info(f"模拟存储数据到 {table}: {data_with_ts.get('stock_code', 'unknown')}")
                return True
            else:
                self.logger.error(f"未知表名: {table}")
                return False
                
        except Exception as e:
            self.logger.error(f"存储历史数据失败: {e}")
            return False
    
    async def store_batch_data(self, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """批量存储数据（模拟）"""
        if not self.connected:
            self.logger.warning("TDengine未连接")
            return False
        
        try:
            success_count = 0
            for data in data_list:
                if await self.store_history_data(table, data):
                    success_count += 1
            
            self.logger.info(f"批量存储完成: {success_count}/{len(data_list)} 条数据")
            return success_count == len(data_list)
            
        except Exception as e:
            self.logger.error(f"批量存储数据失败: {e}")
            return False
    
    async def query_history(self, stock_code: str, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """查询历史数据（模拟）"""
        if not self.connected:
            self.logger.warning("TDengine未连接")
            return []
        
        try:
            # 模拟时间解析
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            results = []
            for table_name, data_list in self.mock_data.items():
                for record in data_list:
                    if record.get('stock_code') == stock_code:
                        record_time = record.get('ts')
                        if isinstance(record_time, str):
                            record_time = datetime.fromisoformat(record_time.replace('Z', '+00:00'))
                        
                        # 确保时区一致
                        if record_time and record_time.tzinfo is None:
                            record_time = record_time.replace(tzinfo=start_dt.tzinfo)
                        
                        if record_time and start_dt <= record_time <= end_dt:
                            results.append(record)
            
            self.logger.info(f"查询历史数据: {stock_code} -> {len(results)} 条记录")
            return results
            
        except Exception as e:
            self.logger.error(f"查询历史数据失败: {e}")
            return []
    
    async def get_table_info(self, table: str) -> Dict[str, Any]:
        """获取表信息（模拟）"""
        if not self.connected:
            self.logger.warning("TDengine未连接")
            return {}
        
        try:
            if table in self.mock_data:
                return {
                    'table_name': table,
                    'record_count': len(self.mock_data[table]),
                    'status': 'active'
                }
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"获取表信息失败: {e}")
            return {}
    
    def close(self):
        """关闭连接"""
        if self.connected:
            self.connected = False
            self.connection = None
            self.logger.info("TDengine连接已关闭（模拟）")
    
    async def get_table_stats(self, table: str) -> Dict[str, Any]:
        """获取表统计信息（模拟）"""
        if not self.connected:
            self.logger.warning("TDengine未连接")
            return {'error': 'TDengine未连接'}
        
        try:
            if table in self.mock_data:
                data_list = self.mock_data[table]
                
                # 计算统计信息
                stats = {
                    'table_name': table,
                    'row_count': len(data_list),
                    'status': 'active',
                    'last_updated': datetime.now().isoformat()
                }
                
                # 如果有数据，添加更多统计信息
                if data_list:
                    # 获取最新的时间戳
                    timestamps = [record.get('ts') for record in data_list if 'ts' in record]
                    if timestamps:
                        # 处理时间戳（可能是字符串或datetime对象）
                        valid_timestamps = []
                        for ts in timestamps:
                            if isinstance(ts, datetime):
                                valid_timestamps.append(ts)
                            elif isinstance(ts, str):
                                try:
                                    valid_timestamps.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
                                except:
                                    pass
                        
                        if valid_timestamps:
                            latest_ts = max(valid_timestamps)
                            oldest_ts = min(valid_timestamps)
                            stats['latest_timestamp'] = latest_ts.isoformat()
                            stats['oldest_timestamp'] = oldest_ts.isoformat()
                            
                            # 计算数据范围（天）
                            time_range = (latest_ts - oldest_ts).total_seconds() / 86400  # 转换为天
                            stats['time_range_days'] = round(time_range, 2)
                
                self.logger.debug(f"获取表统计信息: {table} -> {stats}")
                return stats
            else:
                return {'error': f'表 {table} 不存在'}
                
        except Exception as e:
            self.logger.error(f"获取表统计信息失败: {e}")
            return {'error': str(e)}
    
    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.connected
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            'connected': self.connected,
            'host': self.host,
            'port': self.port,
            'database': self.database
        }
        
        # 添加模拟数据统计
        for table_name, data_list in self.mock_data.items():
            stats[f'{table_name}_count'] = len(data_list)
        
        return stats

# 使用示例
async def example_usage():
    """使用示例"""
    # 创建TDengine管理器
    tdengine_manager = TDengineManagerMock()
    
    # 连接数据库
    connected = tdengine_manager.connect()
    if not connected:
        print("连接失败")
        return
    
    try:
        # 存储单条数据
        stock_data = {
            'stock_code': '000001',
            'price': 15.67,
            'volume': 1000000,
            'turnover': 15670000,
            'pct_change': 1.49,
            'amplitude': 3.2,
            'tags': {'market': 'SZ', 'industry': 'banking'}
        }
        
        await tdengine_manager.store_history_data('stock_prices', stock_data)
        
        # 批量存储数据
        batch_data = [
            {
                'stock_code': '000002',
                'price': 18.45,
                'volume': 800000,
                'turnover': 14760000,
                'pct_change': -0.65,
                'amplitude': 2.1
            },
            {
                'stock_code': '000003', 
                'price': 12.30,
                'volume': 500000,
                'turnover': 6150000,
                'pct_change': 0.82,
                'amplitude': 1.8
            }
        ]
        
        await tdengine_manager.store_batch_data('stock_prices', batch_data)
        
        # 查询历史数据
        start_time = '2024-01-01T00:00:00Z'
        end_time = '2024-12-31T23:59:59Z'
        
        history_data = await tdengine_manager.query_history('000001', start_time, end_time)
        print(f"查询到 {len(history_data)} 条历史数据")
        
        # 获取统计信息
        stats = await tdengine_manager.get_stats()
        print(f"统计信息: {stats}")
        
    finally:
        # 关闭连接
        tdengine_manager.close()

if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())