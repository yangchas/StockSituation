"""
TDengine连接管理器
用于股票数据持久化存储
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TDengineManager')

try:
    import taos
    TAOS_AVAILABLE = True
except ImportError:
    logger.warning("taos包未安装，TDengine功能将不可用")
    TAOS_AVAILABLE = False

class TDengineManager:
    """TDengine连接管理器"""
    
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 6030,
                 user: str = 'root',
                 password: str = 'taosdata',
                 database: str = 'stock_data',
                 keep_days: int = 365):
        """
        初始化TDengine管理器
        
        Args:
            host: TDengine主机地址
            port: TDengine端口
            user: 用户名
            password: 密码
            database: 数据库名称
            keep_days: 数据保留天数
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.keep_days = keep_days
        
        # 连接相关
        self.conn = None
        self.connected = False
        
        # 表结构定义
        self.table_definitions = {
            'stock_prices': """
                CREATE TABLE IF NOT EXISTS stock_prices (
                    ts TIMESTAMP,
                    stock_code NCHAR(10),
                    price FLOAT,
                    volume BIGINT,
                    turnover FLOAT,
                    pct_change FLOAT,
                    amplitude FLOAT,
                    tags JSON
                )
            """,
            'limit_up_history': """
                CREATE TABLE IF NOT EXISTS limit_up_history (
                    ts TIMESTAMP,
                    stock_code NCHAR(10),
                    limit_time TIMESTAMP,
                    reason_type NCHAR(50),
                    reason_detail NCHAR(200)
                )
            """,
            'stock_status': """
                CREATE TABLE IF NOT EXISTS stock_status (
                    ts TIMESTAMP,
                    stock_code NCHAR(10),
                    status_type NCHAR(20),
                    status_value NCHAR(100),
                    description NCHAR(200)
                )
            """
        }
        
        logger.info(f"TDengineManager初始化完成: {host}:{port}/{database}")
    
    def connect(self) -> bool:
        """
        连接到TDengine服务器
        
        Returns:
            bool: 连接是否成功
        """
        if not TAOS_AVAILABLE:
            logger.error("taos包未安装，无法连接TDengine")
            return False
        
        try:
            # 连接到TDengine
            self.conn = taos.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            
            # 创建数据库
            self._create_database()
            
            # 切换到目标数据库
            self.conn.execute(f"USE {self.database}")
            
            # 创建表结构
            self._create_tables()
            
            self.connected = True
            logger.info("TDengine连接成功")
            return True
            
        except Exception as e:
            logger.error(f"TDengine连接失败: {e}")
            self.connected = False
            return False
    
    def _create_database(self) -> None:
        """创建数据库"""
        try:
            # 检查数据库是否存在
            sql = f"""CREATE DATABASE IF NOT EXISTS {self.database} 
                     KEEP {self.keep_days} 
                     COMP 2 
                     REPLICA 1 
                     WALETS 3"""
            self.conn.execute(sql)
            logger.info(f"数据库 {self.database} 创建/检查完成")
            
        except Exception as e:
            logger.error(f"创建数据库失败: {e}")
            raise
    
    def _create_tables(self) -> None:
        """创建表结构"""
        try:
            for table_name, create_sql in self.table_definitions.items():
                self.conn.execute(create_sql)
                logger.info(f"表 {table_name} 创建/检查完成")
                
        except Exception as e:
            logger.error(f"创建表结构失败: {e}")
            raise
    
    async def store_history_data(self, table: str, data: Dict[str, Any]) -> bool:
        """
        存储历史数据
        
        Args:
            table: 表名
            data: 数据字典
            
        Returns:
            bool: 存储是否成功
        """
        if not self.connected:
            logger.warning("TDengine未连接，无法存储数据")
            return False
        
        try:
            # 根据表名处理不同的数据格式
            if table == 'stock_prices':
                return await self._store_stock_prices(data)
            elif table == 'limit_up_history':
                return await self._store_limit_up_history(data)
            elif table == 'stock_status':
                return await self._store_stock_status(data)
            else:
                logger.error(f"未知的表名: {table}")
                return False
                
        except Exception as e:
            logger.error(f"存储历史数据失败: {e}")
            return False
    
    async def _store_stock_prices(self, data: Dict[str, Any]) -> bool:
        """存储股票价格数据"""
        try:
            # 构建插入SQL
            sql = f"""
                INSERT INTO stock_prices (
                    ts, stock_code, price, volume, turnover, 
                    pct_change, amplitude, tags
                ) VALUES (
                    '{data.get('ts', datetime.now().isoformat())}',
                    '{data.get('stock_code', '')}',
                    {data.get('price', 0)},
                    {data.get('volume', 0)},
                    {data.get('turnover', 0)},
                    {data.get('pct_change', 0)},
                    {data.get('amplitude', 0)},
                    '{json.dumps(data.get('tags', {}), ensure_ascii=False)}'
                )
            """
            
            self.conn.execute(sql)
            logger.debug(f"股票价格数据存储成功: {data.get('stock_code', '')}")
            return True
            
        except Exception as e:
            logger.error(f"存储股票价格数据失败: {e}")
            return False
    
    async def _store_limit_up_history(self, data: Dict[str, Any]) -> bool:
        """存储涨停历史数据"""
        try:
            # 构建插入SQL
            sql = f"""
                INSERT INTO limit_up_history (
                    ts, stock_code, limit_time, reason_type, reason_detail
                ) VALUES (
                    '{data.get('ts', datetime.now().isoformat())}',
                    '{data.get('stock_code', '')}',
                    '{data.get('limit_time', datetime.now().isoformat())}',
                    '{data.get('reason_type', '')}',
                    '{data.get('reason_detail', '')}'
                )
            """
            
            self.conn.execute(sql)
            logger.debug(f"涨停历史数据存储成功: {data.get('stock_code', '')}")
            return True
            
        except Exception as e:
            logger.error(f"存储涨停历史数据失败: {e}")
            return False
    
    async def _store_stock_status(self, data: Dict[str, Any]) -> bool:
        """存储股票状态数据"""
        try:
            # 构建插入SQL
            sql = f"""
                INSERT INTO stock_status (
                    ts, stock_code, status_type, status_value, description
                ) VALUES (
                    '{data.get('ts', datetime.now().isoformat())}',
                    '{data.get('stock_code', '')}',
                    '{data.get('status_type', '')}',
                    '{data.get('status_value', '')}',
                    '{data.get('description', '')}'
                )
            """
            
            self.conn.execute(sql)
            logger.debug(f"股票状态数据存储成功: {data.get('stock_code', '')}")
            return True
            
        except Exception as e:
            logger.error(f"存储股票状态数据失败: {e}")
            return False
    
    async def store_batch_data(self, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """
        批量存储数据
        
        Args:
            table: 表名
            data_list: 数据列表
            
        Returns:
            bool: 存储是否成功
        """
        if not self.connected:
            logger.warning("TDengine未连接，无法批量存储数据")
            return False
        
        if not data_list:
            logger.warning("数据列表为空，无需存储")
            return True
        
        try:
            # 构建批量插入SQL
            values_list = []
            
            for data in data_list:
                if table == 'stock_prices':
                    values = f"""
                        ('{data.get('ts', datetime.now().isoformat())}',
                         '{data.get('stock_code', '')}',
                         {data.get('price', 0)},
                         {data.get('volume', 0)},
                         {data.get('turnover', 0)},
                         {data.get('pct_change', 0)},
                         {data.get('amplitude', 0)},
                         '{json.dumps(data.get('tags', {}), ensure_ascii=False)}')
                    """
                elif table == 'limit_up_history':
                    values = f"""
                        ('{data.get('ts', datetime.now().isoformat())}',
                         '{data.get('stock_code', '')}',
                         '{data.get('limit_time', datetime.now().isoformat())}',
                         '{data.get('reason_type', '')}',
                         '{data.get('reason_detail', '')}')
                    """
                elif table == 'stock_status':
                    values = f"""
                        ('{data.get('ts', datetime.now().isoformat())}',
                         '{data.get('stock_code', '')}',
                         '{data.get('status_type', '')}',
                         '{data.get('status_value', '')}',
                         '{data.get('description', '')}')
                    """
                else:
                    logger.error(f"未知的表名: {table}")
                    return False
                
                values_list.append(values)
            
            # 执行批量插入
            sql = f"INSERT INTO {table} VALUES {','.join(values_list)}"
            self.conn.execute(sql)
            
            logger.info(f"批量存储数据成功: {table}, 数量: {len(data_list)}")
            return True
            
        except Exception as e:
            logger.error(f"批量存储数据失败: {e}")
            return False
    
    async def query_history(self, stock_code: str, start_time: str, end_time: str, 
                           table: str = 'stock_prices', limit: int = 1000) -> List[Dict[str, Any]]:
        """
        查询历史数据
        
        Args:
            stock_code: 股票代码
            start_time: 开始时间
            end_time: 结束时间
            table: 表名
            limit: 限制返回数量
            
        Returns:
            List[Dict]: 历史数据列表
        """
        if not self.connected:
            logger.warning("TDengine未连接，无法查询数据")
            return []
        
        try:
            # 构建查询SQL
            sql = f"""
                SELECT * FROM {table} 
                WHERE stock_code = '{stock_code}' 
                AND ts >= '{start_time}' 
                AND ts <= '{end_time}'
                ORDER BY ts DESC
                LIMIT {limit}
            """
            
            result = self.conn.query(sql)
            
            # 转换结果
            data_list = []
            for row in result:
                data = {
                    'ts': row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                    'stock_code': row[1],
                }
                
                # 根据表结构添加字段
                if table == 'stock_prices':
                    data.update({
                        'price': row[2],
                        'volume': row[3],
                        'turnover': row[4],
                        'pct_change': row[5],
                        'amplitude': row[6],
                        'tags': json.loads(row[7]) if row[7] else {}
                    })
                elif table == 'limit_up_history':
                    data.update({
                        'limit_time': row[2].isoformat() if hasattr(row[2], 'isoformat') else str(row[2]),
                        'reason_type': row[3],
                        'reason_detail': row[4]
                    })
                elif table == 'stock_status':
                    data.update({
                        'status_type': row[2],
                        'status_value': row[3],
                        'description': row[4]
                    })
                
                data_list.append(data)
            
            logger.debug(f"查询历史数据成功: {stock_code}, 数量: {len(data_list)}")
            return data_list
            
        except Exception as e:
            logger.error(f"查询历史数据失败: {e}")
            return []
    
    async def get_table_stats(self, table: str) -> Dict[str, Any]:
        """
        获取表统计信息
        
        Args:
            table: 表名
            
        Returns:
            Dict: 统计信息
        """
        if not self.connected:
            return {'error': 'TDengine未连接'}
        
        try:
            # 查询表记录数
            count_sql = f"SELECT COUNT(*) FROM {table}"
            count_result = self.conn.query(count_sql)
            row_count = count_result.fetch_all()[0][0] if count_result else 0
            
            # 查询数据时间范围
            time_sql = f"SELECT MIN(ts), MAX(ts) FROM {table}"
            time_result = self.conn.query(time_sql)
            time_range = time_result.fetch_all()[0] if time_result else (None, None)
            
            stats = {
                'table_name': table,
                'row_count': row_count,
                'min_time': time_range[0].isoformat() if time_range[0] else None,
                'max_time': time_range[1].isoformat() if time_range[1] else None,
                'query_time': datetime.now().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"获取表统计信息失败: {e}")
            return {'error': str(e)}
    
    def close(self) -> None:
        """关闭连接"""
        if self.conn:
            self.conn.close()
        
        self.connected = False
        logger.info("TDengine连接已关闭")

# 使用示例
async def example_usage():
    """TDengineManager使用示例"""
    
    if not TAOS_AVAILABLE:
        print("taos包未安装，跳过TDengine示例")
        return
    
    # 创建TDengine管理器
    tdengine_manager = TDengineManager()
    
    # 连接TDengine
    tdengine_manager.connect()
    
    # 存储股票价格数据
    price_data = {
        'stock_code': '000001',
        'price': 15.67,
        'volume': 1000000,
        'turnover': 15670000,
        'pct_change': 1.49,
        'amplitude': 2.1,
        'tags': {'market': 'SZ', 'industry': '银行'}
    }
    
    await tdengine_manager.store_history_data('stock_prices', price_data)
    
    # 查询历史数据
    start_time = (datetime.now() - timedelta(days=7)).isoformat()
    end_time = datetime.now().isoformat()
    
    history_data = await tdengine_manager.query_history('000001', start_time, end_time)
    print(f"查询到的历史数据: {history_data}")
    
    # 获取表统计信息
    stats = await tdengine_manager.get_table_stats('stock_prices')
    print(f"表统计信息: {stats}")
    
    # 关闭连接
    tdengine_manager.close()

if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())