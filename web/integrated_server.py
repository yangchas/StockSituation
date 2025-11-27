#!/usr/bin/env python3
import asyncio
from tracemalloc import start
import websockets
import json
import pandas as pd
import time
import os
from typing import Dict, Set, List
import logging
import tablib
from aiohttp import web
from datetime import datetime, timedelta
import baostock as bs
import taos
# 修改导入路径
from plate_updater import LazyPlateUpdater, PlateDataSimulator
from redis_storage import RedisStorageManager
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
class TDengineService:
    """TDengine数据库服务"""
    
    def __init__(self, host: str = 'localhost', port: int = 6030, 
                 user: str = 'root', password: str = 'taosdata', database: str = 'market_data1'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self._connect()
    
    def _connect(self):
        """连接TDengine数据库"""
        try:
            self.conn = taos.connect(host=self.host, port=self.port, 
                                   user=self.user, password=self.password, 
                                   database=self.database)
            logger.info("✅ TDengine连接成功")
        except Exception as e:
            logger.error(f"❌ TDengine连接失败: {e}")
            self.conn = None
    
    def execute_query(self, sql: str):
        """执行SQL查询"""
        if not self.conn:
            self._connect()
            if not self.conn:
                return None
        
        try:
            result = self.conn.query(sql)
            return result
        except Exception as e:
            logger.error(f"❌ TDengine查询失败: {e}, SQL: {sql}")
            # 尝试重新连接
            try:
                self._connect()
                if self.conn:
                    result = self.conn.query(sql)
                    return result
            except:
                pass
            return None
    
    def get_minute_kline(self, symbol: str, start_time: str = None, end_time: str = None, 
                        days: int = 1) -> List[Dict]:
        """
        从TDengine获取分钟K线数据
        
        Args:
            symbol: 股票代码 (如 '000001')
            start_time: 开始时间 'YYYY-MM-DD HH:MM:SS'
            end_time: 结束时间 'YYYY-MM-DD HH:MM:SS'
            days: 获取最近几天的数据
            
        Returns:
            List[Dict]: 分钟K线数据
        """
        try:
            # 设置默认时间范围
            if not end_time:
                end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not start_time:
                start_time = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            # 构建SQL查询 - 按1分钟聚合tick数据
            sql = f"""
            SELECT 
                _wstart as time,
                FIRST(lp) as open,
                MAX(lp) as high,
                MIN(lp) as low,
                LAST(lp) as close,
                last(a)-first(a) as amount,
                last(v)-first(v) as volum
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time}' 
                AND ts <= '{end_time}'
            INTERVAL(1m)
            ORDER BY time
            """
            
            logger.info(f"📊 查询TDengine分钟数据: {symbol}, SQL: {sql[:100]}...")
            
            result = self.execute_query(sql)
            if not result:
                return []
            
            # 处理查询结果
            data = []
            for row in result:
                data.append({
                    'time': row[0].strftime('%Y-%m-%d %H:%M:%S'),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': int(row[5]),
                    'amount': float(row[6])
                })
            
            logger.info(f"✅ 从TDengine获取分钟数据成功: {symbol}, 数据点: {len(data)}")
            return data
            
        except Exception as e:
            logger.error(f"❌ 获取TDengine分钟数据失败: {e}")
            return []
    
    def get_tick_data(self, symbol: str, start_time: str = None, end_time: str = None, 
                     limit: int = 1000) -> List[Dict]:
        """
        获取原始tick数据（用于调试）
        
        Args:
            symbol: 股票代码
            start_time: 开始时间
            end_time: 结束时间
            limit: 限制条数
            
        Returns:
            List[Dict]: tick数据
        """
        try:
            if not end_time:
                end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not start_time:
                start_time = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
            
            sql = f"""
            SELECT 
                ts, lp, o, h, l, lc, a, v, p,
                ap1, ap2, ap3, ap4, ap5,
                bp1, bp2, bp3, bp4, bp5,
                av1, av2, av3, av4, av5,
                bv1, bv2, bv3, bv4, bv5,
                inst_vol, inst_amt, large_net
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time}' 
                AND ts <= '{end_time}'
            ORDER BY ts
            LIMIT {limit}
            """
            
            result = self.execute_query(sql)
            if not result:
                return []
            
            data = []
            for row in result:
                data.append({
                    'ts': row[0].strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'last_price': float(row[1]),
                    'open': float(row[2]),
                    'high': float(row[3]),
                    'low': float(row[4]),
                    'last_close': float(row[5]),
                    'amount': float(row[6]),
                    'volume': int(row[7]),
                    'price': float(row[8]),
                    'ask_prices': [float(row[9]), float(row[10]), float(row[11]), float(row[12]), float(row[13])],
                    'bid_prices': [float(row[14]), float(row[15]), float(row[16]), float(row[17]), float(row[18])],
                    'ask_volumes': [int(row[19]), int(row[20]), int(row[21]), int(row[22]), int(row[23])],
                    'bid_volumes': [int(row[24]), int(row[25]), int(row[26]), int(row[27]), int(row[28])],
                    'inst_vol': int(row[29]),
                    'inst_amt': float(row[30]),
                    'large_net': float(row[31])
                })
            
            return data
            
        except Exception as e:
            logger.error(f"❌ 获取TDengine tick数据失败: {e}")
            return []
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 TDengine连接已关闭")
class StockKLineService:
    def __init__(self):
        # 使用现有的RedisStorageManager
        self.redis_storage = RedisStorageManager()
        # 新增：TDengine服务
        self.tdengine = TDengineService()
        # 初始化baostock
        try:
            lg = bs.login()
            if lg.error_code == '0':
                logger.info("✅ Baostock登录成功")
            else:
                logger.error(f"❌ Baostock登录失败: {lg.error_msg}")
        except Exception as e:
            logger.error(f"❌ Baostock初始化失败: {e}")
    
    def get_cache_key(self, code: str, frequency: str, start_date: str, end_date: str) -> str:
        """生成缓存键"""
        key_str = f"kline_{code}_{frequency}_{start_date}_{end_date}"
        return key_str.encode()
    
    def fetch_kline_data(self, code: str, frequency: str = "d", 
                        start_date: str = None, end_date: str = None) -> List[Dict]:
        """获取K线数据"""
        
        # 设置默认日期范围
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            if frequency == "1":
                start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            elif frequency == "5":
                start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            elif frequency == "60":
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            elif frequency == "d":
                start_date = (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')
            elif frequency == "w":
                start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
            
        cache_key = self.get_cache_key(code, frequency, start_date, end_date)
        
        # 检查缓存 - 使用现有的RedisStorageManager
        try:
            cached_data = self.redis_storage.get_data(cache_key)
            if cached_data:
                logger.info(f"📦 从缓存加载K线数据: {code}_{frequency}")
                return json.loads(cached_data)
        except Exception as e:
            logger.error(f"❌ 缓存读取失败: {e}")
                # 根据频率选择数据源
        if frequency in ["1","5"]:
            # 分钟数据从TDengine获取
            data_list = self._fetch_minute_from_tdengine(code, frequency, start_date, end_date)
        else:
            # 日线、周线从baostock获取
            data_list = self._fetch_from_baostock(code, frequency, start_date, end_date)
        
        # 缓存数据（5分钟缓存）
        try:
            cache_time = 300  # 5分钟
            self.redis_storage.store_data(cache_key, data_list, expire_seconds=cache_time)
            logger.info(f"💾 缓存K线数据: {code}_{frequency}, 数据点: {len(data_list)}")
        except Exception as e:
            logger.error(f"❌ 缓存写入失败: {e}")
        
        return data_list
    
    def _fetch_minute_from_tdengine(self, code: str, frequency: str, 
                                  start_date: str, end_date: str) -> List[Dict]:
        """从TDengine获取分钟数据"""
        try:
            # # 提取纯数字股票代码（去除市场前缀）
            # pure_code = self._extract_pure_symbol(code)
            # if not pure_code:
            #     logger.error(f"❌ 无法解析股票代码: {code}")
            #     return []
            
            # 转换为TDengine需要的时间格式
            start_time = f"{start_date} 09:30:00"
            end_time = f"{end_date} 15:00:00"
            
            # 获取分钟数据
            minute_data = self.tdengine.get_minute_kline(code, start_time, end_time)
            
            # 转换数据格式，与baostock保持一致
            converted_data = []
            for item in minute_data:
                converted_data.append({
                    'time': item['time'],
                    'code': code,
                    'open': item['open'],
                    'high': item['high'],
                    'low': item['low'],
                    'close': item['close'],
                    'volume': item['volume'],
                    'amount': item['amount'],
                    'turnover': 0,  # TDengine没有这个字段
                    'pct_chg': 0    # 需要计算
                })
            
            # 计算涨跌幅
            if len(converted_data) > 1:
                for i in range(1, len(converted_data)):
                    prev_close = converted_data[i-1]['close']
                    current_close = converted_data[i]['close']
                    if prev_close > 0:
                        converted_data[i]['pct_chg'] = (current_close - prev_close) / prev_close
            
            logger.info(f"✅ 从TDengine获取{code}的{frequency}分钟数据成功: {len(converted_data)}条")
            return converted_data
            
        except Exception as e:
            logger.error(f"❌ 从TDengine获取分钟数据失败: {e}")
            return []
    def _fetch_from_baostock(self, code: str, frequency: str, 
                           start_date: str, end_date: str) -> List[Dict]:
        # 从baostock获取数据
        try:
            # 构建查询字段
            fields = "date,open,high,low,close,volume,amount"
            print(type(start_date),start_date,type(end_date),end_date)
            # 查询数据
            if code[:2] in ["00","30"]:
                code = f"sz.{code}"
            else:
                code = f"sh.{code}"
            rs = bs.query_history_k_data_plus(
                code, fields, 
                start_date=start_date, 
                end_date=end_date,
                frequency=frequency, 
                adjustflag="3"  # 复权类型: 1-后复权, 2-前复权, 3-不复权
            )
            
            if rs.error_code != '0':
                logger.error(f"❌ 查询K线数据失败: {rs.error_msg}")
                return []
            
            # 处理数据
            data_list = []
            while (rs.error_code == '0') & rs.next():
                row_data = rs.get_row_data()
                data_list.append({
                    'time': row_data[0],
                    # 'code': row_data[1],
                    'open': float(row_data[1]) if row_data[1] else 0,
                    'high': float(row_data[2]) if row_data[2] else 0,
                    'low': float(row_data[3]) if row_data[3] else 0,
                    'close': float(row_data[4]) if row_data[4] else 0,
                    'volume': int(float(row_data[5])) if row_data[5] else 0,
                    'amount': float(row_data[6]) if row_data[6] else 0,
                    # 'turnover': float(row_data[8]) if row_data[8] else 0,
                    # 'pct_chg': float(row_data[9]) if row_data[9] else 0
                })
            
            # 缓存数据（5分钟缓存）- 使用现有的RedisStorageManager
            try:
                cache_time = 300  # 5分钟
                self.redis_storage.store_data(cache_key, json.dumps(data_list), expire_seconds=cache_time)
                logger.info(f"💾 缓存K线数据: {code}_{frequency}, 数据点: {len(data_list)}")
            except Exception as e:
                logger.error(f"❌ 缓存写入失败: {e}")
            
            return data_list
            
        except Exception as e:
            logger.error(f"❌ 获取K线数据异常: {e}")
            return []
    
    def calculate_technical_indicators(self, kline_data: List[Dict]) -> Dict:
        """计算技术指标"""
        if not kline_data:
            return {}
        
        # 这里可以添加BOLL、MACD、KDJ等指标计算
        # 由于计算复杂，这里先返回空指标，后续可以扩展
        if not kline_data or len(kline_data) < 20:
            return {
                'boll': [],
                'macd': [],
                'kdj': []
            }
        try:
            
            # 将数据转换为tablib Dataset
            data = tablib.Dataset()
            data.headers = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']

            for item in kline_data:
                data.append([
                    item['time'],
                    item['open'],
                    item['high'],
                    item['low'],
                    item['close'],
                    item['volume'],
                    item['amount']
                ])

            # 转换为pandas DataFrame进行指标计算
            df = pd.DataFrame(data.dict)

            # 确保数据类型正确
            df['open'] = pd.to_numeric(df['open'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['close'] = pd.to_numeric(df['close'])
            df['volume'] = pd.to_numeric(df['volume'])

            # 按时间排序
            df = df.sort_values('date').reset_index(drop=True)

            indicators = {
                'boll': self._calculate_bollinger_bands(df),
                'macd': self._calculate_macd(df),
                'kdj': self._calculate_kdj(df),
                'ma': self._calculate_moving_averages(df),
                'rsi': self._calculate_rsi(df)
            }

            return indicators

        except Exception as e:
            logger.error(f"❌ 计算技术指标失败: {e}")
            return {
                'boll': [],
                'macd': [],
                'kdj': [],
                'ma': [],
                'rsi': []
            }

    def _calculate_moving_averages(self, df: pd.DataFrame) -> List[Dict]:
        """计算移动平均线"""
        try:
            closes = df['close']

            # 计算不同周期的移动平均线
            ma5 = closes.rolling(window=5).mean()
            ma10 = closes.rolling(window=10).mean()
            ma20 = closes.rolling(window=20).mean()
            ma30 = closes.rolling(window=30).mean()

            ma_data = []
            for i in range(len(df)):
                ma_data.append({
                    'time': df.iloc[i]['date'],
                    'ma5': float(ma5.iloc[i]) if not pd.isna(ma5.iloc[i]) else None,
                    'ma10': float(ma10.iloc[i]) if not pd.isna(ma10.iloc[i]) else None,
                    'ma20': float(ma20.iloc[i]) if not pd.isna(ma20.iloc[i]) else None,
                    'ma30': float(ma30.iloc[i]) if not pd.isna(ma30.iloc[i]) else None
                })

            return ma_data
        except Exception as e:
            logger.error(f"❌ 计算移动平均线失败: {e}")
            return []

    def _calculate_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> List[Dict]:
        """计算布林带指标"""
        try:
            closes = df['close']

            # 计算中轨（20日移动平均线）
            middle_band = closes.rolling(window=period).mean()

            # 计算标准差
            rolling_std = closes.rolling(window=period).std()

            # 计算上轨和下轨
            upper_band = middle_band + (rolling_std * std_dev)
            lower_band = middle_band - (rolling_std * std_dev)

            boll_data = []
            for i in range(len(df)):
                boll_data.append({
                    'time': df.iloc[i]['date'],
                    'upper': float(upper_band.iloc[i]) if not pd.isna(upper_band.iloc[i]) else None,
                    'middle': float(middle_band.iloc[i]) if not pd.isna(middle_band.iloc[i]) else None,
                    'lower': float(lower_band.iloc[i]) if not pd.isna(lower_band.iloc[i]) else None
                })

            return boll_data
        except Exception as e:
            logger.error(f"❌ 计算布林带失败: {e}")
            return []

    def _calculate_macd(self, df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> List[Dict]:
        """计算MACD指标"""
        try:
            closes = df['close']

            # 计算EMA
            ema_fast = closes.ewm(span=fast_period, adjust=False).mean()
            ema_slow = closes.ewm(span=slow_period, adjust=False).mean()

            # 计算DIF（差离值）
            dif = ema_fast - ema_slow

            # 计算DEA（信号线）
            dea = dif.ewm(span=signal_period, adjust=False).mean()

            # 计算MACD柱状图
            macd_hist = (dif - dea) * 2

            macd_data = []
            for i in range(len(df)):
                macd_data.append({
                    'time': df.iloc[i]['date'],
                    'dif': float(dif.iloc[i]) if not pd.isna(dif.iloc[i]) else None,
                    'dea': float(dea.iloc[i]) if not pd.isna(dea.iloc[i]) else None,
                    'macd': float(macd_hist.iloc[i]) if not pd.isna(macd_hist.iloc[i]) else None
                })

            return macd_data
        except Exception as e:
            logger.error(f"❌ 计算MACD失败: {e}")
            return []

    def _calculate_kdj(self, df: pd.DataFrame, period: int = 9) -> List[Dict]:
        """计算KDJ指标"""
        try:
            high = df['high']
            low = df['low']
            close = df['close']

            # 计算最近9日的最高价和最低价
            lowest_low = low.rolling(window=period).min()
            highest_high = high.rolling(window=period).max()

            # 计算RSV（未成熟随机值）
            rsv = ((close - lowest_low) / (highest_high - lowest_low)) * 100

            # 计算K值（RSV的3日指数移动平均）
            k = rsv.ewm(com=2).mean()

            # 计算D值（K值的3日指数移动平均）
            d = k.ewm(com=2).mean()

            # 计算J值（3*K-2*D）
            j = 3 * k - 2 * d

            kdj_data = []
            for i in range(len(df)):
                kdj_data.append({
                    'time': df.iloc[i]['date'],
                    'k': float(k.iloc[i]) if not pd.isna(k.iloc[i]) else None,
                    'd': float(d.iloc[i]) if not pd.isna(d.iloc[i]) else None,
                    'j': float(j.iloc[i]) if not pd.isna(j.iloc[i]) else None
                })

            return kdj_data
        except Exception as e:
            logger.error(f"❌ 计算KDJ失败: {e}")
            return []

    def _calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> List[Dict]:
        """计算RSI指标"""
        try:
            closes = df['close']

            # 计算价格变动
            delta = closes.diff()

            # 分离上涨和下跌
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

            # 计算RS
            rs = gain / loss

            # 计算RSI
            rsi = 100 - (100 / (1 + rs))

            rsi_data = []
            for i in range(len(df)):
                rsi_data.append({
                    'time': df.iloc[i]['date'],
                    'rsi': float(rsi.iloc[i]) if not pd.isna(rsi.iloc[i]) else None
                })

            return rsi_data
        except Exception as e:
            logger.error(f"❌ 计算RSI失败: {e}")
            return []

class IntegratedWebService:
    def __init__(self):
        # 修改：使用LazyPlateUpdater替代原来的PlateUpdater
        self.plate_updater = LazyPlateUpdater(
            'data/板块.csv', 
            'data/个股板块.csv'
        )
        
        # 修改：使用新的PlateDataSimulator
        # self.data_simulator = PlateDataSimulator(self.plate_updater, update_interval=10)
        
        # WebSocket连接管理
        self.plate_connections: Set = set()
        self.volatile_connections: Set = set()
        self.stock_connections: Dict[str, Set] = {}  # 新增：个股订阅连接 {plate_id: set(connections)}
        # 新增：K线服务
        self.kline_service = StockKLineService()
        # 更新统计
        self.update_count = 0
    
    async def start_services(self):
        """启动所有服务"""
        # 启动数据模拟
        # asyncio.create_task(self.data_simulator.start_simulation())
        asyncio.create_task(self.refresh_plate_data_periodically())
        # 启动板块数据广播
        asyncio.create_task(self.broadcast_plate_updates())
        
        # 新增：启动个股数据广播
        asyncio.create_task(self.broadcast_stock_updates())
        
        logger.info("🚀 所有服务已启动")
    async def refresh_plate_data_periodically(self):
        """定期从Redis刷新板块数据（新增）"""
        while True:
            try:
                # 从Redis刷新股票数据并计算板块指标
                self.plate_updater.refresh_stock_data_from_redis()
                
                # 记录刷新状态
                logger.info("🔄 板块数据已从Redis刷新")
                
                # 每10秒刷新一次
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"❌ 刷新板块数据失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_plate_updates(self):
        """定期广播板块更新"""
        while True:
            try:
                if self.plate_connections:
                    # 获取最新数据 - 现在从Redis获取
                    all_metrics = self.plate_updater.get_all_plate_metrics()
                    main_metrics = self.plate_updater.get_main_plates_metrics()
                    
                    update_msg = {
                        'type': 'plate_update',
                        'data': {
                            'all_plates': all_metrics,
                            'main_plates': main_metrics
                        },
                        'timestamp': int(time.time() * 1000),
                        'update_count': self.update_count
                    }
                    
                    # 广播给所有客户端
                    await self.broadcast_to_connections(update_msg, self.plate_connections)
                    
                    self.update_count += 1
                    
                    if self.update_count % 30 == 0:  # 每30次更新记录一次
                        logger.info(f"📤 广播板块更新 #{self.update_count}, 客户端: {len(self.plate_connections)}")
                
                await asyncio.sleep(3)  # 1秒广播一次
                
            except Exception as e:
                logger.error(f"❌ 广播板块更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_stock_updates(self):
        """定期广播个股更新"""
        while True:
            try:
                if self.stock_connections:
                    current_time = int(time.time() * 1000)
                    
                    # 遍历所有被订阅的板块
                    for plate_id, connections in list(self.stock_connections.items()):
                        if not connections:
                            continue
                        
                        # 获取该板块的最新个股数据
                        stocks = self.plate_updater.get_plate_stocks(plate_id)
                        
                        # 构建更新消息
                        update_msg = {
                            'type': 'stock_update',
                            'plate_id': plate_id,
                            'data': stocks,
                            'timestamp': current_time
                        }
                        
                        # 广播给订阅该板块的所有客户端
                        await self.broadcast_to_connections(update_msg, connections)
                    
                    # 每5秒记录一次日志
                    if int(time.time()) % 5 == 0:
                        active_subscriptions = sum(len(conns) for conns in self.stock_connections.values())
                        logger.info(f"📤 广播个股更新, 活跃订阅: {active_subscriptions}个连接")
                
                await asyncio.sleep(3)  # 3秒更新一次
                
            except Exception as e:
                logger.error(f"❌ 广播个股更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_to_connections(self, message: Dict, connections: Set):
        """向连接集合广播消息"""
        if not connections:
            return
            
        disconnected = []
        for ws in connections:
            try:
                await ws.send_str(json.dumps(message, ensure_ascii=False))
            except:
                disconnected.append(ws)
        
        for ws in disconnected:
            connections.remove(ws)
    
    async def handle_plate_websocket(self, request):
        """处理板块数据WebSocket连接"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.plate_connections.add(ws)
        logger.info(f"🔗 板块客户端连接, 总数: {len(self.plate_connections)}")
        
        try:
            # 发送初始数据 - 现在从Redis获取
            hierarchy, main_plates = self.plate_updater.get_plate_hierarchy()
            all_metrics = self.plate_updater.get_all_plate_metrics()
            main_metrics = self.plate_updater.get_main_plates_metrics()
            
            init_data = {
                'type': 'plate_init',
                'data': {
                    'hierarchy': hierarchy,
                    'main_plates': main_plates,
                    'all_plates': all_metrics,
                    'main_plates_metrics': main_metrics
                },
                'timestamp': int(time.time() * 1000)
            }
            
            await ws.send_str(json.dumps(init_data, ensure_ascii=False))
            
            # 处理客户端消息
            async for msg in ws:
                try:
                    if msg.type == web.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        await self.handle_plate_message(data, ws)
                    elif msg.type == web.WSMsgType.ERROR:
                        logger.error(f"WebSocket错误: {ws.exception()}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ 解析消息失败: {e}")
                    
        except Exception as e:
            logger.error(f"❌ 板块WebSocket错误: {e}")
        finally:
            # 连接关闭时清理所有订阅
            self.plate_connections.remove(ws)
            for plate_id in list(self.stock_connections.keys()):
                if ws in self.stock_connections[plate_id]:
                    self.stock_connections[plate_id].remove(ws)
                    if not self.stock_connections[plate_id]:
                        del self.stock_connections[plate_id]
            
            logger.info(f"🔌 板块客户端断开, 总数: {len(self.plate_connections)}")
        
        return ws
    
    async def handle_plate_message(self, data: Dict, websocket):
        """处理板块相关消息"""
        msg_type = data.get('type')
        logger.info(f"📨 收到消息类型: {msg_type}")
        
        if msg_type == 'get_sorted_plates':
            sort_by = data.get('sort_by', 'change_pct')
            plate_type = data.get('plate_type', 'all')  # all, main, sub
            
            if plate_type == 'main':
                plates_data = self.plate_updater.get_main_plates_metrics()
            else:
                plates_data = self.plate_updater.get_all_plate_metrics()
            
            # 排序
            if sort_by == 'change_pct':
                sorted_plates = sorted(plates_data, key=lambda x: x['change_pct'], reverse=True)
            elif sort_by == 'total_volume':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_volume'], reverse=True)
            elif sort_by == 'total_large_net':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_large_net'], reverse=True)
            elif sort_by == 'rise_count':
                sorted_plates = sorted(plates_data, key=lambda x: x['rise_count'], reverse=True)
            else:
                sorted_plates = plates_data
            
            response = {
                'type': 'sorted_plates',
                'data': sorted_plates[:100],  # 限制数量
                'sort_by': sort_by,
                'plate_type': plate_type,
                'timestamp': int(time.time() * 1000)
            }
            
        elif msg_type == 'get_sub_plates':
            main_plate_name = data.get('main_plate')
            logger.info(f"🔍 获取子板块: {main_plate_name}")
            
            sub_plates = self.plate_updater.get_sub_plates_metrics(main_plate_name)
            logger.info(f"📋 找到子板块: {len(sub_plates)}个")
            
            response = {
                'type': 'sub_plates',
                'main_plate': main_plate_name,
                'data': sub_plates,
                'timestamp': int(time.time() * 1000)
            }
        
        elif msg_type == 'get_plate_stocks':
            plate_id = data.get('plate_id')
            logger.info(f"📊 获取板块个股: {plate_id}")
            
            stocks = self.plate_updater.get_plate_stocks(plate_id)
            logger.info(f"📈 找到个股: {len(stocks)}只")
            
            response = {
                'type': 'plate_stocks',
                'plate_id': plate_id,
                'data': stocks,
                'timestamp': int(time.time() * 1000)
            }
        
        elif msg_type == 'get_plate_detail':
            plate_id = data.get('plate_id')
            metrics = self.plate_updater.get_plate_metrics(plate_id)
            
            response = {
                'type': 'plate_detail',
                'data': metrics,
                'timestamp': int(time.time() * 1000)
            }
        
        # 新增：个股订阅消息处理
        elif msg_type == 'subscribe_stocks':
            plate_id = data.get('plate_id')
            action = data.get('action', 'subscribe')  # subscribe 或 unsubscribe
            
            if action == 'subscribe':
                # 订阅个股更新
                if plate_id not in self.stock_connections:
                    self.stock_connections[plate_id] = set()
                self.stock_connections[plate_id].add(websocket)
                logger.info(f"✅ 客户端订阅个股更新: {plate_id}, 当前订阅数: {len(self.stock_connections[plate_id])}")
                
                response = {
                    'type': 'subscribe_result',
                    'plate_id': plate_id,
                    'action': 'subscribed',
                    'message': f'已订阅 {plate_id} 的个股更新'
                }
            else:
                # 取消订阅
                if plate_id in self.stock_connections and websocket in self.stock_connections[plate_id]:
                    self.stock_connections[plate_id].remove(websocket)
                    logger.info(f"❌ 客户端取消订阅个股更新: {plate_id}, 剩余订阅数: {len(self.stock_connections[plate_id])}")
                    
                    # 如果该板块没有订阅者了，清理空集合
                    if not self.stock_connections[plate_id]:
                        del self.stock_connections[plate_id]
                
                response = {
                    'type': 'subscribe_result',
                    'plate_id': plate_id,
                    'action': 'unsubscribed',
                    'message': f'已取消订阅 {plate_id} 的个股更新'
                }
        
        else:
            response = {
                'type': 'error',
                'message': f'未知消息类型: {msg_type}'
            }
        
        # 发送响应
        await websocket.send_str(json.dumps(response, ensure_ascii=False))
    async def handle_stock_kline_api(self, request):
        """处理个股K线数据API请求"""
        try:
            # 获取查询参数
            code = request.query.get('code', '')
            frequency = request.query.get('frequency', 'd')  # d, 5, 60, w
            start_date = request.query.get('start_date', '')
            end_date = request.query.get('end_date', '')
            
            if not code:
                return web.json_response({'error': '股票代码不能为空'}, status=400)
            
            # 验证频率参数
            valid_frequencies = ['5', '60', 'd', 'w']
            if frequency not in valid_frequencies:
                return web.json_response({'error': f'频率参数必须是: {valid_frequencies}'}, status=400)
            
            logger.info(f"📈 请求K线数据: {code}, 频率: {frequency}")
            
            # 获取K线数据
            kline_data = await asyncio.get_event_loop().run_in_executor(
                None, self.kline_service.fetch_kline_data, code, frequency, start_date, end_date
            )
            
            # 计算技术指标
            indicators = self.kline_service.calculate_technical_indicators(kline_data)
            
            response_data = {
                'code': code,
                'frequency': frequency,
                'data': kline_data,
                'indicators': indicators,
                'count': len(kline_data),
                'timestamp': int(time.time() * 1000)
            }
            
            return web.json_response(response_data)
            
        except Exception as e:
            logger.error(f"❌ K线API错误: {e}")
            return web.json_response({'error': str(e)}, status=500)

# HTTP路由处理
async def handle_bankuai(request):
    """板块监控页面"""
    try:
        with open('html/bankuai.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        # 提供简单页面
        html = """
        <!DOCTYPE html>
        <html>
        <head><title>板块监控</title></head>
        <body>
            <h1>板块监控系统</h1>
            <p>请确保 bankuai.html 文件存在</p>
            <p>实时板块数据将通过WebSocket推送</p>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')

async def handle_plate_websocket(request):
    """板块WebSocket"""
    return await service.handle_plate_websocket(request)

async def plate_api(request):
    """板块数据API"""
    try:
        query_type = request.query.get('type', 'all_plates')
        
        if query_type == 'all_plates':
            data = service.plate_updater.get_all_plate_metrics()
        elif query_type == 'main_plates':
            data = service.plate_updater.get_main_plates_metrics()
        elif query_type == 'hierarchy':
            hierarchy, main_plates = service.plate_updater.get_plate_hierarchy()
            data = {'hierarchy': hierarchy, 'main_plates': main_plates}
        else:
            return web.json_response({'error': '未知查询类型'}, status=400)
        
        return web.json_response({
            'data': data,
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ 板块API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def health_check(request):
    """健康检查"""
    return web.json_response({
        'status': 'healthy',
        'plate_connections': len(service.plate_connections),
        'update_count': service.update_count,
        'stock_count': len(service.plate_updater.stock_to_plates),
        'plate_count': len(service.plate_updater.all_plates)
    })

# Redis状态检查
async def redis_status(request):
    """Redis状态检查"""
    try:
        from redis_storage import RedisStorageManager
        storage = RedisStorageManager()
        memory_info = storage.get_memory_info()
        
        return web.json_response({
            'status': 'healthy',
            'redis_memory': memory_info
        })
    except Exception as e:
        return web.json_response({
            'status': 'error',
            'error': str(e)
        }, status=500)

# 调试接口 - 个股数据状态
async def debug_plate_stocks_api(request):
    """调试板块个股API"""
    try:
        plate_id = request.query.get('plate_id', '')
        
        if not plate_id:
            return web.json_response({'error': '请提供plate_id参数'}, status=400)
        
        # 调用调试方法
        service.plate_updater.debug_plate_stocks(plate_id)
        
        # 获取实际的个股数据
        stocks = service.plate_updater.get_plate_stocks(plate_id)
        
        return web.json_response({
            'plate_id': plate_id,
            'stock_count': len(stocks),
            'stocks_sample': stocks[:5]  # 返回前5只作为样本
        })
        
    except Exception as e:
        logger.error(f"❌ 调试接口错误: {e}")
        return web.json_response({'error': str(e)}, status=500)
# 新增：个股K线API路由
async def stock_kline_api(request):
    """个股K线数据API"""
    return await service.handle_stock_kline_api(request)

async def main():
    global service
    service = IntegratedWebService()
    
    # 启动后台服务
    await service.start_services()
    
    # 创建HTTP应用
    app = web.Application()
    
    # 添加路由
    app.router.add_get('/', handle_bankuai)
    app.router.add_get('/bankuai', handle_bankuai)
    app.router.add_get('/ws/plate', handle_plate_websocket)
    app.router.add_get('/api/plate', plate_api)
    app.router.add_get('/api/stock/kline', stock_kline_api)  # 新增K线API
    app.router.add_get('/health', health_check)
    app.router.add_get('/redis-status', redis_status)
    app.router.add_get('/debug/plate-stocks', debug_plate_stocks_api)  # 新增调试接口
    
    # 启动服务器
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("🚀 集成Web服务已启动")
    logger.info("🌐 http://localhost:8080/bankuai - 板块监控")
    logger.info("🔌 ws://localhost:8080/ws/plate - 板块WebSocket")
    logger.info("📊 http://localhost:8080/api/plate - 板块API")
    logger.info("❤️ http://localhost:8080/health - 健康检查")
    logger.info("💾 http://localhost:8080/redis-status - Redis状态")
    logger.info("🐛 http://localhost:8080/debug/plate-stocks?plate_id=801159 - 个股调试")
    
    # 永久运行
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ 服务已停止")