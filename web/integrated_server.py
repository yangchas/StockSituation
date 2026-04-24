#!/usr/bin/env python3
import asyncio
from tracemalloc import start
import aioredis
import websockets
import json
import pandas as pd
import time
import os
from typing import Dict, Set, List, Optional
import logging
import tablib
import base64
import re
# 初始化日志记录器
logger = logging.getLogger(__name__)
from aiohttp import web
from services.f10_service import F10DataService
from services.trading_calendar_service import TradeCalendar, TradingCalendarService
from services.advanced_indicators import OptimizedAdvancedTechnicalIndicators
from services.tdengine_service import TDengineService
from services.kaipan_plate_service import fetch_kaipan_plate_rank

from datetime import datetime, timedelta
import baostock as bs
import numpy as np

# 尝试导入taos库，如果失败则使用模拟实现
try:
    import taos
except Exception as e:
    taos = None
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️  无法导入TDengine库，将使用模拟实现: {e}")
# 修改导入路径
from plate_updater import LazyPlateUpdater,  OptimizedPlateUpdater, OptimizedEnhancedPlateUpdater
from redis_storage import RedisStorageManager
import logging
import asyncio
import os
import sys
import json
from aiohttp import web
import aioredis
from datetime import datetime
import traceback

# Ensure project root is in path to allow importing from ai.API
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import holidays
from market_edge_engine import MarketEdgeEngine
# 题材排行与分析器导入 (整合原有冗余导入)
try:
    try:
        from web.market_edge_theme_ranker import ThemeRanker, SimpleThemeNormalizer
    except ImportError:
        from market_edge_theme_ranker import ThemeRanker, SimpleThemeNormalizer
    logger.info("✅ ThemeRanker & SimpleThemeNormalizer 导入成功")
except ImportError as e:
    logger.error(f"❌ 无法导入 ThemeRanker: {e}")
    ThemeRanker = None
    SimpleThemeNormalizer = None

try:
    try:
        from ai.API.StockAnalyzer import StockAnalyzer
    except ImportError:
        from web.ai.API.StockAnalyzer import StockAnalyzer
    logger.info("✅ StockAnalyzer 导入成功")
except ImportError as e:
    logger.error(f"❌ 无法导入 StockAnalyzer: {e}")
    StockAnalyzer = None

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """统一日志输出配置，重点控制噪声，不改变既有日志风格。"""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    # Python 3.9+ 可重设 stdout/stderr 编码；失败则忽略
    try:
        handler.stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    root.handlers.clear()
    root.addHandler(handler)

    # 常见噪声日志降级
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aioredis").setLevel(logging.WARNING)
    # plate_updater 当前文件内存在历史编码问题，先降噪避免污染控制台
    logging.getLogger("plate_updater").setLevel(
        getattr(logging, os.getenv("PLATE_UPDATER_LOG_LEVEL", "WARNING").upper(), logging.WARNING)
    )


setup_logging()
# 涨停板数据服务 - ztb_service.py
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import asyncio
from collections import defaultdict

logger = logging.getLogger(__name__)

# Local duplicated services (OptimizedAdvancedTechnicalIndicators, EnhancedPlateUpdater, TDengineService) 
# have been removed in favor of imports from the services package and plate_updater module.


class StockKLineService:
    """股票K线服务 - 单例模式"""
    _instance = None
    
    def __new__(cls, tdengine_service=None):
        if cls._instance is None:
            cls._instance = super(StockKLineService, cls).__new__(cls)
            # 使用现有的RedisStorageManager
            cls._instance.redis_storage = RedisStorageManager()
            # 新增：TDengine服务
            cls._instance.tdengine = tdengine_service if tdengine_service else TDengineService()
             # 新增：交易日历服务
            cls._instance.trading_calendar = TradingCalendarService()
            # 初始化baostock
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    logger.info("✅ Baostock登录成功")
                else:
                    logger.error(f"❌ Baostock登录失败: {lg.error_msg}")
            except Exception as e:
                logger.error(f"❌ Baostock初始化失败: {e}")
        return cls._instance
    
    def __init__(self, tdengine_service=None):
        # 单例模式下，__init__可能会被多次调用，所以什么都不做
        pass
    
    def get_cache_key(self, code: str, frequency: str, start_date: str, end_date: str) -> str:
        """生成缓存键"""
        key_str = f"kline_{code}_{frequency}_{start_date}_{end_date}"
        return key_str.encode()
    
    def get_start_date_by_trading_days(self, end_date: str, trading_days_interval: int) -> str:
        """
        从结束日期向前计算指定交易日间隔的开始日期
        
        Args:
            end_date: 结束日期，格式为 'YYYY-MM-DD'
            trading_days_interval: 交易日间隔数
            
        Returns:
            开始日期字符串，格式为 'YYYY-MM-DD'
        """
        current_date = datetime.strptime(end_date, '%Y-%m-%d')
        found_trading_days = 0
        
        # 向前查找指定数量的交易日
        while found_trading_days < trading_days_interval:
            current_date = current_date - timedelta(days=1)
            date_str = current_date.strftime('%Y-%m-%d')
            
            # 如果是交易日，计数加1
            if self.trading_calendar.is_trading_day(date_str):
                found_trading_days += 1
            
            # 防止无限循环
            if (datetime.strptime(end_date, '%Y-%m-%d') - current_date).days > 365 * 2:
                break
        
        return current_date.strftime('%Y-%m-%d')
    
    def fetch_kline_data(self, code: str, frequency: str = "d", 
                        start_date: str = None, end_date: str = None) -> List[Dict]:
        """获取K线数据"""
        
        # 智能设置日期范围，考虑交易日
        if not end_date:
            # 如果是非交易日，使用最后一个交易日作为结束日期
            today = datetime.now().strftime('%Y-%m-%d')
            if not self.trading_calendar.is_trading_day(today):
                end_date = self.trading_calendar.get_previous_trading_day(today)
                logger.info(f"📅 今日({today})非交易日，使用最近交易日: {end_date}")
            else:
                end_date = today
        
        if 1:
            # 为不同频率定义交易日间隔
            trading_days_map = {
                "1": 1,     # 1分钟线：5个交易日
                "5": 1,     # 5分钟线：5个交易日
                "60": 30,   # 60分钟线：30个交易日
                "d": 60,   # 日线：120个交易日
                "w": 100    # 周线：100个交易日（约2年）
            }
            
            # 获取对应频率的交易日间隔
            trading_days_interval = trading_days_map.get(frequency, 120)  # 默认120个交易日
            
            # 计算开始日期
            start_date = self.get_start_date_by_trading_days(end_date, trading_days_interval)
            logger.info(f"📅 基于交易日间隔计算: 频率={frequency}, 结束日期={end_date}, 开始日期={start_date}, 间隔={trading_days_interval}个交易日")
            
        cache_key = self.get_cache_key(code, frequency, start_date, end_date)
        
        # 检查缓存 - 使用现有的RedisStorageManager
        try:
            cached_data = self.redis_storage.get_data(cache_key)
            if cached_data:
                logger.info(f"📦 从缓存加载K线数据: {code}_{frequency}")
                return cached_data
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
             # 检查结束日期是否是交易日，如果不是则调整
            if not self.trading_calendar.is_trading_day(end_date):
                adjusted_end_date = self.trading_calendar.get_previous_trading_day(end_date)
                logger.info(f"📅 结束日期{end_date}非交易日，调整为: {adjusted_end_date}")
                end_date = adjusted_end_date
            # 转换为TDengine需要的时间格式
            start_time = f"{start_date} 09:30:00"
            end_time = f"{end_date} 15:00:00"
            print(f"查询{code} {frequency} 分钟数据时间范围: {start_time} 至 {end_time}")
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
                        converted_data[i]['pct_chg'] = round((current_close - prev_close) / prev_close, 4)
            
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
                # 只在需要时重新登录，避免重复日志
                if rs.error_code in ['10001', '10002', '10003']:  # 常见的登录过期错误码
                    lg = bs.login()
                    if lg.error_code == '0':
                        logger.info("🔄 Baostock重新登录成功")
                    else:
                        logger.error(f"❌ Baostock重新登录失败: {lg.error_msg}")
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

class OptimizedIntegratedWebService:
    """优化版集成Web服务 - 单例模式"""
    _instance = None
    
    def __new__(cls, tdengine_service=None):
        if cls._instance is None:
            cls._instance = super(OptimizedIntegratedWebService, cls).__new__(cls)
            # 初始化Redis存储
            cls._instance.redis_storage = RedisStorageManager()
            
            # 使用优化后的板块更新器（直接集成高级指标）
            cls._instance.plate_updater = OptimizedEnhancedPlateUpdater(
                'data/板块.csv', 
                'data/个股板块.csv',
                cls._instance.redis_storage
            )
            
            # 使用优化后的高级指标服务
            # 使用外部传入的TDengineService实例或创建新实例
            cls._instance.tdengine = tdengine_service if tdengine_service else TDengineService()
            cls._instance.advanced_indicators = OptimizedAdvancedTechnicalIndicators(
                cls._instance.tdengine,  # 使用同一个TDengineService实例
                cls._instance.redis_storage
            )
            
            # WebSocket连接管理
            cls._instance.plate_connections: Set = set()
            cls._instance.plate_data_connections: Set = set()  # 新增：板块数据更新专用连接
            cls._instance.stock_connections: Dict[str, Set] = {}  # 个股订阅连接 {plate_id: set(connections)}
            
            # 新增：K线服务 - 传递同一个TDengineService实例
            cls._instance.kline_service = StockKLineService(cls._instance.tdengine)
            # 新增：F10数据服务
            cls._instance.f10_service = F10DataService('data/f10.csv')
            
            # 更新统计
            cls._instance.update_count = 0
            cls._instance.cached_plate_metrics = []  # 缓存的板块指标
        return cls._instance
    
    def __init__(self, tdengine_service=None):
        # 单例模式下，__init__可能会被多次调用，所以什么都不做
        pass
    
    async def start_optimized_services(self):
        """启动优化后的服务"""
        # 启动板块数据更新
        asyncio.create_task(self.refresh_plate_data_optimized())
        
        # 启动板块数据广播
        asyncio.create_task(self.broadcast_plate_updates_optimized())
        
        # 启动板块数据更新专用广播
        asyncio.create_task(self.broadcast_plate_data_updates())
        
        # 启动个股数据广播
        asyncio.create_task(self.broadcast_stock_updates_optimized())

        logger.info("🚀 优化版服务已启动")

    def _is_trading_active(self) -> bool:
        """判断当前是否处于活跃交易时段 (09:15-11:35, 13:00-15:05)"""
        now = datetime.now()
        val = now.hour * 100 + now.minute
        # 09:15-11:35 or 13:00-15:05
        return (915 <= val <= 1135) or (1300 <= val <= 1505)
    
    async def refresh_plate_data_optimized(self):
        """优化版板块数据刷新"""
        while True:
            try:
                # 使用整合计算获取板块数据（包含高级指标）- Offload to executor to prevent event loop blocking
                loop = asyncio.get_event_loop()
                plate_metrics = await loop.run_in_executor(None, self.plate_updater.get_all_plate_metrics_with_integrated_advanced)
                
                # 缓存到内存供快速访问
                self.cached_plate_metrics = plate_metrics
                
                # 动态调整频率：交易时间 10s，非交易时间 60s
                sleep_time = 6 if self._is_trading_active() else 60
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"❌ 刷新板块数据失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_plate_updates_optimized(self):
        """优化版广播板块更新"""
        while True:
            try:
                if self.plate_connections:
                    # 使用缓存数据或实时获取 - Ensure non-blocking
                    if self.cached_plate_metrics:
                        all_metrics = self.cached_plate_metrics
                    else:
                        loop = asyncio.get_event_loop()
                        all_metrics = await loop.run_in_executor(None, self.plate_updater.get_all_plate_metrics_with_integrated_advanced)
                    
                    # 筛选主板块
                    main_metrics = [m for m in all_metrics if m.get('type') == 'main']
                    
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
                    await self.broadcast_to_connections(update_msg, set(self.plate_connections))
                    
                    self.update_count += 1
                    
                    if self.update_count % 10 == 0:  # 每10次更新记录一次
                        logger.info(f"📤 广播板块更新 #{self.update_count}, 客户端: {len(self.plate_connections)}, 板块数: {len(all_metrics)}")
                
                # 动态调整频率：交易时间 5s，非交易时间 60s
                sleep_time = 5 if self._is_trading_active() else 60
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"❌ 广播板块更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_plate_data_updates(self):
        """广播板块数据更新（专门的WebSocket）"""
        while True:
            try:
                await asyncio.sleep(1)  # 1秒更新一次
                
                if self.plate_data_connections:
                    # 获取最新板块数据（包含高级指标）
                    if self.cached_plate_metrics:
                        all_plates = self.cached_plate_metrics
                    else:
                        loop = asyncio.get_event_loop()
                        all_plates = await loop.run_in_executor(None, self.plate_updater.get_all_plate_metrics_with_integrated_advanced)
                    main_plates = [p for p in all_plates if p.get('type') == 'main']
                    
                    update_msg = {
                        'type': 'plate_data_update',
                        'timestamp': int(time.time() * 1000),
                        'data': {
                            'all_plates': all_plates,
                            'main_plates': main_plates
                        }
                    }
                    
                    # 广播给所有订阅的客户端
                    await self.broadcast_to_connections(update_msg, set(self.plate_data_connections))
                
                # 动态调整频率：交易时间 2s，非交易时间 60s
                sleep_time = 2 if self._is_trading_active() else 60
                await asyncio.sleep(sleep_time)
            except Exception as e:
                logger.error(f"❌ 广播板块数据更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_stock_updates_optimized(self):
        """优化版个股数据广播"""
        while True:
            try:
                all_indicators_dict = {}
                
                if self.stock_connections:
                    current_time = int(time.time() * 1000)
                    
                    # 获取所有活跃股票
                    active_stocks = self._get_active_stocks()
                    if active_stocks:
                        # 使用优化后的批量获取方法 - Offload CPU intensive processing to executor
                        loop = asyncio.get_event_loop()
                        indicators_dict = await loop.run_in_executor(None, self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized, active_stocks)
                        all_indicators_dict = indicators_dict  # 不再 .copy()，减少 ~5000 个 dict 拷贝
                        
                        # 按板块分组广播
                        for plate_id, connections in self.stock_connections.items():
                            if connections and indicators_dict:
                                # 构建优化后的消息
                                update_msg = self._build_optimized_stock_update(plate_id, indicators_dict)
                                await self.broadcast_to_connections(update_msg, set(connections))
                    
                    if int(time.time()) % 10 == 0: # 降低日志频率
                        active_subscriptions = sum(len(conns) for conns in self.stock_connections.values())
                        logger.info(f"📤 广播个股更新, 活跃订阅: {active_subscriptions}个连接")
                
                # 处理Excel页面的股票订阅
                global aiohttp_subscriptions
                if aiohttp_subscriptions:
                    # 获取所有订阅的股票ID
                    all_subscribed_stocks = []
                    for subscription in aiohttp_subscriptions.values():
                        all_subscribed_stocks.extend(subscription['stocks'])
                    
                    # 去重
                    all_subscribed_stocks = list(set(all_subscribed_stocks))
                    
                    if all_subscribed_stocks:
                        # 批量获取所有订阅股票的最新数据
                        loop = asyncio.get_event_loop()
                        subscribed_indicators = await loop.run_in_executor(None, self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized, all_subscribed_stocks)
                        all_indicators_dict.update(subscribed_indicators)
                        
                        # 向订阅客户端推送更新
                        await self.broadcast_stock_updates_to_subscribers(subscribed_indicators)
                
                # 动态调整频率：交易时间 5s，非交易时间 60s
                sleep_time = 5 if self._is_trading_active() else 60
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"❌ 广播个股更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_stock_updates_to_subscribers(self, updated_stocks):
        """广播股票更新到所有订阅了这些股票的客户端"""
        global aiohttp_subscriptions
        
        if not aiohttp_subscriptions or not updated_stocks:
            return
        
        # 按客户端分组需要推送的更新
        client_updates = {}
        
        for ws, subscription_info in aiohttp_subscriptions.items():
            subscribed_stocks = subscription_info['stocks']
            last_data = subscription_info['last_data']
            
            # 找出该客户端订阅的股票中有更新的部分
            client_update = {}
            for stock_id, new_data in updated_stocks.items():
                if stock_id in subscribed_stocks:
                    # 检查是否有实质性变化
                    old_data = last_data.get(stock_id, {})
                    
                    # 只推送有变化的数据
                    changed = False
                    for key in ['change_rate_1min', 'change_pct', 'amount_2min']:
                        # 确保数据是数字类型
                        new_value = float(new_data.get(key, 0))
                        old_value = float(old_data.get(key, 0))
                        if abs(new_value - old_value) > 0.01:
                            changed = True
                            break
                    
                    if changed:
                        client_update[stock_id] = new_data
            
            if client_update:
                client_updates[ws] = client_update
        
        # 推送更新给各个客户端
        for ws, update_data in client_updates.items():
            try:
                # 发送增量更新
                await ws.send_str(json.dumps({
                    'type': 'incremental_update',
                    'data': update_data,
                    'timestamp': int(time.time())
                }))
                
                # 更新客户端的最后数据记录
                if ws in aiohttp_subscriptions:
                    subscription_info = aiohttp_subscriptions[ws]
                    subscription_info['last_data'].update(update_data)
                    
            except Exception as e:
                logger.error(f"❌ 向Excel客户端推送更新出错: {e}")
                # 移除无效连接
                if ws in aiohttp_subscriptions:
                    del aiohttp_subscriptions[ws]
    
    def _get_active_stocks(self) -> List[str]:
        """获取活跃股票列表"""
        active_stocks = []
        for plate_id in self.stock_connections.keys():
            stocks = self.plate_updater.plate_to_stocks.get(plate_id, [])
            active_stocks.extend(stocks)
        
        # 去重
        return list(set(active_stocks))
    
    def _build_optimized_stock_update(self, plate_id: str, indicators_dict: Dict[str, Dict]) -> Dict:
        """构建优化后的个股更新消息 — 内存优化版
        
        优化: 不再逐只调用 get_stock_data() (N+1 Redis 调用)，
        所有数据已在 indicators_dict 中 (通过 batch pipeline 获取)。
        """
        # 获取该板块的股票
        stock_ids = self.plate_updater.plate_to_stocks.get(plate_id, [])
        
        stocks_data = []
        for stock_id in stock_ids:
            indicators = indicators_dict.get(stock_id)
            if indicators:
                # 所有字段已在 indicators 中 (含 name, market_cap 等)
                # 直接引用，不再创建新 dict
                stock_info = {
                    'code': stock_id,
                    'name': indicators.get('name', f"股票{stock_id}"),
                    'change_pct': indicators.get('change_pct', 0),
                    'price': indicators.get('price', 0),
                    'volume': indicators.get('volume', 0),
                    'market_cap': indicators.get('market_cap', 0),
                    'large_net': indicators.get('large_net', 0),
                    'timestamp': indicators.get('timestamp', 0),
                    # 高级指标
                    'change_rate_1min': indicators.get('change_rate_1min', 0),
                    'amount_2min': indicators.get('amount_2min', 0)
                }
                stocks_data.append(stock_info)
        
        return {
            'type': 'stock_update',
            'plate_id': plate_id,
            'data': stocks_data,
            'timestamp': int(time.time() * 1000)
        }
    
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
            # 发送初始数据
            hierarchy, main_plates = self.plate_updater.get_plate_hierarchy()
            all_metrics = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
            main_metrics = [m for m in all_metrics if m.get('type') == 'main']
            
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
    
    async def handle_plate_data_websocket(self, request):
        """处理板块数据WebSocket连接（专门用于实时更新）"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.plate_data_connections.add(ws)
        logger.info(f"🔗 板块数据更新客户端连接, 总数: {len(self.plate_data_connections)}")
        
        try:
            # 发送初始数据
            init_data = {
                'type': 'plate_data_init',
                'timestamp': int(time.time() * 1000),
                'data': {
                    'all_plates': self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced(),
                    'main_plates': [p for p in (self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()) if p.get('type') == 'main']
                }
            }
            await ws.send_str(json.dumps(init_data, ensure_ascii=False))
            
            # 保持连接
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data.get('type') == 'ping':
                        await ws.send_str(json.dumps({'type': 'pong'}))
                elif msg.type == web.WSMsgType.ERROR:
                    break
                    
        except Exception as e:
            logger.error(f"❌ 板块数据WebSocket错误: {e}")
        finally:
            self.plate_data_connections.remove(ws)
            logger.info(f"🔌 板块数据更新客户端断开, 总数: {len(self.plate_data_connections)}")
        
        return ws
    
    async def handle_plate_message(self, data: Dict, websocket):
        """处理板块相关消息"""
        msg_type = data.get('type')
        logger.info(f"📨 收到消息类型: {msg_type}")
        
        if msg_type == 'get_sorted_plates':
            sort_by = data.get('sort_by', 'change_pct')
            plate_type = data.get('plate_type', 'all')  # all, main, sub
            
            if plate_type == 'main':
                plates_data = [p for p in (self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()) if p.get('type') == 'main']
            else:
                plates_data = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
            
            # 排序
            if sort_by == 'change_pct':
                sorted_plates = sorted(plates_data, key=lambda x: x['change_pct'], reverse=True)
            elif sort_by == 'total_volume':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_volume'], reverse=True)
            elif sort_by == 'total_large_net':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_large_net'], reverse=True)
            elif sort_by == 'rise_count':
                sorted_plates = sorted(plates_data, key=lambda x: x['rise_count'], reverse=True)
            elif sort_by == 'change_rate_1min':
                sorted_plates = sorted(plates_data, key=lambda x: x.get('change_rate_1min', 0), reverse=True)
            elif sort_by == 'total_amount_2min':
                sorted_plates = sorted(plates_data, key=lambda x: x.get('total_amount_2min', 0), reverse=True)
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
            
            # 使用优化后的批量获取高级指标
            stock_codes = [stock.get('code') for stock in stocks if stock.get('code')]
            if stock_codes:
                try:
                    # 使用优化后的批量获取方法
                    advanced_indicators_dict = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_codes)
                    
                    # 将高级指标合并到个股数据中
                    for stock in stocks:
                        stock_code = stock.get('code')
                        if stock_code in advanced_indicators_dict:
                            indicators = advanced_indicators_dict[stock_code]
                            stock['advanced_indicators'] = {
                                'change_rate_1min': indicators.get('change_rate_1min', 0),
                                'amount_2min': indicators.get('amount_2min', 0),
                                'timestamp': int(time.time() * 1000)
                            }
                            logger.debug(f"✅ 已合并股票 {stock_code} 的高级指标")
                except Exception as e:
                    logger.error(f"❌ 批量获取高级指标失败: {e}")
            
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
        
        # 新增：获取全部个股数据（支持差异更新）
        elif msg_type == 'get_all_stocks':
            client_timestamp = data.get('last_update', 0)  # 客户端上次更新时间
            force_full = data.get('force_full', False)    # 是否强制全量更新
            
            logger.info(f"📊 请求全部个股数据, 客户端时间戳: {client_timestamp}, 强制全量: {force_full}")
            
            # 获取服务器端最新数据
            all_stocks_data = self.plate_updater.refresh_all_stocks_data()
            
            if force_full or client_timestamp == 0:
                # 全量更新模式
                response = {
                    'type': 'all_stocks',
                    'data': all_stocks_data,
                    'update_type': 'full',
                    'timestamp': int(time.time() * 1000),
                    'count': len(all_stocks_data)
                }
                logger.info(f"📤 全量推送全部个股数据: {len(all_stocks_data)} 只股票")
            else:
                # 差异更新模式 - 只返回有变化的股票
                changed_stocks = {}
                
                for stock_id, stock_data in all_stocks_data.items():
                    # 检查股票数据是否有变化（基于时间戳或关键字段变化）
                    if self._has_stock_changed(stock_id, stock_data, client_timestamp):
                        changed_stocks[stock_id] = stock_data
                
                if changed_stocks:
                    response = {
                        'type': 'all_stocks',
                        'data': changed_stocks,
                        'update_type': 'delta',
                        'timestamp': int(time.time() * 1000),
                        'count': len(changed_stocks),
                        'total_count': len(all_stocks_data)
                    }
                    logger.info(f"📤 差异推送个股数据: {len(changed_stocks)} 只有变化的股票")
                else:
                    # 没有变化，只返回时间戳确认
                    response = {
                        'type': 'all_stocks',
                        'update_type': 'no_change',
                        'timestamp': int(time.time() * 1000),
                        'message': '没有数据变化'
                    }
                    logger.info("📤 个股数据无变化，返回确认")
        
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
    
    def _has_stock_changed(self, stock_id: str, stock_data: Dict, client_timestamp: int) -> bool:
        """检查股票数据是否有变化（支持差异更新）"""
        try:
            # 检查时间戳变化
            stock_timestamp = stock_data.get('basic', {}).get('timestamp', 0)
            if stock_timestamp > client_timestamp:
                return True
            
            # 检查关键字段变化（涨跌幅、价格、成交量等）
            # 从Redis获取上次的数据进行比较
            cache_key = f"stock:last_sent:{stock_id}"
            last_sent_data = self.redis_storage.get_data(cache_key)
            
            if not last_sent_data:
                # 没有历史数据，视为有变化
                self.redis_storage.store_data(cache_key, stock_data, expire_seconds=300)  # 缓存5分钟
                return True
            
            # 比较关键字段
            key_fields = ['change_pct', 'price', 'volume', 'large_net']
            
            for field in key_fields:
                current_value = stock_data.get('basic', {}).get(field, 0)
                last_value = last_sent_data.get('basic', {}).get(field, 0)
                
                # 对于数值字段，检查是否有显著变化
                if isinstance(current_value, (int, float)) and isinstance(last_value, (int, float)):
                    if abs(current_value - last_value) > 0.001:  # 微小变化阈值
                        self.redis_storage.store_data(cache_key, stock_data, expire_seconds=300)
                        return True
            
            # 检查高级指标变化
            current_advanced = stock_data.get('advanced', {})
            last_advanced = last_sent_data.get('advanced', {})
            
            advanced_fields = ['change_rate_1min', 'amount_2min']
            for field in advanced_fields:
                current_val = current_advanced.get(field, 0)
                last_val = last_advanced.get(field, 0)
                
                if abs(current_val - last_val) > 0.01:  # 高级指标变化阈值
                    self.redis_storage.store_data(cache_key, stock_data, expire_seconds=300)
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ 检查股票变化失败 {stock_id}: {e}")
            return True  # 出错时保守地返回有变化
    
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
            valid_frequencies = ['1','5', '60', 'd', 'w']
            if frequency not in valid_frequencies:
                return web.json_response({'error': f'频率参数必须是: {valid_frequencies}'}, status=400)
            
            logger.info(f"📈 请求K线数据: {code}, 频率: {frequency}")
            
            # 获取K线数据
            kline_data = await asyncio.get_event_loop().run_in_executor(
                None, self.kline_service.fetch_kline_data, code, frequency, start_date, end_date
            )
            
            # 计算技术指标 - CPU 密集型，移至线程池
            loop = asyncio.get_event_loop()
            indicators = await loop.run_in_executor(
                None, self.kline_service.calculate_technical_indicators, kline_data
            )
            
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
class StockVolatileMonitor:
    def __init__(self):
        self.redis = None
        self.redis_storage = RedisStorageManager()  # 复用现有Redis缓存工具，读取昨日涨停集合等
        self.connections: Set = set()  # 移除类型注解以兼容两种WebSocket类型
        self.volatile_pool_key = "stock:volatile_pool"  # 修正键名，从真正的异动池获取数据
        self.first_limit_key = "stock:first_limit_up"  # 严格首板票存储键名
        self.last_check_timestamp = 0
        self.monitoring_active = False
        self.calendar = TradingCalendarService()  # 避免在监控循环中重复创建
        self.prev_day_cache = None
        self.yesterday_limit_set_cache = set()
        self.stock_names: Dict[str, str] = {}  # 用于名称解析回退
        # 去重防抖：避免同一异动事件/首板事件在短时间内重复广播
        self.recent_alert_dedup: Dict[str, int] = {}
        self.alert_dedup_window_sec = 8
        self.symbol_broadcast_cooldown_sec = 30
        self.symbol_last_broadcast_ts: Dict[str, int] = {}
        
    async def connect_redis(self):
        """连接Redis"""
        try:
            self.redis = await aioredis.from_url(
                "redis://localhost:6379/0",
                encoding='utf-8',
                decode_responses=True,
                max_connections=10
            )
            
            # 测试连接
            await self.redis.ping()
            logger.info("✅ Redis连接成功")
        except Exception as e:
            logger.error(f"❌ Redis连接失败: {e}")
            raise
    
    async def check_key_exists(self):
        """检查Redis键是否存在"""
        try:
            exists = await self.redis.exists(self.volatile_pool_key)
            if exists:
                key_type = await self.redis.type(self.volatile_pool_key)
                count = await self.redis.zcard(self.volatile_pool_key)
                # if count > 0:  # 只在有数据时记录
                    # logger.info(f"✅ 找到键: {self.volatile_pool_key}, 类型: {key_type}, 数据量: {count}")
                return True
            else:
                logger.warning(f"⚠️ Redis键不存在: {self.volatile_pool_key}")
                return False
        except Exception as e:
            logger.error(f"❌ 检查Redis键失败: {e}")
            return False
    
    async def init_last_check_timestamp(self):
        """初始化最后检查时间戳"""
        try:
            exists = await self.redis.exists(self.volatile_pool_key)
            if not exists:
                logger.warning("Redis键不存在，使用当前时间")
                self.last_check_timestamp = int(time.time() * 1000)
                return
            
            # 获取有序集合中最后一条数据
            last_items = await self.redis.zrevrange(
                self.volatile_pool_key, 0, 0, withscores=True
            )
            
            if last_items:
                last_data_str, last_timestamp = last_items[0]
                self.last_check_timestamp = int(last_timestamp)
                logger.info(f"⏰ 初始化为最后一条消息的时间戳: {self.last_check_timestamp}")
                
                try:
                    if isinstance(last_data_str, bytes):
                        last_data_str = last_data_str.decode('utf-8', errors='ignore')
                    last_data = json.loads(last_data_str)
                    symbol = last_data.get('symbol', '未知')
                    reason = last_data.get('reason', '')
                    logger.info(f"📊 最后一条数据: {symbol} - {reason}")
                except Exception as e:
                    logger.warning(f"解析最后一条数据失败: {e}")
            else:
                self.last_check_timestamp = int(time.time() * 1000)
                logger.info(f"⏰ Redis键为空，使用当前时间戳: {self.last_check_timestamp}")
                
        except Exception as e:
            logger.error(f"❌ 初始化最后检查时间戳失败: {e}")
            self.last_check_timestamp = int(time.time() * 1000)

    def _build_alert_dedup_key(self, data: Dict, score_ms: int) -> str:
        """构建异动事件去重键。"""
        symbol = str(data.get('symbol', '')).strip()
        reason = str(data.get('reason', '')).strip()
        # 以 2 秒时间桶去重，兼顾实时性和抗重复能力
        bucket = int(score_ms // 2000)
        return f"{symbol}|{reason}|{bucket}"

    def _clean_stock_name(self, name: str, code: str) -> str:
        """清理股票名称中重复拼接的代码后缀，如  洲际油气(600759)  -> 洲际油气"""
        if not name:
            return name
        n = str(name).strip()
        c = str(code).strip()
        if c:
            n = re.sub(rf"\({re.escape(c)}\)$", "", n).strip()
        # 兜底：清理任意末尾6位代码括号
        n = re.sub(r"\(\d{6}\)$", "", n).strip()
        return n

    def _should_skip_duplicate_alert(self, data: Dict, score_ms: int) -> bool:
        """判断是否为短时间重复异动事件。"""
        now_sec = int(time.time())
        key = self._build_alert_dedup_key(data, score_ms)
        expire_at = self.recent_alert_dedup.get(key, 0)
        if expire_at > now_sec:
            return True
        self.recent_alert_dedup[key] = now_sec + self.alert_dedup_window_sec

        # 懒清理过期键，防止字典无限增长
        if len(self.recent_alert_dedup) > 5000:
            self.recent_alert_dedup = {
                k: v for k, v in self.recent_alert_dedup.items() if v > now_sec
            }
        return False

    def _should_skip_symbol_cooldown(self, symbol: str, now_ms: int) -> bool:
        """同一只票的广播冷却，避免短时间反复刷屏。"""
        if not symbol:
            return False
        last_ms = self.symbol_last_broadcast_ts.get(symbol, 0)
        if last_ms > 0 and now_ms - last_ms < self.symbol_broadcast_cooldown_sec * 1000:
            return True
        self.symbol_last_broadcast_ts[symbol] = now_ms
        if len(self.symbol_last_broadcast_ts) > 8000:
            threshold = now_ms - self.symbol_broadcast_cooldown_sec * 1000
            self.symbol_last_broadcast_ts = {k: v for k, v in self.symbol_last_broadcast_ts.items() if v >= threshold}
        return False

    async def _is_first_limit_new_symbol_today(self, symbol: str, today_str: str) -> bool:
        """判断今日是否首次触发首板（跨轮询去重）。"""
        if not symbol:
            return False
        if not self.redis:
            return True
        key = f"{self.first_limit_key}:seen:{today_str}"
        try:
            added = await self.redis.sadd(key, symbol)
            await self.redis.expire(key, 2 * 24 * 60 * 60)
            return bool(added)
        except Exception as e:
            logger.warning(f"⚠️ 首板去重写入失败，降级放行 {symbol}: {e}")
            return True
    
    async def monitor_volatile_stocks(self):
        """监控股票异动数据"""
        logger.info("🔍 开始监控股票异动数据...")
        
        check_count = 0
        consecutive_missing_count = 0
        
        while True:
            try:
                check_count += 1
                
                # 检查Redis键是否存在
                key_exists = await self.check_key_exists()
                
                if not key_exists:
                    consecutive_missing_count += 1
                    self.monitoring_active = False
                    
                    if consecutive_missing_count <= 3:
                        wait_time = 5
                    elif consecutive_missing_count <= 10:
                        wait_time = 10
                    else:
                        wait_time = 30
                    
                    if consecutive_missing_count % 5 == 0:
                        logger.warning(f"⚠️ Redis键不存在，等待 {wait_time} 秒后重试 (连续缺失: {consecutive_missing_count})")
                    
                    # 通知客户端监控暂停
                    if self.connections and consecutive_missing_count == 1:
                        await self.broadcast_system_message("监控暂停：数据源暂时不可用，正在重连...")
                    
                    await asyncio.sleep(wait_time)
                    continue
                
                # 键存在时的处理逻辑
                if not self.monitoring_active:
                    logger.info("✅ Redis键已恢复，重新开始监控")
                    self.monitoring_active = True
                    consecutive_missing_count = 0
                    await self.init_last_check_timestamp()
                    
                    # 通知客户端监控恢复
                    if self.connections:
                        await self.broadcast_system_message("监控恢复：数据源已连接")
                
                # 正常监控逻辑
                if check_count % 120 == 0:
                    logger.info("🔄 监控运行中...")
                
                current_timestamp = int(time.time() * 1000)
                
                # 获取新数据
                new_data = await self.redis.zrangebyscore(
                    self.volatile_pool_key,
                    min=self.last_check_timestamp + 1,
                    max=current_timestamp,
                    withscores=True
                )
                
                if new_data:
                    logger.info(f"🎯 发现 {len(new_data)} 条新异动数据")
                    
                    # 当前批次去重，避免同一只票在同一秒多次广播
                    processed_in_batch = set()
                    max_seen_score = self.last_check_timestamp
                    
                    for data_str, score in new_data:
                        try:
                            score_i = int(score)
                            if score_i > max_seen_score:
                                max_seen_score = score_i

                            if isinstance(data_str, bytes):
                                data_str = data_str.decode('utf-8', errors='ignore')
                            
                            data = json.loads(data_str)
                            symbol = data.get('symbol', '未知')

                            # 简单的去重逻辑：同一秒内同一代码只处理一次核心逻辑
                            if symbol in processed_in_batch:
                                continue
                            processed_in_batch.add(symbol)

                            # 跨批次去重：过滤短时间重复异动
                            if self._should_skip_duplicate_alert(data, int(score)):
                                continue
                            # 同票冷却：避免同一股票短时间重复广播
                            if self._should_skip_symbol_cooldown(str(symbol), int(score)):
                                continue

                            # print(data)
                            data['timestamp'] = score_i
                            await self.broadcast_volatile_alert(data)
                            
                            # 检查是否为涨停票，如果是则推送首板票
                            change = data.get('change', 0)
                            try:
                                if isinstance(change, str):
                                    # 去除百分号并转换为浮点数
                                    change = float(change.rstrip('%'))
                                elif not isinstance(change, (int, float)):
                                    change = 0
                            except ValueError:
                                change = 0

                            # 统一为百分比口径：0.2 -> 20.0，11.07 -> 11.07
                            if isinstance(change, (int, float)) and abs(change) <= 1:
                                change = float(change) * 100.0
                            
                            # 根据股票前缀判断涨停阈值
                            symbol = data.get('symbol', '')
                            # 10%板：60/00；20%板：30/68
                            if symbol.startswith(('60', '00')):
                                limit_up_threshold = 9.8
                            elif symbol.startswith(('30', '68')):
                                limit_up_threshold = 19.8
                            else:
                                # 兜底按10%处理，避免误将未知代码误判为20%
                                limit_up_threshold = 9.8
                            
                            # 如果是涨停票，按“严格首板”规则写入 Redis：涨停且不在昨日涨停集合
                            if change >= limit_up_threshold:
                                symbol = str(symbol).strip()

                                # 计算昨日交易日（复用初始化的交易日历，避免循环内重复创建）
                                today_str = datetime.now().strftime('%Y-%m-%d')
                                prev_day = self.calendar.get_previous_trade_day(today_str)

                                # 昨日涨停集合：优先读取 limit_up_{prev_day}（连板数据日更缓存），并fallback到综合视图缓存
                                # 这里做一层缓存，避免每条异动都去读Redis
                                if self.prev_day_cache != prev_day:
                                    self.prev_day_cache = prev_day
                                    self.yesterday_limit_set_cache = set()
                                    try:
                                        # 1) 主优先：limit_up_{prev_day}
                                        key_limit_up = f"limit_up_{prev_day}"
                                        loop = asyncio.get_event_loop()
                                        prev_limit_up_result = await loop.run_in_executor(None, self.redis_storage.get_data, key_limit_up)

                                        # 2) fallback：cache:comprehensive:prev_limit_up:{prev_day}
                                        if not prev_limit_up_result:
                                            key_prev_limit_up = f"cache:comprehensive:prev_limit_up:{prev_day}"
                                            prev_limit_up_result = await loop.run_in_executor(None, self.redis_storage.get_data, key_prev_limit_up)

                                        if prev_limit_up_result:
                                            for item in prev_limit_up_result:
                                                if isinstance(item, dict):
                                                    code = str(item.get('code', '') or item.get('股票代码', '')).strip()
                                                    if code:
                                                        self.yesterday_limit_set_cache.add(code)
                                    except Exception:
                                        # 如果读取失败，保守为空集合
                                        self.yesterday_limit_set_cache = set()

                                yesterday_limit_set = self.yesterday_limit_set_cache

                                # 严格首板：不在昨日涨停集合
                                if symbol in yesterday_limit_set:
                                    continue

                                # 今日首板去重：同一只票当天只推送一次首板
                                if not await self._is_first_limit_new_symbol_today(symbol, today_str):
                                    continue

                                first_limit_data = data.copy()
                                first_limit_data['type'] = 'first_limit'
                                first_limit_data['change_pct'] = change  # 确保change_pct字段存在

                                # 广播首板警报
                                await self.broadcast_first_limit_alert(first_limit_data)

                                # 写入 Redis 的 stock:first_limit_up（严格首板池）
                                try:
                                    await self.redis.zadd(self.first_limit_key, {
                                        json.dumps(first_limit_data): int(time.time())
                                    })
                                    await self.redis.expire(self.first_limit_key, 24 * 60 * 60)
                                    logger.info(f"✅ 成功写入Redis首板数据: {first_limit_data.get('symbol')} ({change}%)")
                                except Exception as e:
                                    logger.error(f"❌ 写入Redis首板数据失败: {e}")
                            
                            # 更新最后检查时间戳
                            if score_i > self.last_check_timestamp:
                                self.last_check_timestamp = score_i
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ JSON解析错误: {e}, 数据: {data_str[:100]}...")
                        except Exception as e:
                            logger.error(f"❌ 处理数据错误: {e}")

                    # 即使中途 continue 跳过业务处理，也要推进检查时间戳，避免重复抓取同一批数据
                    if max_seen_score > self.last_check_timestamp:
                        self.last_check_timestamp = max_seen_score
                    
                    logger.info(f"⏰ 最后检查时间戳更新为: {self.last_check_timestamp}")
                    await asyncio.sleep(3.0)  # 用户要求改为 3秒
                else:
                    count = await self.redis.zcard(self.volatile_pool_key)
                    if count > 0:
                        await asyncio.sleep(2)
                    else:
                        await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ 监控异动数据错误: {e}")
                self.monitoring_active = False
                await asyncio.sleep(5)
    
    async def broadcast_system_message(self, message: str):
        """广播系统消息到所有客户端"""
        system_message = {
            'type': 'system',
            'message': message,
            'timestamp': int(time.time() * 1000)
        }
        
        if self.connections:
            disconnected = []
            for ws in self.connections:
                try:
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(system_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(system_message, ensure_ascii=False))
                except:
                    disconnected.append(ws)
            
            for ws in disconnected:
                self.connections.remove(ws)
    
    async def broadcast_volatile_alert(self, data: Dict):
        """广播异动警报到所有客户端"""
        alert_message = self.format_volatile_alert(data)
        
        logger.info(f"📢 广播: {alert_message['symbol']}({alert_message['name']}) - {alert_message['action_text']}")
        
        if self.connections:
            disconnected = []
            for ws in self.connections:
                try:
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(alert_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(alert_message, ensure_ascii=False))
                except:
                    disconnected.append(ws)
            
            for ws in disconnected:
                self.connections.remove(ws)
    
    async def broadcast_first_limit_alert(self, data: Dict):
        """广播首板票警报到所有客户端"""
        alert_message = self.format_first_limit_alert(data)
        
        logger.info(f"📢 首板广播: {alert_message['code']} - {alert_message['name']}")
        
        # 构建符合前端期望的first_limit消息格式
        ws_message = {
            'type': 'first_limit',
            'payload': alert_message
        }
        
        # 同时也广播incremental_update消息，保持兼容性
        stock_id = alert_message['code']
        incremental_data = {
            stock_id: {
                'name': alert_message['name'],
                'price': alert_message['price'],
                'change_pct': alert_message['change_pct'],
                'amount': alert_message['trade_amount'],
                'change_rate_1min': alert_message.get('change_rate_1min', 0),
                'plate': alert_message['plate'],
                'is_first_limit': True  # 添加首板标识
            }
        }
        
        incremental_ws_message = {
            'type': 'incremental_update',
            'data': incremental_data,
            'timestamp': int(time.time())
        }
        
        if self.connections:
            disconnected = []
            for ws in self.connections:
                try:
                    # 发送first_limit消息（符合前端期望）
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(ws_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(ws_message, ensure_ascii=False))
                    
                    # 发送incremental_update消息（保持兼容性）
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(incremental_ws_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(incremental_ws_message, ensure_ascii=False))
                except:
                    disconnected.append(ws)
            
            for ws in disconnected:
                self.connections.remove(ws)
    
    def format_first_limit_alert(self, data: Dict) -> Dict:
        """格式化首板票警报消息"""
        code = data.get('symbol', '')  # 使用code字段，而不是symbol
        name = data.get('name', '')
        if not name:
             name = self.stock_names.get(code, "")
        name = self._clean_stock_name(name, code)
        change = data.get('change', 0)
        price = data.get('price', 0)
        reason = data.get('reason', '')
        timestamp = data.get('timestamp', 0)
        trade_amount = data.get('amount', 0)
        plate = data.get('plate', '')
        
        if isinstance(reason, bytes):
            reason = reason.decode('utf-8', errors='ignore')
        
        # 转换时间戳
        try:
            dt = time.localtime(timestamp / 1000)
            display_time = time.strftime("%H:%M:%S", dt)
        except:
            display_time = time.strftime("%H:%M:%S")
        
        # 确保change是数字类型，并且转换为百分比形式
        if isinstance(change, str):
            # 如果是字符串，去掉百分号并转换为数字
            change_pct = float(change.replace('%', ''))
        else:
            # 数字口径兼容：0.2 -> 20.0；11.07 -> 11.07
            if isinstance(change, (int, float)):
                change_pct = float(change) * 100 if abs(float(change)) <= 1 else float(change)
            else:
                change_pct = 0
        
        return {
            'code': code,  # 前端使用code字段
            'name': name,
            'price': float(price),
            'change': str(change),
            'change_pct': change_pct,  # 前端需要的涨跌幅百分比
            'reason': reason,
            'action_text': "首板涨停",
            'alert_level': "high",
            'color_class': "limit_up",
            'timestamp': timestamp,
            'display_time': display_time,
            'trade_amount': float(trade_amount),  # 成交额
            'plate': plate,  # 所属板块
            'change_rate_1min': data.get('change_rate_1min', 0)  # 1分钟涨速
        }
    
    def format_volatile_alert(self, data: Dict) -> Dict:
        """格式化异动警报消息"""
        symbol = data.get('symbol', '')
        name_b64 = data.get('name_b64', '')
        name = ""
        if name_b64:
            try:
                if isinstance(name_b64, str):
                    name_b64 = name_b64.encode('utf-8')
                name = base64.b64decode(name_b64).decode('utf-8', errors='ignore')
            except Exception:
                pass
        
        if not name:
            name = self.stock_names.get(symbol, "")
        name = self._clean_stock_name(name, symbol)
        change = data.get('change', '')
        amount = data.get('amount', '')
        reason = data.get('reason', '')
        strength = data.get('strength', 0)
        price = data.get('price', 0)
        change_5min = data.get('change_5min', 0)
        large_net_5min = data.get('large_net_5min', 0)
        amount_5min = data.get('amount_5min', 0)
        timestamp = data.get('timestamp', 0)
        
        if isinstance(reason, bytes):
            reason = reason.decode('utf-8', errors='ignore')
        
        # 根据强度确定警报级别
        if strength >= 8:
            alert_level = "high"
            level_text = "强烈异动"
        elif strength >= 5:
            alert_level = "medium" 
            level_text = "中度异动"
        else:
            alert_level = "low"
            level_text = "轻微异动"
        
        # 生成显示文本
        if "封单" in reason or "Top" in reason:
            action_text = "封单异动"
            color_class = "breakthrough"
        elif "Amount" in reason or "amount" in reason:
            action_text = "成交额异动"
            color_class = "large-order"
        else:
            action_text = reason
            color_class = "normal"
        
        # 转换时间戳
        try:
            dt = time.localtime(timestamp / 1000)
            display_time = time.strftime("%H:%M:%S", dt)
        except:
            display_time = time.strftime("%H:%M:%S")
        
        return {
            'type': 'volatile_alert',
            'symbol': symbol,
            'name' : name,
            'price': float(price),
            'amount':amount,
            'change': change,
            'reason': reason,
            'action_text': action_text,
            'level_text': level_text,
            'alert_level': alert_level,
            'color_class': color_class,
            'strength': int(strength),
            'change_5min': float(change_5min),
            'large_net_5min': float(large_net_5min),
            'amount_5min': float(amount_5min),
            'timestamp': timestamp,
            'display_time': display_time
        }
    
    async def get_recent_volatiles(self, count: int = 50):
        """获取最近的异动数据"""
        try:
            exists = await self.redis.exists(self.volatile_pool_key)
            if not exists:
                logger.warning("Redis键不存在，无法获取历史数据")
                return []
            
            recent_data = await self.redis.zrevrange(
                self.volatile_pool_key, 0, count - 1, withscores=False
            )
            
            volatiles = []
            for data_str in recent_data:
                try:
                    if isinstance(data_str, bytes):
                        data_str = data_str.decode('utf-8', errors='ignore')
                    data = json.loads(data_str)
                    formatted = self.format_volatile_alert(data)
                    volatiles.append(formatted)
                except Exception as e:
                    logger.error(f"解析历史数据错误: {e}")
                    continue
            
            logger.info(f"📚 加载 {len(volatiles)} 条历史异动数据")
            return volatiles
        except Exception as e:
            logger.error(f"❌ 获取历史异动数据错误: {e}")
            return []
    
    async def handler(self, websocket):
        """处理WebSocket连接 - 兼容aiohttp和websockets"""
        self.connections.add(websocket)
        logger.info(f"🔗 客户端连接，当前连接数: {len(self.connections)}")
        
        try:
            # 发送欢迎消息
            if hasattr(websocket, 'send_str'):  # aiohttp WebSocket
                await websocket.send_str(json.dumps({
                    'type': 'system',
                    'message': '连接成功，开始接收股票异动数据...',
                    'monitoring_active': self.monitoring_active
                }))
            else:  # websockets库
                await websocket.send(json.dumps({
                    'type': 'system',
                    'message': '连接成功，开始接收股票异动数据...',
                    'monitoring_active': self.monitoring_active
                }))
            
            # 发送历史数据
            recent_volatiles = await self.get_recent_volatiles(30)
            for volatile in recent_volatiles:
                if hasattr(websocket, 'send_str'):  # aiohttp WebSocket
                    await websocket.send_str(json.dumps(volatile, ensure_ascii=False))
                else:  # websockets库
                    await websocket.send(json.dumps(volatile, ensure_ascii=False))
            
            # 保持连接
            async for message in websocket:
                if hasattr(websocket, 'send_str'):  # aiohttp WebSocket
                    if message.type == web.WSMsgType.TEXT:
                        logger.info(f"📨 收到客户端消息: {message.data}")
                else:  # websockets库
                    logger.info(f"📨 收到客户端消息: {message}")
                    
        except Exception as e:
            logger.error(f"❌ 处理客户端消息错误: {e}")
        finally:
            self.connections.remove(websocket)
            logger.info(f"🔌 客户端断开，当前连接数: {len(self.connections)}")

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

async def handle_plate_data_websocket(request):
    """板块数据WebSocket（专门用于实时更新）"""
    return await service.handle_plate_data_websocket(request)

# 新增高级指标API路由
async def advanced_indicators_stock_api(request):
    """个股高级技术指标API"""
    try:
        service = request.app['service']
        stock_code = request.query.get('code', '')
        
        if not stock_code:
            return web.json_response({'error': '股票代码不能为空'}, status=400)
        
        logger.info(f"📊 请求个股高级指标: {stock_code}")
        
        # 尝试从缓存获取 - 修正阻塞 I/O
        cache_key = f"advanced_indicators_{stock_code}"
        loop = asyncio.get_event_loop()
        cached_data = await loop.run_in_executor(None, service.redis_storage.get_data, cache_key)
        
        if cached_data:
            return web.json_response({
                'data': cached_data,
                'code': stock_code,
                'cached': True,
                'timestamp': int(time.time() * 1000)
            })
        
        # 实时计算
        indicators = service.advanced_indicators.get_stock_advanced_indicators_optimized(stock_code)
        
        return web.json_response({
            'data': indicators,
            'code': stock_code,
            'cached': False,
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ 个股高级指标API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def advanced_indicators_plate_api(request):
    """板块高级技术指标API"""
    try:
        service = request.app['service']
        plate_id = request.query.get('plate_id', '')
        
        if not plate_id:
            return web.json_response({'error': '板块ID不能为空'}, status=400)
        
        logger.info(f"📊 请求板块高级指标: {plate_id}")
        
        # 尝试从缓存获取
        cache_key = f"advanced_indicators_plate_{plate_id}"
        cached_data = service.redis_storage.get_data(cache_key)
        
        if cached_data:
            return web.json_response({
                'data': cached_data,
                'plate_id': plate_id,
                'cached': True,
                'timestamp': int(time.time() * 1000)
            })
        
        # 实时计算
        plate_stocks = service.plate_updater.get_plate_stocks(plate_id)
        stock_codes = [stock.get('code') for stock in plate_stocks if stock.get('code')]
        
        # 批量获取个股指标并聚合
        if stock_codes:
            stock_indicators = service.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_codes)
            
            # 聚合板块指标
            change_rates = [ind.get('change_rate_1min', 0) for ind in stock_indicators.values()]
            amounts_2min = [ind.get('amount_2min', 0) for ind in stock_indicators.values()]
            
            if change_rates:
                indicators = {
                    'avg_change_rate_1min': round(sum(change_rates) / len(change_rates), 4),
                    'total_amount_2min': round(sum(amounts_2min), 2),
                    'stock_count': len(stock_indicators),
                    'update_time': time.time()
                }
            else:
                indicators = {}
        else:
            indicators = {}
        
        return web.json_response({
            'data': indicators,
            'plate_id': plate_id,
            'cached': False,
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ 板块高级指标API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

# 添加批量查询API路由
async def advanced_indicators_batch_stocks_api(request):
    """批量获取个股高级技术指标API"""
    try:
        service = request.app['service']
        
        # 支持GET参数或POST JSON body
        if request.method == 'POST':
            data = await request.json()
            stock_codes = data.get('codes', [])
        else:
            codes_str = request.query.get('codes', '')
            stock_codes = codes_str.split(',') if codes_str else []
        
        if not stock_codes:
            return web.json_response({'error': '股票代码列表不能为空'}, status=400)
        
        logger.info(f"📊 批量请求个股高级指标: {len(stock_codes)} 只股票")
        
        indicators = service.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_codes)
        
        return web.json_response({
            'data': indicators,
            'count': len(indicators),
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ 批量个股高级指标API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def advanced_indicators_batch_plates_api(request):
    """批量获取板块高级技术指标API"""
    try:
        service = request.app['service']
        
        # 支持GET参数或POST JSON body
        if request.method == 'POST':
            data = await request.json()
            plate_ids = data.get('plate_ids', [])
        else:
            plates_str = request.query.get('plate_ids', '')
            plate_ids = plates_str.split(',') if plates_str else []
        
        if not plate_ids:
            return web.json_response({'error': '板块ID列表不能为空'}, status=400)
        
        logger.info(f"📊 批量请求板块高级指标: {len(plate_ids)} 个板块")
        
        indicators = {}
        for plate_id in plate_ids:
            plate_stocks = service.plate_updater.get_plate_stocks(plate_id)
            stock_codes = [stock.get('code') for stock in plate_stocks if stock.get('code')]
            
            if stock_codes:
                stock_indicators = service.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_codes)
                
                # 聚合板块指标
                change_rates = [ind.get('change_rate_1min', 0) for ind in stock_indicators.values()]
                amounts_2min = [ind.get('amount_2min', 0) for ind in stock_indicators.values()]
                
                if change_rates:
                    indicators[plate_id] = {
                        'avg_change_rate_1min': round(sum(change_rates) / len(change_rates), 4),
                        'total_amount_2min': round(sum(amounts_2min), 2),
                        'stock_count': len(stock_indicators),
                        'update_time': time.time()
                    }
        
        return web.json_response({
            'data': indicators,
            'count': len(indicators),
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ 批量板块高级指标API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def plate_api(request):
    """板块数据API"""
    try:
        query_type = request.query.get('type', 'all_plates')
        
        if query_type == 'all_plates':
            data = service.plate_updater.get_all_plate_metrics_with_integrated_advanced()
        elif query_type == 'main_plates':
            all_data = service.plate_updater.get_all_plate_metrics_with_integrated_advanced()
            data = [d for d in all_data if d.get('type') == 'main']
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

# 添加F10数据API路由
async def f10_data_api(request):
    """F10数据API"""
    # try:
    # service = request.app['service']
    stock_code = request.query.get('code', '')
    
    if not stock_code:
        return web.json_response({'error': '股票代码不能为空'}, status=400)
    
    f10_data = service.f10_service.get_stock_f10(stock_code)
    
    if not f10_data:
        return web.json_response({
            'error': f'未找到股票 {stock_code} 的F10数据',
            'code': stock_code
        }, status=404)
    
    return web.json_response({
        'data': f10_data,
        'code': stock_code,
        'timestamp': int(time.time() * 1000)
    })
        
    # except Exception as e:
    #     logger.error(f"❌ F10数据API错误: {e}")
    #     return web.json_response({'error': str(e)}, status=500)

async def f10_search_api(request):
    """F10搜索API"""
    try:
        service = request.app['service']
        keyword = request.query.get('keyword', '')
        limit = int(request.query.get('limit', 50))
        
        if not keyword:
            return web.json_response({'error': '搜索关键词不能为空'}, status=400)
        
        results = service.f10_service.search_stocks(keyword, limit)
        
        return web.json_response({
            'results': results,
            'keyword': keyword,
            'count': len(results),
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ F10搜索API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def f10_cache_stats_api(request):
    """F10缓存统计API"""
    try:
        service = request.app['service']
        stats = service.f10_service.get_cache_stats()
        
        return web.json_response({
            'stats': stats,
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ F10缓存统计API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def health_check(request):
    """健康检查"""
    return web.json_response({
        'status': 'healthy',
        'plate_connections': len(service.plate_connections),
        'plate_data_connections': len(service.plate_data_connections),
        'update_count': service.update_count,
        'stock_count': len(service.plate_updater.stock_to_plates),
        'plate_count': len(service.plate_updater.all_plates),
        'cached_plate_metrics': len(service.cached_plate_metrics)
    })

async def handle_yidong(request):
    """处理异动页面请求"""
    try:
        with open('html/yidong.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='yidong.html not found', status=404)

async def handle_excel(request):
    """处理异动页面请求"""
    try:
        with open('html/excel.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='excel.html not found', status=404)

# 股票订阅WebSocket处理
aiohttp_subscriptions = {}  # {websocket: {'stocks': [], 'last_data': {}}}

async def broadcast_stock_updates():
    """定期广播股票更新数据给所有订阅的客户端"""
    while True:
        try:
            if aiohttp_subscriptions:
                # 获取集成服务实例
                service = OptimizedIntegratedWebService._instance
                
                # 检查服务是否有高级指标功能
                if hasattr(service, 'advanced_indicators'):
                    # 收集所有订阅的股票，避免重复请求
                    all_subscribed_stocks = set()
                    for subscription_info in aiohttp_subscriptions.values():
                        all_subscribed_stocks.update(subscription_info['stocks'])
                    
                    if all_subscribed_stocks:
                        # 批量获取所有订阅股票的高级指标
                        indicators_dict = service.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(all_subscribed_stocks))
                        
                        # 为每个客户端推送其订阅的股票数据
                        disconnected = []
                        for ws, subscription_info in aiohttp_subscriptions.items():
                            try:
                                subscribed_stocks = subscription_info['stocks']
                                if not subscribed_stocks:
                                    continue
                                
                                # 筛选出该客户端订阅的股票数据
                                client_data = {}
                                for stock_code in subscribed_stocks:
                                    if stock_code in indicators_dict:
                                        client_data[stock_code] = indicators_dict[stock_code]
                                
                                # 检查数据是否有变化
                                last_data = subscription_info['last_data']
                                
                                # 计算有变化的股票
                                changed_stocks = {}
                                for stock_code, data in client_data.items():
                                    last = last_data.get(stock_code, {})
                                    # 检查关键指标是否有变化
                                    # 确保所有值都转换为浮点数后再进行比较
                                    current_change_rate_1min = float(data.get('change_5min', data.get('change_rate_1min', 0)))
                                    last_change_rate_1min = float(last.get('change_5min', last.get('change_rate_1min', 0)))
                                    current_amount_2min = float(data.get('amount_5min', data.get('amount_2min', 0)))
                                    last_amount_2min = float(last.get('amount_5min', last.get('amount_2min', 0)))
                                    current_change_pct = float(data.get('change_pct', 0))
                                    last_change_pct = float(last.get('change_pct', 0))
                                    
                                    if (abs(current_change_rate_1min - last_change_rate_1min) > 0.01 or
                                        abs(current_amount_2min - last_amount_2min) > 0.1 or
                                        abs(current_change_pct - last_change_pct) > 0.01):
                                        changed_stocks[stock_code] = data
                                
                                # 如果有变化的数据，才推送更新
                                if changed_stocks:
                                    # 发送增量更新
                                    update_msg = {
                                        'type': 'incremental_update',
                                        'data': changed_stocks,
                                        'timestamp': int(time.time() * 1000)
                                    }
                                    
                                    await ws.send_str(json.dumps(update_msg, ensure_ascii=False))
                                    
                                    # 更新最后数据记录
                                    subscription_info['last_data'].update(changed_stocks)
                                    
                            except Exception as e:
                                logger.error(f"❌ 推送股票更新失败: {e}")
                                disconnected.append(ws)
                        
                        # 清理断开的连接
                        for ws in disconnected:
                            if ws in aiohttp_subscriptions:
                                del aiohttp_subscriptions[ws]
                        
                await asyncio.sleep(3)  # 每3秒检查一次数据更新
            else:
                await asyncio.sleep(10)  # 没有连接时，降低检查频率
        
        except Exception as e:
            logger.error(f"❌ 广播股票更新任务错误: {e}")
            await asyncio.sleep(5)  # 出错后暂停5秒

async def handle_stock_subscription_websocket(request):
    """处理股票订阅WebSocket连接 - 支持订阅特定股票列表"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    # 初始化订阅信息
    subscription_info = {
        'stocks': [],
        'last_data': {}
    }
    aiohttp_subscriptions[ws] = subscription_info
    
    logger.info(f"🔗 Excel页面客户端连接，当前连接数: {len(aiohttp_subscriptions)}")
    
    try:
        # 发送欢迎消息
        await ws.send_str(json.dumps({
            'type': 'system',
            'message': '连接成功，等待股票订阅列表...',
            'timestamp': int(time.time())
        }))
        
        # 处理客户端消息
        async for message in ws:
            if message.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(message.data)
                    
                    if data.get('type') == 'subscribe':
                        logger.info(f"📨 收到Excel页面客户端消息: 处理订阅请求")
                        # 处理订阅请求
                        stock_list = data.get('stocks', [])
                        subscription_info['stocks'] = stock_list
                        
                        # 全量推送订阅股票的数据
                        if stock_list:
                            logger.info(f"📤 全量推送 {len(stock_list)} 只股票数据")
                            
                            try:
                                # 使用全局的service实例获取股票数据
                                global service
                                if service is None:
                                    logger.error("❌ 全局service实例未初始化")
                                    await ws.send_str(json.dumps({
                                        'type': 'error',
                                        'message': '服务未初始化',
                                        'timestamp': int(time.time())
                                    }))
                                    continue
                                
                                # 获取所有订阅股票的高级指标
                                if hasattr(service, 'advanced_indicators'):
                                    indicators_dict = service.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_list)
                                    
                                    logger.info(f"📊 获取到的股票指标数据: {len(indicators_dict)}")
                                    
                                    # 全量推送数据
                                    await ws.send_str(json.dumps({
                                        'type': 'full_update',
                                        'data': indicators_dict,
                                        'timestamp': int(time.time())
                                    }))
                                    
                                    # 更新最后数据记录
                                    subscription_info['last_data'] = indicators_dict
                                else:
                                    logger.error("❌ 高级指标服务不可用")
                                    await ws.send_str(json.dumps({
                                        'type': 'error',
                                        'message': '高级指标服务不可用',
                                        'timestamp': int(time.time())
                                    }))
                            except Exception as e:
                                logger.error(f"❌ 处理订阅请求失败: {e}")
                                await ws.send_str(json.dumps({
                                    'type': 'error',
                                    'message': f'处理订阅请求失败: {str(e)}',
                                    'timestamp': int(time.time())
                                }))
                        else:
                            await ws.send_str(json.dumps({
                                'type': 'system',
                                'message': '未提供股票列表，等待更新...',
                                'timestamp': int(time.time())
                            }))
                    
                    elif data.get('type') == 'ping':
                        # 处理心跳请求
                        await ws.send_str(json.dumps({
                            'type': 'pong',
                            'timestamp': int(time.time())
                        }))
                    
                except json.JSONDecodeError:
                    logger.error(f"❌ 解析客户端消息失败: {message.data}")
                    await ws.send_str(json.dumps({
                        'type': 'error',
                        'message': '消息格式错误',
                        'timestamp': int(time.time())
                    }))
            elif message.type == web.WSMsgType.CLOSE:
                logger.info("🔌 客户端主动关闭连接")
                break
            elif message.type == web.WSMsgType.ERROR:
                logger.error(f"❌ WebSocket连接错误: {ws.exception()}")
                break
    
    finally:
        # 清理连接
        if ws in aiohttp_subscriptions:
            del aiohttp_subscriptions[ws]
        logger.info(f"🔗 Excel页面客户端断开连接，当前连接数: {len(aiohttp_subscriptions)}")
    
    return ws

# 修改后的异动WebSocket处理函数
async def handle_websocket(request):
    """处理WebSocket连接 - 异动股票推送"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    # 调用原来的handler逻辑
    await monitor.handler(ws)
    
    return ws

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

# 调试接口 - 内存占用分析
async def debug_memory_api(request):
    """返回内存中主要数据结构的占用情况"""
    try:
        import sys
        
        def get_size(obj, seen=None):
            """Recursively finds size of objects"""
            size = sys.getsizeof(obj)
            if seen is None:
                seen = set()
            obj_id = id(obj)
            if obj_id in seen:
                return 0
            seen.add(obj_id)
            if isinstance(obj, dict):
                size += sum([get_size(v, seen) for v in obj.values()])
                size += sum([get_size(k, seen) for k in obj.keys()])
            elif hasattr(obj, '__dict__'):
                size += get_size(obj.__dict__, seen)
            elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
                size += sum([get_size(i, seen) for i in obj])
            return size

        memory_stats = {}
        
        # 1. Market Edge Engine Caches
        if hasattr(service, 'market_edge') and service.market_edge:
            me = service.market_edge
            memory_stats['MarketEdgeEngine'] = {
                'stock_state_cache': {'len': len(me.stock_state_cache), 'bytes': get_size(me.stock_state_cache)},
                'auction_profile_cache': {'len': len(me.auction_profile_cache), 'bytes': get_size(me.auction_profile_cache)},
                'plate_weight_cache': {'len': len(me.plate_weight_cache), 'bytes': get_size(me.plate_weight_cache)},
                'return_history': {'len': len(me.return_history), 'bytes': get_size(me.return_history)},
                'log_last_payload': {'len': len(me.log_last_payload), 'bytes': get_size(me.log_last_payload)},
                'code_change_history': {'len': len(me.code_change_history), 'bytes': get_size(me.code_change_history)},
                '_quote_cache': {'len': len(me._quote_cache), 'bytes': get_size(me._quote_cache)},
                'analysis_universe_cache': {'len': len(me.analysis_universe_cache), 'bytes': get_size(me.analysis_universe_cache)},
                'intraday_transition_seen': {'len': len(me.intraday_transition_seen), 'bytes': get_size(me.intraday_transition_seen)},
                'profile_transition_seen': {'len': len(me.profile_transition_seen), 'bytes': get_size(me.profile_transition_seen)},
            }
            
        # 2. Advanced Indicators
        if hasattr(service, 'advanced_indicators') and service.advanced_indicators:
            ai = service.advanced_indicators
            memory_stats['AdvancedIndicators'] = {
                'calculated_indicators': {'len': len(ai.calculated_indicators), 'bytes': get_size(ai.calculated_indicators)}
            }
            
        # 3. F10 Service
        if hasattr(service, 'stock_service') and hasattr(service.stock_service, 'f10_data_service'):
            f10 = service.stock_service.f10_data_service
            memory_stats['F10Service'] = {
                'memory_cache': {'len': len(f10.memory_cache), 'bytes': get_size(f10.memory_cache)},
            }
            
        return web.json_response({
            'status': 'healthy',
            'memory_stats': memory_stats
        })
    except Exception as e:
        logger.error(f"❌ 内存调试接口错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

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

async def debug_kaipan_plate_api(request):
    """调试：开盘啦板块榜与系统画像融合对比"""
    try:
        size = request.query.get('size', '20')
        topn = int(request.query.get('topn', 10))
        today_str = datetime.now().strftime('%Y-%m-%d')

        # 1) 开盘啦原始板块榜
        kp = fetch_kaipan_plate_rank("0", size)
        kp_plates = (kp or {}).get("plates", [])[:topn]

        # 2) 系统板块画像榜
        zkey = f"rank:plate_profile:{today_str}"
        dkey = f"rank:plate_profile:details:{today_str}"
        top_rank = await monitor.redis.zrevrange(zkey, 0, max(0, topn - 1), withscores=True) if monitor and monitor.redis else []

        system_top = []
        for pid, score in top_rank:
            detail_raw = await monitor.redis.hget(dkey, pid) if monitor and monitor.redis else None
            detail = {}
            if detail_raw:
                try:
                    detail = json.loads(detail_raw) if isinstance(detail_raw, str) else detail_raw
                except Exception:
                    detail = {}
            system_top.append({
                "plate_id": pid,
                "plate_name": service.plate_updater.all_plates.get(pid, {}).get("name", pid),
                "process_score": float(score),
                "kaipan_rank": detail.get("kaipan_rank", 0),
                "kaipan_strength": detail.get("kaipan_strength", 0),
                "kaipan_change_pct": detail.get("kaipan_change_pct", 0),
                "kaipan_amount": detail.get("kaipan_amount", 0),
                "kaipan_bonus": detail.get("kaipan_bonus", 0),
            })

        return web.json_response({
            "date": today_str,
            "kaipan_ok": bool((kp or {}).get("ok", False)),
            "kaipan_count": int((kp or {}).get("count", 0)),
            "kaipan_top": kp_plates,
            "system_top": system_top,
            "blend": {
                "enabled": bool(getattr(service, "market_edge", None).enable_kaipan_plate_blend) if getattr(service, "market_edge", None) else None,
                "weight": float(getattr(service, "market_edge", None).kaipan_plate_blend_weight) if getattr(service, "market_edge", None) else None,
            },
            "timestamp": int(time.time() * 1000),
        })
    except Exception as e:
        logger.error(f"❌ 开盘啦调试接口错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

# 新增：个股K线API路由
async def stock_kline_api(request):
    """个股K线数据API"""
    return await service.handle_stock_kline_api(request)
from limit_up_storage import IntegratedStockService, LimitUpDailyUpdater, LimitUpTDEngineStorage

async def main():
    global service, monitor
    # 创建一个TDengineService实例，供所有服务共享
    tdengine_service = TDengineService()
    
    # 将同一个TDengineService实例传递给所有需要使用它的服务
    service = OptimizedIntegratedWebService(tdengine_service=tdengine_service)
    monitor = StockVolatileMonitor()
    # 建立名称映射引用
    if hasattr(service.plate_updater, 'stock_names'):
        monitor.stock_names = service.plate_updater.stock_names
        
    stock_service = IntegratedStockService(web_service=service, tdengine_service=tdengine_service)
    updater = LimitUpDailyUpdater(tdengine_service=tdengine_service)
    
    # 启动时始终同步开盘啦板块映射（使用 to_thread 避免阻塞事件循环）
    logger.info("🚀 启动非阻塞板块同步任务...")
    asyncio.create_task(asyncio.to_thread(updater.sync_kaipanla_plates))
    
    # 检查并更新最新交易日的连板数据（如果缺失，同样使用后台任务）
    if not updater.has_latest_trade_day_data():
        logger.info("📅 检测到数据缺失，启动后台任务更新最新交易日的连板数据")
        asyncio.create_task(asyncio.to_thread(updater.update_latest_trade_day_data))
    
    asyncio.create_task(updater.start_daily_update_scheduler_async())
    try:
        await monitor.connect_redis()
    except Exception as e:
        logger.error(f"❌ 短线精灵Redis连接失败: {e}")

    # Initialize MarketEdgeEngine (live by default, replay only when env is set)
    stock_analyzer_instance = None
    if StockAnalyzer:
        try:
             stock_analyzer_instance = StockAnalyzer()
        except Exception as e:
             logger.error(f"Failed to init StockAnalyzer: {e}")

    theme_ranker = None
    if ThemeRanker and SimpleThemeNormalizer:
        try:
            normalizer = SimpleThemeNormalizer()
            theme_ranker = ThemeRanker(
                theme_normalizer=normalizer,
                stock_analyzer=stock_analyzer_instance,
                plate_updater=service.plate_updater
            )
        except Exception as e:
            logger.error(f"Failed to init ThemeRanker: {e}")

    if not theme_ranker:
        logger.warning("⚠️ ThemeRanker missing - MarketEdgeEngine will start with limited ranking capabilities.")

    try:
        # Create a dedicated calendar instance or reuse one if available
        calendar_service = TradingCalendarService()
        
        market_edge = MarketEdgeEngine(
            redis=monitor.redis,  # Use monitor's redis connection as service.redis might be internal
            redis_storage=service.redis_storage,
            plate_updater=service.plate_updater,
            calendar=calendar_service,
            advanced_indicators=service.advanced_indicators,
            theme_ranker=theme_ranker,
            stock_analyzer=stock_analyzer_instance,
        )
        # 可选回放模式：仅在设置 MARKET_EDGE_REPLAY_DATE 时启用
        replay_date = os.environ.get("MARKET_EDGE_REPLAY_DATE", "").strip()
        if replay_date:
            market_edge.manual_date = replay_date
            logger.info(f"✅ MarketEdgeEngine initialized (Mode: replay, Date: {replay_date})")
        else:
            logger.info("✅ MarketEdgeEngine initialized (Mode: live)")
        service.market_edge = market_edge
        asyncio.create_task(market_edge.run())
    except Exception as e:
        logger.error(f"Failed to start MarketEdgeEngine: {e}")
        traceback.print_exc()
        
    # 启动优化后的服务
    await service.start_optimized_services()
    
    # 启动股票异动监控任务
    asyncio.create_task(monitor.monitor_volatile_stocks())
    
    # 启动WebSocket股票数据持续推送任务
    asyncio.create_task(broadcast_stock_updates())
    
    # 创建HTTP应用
    app = web.Application()
    
    # 添加路由
    app.router.add_get('/', handle_bankuai)
    app.router.add_get('/bankuai', handle_bankuai)
    app.router.add_get('/yidong', handle_yidong)  # 新增：异动页面路由
    app.router.add_get('/excel', handle_excel) 
    app.router.add_get('/ws/plate', handle_plate_websocket)
    app.router.add_get('/ws/plate/data', handle_plate_data_websocket)  # 新增：板块数据WebSocket
    app.router.add_get('/ws/volatile', handle_websocket)  # 股票异动WebSocket路由
    app.router.add_get('/ws', handle_stock_subscription_websocket)  # Excel页面股票订阅WebSocket路由
    app.router.add_get('/api/plate', plate_api)
    app.router.add_get('/api/stock/kline', stock_kline_api)  # 新增K线API
    app.router.add_get('/health', health_check)
    app.router.add_get('/redis-status', redis_status)
    app.router.add_get('/debug/plate-stocks', debug_plate_stocks_api)  # 新增调试接口
    app.router.add_get('/debug/memory', debug_memory_api)  # 新增内存调试接口
    app.router.add_get('/api/debug/kaipan-plate', debug_kaipan_plate_api)  # 开盘啦融合调试
    app.router.add_get('/api/f10/data', f10_data_api)
    app.router.add_get('/api/f10/search', f10_search_api)
    app.router.add_get('/api/f10/cache-stats', f10_cache_stats_api)
    app.router.add_get('/api/advanced/stock', advanced_indicators_stock_api)
    app.router.add_get('/api/advanced/plate', advanced_indicators_plate_api)
    app.router.add_get('/api/advanced/batch/stocks', advanced_indicators_batch_stocks_api)
    app.router.add_get('/api/advanced/batch/plates', advanced_indicators_batch_plates_api)
    
    # 添加路由
    app.router.add_get('/api/limit_up', stock_service.get_limit_up_data_api)
    app.router.add_get('/api/first_limit', stock_service.get_today_first_limit_api)
    app.router.add_get('/api/hot_stocks', stock_service.get_hot_stocks_api)
    app.router.add_get('/api/comprehensive', stock_service.get_comprehensive_view_api)
    app.router.add_get('/api/other_stocks', stock_service.get_other_stocks_api)
    
    # T1进程管理API路由
    app.router.add_get('/api/t1/status', t1_status_api)
    app.router.add_post('/api/t1/start', t1_start_api)
    app.router.add_post('/api/t1/stop', t1_stop_api)
    # 设置服务实例
    app['service'] = service
    
    # 启动服务器
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("🚀 优化版集成Web服务已启动")
    logger.info("🌐 http://localhost:8080/bankuai - 板块监控")
    logger.info("🔌 ws://localhost:8080/ws/plate - 板块WebSocket")
    logger.info("🔌 ws://localhost:8080/ws/plate/data - 板块数据WebSocket（实时更新）")
    logger.info("📊 http://localhost:8080/api/plate - 板块API")
    logger.info("❤️ http://localhost:8080/health - 健康检查")
    logger.info("💾 http://localhost:8080/redis-status - Redis状态")
    logger.info("🐛 http://localhost:8080/debug/plate-stocks?plate_id=801159 - 个股调试")
    
    # 永久运行
    await asyncio.Future()

# T1进程管理类
import subprocess
import psutil
import signal
import os
from datetime import datetime

class T1ProcessManager:
    """exe进程管理器"""
    
    def __init__(self):
        self.t1_process = None
        self.t1_pid = None
        self.t1_exe_path = "/root/work/C/exe"
    def is_t1_running(self) -> dict:
        """检测exe是否正在运行"""
        try:
            # 检查是否有exe进程
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    if proc.info['name'] and 'exe' in proc.info['name'].lower():
                        return {
                            'running': True,
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'start_time': datetime.fromtimestamp(proc.info['create_time']).strftime('%Y-%m-%d %H:%M:%S'),
                            'cmdline': proc.info['cmdline'] if proc.info['cmdline'] else []
                        }
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            return {'running': False, 'message': 'exe未运行'}
            
        except Exception as e:
            logger.error(f"❌ 检测T1进程状态失败: {e}")
            return {'running': False, 'error': str(e)}
    
    async def start_t1(self, mode: str = 'live', replay_date: str = None, replay_time: str = None, replay_speed: float = 1.0) -> dict:
        """启动exe进程 - 异步化优化"""
        try:
            # 检查是否已经在运行
            status = self.is_t1_running()
            if status['running']:
                return {'success': False, 'message': 'exe已经在运行', 'pid': status['pid']}
            
            # 检查exe文件是否存在
            if not os.path.exists(self.t1_exe_path):
                return {'success': False, 'message': f'exe文件不存在: {self.t1_exe_path}'}
            
            # 构建命令行参数
            cmd_args = [self.t1_exe_path]
            
            if mode == 'replay':
                cmd_args.append('--replay')
                if replay_date and replay_time:
                    # 组合日期和时间
                    start_datetime = f"{replay_date} {replay_time}"
                    cmd_args.extend(['--start', start_datetime])
                if replay_speed != 1.0:
                    cmd_args.extend(['--speed', str(replay_speed)])
                
                logger.info(f"🎯 启动回放模式: 日期={replay_date}, 时间={replay_time}, 速度={replay_speed}")
            else:
                cmd_args.append('--live')
                logger.info(f"🚀 启动实盘模式")
            
            # 启动进程（独立进程，避免Python关闭时被终止）
            if os.name == 'nt':  # Windows
                # 使用start命令启动独立进程
                cmd_line = 'start "T1 Process" /B ' + ' '.join(f'"{arg}"' for arg in cmd_args)
                os.system(cmd_line)
                
                # 等待进程启动，然后获取PID
                await asyncio.sleep(2.0)  # 使用异步 sleep，避免阻塞主循环
                status = self.is_t1_running()
                if status['running']:
                    self.t1_pid = status['pid']
                    logger.info(f"🚀 exe已作为独立进程启动 (PID: {self.t1_pid})")
                else:
                    logger.warning("⚠️ 无法检测到exe进程，可能启动失败")
                    return {'success': False, 'message': 'exe启动失败'}
            else:  # Linux/Mac
                self.t1_process = subprocess.Popen(
                    cmd_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True
                )
                self.t1_pid = self.t1_process.pid
            if self.t1_process is not None:
                self.t1_pid = self.t1_process.pid
            
            logger.info(f"🚀 exe已启动 (PID: {self.t1_pid}, 模式: {mode})")
            
            return {
                'success': True, 
                'pid': self.t1_pid, 
                'mode': mode,
                'message': f'exe启动成功 (PID: {self.t1_pid})'
            }
            
        except Exception as e:
            logger.error(f"❌ 启动exe失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def stop_t1(self) -> dict:
        """停止exe进程"""
        try:
            # 只停止当前管理的进程，避免误杀其他exe进程
            if self.t1_pid:
                try:
                    process = psutil.Process(self.t1_pid)
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        process.kill()
                    
                    logger.info(f"⏹️ exe已停止 (PID: {self.t1_pid})")
                    self.t1_process = None
                    self.t1_pid = None
                    return {'success': True, 'message': f'exe已停止 (PID: {self.t1_pid})'}
                except psutil.NoSuchProcess:
                    logger.warning(f"⚠️ 无法找到exe进程 (PID: {self.t1_pid})")
                    self.t1_process = None
                    self.t1_pid = None
                    return {'success': False, 'message': 'exe进程不存在'}
            else:
                return {'success': False, 'message': '没有正在管理的exe进程'}
                
        except Exception as e:
            logger.error(f"❌ 停止exe失败: {e}")
            return {'success': False, 'error': str(e)}

# 全局T1进程管理器实例
t1_manager = T1ProcessManager()

# T1进程状态API
async def t1_status_api(request):
    """获取exe运行状态"""
    try:
        status = t1_manager.is_t1_running()
        return web.json_response(status)
    except Exception as e:
        logger.error(f"❌ T1状态API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

# T1启动API
async def t1_start_api(request):
    """启动exe"""
    try:
        # 获取参数
        data = await request.json() if request.content_type == 'application/json' else {}
        mode = data.get('mode', 'live')
        replay_date = data.get('replay_date')
        replay_time = data.get('replay_time')
        replay_speed = float(data.get('replay_speed', 1.0))
        
        result = await t1_manager.start_t1(mode, replay_date, replay_time, replay_speed)
        return web.json_response(result)
    except Exception as e:
        logger.error(f"❌ T1启动API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

# T1停止API
async def t1_stop_api(request):
    """停止exe"""
    try:
        result = t1_manager.stop_t1()
        return web.json_response(result)
    except Exception as e:
        logger.error(f"❌ T1停止API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ 服务已停止")
