#!/usr/bin/env python3
import asyncio
from tracemalloc import start
import websockets
import json
import pandas as pd
import time
import os
from typing import Dict, Set, List, Optional
import logging
import tablib
from aiohttp import web
from datetime import datetime, timedelta
import baostock as bs
# import taos  # 暂时注释以避免依赖问题
# 修改导入路径
from plate_updater import LazyPlateUpdater, PlateDataSimulator
from redis_storage import RedisStorageManager
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import json
import requests
from datetime import datetime, timedelta
import logging
from typing import List, Set, Dict
import holidays

logger = logging.getLogger(__name__)
class F10DataService:
    """F10数据服务 - 按需加载和缓存"""
    
    def __init__(self, csv_file_path: str = 'data/f10.csv'):
        self.csv_file_path = csv_file_path
        self.data_loaded = False
        self.f10_data = None
        self.index_by_code = {}  # 按股票代码索引
        self.memory_cache = {}   # 内存缓存
        self.redis_storage = RedisStorageManager()  # 复用Redis缓存
        
        # 预加载索引，但不加载全部数据
        self._load_index()
    
    def _load_index(self):
        """加载数据索引，不加载具体数据"""
        # try:
        if not os.path.exists(self.csv_file_path):
            logger.error(f"❌ F10数据文件不存在: {self.csv_file_path}")
            return
        
        # 只读取股票代码列来构建索引
        # 将ANSI改为gbk，因为在Windows系统上ANSI通常指的是系统默认编码（在中国通常是gbk）
        df_codes = pd.read_csv(self.csv_file_path, usecols=['股票代码'],encoding='gbk')
        for idx, row in df_codes.iterrows():
            code = self._normalize_stock_code(row['股票代码'])
            self.index_by_code[code] = idx
        
        logger.info(f"✅ F10数据索引加载完成: {len(self.index_by_code)} 只股票")
            
        # except Exception as e:
            # logger.error(f"❌ 加载F10数据索引失败: {e}")
    
    def _normalize_stock_code(self, code: str) -> str:
        """标准化股票代码格式"""
        if pd.isna(code):
            return ""
        
        code_str = str(code).strip().upper()
        
        # 统一格式: 000001.SZ -> 000001
        if '.' in code_str:
            code_str = code_str.split('.')[0]
        
        return code_str
    
    def _load_full_data_if_needed(self):
        """按需加载完整数据"""
        if not self.data_loaded:
            try:
                # 使用与_load_index方法相同的编码格式gbk
                self.f10_data = pd.read_csv(self.csv_file_path, encoding='gbk')
                self.data_loaded = True
                logger.info("✅ F10完整数据加载完成")
            except Exception as e:
                logger.error(f"❌ 加载F10完整数据失败: {e}")
    
    def get_stock_f10(self, stock_code: str) -> Optional[Dict]:
        """获取单只股票的F10数据"""
        print("获取单只股票的F10数据")
        # try:
        normalized_code = self._normalize_stock_code(stock_code)
        
        if not normalized_code:
            return None
        
        # 检查内存缓存
        cache_key = f"f10_{normalized_code}"
        if cache_key in self.memory_cache:
            logger.debug(f"📦 从内存缓存获取F10数据: {normalized_code}")
            return self.memory_cache[cache_key]
        
        # 检查Redis缓存
        cached_data = self.redis_storage.get_data(cache_key)
        if cached_data:
            logger.debug(f"📦 从Redis缓存获取F10数据: {normalized_code}")
            self.memory_cache[cache_key] = cached_data  # 写入内存缓存
            return cached_data
        
        # 从CSV文件加载
        if normalized_code not in self.index_by_code:
            logger.warning(f"⚠️ 未找到股票的F10数据: {stock_code} -> {normalized_code}")
            return None
        
        # 按需加载完整数据
        self._load_full_data_if_needed()
        if self.f10_data is None:
            return None
        
        row_index = self.index_by_code[normalized_code]
        if row_index >= len(self.f10_data):
            return None
        
        row_data = self.f10_data.iloc[row_index]
        
        # 转换为字典格式
        f10_info = self._format_f10_data(row_data)
        
        # 更新缓存
        self.memory_cache[cache_key] = f10_info
        # Redis缓存1小时
        self.redis_storage.store_data(cache_key, f10_info, expire_seconds=3600)
        
        logger.debug(f"✅ 从CSV文件加载F10数据: {normalized_code}")
        return f10_info
            
        # except Exception as e:
        #     logger.error(f"❌ 获取F10数据失败 {stock_code}: {e}")
        #     return None
    
    def _format_f10_data(self, row_data) -> Dict:
        """格式化F10数据"""
        # 基础信息
        basic_info = {
            'stock_code': str(row_data.get('股票代码', '')),
            'stock_name': str(row_data.get('股票简称', '')),
            'industry': str(row_data.get('所属同花顺行业', '')),
            'city': str(row_data.get('城市', '')),
            'listing_date': str(row_data.get('新股上市日期', ''))
        }
        
        # 财务指标
        financial_info = {
            'total_market_cap': self._safe_float(row_data.get('总市值')),
            'circulating_market_cap': self._safe_float(row_data.get('a股市值(不含限售股)')),
            'total_shares': self._safe_float(row_data.get('总股本')),
            'circulating_shares': self._safe_float(row_data.get('流通a股')),
            'revenue': self._safe_float(row_data.get('营业收入')),
            'net_profit': self._safe_float(row_data.get('归属于母公司所有者的净利润')),
            'roe': self._safe_float(row_data.get('净资产收益率roe(加权,公布值)')),
            'pb': self._safe_float(row_data.get('市净率(pb)')),
            'pe': self._safe_float(row_data.get('市盈率(pe)')),
            'debt_ratio': self._safe_float(row_data.get('资产负债率')),
            'gross_margin': self._safe_float(row_data.get('销售毛利率'))
        }
        
        # 业务信息
        business_info = {
            'main_products': self._parse_main_products(row_data.get('主营产品名称')),
            'product_categories': self._extract_product_categories(row_data.get('主营产品名称'))
        }
        
        return {
            'basic': basic_info,
            'financial': financial_info,
            'business': business_info,
            'timestamp': pd.Timestamp.now().isoformat()
        }
    
    def _safe_float(self, value):
        """安全转换为float"""
        if pd.isna(value) or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_main_products(self, products_str):
        """解析主营产品"""
        if pd.isna(products_str) or not products_str:
            return []
        
        try:
            # 按||分割产品
            products = str(products_str).split('||')
            return [p.strip() for p in products if p.strip()]
        except:
            return []
    
    def _extract_product_categories(self, products_str):
        """提取产品分类"""
        if pd.isna(products_str) or not products_str:
            return []
        
        products = self._parse_main_products(products_str)
        
        # 简单的分类提取（可以根据需要扩展）
        categories = set()
        for product in products:
            if '材料' in product:
                categories.add('材料')
            if '设备' in product or '机器' in product:
                categories.add('设备')
            if '服务' in product:
                categories.add('服务')
            if '技术' in product or '研发' in product:
                categories.add('技术')
            if '贸易' in product:
                categories.add('贸易')
            if '金融' in product:
                categories.add('金融')
        
        return list(categories)
    
    def batch_get_f10(self, stock_codes: list) -> Dict[str, Dict]:
        """批量获取F10数据"""
        results = {}
        
        for code in stock_codes:
            f10_data = self.get_stock_f10(code)
            if f10_data:
                results[code] = f10_data
        
        return results
    
    def search_stocks(self, keyword: str, limit: int = 50) -> list:
        """根据关键词搜索股票"""
        try:
            self._load_full_data_if_needed()
            if self.f10_data is None:
                return []
            
            results = []
            keyword_lower = keyword.lower()
            
            for idx, row in self.f10_data.iterrows():
                # 搜索股票代码、名称、行业、主营产品
                stock_code = str(row.get('股票代码', '')).lower()
                stock_name = str(row.get('股票简称', '')).lower()
                industry = str(row.get('所属同花顺行业', '')).lower()
                products = str(row.get('主营产品名称', '')).lower()
                
                if (keyword_lower in stock_code or 
                    keyword_lower in stock_name or 
                    keyword_lower in industry or 
                    keyword_lower in products):
                    
                    results.append({
                        'code': row.get('股票代码', ''),
                        'name': row.get('股票简称', ''),
                        'industry': row.get('所属同花顺行业', ''),
                        'match_field': self._get_match_field(keyword_lower, stock_code, stock_name, industry, products)
                    })
                
                if len(results) >= limit:
                    break
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 搜索股票失败 {keyword}: {e}")
            return []
    
    def _get_match_field(self, keyword, code, name, industry, products):
        """获取匹配的字段"""
        if keyword in code:
            return 'code'
        elif keyword in name:
            return 'name'
        elif keyword in industry:
            return 'industry'
        elif keyword in products:
            return 'products'
        return 'other'
    
    def clear_cache(self, stock_code: str = None):
        """清理缓存"""
        if stock_code:
            normalized_code = self._normalize_stock_code(stock_code)
            cache_key = f"f10_{normalized_code}"
            self.memory_cache.pop(cache_key, None)
            self.redis_storage.delete_data(cache_key)
            logger.info(f"🧹 清理F10缓存: {normalized_code}")
        else:
            self.memory_cache.clear()
            logger.info("🧹 清理所有F10内存缓存")
    
    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        return {
            'memory_cache_size': len(self.memory_cache),
            'index_size': len(self.index_by_code),
            'data_loaded': self.data_loaded,
            'cached_stocks': list(self.memory_cache.keys())[:10]  # 前10个作为样本
        }
class TradingCalendarService:
    """交易日历服务"""
    
    def __init__(self):
        self.cache = {}
        self.cn_holidays = holidays.CN()  # 中国公共假期
        self._init_trading_calendar()
    
    def _init_trading_calendar(self):
        """初始化交易日历"""
        # 中国股市交易时间：周一至周五 9:30-11:30, 13:00-15:00
        # 不交易的时间：周六、周日、法定节假日
        self.trading_hours = {
            'morning_start': '09:30:00',
            'morning_end': '11:30:00',
            'afternoon_start': '13:00:00',
            'afternoon_end': '15:00:00'
        }
    
    def is_trading_day(self, date_str: str) -> bool:
        """判断是否为交易日"""
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            print(f"判断日期{date_str}是否为交易日")
            # 检查是否是周末
            if date_obj.weekday() >= 5:  # 5=周六, 6=周日
                return False
            
            # 检查是否是法定节假日
            if date_obj in self.cn_holidays:
                return False
            
            # 这里可以添加更多的特殊日期判断
            # 比如调休上班的周末等
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 判断交易日失败 {date_str}: {e}")
            return False
    
    def get_previous_trading_day(self, date_str: str = None) -> str:
        """获取前一个交易日"""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # 向前查找，最多找30天
        for i in range(1, 31):
            prev_date = current_date - timedelta(days=i)
            prev_date_str = prev_date.strftime('%Y-%m-%d')
            
            if self.is_trading_day(prev_date_str):
                return prev_date_str
        
        # 如果没找到，返回30天前的日期
        return (current_date - timedelta(days=30)).strftime('%Y-%m-%d')
    
    def get_next_trading_day(self, date_str: str = None) -> str:
        """获取下一个交易日"""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # 向后查找，最多找30天
        for i in range(1, 31):
            next_date = current_date + timedelta(days=i)
            next_date_str = next_date.strftime('%Y-%m-%d')
            
            if self.is_trading_day(next_date_str):
                return next_date_str
        
        # 如果没找到，返回30天后的日期
        return (current_date + timedelta(days=30)).strftime('%Y-%m-%d')
    
    def get_recent_trading_days(self, days: int = 30) -> List[str]:
        """获取最近N个交易日"""
        end_date = datetime.now()
        trading_days = []
        
        current_date = end_date
        while len(trading_days) < days:
            date_str = current_date.strftime('%Y-%m-%d')
            if self.is_trading_day(date_str):
                print("交易日：",date_str)
                trading_days.append(date_str)
            current_date -= timedelta(days=1)
            
            # 防止无限循环
            if (end_date - current_date).days > 365:
                break
        
        return sorted(trading_days)
    
    def is_trading_time(self, datetime_str: str = None) -> bool:
        """判断当前是否在交易时间内"""
        if not datetime_str:
            datetime_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            dt_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            date_str = dt_obj.strftime('%Y-%m-%d')
            time_str = dt_obj.strftime('%H:%M:%S')
            
            # 首先检查是否是交易日
            if not self.is_trading_day(date_str):
                return False
            
            # 检查是否在交易时间段内
            morning_session = (time_str >= self.trading_hours['morning_start'] and 
                             time_str <= self.trading_hours['morning_end'])
            afternoon_session = (time_str >= self.trading_hours['afternoon_start'] and 
                               time_str <= self.trading_hours['afternoon_end'])
            
            return morning_session or afternoon_session
            
        except Exception as e:
            logger.error(f"❌ 判断交易时间失败 {datetime_str}: {e}")
            return False
    
    def get_today_trading_status(self) -> Dict:
        """获取今日交易状态"""
        today = datetime.now().strftime('%Y-%m-%d')
        current_time = datetime.now().strftime('%H:%M:%S')
        
        is_trading_day = self.is_trading_day(today)
        is_trading_time = self.is_trading_time()
        
        status = "非交易日"
        if is_trading_day:
            if is_trading_time:
                status = "交易中"
            else:
                if current_time < self.trading_hours['morning_start']:
                    status = "开盘前"
                elif current_time > self.trading_hours['afternoon_end']:
                    status = "已收盘"
                else:
                    status = "午间休市"
        
        return {
            'date': today,
            'is_trading_day': is_trading_day,
            'is_trading_time': is_trading_time,
            'status': status,
            'trading_hours': self.trading_hours,
            'current_time': current_time
        }
class TDengineService:
    """TDengine数据库服务 - 基于官方示例的连接方式"""
    
    def __init__(self, host: str = '127.0.0.1', port: int = 6030, 
                 user: str = 'root', password: str = 'taosdata', 
                 database: str = 'market_data1', config: str = '/etc/taos', 
                 timezone: str = 'Asia/Shanghai'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.config = config
        self.timezone = timezone
        self.conn = None
        self.cursor = None
        self._connect()
    
    def _connect(self):
        """连接TDengine数据库 - 使用官方示例的连接方式"""
        try:
            self.conn = taos.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                config=self.config,
                timezone=self.timezone
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ TDengine连接成功")
        except Exception as e:
            logger.error(f"❌ TDengine连接失败: {e}")
            self.conn = None
            self.cursor = None
    
    def execute_query(self, sql: str):
        """执行SQL查询 - 使用cursor方式"""
        if not self.conn:
            self._connect()
            if not self.conn:
                return None
        
        try:
            self.cursor.execute(sql)
            return self.cursor
        except Exception as e:
            logger.error(f"❌ TDengine查询失败: {e}, SQL: {sql}")
            # 尝试重新连接
            try:
                self._connect()
                if self.conn:
                    self.cursor.execute(sql)
                    return self.cursor
            except Exception as reconnect_error:
                logger.error(f"❌ TDengine重连失败: {reconnect_error}")
            return None
    
    def get_minute_kline(self, symbol: str, start_time: str = None, end_time: str = None, 
                        days: int = 1) -> List[Dict]:
        """
        从TDengine获取分钟K线数据 - 正确处理累计值volume和amount
        
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
            
            # 构建SQL查询 - 按1分钟聚合tick数据，正确处理累计值
            sql = f"""
            SELECT 
                _wstart as time,
                FIRST(lp) as open,
                MAX(lp) as high,
                MIN(lp) as low,
                LAST(lp) as close,
                LAST(v) - FIRST(v) as volume,  -- 处理累计值：用最后一个值减去第一个值
                LAST(a) - FIRST(a) as amount   -- 处理累计值：用最后一个值减去第一个值
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time}' 
                AND ts <= '{end_time}'
            INTERVAL(1m)
            ORDER BY time
            """
            
            logger.info(f"📊 查询TDengine分钟数据: {symbol}")
            
            cursor = self.execute_query(sql)
            if not cursor:
                return []
            
            # 处理查询结果 - 使用cursor.fetchall()方式，并确保浮点数保留两位小数
            data = []
            rows = cursor.fetchall()
            for row in rows:
                # row是一个元组，按SELECT字段顺序
                data.append({
                    'time': row[0].strftime('%Y-%m-%d %H:%M:%S'),
                    'open': round(float(row[1]) if row[1] is not None else 0, 2),
                    'high': round(float(row[2]) if row[2] is not None else 0, 2),
                    'low': round(float(row[3]) if row[3] is not None else 0, 2),
                    'close': round(float(row[4]) if row[4] is not None else 0, 2),
                    'volume': int(row[5]) if row[5] is not None else 0,
                    'amount': float(row[6]) if row[6] is not None else 0
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
            
            cursor = self.execute_query(sql)
            if not cursor:
                return []
            
            # 获取字段信息
            fields = [field[0] for field in cursor.description]
            
            data = []
            rows = cursor.fetchall()
            for row in rows:
                row_dict = {}
                for i, field_name in enumerate(fields):
                    value = row[i]
                    if isinstance(value, datetime):
                        row_dict[field_name] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
                    elif value is None:
                        row_dict[field_name] = None
                    else:
                        row_dict[field_name] = value
                data.append(row_dict)
            
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
         # 新增：交易日历服务
        self.trading_calendar = TradingCalendarService()
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
        
        # 智能设置日期范围，考虑交易日
        if not end_date:
            # 如果是非交易日，使用最后一个交易日作为结束日期
            today = datetime.now().strftime('%Y-%m-%d')
            if not self.trading_calendar.is_trading_day(today):
                end_date = self.trading_calendar.get_previous_trading_day(today)
                logger.info(f"📅 今日({today})非交易日，使用最近交易日: {end_date}")
            else:
                end_date = today
        if not start_date or start_date>end_date:
            if frequency in ["1","5"]:
                trading_days = self.trading_calendar.get_recent_trading_days(2)
                if trading_days:
                    start_date = trading_days[0]  # 最早的交易日
                else:
                    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
            # elif frequency == "5":
            #     start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
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
                lg = bs.login()
                if lg.error_code == '0':
                    logger.info("✅ Baostock登录成功")
                else:
                    logger.error(f"❌ Baostock登录失败: {lg.error_msg}")
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
        # 新增：F10数据服务
        self.f10_service = F10DataService('data/f10.csv')
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
                    
                    # 广播给所有客户端 - 创建副本避免在迭代时修改集合
                    await self.broadcast_to_connections(update_msg, set(self.plate_connections))
                    
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
                        
                        # 广播给订阅该板块的所有客户端 - 创建副本避免在迭代时修改集合
                        await self.broadcast_to_connections(update_msg, set(connections))
                    
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
            valid_frequencies = ['1','5', '60', 'd', 'w']
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
    app.router.add_get('/api/f10/data', f10_data_api)
    app.router.add_get('/api/f10/search', f10_search_api)
    app.router.add_get('/api/f10/cache-stats', f10_cache_stats_api)
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