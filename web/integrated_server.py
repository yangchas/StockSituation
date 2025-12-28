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

# 初始化日志记录器
logger = logging.getLogger(__name__)
from aiohttp import web
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
import json
import requests
from datetime import datetime, timedelta
import logging
from typing import List, Set, Dict
import holidays
from redis_storage import RedisStorageManager
from limit_up_storage import ZTBService
from trade_calendar import TradeCalendar

logger = logging.getLogger(__name__)
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

class OptimizedAdvancedTechnicalIndicators:
    """优化版高级技术指标服务 - 减少重复字段，降低计算频率"""
    
    def __init__(self, tdengine_service, redis_storage):
        self.tdengine = tdengine_service
        self.redis_storage = redis_storage
        self.calculated_indicators = {}  # 缓存已计算的指标
        self.last_calculation_time = {}  # 记录上次计算时间
        
    def get_stock_advanced_indicators_optimized(self, symbol: str, force_recalc: bool = False) -> Dict:
        """获取个股的高级技术指标 - 优化版本"""
        try:
            # 检查缓存和是否需要重新计算
            cache_key = f"advanced_indicators_{symbol}"
            current_time = time.time()
            
            # 如果不在强制重算且缓存有效（5秒内），直接返回缓存
            if not force_recalc and symbol in self.calculated_indicators:
                last_time = self.last_calculation_time.get(symbol, 0)
                if current_time - last_time < 5:  # 5秒缓存
                    return self.calculated_indicators[symbol]
            
            # 从Redis获取基础数据
            stock_data = self.redis_storage.get_stock_data(symbol)
            if not stock_data:
                return {}
            
            # 只计算必要的核心指标，避免重复字段
            indicators = {
                # 基础字段（直接从Redis获取，确保转换为数字类型）
                'price': float(stock_data.get('price', 0)),
                'change_pct': float(stock_data.get('change_pct', 0)),
                'volume': float(stock_data.get('volume', 0)),
                'amount': float(stock_data.get('amount', 0)),
                
                # 核心高级指标（避免重复计算）
                'change_rate_1min': self._calculate_change_rate_1min(symbol),
                'amount_2min': self._calculate_amount_2min(symbol),
                
                # 从Redis直接获取的大单净额（确保转换为数字类型）
                'large_net': float(stock_data.get('large_net', 0)),
                
                # 元数据
                'timestamp': current_time,
                'update_count': 1  # 用于跟踪更新频率
            }
            
            # 更新缓存
            self.calculated_indicators[symbol] = indicators
            self.last_calculation_time[symbol] = current_time
            
            # 存储到Redis（短期缓存）
            self.redis_storage.store_data(
                cache_key, indicators, expire_seconds=10
            )
            
            return indicators
            
        except Exception as e:
            logger.error(f"❌ 获取个股高级指标失败 {symbol}: {e}")
            return {}
    
    def _calculate_change_rate_1min(self, symbol: str) -> float:
        """计算1分钟涨速 - 优化版"""
        try:
            # 从TDengine获取最近2分钟的收盘价，按时间倒序排列
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=2)
            
            # 获取最近的两个价格点
            sql = f"""
            SELECT lp as price
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
                AND ts <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY ts DESC
            LIMIT 2
            """
            
            cursor = self.tdengine.execute_query(sql)
            if not cursor:
                return 0.0
            
            rows = cursor.fetchall()
            if not rows or len(rows) < 2:
                return 0.0
            
            # 计算涨速
            if rows[0][0] and rows[1][0] and rows[1][0] > 0:
                return ((rows[0][0] - rows[1][0]) / rows[1][0]) * 100
            
            return 0.0
            
        except Exception:
            return 0.0
    
    def _calculate_amount_2min(self, symbol: str) -> float:
        """计算2分钟成交额 - 优化版"""
        try:
            # 直接从Redis获取最近的数据，避免频繁查询数据库
            cache_key = f"amount_2min_{symbol}"
            cached = self.redis_storage.get_data(cache_key)
            
            if cached:
                return float(cached)
            
            # 必要时从TDengine计算
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=2)
            
            sql = f"""
            SELECT SUM(a) as total_amount
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
                AND ts <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            
            cursor = self.tdengine.execute_query(sql)
            if not cursor:
                return 0.0
            
            rows = cursor.fetchall()
            if rows and rows[0][0]:
                amount = float(rows[0][0])
                # 缓存结果
                self.redis_storage.store_data(cache_key, amount, expire_seconds=60)
                return amount
            
            return 0.0
            
        except Exception:
            return 0.0
    
    def batch_get_stocks_advanced_indicators_optimized(self, symbols: List[str]) -> Dict[str, Dict]:
        """批量获取个股高级指标 - 优化版本"""
        try:
            results = {}
            
            # 批量从Redis获取基础数据
            pipeline = self.redis_storage.redis.pipeline()
            for symbol in symbols:
                pipeline.hgetall(f"stock:quote:{symbol}")
            redis_results = pipeline.execute()
            
            # 批量计算必要的高级指标
            for i, symbol in enumerate(symbols):
                stock_data = redis_results[i]
                if not stock_data:
                    continue
                
                # 转换数据类型，使用errors='replace'处理无法解码的字符
                decoded_data = {}
                for field, value in stock_data.items():
                    field_str = field.decode('utf-8', errors='replace') if isinstance(field, bytes) else field
                    value_str = value.decode('utf-8', errors='replace') if isinstance(value, bytes) else str(value)
                    
                    if field_str in ['price', 'change_pct', 'change_rate_1min']:
                        decoded_data[field_str] = float(value_str) if value_str else 0.0
                    elif field_str in ['volume', 'amount', 'large_net', 'timestamp']:
                        decoded_data[field_str] = int(value_str) if value_str and '.' not in value_str else float(value_str) if value_str else 0
                    else:
                        decoded_data[field_str] = value_str
                
                # 构建结果（只包含核心字段）
                results[symbol] = {
                    'price': decoded_data.get('price', 0),
                    'change_pct': decoded_data.get('change_pct', 0),
                    'volume': decoded_data.get('volume', 0),
                    'change_rate_1min': decoded_data.get('change_rate_1min', 0),
                    'amount_2min': decoded_data.get('amount_2min', decoded_data.get('amount', 0)),
                    'large_net': decoded_data.get('large_net', 0),
                    'timestamp': int(time.time())
                }
            
            return results
            
        except Exception as e:
            logger.error(f"❌ 批量获取个股高级指标失败: {e}")
            return {}

class EnhancedPlateUpdater(OptimizedPlateUpdater):
    """增强的板块更新器 - 集成高级技术指标"""
    
    def __init__(self, plate_csv_path: str, stock_plate_csv_path: str, advanced_indicators_service: OptimizedAdvancedTechnicalIndicators):
        super().__init__(plate_csv_path, stock_plate_csv_path)
        self.advanced_indicators = advanced_indicators_service
    
    def get_all_plate_metrics_with_advanced(self) -> List[Dict]:
        """获取所有板块指标（包含高级指标）- 使用优化版本"""
        return self.get_all_plate_metrics_optimized()
    
    def get_main_plates_metrics_with_advanced(self) -> List[Dict]:
        """获取主板块指标（包含高级指标）- 使用优化版本"""
        all_metrics = self.get_all_plate_metrics_optimized()
        main_metrics = [m for m in all_metrics if m.get('type') == 'main']
        return main_metrics
    
    def get_plate_stocks(self, plate_id: str) -> List[Dict]:
        """获取板块个股数据 - 使用优化版本"""
        return self.get_plate_stocks_optimized(plate_id)
        

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
                # 与_load_index方法保持一致，使用utf-8编码
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
    _instance = None
    
    def __new__(cls, host: str = '127.0.0.1', port: int = 6030, 
                 user: str = 'root', password: str = 'taosdata', 
                 database: str = 'market_data1', config: str = '/etc/taos', 
                 timezone: str = 'Asia/Shanghai'):
        # 修复单例模式：只创建一个实例，忽略后续参数差异
        if cls._instance is None:
            cls._instance = super(TDengineService, cls).__new__(cls)
            # 初始化参数
            cls._instance.host = host
            cls._instance.port = port
            cls._instance.user = user
            cls._instance.password = password
            cls._instance.database = database
            cls._instance.config = config
            cls._instance.timezone = timezone
            cls._instance.conn = None
            cls._instance.cursor = None
            # 建立连接
            cls._instance._connect()
        return cls._instance
    
    def __init__(self, host: str = '127.0.0.1', port: int = 6030, 
                 user: str = 'root', password: str = 'taosdata', 
                 database: str = 'market_data1', config: str = '/etc/taos', 
                 timezone: str = 'Asia/Shanghai'):
        # 单例模式下，__init__可能会被多次调用，所以只在__new__中初始化
        pass
    
    def _connect(self):
        """连接TDengine数据库 - 使用官方示例的连接方式"""
        try:
            if taos is not None:
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
            else:
                # 模拟连接成功
                self.conn = "mock_conn"
                self.cursor = "mock_cursor"
                logger.info("✅ TDengine连接成功 (模拟)")
        except Exception as e:
            logger.error(f"❌ TDengine连接失败: {e}")
            self.conn = None
            self.cursor = None
    
    def execute_query(self, sql: str):
        """执行SQL查询 - 使用cursor方式"""
        if not self.conn:
            # 连接不存在，重新连接
            self._connect()
            if not self.conn:
                return None
        
        try:
            if taos is not None:
                self.cursor.execute(sql)
                return self.cursor
            else:
                # 模拟查询结果
                logger.info(f"✅ 模拟执行SQL查询: {sql}")
                return "mock_cursor"
        except Exception as e:
            logger.error(f"❌ TDengine查询失败: {e}, SQL: {sql}")
            # 尝试重新连接
            try:
                self._connect()
                if self.conn:
                    if taos is not None:
                        self.cursor.execute(sql)
                        return self.cursor
                    else:
                        logger.info(f"✅ 模拟执行SQL查询(重连后): {sql}")
                        return "mock_cursor"
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
            
            # 缓存数据（5分钟缓存）- 使用现有的RedisStorageManager
            try:
                cache_time = 300  # 5分钟
                # 生成缓存键
                cache_key = self.get_cache_key(code, frequency, start_date, end_date)
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
    
    async def refresh_plate_data_optimized(self):
        """优化版板块数据刷新"""
        while True:
            try:
                # 使用整合计算获取板块数据（包含高级指标）
                plate_metrics = self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
                
                # 缓存到内存供快速访问
                self.cached_plate_metrics = plate_metrics
                
                # 每5秒刷新一次
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ 刷新板块数据失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_plate_updates_optimized(self):
        """优化版广播板块更新"""
        while True:
            try:
                if self.plate_connections:
                    # 使用缓存数据或实时获取
                    all_metrics = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
                    
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
                
                await asyncio.sleep(3)  # 3秒广播一次
                
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
                    all_plates = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
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
                        # 使用优化后的批量获取方法
                        indicators_dict = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(active_stocks)
                        all_indicators_dict = indicators_dict.copy()
                        
                        # 按板块分组广播
                        for plate_id, connections in self.stock_connections.items():
                            if connections and indicators_dict:
                                # 构建优化后的消息
                                update_msg = self._build_optimized_stock_update(plate_id, indicators_dict)
                                await self.broadcast_to_connections(update_msg, set(connections))
                    
                    # 每3秒记录一次日志
                    if int(time.time()) % 5 == 0:
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
                        subscribed_indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(all_subscribed_stocks)
                        all_indicators_dict.update(subscribed_indicators)
                        
                        # 向订阅客户端推送更新
                        await self.broadcast_stock_updates_to_subscribers(subscribed_indicators)
                
                await asyncio.sleep(3)  # 3秒更新一次
                
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
        """构建优化后的个股更新消息"""
        # 获取该板块的股票
        stock_ids = self.plate_updater.plate_to_stocks.get(plate_id, [])
        
        stocks_data = []
        for stock_id in stock_ids:
            if stock_id in indicators_dict:
                indicators = indicators_dict[stock_id]
                
                # 获取股票基础信息
                stock_data = self.redis_storage.get_stock_data(stock_id) or {}
                
                # 构建完整的股票数据
                stock_info = {
                    'code': stock_id,
                    'name': stock_data.get('name', f"股票{stock_id}"),
                    'change_pct': indicators.get('change_pct', 0),
                    'price': indicators.get('price', 0),
                    'volume': indicators.get('volume', 0),
                    'market_cap': stock_data.get('market_cap', 0),
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
class StockVolatileMonitor:
    def __init__(self):
        self.redis = None
        self.connections: Set = set()  # 移除类型注解以兼容两种WebSocket类型
        self.volatile_pool_key = "stock:volatile_pool"  # 修正键名，从真正的异动池获取数据
        self.first_limit_key = "stock:first_limit_up"  # 涨停票存储键名
        self.last_check_timestamp = 0
        self.monitoring_active = False
        
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
                    
                    for data_str, score in new_data:
                        try:
                            if isinstance(data_str, bytes):
                                data_str = data_str.decode('utf-8', errors='ignore')
                            
                            data = json.loads(data_str)
                            # print(data)
                            data['timestamp'] = int(score)
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
                            
                            # 根据股票前缀判断涨停阈值
                            symbol = data.get('symbol', '')
                            limit_up_threshold = 9.8 if symbol.startswith(('6', '0')) else 19.8
                            
                            # 如果是涨停票，标记为first_limit类型并广播，同时写入Redis
                            if change >= limit_up_threshold:
                                first_limit_data = data.copy()
                                first_limit_data['type'] = 'first_limit'
                                first_limit_data['change_pct'] = change  # 确保change_pct字段存在
                                
                                # 广播涨停警报
                                await self.broadcast_first_limit_alert(first_limit_data)
                                
                                # 将涨停票写入Redis的stock:first_limit_up键中
                                try:
                                    # 将股票代码作为member，时间戳作为score写入zset
                                    await self.redis.zadd(self.first_limit_key, {
                                        json.dumps(first_limit_data): int(time.time())
                                    })
                                    
                                    # 设置过期时间为24小时
                                    await self.redis.expire(self.first_limit_key, 24 * 60 * 60)
                                    
                                    logger.info(f"✅ 成功写入Redis涨停票数据: {first_limit_data.get('symbol')} ({change}%)")
                                    
                                except Exception as e:
                                    logger.error(f"❌ 写入Redis涨停票数据失败: {e}")
                            
                            # 更新最后检查时间戳
                            if int(score) > self.last_check_timestamp:
                                self.last_check_timestamp = int(score)
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ JSON解析错误: {e}, 数据: {data_str[:100]}...")
                        except Exception as e:
                            logger.error(f"❌ 处理数据错误: {e}")
                    
                    logger.info(f"⏰ 最后检查时间戳更新为: {self.last_check_timestamp}")
                    await asyncio.sleep(0.5)
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
        
        logger.info(f"📢 广播: {alert_message['symbol']} - {alert_message['action_text']}")
        
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
            # 如果已经是数字，转换为百分比（乘以100）
            change_pct = float(change) * 100 if isinstance(change, (int, float)) else 0
        
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
        name = data.get('name', '')
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
        
        # 尝试从缓存获取
        cache_key = f"advanced_indicators_{stock_code}"
        cached_data = service.redis_storage.get_data(cache_key)
        
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
from limit_up_storage import IntegratedStockService, LimitUpDailyUpdater, LimitUpTDEngineStorage

async def main():
    global service, monitor
    # 创建一个TDengineService实例，供所有服务共享
    tdengine_service = TDengineService()
    
    # 将同一个TDengineService实例传递给所有需要使用它的服务
    service = OptimizedIntegratedWebService(tdengine_service=tdengine_service)
    monitor = StockVolatileMonitor()
    stock_service = IntegratedStockService(web_service=service, tdengine_service=tdengine_service)
    updater = LimitUpDailyUpdater(tdengine_service=tdengine_service)
    # 检查是否需要立即更新上一个交易日的连板数据
    if not updater.has_previous_trade_day_data():
        logger.info("📅 开始立即更新上一个交易日的连板数据")
        updater.update_previous_trade_day()
    
    asyncio.create_task(updater.start_daily_update_scheduler_async())
    try:
        await monitor.connect_redis()
    except Exception as e:
        logger.error(f"❌ 短线精灵Redis连接失败: {e}")
        
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
    
    def start_t1(self, mode: str = 'live', replay_date: str = None, replay_time: str = None, replay_speed: float = 1.0) -> dict:
        """启动exe进程"""
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
                import time
                time.sleep(2)
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
        
        result = t1_manager.start_t1(mode, replay_date, replay_time, replay_speed)
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