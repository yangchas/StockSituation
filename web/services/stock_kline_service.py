
import logging
import json
import tablib
import pandas as pd
import baostock as bs
import threading
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta

# Adjust imports based on project structure
try:
    from web.redis_storage import RedisStorageManager
    from web.services.trading_calendar_service import TradeCalendar
    from web.services.tdengine_service import TDengineService
except ImportError:
    # Fallback for when running from within the web directory or different context
    import sys
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from web.redis_storage import RedisStorageManager
    from web.services.trading_calendar_service import TradeCalendar
    from web.services.tdengine_service import TDengineService

logger = logging.getLogger(__name__)

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
            cls._instance.latest_dates_map = {} # [V3.1] 聚合水位哨兵映射
             # 新增：交易日历服务
            cls._instance.trading_calendar = TradeCalendar()
            # 初始化 baostock 锁 (防止多线程冲突)
            cls._instance.bs_lock = threading.RLock()
            cls._instance._baostock_logged_in = False
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

    def _resolve_baostock_code(self, code: str) -> str:
        code6 = str(code or "").strip()[-6:]
        if code6.startswith(("000", "001", "002", "003", "300", "301")):
            return f"sz.{code6}"
        if code6.startswith(("600", "601", "603", "605", "688", "689")):
            return f"sh.{code6}"
        return ""

    def _ensure_baostock_login(self, force: bool = False) -> bool:
        with self.bs_lock:
            if self._baostock_logged_in and not force:
                return True
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self._baostock_logged_in = True
                    logger.info("Baostock session ready")
                    return True
                self._baostock_logged_in = False
                logger.warning(f"Baostock login failed: {lg.error_code} - {lg.error_msg}")
            except Exception as e:
                self._baostock_logged_in = False
                logger.warning(f"Baostock login exception: {e}")
            return False

    def _reset_baostock_session(self):
        with self.bs_lock:
            try:
                bs.logout()
            except Exception:
                pass
            self._baostock_logged_in = False
            self._ensure_baostock_login(force=True)

    def _should_retry_baostock_error(self, error_code: str, error_msg: str) -> bool:
        message = f"{error_code} {error_msg}".lower()
        retry_tokens = (
            "10001001",
            "10002007",
            "网络接收错误",
            "接收数据异常",
            "invalid continuation byte",
            "decompress",
            "invalid distance too far back",
            "socket",
            "timed out",
        )
        return any(token.lower() in message for token in retry_tokens)

    def ensure_baostock_session(self) -> bool:
        return self._ensure_baostock_login()

    def reset_baostock_session(self) -> None:
        self._reset_baostock_session()

    def query_all_stock_rows(self, day: str) -> List[List[str]]:
        last_error = ""
        for attempt in range(1, 4):
            try:
                with self.bs_lock:
                    if not self._ensure_baostock_login():
                        last_error = "login_failed"
                        time.sleep(0.4 * attempt)
                        continue
                    rs = bs.query_all_stock(day=day)

                if rs.error_code != '0':
                    last_error = f"{rs.error_code} - {rs.error_msg}"
                    if self._should_retry_baostock_error(rs.error_code, rs.error_msg) and attempt < 3:
                        logger.warning("Baostock query_all_stock retry %s/3 for %s: %s", attempt, day, last_error)
                        self._reset_baostock_session()
                        time.sleep(0.6 * attempt)
                        continue
                    logger.error("Baostock query_all_stock failed | day=%s | error=%s", day, last_error)
                    return []

                rows: List[List[str]] = []
                while rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
                    if row:
                        rows.append(row)
                return rows
            except Exception as e:
                last_error = str(e)
                if self._should_retry_baostock_error("", last_error) and attempt < 3:
                    logger.warning("Baostock query_all_stock exception retry %s/3 for %s: %s", attempt, day, last_error)
                    self._reset_baostock_session()
                    time.sleep(0.6 * attempt)
                    continue
                logger.error("Baostock query_all_stock exception | day=%s | error=%s", day, e)
                return []

        logger.error("Baostock query_all_stock exhausted retries | day=%s | error=%s", day, last_error)
        return []

    def _safe_float(self, value, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _safe_int(self, value, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _parse_baostock_row(self, code: str, row) -> Optional[Dict]:
        if not row or len(row) < 9:
            logger.warning("Skip malformed Baostock row | code=%s | row=%s", code, row)
            return None
        return {
            'time': row[0],
            'open': self._safe_float(row[1]),
            'high': self._safe_float(row[2]),
            'low': self._safe_float(row[3]),
            'close': self._safe_float(row[4]),
            'volume': self._safe_int(row[5]),
            'amount': self._safe_float(row[6]),
            'turnover': self._safe_float(row[7]),
            'pct_chg': self._safe_float(row[8]),
        }
    
    def get_cache_key(self, code: str, frequency: str = "d") -> str:
        """生成标准化缓存键 (V3.0)"""
        return f"kline:{frequency}:{code}"
    
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
            if self.trading_calendar.is_trade_day(date_str):
                found_trading_days += 1
            
            # 防止无限循环
            if (datetime.strptime(end_date, '%Y-%m-%d') - current_date).days > 365 * 2:
                break
        
        return current_date.strftime('%Y-%m-%d')
    
    def fetch_kline_data(self, code: str, frequency: str = "d", 
                        start_date: str = None, end_date: str = None) -> List[Dict]:
        """获取K线数据 - V3.1 集成版：物理锁保护"""
        # 使用锁保护 Baostock 网络请求，防止高并发导致 Socket 阻塞
        with self.bs_lock:
            # 1. 如果没有指定日期，默认对齐今日或昨日
            if not end_date:
                today = datetime.now().strftime('%Y-%m-%d')
                end_date = today if self.trading_calendar.is_trade_day(today) else self.trading_calendar.get_previous_trade_day(today)
            
            # 2. 如果是日线，走高性能三级同步链路
            if frequency == "d":
                return self.ensure_kline_ready(code, end_date, days=300)
                
            # 3. 其他非日线接口
            return self._fetch_from_baostock(code, frequency, start_date, end_date)

    def preload_latest_dates(self):
        """[V3.1 聚合优化] 在全量任务启动前，预加载 TDengine 的聚合水位"""
        logger.info("🔍 [TDengine] 正在执行超级表全量心跳扫描 (GROUP BY symbol)...")
        self.latest_dates_map = self.tdengine.get_all_latest_dates("daily_kline")
        logger.info(f"✅ [TDengine] 心跳扫描完成: 捕获 {len(self.latest_dates_map)} 个进度锚点")

    def ensure_kline_ready(self, code: str, target_date: str, days: int = 300) -> List[Dict]:
        """核心哨兵：确保 L1/L2/L3 全链路对齐 (V3.2 物理对准增强版)"""
        cache_key = self.get_cache_key(code, "d")
        l1_data: List[Dict] = []
        
        # ── Step 1: 尝试获取 L1 (Redis) ──
        try:
            cached = self.redis_storage.get_data(cache_key)
            if cached and isinstance(cached, list) and len(cached) > 0:
                l1_data = cached
        except Exception: pass

        # ── Step 2: 物理水位核对 (L2 - TDengine) ──
        latest_l2 = self.latest_dates_map.get(code)
        if not latest_l2:
            latest_l2 = self.tdengine.get_latest_daily_date(code)
        
        # ── Step 3: [对准决策] 是否需要物理补齐 ──
        if not latest_l2 or latest_l2 < target_date:
            # A) 尝试从 L1 提取数据直接写回 L2 (Fast Write-back)
            if l1_data and l1_data[-1].get('time', '') >= target_date:
                # logger.info(f"🔄 [L1->L2 Writeback] {code} 物理补齐: {target_date}")
                df_l1 = pd.DataFrame(l1_data)
                df_l1['date'] = df_l1['time']
                self.tdengine.save_daily_kline(code, df_l1)
                latest_l2 = target_date
            else:
                # B) 物理缺失或 L1 数据不足，强制从 L3 (Baostock) 抓取
                seg_start = self.trading_calendar.get_next_trade_day(latest_l2) if latest_l2 else self.get_start_date_by_trading_days(target_date, days)
                logger.debug(f"🛠️ [L3-Sync] K线增量补抓 {code}: {seg_start} -> {target_date}")
                new_data = self._fetch_raw_from_baostock(code, "d", seg_start, target_date)
                if new_data:
                    df_new = pd.DataFrame(new_data)
                    df_new['date'] = df_new['time']
                    self.tdengine.save_daily_kline(code, df_new)
                    latest_l2 = target_date

        # ── Step 4: [物理对撞校验] 强制从 L2 读取结果 ──
        # [V3.4] 弃用 L1 回滚，禁止“拿着 Redis 数据冒充同步成功”
        start_date = self.get_start_date_by_trading_days(target_date, days)
        full_data = self.tdengine.get_daily_kline(code, start_date, target_date)
        
        if full_data and len(full_data) > 0:
            last_date = full_data[-1].get('time', '')
            if last_date >= target_date:
                # [V7.6 减重] 物理对齐成功，截断最新 60 天数据刷新 L1 缓存
                redis_data = full_data[-60:]
                self.redis_storage.store_data(cache_key, redis_data, expire_seconds=86400)
                return full_data
        
        return []

        # 2. 检查 Redis 极致缓存 (5分钟内同一个请求直接返回)
        cache_key = self.get_cache_key(code, frequency, start_date, end_date)
        try:
            cached_data = self.redis_storage.get_data(cache_key)
            if cached_data:
                return cached_data if isinstance(cached_data, list) else json.loads(cached_data)
        except: pass

        # 3. 如果是日线，尝试从 TDengine 增量获取
        if frequency == "d":
            return self._fetch_daily_incrementally(code, start_date, end_date, cache_key)
            
        # 4. 其他频率原样处理
        if frequency in ["1", "5"]:
            data_list = self._fetch_minute_from_tdengine(code, frequency, start_date, end_date)
        else:
            data_list = self._fetch_from_baostock(code, frequency, start_date, end_date)
        
        return data_list

    def _fetch_daily_incrementally(self, code: str, start_date: str, end_date: str, cache_key: bytes) -> List[Dict]:
        """日线增量抓取逻辑"""
        try:
            # A) 查询本地最新日期
            latest_local = self.tdengine.get_latest_daily_date(code)
            
            # B) 策略判断
            full_fetch_needed = False
            missing_segments = [] # [(start, end)]
            
            if not latest_local:
                # 全新股票，全量抓取
                full_fetch_needed = True
                missing_segments = [(start_date, end_date)]
            elif latest_local < start_date:
                # 本地数据太旧，且不在请求范围内，全量抓取新范围
                full_fetch_needed = True
                missing_segments = [(start_date, end_date)]
            elif latest_local < end_date:
                # 增量抓取：本地有部分数据，抓取缺失的尾部
                # 检查最新日期之后的下一个交易日
                next_trade_day = self.trading_calendar.get_next_trade_day(latest_local)
                if next_trade_day and next_trade_day <= end_date:
                    missing_segments = [(next_trade_day, end_date)]
            
            # C) 执行抓取并存入 TDengine
            for seg_start, seg_end in missing_segments:
                logger.debug(f"🚀 增量抓取 K 线 {code}: {seg_start} -> {seg_end}")
                new_data = self._fetch_raw_from_baostock(code, "d", seg_start, seg_end)
                if new_data:
                    df_new = pd.DataFrame(new_data)
                    df_new['date'] = df_new['time'] # save_daily_kline 预期 date 字段
                    self.tdengine.save_daily_kline(code, df_new)
            
            # D) 从 TDengine 读取完整范围并返回
            data_list = self.tdengine.get_daily_kline(code, start_date, end_date)
            
            # E) 补充完整性检查：如果读取到的天数显著少于预期交易日，可能中间有断档
            # (这里简单实现：存储到 Redis 以便下次快速读取)
            if data_list:
                self.redis_storage.store_data(cache_key, json.dumps(data_list), expire_seconds=300)
            
            return data_list
        except Exception as e:
            logger.error(f"❌ 增量抓取异常 {code}: {e}")
            # 降级：直接从 baostock 抓
            return self._fetch_from_baostock(code, "d", start_date, end_date)

    def _fetch_raw_from_baostock(self, code: str, frequency: str, start_date: str, end_date: str) -> List[Dict]:
        """仅抓取不缓存不处理，供增量逻辑调用 (V7.9 加强版)"""
        try:
            # 🛡️ 物理层对齐：单例初始化已登录，此处不再执行高频 login() 刷屏
            # 如果怀疑连接已断开，可以通过校验错误码触发重连逻辑，但禁止在循环内强制 login

            # 恢复简单的 sh/sz 逻辑以兼容旧版 Baostock (规避 bj. 报错 10004011)
            full_code = f"sz.{code}" if code[:1] in ["0", "3"] else f"sh.{code}"
            
            rs = bs.query_history_k_data_plus(
                full_code, "date,open,high,low,close,volume,amount,turn,pctChg", 
                start_date=start_date, end_date=end_date,
                frequency=frequency, adjustflag="3"
            )
            
            # [V3.5 自动补盲] 如果返回“未登录”错误，尝试静默修复一次
            if rs.error_code == '10001001':
                logger.warning(f"🔄 Baostock 会话已过期，正在尝试静默重连...")
                bs.login()
                rs = bs.query_history_k_data_plus(full_code, "date,open,high,low,close,volume,amount,turn,pctChg", 
                                                start_date=start_date, end_date=end_date, frequency=frequency, adjustflag="3")

            if rs.error_code != '0':
                logger.error(f"❌ Baostock 查询失败 {code}: {rs.error_code} - {rs.error_msg}")
                return []
                
            data = []
            while rs.next():
                r = rs.get_row_data()
                data.append({
                    'time': r[0],
                    'open': float(r[1] or 0), 'high': float(r[2] or 0),
                    'low': float(r[3] or 0), 'close': float(r[4] or 0),
                    'volume': int(float(r[5] or 0)), 'amount': float(r[6] or 0),
                    'turnover': float(r[7] or 0), 'pct_chg': float(r[8] or 0)
                })
            return data
        except Exception as e:
            logger.error(f"❌ Baostock 抓取异常 {code}: {e}")
            return []

    def _fetch_raw_from_baostock(self, code: str, frequency: str, start_date: str, end_date: str) -> List[Dict]:
        """Retry-safe Baostock fetch with symbol filtering and session reset."""
        full_code = self._resolve_baostock_code(code)
        if not full_code:
            logger.warning(f"Skip unsupported symbol for Baostock daily sync: {code}")
            return []

        fields = "date,open,high,low,close,volume,amount,turn,pctChg"
        last_error = ""
        for attempt in range(1, 4):
            try:
                with self.bs_lock:
                    if not self._ensure_baostock_login():
                        last_error = "login_failed"
                        time.sleep(0.4 * attempt)
                        continue
                    rs = bs.query_history_k_data_plus(
                        full_code,
                        fields,
                        start_date=start_date,
                        end_date=end_date,
                        frequency=frequency,
                        adjustflag="3",
                    )
                if rs.error_code != '0':
                    last_error = f"{rs.error_code} - {rs.error_msg}"
                    if self._should_retry_baostock_error(rs.error_code, rs.error_msg) and attempt < 3:
                        logger.warning(f"Baostock retry {attempt}/3 for {code}: {last_error}")
                        self._reset_baostock_session()
                        time.sleep(0.6 * attempt)
                        continue
                    logger.error(f"❌ Baostock 查询失败 {code}: {last_error}")
                    return []

                data = []
                while rs.next():
                    r = rs.get_row_data()
                    parsed = self._parse_baostock_row(code, r)
                    if parsed is not None:
                        data.append(parsed)
                return data
            except Exception as e:
                last_error = str(e)
                if self._should_retry_baostock_error("", last_error) and attempt < 3:
                    logger.warning(f"Baostock exception retry {attempt}/3 for {code}: {last_error}")
                    self._reset_baostock_session()
                    time.sleep(0.6 * attempt)
                    continue
                logger.error(f"❌ Baostock 查询失败 {code}: {e}")
                return []

        logger.error(f"❌ Baostock exhausted retries {code}: {last_error}")
        return []

    def _fetch_from_baostock(self, code: str, frequency: str, 
                           start_date: str, end_date: str) -> List[Dict]:
        """为了保持向后兼容，保留此方法作为降级或非日线使用"""
        data_list = self._fetch_raw_from_baostock(code, frequency, start_date, end_date)
        if data_list:
            cache_key = self.get_cache_key(code, frequency, start_date, end_date)
            self.redis_storage.store_data(cache_key, json.dumps(data_list), expire_seconds=300)
        return data_list
    
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
