"""
筹码峰 + 近5日涨幅 + 换手率活跃度 + 真实市值 盘后批量计算引擎
每日盘后执行一次，结果写入 Redis cache:chip_peaks:{date} 和 cache:stock_extra:{date}

依赖: baostock (K线), f10.csv (市值), Redis
"""
import os
import sys
import time
import json
import logging
import gc
import threading
import numpy as np
import pandas as pd
import talib
from typing import List, Dict, Optional
from datetime import datetime, timedelta

# 确保项目根目录在 sys.path
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from web.trade_calendar import TradeCalendar

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ChipBatchRunner')

class ChipBatchRunner:
    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, kline_service=None):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super(ChipBatchRunner, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, kline_service=None):
        if getattr(self, "_initialized", False):
            if kline_service is not None and getattr(self, "kline_service", None) is None:
                self.kline_service = kline_service
            return
        import redis as redis_lib
        from web.services.f10_service import F10DataService
        from web.services.stock_kline_service import StockKLineService
        self.redis = redis_lib.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.calendar = TradeCalendar()
        self.f10_service = F10DataService()
        self.kline_service = kline_service if kline_service else StockKLineService()
        self.f10_market_cap: Dict[str, float] = {}
        self.bs_error_count: int = 0
        self.load_f10_market_cap()
        self._initialized = True

    def load_f10_market_cap(self):
        """加载全量市值数据用于计算真实市值"""
        try:
            stocks = list(self.f10_service.index_by_code.keys())
            for code in stocks:
                f10 = self.f10_service.get_stock_f10(code)
                if f10 and 'financial' in f10:
                    cap = f10['financial'].get('circulating_market_cap')
                    if cap:
                        # 统一为亿元
                        self.f10_market_cap[code] = round(cap / 100000000, 2)
            logger.info(f"✅ 已加载 {len(self.f10_market_cap)} 只股票的流通市值数据")
        except Exception as e:
            logger.error(f"❌ 加载市值数据失败: {e}")

    def calculate_chip_peak(self, df: pd.DataFrame) -> Dict:
        """计算筹码峰位置与集中度 (简化算法)"""
        try:
            if df is None or len(df) < 5:
                return {}
            
            # 使用最近120个交易日的数据 (约半年)
            data = df.tail(120).copy()
            prices = data['close'].values
            turns = data['turn'].values / 100.0 # 转换为小数
            
            # 价格区间划分
            p_min, p_max = prices.min(), prices.max()
            if p_max == p_min: return {"peak_price": p_min, "concentration": 0, "dense_area_count": 1}
            
            bins = 50
            bin_edges = np.linspace(p_min, p_max, bins + 1)
            chips = np.zeros(bins)
            
            # 模拟筹码衰减累加 (Decay model)
            # 每日成交量(turn)覆盖旧筹码
            current_chips = np.zeros(bins)
            for i in range(len(prices)):
                p = prices[i]
                t = min(turns[i], 1.0)
                
                # 旧筹码衰减
                current_chips *= (1.0 - t)
                
                # 新筹码加入
                idx = min(int((p - p_min) / (p_max - p_min) * bins), bins - 1)
                current_chips[idx] += t
            
            peak_idx = np.argmax(current_chips)
            peak_price = round((bin_edges[peak_idx] + bin_edges[peak_idx+1]) / 2, 2)
            
            # 集中度 (前 70% 筹码占据的价格区间比例)
            total = current_chips.sum()
            if total > 0:
                sorted_chips = np.sort(current_chips)[::-1]
                cum_chips = np.cumsum(sorted_chips)
                cutoff_idx = np.searchsorted(cum_chips, total * 0.7)
                concentration = round((cutoff_idx + 1) / bins, 4)
            else:
                concentration = 1.0
                
            return {
                "peak_price": peak_price,
                "concentration": concentration,
                "dense_area_count": int(np.sum(current_chips > total * 0.05))
            }
        except Exception as e:
            logger.error(f"计算筹码峰异常: {e}")
            return {}

    def calculate_extra_factors(self, code: str, df: pd.DataFrame) -> Dict:
        """🚀 [V39.2] 增强版多因子精算 (MACD, KDJ, BOLL, MA)"""
        try:
            # 增加稳定性校验：指标计算至少需要 35-40 天数据（尤其是 MACD/MA20）
            if df is None or len(df) < 35:
                return {}
                
            last_idx = len(df) - 1
            curr_close = df.iloc[last_idx]['close']
            
            # 5日涨幅
            idx_5d = max(0, last_idx - 5)
            close_5d = df.iloc[idx_5d]['close']
            change_5d = round((curr_close - close_5d) / close_5d * 100, 2)
            
            # 5日均换手
            avg_turn = round(df.tail(5)['turn'].mean(), 2)
            
            # 5日内涨停天数
            limit_ups = 0
            if 'pct_chg' in df.columns:
                limit_ups = int((df.tail(5)['pct_chg'] > 9.8).sum())
            
            # --- [V39.2] 经典技术指标增强 ---
            rsi_6, bias_20 = 0, 0
            ma5, ma10, ma20 = 0, 0, 0
            macd_dif, macd_dea, macd_hist = 0, 0, 0
            kdj_k, kdj_d, kdj_j = 0, 0, 0
            boll_up, boll_mid, boll_low = 0, 0, 0
            
            # --- [V39.5] 历史身位溯源 (用于反包/龙回头判定) ---
            t2_lb_days = 0
            t2_pct = 0.0
            if len(df) >= 3:
                # T-0 为最后一行，T-1 为倒数第二行，T-2 为倒数第三行
                t2_row = df.iloc[len(df)-3]
                t2_pct = round(t2_row['pct_chg'], 2) if 'pct_chg' in t2_row else 0.0
                # 简单计算 T-2 时刻的连板数 (此处为近似值)
                t2_history = df.iloc[:len(df)-2] # 截止到 T-2 的所有数据
                lb_count = 0
                for i in range(len(t2_history)-1, -1, -1):
                    if t2_history.iloc[i].get('pct_chg', 0) > 9.8: lb_count += 1
                    else: break
                t2_lb_days = lb_count

            if talib and len(df) >= 30:
                closes = df['close'].values.astype(float)
                highs = df['high'].values.astype(float)
                lows = df['low'].values.astype(float)
                
                # 1. 均线类 (SMA)
                ma5_arr = talib.SMA(closes, timeperiod=5)
                ma10_arr = talib.SMA(closes, timeperiod=10)
                ma20_arr = talib.SMA(closes, timeperiod=20)
                ma5 = round(ma5_arr[-1], 2)
                ma10 = round(ma10_arr[-1], 2)
                ma20 = round(ma20_arr[-1], 2)

                # 2. RSI 6
                rsi_6_arr = talib.RSI(closes, timeperiod=6)
                rsi_6 = round(rsi_6_arr[-1], 2) if not np.isnan(rsi_6_arr[-1]) else 0
                
                # 3. BIAS 20 (基于 MA20)
                bias_20 = round((closes[-1] - ma20) / ma20 * 100, 2) if ma20 != 0 else 0

                # 4. MACD (12, 26, 9)
                dif, dea, hist = talib.MACD(closes, fastperiod=12, slowperiod=26, signalperiod=9)
                macd_dif = round(dif[-1], 3) if not np.isnan(dif[-1]) else 0
                macd_dea = round(dea[-1], 3) if not np.isnan(dea[-1]) else 0
                macd_hist = round(hist[-1] * 2, 3) if not np.isnan(hist[-1]) else 0 # 乘2对位通达信

                # 5. BOLL (20, 2)
                upper, middle, lower = talib.BBANDS(closes, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
                boll_up = round(upper[-1], 2) if not np.isnan(upper[-1]) else 0
                boll_mid = round(middle[-1], 2) if not np.isnan(middle[-1]) else 0
                boll_low = round(lower[-1], 2) if not np.isnan(lower[-1]) else 0

                # 6. KDJ (9, 3, 3) - 适配中国行情标准 (RSV=9, K=3, D=3)
                # RSV = (Close - L9) / (H9 - L9) * 100
                low_9 = talib.MIN(lows, timeperiod=9)
                high_9 = talib.MAX(highs, timeperiod=9)
                rsv = (closes - low_9) / (high_9 - low_9 + 0.001) * 100
                # 标准 KDJ 使用 1/3 的平滑系数，对应 EMA(com=2)
                k_arr = pd.Series(rsv).ewm(com=2, adjust=False).mean().values
                d_arr = pd.Series(k_arr).ewm(com=2, adjust=False).mean().values
                j_arr = 3 * k_arr - 2 * d_arr
                kdj_k, kdj_d, kdj_j = round(k_arr[-1], 2), round(d_arr[-1], 2), round(j_arr[-1], 2)

            return {
                "change_pct_5d": change_5d,
                "avg_turnover_5d": avg_turn,
                "limit_up_days_5": limit_ups,
                "real_market_cap": self.f10_market_cap.get(code, 0),
                "rsi_6": rsi_6,
                "bias_20": bias_20,
                "vol_ratio": round(avg_turn / (df['turn'].iloc[-20:-5].mean() + 0.1), 2),
                "ma5": ma5, "ma10": ma10, "ma20": ma20,
                "macd_dif": macd_dif, "macd_dea": macd_dea, "macd_hist": macd_hist,
                "kdj_k": kdj_k, "kdj_d": kdj_d, "kdj_j": kdj_j,
                "boll_up": boll_up, "boll_mid": boll_mid, "boll_low": boll_low,
                "t2_lb_days": t2_lb_days, "t2_pct": t2_pct
            }
        except Exception as e:
            logger.error(f"计算多因子异常 {code}: {e}")
            return {}

    def get_all_active_stocks(self, date_str: str) -> List[str]:
        """获取股票列表，带重试 + 代码白名单过滤 + F10索引兜底"""
        def _filter(code_full: str) -> bool:
            # 上交所：60/601/603/605/688；深交所：000/001/002/003/300/301；北交所：83/87
            code6 = code_full.split(".")[1]
            return code6.startswith(("60", "601", "603", "605", "688",
                                     "000", "001", "002", "003", "300", "301",
                                     "83", "87"))

        for i in range(3):
            try:
                stocks = []
                rows = self.kline_service.query_all_stock_rows(date_str)
                for row in rows:
                    code_full = row[0]
                    trade_status = row[1] if len(row) > 1 else "1"
                    if trade_status == "1" and _filter(code_full):
                        stocks.append(code_full.split(".")[1])
                if stocks:
                    return stocks
            except Exception as e:
                logger.warning(f"获取股票列表异常({i+1}/3): {e}")
                time.sleep(0.5 * (i + 1))
        # F10索引兜底，减少空跑
        if self.f10_service.index_by_code:
            stocks = [c for c in self.f10_service.index_by_code.keys() if _filter(f"sh.{c}" if c.startswith('6') else f"sz.{c}")]
            if stocks:
                logger.warning(f"⚠️ 使用F10索引兜底股票列表，数量: {len(stocks)}")
                return stocks
        logger.error("获取股票列表失败，重试用尽且兜底为空")
        return []

    def _process_single_stock(self, code: str, start_date: str, target_date: str):
        """兼容旧版接口：自获取 K 线并计算"""
        k_list = self.kline_service.fetch_kline_data(code, frequency='d', start_date=start_date, end_date=target_date)
        return self.calculate_for_stock(code, k_list, target_date)

    def calculate_for_stock(self, code: str, k_list: List[Dict], target_date: str):
        """核心计算原子 (V3.1)：支持 TA-Lib 加速与内存自愈"""
        if not k_list or len(k_list) < 5:
            return code, None, None
            
        try:
            # 1. 内存紧凑型转换
            df = pd.DataFrame(k_list)
            df = df.rename(columns={'time': 'date'})
            
            # 2. 调用算法核心 (TA-Lib 加速版)
            peak = self.calculate_chip_peak(df)
            factors = self.calculate_extra_factors(code, df)
            
            # 为 TDengine 双写补全数据
            if peak: peak['date'] = target_date
            if factors: factors['date'] = target_date

            # 3. 内存释放信号 (显式清理大型 DF)
            del df
            # 注意：此处不执行 gc.collect()，因为并发环境下频繁 GC 会导致 STW 延迟
            # 交由 Python 引用计数机制处理，或在 Orchestrator 批次结束执行
            
            return code, peak, factors
        except Exception as e:
            logger.error(f"❌ 筹码/因子计算失败 [{code}]: {e}")
            return code, None, None

    def run_batch(self, target_date: str = None, max_kline_days: int = 250, max_workers: int = 1):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # 1) 规范目标日期：非交易日回退上一交易日
        if not target_date:
            target_date = datetime.now().strftime('%Y-%m-%d')
        if not self.calendar.is_trade_day(target_date):
            target_date = self.calendar.get_previous_trade_day(target_date)
        if not target_date:
            logger.error("无法确定目标交易日，退出")
            return

        start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=int(max_kline_days*1.5))).strftime('%Y-%m-%d')
        if os.getenv("BAOSTOCK_THREAD_SAFE", "1") != "1":
            max_workers = 1
        logger.info(f"开始批量计算 (并行度:{max_workers}), 基准日: {target_date}")
        
        if not self.kline_service.ensure_baostock_session():
            logger.error("Baostock session unavailable, abort chip batch run")
            return
        # 确保市值数据已加载
        if not self.f10_market_cap:
            self.load_f10_market_cap()
        stocks = self.get_all_active_stocks(target_date)
        # 若当日列表为空，尝试上一交易日兜底
        if not stocks:
            fallback_day = self.calendar.get_previous_trade_day(target_date)
            if fallback_day and fallback_day != target_date:
                logger.warning(f"⚠️ 当日股票列表为空，回退上一交易日: {fallback_day}")
                stocks = self.get_all_active_stocks(fallback_day)
                if stocks:
                    target_date = fallback_day
                    start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=int(max_kline_days*1.5))).strftime('%Y-%m-%d')
        # 仍为空则从F10索引兜底（避免接口异常导致空跑）
        if not stocks and self.f10_service.index_by_code:
            stocks = list(self.f10_service.index_by_code.keys())
            logger.warning(f"⚠️ 使用F10索引兜底股票列表，数量: {len(stocks)}")
        if not stocks:
            logger.error("未获取到股票列表，放弃本次批算（不覆盖已有缓存）")
            return
        chip_key = f"cache:chip_peaks:{target_date}"
        extra_key = f"cache:stock_extra:{target_date}"
        
        # Incremental Load: Identify missing stocks
        existing_chips = set(self.redis.hkeys(chip_key) or [])
        existing_extras = set(self.redis.hkeys(extra_key) or [])
        completed_stocks = existing_chips.intersection(existing_extras)
        
        # [Startup Recovery] 额外检查 TDengine K线完整性
        # 改进：不再强制清空已完成记录，仅对缺失部分进行补抓。
        try:
            td_count = self.kline_service.tdengine.get_daily_count(target_date)
            if td_count < 1000: # 调低硬阈值，1000以下才视为严重异常
                logger.warning(f"⚠️ [Startup Recovery] TDengine K线数据严重不足 ({td_count} < 1000)，检查数据源...")
            elif td_count < len(stocks):
                logger.info(f"ℹ️ [Incremental] 数据库当前记录数 {td_count}/{len(stocks)}，将为剩余缺口执行补算。")
        except Exception as e:
            logger.error(f"❌ 检查 TDengine 完整性时异常: {e}")

        pending_stocks = [c for c in stocks if c not in completed_stocks]
        if not pending_stocks:
            logger.info(f"✅ 所有股票(总计{len(stocks)})已完成批算，跳过。")
            return
            
        logger.info(f"增量批算: 发现 {len(completed_stocks)} 已完成, 剩余 {len(pending_stocks)} 待处理。")
        stocks_to_process = pending_stocks

        success = 0
        batch_size = 100
        
        # 使用线程池加速 I/O 绑定任务 (P1 Parallelization)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._process_single_stock, code, start_date, target_date) for code in stocks_to_process]
            
            chip_batch = {}
            extra_batch = {}
            
            for i, future in enumerate(as_completed(futures)):
                code, peak, factors = future.result()
                if peak:
                    chip_batch[code] = json.dumps(peak)
                if factors:
                    extra_batch[code] = json.dumps(factors)
                
                if peak or factors:
                    success += 1

                # 批量写入 Redis
                if len(chip_batch) >= batch_size or i == len(stocks_to_process) - 1:
                    if chip_batch:
                        self.redis.hset(chip_key, mapping=chip_batch)
                        chip_batch.clear()
                    if extra_batch:
                        self.redis.hset(extra_key, mapping=extra_batch)
                        extra_batch.clear()
                    # 进度条单行刷新 (Carrier Return)
                    pct = ((i + 1) / len(stocks_to_process)) * 100
                    sys.stdout.write(f"\r📊 筹码批算: [{'#' * (int(pct)//5)}{'-' * (20 - int(pct)//5)}] {i+1}/{len(stocks_to_process)} (成功:{success})   ")
                    sys.stdout.flush()
                    if i == len(stocks_to_process) - 1:
                        print()

        self.redis.expire(chip_key, 172800)
        self.redis.expire(extra_key, 172800)
        logger.info(f"✅ 完成! 总计={success}")


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    runner = ChipBatchRunner()
    runner.run_batch(target_date=target)

