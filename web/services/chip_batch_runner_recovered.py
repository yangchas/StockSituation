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
import threading
import numpy as np
import pandas as pd
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
        self._initialized = True

    def get_all_active_stocks(self, date_str: str) -> List[str]:
        """获取股票列表，带重试 + 代码白名单过滤 + F10索引兜底"""
        import baostock as bs
        def _filter(code_full: str) -> bool:
            # 上交所：60/601/603/605/688；深交所：000/001/002/003/300/301；北交所：83/87
            code6 = code_full.split(".")[1]
            return code6.startswith(("60", "601", "603", "605", "688",
                                     "000", "001", "002", "003", "300", "301",
                                     "83", "87"))

        for i in range(3):
            try:
                rs = bs.query_all_stock(day=date_str)
                stocks = []
                while rs.error_code == '0' and rs.next():
                    row = rs.get_row_data()
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
        """单只股票处理任务 (并入共享 K 线服务)"""
        # 使用统一服务获取 K 线 (受益于增量更新与 TDengine 缓存)
        k_list = self.kline_service.fetch_kline_data(code, frequency='d', start_date=start_date, end_date=target_date)
        
        if k_list and len(k_list) >= 5:
            # 转换为 DataFrame 以兼容后续计算逻辑
            df = pd.DataFrame(k_list)
            # 确保列名兼容 (fetch_kline_data 返回的是 time, open, high, low, close, volume, amount, turn, pct_chg)
            # calculate_chip_peak 和 calculate_extra_factors 预期 date, open, high, low, close, volume, amount, turn
            df = df.rename(columns={'time': 'date', 'turn': 'turn'})
            
            peak = self.calculate_chip_peak(df)
            factors = self.calculate_extra_factors(code, df)
            return code, peak, factors
        return code, None, None

    def run_batch(self, target_date: str = None, max_kline_days: int = 250, max_workers: int = 10):
        import baostock as bs
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
        if os.getenv("BAOSTOCK_THREAD_SAFE", "0") != "1":
            max_workers = 1
        logger.info(f"开始批量计算 (并行度:{max_workers}), 基准日: {target_date}")
        
        bs.login()
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
            bs.logout()
            return
        chip_key = f"cache:chip_peaks:{target_date}"
        extra_key = f"cache:stock_extra:{target_date}"
        
        # Incremental Load: Identify missing stocks
        existing_chips = set(self.redis.hkeys(chip_key) or [])
        existing_extras = set(self.redis.hkeys(extra_key) or [])
        completed_stocks = existing_chips.intersection(existing_extras)
        
        # [Startup Recovery] 额外检查 TDengine K线完整性
        # 如果 Redis 里觉得跑完了，但 TDengine 里还是空的，说明肯定有问题，强制重跑
        try:
            td_count = self.kline_service.tdengine.get_daily_count(target_date)
            if td_count < 4000:
                logger.warning(f"⚠️ [Startup Recovery] TDengine K线数据严重不足 ({td_count} < 4000)，强制清除进度记录执行补抓...")
                completed_stocks = set()
        except Exception as e:
            logger.error(f"❌ 检查 TDengine 完整性时异常: {e}")

        pending_stocks = [c for c in stocks if c not in completed_stocks]
        if not pending_stocks:
            logger.info(f"✅ 所有股票(总计{len(stocks)})已完成批算，跳过。")
            bs.logout()
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
                    logger.info(f"进度: {i+1}/{len(stocks_to_process)}, 本批新增成功={success}")

        self.redis.expire(chip_key, 172800)
        self.redis.expire(extra_key, 172800)
        bs.logout()
        logger.info(f"✅ 完成! 总计={success}")


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    runner = ChipBatchRunner()
    runner.run_batch(target_date=target)

