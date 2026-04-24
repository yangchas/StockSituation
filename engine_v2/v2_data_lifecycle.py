"""
v2_data_lifecycle.py - V3.1 集成对撞版
数据生命周期管理器 — 核心调度中枢
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, date, time as dt_time, timedelta
from typing import Callable, Coroutine, List, Optional

from v2_async_pipeline import AsyncDataPipeline, Checkpoint, FetchTask, build_stock_tasks, TaskStatus
from web.services.trading_calendar_service import TradingCalendarService
from web.services.stock_kline_service import StockKLineService

logger = logging.getLogger("V2Lifecycle")
STATE_FILE = os.path.join(os.path.dirname(__file__), "data_state.json")

def _load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception: pass
    return {}

def _save_state(state: dict):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e: logger.warning(f"保存状态失败: {e}")

class DataLifecycleManager:
    def __init__(
        self,
        symbol_list: List[str],
        fetch_daily_kline_fn: Optional[Callable] = None,
        fetch_dde_fn: Optional[Callable] = None,
        fetch_yest_bans_fn: Optional[Callable] = None,
        fetch_yest_plates_fn: Optional[Callable] = None,
        trigger_rust_calc_fn: Optional[Callable] = None,
        check_status_fn: Optional[Callable] = None, # 🚀 [V39.2] 新增全维度状态自检函数
        pipeline: Optional[AsyncDataPipeline] = None,
        checkpoint: Optional[Checkpoint] = None,
    ):
        self.symbols = symbol_list
        self._fn_kline   = fetch_daily_kline_fn
        self._fn_dde     = fetch_dde_fn
        self._fn_bans    = fetch_yest_bans_fn
        self._fn_plates  = fetch_yest_plates_fn
        self._fn_rust    = trigger_rust_calc_fn
        self._fn_check   = check_status_fn
        self._cp         = checkpoint or Checkpoint()
        # V3.3 稳定性验证：在 TLS 线程局部连接模式下，恢复 4 个并发
        self._pipeline   = AsyncDataPipeline(max_retry=3, concurrency=4, delay_jitter=0.05, checkpoint=self._cp)
        self.calendar    = TradingCalendarService()
        self._state      = _load_state()

    async def on_startup(self, override_date: str = None):
        """启动自检：全时窗排期检测 (V3.1 集成版)"""
        now = datetime.now().time()
        logger.info(f"[Lifecycle] 🚀 智库自检中 (Time: {now.strftime('%H:%M:%S')})")
        
        # 1. 元数据快速补齐
        if dt_time(0, 0) <= now <= dt_time(15, 0):
            await self._sync_metadata()

        # 2. 核心调度：17:40 - 24:00 或 00:00 - 09:00 进入数据对撞窗口
        # 将触发时间对准至 17:40，确保 Baostock 日线数据结清
        if dt_time(17, 40) <= now <= dt_time(23, 59) or dt_time(0, 0) <= now < dt_time(9, 25):

            # [A.全量对撞] 数据补齐
            logger.info(f"[Lifecycle] 🕒 [A.全量对撞] 窗口开启 (当前: {now.strftime('%H:%M')})...")
            await self._sync_daily_kline(override_date)
            await self._trigger_factors()
            
            # [B.物理审计] 跨日对账逻辑 (建议在 01:00 - 08:30 之间执行)
            if dt_time(1, 0) <= now < dt_time(8, 30):
                target_day = self.calendar.get_previous_trade_day(date.today().strftime("%Y-%m-%d"))
                logger.info(f"[Lifecycle] 📊 [B.物理审计] 启动对昨日 ({target_day}) 的全网对账...")
                try:
                    import subprocess
                    subprocess.Popen([sys.executable, "v2_recap_engine_final.py", "--date", target_day])
                except Exception as e:
                    logger.error(f"启动复盘引擎失败: {e}")
        else:
            logger.info("[Lifecycle] 🛡️ 战时稳定优先，仅加载本地数据种子。")

    async def on_eod(self):
        """16:30 盘后结算触发"""
        logger.info("[Lifecycle] 🏁 盘后结算黄金窗开启 (16:30)...")
        await self._sync_daily_kline()
        await self._trigger_factors()

    async def _sync_metadata(self):
        """同步昨日极值元数据"""
        try:
            if self._fn_bans: await self._fn_bans()
            if self._fn_plates: await self._fn_plates()
        except Exception as e: logger.error(f"[Lifecycle] Metadata Error: {e}")

    async def _sync_daily_kline(self, override_date: str = None):
        """V3.1 集成对撞逻辑：K线与因子并行处理"""
        today = override_date or date.today().strftime("%Y-%m-%d")
        now = datetime.now().time()
        
        # 确定目标日期：15:30前算上一交易日，15:30后算今日
        # [V19.6] 如果 override_date 存在，则跳过逻辑判断直接对齐
        if override_date:
            target_date = override_date
        elif not self.calendar.is_trade_day(today) or now < dt_time(15, 30):
            target_date = self.calendar.get_previous_trade_day(today)
        else:
            target_date = today
            
        date_tag = target_date.replace("-", "")
        logger.info(f"[Lifecycle] 🚀 执行 V3.1 集成流水线 (Tag: {date_tag})")

        # [V7.5] 物理对撞校验函数：确保 Checkpoint 的 'done' 与数据库物理水位一致
        k_service = StockKLineService()
        def physical_validator(sym: str, tag: str) -> bool:
            # 🚀 [V39.2] 优先使用外部注入的全维度校验逻辑
            if self._fn_check:
                return self._fn_check(sym, tag)
                
            # 降级：仅校验 K线
            target_iso = f"{tag[:4]}-{tag[4:6]}-{tag[6:]}"
            latest = k_service.latest_dates_map.get(sym)
            if not latest: return False
            return latest >= target_iso

        # 核心：使用 integrated_sync 任务名，触发 Orchestrator 的边拉边算逻辑
        tasks = build_stock_tasks(self.symbols, "integrated_sync", self._cp, date_tag=date_tag, validate_fn=physical_validator)
        pending = [t for t in tasks if t.status != TaskStatus.DONE]
        
        if not pending or not self._fn_kline:
            logger.info(f"✅ [Lifecycle] 集成对齐完成 (跳过 {len(tasks)-len(pending)} 只，待对撞 0 只)")
            return

        logger.info(f"📊 [Lifecycle] 集成处理中: {len(pending)} 只待处理 [并发: {self._pipeline.concurrency}]")
        results = await self._pipeline.run_batch(
            pending,
            func_map={"integrated_sync": self._fn_kline},
            arg_builder=lambda t: (t.symbol,),
        )
        
        # 🚀 [V34.3] 自动黑名单构建：将最终失败的僵尸股加入黑名单
        fails = [r for r in results if r.status == TaskStatus.FAILED]
        if fails:
            from v2_infra_provider import get_global_redis
            r_client = await get_global_redis()
            for f_task in fails:
                # 判据：如果是由于函数返回 False (数据源缺失) 导致的最终失败
                if f_task.error == "Function returned False" and f_task.symbol:
                    await r_client.sadd("market_edge:blacklist", f_task.symbol)
                    logger.warning(f"🗄️ [Blacklist-Auto] {f_task.symbol} 物理同步失败已达上限，已永久列入黑名单过滤池")
        
        done = sum(1 for r in results if r.status == TaskStatus.DONE)
        if done == len(pending):
            self._state["daily_kline_last_sync"] = today
            _save_state(self._state)
            logger.info(f"[Lifecycle] 🏆 集成同步全量完成 ✅ ({done} 只已更新完毕)")

    async def _trigger_factors(self):
        """通知后端镜像更新"""
        if self._fn_rust:
            logger.info("[Lifecycle] 🔔 指通后端 (Rust Core) 刷新计算镜像...")
            await self._fn_rust()
            self._state["factor_matrix_last_calc"] = date.today().strftime("%Y-%m-%d")
            _save_state(self._state)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")
    print("V3.1 DataLifecycle Manager Loaded.")
