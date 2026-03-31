"""
v2_data_lifecycle.py
数据生命周期管理器 — 盘后/盘前/启动时的数据同步调度

时序：
  16:30 盘后  → 触发全量增量日K / DDE / 昨日涨停异步同步
  17:00       → 数据到齐后触发 Rust 多因子计算
  08:30 盘前  → 快速完整性检查，缺失项按需补请求
  09:00       → 快速补拉昨日板块/涨停（几秒内完成）
  启动时      → 检查数据状态，根据时间决策，不全量重拉K线

规则：
  - 所有网络请求全部通过 AsyncDataPipeline（断点续传 + 重试）
  - 盘中严禁触发全量K线同步
  - 每次成功同步日期写入 Redis/本地 checkpoint
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, date, time as dt_time, timedelta
from typing import Callable, Coroutine, List, Optional

from v2_async_pipeline import AsyncDataPipeline, Checkpoint, FetchTask, build_stock_tasks, TaskStatus
from web.services.trading_calendar_service import TradingCalendarService

logger = logging.getLogger("V2Lifecycle")

# ─────────────────────────────────────────────────────────────────────────────
# 状态文件（轻量 JSON，避免 Redis 依赖失败时无法启动）
# ─────────────────────────────────────────────────────────────────────────────

STATE_FILE = os.path.join(os.path.dirname(__file__), "data_state.json")


def _load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_state(state: dict):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存状态失败: {e}")


def _today() -> str:
    return date.today().strftime("%Y-%m-%d")


def _now_time() -> dt_time:
    return datetime.now().time()


# ─────────────────────────────────────────────────────────────────────────────
# 数据完整性检查
# ─────────────────────────────────────────────────────────────────────────────

class DataIntegrityChecker:
    """
    检查各类数据的同步状态，返回缺失项列表。
    轻量检查：只看状态文件和 checkpoint，不查 DB 避免启动慢。
    """

    def __init__(self, state: dict):
        self.state = state
        self.today = _today()

    def check(self) -> dict:
        missing = {}

        # 日K（最关键，昨日16:30后应已同步）
        daily_kline_date = self.state.get("daily_kline_last_sync", "")
        if daily_kline_date < self.today:
            missing["daily_kline"] = f"最后同步: {daily_kline_date or '从未'}"

        # DDE（近20日，每日盘后同步）
        dde_date = self.state.get("dde_last_sync", "")
        if dde_date < self.today:
            missing["dde"] = f"最后同步: {dde_date or '从未'}"

        # 昨日涨停（可以快速补，<5秒）
        yest_bans_date = self.state.get("yest_bans_last_sync", "")
        if yest_bans_date < self.today:
            missing["yest_bans"] = f"最后同步: {yest_bans_date or '从未'}"

        # 昨日热门板块（可以快速补，<5秒）
        yest_plates_date = self.state.get("yest_plates_last_sync", "")
        if yest_plates_date < self.today:
            missing["yest_plates"] = f"最后同步: {yest_plates_date or '从未'}"

        # 多因子矩阵（依赖日K，Rust计算后置位）
        factor_date = self.state.get("factor_matrix_last_calc", "")
        if factor_date < self.today:
            missing["factor_matrix"] = f"最后计算: {factor_date or '从未'}"

        return missing


# ─────────────────────────────────────────────────────────────────────────────
# 数据生命周期管理器
# ─────────────────────────────────────────────────────────────────────────────

class DataLifecycleManager:
    """
    所有数据同步入口，根据当前时间决策哪些数据需要同步。
    fetch_daily_kline_fn: async (symbol) -> bool  （外部注入，避免循环依赖）
    fetch_dde_fn:         async (symbol) -> bool
    fetch_yest_bans_fn:   async () -> bool
    fetch_yest_plates_fn: async () -> bool
    trigger_rust_calc_fn: async () -> bool
    """

    def __init__(
        self,
        symbol_list: List[str],
        fetch_daily_kline_fn: Optional[Callable] = None,
        fetch_dde_fn: Optional[Callable] = None,
        fetch_yest_bans_fn: Optional[Callable] = None,
        fetch_yest_plates_fn: Optional[Callable] = None,
        trigger_rust_calc_fn: Optional[Callable] = None,
        pipeline: Optional[AsyncDataPipeline] = None,
        checkpoint: Optional[Checkpoint] = None,
    ):
        self.symbols = symbol_list
        self._fn_kline   = fetch_daily_kline_fn
        self._fn_dde     = fetch_dde_fn
        self._fn_bans    = fetch_yest_bans_fn
        self._fn_plates  = fetch_yest_plates_fn
        self._fn_rust    = trigger_rust_calc_fn
        self._cp         = checkpoint or Checkpoint()
        # 修正：Baostock 建议单线程 (concurrency=1)，延迟可大幅缩短至 0.1s
        self._pipeline   = AsyncDataPipeline(
            max_retry=3, 
            concurrency=1, 
            delay_jitter=0.1,
            checkpoint=self._cp
        )
        self.calendar    = TradingCalendarService()
        self._state      = _load_state()

    # ── 公开入口 ──────────────────────────────────────────────────────────────

    async def on_startup(self):
        """启动自检：决策执行全量同步还是轻量同步"""
        logger.info(f"[Lifecycle] 🚀 正在执行启动数据自检 (Symbols: {len(self.symbols)})")
        
        # 1. 启动必同步：昨日涨停、热门板块 (Guard 核心依赖，秒级)
        await self._sync_metadata()
        
        # 2. 时段判定：盘中 (09:15-15:31) 跳过耗时任务
        now = _now_time()
        is_intra_day = dt_time(9, 15) <= now <= dt_time(15, 31)
        
        if is_intra_day:
            logger.info("[Lifecycle] ⚡ [Intra-Day] 盘中启动：跳过日K/DDE全量对齐，保持极速护航模式。")
            return
        
        # 3. 非交易时段 (盘后或凌晨) 触发完整的 EOD 数据回填
        logger.info("[Lifecycle] 📊 [EOD-Window] 处于非盘中时段，启动全量数据同步流程...")
        await self._sync_eod(skip_metadata=True)

    async def on_eod(self):
        """16:30 盘后触发：全量数据结算"""
        logger.info("[Lifecycle] 🏁 盘后结算时刻 (16:30)，启动全量同步...")
        await self._sync_eod()

    async def _sync_metadata(self):
        """同步轻量级元数据 (涨停、板块)"""
        today = _today()
        logger.info("[Lifecycle] 正在同步盘前极值数据 (涨停/板块)...")
        try:
            if self._fn_bans: 
                await self._fn_bans()
                self._state["yest_bans_last_sync"] = today
            if self._fn_plates: 
                await self._fn_plates()
                self._state["yest_plates_last_sync"] = today
            _save_state(self._state)
        except Exception as e:
            logger.error(f"[Lifecycle] Metadata Sync Error: {e}")

    async def _sync_eod(self, skip_metadata: bool = False):
        """完整的盘后同步流程"""
        if not skip_metadata:
            await self._sync_metadata()
            
        # 1. 日K线同步
        if self._fn_kline:
            await self._sync_daily_kline()
            
        # 2. DDE 同步
        if self._fn_dde:
            await self._sync_dde()
            
        # 3. 因子计算
        if self._fn_rust:
            logger.info("[Lifecycle] 触发核心因子矩阵计算刷新...")
            try:
                await self._fn_rust()
                self._state["factor_matrix_last_calc"] = _today()
                _save_state(self._state)
            except Exception as e:
                logger.error(f"[Lifecycle] Factor Calc Failure: {e}")

    async def _sync_daily_kline(self):
        """全量增量日K同步（断点续传，异步后台）"""
        today = _today()
        # 日期对齐：如果在结算前启动，对齐到上一个交易日，防止污染 Checkpoint
        now = _now_time()
        if now < dt_time(16, 30):
            target_date = self.calendar.get_previous_trade_day(today)
            date_tag = target_date.replace("-", "")
        else:
            date_tag = today.replace("-", "")

        logger.info(f"[Lifecycle] 日K同步开始 (目标日期: {date_tag})")

        tasks = build_stock_tasks(self.symbols, "daily_kline", self._cp, date_tag=date_tag)
        pending = [t for t in tasks if t.status != TaskStatus.DONE]
        logger.info(f"[Lifecycle] 需同步 {len(pending)} 只（跳过已完成 {len(tasks)-len(pending)} 只）")

        if not pending or not self._fn_kline:
            return

        results = await self._pipeline.run_batch(
            pending,
            func_map={"daily_kline": self._fn_kline},
            arg_builder=lambda t: (t.symbol,),
        )
        done = sum(1 for r in results if r.status == TaskStatus.DONE)
        if done == len(pending):
            self._state["daily_kline_last_sync"] = today
            _save_state(self._state)
            logger.info(f"[Lifecycle] 日K全量同步完成 ✅ ({done} 只)")
        else:
            logger.warning(f"[Lifecycle] 日K同步部分完成: {done}/{len(pending)}")

    async def _sync_dde(self):
        """20日DDE同步（与K线同批异步）"""
        today = _today()
        # 专项纠偏：DDE 数据 0点更新，结算前始终锁定上一个交易日
        now = _now_time()
        if now < dt_time(16, 30):
            target_date = self.calendar.get_previous_trade_day(today)
            date_tag = target_date.replace("-", "")
        else:
            date_tag = today.replace("-", "")
        
        logger.info(f"[Lifecycle] DDE 同步开始 (目标日期: {date_tag})")
        tasks = build_stock_tasks(self.symbols, "dde", self._cp, date_tag=date_tag)
        pending = [t for t in tasks if t.status != TaskStatus.DONE]

        if not pending or not self._fn_dde:
            return

        results = await self._pipeline.run_batch(
            pending,
            func_map={"dde": self._fn_dde},
            arg_builder=lambda t: (t.symbol,),
        )
        done = sum(1 for r in results if r.status == TaskStatus.DONE)
        if done == len(pending):
            self._state["dde_last_sync"] = today
            _save_state(self._state)
            logger.info(f"[Lifecycle] DDE全量同步完成 ✅ ({done} 只)")


# ─────────────────────────────────────────────────────────────────────────────
# 测试入口
# ─────────────────────────────────────────────────────────────────────────────

async def _mock_fetch_kline(symbol: str) -> bool:
    await asyncio.sleep(0.01)
    return True

async def _mock_fetch_bans() -> bool:
    logger.info("[Mock] 昨日涨停数据获取完成")
    return True

async def _mock_fetch_plates() -> bool:
    logger.info("[Mock] 昨日热门板块数据获取完成")
    return True


async def demo():
    mgr = DataLifecycleManager(
        symbol_list=["000001", "600519", "300308"],
        fetch_daily_kline_fn=_mock_fetch_kline,
        fetch_yest_bans_fn=_mock_fetch_bans,
        fetch_yest_plates_fn=_mock_fetch_plates,
    )
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 模拟启动检查...")
    await mgr.on_startup()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 完成")
    print(f"当前状态: {_load_state()}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")
    asyncio.run(demo())
