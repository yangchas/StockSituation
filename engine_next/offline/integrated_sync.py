from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import queue
import shutil
import sys
import threading
import time
from datetime import datetime
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from typing import Any, Iterable, Sequence

import pandas as pd

from engine_next.adapters.runtime_kline_service import KlineReadyPlan, RuntimeKlineService
from engine_next.contracts.baostock_contracts import check_baostock_daily_kline_availability
from engine_next.contracts.offline_sync_contracts import (
    ChipResult,
    DdeResult,
    FactorResult,
    IntegratedSyncResult,
    KlineWindow,
    ResumeCheckpointState,
    WatermarkSnapshot,
)
from engine_next.contracts.schema_contracts import get_storage_schema_spec, trim_record_to_fields
from engine_next.domain.enums import ExecutionEnvironment
from engine_next.resources.service_singletons import get_shared_chip_batch_runner
from engine_next.runtime.execution_profile import get_default_execution_profile


logger = logging.getLogger(__name__)

PROGRESS_LOG_INTERVAL = 10
HEARTBEAT_INTERVAL_SECONDS = 15
STALL_WARNING_SECONDS = 30
STAGE_NETWORK = "network"
STAGE_ANALYTICS = "analytics"
STAGE_FULL = "full"


@dataclass(frozen=True)
class IntegratedSyncTask:
    task_id: str
    symbol: str
    target_date: str
    needs_network_kline: bool
    needs_network_dde: bool
    needs_factor: bool
    needs_chip: bool


@dataclass(frozen=True)
class NetworkTaskResult:
    task: IntegratedSyncTask
    result: IntegratedSyncResult
    kline_window: KlineWindow | None


@dataclass(frozen=True)
class DeferredKlinePersistence:
    symbol: str
    target_date: str
    plan: KlineReadyPlan


def _render_stage_progress(stage: str, done: int, total: int, extra: str = "") -> None:
    if total <= 0:
        return
    percent = max(0.0, min(100.0, (done / total) * 100.0))
    filled = min(20, int(percent) // 5)
    bar = f"{'#' * filled}{'-' * (20 - filled)}"
    suffix = f" | {extra}" if extra else ""
    sys.stdout.write(f"\r[settlement:{stage}] [{bar}] {done}/{total} ({percent:.1f}%){suffix}   ")
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n")
        sys.stdout.flush()


def _render_pipeline_progress(
    done: int,
    total: int,
    *,
    network_done: int,
    network_total: int,
    analytics_done: int,
    analytics_total: int,
    extra: str = "",
) -> None:
    if total <= 0:
        return
    percent = max(0.0, min(100.0, (done / total) * 100.0))
    line = (
        f"[进度] {percent:4.1f}% {done}/{total}"
        f" | 联网{network_done}/{network_total}"
        f" | 重算{analytics_done}/{analytics_total}"
    )
    if extra:
        line = f"{line} | {extra}"
    if hasattr(sys.stdout, "isatty") and sys.stdout.isatty():
        terminal_width = shutil.get_terminal_size((120, 20)).columns
        safe_width = max(40, terminal_width - 4)
        if len(line) > safe_width:
            line = line[: max(0, safe_width - 3)] + "..."
        sys.stdout.write(f"\r\033[2K{line}")
        sys.stdout.flush()
        if done >= total:
            sys.stdout.write("\n")
            sys.stdout.flush()
        return
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


class WatermarkAuditService:
    """Bulk watermark audit aligned with engine_v2._preload_all_watermarks."""

    def __init__(self, tdengine_service: Any | None = None) -> None:
        self._tdengine = tdengine_service

    @property
    def tdengine(self) -> Any:
        if self._tdengine is None:
            from web.services.tdengine_service import TDengineService

            self._tdengine = TDengineService()
        return self._tdengine

    def preload_all_watermarks(self, target_date: str) -> WatermarkSnapshot:
        logger.debug("watermark audit query start | target_date=%s", target_date)
        kline_latest_dates = self.tdengine.get_all_latest_dates("daily_kline") or {}
        dde_latest_dates = self.tdengine.get_all_latest_dates("daily_dde", "ddje") or {}
        factor_latest_dates = self.tdengine.get_all_latest_dates("daily_factors", "profit_ratio") or {}
        logger.debug(
            "watermark audit query done | kline=%s | dde=%s | factor=%s",
            len(kline_latest_dates),
            len(dde_latest_dates),
            len(factor_latest_dates),
        )
        return WatermarkSnapshot(
            target_date=target_date,
            kline_latest_dates=kline_latest_dates,
            dde_latest_dates=dde_latest_dates,
            factor_latest_dates=factor_latest_dates,
            notes=(
                "Bulk TDengine watermark audit uses GROUP BY symbol + LAST/MAX(ts).",
                "Do not replace this with per-symbol latest-date queries on startup.",
            ),
        )


class KlineWindowProvider:
    """Provides enough historical daily-kline rows for factor and chip computation."""

    def __init__(self, kline_service: Any | None = None) -> None:
        self._kline_service = kline_service

    @property
    def kline_service(self) -> Any:
        if self._kline_service is None:
            self._kline_service = RuntimeKlineService()
        return self._kline_service

    def ensure_kline_ready(
        self,
        symbol: str,
        target_date: str,
        lookback_days: int = 60,
        latest_local: str | None = None,
    ) -> KlineWindow:
        rows = (
            self.kline_service.ensure_kline_ready(
                symbol,
                target_date,
                days=lookback_days,
                latest_local=latest_local,
            )
            or []
        )
        return KlineWindow(
            symbol=symbol,
            target_date=target_date,
            lookback_days=lookback_days,
            rows=tuple(rows),
            notes=(
                f"ensure_kline_ready returned {len(rows)} rows.",
                "Redis must not store the full kline window; only lightweight runtime views may persist.",
            ),
        )

    def ensure_kline_ready_plan(
        self,
        symbol: str,
        target_date: str,
        lookback_days: int = 60,
        latest_local: str | None = None,
    ) -> tuple[KlineWindow, KlineReadyPlan]:
        plan = self.kline_service.ensure_kline_ready_plan(
            symbol,
            target_date,
            days=lookback_days,
            latest_local=latest_local,
        )
        return (
            KlineWindow(
                symbol=symbol,
                target_date=target_date,
                lookback_days=lookback_days,
                rows=tuple(plan.window_rows),
                notes=(
                    f"ensure_kline_ready_plan returned {len(plan.window_rows)} rows.",
                    "Kline window was built from in-memory source rows before deferred persistence.",
                ),
            ),
            plan,
        )

    def load_existing_window(self, symbol: str, target_date: str, lookback_days: int = 60) -> KlineWindow:
        start_date = self.kline_service.get_start_date_by_trading_days(target_date, lookback_days)
        rows = self.kline_service.tdengine.get_daily_kline(symbol, start_date, target_date) or []
        return KlineWindow(
            symbol=symbol,
            target_date=target_date,
            lookback_days=lookback_days,
            rows=tuple(rows),
            notes=(
                f"Loaded {len(rows)} existing rows from TDengine without triggering incremental sync.",
            ),
        )


class FactorComputationService:
    """Computes the stock_extra style factor payload from a real kline window."""

    def __init__(self, chip_runner: Any | None = None) -> None:
        self._chip_runner = chip_runner

    @property
    def chip_runner(self) -> Any:
        if self._chip_runner is None:
            self._chip_runner = get_shared_chip_batch_runner()
        return self._chip_runner

    def compute(self, symbol: str, kline_window: KlineWindow, target_date: str) -> FactorResult:
        rows = list(kline_window.rows)
        if len(rows) < 35:
            return FactorResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=(f"Kline history too short for factors: {len(rows)} rows.",),
            )

        df = pd.DataFrame(rows).rename(columns={"time": "date"})
        payload = self.chip_runner.calculate_extra_factors(symbol, df)
        if not payload:
            return FactorResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=("ChipBatchRunner.calculate_extra_factors returned empty payload.",),
            )

        payload = dict(payload)
        payload["trade_date"] = target_date
        payload["date"] = target_date
        payload["ts"] = f"{target_date} 00:00:00"
        payload["symbol"] = symbol
        payload["source"] = "chip_batch_runner"
        payload["profit_ratio"] = float(payload.get("profit_ratio", 0.0) or 0.0)
        payload["concentration"] = float(payload.get("concentration", 0.0) or 0.0)
        payload["avg_cost"] = float(payload.get("avg_cost", 0.0) or 0.0)
        return FactorResult(
            symbol=symbol,
            trade_date=target_date,
            ready=True,
            payload=payload,
            notes=("Factor payload follows cache:stock_extra:{date} consumption fields.",),
        )


class ChipComputationService:
    """Computes the lightweight chip peak payload from a real kline window."""

    def __init__(self, chip_runner: Any | None = None) -> None:
        self._chip_runner = chip_runner

    @property
    def chip_runner(self) -> Any:
        if self._chip_runner is None:
            self._chip_runner = get_shared_chip_batch_runner()
        return self._chip_runner

    def compute(self, symbol: str, kline_window: KlineWindow, target_date: str) -> ChipResult:
        rows = list(kline_window.rows)
        if len(rows) < 5:
            return ChipResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=(f"Kline history too short for chip peak: {len(rows)} rows.",),
            )

        df = pd.DataFrame(rows).rename(columns={"time": "date"})
        payload = self.chip_runner.calculate_chip_peak(df)
        if not payload:
            return ChipResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=("ChipBatchRunner.calculate_chip_peak returned empty payload.",),
            )

        payload = dict(payload)
        payload["trade_date"] = target_date
        payload["date"] = target_date
        payload["ts"] = f"{target_date} 00:00:00"
        payload["symbol"] = symbol
        payload["source"] = "chip_batch_runner"
        return ChipResult(
            symbol=symbol,
            trade_date=target_date,
            ready=True,
            payload=payload,
            notes=("Chip payload follows cache:chip_peaks:{date} lightweight storage.",),
        )


class DdeSyncService:
    """Real DDE fetch-and-persist path aligned with engine_v2._sync_stock_dde."""

    def __init__(self, stock_analyzer: Any | None = None, tdengine_service: Any | None = None) -> None:
        self._stock_analyzer = stock_analyzer
        self._tdengine = tdengine_service

    @property
    def stock_analyzer(self) -> Any:
        if self._stock_analyzer is None:
            sink = StringIO()
            with redirect_stdout(sink), redirect_stderr(sink):
                from ai.API.StockAnalyzer import StockAnalyzer

                self._stock_analyzer = StockAnalyzer()
        return self._stock_analyzer

    @property
    def tdengine(self) -> Any:
        if self._tdengine is None:
            from web.services.tdengine_service import TDengineService

            self._tdengine = TDengineService()
        return self._tdengine

    def sync(self, symbol: str, target_date: str, persist: bool = True) -> DdeResult:
        target_compact = target_date.replace("-", "")
        source_payload = self.stock_analyzer.get_his_stock_dde(symbol.split(".")[0], target_compact)
        if not source_payload:
            return DdeResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=("StockAnalyzer.get_his_stock_dde returned empty payload.",),
            )

        data: dict[str, Any] = {}
        mapping = {"DDJE": "ddje", "Date": "date", "DDX": "ddx", "DDY": "ddy", "DDZ": "ddz"}
        for api_key, db_key in mapping.items():
            if api_key in source_payload:
                data[db_key] = source_payload[api_key]
            elif api_key.lower() in source_payload:
                data[db_key] = source_payload[api_key.lower()]

        if not data or "date" not in data:
            rows = source_payload.get("data", []) if isinstance(source_payload, dict) else source_payload
            if isinstance(rows, Sequence) and rows:
                data = {
                    db_key: [row.get(api_key, row.get(db_key, 0)) for row in rows] for api_key, db_key in mapping.items()
                }

        if not data or "date" not in data or not data["date"]:
            return DdeResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=("DDE payload missing date column after normalization.",),
            )

        df = pd.DataFrame(data).head(20).fillna(0)
        try:
            latest_in_payload = pd.to_datetime(df["date"]).max().strftime("%Y-%m-%d")
        except Exception as exc:
            return DdeResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=(f"Unable to parse DDE payload date column: {exc}",),
            )

        if latest_in_payload < target_date:
            return DdeResult(
                symbol=symbol,
                trade_date=target_date,
                ready=False,
                payload=None,
                notes=(f"DDE payload is stale: {latest_in_payload} < {target_date}.",),
            )

        for col in ("ddje", "ddx", "ddy", "ddz"):
            if col not in df.columns:
                df[col] = 0.0

        saved = True if not persist else bool(self.tdengine.save_daily_dde(symbol, df))
        payload = {
            "trade_date": target_date,
            "symbol": symbol,
            "ddje": float(df.iloc[0]["ddje"]),
            "ddx": float(df.iloc[0]["ddx"]),
            "ddy": float(df.iloc[0]["ddy"]),
            "ddz": float(df.iloc[0]["ddz"]),
            "source": "kaipan",
        }
        return DdeResult(
            symbol=symbol,
            trade_date=target_date,
            ready=bool(saved),
            payload=payload if saved else None,
            notes=("DDE payload normalized from historical source and written to TDengine.",),
        )


class IntegratedSyncExecutor:
    """Server-only atomic sync path for kline + dde + factors + chip peaks."""

    def __init__(
        self,
        *,
        environment: ExecutionEnvironment = ExecutionEnvironment.SERVER,
        write_enabled: bool = True,
        max_batch_workers: int = 4,
        watermark_audit_service: WatermarkAuditService | None = None,
        kline_provider: KlineWindowProvider | None = None,
        dde_service: DdeSyncService | None = None,
        factor_service: FactorComputationService | None = None,
        chip_service: ChipComputationService | None = None,
        tdengine_service: Any | None = None,
        redis_client: Any | None = None,
    ) -> None:
        self.environment = environment
        self.write_enabled = write_enabled
        self.max_batch_workers = max(1, int(max_batch_workers or 1))
        self._watermark_audit_service = watermark_audit_service or WatermarkAuditService(tdengine_service=tdengine_service)
        self._kline_provider = kline_provider or KlineWindowProvider()
        self._dde_service = dde_service or DdeSyncService(tdengine_service=tdengine_service)
        self._factor_service = factor_service or FactorComputationService()
        self._chip_service = chip_service or ChipComputationService()
        self._tdengine = tdengine_service
        self._redis_client = redis_client

    @property
    def tdengine(self) -> Any:
        if self._tdengine is None:
            from web.services.tdengine_service import TDengineService

            self._tdengine = TDengineService()
        return self._tdengine

    @property
    def redis(self) -> Any:
        if self._redis_client is None:
            import redis as redis_lib

            self._redis_client = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        return self._redis_client

    @property
    def watermark_audit_service(self) -> WatermarkAuditService:
        return self._watermark_audit_service

    def resolve_formal_target_date(self, now: datetime, target_date: str, previous_trade_date: str) -> str:
        availability = check_baostock_daily_kline_availability(
            now=now,
            target_date=target_date,
            previous_trade_date=previous_trade_date,
        )
        return target_date if availability.ready else availability.fallback_trade_date or previous_trade_date

    def _redis_hash_exists(self, key: str, field: str) -> bool:
        try:
            return bool(self.redis.hexists(key, field))
        except Exception:
            return False

    def _write_redis_hash_json(self, key: str, field: str, payload: dict[str, Any]) -> None:
        if not self.write_enabled:
            return
        self.redis.hset(key, field, json.dumps(payload, ensure_ascii=False))

    def _write_trimmed_kline_cache(self, symbol: str, target_date: str, kline_window: KlineWindow) -> bool:
        if not kline_window.rows:
            return False
        latest = dict(kline_window.rows[-1])
        latest.setdefault("trade_date", target_date)
        latest.setdefault("symbol", symbol)
        latest.setdefault("source", "baostock")
        trimmed = trim_record_to_fields(latest, get_storage_schema_spec("daily_kline").runtime_cache_view_fields)
        self._write_redis_hash_json(f"cache:kline_ready:{target_date}", symbol, trimmed)
        return True

    def _persist_deferred_kline(self, payload: DeferredKlinePersistence) -> IntegratedSyncResult:
        saved = self._kline_provider.kline_service.persist_ready_plan(
            payload.symbol,
            payload.target_date,
            payload.plan,
        )
        wrote_tdengine = ["daily_kline"] if saved and payload.plan.fetched_rows else []
        notes = [] if saved else ["Deferred kline persistence failed."]
        return self._build_result(
            symbol=payload.symbol,
            target_date=payload.target_date,
            kline_ready=bool(payload.plan.window_rows and len(payload.plan.window_rows) >= 35),
            dde_ready=False,
            factor_ready=False,
            chip_ready=False,
            factor_cache_ready=False,
            wrote_tdengine=wrote_tdengine,
            wrote_redis=[],
            notes=notes,
        )

    def _persist_factor_payload(self, symbol: str, target_date: str, payload: dict[str, Any]) -> bool:
        df = pd.DataFrame([payload])
        saved = True if not self.write_enabled else bool(self.tdengine.save_factors(symbol, df))
        if saved:
            trimmed = trim_record_to_fields(payload, get_storage_schema_spec("daily_factors").runtime_cache_view_fields)
            self._write_redis_hash_json(f"cache:stock_extra:{target_date}", symbol, trimmed)
        return bool(saved)

    def _persist_chip_payload(self, symbol: str, target_date: str, payload: dict[str, Any]) -> bool:
        saved = True if not self.write_enabled else bool(self.tdengine.save_chips(symbol, payload))
        if saved:
            trimmed = trim_record_to_fields(payload, get_storage_schema_spec("chip_peaks").runtime_cache_view_fields)
            self._write_redis_hash_json(f"cache:chip_peaks:{target_date}", symbol, trimmed)
        return bool(saved)

    def _persist_dde_payload(self, symbol: str, target_date: str, payload: dict[str, Any]) -> None:
        trimmed = trim_record_to_fields(payload, get_storage_schema_spec("daily_dde").runtime_cache_view_fields)
        self._write_redis_hash_json(f"cache:dde_ready:{target_date}", symbol, trimmed)

    def _build_result(
        self,
        *,
        symbol: str,
        target_date: str,
        kline_ready: bool,
        dde_ready: bool,
        factor_ready: bool,
        chip_ready: bool,
        factor_cache_ready: bool,
        wrote_tdengine: list[str],
        wrote_redis: list[str],
        notes: list[str],
    ) -> IntegratedSyncResult:
        return IntegratedSyncResult(
            symbol=symbol,
            target_date=target_date,
            kline_ready=kline_ready,
            dde_ready=dde_ready,
            factor_ready=factor_ready,
            chip_ready=chip_ready,
            redis_cache_ready=bool(factor_cache_ready and chip_ready),
            wrote_tdengine=tuple(dict.fromkeys(wrote_tdengine)),
            wrote_redis=tuple(dict.fromkeys(wrote_redis)),
            notes=tuple(notes or ("Integrated sync completed without extra warnings.",)),
        )

    def _checkpoint_key(self, target_date: str) -> str:
        return f"engine_next:settlement:checkpoint:{target_date}"

    def _load_checkpoint_state(self, target_date: str) -> ResumeCheckpointState:
        try:
            members = self.redis.smembers(self._checkpoint_key(target_date)) or set()
            completed = tuple(str(member) for member in members if str(member))
            return ResumeCheckpointState(
                task_type="integrated_sync",
                date_tag=target_date,
                completed_task_ids=completed,
            )
        except Exception:
            return ResumeCheckpointState(
                task_type="integrated_sync",
                date_tag=target_date,
                completed_task_ids=(),
            )

    def _mark_checkpoint_complete(self, task_id: str, target_date: str) -> None:
        if not task_id:
            return
        try:
            key = self._checkpoint_key(target_date)
            self.redis.sadd(key, task_id)
            self.redis.expire(key, 3 * 24 * 60 * 60)
        except Exception:
            return

    def _clear_checkpoint_task(self, task_id: str, target_date: str) -> None:
        if not task_id:
            return
        try:
            self.redis.srem(self._checkpoint_key(target_date), task_id)
        except Exception:
            return

    def _build_task(
        self,
        *,
        symbol: str,
        target_date: str,
        watermark_snapshot: WatermarkSnapshot,
        analytics_set: set[str],
    ) -> IntegratedSyncTask:
        latest_kline = watermark_snapshot.kline_latest_dates.get(symbol)
        latest_dde = watermark_snapshot.dde_latest_dates.get(symbol)
        latest_factor = watermark_snapshot.factor_latest_dates.get(symbol)
        factor_cache_ready = self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol)
        chip_ready = self._redis_hash_exists(f"cache:chip_peaks:{target_date}", symbol)
        return IntegratedSyncTask(
            task_id=f"{target_date}:{symbol}",
            symbol=symbol,
            target_date=target_date,
            needs_network_kline=not bool(latest_kline and latest_kline >= target_date),
            needs_network_dde=not bool(latest_dde and latest_dde >= target_date),
            needs_factor=symbol in analytics_set and not bool(latest_factor and latest_factor >= target_date and factor_cache_ready),
            needs_chip=symbol in analytics_set and not chip_ready,
        )

    def _run_network_task(
        self,
        task: IntegratedSyncTask,
        watermark_snapshot: WatermarkSnapshot,
    ) -> tuple[NetworkTaskResult, DeferredKlinePersistence | None]:
        symbol = task.symbol
        target_date = task.target_date
        latest_kline = watermark_snapshot.kline_latest_dates.get(symbol)
        latest_dde = watermark_snapshot.dde_latest_dates.get(symbol)
        latest_factor = watermark_snapshot.factor_latest_dates.get(symbol)
        factor_cache_ready = self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol)
        chip_ready = self._redis_hash_exists(f"cache:chip_peaks:{target_date}", symbol)
        dde_cache_ready = self._redis_hash_exists(f"cache:dde_ready:{target_date}", symbol)

        wrote_tdengine: list[str] = []
        wrote_redis: list[str] = []
        notes: list[str] = []
        kline_window: KlineWindow | None = None
        deferred_kline_persist: DeferredKlinePersistence | None = None
        kline_ready = bool(latest_kline and latest_kline >= target_date)
        dde_ready = bool(latest_dde and latest_dde >= target_date)
        factor_ready = bool(latest_factor and latest_factor >= target_date)

        if task.needs_network_kline:
            kline_window, kline_plan = self._kline_provider.ensure_kline_ready_plan(
                symbol,
                target_date,
                lookback_days=60,
                latest_local=latest_kline,
            )
            kline_ready = bool(kline_window.rows and len(kline_window.rows) >= 35)
            if kline_ready and self._write_trimmed_kline_cache(symbol, target_date, kline_window):
                wrote_redis.append(f"cache:kline_ready:{target_date}")
            if kline_ready and kline_plan.fetched_rows:
                deferred_kline_persist = DeferredKlinePersistence(
                    symbol=symbol,
                    target_date=target_date,
                    plan=kline_plan,
                )
                notes.append("Kline persistence deferred behind network fetch.")
            elif kline_ready:
                wrote_tdengine.append("daily_kline")
            else:
                notes.append("Kline window is still not ready after ensure_kline_ready.")
        elif task.needs_factor or task.needs_chip:
            existing_window = self._kline_provider.load_existing_window(symbol, target_date, lookback_days=60)
            if existing_window.rows:
                kline_window = existing_window
                if len(existing_window.rows) >= 35:
                    kline_ready = True

        if task.needs_network_dde:
            dde_result = self._dde_service.sync(symbol, target_date, persist=self.write_enabled)
            dde_ready = dde_result.ready
            if dde_result.ready and dde_result.payload:
                wrote_tdengine.append("daily_dde")
                self._persist_dde_payload(symbol, target_date, dde_result.payload)
                wrote_redis.append(f"cache:dde_ready:{target_date}")
            else:
                notes.extend(dde_result.notes)
        elif dde_cache_ready:
            notes.append("DDE watermark and Redis cache are already ready.")

        result = self._build_result(
            symbol=symbol,
            target_date=target_date,
            kline_ready=kline_ready,
            dde_ready=dde_ready,
            factor_ready=factor_ready,
            chip_ready=chip_ready,
            factor_cache_ready=factor_cache_ready,
            wrote_tdengine=wrote_tdengine,
            wrote_redis=wrote_redis,
            notes=notes,
        )
        return NetworkTaskResult(task=task, result=result, kline_window=kline_window), deferred_kline_persist

    def _run_analytics_task(
        self,
        task: IntegratedSyncTask,
        network_result: NetworkTaskResult,
    ) -> IntegratedSyncResult:
        symbol = task.symbol
        target_date = task.target_date
        base = network_result.result
        kline_window = network_result.kline_window
        if kline_window is None or not kline_window.rows:
            existing_window = self._kline_provider.load_existing_window(symbol, target_date, lookback_days=60)
            if existing_window.rows:
                kline_window = existing_window

        wrote_tdengine = list(base.wrote_tdengine)
        wrote_redis = list(base.wrote_redis)
        notes = list(base.notes)
        factor_ready = base.factor_ready
        chip_ready = base.chip_ready
        factor_cache_ready = self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol)

        if task.needs_factor and kline_window and kline_window.rows:
            factor_result = self._factor_service.compute(symbol, kline_window, target_date)
            if factor_result.ready and factor_result.payload:
                factor_ready = self._persist_factor_payload(symbol, target_date, factor_result.payload)
                factor_cache_ready = factor_ready
                if factor_ready:
                    wrote_tdengine.append("daily_factors")
                    wrote_redis.append(f"cache:stock_extra:{target_date}")
            else:
                notes.extend(factor_result.notes)

        if task.needs_chip and kline_window and kline_window.rows:
            chip_result = self._chip_service.compute(symbol, kline_window, target_date)
            if chip_result.ready and chip_result.payload:
                chip_ready = self._persist_chip_payload(symbol, target_date, chip_result.payload)
                if chip_ready:
                    wrote_tdengine.append("chip_peaks")
                    wrote_redis.append(f"cache:chip_peaks:{target_date}")
            else:
                notes.extend(chip_result.notes)

        return self._build_result(
            symbol=symbol,
            target_date=target_date,
            kline_ready=base.kline_ready,
            dde_ready=base.dde_ready,
            factor_ready=factor_ready,
            chip_ready=chip_ready,
            factor_cache_ready=factor_cache_ready,
            wrote_tdengine=wrote_tdengine,
            wrote_redis=wrote_redis,
            notes=notes,
        )

    @staticmethod
    def _merge_results(
        symbol: str,
        target_date: str,
        primary: IntegratedSyncResult | None,
        secondary: IntegratedSyncResult | None,
    ) -> IntegratedSyncResult:
        left = primary
        right = secondary
        if left is None and right is None:
            return IntegratedSyncResult(
                symbol=symbol,
                target_date=target_date,
                kline_ready=False,
                dde_ready=False,
                factor_ready=False,
                chip_ready=False,
                redis_cache_ready=False,
                wrote_tdengine=(),
                wrote_redis=(),
                notes=("Integrated sync produced no result.",),
            )
        if left is None:
            return right
        if right is None:
            return left
        return IntegratedSyncResult(
            symbol=symbol,
            target_date=target_date,
            kline_ready=bool(left.kline_ready or right.kline_ready),
            dde_ready=bool(left.dde_ready or right.dde_ready),
            factor_ready=bool(left.factor_ready or right.factor_ready),
            chip_ready=bool(left.chip_ready or right.chip_ready),
            redis_cache_ready=bool(left.redis_cache_ready or right.redis_cache_ready),
            wrote_tdengine=tuple(dict.fromkeys(left.wrote_tdengine + right.wrote_tdengine)),
            wrote_redis=tuple(dict.fromkeys(left.wrote_redis + right.wrote_redis)),
            notes=tuple(dict.fromkeys(left.notes + right.notes)),
        )

    def _build_observed_result(
        self,
        *,
        symbol: str,
        target_date: str,
        watermark_snapshot: WatermarkSnapshot,
    ) -> IntegratedSyncResult:
        factor_cache_ready = self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol)
        chip_cache_ready = self._redis_hash_exists(f"cache:chip_peaks:{target_date}", symbol)
        dde_cache_ready = self._redis_hash_exists(f"cache:dde_ready:{target_date}", symbol)
        return self._build_result(
            symbol=symbol,
            target_date=target_date,
            kline_ready=bool(
                watermark_snapshot.kline_latest_dates.get(symbol)
                and watermark_snapshot.kline_latest_dates.get(symbol) >= target_date
            ),
            dde_ready=bool(
                (watermark_snapshot.dde_latest_dates.get(symbol) and watermark_snapshot.dde_latest_dates.get(symbol) >= target_date)
                or dde_cache_ready
            ),
            factor_ready=bool(
                watermark_snapshot.factor_latest_dates.get(symbol)
                and watermark_snapshot.factor_latest_dates.get(symbol) >= target_date
            ),
            chip_ready=chip_cache_ready,
            factor_cache_ready=factor_cache_ready,
            wrote_tdengine=[],
            wrote_redis=[],
            notes=[],
        )

    def _is_checkpoint_ready_for_task(
        self,
        task: IntegratedSyncTask,
        result: IntegratedSyncResult,
    ) -> bool:
        if task.needs_network_kline and not result.kline_ready:
            return False
        if task.needs_network_dde and not result.dde_ready:
            return False
        if task.needs_factor and not result.factor_ready:
            return False
        if task.needs_chip and not result.chip_ready:
            return False
        if task.needs_factor and not self._redis_hash_exists(f"cache:stock_extra:{task.target_date}", task.symbol):
            return False
        if task.needs_chip and not self._redis_hash_exists(f"cache:chip_peaks:{task.target_date}", task.symbol):
            return False
        if task.needs_network_dde and not self._redis_hash_exists(f"cache:dde_ready:{task.target_date}", task.symbol):
            return False
        return True

    def sync_pipeline(
        self,
        *,
        target_symbols: Sequence[str],
        network_symbols: Sequence[str],
        analytics_symbols: Sequence[str],
        target_date: str,
        watermark_snapshot: WatermarkSnapshot,
        analytics_workers: int | None = None,
    ) -> list[IntegratedSyncResult]:
        ordered_targets = tuple(dict.fromkeys(str(symbol) for symbol in target_symbols if str(symbol)))
        if not ordered_targets:
            return []

        ordered_target_set = set(ordered_targets)
        network_set = {str(symbol) for symbol in network_symbols if str(symbol) in ordered_target_set}
        analytics_set = {str(symbol) for symbol in analytics_symbols if str(symbol) in ordered_target_set}
        total = len(ordered_targets)
        network_total = len(network_set)
        analytics_total = len(analytics_set)
        analytics_worker_count = max(1, int(analytics_workers or self.max_batch_workers or 1))
        network_worker_count = min(max(1, int(self.max_batch_workers or 1)), network_total) if network_total else 0
        analytics_queue_limit = min(
            total,
            max(8, analytics_worker_count * 2, network_worker_count or 1),
        )
        persistence_queue_limit = min(
            total,
            max(4, network_worker_count or 1),
        )
        checkpoint_state = self._load_checkpoint_state(target_date)
        completed_checkpoint_ids = set(checkpoint_state.completed_task_ids)

        results_by_symbol: dict[str, IntegratedSyncResult] = {}
        network_results_by_symbol: dict[str, NetworkTaskResult] = {}
        completed = 0
        network_done = 0
        analytics_done = 0
        started_at = time.monotonic()
        network_elapsed_total = 0.0
        network_timed_count = 0
        analytics_elapsed_total = 0.0
        analytics_timed_count = 0
        deferred_local_analytics: list[str] = []
        analytics_queue: queue.Queue[NetworkTaskResult | None] = queue.Queue(maxsize=analytics_queue_limit)
        persistence_queue: queue.Queue[DeferredKlinePersistence | None] = queue.Queue(maxsize=persistence_queue_limit)
        progress_lock = threading.Lock()
        peak_queue_depth = 0
        analytics_completed_symbols: set[str] = set()
        persistence_required_symbols: set[str] = set()
        persistence_completed_symbols: set[str] = set()
        finalized_symbols: set[str] = set()
        last_progress_signature: tuple[int, int, int] | None = None
        last_progress_emit_at = 0.0
        checkpoint_reused = 0
        checkpoint_invalidated = 0
        checkpoint_stored = 0

        def _build_task_for_symbol(symbol: str) -> IntegratedSyncTask:
            return self._build_task(
                symbol=symbol,
                target_date=target_date,
                watermark_snapshot=watermark_snapshot,
                analytics_set=analytics_set,
            )

        def _try_finalize_symbol(symbol: str) -> None:
            nonlocal completed, checkpoint_stored
            if symbol in results_by_symbol and symbol not in finalized_symbols:
                analytics_ready = symbol not in analytics_set or symbol in analytics_completed_symbols
                persistence_ready = symbol not in persistence_required_symbols or symbol in persistence_completed_symbols
                if analytics_ready and persistence_ready:
                    task = _build_task_for_symbol(symbol)
                    final_result = results_by_symbol[symbol]
                    finalized_symbols.add(symbol)
                    completed += 1
                    task_id = f"{target_date}:{symbol}"
                    if self._is_checkpoint_ready_for_task(task, final_result):
                        self._mark_checkpoint_complete(task_id, target_date)
                        checkpoint_stored += 1
                    else:
                        self._clear_checkpoint_task(task_id, target_date)

        def _render_progress_if_changed(force: bool = False) -> None:
            nonlocal last_progress_signature, last_progress_emit_at
            extra = _progress_extra()
            percent_bucket = int((completed / max(total, 1)) * 20)
            signature = (
                percent_bucket,
                int(completed >= total),
                int(network_done >= network_total) if network_total > 0 else 1,
                int(analytics_done >= analytics_total) if analytics_total > 0 else 1,
            )
            now_monotonic = time.monotonic()
            if force and signature == last_progress_signature and completed >= total:
                return
            if (
                not force
                and signature == last_progress_signature
                and (now_monotonic - last_progress_emit_at) < HEARTBEAT_INTERVAL_SECONDS
            ):
                return
            last_progress_signature = signature
            last_progress_emit_at = now_monotonic
            _render_pipeline_progress(
                completed,
                total,
                network_done=network_done,
                network_total=network_total,
                analytics_done=analytics_done,
                analytics_total=analytics_total,
                extra=extra,
            )

        print(
            f"[settlement] 断点续跑 | 已存={len(completed_checkpoint_ids)} | 复用=0 | 失效=0 | 新提交=0"
        )

        def _complete_symbol(symbol: str, analytics_result: IntegratedSyncResult | None = None) -> None:
            nonlocal analytics_done
            merged = self._merge_results(
                symbol,
                target_date,
                network_results_by_symbol.get(symbol).result if symbol in network_results_by_symbol else None,
                analytics_result,
            )
            results_by_symbol[symbol] = merged
            if analytics_result is not None and symbol in analytics_set:
                analytics_done += 1
                analytics_completed_symbols.add(symbol)
            _try_finalize_symbol(symbol)

        def _progress_extra() -> str:
            avg_net_ms = 0
            avg_calc_ms = 0
            if network_timed_count:
                avg_net_ms = max(1, round((network_elapsed_total / network_timed_count) * 1000)) if network_elapsed_total > 0 else 0
            if analytics_timed_count:
                avg_calc_ms = max(1, round((analytics_elapsed_total / analytics_timed_count) * 1000)) if analytics_elapsed_total > 0 else 0
            return f"队{analytics_queue.qsize()}/{analytics_queue_limit} | 峰{peak_queue_depth} | {avg_net_ms}/{avg_calc_ms}ms"

        def _enqueue_analytics(network_result: NetworkTaskResult) -> None:
            nonlocal peak_queue_depth
            analytics_queue.put(network_result)
            current_depth = analytics_queue.qsize()
            if current_depth > peak_queue_depth:
                peak_queue_depth = current_depth

        def _persistence_worker() -> None:
            while True:
                item = persistence_queue.get()
                if item is None:
                    persistence_queue.task_done()
                    return
                persistence_result = self._persist_deferred_kline(item)
                with progress_lock:
                    existing = network_results_by_symbol.get(item.symbol)
                    if existing is not None:
                        merged_network_result = NetworkTaskResult(
                            task=existing.task,
                            result=self._merge_results(item.symbol, item.target_date, existing.result, persistence_result),
                            kline_window=existing.kline_window,
                        )
                        network_results_by_symbol[item.symbol] = merged_network_result
                        if item.symbol in results_by_symbol:
                            results_by_symbol[item.symbol] = self._merge_results(
                                item.symbol,
                                item.target_date,
                                results_by_symbol[item.symbol],
                                persistence_result,
                            )
                    persistence_completed_symbols.add(item.symbol)
                    _try_finalize_symbol(item.symbol)
                    _render_progress_if_changed()
                persistence_queue.task_done()

        def _build_local_only_network_result(symbol: str) -> NetworkTaskResult:
            task = self._build_task(
                symbol=symbol,
                target_date=target_date,
                watermark_snapshot=watermark_snapshot,
                analytics_set=analytics_set,
            )
            return NetworkTaskResult(
                task=task,
                result=self._build_result(
                    symbol=symbol,
                    target_date=target_date,
                    kline_ready=bool(
                        watermark_snapshot.kline_latest_dates.get(symbol)
                        and watermark_snapshot.kline_latest_dates.get(symbol) >= target_date
                    ),
                    dde_ready=bool(
                        watermark_snapshot.dde_latest_dates.get(symbol)
                        and watermark_snapshot.dde_latest_dates.get(symbol) >= target_date
                    ),
                    factor_ready=bool(
                        watermark_snapshot.factor_latest_dates.get(symbol)
                        and watermark_snapshot.factor_latest_dates.get(symbol) >= target_date
                    ),
                    chip_ready=self._redis_hash_exists(f"cache:chip_peaks:{target_date}", symbol),
                    factor_cache_ready=self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol),
                    wrote_tdengine=[],
                    wrote_redis=[],
                    notes=[],
                ),
                kline_window=None,
            )

        def _analytics_worker() -> None:
            nonlocal analytics_elapsed_total, analytics_timed_count
            while True:
                item = analytics_queue.get()
                if item is None:
                    analytics_queue.task_done()
                    return
                analytics_started = time.monotonic()
                analytics_result = self._run_analytics_task(item.task, item)
                analytics_elapsed = time.monotonic() - analytics_started
                with progress_lock:
                    analytics_elapsed_total += analytics_elapsed
                    analytics_timed_count += 1
                    _complete_symbol(item.task.symbol, analytics_result=analytics_result)
                    _render_progress_if_changed()
                analytics_queue.task_done()

        def _run_network_task_timed(task: IntegratedSyncTask) -> tuple[NetworkTaskResult, DeferredKlinePersistence | None, float]:
            network_started = time.monotonic()
            network_result, deferred_kline_persist = self._run_network_task(task, watermark_snapshot)
            return network_result, deferred_kline_persist, time.monotonic() - network_started

        def _handle_network_completion(
            network_result: NetworkTaskResult,
            deferred_kline_persist: DeferredKlinePersistence | None,
            network_elapsed: float,
        ) -> None:
            nonlocal network_done, network_elapsed_total, network_timed_count, last_progress_signature
            symbol = network_result.task.symbol
            with progress_lock:
                network_results_by_symbol[symbol] = network_result
                network_done += 1
                network_elapsed_total += network_elapsed
                network_timed_count += 1
                if deferred_kline_persist is not None:
                    persistence_required_symbols.add(symbol)
                if symbol not in analytics_set:
                    _complete_symbol(symbol)
                extra = _progress_extra()
                elapsed = int(time.monotonic() - started_at)
                if completed == 0 and elapsed >= STALL_WARNING_SECONDS:
                    last_progress_signature = None
                    _render_pipeline_progress(
                        completed,
                        total,
                        network_done=network_done,
                        network_total=network_total,
                        analytics_done=analytics_done,
                        analytics_total=analytics_total,
                        extra=f"{extra} | 首个结果等待={elapsed}s",
                    )
                else:
                    _render_progress_if_changed()
            if symbol in analytics_set:
                _enqueue_analytics(network_result)
            if deferred_kline_persist is not None:
                persistence_queue.put(deferred_kline_persist)

        _render_pipeline_progress(
            0,
            total,
            network_done=0,
            network_total=network_total,
            analytics_done=0,
            analytics_total=analytics_total,
            extra=_progress_extra(),
        )

        analytics_threads = [
            threading.Thread(
                target=_analytics_worker,
                name=f"integrated-sync-analytics-{idx}",
                daemon=True,
            )
            for idx in range(analytics_worker_count)
        ]
        for thread in analytics_threads:
            thread.start()
        persistence_thread = threading.Thread(
            target=_persistence_worker,
            name="integrated-sync-kline-persist",
            daemon=True,
        )
        persistence_thread.start()

        try:
            pending_network_futures = set()
            reused_network_symbols: set[str] = set()
            for symbol in ordered_targets:
                task_id = f"{target_date}:{symbol}"
                if task_id in completed_checkpoint_ids:
                    task = _build_task_for_symbol(symbol)
                    observed_result = self._build_observed_result(
                        symbol=symbol,
                        target_date=target_date,
                        watermark_snapshot=watermark_snapshot,
                    )
                    if not self._is_checkpoint_ready_for_task(task, observed_result):
                        checkpoint_invalidated += 1
                        self._clear_checkpoint_task(task_id, target_date)
                    else:
                        checkpoint_reused += 1
                        with progress_lock:
                            checkpoint_result = self._build_result(
                                symbol=symbol,
                                target_date=target_date,
                                kline_ready=observed_result.kline_ready,
                                dde_ready=observed_result.dde_ready,
                                factor_ready=observed_result.factor_ready,
                                chip_ready=observed_result.chip_ready,
                                factor_cache_ready=self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol),
                                wrote_tdengine=[],
                                wrote_redis=[],
                                notes=["resumed_from_checkpoint"],
                            )
                            results_by_symbol[symbol] = checkpoint_result
                            if symbol in network_set:
                                network_done += 1
                                reused_network_symbols.add(symbol)
                            if symbol in analytics_set:
                                analytics_done += 1
                                analytics_completed_symbols.add(symbol)
                            persistence_completed_symbols.add(symbol)
                            _try_finalize_symbol(symbol)
                            _render_progress_if_changed()
                        continue

                if symbol not in network_set:
                    if symbol in analytics_set:
                        deferred_local_analytics.append(symbol)
                    continue

            for symbol in deferred_local_analytics:
                _enqueue_analytics(_build_local_only_network_result(symbol))

            if network_worker_count:
                with ThreadPoolExecutor(
                    max_workers=network_worker_count,
                    thread_name_prefix="integrated-sync-network",
                ) as network_executor:
                    for symbol in ordered_targets:
                        if (
                            symbol in network_set
                            and symbol not in network_results_by_symbol
                            and symbol not in reused_network_symbols
                        ):
                            task = self._build_task(
                                symbol=symbol,
                                target_date=target_date,
                                watermark_snapshot=watermark_snapshot,
                                analytics_set=analytics_set,
                            )
                            pending_network_futures.add(network_executor.submit(_run_network_task_timed, task))

                    while pending_network_futures:
                        done, pending_network_futures = wait(
                            pending_network_futures,
                            timeout=1.0,
                            return_when=FIRST_COMPLETED,
                        )
                        if not done:
                            with progress_lock:
                                elapsed = int(time.monotonic() - started_at)
                                if completed == 0 and elapsed >= STALL_WARNING_SECONDS:
                                    last_progress_signature = None
                                    _render_pipeline_progress(
                                        completed,
                                        total,
                                        network_done=network_done,
                                        network_total=network_total,
                                        analytics_done=analytics_done,
                                        analytics_total=analytics_total,
                                        extra=f"{_progress_extra()} | 首个结果等待={elapsed}s",
                                    )
                            continue
                        for future in done:
                            network_result, deferred_kline_persist, network_elapsed = future.result()
                            _handle_network_completion(network_result, deferred_kline_persist, network_elapsed)

            analytics_queue.join()
            persistence_queue.join()
        finally:
            for _ in analytics_threads:
                analytics_queue.put(None)
            for thread in analytics_threads:
                thread.join(timeout=1)
            persistence_queue.put(None)
            persistence_thread.join(timeout=1)

        with progress_lock:
            _render_progress_if_changed(force=True)
        print(
            f"[settlement] 断点续跑 | 已存={len(completed_checkpoint_ids)} | 复用={checkpoint_reused} | 失效={checkpoint_invalidated} | 新提交={checkpoint_stored}"
        )

        return [results_by_symbol[symbol] for symbol in ordered_targets if symbol in results_by_symbol]

    def sync_symbol(
        self,
        symbol: str,
        target_date: str,
        watermark_snapshot: WatermarkSnapshot,
        stage: str = STAGE_FULL,
    ) -> IntegratedSyncResult:
        profile = get_default_execution_profile(self.environment)
        if not profile.allow_runtime_jobs:
            return IntegratedSyncResult(
                symbol=symbol,
                target_date=target_date,
                kline_ready=False,
                dde_ready=False,
                factor_ready=False,
                chip_ready=False,
                redis_cache_ready=False,
                wrote_tdengine=(),
                wrote_redis=(),
                notes=("Integrated sync is server-only. Local Windows must not run formal sync jobs.",),
            )

        wrote_tdengine: list[str] = []
        wrote_redis: list[str] = []
        notes: list[str] = []
        latest_kline = watermark_snapshot.kline_latest_dates.get(symbol)
        latest_dde = watermark_snapshot.dde_latest_dates.get(symbol)
        latest_factor = watermark_snapshot.factor_latest_dates.get(symbol)

        kline_ready = bool(latest_kline and latest_kline >= target_date)
        dde_ready = bool(latest_dde and latest_dde >= target_date)
        factor_ready = bool(latest_factor and latest_factor >= target_date)
        chip_ready = self._redis_hash_exists(f"cache:chip_peaks:{target_date}", symbol)
        factor_cache_ready = self._redis_hash_exists(f"cache:stock_extra:{target_date}", symbol)
        dde_cache_ready = self._redis_hash_exists(f"cache:dde_ready:{target_date}", symbol)

        kline_window: KlineWindow | None = None
        should_run_network = stage in (STAGE_NETWORK, STAGE_FULL)
        should_run_analytics = stage in (STAGE_ANALYTICS, STAGE_FULL)

        if should_run_analytics and kline_ready and (not chip_ready or not factor_ready or not factor_cache_ready):
            kline_window = self._kline_provider.load_existing_window(symbol, target_date, lookback_days=60)
            if len(kline_window.rows) < 35:
                notes.append("Existing TDengine kline window is too short; falling back to ensure_kline_ready.")
                kline_window = None

        if should_run_analytics and kline_window is None:
            existing_window = self._kline_provider.load_existing_window(symbol, target_date, lookback_days=60)
            if existing_window.rows:
                kline_window = existing_window
                if len(existing_window.rows) >= 35:
                    kline_ready = True

        if should_run_network and not kline_ready:
            logger.debug("integrated sync symbol stage | symbol=%s | stage=kline_ensure", symbol)
            kline_window = self._kline_provider.ensure_kline_ready(
                symbol,
                target_date,
                lookback_days=60,
                latest_local=latest_kline,
            )
            kline_ready = bool(kline_window.rows and len(kline_window.rows) >= 35)
            if kline_ready and self._write_trimmed_kline_cache(symbol, target_date, kline_window):
                wrote_redis.append(f"cache:kline_ready:{target_date}")
            if kline_ready and latest_kline and latest_kline >= target_date:
                notes.append("Kline watermark was already ready; reused TDengine history for downstream computation.")
            elif kline_ready:
                wrote_tdengine.append("daily_kline")
            else:
                notes.append("Kline window is still not ready after ensure_kline_ready.")

        if should_run_network and not dde_ready:
            logger.debug("integrated sync symbol stage | symbol=%s | stage=dde_sync", symbol)
            dde_result = self._dde_service.sync(symbol, target_date, persist=self.write_enabled)
            dde_ready = dde_result.ready
            if dde_result.ready and dde_result.payload:
                wrote_tdengine.append("daily_dde")
                self._persist_dde_payload(symbol, target_date, dde_result.payload)
                wrote_redis.append(f"cache:dde_ready:{target_date}")
            else:
                notes.extend(dde_result.notes)
        elif dde_cache_ready:
            notes.append("DDE watermark and Redis cache are already ready.")

        if stage == STAGE_NETWORK:
            return self._build_result(
                symbol=symbol,
                target_date=target_date,
                kline_ready=kline_ready,
                dde_ready=dde_ready,
                factor_ready=factor_ready,
                chip_ready=chip_ready,
                factor_cache_ready=factor_cache_ready,
                wrote_tdengine=wrote_tdengine,
                wrote_redis=wrote_redis,
                notes=notes,
            )

        if should_run_analytics and (not factor_ready or not factor_cache_ready) and kline_window and kline_window.rows:
            logger.debug("integrated sync symbol stage | symbol=%s | stage=factor_compute", symbol)
            factor_result = self._factor_service.compute(symbol, kline_window, target_date)
            if factor_result.ready and factor_result.payload:
                factor_ready = self._persist_factor_payload(symbol, target_date, factor_result.payload)
                factor_cache_ready = factor_ready
                if factor_ready:
                    wrote_tdengine.append("daily_factors")
                    wrote_redis.append(f"cache:stock_extra:{target_date}")
                else:
                    notes.append("TDengine save_factors returned false.")
            else:
                notes.extend(factor_result.notes)

        if should_run_analytics and not chip_ready and kline_window and kline_window.rows:
            logger.debug("integrated sync symbol stage | symbol=%s | stage=chip_compute", symbol)
            chip_result = self._chip_service.compute(symbol, kline_window, target_date)
            if chip_result.ready and chip_result.payload:
                chip_ready = self._persist_chip_payload(symbol, target_date, chip_result.payload)
                if chip_ready:
                    wrote_tdengine.append("chip_peaks")
                    wrote_redis.append(f"cache:chip_peaks:{target_date}")
                else:
                    notes.append("TDengine save_chips returned false.")
            else:
                notes.extend(chip_result.notes)

        result = self._build_result(
            symbol=symbol,
            target_date=target_date,
            kline_ready=kline_ready,
            dde_ready=dde_ready,
            factor_ready=factor_ready,
            chip_ready=chip_ready,
            factor_cache_ready=factor_cache_ready,
            wrote_tdengine=wrote_tdengine,
            wrote_redis=wrote_redis,
            notes=notes,
        )
        logger.debug(
            "integrated sync symbol done | symbol=%s | kline=%s | dde=%s | factor=%s | chip=%s | redis=%s",
            symbol,
            result.kline_ready,
            result.dde_ready,
            result.factor_ready,
            result.chip_ready,
            result.redis_cache_ready,
        )
        return result

    def sync_batch(
        self,
        symbols: Iterable[str],
        target_date: str,
        watermark_snapshot: WatermarkSnapshot,
        stage: str = STAGE_FULL,
        max_workers_override: int | None = None,
    ) -> list[IntegratedSyncResult]:
        normalized_symbols = tuple(dict.fromkeys(str(symbol) for symbol in symbols if str(symbol)))
        logger.debug(
            "integrated sync batch start | stage=%s | target_date=%s | symbols=%s",
            stage,
            target_date,
            len(normalized_symbols),
        )
        _render_stage_progress(stage, 0, len(normalized_symbols))
        if len(normalized_symbols) <= 1 or self.max_batch_workers <= 1:
            results = []
            for idx, symbol in enumerate(normalized_symbols, start=1):
                if idx == 1 or idx % PROGRESS_LOG_INTERVAL == 0 or idx == len(normalized_symbols):
                    percent = round(idx * 100.0 / len(normalized_symbols), 1)
                    logger.debug(
                        "integrated sync progress | stage=%s | done=%s/%s | pct=%s%% | current=%s",
                        stage,
                        idx,
                        len(normalized_symbols),
                        percent,
                        symbol,
                    )
                results.append(self.sync_symbol(symbol, target_date, watermark_snapshot, stage=stage))
                if idx == 1 or idx % 2 == 0 or idx == len(normalized_symbols):
                    _render_stage_progress(stage, idx, len(normalized_symbols))
            logger.debug("integrated sync batch done | stage=%s | results=%s", stage, len(results))
            return results

        worker_budget = self.max_batch_workers if max_workers_override is None else max(1, int(max_workers_override))
        max_workers = min(worker_budget, len(normalized_symbols))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            logger.debug(
                "integrated sync dispatch | stage=%s | target_date=%s | queued=%s | workers=%s",
                stage,
                target_date,
                len(normalized_symbols),
                max_workers,
            )
            futures = {
                executor.submit(self.sync_symbol, symbol, target_date, watermark_snapshot, stage): symbol
                for symbol in normalized_symbols
            }
            results_by_symbol: dict[str, IntegratedSyncResult] = {}
            completed = 0
            total = len(normalized_symbols)
            stop_heartbeat = threading.Event()
            started_at = time.monotonic()

            def _heartbeat() -> None:
                while not stop_heartbeat.wait(HEARTBEAT_INTERVAL_SECONDS):
                    running = max(total - completed, 0)
                    elapsed = int(time.monotonic() - started_at)
                    logger.debug(
                        "integrated sync heartbeat | stage=%s | done=%s/%s | running=%s",
                        stage,
                        completed,
                        total,
                        min(max_workers, running),
                    )
                    if completed == 0 and elapsed >= STALL_WARNING_SECONDS:
                        _render_stage_progress(
                            stage,
                            completed,
                            total,
                            extra=f"running={min(max_workers, running)} | waiting_first_result={elapsed}s",
                        )

            heartbeat_thread = threading.Thread(
                target=_heartbeat,
                name="integrated-sync-heartbeat",
                daemon=True,
            )
            heartbeat_thread.start()
            for future in as_completed(futures):
                symbol = futures[future]
                completed += 1
                if completed == 1 or completed % PROGRESS_LOG_INTERVAL == 0 or completed == total:
                    percent = round(completed * 100.0 / total, 1)
                    logger.debug(
                        "integrated sync progress | stage=%s | done=%s/%s | pct=%s%% | last=%s",
                        stage,
                        completed,
                        total,
                        percent,
                        symbol,
                    )
                if completed == 1 or completed % 2 == 0 or completed == total:
                    _render_stage_progress(stage, completed, total)
                results_by_symbol[symbol] = future.result()
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=1)
        results = [results_by_symbol[symbol] for symbol in normalized_symbols if symbol in results_by_symbol]
        logger.debug("integrated sync batch done | stage=%s | results=%s | workers=%s", stage, len(results), max_workers)
        return results
