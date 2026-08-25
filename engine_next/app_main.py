from __future__ import annotations

import argparse
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Iterable

from engine_next.contracts.offline_sync_contracts import IntegratedSyncResult, WatermarkSnapshot
from engine_next.domain.enums import ExecutionEnvironment, RunPhase
from engine_next.domain.models import IntradayContext, RuntimeEventSpec
from engine_next.runtime.controllers.auction_runtime_controller import AuctionRuntimeController
from engine_next.runtime.controllers.live_runtime_controller import (
    LiveRuntimeController,
    LiveRuntimeRequest,
)
from engine_next.runtime.controllers.night_recap_controller import NightRecapController
from engine_next.runtime.controllers.postmarket_runtime_controller import PostmarketRuntimeController
from engine_next.runtime.controllers.settlement_controller import SettlementController
from engine_next.runtime.controllers.startup_bootstrap_controller import (
    StartupBootstrapController,
    StartupBootstrapRequest,
)
from engine_next.runtime.intraday_data_hub import IntradayDataHub, IntradayFetchResult
from engine_next.runtime.intraday_context_builder import (
    IntradayContextBuilder,
    PrimedIntradayRuntimeState,
)
from engine_next.runtime.market_runtime_summary import MarketRuntimeSummaryResult, MarketRuntimeSummaryService
from engine_next.runtime.offline_sync_executor import OfflineSyncRequest, ServerOnlyOfflineSyncExecutor
from engine_next.runtime.production_reporting import ProductionReportingCoordinator
from engine_next.runtime.original_timeline import iter_phase_events
from engine_next.runtime.renderers.live_phase_summary_renderer import (
    LivePhaseSummaryRenderer,
    render_quote_freshness_line,
)
from engine_next.runtime.startup_runtime_coordinator import (
    RuntimeStartupCoordinator,
    StartupCoordinatorRequest,
    StartupExecutionBundle,
)
from engine_next.runtime.startup_self_check import PREMARKET_HEAVY_SYNC_CUTOFF, infer_run_phase
from web.services.trading_calendar_service import TradingCalendarService


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _normalize_symbol(value: str) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


def _is_equity_symbol(value: str) -> bool:
    code = _normalize_symbol(value)
    return bool(
        code
        and code.startswith(
            (
                "000",
                "001",
                "002",
                "003",
                "300",
                "301",
                "600",
                "601",
                "603",
                "605",
                "688",
                "689",
            )
        )
    )


def _native_ingested_count(primed_runtime_state: PrimedIntradayRuntimeState | None) -> int:
    if primed_runtime_state is None:
        return 0
    value = getattr(primed_runtime_state, "native_ingested", None)
    if value is None:
        value = getattr(primed_runtime_state, "rust_ingested", 0)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _dedupe_symbols(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            normalized
            for value in values
            for normalized in (_normalize_symbol(value),)
            if normalized and _is_equity_symbol(normalized)
        )
    )


def _default_previous_trade_date(trade_date: str) -> str:
    try:
        return TradingCalendarService().get_previous_trading_day(trade_date)
    except Exception:
        return (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")


def _infer_minute_index(now: datetime) -> int:
    return now.hour * 60 + now.minute


def _hot_plate_cache_date(now: datetime, trade_date: str, previous_trade_date: str) -> str:
    if now.time().strftime("%H:%M") < "09:15":
        return previous_trade_date
    return trade_date


def _resolve_default_request_now(
    *,
    wall_now: datetime,
    effective_trade_date: str,
    now_explicit: bool,
) -> datetime:
    if now_explicit:
        return wall_now
    target_date = str(effective_trade_date or "").strip()
    if not target_date or wall_now.strftime("%Y-%m-%d") == target_date:
        return wall_now
    return datetime.strptime(
        f"{target_date} {wall_now.strftime('%H:%M:%S')}",
        "%Y-%m-%d %H:%M:%S",
    )


def _is_historical_replay_request(
    *,
    wall_now: datetime,
    effective_trade_date: str,
    now_explicit: bool,
) -> bool:
    if now_explicit:
        return False
    return wall_now.strftime("%Y-%m-%d") != str(effective_trade_date or "").strip()


def _is_live_target_session(now: datetime, trade_date: str) -> bool:
    return now.strftime("%Y-%m-%d") == str(trade_date or "").strip()


@dataclass(frozen=True)
class EngineAppRequest:
    now: datetime
    trade_date: str
    previous_trade_date: str
    symbols: tuple[str, ...]
    environment: ExecutionEnvironment = ExecutionEnvironment.SERVER
    offline_context_date: str | None = None
    minute_index: int | None = None
    require_auction_recovery: bool = False
    watermark_snapshot: WatermarkSnapshot | None = None
    kline_watermarks: dict[str, str] | None = None
    factor_watermarks: dict[str, str] | None = None
    redis_factor_cache_ready: dict[str, bool] | None = None
    yest_limit_pool_ready: bool = False
    hot_plates_ready: bool = False
    hot_plates_today_ready: bool = False
    hot_plates_effective_ready: bool = False
    hot_plates_effective_trade_date: str = ""
    stock_plate_mapping_ready: bool = False
    auction_anchor_ready: bool = False
    redis_chip_ready_count: int = 0
    redis_dde_ready_count: int = 0
    cached_listing_dates: dict[str, str] | None = None
    cached_kline_row_counts: dict[str, int] | None = None
    cached_structural_factor_gap: dict[str, bool] | None = None
    historical_replay: bool = False
    run_integrated_sync: bool = True


@dataclass(frozen=True)
class EngineAppResult:
    phase: RunPhase
    startup_bundle: StartupExecutionBundle
    watermark_snapshot: WatermarkSnapshot
    integrated_sync_results: tuple[IntegratedSyncResult, ...]
    intraday_context: IntradayContext | None
    phase_events: tuple[RuntimeEventSpec, ...]
    loop_event: str = ""
    loop_event_label: str = ""
    lifecycle_audit_ran: bool = False
    used_cached_startup_state: bool = False
    should_render: bool = True
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeLoopDecision:
    name: str
    label: str
    audit_token: str | None
    should_run_lifecycle_audit: bool
    scheduled_event_name: str = ""
    scheduled_event_label: str = ""


@dataclass(frozen=True)
class RuntimeScheduledEventResult:
    name: str
    label: str
    executed: bool
    notes: tuple[str, ...] = ()
    yest_limit_result: IntradayFetchResult | None = None
    hot_plate_result: IntradayFetchResult | None = None
    auction_result: IntradayFetchResult | None = None
    market_runtime_summary_result: MarketRuntimeSummaryResult | None = None


@dataclass(frozen=True)
class LateStartAuctionRecoveryResult:
    status: str
    executed: bool
    notes: tuple[str, ...] = ()
    yest_limit_result: IntradayFetchResult | None = None
    hot_plate_result: IntradayFetchResult | None = None
    auction_result: IntradayFetchResult | None = None
    market_runtime_summary_result: MarketRuntimeSummaryResult | None = None


class EngineApp:
    """
    Minimal runnable orchestrator for engine_next.

    This intentionally stops at stage orchestration:
    startup audit -> allowed repairs -> optional integrated sync -> intraday context.
    Strategy execution and dense operator views are deferred to later phases.
    """

    AUTO_DISCOVERED_SYNC_LIMIT = 50
    DEFAULT_LOOP_INTERVAL_SECONDS = 30
    MIN_STOCK_PLATE_MAPPING_COUNT = 1000
    MIN_FULL_UNIVERSE_SIZE = 1000
    STARTUP_AUDIT_CHECKPOINTS = ("08:30", "09:00", "17:40")
    AUCTION_FINALIZE_EARLIEST = dt_time(9, 25, 10)
    OPENING_FACTS_EARLIEST = dt_time(9, 32, 10)

    def __init__(
        self,
        *,
        startup_coordinator: RuntimeStartupCoordinator | None = None,
        offline_executor: ServerOnlyOfflineSyncExecutor | None = None,
        intraday_context_builder: IntradayContextBuilder | None = None,
        redis_client: object | None = None,
        production_reporting: ProductionReportingCoordinator | None = None,
    ) -> None:
        self._startup_coordinator = startup_coordinator or RuntimeStartupCoordinator()
        self._offline_executor = offline_executor or ServerOnlyOfflineSyncExecutor()
        self._intraday_context_builder = intraday_context_builder or IntradayContextBuilder()
        self._redis_client = redis_client
        self._production_reporting = production_reporting
        self._intraday_hub = IntradayDataHub(redis_client=redis_client)
        self._market_runtime_summary_service = MarketRuntimeSummaryService(redis_client=self.redis)
        self._auction_runtime = AuctionRuntimeController(
            intraday_hub=self._intraday_hub,
            market_runtime_summary_service=self._market_runtime_summary_service,
        )
        self._postmarket_runtime = PostmarketRuntimeController(
            intraday_hub=self._intraday_hub,
            market_runtime_summary_service=self._market_runtime_summary_service,
            state_writer=self._safe_set,
        )
        self._startup_bootstrap = StartupBootstrapController(
            startup_coordinator=self._startup_coordinator,
            offline_executor=self._offline_executor,
            runtime_readiness_loader=self._load_runtime_readiness,
        )
        self._live_runtime = LiveRuntimeController(
            intraday_context_builder=self._intraday_context_builder,
        )
        self._settlement = SettlementController(
            offline_executor=self._offline_executor,
            auto_discovered_sync_limit=self.AUTO_DISCOVERED_SYNC_LIMIT,
            redis_client=self.redis,
        )
        self._night_recap = NightRecapController()
        self._live_summary_renderer = LivePhaseSummaryRenderer()
        self._last_scheduled_event_token: str | None = None
        self._last_render_token: str | None = None
        self._last_auction_cleanup_trade_date: str | None = None
        self._last_intraday_auction_recap_trade_date: str | None = None
        self._last_opening_validation_checkpoint_token: str | None = None
        self._last_open_2m_runtime_refresh_token: str | None = None
        self._last_late_start_auction_recovery_token: str | None = None

    @property
    def redis(self):
        if self._redis_client is None:
            import redis as redis_lib

            self._redis_client = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        return self._redis_client

    def _discover_runtime_symbols(self) -> tuple[str, ...]:
        return ()

    def _safe_hkeys(self, key: str) -> tuple[str, ...]:
        try:
            if hasattr(self.redis, "hkeys"):
                values = self.redis.hkeys(key) or []
                return tuple(str(value) for value in values if str(value))
        except Exception:
            return ()
        return ()

    def _safe_keys(self, pattern: str) -> tuple[str, ...]:
        try:
            if hasattr(self.redis, "scan_iter"):
                values = self.redis.scan_iter(match=pattern, count=512)
                return tuple(str(value) for value in values if str(value))
            if hasattr(self.redis, "keys"):
                values = self.redis.keys(pattern) or []
                return tuple(str(value) for value in values if str(value))
        except Exception:
            return ()
        return ()

    def _safe_smembers(self, key: str) -> tuple[str, ...]:
        try:
            if hasattr(self.redis, "smembers"):
                values = self.redis.smembers(key) or []
                return tuple(str(value) for value in values if str(value))
        except Exception:
            return ()
        return ()

    def _safe_hexists(self, key: str, field: str) -> bool:
        try:
            if hasattr(self.redis, "hexists"):
                return bool(self.redis.hexists(key, field))
            if hasattr(self.redis, "hget"):
                return self.redis.hget(key, field) not in (None, "")
        except Exception:
            return False
        return False

    def _safe_exists(self, key: str) -> bool:
        try:
            if hasattr(self.redis, "exists"):
                return bool(self.redis.exists(key))
            if hasattr(self.redis, "get"):
                return self.redis.get(key) not in (None, "")
        except Exception:
            return False
        return False

    def _safe_get(self, key: str) -> str | None:
        try:
            if hasattr(self.redis, "get"):
                value = self.redis.get(key)
                return str(value) if value not in (None, "") else None
        except Exception:
            return None
        return None

    def _safe_get_json(self, key: str) -> dict[str, object]:
        raw = self._safe_get(key)
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _safe_hget(self, key: str, field: str) -> str | None:
        try:
            if hasattr(self.redis, "hget"):
                value = self.redis.hget(key, field)
                return str(value) if value not in (None, "") else None
        except Exception:
            return None
        return None

    def _safe_hlen(self, key: str) -> int:
        try:
            if hasattr(self.redis, "hlen"):
                return int(self.redis.hlen(key) or 0)
            if hasattr(self.redis, "hgetall"):
                return len(self.redis.hgetall(key) or {})
        except Exception:
            return 0
        return 0

    def _safe_hmget_count(self, key: str, fields: tuple[str, ...]) -> int:
        if not fields:
            return 0
        try:
            if hasattr(self.redis, "hmget"):
                values = self.redis.hmget(key, list(fields)) or []
                return sum(1 for value in values if value not in (None, ""))
            if hasattr(self.redis, "hget"):
                return sum(1 for field in fields if self.redis.hget(key, field) not in (None, ""))
        except Exception:
            return 0
        return 0

    def _safe_hmget_presence_map(self, key: str, fields: tuple[str, ...]) -> dict[str, bool]:
        if not fields:
            return {}
        payloads: dict[str, bool] = {str(field): False for field in fields if str(field)}
        try:
            if hasattr(self.redis, "hmget"):
                values = self.redis.hmget(key, list(fields)) or []
                for field, raw in zip(fields, values):
                    payloads[str(field)] = raw not in (None, "")
                return payloads
            if hasattr(self.redis, "hget"):
                for field in fields:
                    payloads[str(field)] = self.redis.hget(key, field) not in (None, "")
                return payloads
        except Exception:
            return payloads
        return payloads

    def _safe_hmget_json_map(self, key: str, fields: tuple[str, ...]) -> dict[str, dict[str, object]]:
        if not fields:
            return {}
        payloads: dict[str, dict[str, object]] = {}
        try:
            if hasattr(self.redis, "hmget"):
                values = self.redis.hmget(key, list(fields)) or []
                for field, raw in zip(fields, values):
                    if raw in (None, ""):
                        continue
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(parsed, dict):
                        payloads[str(field)] = parsed
                return payloads
            if hasattr(self.redis, "hget"):
                for field in fields:
                    raw = self.redis.hget(key, field)
                    if raw in (None, ""):
                        continue
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(parsed, dict):
                        payloads[str(field)] = parsed
        except Exception:
            return {}
        return payloads

    def _safe_set(self, key: str, value: str) -> bool:
        try:
            if hasattr(self.redis, "set"):
                self.redis.set(key, value)
                return True
        except Exception:
            return False
        return False

    def _safe_delete(self, keys: Iterable[str]) -> int:
        normalized = tuple(dict.fromkeys(str(key) for key in keys if str(key)))
        if not normalized:
            return 0
        try:
            if hasattr(self.redis, "delete"):
                deleted = self.redis.delete(*normalized)
                return int(deleted or 0)
        except Exception:
            return 0
        return 0

    def _cleanup_auction_temp_state_if_needed(
        self,
        *,
        now: datetime,
        trade_date: str,
    ) -> tuple[str, ...]:
        if self._last_auction_cleanup_trade_date == trade_date:
            return ()
        if infer_run_phase(now) != RunPhase.PREMARKET:
            return ()
        if now.strftime("%H:%M") >= "09:15":
            return ()
        tag = trade_date.replace("-", "")
        keys = {
            f"market:auction:anchor:{tag}",
            f"market:auction:{tag}:latest",
            f"market:auction:{tag}:0920",
            f"market:auction:{tag}:0924",
            f"market:auction:{tag}:0925",
        }
        keys.update(self._safe_keys(f"market:auction:{tag}:09*"))
        deleted = self._safe_delete(keys)
        self._last_auction_cleanup_trade_date = trade_date
        return (f"auction_temp_cleanup | trade_date={trade_date} | deleted={deleted}",)

    def _should_emit_intraday_startup_auction_recap(
        self,
        *,
        phase: RunPhase,
        trade_date: str,
        now: datetime,
        lifecycle_audit_ran: bool,
    ) -> bool:
        if phase != RunPhase.INTRADAY or not lifecycle_audit_ran:
            return False
        if not self._is_opening_strategy_window(now, phase):
            return False
        if self._last_intraday_auction_recap_trade_date == trade_date:
            return False
        self._last_intraday_auction_recap_trade_date = trade_date
        return True

    def _persist_opening_validation_checkpoint_if_needed(
        self,
        *,
        request: EngineAppRequest,
        phase: RunPhase,
        intraday_context: IntradayContext | None,
    ) -> tuple[str, ...]:
        if request.historical_replay or phase != RunPhase.INTRADAY:
            return ()
        minute_tag = request.now.strftime("%H:%M")
        if minute_tag < "09:31" or minute_tag > "09:33":
            return ()
        token = f"{request.trade_date}:opening_validation_window"
        if self._last_opening_validation_checkpoint_token == token:
            return ()
        if self._auction_runtime.has_opening_validation_checkpoint(request.trade_date):
            self._last_opening_validation_checkpoint_token = token
            return ()
        notes = self._auction_runtime.persist_opening_validation_checkpoint(
            trade_date=request.trade_date,
            intraday_context=intraday_context,
            now=request.now,
        )
        if notes and notes[0] == "opening_validation_checkpoint persisted":
            self._last_opening_validation_checkpoint_token = token
        return notes

    def _refresh_open_2m_runtime_summary_if_needed(
        self,
        *,
        request: EngineAppRequest,
        phase: RunPhase,
        offline_context_date: str,
    ) -> tuple[str, ...]:
        if request.historical_replay or phase != RunPhase.INTRADAY:
            return ()
        minute_tag = request.now.strftime("%H:%M")
        if minute_tag < "09:32" or minute_tag > "09:34":
            return ()
        if not self._is_opening_strategy_window(request.now, phase):
            return ()
        token = f"{request.trade_date}:open2m_runtime_summary"
        if self._last_open_2m_runtime_refresh_token == token:
            return ()
        result = self._market_runtime_summary_service.refresh_open_2m_runtime_summary(
            request.trade_date,
            offline_context_date=offline_context_date,
            create_if_missing=False,
        )
        if result is None:
            return ()
        self._last_open_2m_runtime_refresh_token = token
        summary = result.summary
        return (
            "open2m_runtime_summary refreshed",
            (
                "open2m_slice "
                f"| top10={float(summary.get('open_2m_top10_amount', 0.0) or 0.0):.2f} "
                f"| top20={float(summary.get('open_2m_top20_amount', 0.0) or 0.0):.2f} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            ),
        )

    def _hot_plate_freshness_limit_seconds(self, phase: RunPhase) -> int:
        if phase == RunPhase.PREMARKET:
            return 96 * 60 * 60
        if phase in {RunPhase.AUCTION, RunPhase.INTRADAY}:
            return 45 * 60
        if phase == RunPhase.POSTMARKET:
            return 8 * 60 * 60
        return 24 * 60 * 60

    def _analytics_fact_cache_date(
        self,
        *,
        now: datetime,
        trade_date: str,
        offline_context_date: str,
    ) -> str:
        phase = infer_run_phase(now)
        if phase in {RunPhase.POSTMARKET, RunPhase.NIGHT} and now.time().strftime("%H:%M") >= "17:30":
            return trade_date
        return offline_context_date

    def _extract_symbols_from_json_text(self, payload: str | None) -> tuple[str, ...]:
        if not payload:
            return ()
        try:
            parsed = json.loads(payload)
        except Exception:
            return ()
        values: list[str] = []
        if isinstance(parsed, dict):
            values.extend(str(key) for key in parsed.keys())
        elif isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    for key in ("code", "symbol"):
                        if item.get(key):
                            values.append(str(item[key]))
                            break
                elif item:
                    values.append(str(item))
        return _dedupe_symbols(values)

    def _load_live_quote_symbols(
        self,
        *,
        trade_date: str,
        allow_full_scan_fallback: bool = False,
    ) -> tuple[str, ...]:
        date_tag = trade_date.replace("-", "")
        q2_active_symbols = self._safe_smembers(f"q2:active:{date_tag}")
        if q2_active_symbols:
            return _dedupe_symbols(q2_active_symbols)
        legacy_quote_symbols = tuple(
            key.replace("stock:quote:", "")
            for key in self._safe_keys("stock:quote:*")
            if str(key).startswith("stock:quote:")
        )
        if legacy_quote_symbols:
            return _dedupe_symbols(legacy_quote_symbols)
        if not allow_full_scan_fallback:
            return ()
        q2_keys = self._safe_keys("q2:*")
        return _dedupe_symbols(
            key.replace("q2:", "")
            for key in q2_keys
            if str(key).startswith("q2:")
        )

    def _discover_runtime_symbols(self, trade_date: str, previous_trade_date: str) -> tuple[str, ...]:
        candidates: list[str] = []
        candidates.extend(self._safe_hkeys(f"cache:yest_limit_pool:{previous_trade_date}"))
        candidates.extend(self._extract_symbols_from_json_text(self._safe_get("market:alpha:candidates")))
        candidates.extend(
            self._extract_symbols_from_json_text(
                self._safe_hget(f"market:auction:{trade_date.replace('-', '')}:0925", "top_amount")
            )
        )
        candidates.extend(self._safe_hkeys("market:stock_plate"))
        candidates.extend(self._safe_hkeys("config:plate_mapping:s2p"))
        candidates.extend(
            self._load_live_quote_symbols(
                trade_date=trade_date,
                allow_full_scan_fallback=True,
            )
        )
        return _dedupe_symbols(candidates)

    def _filter_active_runtime_symbols(
        self,
        symbols: tuple[str, ...],
        *,
        now: datetime,
        trade_date: str,
        previous_trade_date: str,
        historical_replay: bool = False,
    ) -> tuple[str, ...]:
        phase = infer_run_phase(now)
        if phase in {RunPhase.PREMARKET, RunPhase.NIGHT}:
            return symbols
        if historical_replay:
            return symbols
        if not _is_live_target_session(now, trade_date):
            return symbols

        focus_symbols = set(self._safe_hkeys(f"cache:yest_limit_pool:{previous_trade_date}"))
        focus_symbols.update(self._extract_symbols_from_json_text(self._safe_get("market:alpha:candidates")))
        focus_symbols.update(
            self._extract_symbols_from_json_text(
                self._safe_hget(f"market:auction:{trade_date.replace('-', '')}:0925", "top_amount")
            )
        )
        active_quote_symbols = set(
            self._load_live_quote_symbols(
                trade_date=trade_date,
                allow_full_scan_fallback=False,
            )
        )
        if not active_quote_symbols:
            return symbols

        active_symbols = tuple(
            symbol
            for symbol in symbols
            if symbol in active_quote_symbols or symbol in focus_symbols
        )
        return active_symbols or symbols

    def _discover_full_universe_symbols(self) -> tuple[str, ...]:
        try:
            from web.services.f10_service import F10DataService

            service = F10DataService()
            return _dedupe_symbols(service.index_by_code.keys())
        except Exception:
            return ()

    def _summarize_integrated_sync(self, results: tuple[IntegratedSyncResult, ...]) -> tuple[str, ...]:
        if not results:
            return ()

        fully_ready = 0
        partial_ready = 0
        failed = 0
        kline_ready = 0
        dde_ready = 0
        factor_ready = 0
        chip_ready = 0
        redis_ready = 0
        tdengine_writes = 0
        redis_writes = 0

        for result in results:
            all_ready = (
                result.kline_ready
                and result.dde_ready
                and result.factor_ready
                and result.chip_ready
                and result.redis_cache_ready
            )
            any_ready = (
                result.kline_ready
                or result.dde_ready
                or result.factor_ready
                or result.chip_ready
                or result.redis_cache_ready
            )
            if all_ready:
                fully_ready += 1
            elif any_ready:
                partial_ready += 1
            else:
                failed += 1

            kline_ready += int(result.kline_ready)
            dde_ready += int(result.dde_ready)
            factor_ready += int(result.factor_ready)
            chip_ready += int(result.chip_ready)
            redis_ready += int(result.redis_cache_ready)
            tdengine_writes += len(result.wrote_tdengine)
            redis_writes += len(result.wrote_redis)

        partial_details: list[str] = []
        failed_details: list[str] = []
        failed_roots: list[str] = []
        root_counter: dict[str, int] = {}
        for result in results:
            missing_parts = self._format_missing_parts(result)
            if not missing_parts:
                continue
            detail = f"{result.symbol}:{'/'.join(missing_parts)}"
            any_ready = (
                result.kline_ready
                or result.dde_ready
                or result.factor_ready
                or result.chip_ready
                or result.redis_cache_ready
            )
            if any_ready:
                partial_details.append(detail)
            else:
                failed_details.append(detail)
                root = self._infer_sync_root_cause(result)
                failed_roots.append(f"{result.symbol}:{root}")
                root_counter[root] = root_counter.get(root, 0) + 1

        lines = [
            f"sync.summary | full={fully_ready} | partial={partial_ready} | failed={failed}",
            (
                "sync.datasets "
                f"| kline={kline_ready}/{len(results)} "
                f"| dde={dde_ready}/{len(results)} "
                f"| factor={factor_ready}/{len(results)} "
                f"| chip={chip_ready}/{len(results)} "
                f"| redis={redis_ready}/{len(results)}"
            ),
            f"sync.writes | tdengine_ops={tdengine_writes} | redis_ops={redis_writes}",
        ]
        if partial_details:
            shown = partial_details[:5]
            suffix = f" ...等{len(partial_details)}只" if len(partial_details) > len(shown) else ""
            lines.append(f"sync.partial_symbols | {', '.join(shown)}{suffix}")
        if failed_details:
            shown = failed_details[:5]
            suffix = f" ...等{len(failed_details)}只" if len(failed_details) > len(shown) else ""
            lines.append(f"sync.failed_symbols | {', '.join(shown)}{suffix}")
        if failed_roots:
            shown = failed_roots[:5]
            suffix = f" ...等{len(failed_roots)}只" if len(failed_roots) > len(shown) else ""
            lines.append(f"sync.failed_root | {', '.join(shown)}{suffix}")
        if root_counter:
            summary = ", ".join(
                f"{root}={count}"
                for root, count in sorted(root_counter.items(), key=lambda item: (-item[1], item[0]))
            )
            lines.append(f"sync.root_summary | {summary}")
        remaining_kline = max(len(results) - kline_ready, 0)
        remaining_dde = max(len(results) - dde_ready, 0)
        remaining_factor = max(len(results) - factor_ready, 0)
        remaining_chip = max(len(results) - chip_ready, 0)
        sync_digest = (
            "同步结果 | "
            f"已全量补齐{fully_ready}只，部分补齐{partial_ready}只，仍失败{failed}只；"
            f"当前剩余缺口为日线{remaining_kline}只，DDE{remaining_dde}只，"
            f"因子{remaining_factor}只，筹码{remaining_chip}只。"
        )
        if root_counter:
            root_summary = ", ".join(
                f"{root}={count}"
                for root, count in sorted(root_counter.items(), key=lambda item: (-item[1], item[0]))
            )
            sync_digest += f" 主失败根因为 {root_summary}。"
        lines.insert(1, sync_digest)
        return tuple(lines)

    def _format_missing_parts(self, result: IntegratedSyncResult) -> list[str]:
        parts: list[str] = []
        reasons = self._extract_sync_reason_map(result.notes)
        for dataset, ready in (
            ("kline", result.kline_ready),
            ("dde", result.dde_ready),
            ("factor", result.factor_ready),
            ("chip", result.chip_ready),
            ("redis", result.redis_cache_ready),
        ):
            if ready:
                continue
            reason = reasons.get(dataset)
            parts.append(f"{dataset}({reason})" if reason else dataset)
        return parts

    def _infer_sync_root_cause(self, result: IntegratedSyncResult) -> str:
        reasons = self._extract_sync_reason_map(result.notes)
        for dataset, ready in (
            ("kline", result.kline_ready),
            ("dde", result.dde_ready),
            ("factor", result.factor_ready),
            ("chip", result.chip_ready),
            ("redis", result.redis_cache_ready),
        ):
            if not ready:
                reason = reasons.get(dataset)
                return f"{dataset}({reason})" if reason else dataset
        return "unknown"

    def _extract_sync_reason_map(self, notes: tuple[str, ...]) -> dict[str, str]:
        reasons: dict[str, str] = {}
        for raw_note in notes:
            note = str(raw_note or "").strip()
            if not note:
                continue
            lower = note.lower()
            datasets = self._infer_note_datasets(lower)
            if not datasets:
                continue
            reason = self._classify_sync_note(lower)
            if not reason:
                continue
            for dataset in datasets:
                reasons.setdefault(dataset, reason)
        return reasons

    def _infer_note_datasets(self, lower_note: str) -> tuple[str, ...]:
        datasets: list[str] = []
        if "kline" in lower_note or "baostock" in lower_note:
            datasets.append("kline")
        if "dde" in lower_note:
            datasets.append("dde")
        if "factor" in lower_note:
            datasets.append("factor")
        if "chip" in lower_note:
            datasets.append("chip")
        if "redis" in lower_note or "cache" in lower_note:
            datasets.append("redis")
        return tuple(dict.fromkeys(datasets))

    def _classify_sync_note(self, lower_note: str) -> str | None:
        if "login failed" in lower_note or "network" in lower_note:
            return "network"
        if "stale" in lower_note:
            return "stale"
        if "empty payload" in lower_note or "returned empty payload" in lower_note:
            return "empty"
        if "missing date column" in lower_note:
            return "missing_date"
        if "unable to parse" in lower_note or "parse" in lower_note:
            return "parse_error"
        if "history too short" in lower_note or "too short" in lower_note:
            return "short_history"
        if "not ready after ensure" in lower_note or "still not ready" in lower_note:
            return "not_ready"
        if "save_" in lower_note and "returned false" in lower_note:
            return "persist_failed"
        if "deferred" in lower_note and "failed" in lower_note:
            return "persist_failed"
        return None

    def _render_runtime_cache_counters(
        self,
        *,
        runtime_readiness: dict[str, object],
        symbol_count: int,
    ) -> str:
        factor_prev_ready_count = int(runtime_readiness.get("redis_factor_ready_count", 0) or 0)
        factor_current_ready_count = int(runtime_readiness.get("current_trade_factor_ready_count", 0) or 0)
        chip_prev_ready_count = int(runtime_readiness.get("redis_chip_ready_count", 0) or 0)
        chip_current_ready_count = int(runtime_readiness.get("current_trade_chip_ready_count", 0) or 0)
        return (
            "runtime_cache_counts "
            f"| factor_prev={factor_prev_ready_count}/{symbol_count} "
            f"| factor_current={factor_current_ready_count}/{symbol_count} "
            f"| chip_prev={chip_prev_ready_count}/{symbol_count} "
            f"| chip_current={chip_current_ready_count}/{symbol_count}"
        )

    def _load_runtime_readiness(
        self,
        *,
        now: datetime,
        trade_date: str,
        previous_trade_date: str,
        offline_context_date: str,
        symbols: tuple[str, ...],
        historical_replay: bool = False,
    ) -> dict[str, object]:
        def _load_hot_plate_cache_state(cache_date: str) -> dict[str, object]:
            cache_key = f"cache:hot_plates:{cache_date}"
            meta = self._safe_get_json(f"cache:hot_plates_meta:{cache_date}")
            count = self._safe_hlen(cache_key)
            row_count = 0
            updated_at_ts = 0
            try:
                row_count = int(meta.get("row_count", 0) or 0)
            except (TypeError, ValueError):
                row_count = 0
            try:
                updated_at_ts = int(float(meta.get("updated_at_ts", 0) or 0))
            except (TypeError, ValueError):
                updated_at_ts = 0
            meta_trade_date = str(meta.get("trade_date") or "").strip()
            return {
                "count": count,
                "row_count": row_count,
                "updated_at_ts": updated_at_ts,
                "meta_trade_date": meta_trade_date,
                "ready": (
                    count > 0
                    and row_count > 0
                    and meta_trade_date == cache_date
                    and updated_at_ts > 0
                ),
            }

        symbol_fields = tuple(str(symbol) for symbol in symbols if str(symbol))
        factor_key = f"cache:stock_extra:{offline_context_date}"
        factor_ready = self._safe_hmget_presence_map(factor_key, symbol_fields)
        current_trade_factor_ready = self._safe_hmget_presence_map(f"cache:stock_extra:{trade_date}", symbol_fields)
        current_trade_chip_ready = self._safe_hmget_presence_map(f"cache:chip_peaks:{trade_date}", symbol_fields)
        redis_chip_ready = self._safe_hmget_presence_map(f"cache:chip_peaks:{offline_context_date}", symbol_fields)
        redis_dde_ready = self._safe_hmget_presence_map(f"cache:dde_ready:{offline_context_date}", symbol_fields)
        hot_plate_date = _hot_plate_cache_date(now, trade_date, previous_trade_date)
        phase = infer_run_phase(now)
        analytics_cache_date = self._analytics_fact_cache_date(
            now=now,
            trade_date=trade_date,
            offline_context_date=offline_context_date,
        )
        analytics_readiness_map = self._safe_hmget_json_map(
            f"cache:analytics_readiness:{analytics_cache_date}",
            symbol_fields,
        )
        symbol_meta_map = self._safe_hmget_json_map(
            f"cache:symbol_meta:{analytics_cache_date}",
            symbol_fields,
        )
        runtime_plate_count = self._safe_hlen("market:stock_plate")
        stock_theme_count = self._safe_hlen("config:plate_mapping:s2p")
        hot_plate_key = f"cache:hot_plates:{hot_plate_date}"
        hot_plate_state = _load_hot_plate_cache_state(hot_plate_date)
        today_hot_plate_state = _load_hot_plate_cache_state(trade_date)
        yesterday_hot_plate_state = _load_hot_plate_cache_state(previous_trade_date)
        hot_plate_meta = self._safe_get_json(f"cache:hot_plates_meta:{hot_plate_date}")
        hot_plate_count = int(hot_plate_state["count"] or 0)
        hot_plate_row_count = int(hot_plate_state["row_count"] or 0)
        hot_plate_updated_at_ts = int(hot_plate_state["updated_at_ts"] or 0)
        hot_plate_freshness_limit_seconds = self._hot_plate_freshness_limit_seconds(phase)
        hot_plate_age_seconds = max(int(now.timestamp()) - hot_plate_updated_at_ts, 0) if hot_plate_updated_at_ts > 0 else None
        live_target_session = (not historical_replay) and _is_live_target_session(now, trade_date)
        previous_settlement_payload = self._safe_get_json(
            f"market:settlement:{previous_trade_date.replace('-', '')}:done"
        )
        hot_plate_cache_ready = bool(hot_plate_state["ready"])
        hot_plates_today_ready = bool(today_hot_plate_state["ready"])
        effective_hot_plate_state = today_hot_plate_state
        effective_hot_plate_trade_date = trade_date
        if (
            phase in (RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.INTRADAY)
            and not bool(effective_hot_plate_state["ready"])
            and bool(yesterday_hot_plate_state["ready"])
        ):
            effective_hot_plate_state = yesterday_hot_plate_state
            effective_hot_plate_trade_date = previous_trade_date
        hot_plates_live_fresh = hot_plate_cache_ready and (
            not live_target_session
            or (
                hot_plate_age_seconds is not None
                and hot_plate_age_seconds <= hot_plate_freshness_limit_seconds
            )
        )
        return {
            "redis_factor_cache_ready": factor_ready,
            "current_trade_factor_cache_ready": current_trade_factor_ready,
            "redis_factor_ready_count": sum(1 for ok in factor_ready.values() if ok),
            "current_trade_factor_ready_count": sum(1 for ok in current_trade_factor_ready.values() if ok),
            "yest_limit_pool_ready": self._safe_hlen(f"cache:yest_limit_pool:{previous_trade_date}") > 0,
            "hot_plates_ready": hot_plate_cache_ready,
            "hot_plates_today_ready": hot_plates_today_ready,
            "hot_plates_effective_ready": bool(effective_hot_plate_state["ready"]),
            "hot_plates_effective_trade_date": effective_hot_plate_trade_date,
            "hot_plates_live_fresh": hot_plates_live_fresh,
            "stock_plate_mapping_ready": (
                runtime_plate_count >= self.MIN_STOCK_PLATE_MAPPING_COUNT
                or stock_theme_count >= self.MIN_STOCK_PLATE_MAPPING_COUNT
            ),
            "auction_anchor_ready": self._safe_exists(f"market:auction:anchor:{trade_date.replace('-', '')}"),
            "redis_chip_ready_count": sum(1 for ok in redis_chip_ready.values() if ok),
            "current_trade_chip_ready_count": sum(1 for ok in current_trade_chip_ready.values() if ok),
            "current_trade_chip_cache_ready": current_trade_chip_ready,
            "redis_dde_ready_count": sum(1 for ok in redis_dde_ready.values() if ok),
            "hot_plate_count": hot_plate_count,
            "hot_plate_row_count": hot_plate_row_count,
            "hot_plate_cache_date": hot_plate_date,
            "analytics_cache_date": analytics_cache_date,
            "hot_plate_age_seconds": hot_plate_age_seconds,
            "live_target_session": live_target_session,
            "previous_settlement_payload": previous_settlement_payload,
            "cached_listing_dates": {
                symbol: str((payload.get("listing_date") or "")).strip()
                for symbol, payload in symbol_meta_map.items()
                if str(payload.get("listing_date") or "").strip()
            },
            "cached_kline_row_counts": {
                symbol: int(payload.get("kline_rows", 0) or 0)
                for symbol, payload in analytics_readiness_map.items()
                if int(payload.get("kline_rows", 0) or 0) > 0
            },
            "cached_structural_factor_gap": {
                symbol: True
                for symbol, payload in analytics_readiness_map.items()
                if bool(payload.get("structural_factor_gap"))
            },
        }

    def _current_audit_token(self, now: datetime, trade_date: str) -> str | None:
        minute_tag = now.strftime("%H:%M")
        if (
            self._startup_bootstrap.last_audit_trade_date != trade_date
            or self._startup_bootstrap.last_audit_token is None
        ):
            return f"startup:{trade_date}:{minute_tag}"
        if minute_tag in self.STARTUP_AUDIT_CHECKPOINTS:
            return f"checkpoint:{trade_date}:{minute_tag}"
        return None

    def _build_loop_decision(self, now: datetime, trade_date: str) -> RuntimeLoopDecision:
        minute_tag = now.strftime("%H:%M")
        audit_token = self._current_audit_token(now, trade_date)
        phase = infer_run_phase(now)
        if minute_tag == "09:25":
            if now.time() < self.AUCTION_FINALIZE_EARLIEST:
                return RuntimeLoopDecision(
                    name="auction_anchor_settling",
                    label="auction anchor settling",
                    audit_token=None,
                    should_run_lifecycle_audit=False,
                )
            return RuntimeLoopDecision(
                name="auction_anchor_finalize",
                label="auction anchor finalize 09:25",
                audit_token=None,
                should_run_lifecycle_audit=False,
                scheduled_event_name="auction_finalize_0925",
                scheduled_event_label="auction finalize event 09:25",
            )
        if minute_tag == "09:26":
            return RuntimeLoopDecision(
                name="auction_followup_checkpoint",
                label="auction follow-up checkpoint 09:26",
                audit_token=None,
                should_run_lifecycle_audit=False,
                scheduled_event_name="auction_followup_0926",
                scheduled_event_label="auction follow-up event 09:26",
            )
        if dt_time(9, 32, 10) <= now.time() < dt_time(9, 34):
            return RuntimeLoopDecision(
                name="opening_facts_checkpoint",
                label="opening facts checkpoint 09:32",
                audit_token=None,
                should_run_lifecycle_audit=False,
                scheduled_event_name="opening_facts_0932",
                scheduled_event_label="opening facts event 09:32",
            )
        if (
            self._startup_bootstrap.last_audit_trade_date != trade_date
            or self._startup_bootstrap.last_audit_token is None
        ):
            return RuntimeLoopDecision(
                name="startup_bootstrap",
                label="startup bootstrap audit",
                audit_token=audit_token,
                should_run_lifecycle_audit=bool(audit_token),
            )
        if minute_tag in self.STARTUP_AUDIT_CHECKPOINTS:
            return RuntimeLoopDecision(
                name=f"startup_checkpoint_{minute_tag.replace(':', '')}",
                label=f"startup checkpoint {minute_tag}",
                audit_token=audit_token,
                should_run_lifecycle_audit=bool(audit_token and audit_token != self._startup_bootstrap.last_audit_token),
            )
        if minute_tag == "15:05":
            return RuntimeLoopDecision(
                name="market_close_marker",
                label="market close marker 15:05",
                audit_token=None,
                should_run_lifecycle_audit=False,
                scheduled_event_name="market_close_1505",
                scheduled_event_label="market close event 15:05",
            )
        if minute_tag == "17:40":
            return RuntimeLoopDecision(
                name="postmarket_settlement_checkpoint",
                label="postmarket settlement checkpoint 17:40",
                audit_token=audit_token,
                should_run_lifecycle_audit=bool(audit_token and audit_token != self._startup_bootstrap.last_audit_token),
                scheduled_event_name="postmarket_settlement_1740",
                scheduled_event_label="postmarket settlement event 17:40",
            )
        if phase == RunPhase.INTRADAY:
            return RuntimeLoopDecision(
                name="intraday_live_loop",
                label="intraday live loop",
                audit_token=None,
                should_run_lifecycle_audit=False,
            )
        if phase == RunPhase.AUCTION:
            return RuntimeLoopDecision(
                name="auction_live_loop",
                label="auction live loop",
                audit_token=None,
                should_run_lifecycle_audit=False,
            )
        if phase == RunPhase.POSTMARKET:
            return RuntimeLoopDecision(
                name="postmarket_live_loop",
                label="postmarket live loop",
                audit_token=None,
                should_run_lifecycle_audit=False,
            )
        if phase == RunPhase.NIGHT:
            return RuntimeLoopDecision(
                name="night_watch_loop",
                label="night watch loop",
                audit_token=None,
                should_run_lifecycle_audit=False,
            )
        return RuntimeLoopDecision(
            name="premarket_live_loop",
            label="premarket live loop",
            audit_token=None,
            should_run_lifecycle_audit=False,
        )

    def _render_cached_runtime_summary(
        self,
        *,
        startup_bundle: StartupExecutionBundle,
        runtime_readiness: dict[str, object],
        symbol_count: int,
    ) -> tuple[str, ...]:
        report = startup_bundle.plan.report
        status_map = report.by_dataset()
        daily_kline = status_map["daily_kline"]
        daily_factors = status_map["daily_factors"]
        hot_plates_ready = bool(runtime_readiness.get("hot_plates_ready"))
        hot_plates_today_ready = bool(runtime_readiness.get("hot_plates_today_ready"))
        hot_plates_effective_ready = bool(runtime_readiness.get("hot_plates_effective_ready", hot_plates_ready))
        hot_plates_effective_trade_date = str(runtime_readiness.get("hot_plates_effective_trade_date") or "-")
        yest_limit_ready = bool(runtime_readiness.get("yest_limit_pool_ready"))
        stock_plate_ready = bool(runtime_readiness.get("stock_plate_mapping_ready"))
        auction_ready = bool(runtime_readiness.get("auction_anchor_ready"))
        redis_chip_ready_count = int(runtime_readiness.get("redis_chip_ready_count", 0) or 0)
        redis_dde_ready_count = int(runtime_readiness.get("redis_dde_ready_count", 0) or 0)
        return (
            (
                "cached_audit "
                f"| trade_date={report.target_trade_date} "
                f"| offline_formal={report.formal_offline_date} "
                f"| readiness={report.readiness.value} "
                f"| symbols={symbol_count}"
            ),
            (
                "cached_gaps "
                f"| daily_kline={daily_kline.missing_count}/{daily_kline.total_count} "
                f"| daily_factors={daily_factors.missing_count}/{daily_factors.total_count} "
                f"| chip_peaks={max(symbol_count - redis_chip_ready_count, 0)}/{symbol_count} "
                f"| daily_dde={max(symbol_count - redis_dde_ready_count, 0)}/{symbol_count}"
            ),
            self._render_runtime_cache_counters(
                runtime_readiness=runtime_readiness,
                symbol_count=symbol_count,
            ),
            (
                "runtime_cache "
                f"| yest_limit_pool={'ok' if yest_limit_ready else 'missing'} "
                f"| hot_plates={'ok' if hot_plates_ready else 'missing'} "
                f"| hot_plates_today={'ok' if hot_plates_today_ready else 'missing'} "
                f"| hot_plates_effective={'ok' if hot_plates_effective_ready else 'missing'}@{hot_plates_effective_trade_date} "
                f"| stock_plate_mapping={'ok' if stock_plate_ready else 'missing'} "
                f"| auction_anchor={'ok' if auction_ready else 'missing'}"
            ),
        )

    @staticmethod
    def _is_opening_strategy_window(now: datetime, phase: RunPhase) -> bool:
        if phase != RunPhase.INTRADAY:
            return False
        minute_tag = now.strftime("%H:%M")
        return "09:30" <= minute_tag < "09:40"

    def _derive_runtime_readiness(
        self,
        *,
        phase: RunPhase,
        runtime_readiness: dict[str, object],
        primed_runtime_state: PrimedIntradayRuntimeState | None = None,
    ) -> str:
        yest_limit_ready = bool(runtime_readiness.get("yest_limit_pool_ready"))
        hot_plates_ready = bool(runtime_readiness.get("hot_plates_ready"))
        stock_plate_ready = bool(runtime_readiness.get("stock_plate_mapping_ready"))
        auction_ready = bool(runtime_readiness.get("auction_anchor_ready"))
        fresh_ratio = primed_runtime_state.quote_fresh_ratio if primed_runtime_state is not None else 0.0
        latest_age_seconds = primed_runtime_state.latest_quote_age_seconds if primed_runtime_state is not None else None
        stale_threshold_seconds = (
            primed_runtime_state.quote_stale_threshold_seconds if primed_runtime_state is not None else 0
        )
        quote_rows = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
        has_live_quote_feed = (
            quote_rows > 0
            and latest_age_seconds is not None
            and stale_threshold_seconds > 0
            and latest_age_seconds <= stale_threshold_seconds
            and fresh_ratio > 0
        )

        if phase in (RunPhase.AUCTION, RunPhase.INTRADAY):
            max_healthy_age_seconds = 15 if phase == RunPhase.INTRADAY else stale_threshold_seconds
            min_healthy_fresh_ratio = 0.95 if phase == RunPhase.INTRADAY else 0.90
            quote_feed_healthy = (
                quote_rows > 0
                and fresh_ratio >= min_healthy_fresh_ratio
                and latest_age_seconds is not None
                and stale_threshold_seconds > 0
                and latest_age_seconds <= max_healthy_age_seconds
            )
            quote_feed_degraded = (
                quote_rows > 0
                and latest_age_seconds is not None
                and stale_threshold_seconds > 0
                and latest_age_seconds <= (stale_threshold_seconds * 4)
                and fresh_ratio >= 0.20
            )
            if phase == RunPhase.AUCTION and auction_ready:
                return "trade_ready_runtime" if quote_feed_healthy else "anchor_ready_runtime"
            if yest_limit_ready and hot_plates_ready and stock_plate_ready and auction_ready:
                if quote_feed_healthy:
                    return "trade_ready_runtime"
                if quote_feed_degraded:
                    return "degraded_runtime"
                return "observe_runtime"
            if yest_limit_ready and stock_plate_ready:
                return "degraded_runtime" if quote_feed_healthy or quote_feed_degraded else "observe_runtime"
            return "observe_runtime"

        if phase == RunPhase.POSTMARKET:
            if yest_limit_ready and stock_plate_ready and hot_plates_ready:
                return "postmarket_recap_ready"
            if yest_limit_ready or stock_plate_ready:
                return "postmarket_partial"
            return "postmarket_warming"

        if phase == RunPhase.PREMARKET:
            if yest_limit_ready and stock_plate_ready and hot_plates_ready:
                return "trade_ready_runtime" if has_live_quote_feed else "historical_context_only"
            if yest_limit_ready or stock_plate_ready:
                return "degraded_runtime" if has_live_quote_feed else "observe_runtime"
            return "observe_runtime"

        if yest_limit_ready and stock_plate_ready and hot_plates_ready:
            return "trade_ready_runtime"
        if yest_limit_ready or stock_plate_ready:
            return "degraded_runtime"
        return "observe_runtime"

    def _render_postmarket_snapshot_line(
        self,
        *,
        primed_runtime_state: PrimedIntradayRuntimeState | None,
        symbol_count: int,
    ) -> str | None:
        if primed_runtime_state is None:
            return None
        latest = primed_runtime_state.latest_quote_time or "-"
        lag = primed_runtime_state.latest_quote_age_seconds
        lag_text = "-" if lag is None else f"{lag}s"
        quote_rows = len(primed_runtime_state.quote_rows)
        mode = "收盘近似快照"
        if lag is not None and lag > 60 * 30:
            mode = "盘中冻结快照"
        return (
            "close_snapshot "
            f"| captured={quote_rows}/{symbol_count} "
            f"| missing={primed_runtime_state.quote_missing_count} "
            f"| last_tick={latest} "
            f"| age={lag_text} "
            f"| mode={mode} "
            f"| market_closed=yes"
        )

    def _render_postmarket_takeover_summary(
        self,
        *,
        runtime_readiness_label: str,
        symbols: int,
        intraday_context: IntradayContext | None,
        primed_runtime_state: PrimedIntradayRuntimeState | None,
        now: datetime | None = None,
    ) -> tuple[str, ...]:
        quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
        native_count = _native_ingested_count(primed_runtime_state)
        live_notes = list(
            self._auction_runtime.render_postmarket_runtime_loop(
                intraday_context=intraday_context,
                runtime_readiness_label=runtime_readiness_label,
                symbols=symbols,
                quotes=quote_count,
                native=native_count,
                now=now or datetime.now(),
                quote_freshness_line=self._render_postmarket_snapshot_line(
                    primed_runtime_state=primed_runtime_state,
                    symbol_count=symbols,
                ),
            )
        )
        return tuple(
            [
                "runtime_event=postmarket takeover",
                *live_notes,
            ]
        )

    def _refresh_startup_state_after_settlement(
        self,
        *,
        request: EngineAppRequest,
        symbols: tuple[str, ...],
        offline_context_date: str,
        startup_bundle: StartupExecutionBundle,
    ) -> tuple[StartupExecutionBundle, WatermarkSnapshot, dict[str, object]]:
        refreshed_runtime_readiness = self._load_runtime_readiness(
            now=request.now,
            trade_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            offline_context_date=offline_context_date,
            symbols=symbols,
            historical_replay=request.historical_replay,
        )
        refreshed_watermark_snapshot = self._offline_executor.preload_watermark_snapshot(
            OfflineSyncRequest(
                now=request.now,
                target_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                symbols=symbols,
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready=refreshed_runtime_readiness["redis_factor_cache_ready"],
                environment=request.environment,
            )
        )
        refreshed_plan = self._startup_coordinator.build_plan(
            StartupCoordinatorRequest(
                now=request.now,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                symbols=symbols,
                kline_watermarks=refreshed_watermark_snapshot.kline_latest_dates,
                factor_watermarks=refreshed_watermark_snapshot.factor_latest_dates,
                redis_factor_cache_ready=refreshed_runtime_readiness["redis_factor_cache_ready"],
                current_trade_factor_cache_ready=refreshed_runtime_readiness["current_trade_factor_cache_ready"],
                current_trade_chip_cache_ready=refreshed_runtime_readiness["current_trade_chip_cache_ready"],
                yest_limit_pool_ready=bool(refreshed_runtime_readiness["yest_limit_pool_ready"]),
                hot_plates_ready=bool(refreshed_runtime_readiness["hot_plates_ready"]),
                hot_plates_today_ready=bool(refreshed_runtime_readiness.get("hot_plates_today_ready", False)),
                hot_plates_effective_ready=bool(refreshed_runtime_readiness.get("hot_plates_effective_ready", refreshed_runtime_readiness["hot_plates_ready"])),
                hot_plates_effective_trade_date=str(refreshed_runtime_readiness.get("hot_plates_effective_trade_date") or ""),
                stock_plate_mapping_ready=bool(refreshed_runtime_readiness["stock_plate_mapping_ready"]),
                auction_anchor_ready=bool(refreshed_runtime_readiness["auction_anchor_ready"]),
                redis_chip_ready_count=int(refreshed_runtime_readiness["redis_chip_ready_count"] or 0),
                redis_dde_ready_count=int(refreshed_runtime_readiness["redis_dde_ready_count"] or 0),
                watermark_snapshot=refreshed_watermark_snapshot,
                environment=request.environment,
            )
        )
        refreshed_startup_bundle = StartupExecutionBundle(
            plan=refreshed_plan,
            stock_plate_result=startup_bundle.stock_plate_result,
            auction_result=startup_bundle.auction_result,
            hot_plate_result=startup_bundle.hot_plate_result,
            yest_limit_result=startup_bundle.yest_limit_result,
            market_runtime_summary_result=startup_bundle.market_runtime_summary_result,
        )
        self._startup_bootstrap.refresh_cached_state(
            trade_date=request.trade_date,
            startup_bundle=refreshed_startup_bundle,
            watermark_snapshot=refreshed_watermark_snapshot,
            runtime_readiness=refreshed_runtime_readiness,
        )
        return refreshed_startup_bundle, refreshed_watermark_snapshot, refreshed_runtime_readiness

    def _render_auction_takeover_summary(
        self,
        *,
        runtime_readiness_label: str,
        symbols: int,
        intraday_context: IntradayContext | None,
        primed_runtime_state: PrimedIntradayRuntimeState | None,
    ) -> tuple[str, ...]:
        quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
        native_count = _native_ingested_count(primed_runtime_state)
        return self._auction_runtime.render_auction_takeover(
            intraday_context=intraday_context,
            runtime_readiness_label=runtime_readiness_label,
            symbols=symbols,
            quotes=quote_count,
            native=native_count,
            quote_freshness_line=render_quote_freshness_line(
                primed_runtime_state,
                symbol_count=symbols,
            ),
        )

    def _render_scheduled_event_summary(self, event_result: RuntimeScheduledEventResult) -> tuple[str, ...]:
        if not event_result.executed:
            return ()
        details = [f"scheduled_event={event_result.label}"]
        if event_result.yest_limit_result is not None:
            result = event_result.yest_limit_result
            details.append(
                "event.yest_limit "
                f"| rows={len(result.rows)} "
                f"| source={result.source} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            )
        if event_result.hot_plate_result is not None:
            result = event_result.hot_plate_result
            details.append(
                "event.hot_plates "
                f"| rows={len(result.rows)} "
                f"| source={result.source} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            )
        if event_result.auction_result is not None:
            result = event_result.auction_result
            details.append(
                "event.auction "
                f"| rows={len(result.rows)} "
                f"| source={result.source} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            )
        if event_result.market_runtime_summary_result is not None:
            summary = event_result.market_runtime_summary_result.summary
            details.append(
                "event.market_summary "
                f"| top_plate={summary.get('top_plate_name', '')} "
                f"| hot_count={summary.get('hot_plate_count', 0)} "
                f"| auc_sample={summary.get('auction_sample_size', 0)} "
                f"| source={summary.get('source', '')}"
            )
        details.extend(event_result.notes)
        return tuple(details)

    def _render_settlement_quality(self, payload: dict[str, object] | None) -> tuple[str, ...]:
        if not payload:
            return ()
        effective_targets = int(payload.get("effective_targets", 0) or 0)
        result_count = int(payload.get("result_count", 0) or 0)
        if effective_targets <= 0 and result_count <= 0:
            return ()
        return (
            (
                "settlement_quality "
                f"| targets={effective_targets} "
                f"| results={result_count} "
                f"| full={int(payload.get('full_ready_count', 0) or 0)} "
                f"| partial={int(payload.get('partial_ready_count', 0) or 0)} "
                f"| failed={int(payload.get('failed_count', 0) or 0)} "
                f"| short_history={int(payload.get('short_history_count', 0) or 0)} "
                f"| factor_cache_gap={int(payload.get('factor_cache_gap_count', 0) or 0)} "
                f"| startup_fact={int(payload.get('startup_fact_analytics_count', 0) or 0)} "
                f"| symbol_meta={int(payload.get('startup_fact_symbol_meta_count', 0) or 0)} "
                f"| structural={int(payload.get('startup_fact_structural_count', 0) or 0)}"
            ),
        )

    def _should_render_cycle(
        self,
        *,
        request: EngineAppRequest,
        phase: RunPhase,
        lifecycle_audit_ran: bool,
        scheduled_event_result: RuntimeScheduledEventResult,
    ) -> bool:
        token = f"{request.trade_date}:{request.now.strftime('%H:%M')}"
        if lifecycle_audit_ran or scheduled_event_result.executed:
            self._last_render_token = token
            return True
        if phase in (RunPhase.PREMARKET, RunPhase.POSTMARKET, RunPhase.NIGHT):
            return False
        if self._last_render_token != token:
            self._last_render_token = token
            return True
        return False

    def _next_trade_day_checkpoint(self, now: datetime, clock_time: dt_time) -> datetime:
        date_text = now.strftime("%Y-%m-%d")
        try:
            next_trade_date = TradingCalendarService().get_next_trading_day(date_text)
        except Exception:
            next_trade_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        return datetime.strptime(
            f"{next_trade_date} {clock_time.strftime('%H:%M:%S')}",
            "%Y-%m-%d %H:%M:%S",
        )

    def _resolve_loop_sleep_seconds(
        self,
        *,
        now: datetime,
        phase: RunPhase,
        default_interval_seconds: int,
    ) -> int:
        target: datetime | None = None
        if phase == RunPhase.PREMARKET and now.time() < dt_time(8, 30):
            target = datetime.combine(now.date(), dt_time(8, 30))
        elif phase == RunPhase.POSTMARKET:
            if now.time() < dt_time(15, 5):
                target = datetime.combine(now.date(), dt_time(15, 5))
            elif now.time() < dt_time(17, 40):
                target = datetime.combine(now.date(), dt_time(17, 40))
            else:
                target = self._next_trade_day_checkpoint(now, dt_time(8, 30))
        elif phase == RunPhase.NIGHT:
            target = self._next_trade_day_checkpoint(now, dt_time(8, 30))

        if target is None:
            return max(1, int(default_interval_seconds))
        delta_seconds = int((target - now).total_seconds())
        return max(1, delta_seconds)

    def _execute_scheduled_event(
        self,
        *,
        loop_decision: RuntimeLoopDecision,
        request: EngineAppRequest,
        phase: RunPhase,
        offline_context_date: str,
    ) -> RuntimeScheduledEventResult:
        if not loop_decision.scheduled_event_name:
            return RuntimeScheduledEventResult(
                name="",
                label="",
                executed=False,
            )

        if request.historical_replay and loop_decision.scheduled_event_name in {
            "market_close_1505",
            "postmarket_settlement_1740",
        }:
            return RuntimeScheduledEventResult(
                name=loop_decision.scheduled_event_name,
                label=loop_decision.scheduled_event_label,
                executed=False,
                notes=("scheduled event skipped for historical replay.",),
            )

        event_token = f"{request.trade_date}:{loop_decision.scheduled_event_name}:{request.now.strftime('%H:%M')}"
        if self._last_scheduled_event_token == event_token:
            return RuntimeScheduledEventResult(
                name=loop_decision.scheduled_event_name,
                label=loop_decision.scheduled_event_label,
                executed=False,
                notes=("scheduled event already executed for this minute; skipping duplicate loop run.",),
            )

        yest_limit_result = None
        hot_plate_result = None
        auction_result = None
        market_runtime_summary_result = None
        notes: list[str] = []

        if loop_decision.scheduled_event_name == "auction_finalize_0925":
            replay_result = self._auction_runtime.execute_auction_finalize_0925(
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
            )
            yest_limit_result = replay_result.yest_limit_result
            hot_plate_result = replay_result.hot_plate_result
            auction_result = replay_result.auction_result
            market_runtime_summary_result = replay_result.market_runtime_summary_result
            notes.extend(replay_result.notes)
        elif loop_decision.scheduled_event_name == "auction_followup_0926":
            replay_result = self._auction_runtime.execute_auction_followup_0926(
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
            )
            yest_limit_result = replay_result.yest_limit_result
            hot_plate_result = replay_result.hot_plate_result
            auction_result = replay_result.auction_result
            market_runtime_summary_result = replay_result.market_runtime_summary_result
            notes.extend(replay_result.notes)
            if self._production_reporting is not None and not request.historical_replay:
                try:
                    status, report_hash = self._production_reporting.send_auction(
                        trade_date=request.trade_date,
                        request=request,
                        send_eligibility=True,
                    )
                    notes.append(
                        f"auction_report | status={status} | report_hash={report_hash} | "
                        "execution_mode=normal"
                    )
                except Exception as exc:
                    logger.exception("auction facts report failed")
                    if self._production_reporting is not None:
                        try:
                            status, report_hash = self._production_reporting.send_auction_unavailable(
                                trade_date=request.trade_date,
                                request=request,
                                send_eligibility=True,
                            )
                            notes.append(
                                f"auction_report | status=DATA_UNAVAILABLE | delivery={status} | "
                                f"report_hash={report_hash} | error={type(exc).__name__}"
                            )
                        except Exception:
                            logger.exception("auction unavailable report failed")
                            notes.append(f"auction_report | status=DATA_UNAVAILABLE | error={type(exc).__name__}")
                    else:
                        notes.append(f"auction_report | status=DATA_UNAVAILABLE | error={type(exc).__name__}")
        elif loop_decision.scheduled_event_name == "opening_facts_0932":
            if self._production_reporting is None or request.historical_replay:
                notes.append("opening_report | status=disabled_or_historical_replay | send_eligibility=false")
            else:
                try:
                    status, report_hash = self._production_reporting.send_opening(
                        trade_date=request.trade_date,
                        request=request,
                        observation_cutoff=request.now,
                        send_eligibility=True,
                    )
                    notes.append(
                        f"opening_report | status={status} | report_hash={report_hash} | "
                        f"observation_cutoff={request.now.isoformat()}"
                    )
                except Exception as exc:
                    logger.exception("opening facts report failed")
                    if self._production_reporting is not None:
                        try:
                            status, report_hash = self._production_reporting.send_opening_unavailable(
                                trade_date=request.trade_date,
                                request=request,
                                observation_cutoff=request.now,
                                send_eligibility=True,
                            )
                            notes.append(
                                f"opening_report | status=DATA_UNAVAILABLE | delivery={status} | "
                                f"report_hash={report_hash} | error={type(exc).__name__}"
                            )
                        except Exception:
                            logger.exception("opening unavailable report failed")
                            notes.append(f"opening_report | status=DATA_UNAVAILABLE | error={type(exc).__name__}")
                    else:
                        notes.append(f"opening_report | status=DATA_UNAVAILABLE | error={type(exc).__name__}")
        elif loop_decision.scheduled_event_name == "market_close_1505":
            close_result = self._postmarket_runtime.execute_close_marker(
                trade_date=request.trade_date,
            )
            notes.extend(close_result.notes)
        elif loop_decision.scheduled_event_name == "postmarket_settlement_1740":
            settlement_result = self._postmarket_runtime.execute_settlement_window(
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
            )
            hot_plate_result = settlement_result.hot_plate_result
            yest_limit_result = settlement_result.yest_limit_result
            market_runtime_summary_result = settlement_result.market_runtime_summary_result
            notes.extend(settlement_result.notes)

        self._last_scheduled_event_token = event_token
        return RuntimeScheduledEventResult(
            name=loop_decision.scheduled_event_name,
            label=loop_decision.scheduled_event_label,
            executed=True,
            notes=tuple(notes),
            yest_limit_result=yest_limit_result,
            hot_plate_result=hot_plate_result,
            auction_result=auction_result,
            market_runtime_summary_result=market_runtime_summary_result,
        )

    def _should_attempt_late_start_auction_recovery(
        self,
        *,
        request: EngineAppRequest,
        phase: RunPhase,
    ) -> bool:
        if request.historical_replay or phase != RunPhase.INTRADAY:
            return False
        if request.require_auction_recovery:
            return True
        if not _is_live_target_session(request.now, request.trade_date):
            return False
        minute_tag = request.now.strftime("%H:%M")
        return "09:30" <= minute_tag < "15:00"

    def _ensure_late_start_auction_recovery(
        self,
        *,
        request: EngineAppRequest,
        phase: RunPhase,
        offline_context_date: str,
        runtime_readiness: dict[str, object],
    ) -> LateStartAuctionRecoveryResult:
        if not self._should_attempt_late_start_auction_recovery(request=request, phase=phase):
            return LateStartAuctionRecoveryResult(status="not_applicable", executed=False)
        token = f"{request.trade_date}:late_start_auction_recovery"
        if self._last_late_start_auction_recovery_token == token:
            return LateStartAuctionRecoveryResult(status="already_checked", executed=False)
        self._last_late_start_auction_recovery_token = token
        if bool(runtime_readiness.get("auction_anchor_ready")):
            return LateStartAuctionRecoveryResult(
                status="already_ready",
                executed=False,
                notes=(
                    "late_start_auction_recovery | status=already_ready | action=skip_recovery",
                    "auction_anchor_source | status=existing_before_intraday_start | source=redis_runtime",
                ),
            )
        replay_result = self._auction_runtime.execute_auction_followup_0926(
            trade_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            offline_context_date=offline_context_date,
        )
        recovered = bool(replay_result.auction_result is not None and replay_result.auction_result.rows)
        status = "recovered" if recovered else "still_missing"
        notes = [
            f"late_start_auction_recovery | status={status} | minute={request.now.strftime('%H:%M')} | action=followup_0926_replay",
            (
                "auction_anchor_source | status=recovered_after_late_start | "
                "source=auction_followup_0926"
                if recovered
                else "auction_anchor_source | status=missing_after_late_start_recovery | source=auction_followup_0926"
            ),
            *replay_result.notes,
        ]
        return LateStartAuctionRecoveryResult(
            status=status,
            executed=True,
            notes=tuple(notes),
            yest_limit_result=replay_result.yest_limit_result,
            hot_plate_result=replay_result.hot_plate_result,
            auction_result=replay_result.auction_result,
            market_runtime_summary_result=replay_result.market_runtime_summary_result,
        )

    def run(self, request: EngineAppRequest) -> EngineAppResult:
        loop_decision = self._build_loop_decision(request.now, request.trade_date)
        preflight_notes = list(
            self._cleanup_auction_temp_state_if_needed(
                now=request.now,
                trade_date=request.trade_date,
            )
        )
        requested_symbols = _dedupe_symbols(request.symbols)
        discovered_symbols = requested_symbols or self._discover_runtime_symbols(request.trade_date, request.previous_trade_date)
        symbols = discovered_symbols if requested_symbols else self._filter_active_runtime_symbols(
            discovered_symbols,
            now=request.now,
            trade_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            historical_replay=request.historical_replay,
        )
        active_universe_dropped = max(len(discovered_symbols) - len(symbols), 0)
        should_expand_full_universe = (
            not requested_symbols
            and (
                request.now.time() < PREMARKET_HEAVY_SYNC_CUTOFF
                or request.now.time().strftime("%H:%M") >= "15:00"
            )
            and len(symbols) < self.MIN_FULL_UNIVERSE_SIZE
        )
        expanded_full_universe = False
        if should_expand_full_universe:
            full_universe = self._discover_full_universe_symbols()
            if full_universe:
                symbols = _dedupe_symbols((*symbols, *full_universe))
                expanded_full_universe = True
                logger.debug("startup symbol universe expanded by union with full F10 pool | symbols=%s", len(symbols))
        offline_context_date = request.offline_context_date or request.previous_trade_date
        audit_token = loop_decision.audit_token
        should_run_lifecycle_audit = loop_decision.should_run_lifecycle_audit
        if should_run_lifecycle_audit or loop_decision.scheduled_event_name:
            logger.debug(
                "engine_next cycle start | now=%s | trade_date=%s | previous_trade_date=%s",
                request.now.strftime("%Y-%m-%d %H:%M:%S"),
                request.trade_date,
                request.previous_trade_date,
            )
            logger.debug(
                "runtime loop decision | event=%s | label=%s | audit=%s",
                loop_decision.name,
                loop_decision.label,
                loop_decision.should_run_lifecycle_audit,
            )
        bootstrap_result = self._startup_bootstrap.execute(
            StartupBootstrapRequest(
                now=request.now,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                symbols=symbols,
                offline_context_date=offline_context_date,
                environment=request.environment,
                watermark_snapshot=request.watermark_snapshot,
                kline_watermarks=request.kline_watermarks,
                factor_watermarks=request.factor_watermarks,
                redis_factor_cache_ready=request.redis_factor_cache_ready,
                yest_limit_pool_ready=request.yest_limit_pool_ready,
                hot_plates_ready=request.hot_plates_ready,
                hot_plates_live_fresh=False,
                stock_plate_mapping_ready=request.stock_plate_mapping_ready,
                auction_anchor_ready=request.auction_anchor_ready,
                redis_chip_ready_count=request.redis_chip_ready_count,
                redis_dde_ready_count=request.redis_dde_ready_count,
                live_target_session=(not request.historical_replay) and _is_live_target_session(request.now, request.trade_date),
                cached_listing_dates=request.cached_listing_dates,
                cached_kline_row_counts=request.cached_kline_row_counts,
                cached_structural_factor_gap=request.cached_structural_factor_gap,
            ),
            should_run_lifecycle_audit=should_run_lifecycle_audit,
            audit_token=audit_token,
        )
        startup_bundle = bootstrap_result.startup_bundle
        watermark_snapshot = bootstrap_result.watermark_snapshot
        runtime_readiness = bootstrap_result.runtime_readiness

        redis_factor_cache_ready = request.redis_factor_cache_ready or dict(runtime_readiness["redis_factor_cache_ready"])
        effective_offline_request = OfflineSyncRequest(
            now=request.now,
            target_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            symbols=symbols,
            kline_watermarks=request.kline_watermarks or watermark_snapshot.kline_latest_dates,
            factor_watermarks=request.factor_watermarks or watermark_snapshot.factor_latest_dates,
            redis_factor_cache_ready=redis_factor_cache_ready,
            environment=request.environment,
        )

        phase = infer_run_phase(request.now)
        scheduled_event_result = self._execute_scheduled_event(
            loop_decision=loop_decision,
            request=request,
            phase=phase,
            offline_context_date=offline_context_date,
        )
        if scheduled_event_result.executed:
            startup_bundle, watermark_snapshot, runtime_readiness = self._refresh_startup_state_after_settlement(
                request=request,
                symbols=symbols,
                offline_context_date=offline_context_date,
                startup_bundle=startup_bundle,
            )
        late_start_recovery_result = self._ensure_late_start_auction_recovery(
            request=request,
            phase=phase,
            offline_context_date=offline_context_date,
            runtime_readiness=runtime_readiness,
        )
        if late_start_recovery_result.executed:
            startup_bundle, watermark_snapshot, runtime_readiness = self._refresh_startup_state_after_settlement(
                request=request,
                symbols=symbols,
                offline_context_date=offline_context_date,
                startup_bundle=startup_bundle,
            )
        offline_decision = startup_bundle.plan.offline_decision or self._offline_executor.build_decision(effective_offline_request)
        should_audit_integrated_sync = self._settlement.should_audit_integrated_sync(
            phase=phase,
            now=request.now,
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_name=scheduled_event_result.name,
            scheduled_event_executed=scheduled_event_result.executed,
        )
        if request.historical_replay and phase == RunPhase.POSTMARKET:
            should_audit_integrated_sync = False
        settlement_result = self._settlement.execute(
            request=effective_offline_request,
            phase=phase,
            should_audit_integrated_sync=should_audit_integrated_sync,
            integrated_sync_requested=request.run_integrated_sync,
            requested_symbols=requested_symbols,
            offline_decision=offline_decision,
            watermark_snapshot=watermark_snapshot,
        )
        integrated_sync_results = settlement_result.integrated_sync_results
        effective_sync_symbols = settlement_result.effective_sync_symbols
        integrated_sync_allowed = settlement_result.integrated_sync_allowed
        if integrated_sync_results:
            startup_bundle, watermark_snapshot, runtime_readiness = self._refresh_startup_state_after_settlement(
                request=request,
                symbols=symbols,
                offline_context_date=offline_context_date,
                startup_bundle=startup_bundle,
            )
        recap_result = self._night_recap.execute(
            phase=phase,
            now=request.now,
            trade_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            settlement_ready=settlement_result.recap_ready,
        )

        should_render_cycle = self._should_render_cycle(
            request=request,
            phase=phase,
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_result=scheduled_event_result,
        )
        open_2m_refresh_notes = self._refresh_open_2m_runtime_summary_if_needed(
            request=request,
            phase=phase,
            offline_context_date=offline_context_date,
        )
        live_runtime_result = self._live_runtime.execute(
            LiveRuntimeRequest(
                phase=phase,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
                symbols=symbols,
                now=request.now,
                minute_index=request.minute_index,
                require_auction_recovery=request.require_auction_recovery,
            ),
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_executed=scheduled_event_result.executed,
            should_render_cycle=should_render_cycle,
        )
        intraday_context = live_runtime_result.intraday_context
        primed_runtime_state: PrimedIntradayRuntimeState | None = live_runtime_result.primed_runtime_state
        opening_validation_notes = self._persist_opening_validation_checkpoint_if_needed(
            request=request,
            phase=phase,
            intraday_context=intraday_context,
        )

        runtime_readiness_label = self._derive_runtime_readiness(
            phase=phase,
            runtime_readiness=runtime_readiness,
            primed_runtime_state=primed_runtime_state,
        )
        quote_freshness_line = render_quote_freshness_line(
            primed_runtime_state,
            symbol_count=len(symbols),
        )
        auction_recap_notes: list[str] = []
        if self._should_emit_intraday_startup_auction_recap(
            phase=phase,
            trade_date=request.trade_date,
            now=request.now,
            lifecycle_audit_ran=should_run_lifecycle_audit,
        ):
            recap_lines = self._auction_runtime.render_auction_view(intraday_context)
            if recap_lines:
                auction_recap_notes = [
                    "runtime_event=intraday startup auction recap",
                    "startup_recap_scope | startup replay of auction snapshot; not the main intraday view",
                    *recap_lines,
                ]
        notes = [f"runtime_event={loop_decision.label}"]
        if late_start_recovery_result.notes:
            notes.extend(late_start_recovery_result.notes)
        if opening_validation_notes:
            notes.extend(opening_validation_notes)
        if open_2m_refresh_notes:
            notes.extend(open_2m_refresh_notes)
        is_auction_takeover = should_run_lifecycle_audit and phase == RunPhase.AUCTION
        is_postmarket_takeover = should_run_lifecycle_audit and phase == RunPhase.POSTMARKET
        if is_auction_takeover:
            notes = list(
                self._render_auction_takeover_summary(
                    runtime_readiness_label=runtime_readiness_label,
                    symbols=len(symbols),
                    intraday_context=intraday_context,
                    primed_runtime_state=primed_runtime_state,
                )
            )
        elif is_postmarket_takeover:
            notes = list(
                self._render_postmarket_takeover_summary(
                    runtime_readiness_label=runtime_readiness_label,
                    symbols=len(symbols),
                    intraday_context=intraday_context,
                    primed_runtime_state=primed_runtime_state,
                    now=request.now,
                )
            )
        elif should_run_lifecycle_audit:
            notes.extend(
                [
                    self._startup_coordinator.render_console_summary(startup_bundle.plan),
                    self._render_runtime_cache_counters(
                        runtime_readiness=runtime_readiness,
                        symbol_count=len(symbols),
                    ),
                    f"formal_readiness={startup_bundle.plan.report.readiness.value} | runtime_readiness={runtime_readiness_label}",
                    f"symbols={len(symbols)}",
                    f"integrated_sync={len(integrated_sync_results)}",
                    f"context={'ready' if intraday_context is not None else 'skipped'}",
                ]
            )
            if quote_freshness_line:
                notes.append(quote_freshness_line)
            render_execution_summary = getattr(self._startup_coordinator, "render_execution_summary", None)
            if not is_auction_takeover and not is_postmarket_takeover and callable(render_execution_summary):
                notes.extend(render_execution_summary(startup_bundle))
            if request.run_integrated_sync and should_audit_integrated_sync and not integrated_sync_allowed:
                notes.append(
                    f"integrated_sync deferred: effective_targets={len(effective_sync_symbols)} | "
                    f"pipe={settlement_result.sync_pipeline_targets} | "
                    f"net={settlement_result.sync_network_targets} | "
                    f"calc={settlement_result.sync_analytics_targets} | "
                    f"factor_cache_gap={settlement_result.sync_factor_cache_gaps} | "
                    f"load={settlement_result.sync_load_units} | "
                    f"auto_discovered_universe={len(symbols)} | safe_limit={self.AUTO_DISCOVERED_SYNC_LIMIT}"
                )
            if settlement_result.settlement_cached:
                notes.append(f"settlement_cached | trade_date={request.trade_date} | integrated_sync=reused")
            elif settlement_result.settlement_running:
                notes.append(f"settlement_running | trade_date={request.trade_date} | integrated_sync=skipped_while_in_progress")
            elif request.run_integrated_sync and should_audit_integrated_sync and effective_sync_symbols:
                notes.append(
                    f"integrated_sync targets={len(effective_sync_symbols)} | "
                    f"pipe={settlement_result.sync_pipeline_targets} | "
                    f"net={settlement_result.sync_network_targets} | "
                    f"calc={settlement_result.sync_analytics_targets} | "
                    f"factor_cache_gap={settlement_result.sync_factor_cache_gaps} | "
                    f"load={settlement_result.sync_load_units}"
                )
            notes.extend(self._summarize_integrated_sync(integrated_sync_results))
            if self._is_opening_strategy_window(request.now, phase):
                quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
                native_count = _native_ingested_count(primed_runtime_state)
                notes.extend(
                    self._auction_runtime.render_opening_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=None,
                    )
                )
            elif phase == RunPhase.PREMARKET:
                quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
                native_count = _native_ingested_count(primed_runtime_state)
                notes.extend(
                    self._auction_runtime.render_premarket_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=None,
                        startup_report=startup_bundle.plan.report,
                        historical_only=(runtime_readiness_label == "historical_context_only"),
                    )
                )
            elif phase == RunPhase.INTRADAY:
                quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
                native_count = _native_ingested_count(primed_runtime_state)
                notes.extend(
                    self._auction_runtime.render_intraday_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=None,
                    )
                )
        else:
            quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
            native_count = _native_ingested_count(primed_runtime_state)
            if phase == RunPhase.AUCTION:
                notes.extend(
                    self._auction_runtime.render_auction_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=quote_freshness_line,
                    )
                )
            elif self._is_opening_strategy_window(request.now, phase):
                notes.extend(
                    self._auction_runtime.render_opening_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=quote_freshness_line,
                    )
                )
            elif phase == RunPhase.PREMARKET:
                notes.extend(
                    self._auction_runtime.render_premarket_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=quote_freshness_line,
                        startup_report=startup_bundle.plan.report,
                        historical_only=(runtime_readiness_label == "historical_context_only"),
                    )
                )
            elif phase == RunPhase.INTRADAY:
                notes.extend(
                    self._auction_runtime.render_intraday_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=quote_freshness_line,
                    )
                )
            elif phase == RunPhase.POSTMARKET:
                notes.extend(
                    self._auction_runtime.render_postmarket_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        native=native_count,
                        now=request.now,
                        quote_freshness_line=self._render_postmarket_snapshot_line(
                            primed_runtime_state=primed_runtime_state,
                            symbol_count=len(symbols),
                        ),
                    )
                )
            else:
                notes.extend(
                    self._live_summary_renderer.render(
                        phase=phase,
                        intraday_context=intraday_context,
                        primed_runtime_state=primed_runtime_state,
                        runtime_readiness_label=runtime_readiness_label,
                        symbol_count=len(symbols),
                    )
                )
        if expanded_full_universe:
            notes.append(f"startup universe expanded to full F10 pool: {len(symbols)}")
        elif active_universe_dropped > 0:
            notes.append(
                f"active_universe | selected={len(symbols)} | discovered={len(discovered_symbols)} | dropped={active_universe_dropped}"
            )
        if preflight_notes:
            notes = list(preflight_notes) + notes
        if auction_recap_notes:
            notes = auction_recap_notes + notes
        notes.extend(self._render_settlement_quality(settlement_result.settlement_payload))
        notes.extend(self._render_scheduled_event_summary(scheduled_event_result))
        if phase == RunPhase.POSTMARKET:
            notes.extend(self._night_recap.render_summary(recap_result))
        phase_events = tuple(iter_phase_events(phase)) if should_run_lifecycle_audit else ()
        return EngineAppResult(
            phase=phase,
            startup_bundle=startup_bundle,
            watermark_snapshot=watermark_snapshot,
            integrated_sync_results=integrated_sync_results,
            intraday_context=intraday_context,
            phase_events=phase_events,
            loop_event=loop_decision.name,
            loop_event_label=loop_decision.label,
            lifecycle_audit_ran=bootstrap_result.lifecycle_audit_ran,
            used_cached_startup_state=bootstrap_result.used_cached_startup_state,
            should_render=should_render_cycle,
            notes=tuple(notes),
        )

    def run_forever(
        self,
        request_builder,
        *,
        interval_seconds: int = DEFAULT_LOOP_INTERVAL_SECONDS,
        max_cycles: int | None = None,
    ) -> None:
        cycles = 0
        while max_cycles is None or cycles < max_cycles:
            request = request_builder()
            result = self.run(request)
            if result.should_render:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}]", flush=True)
                print(render_result_summary(result), flush=True)
                print("", flush=True)
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
            sleep_seconds = self._resolve_loop_sleep_seconds(
                now=request.now,
                phase=result.phase,
                default_interval_seconds=interval_seconds,
            )
            time.sleep(sleep_seconds)


def build_default_request(
    *,
    now: datetime | None = None,
    trade_date: str | None = None,
    previous_trade_date: str | None = None,
    symbols: Iterable[str] = (),
    environment: ExecutionEnvironment = ExecutionEnvironment.SERVER,
    offline_context_date: str | None = None,
    minute_index: int | None = None,
    require_auction_recovery: bool = False,
    run_integrated_sync: bool = True,
) -> EngineAppRequest:
    wall_now = now or datetime.now()
    if trade_date:
        effective_trade_date = trade_date
    else:
        calendar = TradingCalendarService()
        today_str = wall_now.strftime("%Y-%m-%d")
        effective_trade_date = today_str if calendar.is_trading_day(today_str) else calendar.get_previous_trading_day(today_str)
    effective_now = _resolve_default_request_now(
        wall_now=wall_now,
        effective_trade_date=effective_trade_date,
        now_explicit=now is not None,
    )
    historical_replay = _is_historical_replay_request(
        wall_now=wall_now,
        effective_trade_date=effective_trade_date,
        now_explicit=now is not None,
    )
    effective_previous_trade_date = previous_trade_date or _default_previous_trade_date(effective_trade_date)
    effective_symbols = _dedupe_symbols(symbols)
    return EngineAppRequest(
        now=effective_now,
        trade_date=effective_trade_date,
        previous_trade_date=effective_previous_trade_date,
        symbols=effective_symbols,
        environment=environment,
        offline_context_date=offline_context_date or effective_previous_trade_date,
        minute_index=minute_index if minute_index is not None else _infer_minute_index(effective_now),
        require_auction_recovery=require_auction_recovery,
        historical_replay=historical_replay,
        run_integrated_sync=run_integrated_sync,
    )


def render_result_summary(result: EngineAppResult) -> str:
    phase_text = {
        RunPhase.PREMARKET: "盘前",
        RunPhase.AUCTION: "竞价",
        RunPhase.INTRADAY: "盘中",
        RunPhase.POSTMARKET: "盘后",
        RunPhase.NIGHT: "夜间",
    }.get(result.phase, result.phase.value)

    def parse_quote_snapshot(line: str | None) -> tuple[int, str, int | None]:
        if not line:
            return 0, "", None
        matched = re.search(
            r"fresh=(\d+)/\d+\s*\|\s*stale=\d+\s*(?:\|\s*cache_only=\d+\s*)?\|\s*missing=\d+\s*\|\s*latest=([^|]+)\s*\|\s*lag=([0-9]+)s",
            line,
        )
        if not matched:
            return 0, "", None
        fresh_text, latest_text, lag_text = matched.groups()
        try:
            fresh_count = int(fresh_text or 0)
        except (TypeError, ValueError):
            fresh_count = 0
        try:
            lag_seconds = int(lag_text or 0)
        except (TypeError, ValueError):
            lag_seconds = None
        return fresh_count, str(latest_text or "").strip(), lag_seconds

    def auction_live_quote_ready(line: str | None) -> bool:
        fresh_count, latest_hms, lag_seconds = parse_quote_snapshot(line)
        if result.phase != RunPhase.AUCTION:
            return True
        if fresh_count <= 0 or not latest_hms or lag_seconds is None:
            return False
        if lag_seconds > 120:
            return False
        if not re.fullmatch(r"\d{2}:\d{2}:\d{2}", latest_hms):
            return False
        now_hms = datetime.now().strftime("%H:%M:%S")
        if latest_hms < "09:15:00" or latest_hms >= "15:00:00":
            return False
        if latest_hms > now_hms:
            return False
        return True

    raw_lines: list[str] = []
    for note in result.notes:
        raw_lines.extend(str(note).splitlines())

    if result.phase == RunPhase.INTRADAY:
        strategy_phase_line = next((line for line in raw_lines if "| 阶段=" in line), "")
        if "开盘确认" in strategy_phase_line:
            phase_text = "开盘确认"

    def find_first(*prefixes: str) -> str | None:
        for line in raw_lines:
            if any(line.startswith(prefix) for prefix in prefixes):
                return line
        return None

    def collect_block(start_prefix: str, stop_prefixes: tuple[str, ...]) -> list[str]:
        start = -1
        for idx, line in enumerate(raw_lines):
            if line.startswith(start_prefix):
                start = idx
                break
        if start < 0:
            return []
        block: list[str] = []
        for idx in range(start, len(raw_lines)):
            line = raw_lines[idx]
            if idx > start and any(line.startswith(prefix) for prefix in stop_prefixes):
                break
            block.append(line)
        return block

    def trim_prefixed_body(line: str, prefix: str) -> str:
        body = line[len(prefix):].strip()
        return body.lstrip("| ").strip()

    def format_diagnostic_line(prefix: str, line: str) -> str:
        body = trim_prefixed_body(line, prefix)
        if prefix == "integrated_sync targets=" and body:
            target, _, remainder = body.partition("|")
            target = target.strip()
            remainder = remainder.strip()
            if remainder:
                return f"targets={target} | {remainder}"
            return f"targets={target}"
        return body

    engineering_stop_prefixes = (
        "active_universe |",
        "startup universe expanded to full F10 pool:",
        "settlement_quality ",
        "recap_wait |",
        "recap_status=",
        "recap_failed |",
        "recap_enrichment |",
        "recap_degraded |",
        "recap_sources |",
        "self_check ",
        "integrated_sync targets=",
        "sync.summary |",
        "sync.datasets |",
        "sync.partial_symbols |",
        "sync.failed_symbols |",
        "sync.failed_root |",
        "sync.root_summary |",
        "上下文快照=",
        "phase_events:",
    )

    status_line = find_first("状态摘要 |")
    data_gap_line = find_first("数据缺口 |")
    impact_line = find_first("影响判断 |")
    sync_digest_line = find_first("同步结果 |")
    quote_line = find_first("quote_freshness |")
    runtime_line = find_first("运行状态=")
    integrated_sync_line = find_first("integrated_sync targets=")
    late_start_recovery_line = find_first("late_start_auction_recovery |")
    auction_anchor_source_line = find_first("auction_anchor_source |")

    target_trade_date = ""
    formal_date = ""
    readiness = ""
    if status_line:
        matched = re.search(
            r"目标交易日=(\d{4}-\d{2}-\d{2})，正式离线日=(\d{4}-\d{2}-\d{2})，readiness=([A-Za-z0-9_]+)",
            status_line,
        )
        if matched:
            target_trade_date, formal_date, readiness = matched.groups()

    runtime_state = ""
    quote_coverage = ""
    native_count = ""
    if runtime_line:
        matched = re.search(r"运行状态=([^|]+)\s*\|\s*行情=([^|]+)\s*\|\s*(?:Native|Rust)=(.+)$", runtime_line)
        if matched:
            runtime_state, quote_coverage, native_count = [part.strip() for part in matched.groups()]

    latest_quote = ""
    lag_seconds = ""
    missing_quotes = ""
    fresh_quotes = ""
    stale_quotes = ""
    if quote_line:
        matched = re.search(
            r"fresh=(\d+)/\d+\s*\|\s*stale=(\d+)\s*(?:\|\s*cache_only=\d+\s*)?\|\s*missing=(\d+)\s*\|\s*latest=([^|]+)\s*\|\s*lag=(\d+)s",
            quote_line,
        )
        if matched:
            fresh_quotes, stale_quotes, missing_quotes, latest_quote, lag_seconds = [part.strip() for part in matched.groups()]
    auction_waiting_live = result.phase == RunPhase.AUCTION and not auction_live_quote_ready(quote_line)
    if auction_waiting_live:
        phase_text = "等待竞价"
        runtime_state = "等待实时恢复"

    overview_section = [
        f"当前阶段：{phase_text}",
        f"系统状态：{runtime_state or '-'}",
    ]
    if target_trade_date:
        overview_section.append(f"目标交易日：{target_trade_date}")
    if formal_date:
        overview_section.append(f"正式数据日：{formal_date}")
    quote_summary_bits: list[str] = []
    if quote_coverage:
        quote_summary_bits.append(f"覆盖 {quote_coverage}")
    if latest_quote:
        quote_summary_bits.append(f"最新 {latest_quote}")
    if lag_seconds:
        quote_summary_bits.append(f"滞后 {lag_seconds} 秒")
    if missing_quotes:
        quote_summary_bits.append(f"缺失 {missing_quotes} 只")
    if stale_quotes:
        quote_summary_bits.append(f"只读缓存 {stale_quotes} 只")
    if fresh_quotes:
        quote_summary_bits.append(f"实时可用 {fresh_quotes} 只")
    if native_count:
        quote_summary_bits.append(f"Native {native_count}")
    if quote_summary_bits:
        overview_section.append("行情状态：" + "，".join(quote_summary_bits))
    if auction_waiting_live:
        overview_section.append("竞价状态：未恢复到当日09:15+有效实时流，暂不输出竞价策略。")
    if readiness:
        overview_section.append(f"就绪等级：{readiness}")

    gap_section: list[str] = []
    if data_gap_line:
        gap_section.append("数据缺口：" + data_gap_line.split("|", 1)[1].strip())
    if sync_digest_line:
        gap_section.append("同步结果：" + sync_digest_line.split("|", 1)[1].strip())
    if impact_line:
        gap_section.append("影响判断：" + impact_line.split("|", 1)[1].strip())

    gap_section = []
    normalized_impact_line = ""
    if sync_digest_line:
        sync_payload = sync_digest_line.split("|", 1)[1].strip()
        gap_section.append("同步结果：" + sync_payload)
        number_matches = [int(value) for value in re.findall(r"\d+", sync_payload)]
        if len(number_matches) >= 7:
            _, _, _, remaining_kline, remaining_dde, remaining_factor, remaining_chip = number_matches[:7]
            remaining_parts: list[str] = []
            if remaining_kline > 0:
                remaining_parts.append(f"日线缺{remaining_kline}只")
            if remaining_dde > 0:
                remaining_parts.append(f"DDE缺{remaining_dde}只")
            if remaining_factor > 0:
                remaining_parts.append(f"因子缺{remaining_factor}只")
            if remaining_chip > 0:
                remaining_parts.append(f"筹码缺{remaining_chip}只")
            if remaining_parts:
                normalized_impact_line = (
                    "影响判断：当前以同步后剩余缺口为准，"
                    + "，".join(remaining_parts)
                    + "；适合先看历史上下文和缺口修复。"
                )
            else:
                normalized_impact_line = "影响判断：当前以同步后状态为准，正式离线链路已补齐，可按正常盘前视图使用。"
    elif data_gap_line:
        gap_section.append("数据缺口：" + data_gap_line.split("|", 1)[1].strip())
    if normalized_impact_line:
        gap_section.append(normalized_impact_line)
    elif impact_line:
        gap_section.append("影响判断：" + impact_line.split("|", 1)[1].strip())

    if auction_anchor_source_line:
        gap_section.append("auction_anchor | " + auction_anchor_source_line.split("|", 1)[1].strip())

    strategy_lines: list[str] = []
    for prefix in ("策略看板 |", "情绪总览 |"):
        line = find_first(prefix)
        if line:
            strategy_lines.append(line)
    plan_mode_line = find_first("盘前预案 |")
    if plan_mode_line:
        strategy_lines.append(plan_mode_line)
    strategy_lines.extend(
        collect_block(
            "【收盘定性】",
            ("【全天回放】", "【主线复盘】", "【今日热点】", "【涨停板块】", "【竞价结局】", "【高位梯队复盘】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【全天回放】",
            ("【主线复盘】", "【今日热点】", "【涨停板块】", "【竞价结局】", "【高位梯队复盘】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【主线复盘】",
            ("【今日热点】", "【涨停板块】", "【竞价结局】", "【高位梯队复盘】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【今日热点】",
            ("【涨停板块】", "【竞价结局】", "【高位梯队复盘】", "【高标生死簿】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【涨停板块】",
            ("【竞价结局】", "【高位梯队复盘】", "【高标生死簿】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【竞价结局】",
            ("【高位梯队复盘】", "【高标生死簿】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【高位梯队复盘】",
            ("【高标生死簿】", "【梯队映射】", "【明日预案】", "【明日观察池】", "【复盘风控】", "【风险提示】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【主线脉络】",
            ("【高标生死簿】", "【题材分桶】", "【题材层级】", "【梯队映射】", "【核心观察池】", "【明日观察池】", "【风险提示】", "【复盘风控】"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【高标生死簿】",
            (
                "【竞价极值榜】",
                "【承接转强榜】",
                "【题材分桶】",
                "【题材层级】",
                "【梯队映射】",
                "【核心观察池】",
                "【明日观察池】",
                "【风险提示】",
                "【复盘风控】",
            ),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【梯队映射】",
            ("【明日预案】", "【核心观察池】", "【明日观察池】", "【风险提示】", "【复盘风控】", "上下文快照", "phase_events:"),
        )
    )
    strategy_lines.extend(
        collect_block(
            "【明日预案】",
            ("【核心观察池】", "【明日观察池】", "【风险提示】", "【复盘风控】", "上下文快照=", "phase_events:"),
        )
    )
    focus_block = collect_block(
        "【核心观察池】",
        ("【风险提示】", "【复盘风控】", "上下文快照=", "phase_events:"),
    )
    if not focus_block:
        focus_block = collect_block(
            "【明日观察池】",
            ("【风险提示】", "【复盘风控】", "上下文快照=", "phase_events:"),
        )
    strategy_lines.extend(focus_block)
    strategy_lines.extend(
        collect_block(
            "【风险提示】",
            engineering_stop_prefixes,
        )
    )
    strategy_lines.extend(
        collect_block(
            "【复盘风控】",
            engineering_stop_prefixes,
        )
    )

    detailed_section: list[str] = []
    if result.lifecycle_audit_ran and find_first("self_check "):
        detailed_section.append("详细诊断：")
        diagnostic_map = {
            "self_check ": "自检缺口 | ",
            "self_check.sync_scope ": "同步范围 | ",
            "self_check.factor_breakdown ": "因子拆解 | ",
            "integrated_sync targets=": "本轮同步 | ",
            "sync.datasets ": "同步明细 | ",
            "sync.partial_symbols ": "部分补齐 | ",
            "sync.failed_symbols ": "失败个股 | ",
            "sync.failed_root ": "失败根因明细 | ",
            "sync.root_summary ": "失败根因汇总 | ",
        }
        for prefix, label in diagnostic_map.items():
            line = find_first(prefix)
            if line:
                detailed_section.append(label + format_diagnostic_line(prefix, line))

    if late_start_recovery_line:
        if not detailed_section:
            detailed_section.append("详细诊断：")
        detailed_section.append("late_start_recovery | " + format_diagnostic_line("late_start_auction_recovery |", late_start_recovery_line))
    if auction_anchor_source_line:
        if not detailed_section:
            detailed_section.append("详细诊断：")
        detailed_section.append("auction_anchor_source | " + format_diagnostic_line("auction_anchor_source |", auction_anchor_source_line))

    phase_section: list[str] = []
    if result.phase_events:
        phase_section.append("时序提示：")
        for event in result.phase_events[:3]:
            phase_section.append(f"- {event.time_window} | {event.component} | {event.action}")

    output: list[str] = []
    output.extend(overview_section)
    if gap_section:
        output.append("")
        output.extend(gap_section)
    if strategy_lines:
        output.append("")
        output.extend(strategy_lines)
    if detailed_section:
        output.append("")
        output.extend(detailed_section)
    if phase_section:
        output.append("")
        output.extend(phase_section)
    return "\n".join(output)


def _parse_now(value: str | None) -> datetime | None:
    if not value:
        return None
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(value, pattern)
        except ValueError:
            continue
    raise ValueError(f"Unsupported --now format: {value}")


def _read_symbols_args(symbols: Iterable[str], symbols_file: str | None) -> tuple[str, ...]:
    materialized = list(symbols)
    if symbols_file:
        for line in Path(symbols_file).read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if text:
                materialized.append(text)
    return _dedupe_symbols(materialized)


def _discover_default_symbols_file() -> str | None:
    candidates = (
        Path.cwd() / "symbols.txt",
        Path.cwd() / "watchlist.txt",
        Path(__file__).resolve().parent / "symbols.txt",
        Path(__file__).resolve().parent / "watchlist.txt",
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return None


def _build_request_from_args(args) -> tuple[EngineAppRequest, str | None]:
    default_symbols_file = _discover_default_symbols_file()
    symbols_file = args.symbols_file or default_symbols_file
    symbols = _read_symbols_args(args.symbols, symbols_file)
    request = build_default_request(
        now=_parse_now(args.now),
        trade_date=args.trade_date,
        previous_trade_date=args.previous_trade_date,
        symbols=symbols,
        environment=ExecutionEnvironment(args.environment),
        offline_context_date=args.offline_context_date,
        minute_index=args.minute_index,
        require_auction_recovery=args.require_auction_recovery,
        run_integrated_sync=not args.no_integrated_sync,
    )
    return request, symbols_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the minimal engine_next stage orchestrator.")
    parser.add_argument("--now", dest="now", default=None)
    parser.add_argument("--trade-date", dest="trade_date", default=None)
    parser.add_argument("--previous-trade-date", dest="previous_trade_date", default=None)
    parser.add_argument("--symbols", nargs="*", default=())
    parser.add_argument("--symbols-file", dest="symbols_file", default=None)
    parser.add_argument("--offline-context-date", dest="offline_context_date", default=None)
    parser.add_argument("--minute-index", dest="minute_index", type=int, default=None)
    parser.add_argument("--require-auction-recovery", action="store_true")
    parser.add_argument("--no-integrated-sync", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop-interval", dest="loop_interval", type=int, default=EngineApp.DEFAULT_LOOP_INTERVAL_SECONDS)
    parser.add_argument(
        "--environment",
        choices=(ExecutionEnvironment.SERVER.value, ExecutionEnvironment.LOCAL_WINDOWS.value),
        default=ExecutionEnvironment.SERVER.value,
    )
    args = parser.parse_args(argv)

    app = EngineApp()

    if args.once:
        request, symbols_file = _build_request_from_args(args)
        result = app.run(request)
        print(render_result_summary(result))
        if symbols_file and not args.symbols and not args.symbols_file:
            print(f"symbols_file={symbols_file}")
        return 0

    def request_builder() -> EngineAppRequest:
        request, _ = _build_request_from_args(args)
        return request

    app.run_forever(
        request_builder,
        interval_seconds=args.loop_interval,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
