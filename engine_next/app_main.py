from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
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
from engine_next.runtime.original_timeline import iter_phase_events
from engine_next.runtime.renderers.live_phase_summary_renderer import LivePhaseSummaryRenderer
from engine_next.runtime.startup_runtime_coordinator import (
    RuntimeStartupCoordinator,
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
    stock_plate_mapping_ready: bool = False
    auction_anchor_ready: bool = False
    redis_chip_ready_count: int = 0
    redis_dde_ready_count: int = 0
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
    STARTUP_AUDIT_CHECKPOINTS = ("08:30", "09:00", "09:25", "17:40")

    def __init__(
        self,
        *,
        startup_coordinator: RuntimeStartupCoordinator | None = None,
        offline_executor: ServerOnlyOfflineSyncExecutor | None = None,
        intraday_context_builder: IntradayContextBuilder | None = None,
        redis_client: object | None = None,
    ) -> None:
        self._startup_coordinator = startup_coordinator or RuntimeStartupCoordinator()
        self._offline_executor = offline_executor or ServerOnlyOfflineSyncExecutor()
        self._intraday_context_builder = intraday_context_builder or IntradayContextBuilder()
        self._redis_client = redis_client
        self._intraday_hub = IntradayDataHub(redis_client=redis_client)
        self._market_runtime_summary_service = MarketRuntimeSummaryService(redis_client=self.redis)
        self._auction_runtime = AuctionRuntimeController(
            intraday_hub=self._intraday_hub,
            market_runtime_summary_service=self._market_runtime_summary_service,
        )
        self._startup_bootstrap = StartupBootstrapController(
            startup_coordinator=self._startup_coordinator,
            offline_executor=self._offline_executor,
            runtime_readiness_loader=self._load_runtime_readiness,
        )
        self._live_runtime = LiveRuntimeController(
            intraday_context_builder=self._intraday_context_builder,
        )
        self._live_summary_renderer = LivePhaseSummaryRenderer()
        self._last_scheduled_event_token: str | None = None
        self._last_render_token: str | None = None

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
            if hasattr(self.redis, "keys"):
                values = self.redis.keys(pattern) or []
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

    def _safe_set(self, key: str, value: str) -> bool:
        try:
            if hasattr(self.redis, "set"):
                self.redis.set(key, value)
                return True
        except Exception:
            return False
        return False

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
        quote_keys = self._safe_keys("stock:quote:*")
        candidates.extend(key.replace("stock:quote:", "") for key in quote_keys)
        return _dedupe_symbols(candidates)

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

        return (
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
        )

    def _load_runtime_readiness(
        self,
        *,
        now: datetime,
        trade_date: str,
        previous_trade_date: str,
        offline_context_date: str,
        symbols: tuple[str, ...],
    ) -> dict[str, object]:
        factor_key = f"cache:stock_extra:{offline_context_date}"
        factor_ready_fields = set(self._safe_hkeys(factor_key))
        factor_ready = {symbol: symbol in factor_ready_fields for symbol in symbols}
        hot_plate_date = _hot_plate_cache_date(now, trade_date, previous_trade_date)
        return {
            "redis_factor_cache_ready": factor_ready,
            "yest_limit_pool_ready": self._safe_hlen(f"cache:yest_limit_pool:{previous_trade_date}") > 0,
            "hot_plates_ready": self._safe_hlen(f"cache:hot_plates:{hot_plate_date}") > 0,
            "stock_plate_mapping_ready": self._safe_hlen("market:stock_plate") >= self.MIN_STOCK_PLATE_MAPPING_COUNT,
            "auction_anchor_ready": self._safe_exists(f"market:auction:anchor:{trade_date.replace('-', '')}"),
            "redis_chip_ready_count": self._safe_hlen(f"cache:chip_peaks:{offline_context_date}"),
            "redis_dde_ready_count": self._safe_hlen(f"cache:dde_ready:{offline_context_date}"),
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
        if minute_tag == "09:26":
            return RuntimeLoopDecision(
                name="auction_analysis_checkpoint",
                label="auction replay checkpoint 09:26",
                audit_token=None,
                should_run_lifecycle_audit=False,
                scheduled_event_name="auction_replay_0926",
                scheduled_event_label="auction replay event 09:26",
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
        yest_limit_ready = bool(runtime_readiness.get("yest_limit_pool_ready"))
        stock_plate_ready = bool(runtime_readiness.get("stock_plate_mapping_ready"))
        auction_ready = bool(runtime_readiness.get("auction_anchor_ready"))
        redis_chip_ready_count = int(runtime_readiness.get("redis_chip_ready_count", 0) or 0)
        redis_dde_ready_count = int(runtime_readiness.get("redis_dde_ready_count", 0) or 0)
        return (
            (
                "cached_audit "
                f"| formal={report.formal_offline_date} "
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
            (
                "runtime_cache "
                f"| yest_limit_pool={'ok' if yest_limit_ready else 'missing'} "
                f"| hot_plates={'ok' if hot_plates_ready else 'missing'} "
                f"| stock_plate_mapping={'ok' if stock_plate_ready else 'missing'} "
                f"| auction_anchor={'ok' if auction_ready else 'missing'}"
            ),
        )

    def _derive_runtime_readiness(
        self,
        *,
        phase: RunPhase,
        runtime_readiness: dict[str, object],
    ) -> str:
        yest_limit_ready = bool(runtime_readiness.get("yest_limit_pool_ready"))
        hot_plates_ready = bool(runtime_readiness.get("hot_plates_ready"))
        stock_plate_ready = bool(runtime_readiness.get("stock_plate_mapping_ready"))
        auction_ready = bool(runtime_readiness.get("auction_anchor_ready"))

        if phase in (RunPhase.AUCTION, RunPhase.INTRADAY):
            if yest_limit_ready and hot_plates_ready and stock_plate_ready and auction_ready:
                return "trade_ready_runtime"
            if yest_limit_ready and stock_plate_ready:
                return "degraded_runtime"
            return "observe_runtime"

        if yest_limit_ready and stock_plate_ready and hot_plates_ready:
            return "trade_ready_runtime"
        if yest_limit_ready or stock_plate_ready:
            return "degraded_runtime"
        return "observe_runtime"

    def _render_postmarket_takeover_summary(
        self,
        *,
        runtime_readiness_label: str,
        symbols: int,
        intraday_context: IntradayContext | None,
        primed_runtime_state: PrimedIntradayRuntimeState | None,
    ) -> tuple[str, ...]:
        live_notes = list(
            self._live_summary_renderer.render(
                phase=RunPhase.POSTMARKET,
                intraday_context=intraday_context,
                primed_runtime_state=primed_runtime_state,
                runtime_readiness_label=runtime_readiness_label,
                symbol_count=symbols,
            )
        )
        if live_notes:
            first_line = live_notes[0]
            if first_line.startswith("runtime_readiness="):
                live_notes[0] = f"{first_line} | settlement_window=17:40+"
        return tuple(
            [
                "runtime_event=postmarket takeover",
                *live_notes,
            ]
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
        if self._last_render_token != token:
            self._last_render_token = token
            return True
        if phase not in (RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET):
            self._last_render_token = token
            return True
        return False

    def _should_audit_integrated_sync(
        self,
        *,
        phase: RunPhase,
        now: datetime,
        lifecycle_audit_ran: bool,
        scheduled_event_result: RuntimeScheduledEventResult,
    ) -> bool:
        if lifecycle_audit_ran and phase in (RunPhase.PREMARKET, RunPhase.NIGHT):
            return True
        if phase == RunPhase.POSTMARKET:
            if scheduled_event_result.name == "postmarket_settlement_1740" and scheduled_event_result.executed:
                return True
            return now.strftime("%H:%M") >= "17:40"
        return False

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

        if loop_decision.scheduled_event_name == "auction_replay_0926":
            replay_result = self._auction_runtime.execute_replay_0926(
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
            )
            yest_limit_result = replay_result.yest_limit_result
            auction_result = replay_result.auction_result
            market_runtime_summary_result = replay_result.market_runtime_summary_result
            notes.extend(replay_result.notes)
        elif loop_decision.scheduled_event_name == "market_close_1505":
            logger.info("scheduled event execute | name=%s", loop_decision.scheduled_event_name)
            state_written = self._safe_set("market:state:last_phase", "postmarket")
            date_written = self._safe_set("market:state:last_close_trade_date", request.trade_date)
            notes.append(
                f"15:05 close marker persisted | phase_key={'ok' if state_written else 'skip'} | date_key={'ok' if date_written else 'skip'}"
            )
        elif loop_decision.scheduled_event_name == "postmarket_settlement_1740":
            logger.info("scheduled event execute | name=%s", loop_decision.scheduled_event_name)
            hot_plate_result = self._intraday_hub.fetch_hot_plates(request.trade_date, RunPhase.POSTMARKET, today_mode=True)
            yest_limit_result = self._intraday_hub.fetch_yest_limit_pool(request.previous_trade_date, RunPhase.POSTMARKET)
            market_runtime_summary_result = self._market_runtime_summary_service.build_and_write(
                request.trade_date,
                offline_context_date=offline_context_date,
            )
            notes.append("17:40 settlement event refreshed postmarket hot plates, yesterday limit pool, and market runtime summary.")

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

    def run(self, request: EngineAppRequest) -> EngineAppResult:
        loop_decision = self._build_loop_decision(request.now, request.trade_date)
        requested_symbols = _dedupe_symbols(request.symbols)
        symbols = requested_symbols or self._discover_runtime_symbols(request.trade_date, request.previous_trade_date)
        should_expand_full_universe = (
            not requested_symbols
            and (
                request.now.time() < PREMARKET_HEAVY_SYNC_CUTOFF
                or request.now.time().strftime("%H:%M") >= "15:00"
            )
            and len(symbols) < self.MIN_FULL_UNIVERSE_SIZE
        )
        if should_expand_full_universe:
            full_universe = self._discover_full_universe_symbols()
            if full_universe:
                symbols = full_universe
                logger.info("startup symbol universe expanded from runtime set to full F10 pool | symbols=%s", len(symbols))
        offline_context_date = request.offline_context_date or request.previous_trade_date
        audit_token = loop_decision.audit_token
        should_run_lifecycle_audit = loop_decision.should_run_lifecycle_audit
        if should_run_lifecycle_audit or loop_decision.scheduled_event_name:
            logger.info(
                "engine_next cycle start | now=%s | trade_date=%s | previous_trade_date=%s",
                request.now.strftime("%Y-%m-%d %H:%M:%S"),
                request.trade_date,
                request.previous_trade_date,
            )
            logger.info(
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
                stock_plate_mapping_ready=request.stock_plate_mapping_ready,
                auction_anchor_ready=request.auction_anchor_ready,
                redis_chip_ready_count=request.redis_chip_ready_count,
                redis_dde_ready_count=request.redis_dde_ready_count,
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

        integrated_sync_results: tuple[IntegratedSyncResult, ...] = ()
        integrated_sync_allowed = bool(request.run_integrated_sync)
        phase = startup_bundle.plan.phase
        scheduled_event_result = self._execute_scheduled_event(
            loop_decision=loop_decision,
            request=request,
            phase=phase,
            offline_context_date=offline_context_date,
        )
        offline_decision = startup_bundle.plan.offline_decision or self._offline_executor.build_decision(effective_offline_request)
        effective_sync_symbols: tuple[str, ...] = ()
        should_audit_integrated_sync = self._should_audit_integrated_sync(
            phase=phase,
            now=request.now,
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_result=scheduled_event_result,
        )
        if should_audit_integrated_sync:
            effective_sync_symbols = self._offline_executor.resolve_effective_target_symbols(
                effective_offline_request,
                offline_decision,
            )
            logger.info(
                "integrated sync audit | phase=%s | universe=%s | effective_targets=%s",
                phase.value,
                len(symbols),
                len(effective_sync_symbols),
            )
        if (
            should_audit_integrated_sync
            and integrated_sync_allowed
            and not requested_symbols
            and len(symbols) > self.AUTO_DISCOVERED_SYNC_LIMIT
        ):
            allow_large_auto_discovered_sync = (
                phase == RunPhase.NIGHT
                or phase == RunPhase.POSTMARKET
                or (phase == RunPhase.PREMARKET and request.now.time() < PREMARKET_HEAVY_SYNC_CUTOFF)
            )
            if len(effective_sync_symbols) <= self.AUTO_DISCOVERED_SYNC_LIMIT:
                logger.info(
                    "integrated sync small-gap override | effective_targets=%s <= safe_limit=%s",
                    len(effective_sync_symbols),
                    self.AUTO_DISCOVERED_SYNC_LIMIT,
                )
            elif not allow_large_auto_discovered_sync:
                integrated_sync_allowed = False
        if request.run_integrated_sync:
            heavy_sync_allowed = phase in (RunPhase.PREMARKET, RunPhase.POSTMARKET, RunPhase.NIGHT)
            if should_audit_integrated_sync and integrated_sync_allowed and heavy_sync_allowed and effective_sync_symbols:
                logger.info(
                    "integrated sync start | phase=%s | effective_targets=%s | universe=%s",
                    phase.value,
                    len(effective_sync_symbols),
                    len(symbols),
                )
                integrated_sync_results = tuple(
                    self._offline_executor.execute_integrated_sync(
                        effective_offline_request,
                        watermark_snapshot=watermark_snapshot,
                    )
                )
                logger.info("integrated sync done | results=%s", len(integrated_sync_results))

        should_render_cycle = self._should_render_cycle(
            request=request,
            phase=phase,
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_result=scheduled_event_result,
        )
        live_runtime_result = self._live_runtime.execute(
            LiveRuntimeRequest(
                phase=startup_bundle.plan.phase,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
                symbols=symbols,
                minute_index=request.minute_index,
                require_auction_recovery=request.require_auction_recovery,
            ),
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_executed=scheduled_event_result.executed,
            should_render_cycle=should_render_cycle,
        )
        intraday_context = live_runtime_result.intraday_context
        primed_runtime_state: PrimedIntradayRuntimeState | None = live_runtime_result.primed_runtime_state

        runtime_readiness_label = self._derive_runtime_readiness(
            phase=phase,
            runtime_readiness=runtime_readiness,
        )
        notes = [f"runtime_event={loop_decision.label}"]
        is_postmarket_takeover = should_run_lifecycle_audit and phase == RunPhase.POSTMARKET
        if is_postmarket_takeover:
            notes = list(
                self._render_postmarket_takeover_summary(
                    runtime_readiness_label=runtime_readiness_label,
                    symbols=len(symbols),
                    intraday_context=intraday_context,
                    primed_runtime_state=primed_runtime_state,
                )
            )
        elif should_run_lifecycle_audit:
            notes.extend(
                [
                    self._startup_coordinator.render_console_summary(startup_bundle.plan),
                    f"formal_readiness={startup_bundle.plan.report.readiness.value} | runtime_readiness={runtime_readiness_label}",
                    f"symbols={len(symbols)}",
                    f"integrated_sync={len(integrated_sync_results)}",
                    f"context={'ready' if intraday_context is not None else 'skipped'}",
                ]
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
            if phase == RunPhase.AUCTION:
                notes.extend(self._auction_runtime.render_auction_view(intraday_context))
        if should_expand_full_universe:
            notes.append(f"startup universe expanded to full F10 pool: {len(symbols)}")
        render_execution_summary = getattr(self._startup_coordinator, "render_execution_summary", None)
        if should_run_lifecycle_audit and not is_postmarket_takeover and callable(render_execution_summary):
            notes.extend(render_execution_summary(startup_bundle))
        if request.run_integrated_sync and should_audit_integrated_sync and not integrated_sync_allowed:
            notes.append(
                f"integrated_sync deferred: effective_targets={len(effective_sync_symbols)} | "
                f"auto_discovered_universe={len(symbols)} | safe_limit={self.AUTO_DISCOVERED_SYNC_LIMIT}"
            )
        elif request.run_integrated_sync and should_audit_integrated_sync and effective_sync_symbols:
            notes.append(f"integrated_sync targets={len(effective_sync_symbols)}")
        notes.extend(self._render_scheduled_event_summary(scheduled_event_result))
        notes.extend(self._summarize_integrated_sync(integrated_sync_results))
        phase_events = tuple(iter_phase_events(startup_bundle.plan.phase)) if should_run_lifecycle_audit else ()
        return EngineAppResult(
            phase=startup_bundle.plan.phase,
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
            result = self.run(request_builder())
            if result.should_render:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}]", flush=True)
                print(render_result_summary(result), flush=True)
                print("", flush=True)
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
            time.sleep(max(1, int(interval_seconds)))


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
    effective_now = now or datetime.now()
    effective_trade_date = trade_date or effective_now.strftime("%Y-%m-%d")
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
        run_integrated_sync=run_integrated_sync,
    )


def render_result_summary(result: EngineAppResult) -> str:
    lines = [f"phase={result.phase.value}"]
    suppress_internal_header = result.phase == RunPhase.POSTMARKET and any(
        str(note).startswith("runtime_event=postmarket takeover") for note in result.notes
    )
    if result.lifecycle_audit_ran and not suppress_internal_header:
        lines.append(
            f"loop_event={result.loop_event}"
            f" | audit={'fresh' if result.lifecycle_audit_ran else 'cached'}"
            f" | startup_state={'cached' if result.used_cached_startup_state else 'fresh'}"
        )
    lines.extend(result.notes)
    if result.intraday_context is not None:
        lines.append(
            f"context_snapshots={len(result.intraday_context.stock_snapshots)} "
            f"| top_plate={result.intraday_context.market_summary.top_plate_name or ''}"
        )
    if result.phase_events and not suppress_internal_header:
        lines.append("phase_events:")
        for event in result.phase_events[:6]:
            lines.append(f"- {event.time_window} | {event.component} | {event.action}")
    return "\n".join(lines)


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
