from __future__ import annotations

import argparse
import json
import logging
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
    STARTUP_AUDIT_CHECKPOINTS = ("08:30", "09:00", "17:40")
    AUCTION_FINALIZE_EARLIEST = dt_time(9, 25, 10)

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

    def _safe_set(self, key: str, value: str) -> bool:
        try:
            if hasattr(self.redis, "set"):
                self.redis.set(key, value)
                return True
        except Exception:
            return False
        return False

    def _hot_plate_freshness_limit_seconds(self, phase: RunPhase) -> int:
        if phase == RunPhase.PREMARKET:
            return 96 * 60 * 60
        if phase in {RunPhase.AUCTION, RunPhase.INTRADAY}:
            return 45 * 60
        if phase == RunPhase.POSTMARKET:
            return 8 * 60 * 60
        return 24 * 60 * 60

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
        candidates.extend(self._safe_hkeys("config:plate_mapping:s2p"))
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
            lines.append(f"sync.partial_symbols | {', '.join(partial_details[:12])}")
        if failed_details:
            lines.append(f"sync.failed_symbols | {', '.join(failed_details[:12])}")
        if failed_roots:
            lines.append(f"sync.failed_root | {', '.join(failed_roots[:12])}")
        if root_counter:
            summary = ", ".join(
                f"{root}={count}"
                for root, count in sorted(root_counter.items(), key=lambda item: (-item[1], item[0]))
            )
            lines.append(f"sync.root_summary | {summary}")
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
        phase = infer_run_phase(now)
        runtime_plate_count = self._safe_hlen("market:stock_plate")
        stock_theme_count = self._safe_hlen("config:plate_mapping:s2p")
        hot_plate_key = f"cache:hot_plates:{hot_plate_date}"
        hot_plate_meta = self._safe_get_json(f"cache:hot_plates_meta:{hot_plate_date}")
        hot_plate_count = self._safe_hlen(hot_plate_key)
        hot_plate_row_count = 0
        hot_plate_updated_at_ts = 0
        try:
            hot_plate_row_count = int(hot_plate_meta.get("row_count", 0) or 0)
        except (TypeError, ValueError):
            hot_plate_row_count = 0
        try:
            hot_plate_updated_at_ts = int(float(hot_plate_meta.get("updated_at_ts", 0) or 0))
        except (TypeError, ValueError):
            hot_plate_updated_at_ts = 0
        hot_plate_meta_trade_date = str(hot_plate_meta.get("trade_date") or "").strip()
        hot_plate_freshness_limit_seconds = self._hot_plate_freshness_limit_seconds(phase)
        hot_plate_age_seconds = max(int(now.timestamp()) - hot_plate_updated_at_ts, 0) if hot_plate_updated_at_ts > 0 else None
        hot_plates_ready = (
            hot_plate_count > 0
            and hot_plate_row_count > 0
            and hot_plate_meta_trade_date == hot_plate_date
            and hot_plate_updated_at_ts > 0
            and (
                hot_plate_age_seconds is not None
                and hot_plate_age_seconds <= hot_plate_freshness_limit_seconds
            )
        )
        return {
            "redis_factor_cache_ready": factor_ready,
            "yest_limit_pool_ready": self._safe_hlen(f"cache:yest_limit_pool:{previous_trade_date}") > 0,
            "hot_plates_ready": hot_plates_ready,
            "stock_plate_mapping_ready": (
                runtime_plate_count >= self.MIN_STOCK_PLATE_MAPPING_COUNT
                or stock_theme_count >= self.MIN_STOCK_PLATE_MAPPING_COUNT
            ),
            "auction_anchor_ready": self._safe_exists(f"market:auction:anchor:{trade_date.replace('-', '')}"),
            "redis_chip_ready_count": self._safe_hlen(f"cache:chip_peaks:{offline_context_date}"),
            "redis_dde_ready_count": self._safe_hlen(f"cache:dde_ready:{offline_context_date}"),
            "hot_plate_count": hot_plate_count,
            "hot_plate_row_count": hot_plate_row_count,
            "hot_plate_cache_date": hot_plate_date,
            "hot_plate_age_seconds": hot_plate_age_seconds,
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
            quote_feed_healthy = (
                quote_rows > 0
                and fresh_ratio >= 0.90
                and latest_age_seconds is not None
                and stale_threshold_seconds > 0
                and latest_age_seconds <= stale_threshold_seconds
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
        rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
        live_notes = list(
            self._auction_runtime.render_postmarket_runtime_loop(
                intraday_context=intraday_context,
                runtime_readiness_label=runtime_readiness_label,
                symbols=symbols,
                quotes=quote_count,
                rust=rust_count,
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
                yest_limit_pool_ready=bool(refreshed_runtime_readiness["yest_limit_pool_ready"]),
                hot_plates_ready=bool(refreshed_runtime_readiness["hot_plates_ready"]),
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
        rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
        return self._auction_runtime.render_auction_takeover(
            intraday_context=intraday_context,
            runtime_readiness_label=runtime_readiness_label,
            symbols=symbols,
            quotes=quote_count,
            rust=rust_count,
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
        if phase == RunPhase.POSTMARKET:
            return False
        if self._last_render_token != token:
            self._last_render_token = token
            return True
        if phase not in (RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET):
            self._last_render_token = token
            return True
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

        if loop_decision.scheduled_event_name == "auction_finalize_0925":
            replay_result = self._auction_runtime.execute_auction_finalize_0925(
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=offline_context_date,
            )
            yest_limit_result = replay_result.yest_limit_result
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
            auction_result = replay_result.auction_result
            market_runtime_summary_result = replay_result.market_runtime_summary_result
            notes.extend(replay_result.notes)
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
                logger.debug("startup symbol universe expanded from runtime set to full F10 pool | symbols=%s", len(symbols))
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
        offline_decision = startup_bundle.plan.offline_decision or self._offline_executor.build_decision(effective_offline_request)
        should_audit_integrated_sync = self._settlement.should_audit_integrated_sync(
            phase=phase,
            now=request.now,
            lifecycle_audit_ran=should_run_lifecycle_audit,
            scheduled_event_name=scheduled_event_result.name,
            scheduled_event_executed=scheduled_event_result.executed,
        )
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

        runtime_readiness_label = self._derive_runtime_readiness(
            phase=phase,
            runtime_readiness=runtime_readiness,
            primed_runtime_state=primed_runtime_state,
        )
        quote_freshness_line = render_quote_freshness_line(
            primed_runtime_state,
            symbol_count=len(symbols),
        )
        notes = [f"runtime_event={loop_decision.label}"]
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
                    f"formal_readiness={startup_bundle.plan.report.readiness.value} | runtime_readiness={runtime_readiness_label}",
                    f"symbols={len(symbols)}",
                    f"integrated_sync={len(integrated_sync_results)}",
                    f"context={'ready' if intraday_context is not None else 'skipped'}",
                ]
            )
            if quote_freshness_line:
                notes.append(quote_freshness_line)
            if self._is_opening_strategy_window(request.now, phase):
                quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
                rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
                notes.extend(
                    self._auction_runtime.render_opening_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        rust=rust_count,
                        now=request.now,
                        quote_freshness_line=None,
                    )
                )
            elif phase == RunPhase.PREMARKET:
                quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
                rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
                notes.extend(
                    self._auction_runtime.render_premarket_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        rust=rust_count,
                        now=request.now,
                        quote_freshness_line=None,
                        startup_report=startup_bundle.plan.report,
                        historical_only=(runtime_readiness_label == "historical_context_only"),
                    )
                )
            elif phase == RunPhase.INTRADAY:
                quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
                rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
                notes.extend(
                    self._auction_runtime.render_intraday_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        rust=rust_count,
                        now=request.now,
                        quote_freshness_line=None,
                    )
                )
        else:
            quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
            rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
            if phase == RunPhase.AUCTION:
                notes.extend(
                    self._auction_runtime.render_auction_runtime_loop(
                        intraday_context=intraday_context,
                        runtime_readiness_label=runtime_readiness_label,
                        symbols=len(symbols),
                        quotes=quote_count,
                        rust=rust_count,
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
                        rust=rust_count,
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
                        rust=rust_count,
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
                        rust=rust_count,
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
                        rust=rust_count,
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
        if should_expand_full_universe:
            notes.append(f"startup universe expanded to full F10 pool: {len(symbols)}")
        render_execution_summary = getattr(self._startup_coordinator, "render_execution_summary", None)
        if should_run_lifecycle_audit and not is_auction_takeover and not is_postmarket_takeover and callable(render_execution_summary):
            notes.extend(render_execution_summary(startup_bundle))
        if request.run_integrated_sync and should_audit_integrated_sync and not integrated_sync_allowed:
            notes.append(
                f"integrated_sync deferred: effective_targets={len(effective_sync_symbols)} | "
                f"auto_discovered_universe={len(symbols)} | safe_limit={self.AUTO_DISCOVERED_SYNC_LIMIT}"
            )
        if settlement_result.settlement_cached:
            notes.append(f"settlement_cached | trade_date={request.trade_date} | integrated_sync=reused")
        elif settlement_result.settlement_running:
            notes.append(f"settlement_running | trade_date={request.trade_date} | integrated_sync=skipped_while_in_progress")
        elif request.run_integrated_sync and should_audit_integrated_sync and effective_sync_symbols:
            notes.append(f"integrated_sync targets={len(effective_sync_symbols)}")
        notes.extend(self._render_scheduled_event_summary(scheduled_event_result))
        notes.extend(self._summarize_integrated_sync(integrated_sync_results))
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
    has_strategy_console = any(str(note).startswith("strategy_console |") for note in result.notes)
    suppress_internal_header = result.phase in (RunPhase.AUCTION, RunPhase.POSTMARKET) and any(
        str(note).startswith("runtime_event=postmarket takeover")
        or str(note).startswith("runtime_event=auction takeover")
        for note in result.notes
    )
    if result.lifecycle_audit_ran and not suppress_internal_header:
        lines.append(
            f"loop_event={result.loop_event}"
            f" | audit={'fresh' if result.lifecycle_audit_ran else 'cached'}"
            f" | startup_state={'cached' if result.used_cached_startup_state else 'fresh'}"
        )
    lines.extend(result.notes)
    if result.intraday_context is not None and not has_strategy_console:
        lines.append(
            f"上下文快照={len(result.intraday_context.stock_snapshots)} "
            f"| 主线板块={result.intraday_context.market_summary.top_plate_name or ''}"
        )
    if result.phase_events and not suppress_internal_header and not has_strategy_console:
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
