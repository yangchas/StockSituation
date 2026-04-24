from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time
import logging

from engine_next.contracts.offline_sync_contracts import WatermarkSnapshot
from engine_next.domain.enums import ExecutionEnvironment, RunPhase, StartupReadinessLevel
from engine_next.domain.enums import StartupAction
from engine_next.domain.models import StartupSelfCheckReport
from engine_next.runtime.intraday_data_hub import IntradayDataHub, IntradayFetchResult
from engine_next.runtime.offline_sync_executor import (
    OfflineSyncDecision,
    OfflineSyncRequest,
    ServerOnlyOfflineSyncExecutor,
)
from engine_next.runtime.market_runtime_summary import (
    MarketRuntimeSummaryResult,
    MarketRuntimeSummaryService,
)
from engine_next.runtime.startup_static_loader import StaticDataLoadResult, StartupStaticDataLoader
from engine_next.runtime.startup_self_check import (
    PREMARKET_HEAVY_SYNC_CUTOFF,
    StartupSelfCheckRequest,
    StartupSelfCheckService,
    infer_run_phase,
)


logger = logging.getLogger(__name__)
INTRADAY_AUCTION_RECOVERY_CUTOFF = dt_time(9, 35)


@dataclass(frozen=True)
class StartupCoordinatorRequest:
    now: datetime
    trade_date: str
    previous_trade_date: str
    symbols: tuple[str, ...]
    kline_watermarks: dict[str, str]
    factor_watermarks: dict[str, str]
    redis_factor_cache_ready: dict[str, bool]
    yest_limit_pool_ready: bool = False
    hot_plates_ready: bool = False
    stock_plate_mapping_ready: bool = False
    auction_anchor_ready: bool = False
    redis_chip_ready_count: int = 0
    redis_dde_ready_count: int = 0
    watermark_snapshot: WatermarkSnapshot | None = None
    environment: ExecutionEnvironment = ExecutionEnvironment.SERVER


@dataclass(frozen=True)
class StartupCoordinationPlan:
    phase: RunPhase
    report: StartupSelfCheckReport
    offline_decision: OfflineSyncDecision | None
    should_attempt_auction_recovery: bool
    should_refresh_hot_plates: bool
    should_refresh_yest_limit_pool: bool
    should_refresh_market_runtime_summary: bool
    should_run_postmarket_recap: bool
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class StartupExecutionBundle:
    plan: StartupCoordinationPlan
    stock_plate_result: StaticDataLoadResult | None = None
    auction_result: IntradayFetchResult | None = None
    hot_plate_result: IntradayFetchResult | None = None
    yest_limit_result: IntradayFetchResult | None = None
    market_runtime_summary_result: MarketRuntimeSummaryResult | None = None


class RuntimeStartupCoordinator:
    """Connect startup self-check with concrete runtime repair actions."""

    def __init__(
        self,
        *,
        offline_executor: ServerOnlyOfflineSyncExecutor | None = None,
        intraday_hub: IntradayDataHub | None = None,
        self_check_service: StartupSelfCheckService | None = None,
        market_runtime_summary_service: MarketRuntimeSummaryService | None = None,
        static_data_loader: StartupStaticDataLoader | None = None,
    ) -> None:
        self._offline_executor = offline_executor or ServerOnlyOfflineSyncExecutor()
        self._intraday_hub = intraday_hub or IntradayDataHub()
        self._self_check_service = self_check_service or StartupSelfCheckService()
        self._market_runtime_summary_service = market_runtime_summary_service
        self._static_data_loader = static_data_loader or StartupStaticDataLoader(redis_client=self._intraday_hub.redis)

    def build_plan(self, request: StartupCoordinatorRequest) -> StartupCoordinationPlan:
        logger.debug(
            "startup plan build | trade_date=%s | previous_trade_date=%s | symbols=%s",
            request.trade_date,
            request.previous_trade_date,
            len(request.symbols),
        )
        offline_request = OfflineSyncRequest(
            now=request.now,
            target_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            symbols=request.symbols,
            kline_watermarks=request.kline_watermarks,
            factor_watermarks=request.factor_watermarks,
            redis_factor_cache_ready=request.redis_factor_cache_ready,
            environment=request.environment,
        )
        offline_decision = self._offline_executor.build_decision(offline_request)
        watermark_snapshot = request.watermark_snapshot or self._offline_executor.preload_watermark_snapshot(offline_request)
        report = self._self_check_service.build_report(
            StartupSelfCheckRequest(
                now=request.now,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                symbol_count=len(request.symbols),
                watermark_snapshot=watermark_snapshot,
                yest_limit_pool_ready=request.yest_limit_pool_ready,
                hot_plates_ready=request.hot_plates_ready,
                stock_plate_mapping_ready=request.stock_plate_mapping_ready,
                auction_anchor_ready=request.auction_anchor_ready,
                redis_factor_ready_count=sum(1 for ok in request.redis_factor_cache_ready.values() if ok),
                redis_chip_ready_count=request.redis_chip_ready_count,
                redis_dde_ready_count=request.redis_dde_ready_count,
            )
        )
        phase = report.phase
        should_attempt_auction_recovery = self._should_attempt_auction_recovery(
            phase=phase,
            now=request.now,
            auction_anchor_ready=request.auction_anchor_ready,
        )
        should_refresh_hot_plates = not request.hot_plates_ready
        should_refresh_yest_limit_pool = not request.yest_limit_pool_ready
        should_refresh_market_runtime_summary = phase in (
            RunPhase.PREMARKET,
            RunPhase.AUCTION,
            RunPhase.INTRADAY,
            RunPhase.POSTMARKET,
        )
        should_run_postmarket_recap = phase == RunPhase.POSTMARKET and report.readiness != StartupReadinessLevel.OBSERVE_ONLY

        notes = [getattr(offline_decision, "notes", "")]
        if phase == RunPhase.PREMARKET and should_refresh_yest_limit_pool:
            notes.append("Premarket startup may repair yesterday limit pool before strategy consumption.")
        if phase == RunPhase.PREMARKET and request.now.time() >= PREMARKET_HEAVY_SYNC_CUTOFF:
            notes.append("After 09:00, only small heavy-data repairs should continue; large sync stays deferred.")
        if should_attempt_auction_recovery:
            notes.append("Auction anchor recovery should execute before auction or intraday strategy consumption.")
            if phase == RunPhase.INTRADAY:
                notes.append("If intraday startup is already past 09:30, fallback recovery should write the recovered anchor back to Redis immediately.")
        if should_refresh_market_runtime_summary:
            notes.append("Market runtime summary should be refreshed from full Redis cache buckets before dense intraday context consumption.")
        return StartupCoordinationPlan(
            phase=phase,
            report=report,
            offline_decision=offline_decision,
            should_attempt_auction_recovery=should_attempt_auction_recovery,
            should_refresh_hot_plates=should_refresh_hot_plates,
            should_refresh_yest_limit_pool=should_refresh_yest_limit_pool,
            should_refresh_market_runtime_summary=should_refresh_market_runtime_summary,
            should_run_postmarket_recap=should_run_postmarket_recap,
            notes=tuple(note for note in notes if note),
        )

    def _should_attempt_auction_recovery(
        self,
        *,
        phase: RunPhase,
        now: datetime,
        auction_anchor_ready: bool,
    ) -> bool:
        if auction_anchor_ready:
            return False
        if phase == RunPhase.AUCTION:
            return True
        if phase == RunPhase.INTRADAY and now.time() < INTRADAY_AUCTION_RECOVERY_CUTOFF:
            return True
        return False

    def execute_allowed_repairs(self, request: StartupCoordinatorRequest) -> StartupExecutionBundle:
        plan = self.build_plan(request)
        phase = plan.phase
        logger.debug(
            "startup repairs execute | phase=%s | refresh_hot_plates=%s | refresh_yest_limit=%s | auction_recovery=%s",
            phase.value,
            plan.should_refresh_hot_plates,
            plan.should_refresh_yest_limit_pool,
            plan.should_attempt_auction_recovery,
        )
        stock_plate_result = None
        auction_result = None
        hot_plate_result = None
        yest_limit_result = None
        market_runtime_summary_result = None

        if not request.stock_plate_mapping_ready:
            logger.debug("startup repair step | stock_plate_mapping reload")
            stock_plate_result = self._static_data_loader.load_stock_plate_mapping()

        if plan.should_attempt_auction_recovery:
            logger.debug("startup repair step | auction anchor recovery")
            auction_result = self._intraday_hub.recover_auction_anchor(request.trade_date, phase)

        if plan.should_refresh_hot_plates:
            logger.debug("startup repair step | hot plates refresh")
            hot_plate_date = request.previous_trade_date if phase == RunPhase.PREMARKET else request.trade_date
            hot_plate_today_mode = phase in (RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET)
            hot_plate_result = self._intraday_hub.fetch_hot_plates(
                hot_plate_date,
                phase,
                today_mode=hot_plate_today_mode,
            )

        if plan.should_refresh_yest_limit_pool:
            logger.debug("startup repair step | yesterday limit pool refresh")
            yest_limit_result = self._intraday_hub.fetch_yest_limit_pool(request.previous_trade_date, phase)

        if plan.should_refresh_market_runtime_summary:
            logger.debug("startup repair step | market runtime summary rebuild")
            service = self._market_runtime_summary_service or MarketRuntimeSummaryService(redis_client=self._intraday_hub.redis)
            offline_context_date = (
                plan.report.formal_offline_date if phase == RunPhase.POSTMARKET else request.previous_trade_date
            )
            market_runtime_summary_result = service.build_and_write(
                request.trade_date,
                offline_context_date=offline_context_date,
            )

        return StartupExecutionBundle(
            plan=plan,
            stock_plate_result=stock_plate_result,
            auction_result=auction_result,
            hot_plate_result=hot_plate_result,
            yest_limit_result=yest_limit_result,
            market_runtime_summary_result=market_runtime_summary_result,
        )

    def render_console_summary(self, plan: StartupCoordinationPlan) -> str:
        report = plan.report
        status_map = report.by_dataset()
        daily_kline = status_map["daily_kline"]
        daily_factors = status_map["daily_factors"]
        yest_limit = status_map["yest_limit_pool"]
        hot_plates = status_map["hot_plates"]
        auction = status_map["auction_anchor"]
        if auction.ready:
            auction_state = "ok"
        elif auction.action == StartupAction.PRELOAD_ONLY:
            auction_state = "preload"
        elif auction.action == StartupAction.AUCTION_FALLBACK_RECOVERY:
            auction_state = "recover"
        else:
            auction_state = "missing"

        line1 = (
            f"[{plan.phase.value}] ready={report.readiness.value} "
            f"| formal={report.formal_offline_date} "
            f"| kline_gap={daily_kline.missing_count}/{daily_kline.total_count} "
            f"| factor_gap={daily_factors.missing_count}/{daily_factors.total_count}"
        )
        line2 = (
            f"fast_repair "
            f"| yest_limit={'ok' if yest_limit.ready else 'repair'} "
            f"| hot_plates={'ok' if hot_plates.ready else 'repair'} "
            f"| auction={auction_state} "
            f"| market_summary={'refresh' if plan.should_refresh_market_runtime_summary else 'skip'}"
        )
        line3 = "actions | " + " | ".join(report.recommended_actions[:4]) if report.recommended_actions else "actions | none"
        return "\n".join((line1, line2, line3))

    def render_execution_summary(self, bundle: StartupExecutionBundle) -> tuple[str, ...]:
        details: list[str] = []
        report = bundle.plan.report
        gap_parts = []
        for status in report.statuses:
            if status.total_count > 0:
                gap_parts.append(f"{status.dataset}={status.missing_count}/{status.total_count}")
            else:
                if status.dataset == "auction_anchor" and bundle.plan.phase.value == "premarket" and not status.ready:
                    gap_parts.append(f"{status.dataset}=pending")
                else:
                    gap_parts.append(f"{status.dataset}={'ok' if status.ready else 'missing'}")
        if gap_parts:
            details.append("self_check " + " | ".join(gap_parts))
        if bundle.stock_plate_result is not None:
            result = bundle.stock_plate_result
            details.append(
                "repair.stock_plate "
                f"| rows={result.rows_loaded} "
                f"| key={','.join(result.redis_keys_written) or '-'} "
                f"| source={result.source_path or 'missing'}"
            )
        if bundle.yest_limit_result is not None:
            result = bundle.yest_limit_result
            details.append(
                "repair.yest_limit "
                f"| rows={len(result.rows)} "
                f"| source={result.source} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            )
        if bundle.hot_plate_result is not None:
            result = bundle.hot_plate_result
            details.append(
                "repair.hot_plates "
                f"| rows={len(result.rows)} "
                f"| source={result.source} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            )
        if bundle.auction_result is not None:
            result = bundle.auction_result
            details.append(
                "repair.auction "
                f"| rows={len(result.rows)} "
                f"| source={result.source} "
                f"| keys={','.join(result.redis_keys_written) or '-'}"
            )
        if bundle.market_runtime_summary_result is not None:
            summary = bundle.market_runtime_summary_result.summary
            details.append(
                "repair.market_summary "
                f"| top_plate={summary.get('top_plate_name', '')} "
                f"| hot_count={summary.get('hot_plate_count', 0)} "
                f"| auc_sample={summary.get('auction_sample_size', 0)} "
                f"| source={summary.get('source', '')}"
            )
        return tuple(details)


def infer_phase_from_clock(now: datetime) -> RunPhase:
    return infer_run_phase(now)
