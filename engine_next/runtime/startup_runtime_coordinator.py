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
    OfflineSyncScope,
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
MARKET_RUNTIME_SUMMARY_MAX_AGE_SECONDS = {
    RunPhase.PREMARKET: None,
    RunPhase.AUCTION: 60,
    RunPhase.INTRADAY: 5 * 60,
    RunPhase.POSTMARKET: 30 * 60,
}


@dataclass(frozen=True)
class StartupCoordinatorRequest:
    now: datetime
    trade_date: str
    previous_trade_date: str
    symbols: tuple[str, ...]
    kline_watermarks: dict[str, str]
    factor_watermarks: dict[str, str]
    redis_factor_cache_ready: dict[str, bool]
    current_trade_factor_cache_ready: dict[str, bool]
    current_trade_chip_cache_ready: dict[str, bool]
    yest_limit_pool_ready: bool = False
    hot_plates_ready: bool = False
    hot_plates_today_ready: bool = False
    hot_plates_effective_ready: bool = False
    hot_plates_effective_trade_date: str = ""
    hot_plates_live_fresh: bool = True
    stock_plate_mapping_ready: bool = False
    auction_anchor_ready: bool = False
    redis_chip_ready_count: int = 0
    redis_dde_ready_count: int = 0
    live_target_session: bool = True
    previous_settlement_payload: dict[str, object] | None = None
    cached_listing_dates: dict[str, str] | None = None
    cached_kline_row_counts: dict[str, int] | None = None
    cached_structural_factor_gap: dict[str, bool] | None = None
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
    sync_pipeline_targets: int = 0
    sync_network_targets: int = 0
    sync_analytics_targets: int = 0
    sync_factor_cache_gaps: int = 0
    market_runtime_summary_cache_hit: bool = False
    previous_settlement_payload: dict[str, object] | None = None
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

    def _market_runtime_summary_max_age_seconds(self, phase: RunPhase, *, live_target_session: bool) -> int | None:
        if not live_target_session:
            return None
        return MARKET_RUNTIME_SUMMARY_MAX_AGE_SECONDS.get(phase)

    def _should_refresh_hot_plates(
        self,
        *,
        phase: RunPhase,
        hot_plates_ready: bool,
        hot_plates_live_fresh: bool,
    ) -> bool:
        if hot_plates_ready and hot_plates_live_fresh:
            return False
        return phase in (
            RunPhase.PREMARKET,
            RunPhase.AUCTION,
            RunPhase.INTRADAY,
            RunPhase.POSTMARKET,
        )

    def _should_refresh_yest_limit_pool(
        self,
        *,
        phase: RunPhase,
        yest_limit_pool_ready: bool,
    ) -> bool:
        if yest_limit_pool_ready:
            return False
        return phase in (
            RunPhase.PREMARKET,
            RunPhase.AUCTION,
            RunPhase.INTRADAY,
            RunPhase.POSTMARKET,
        )

    def _market_runtime_summary_offline_context_date(
        self,
        *,
        phase: RunPhase,
        request: StartupCoordinatorRequest,
        report: StartupSelfCheckReport,
    ) -> str:
        if phase == RunPhase.POSTMARKET:
            return report.formal_offline_date
        return request.previous_trade_date

    def _should_refresh_market_runtime_summary(
        self,
        *,
        phase: RunPhase,
        market_runtime_summary_cache_hit: bool,
        should_attempt_auction_recovery: bool,
        should_refresh_hot_plates: bool,
        should_refresh_yest_limit_pool: bool,
    ) -> bool:
        if phase not in (
            RunPhase.PREMARKET,
            RunPhase.AUCTION,
            RunPhase.INTRADAY,
            RunPhase.POSTMARKET,
        ):
            return False
        if not market_runtime_summary_cache_hit:
            return True
        return (
            should_attempt_auction_recovery
            or should_refresh_hot_plates
            or should_refresh_yest_limit_pool
        )

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
                symbols=request.symbols,
                symbol_count=len(request.symbols),
                watermark_snapshot=watermark_snapshot,
                redis_factor_cache_ready=request.redis_factor_cache_ready,
                current_trade_factor_cache_ready=request.current_trade_factor_cache_ready,
                current_trade_chip_cache_ready=request.current_trade_chip_cache_ready,
                yest_limit_pool_ready=request.yest_limit_pool_ready,
                hot_plates_ready=request.hot_plates_ready,
                hot_plates_today_ready=request.hot_plates_today_ready,
                hot_plates_effective_ready=request.hot_plates_effective_ready,
                hot_plates_effective_trade_date=request.hot_plates_effective_trade_date,
                stock_plate_mapping_ready=request.stock_plate_mapping_ready,
                auction_anchor_ready=request.auction_anchor_ready,
                redis_factor_ready_count=sum(1 for ok in request.redis_factor_cache_ready.values() if ok),
                redis_chip_ready_count=request.redis_chip_ready_count,
                redis_dde_ready_count=request.redis_dde_ready_count,
                listing_dates=request.cached_listing_dates or None,
                kline_row_counts=request.cached_kline_row_counts or None,
                cached_structural_factor_gap=request.cached_structural_factor_gap or None,
                dataset_gap_matrix=offline_decision.dataset_gap_matrix,
            )
        )
        phase = report.phase
        sync_scope: OfflineSyncScope | None = None
        sync_pipeline_targets = 0
        sync_network_targets = 0
        sync_analytics_targets = 0
        sync_factor_cache_gaps = 0
        if offline_decision.allowed:
            sync_scope = self._offline_executor.build_sync_scope(
                offline_request,
                watermark_snapshot,
                offline_decision,
            )
            sync_pipeline_targets = sync_scope.pipeline_count
            sync_network_targets = sync_scope.network_count
            sync_analytics_targets = sync_scope.analytics_count
            sync_factor_cache_gaps = sync_scope.factor_cache_gap_count
        should_attempt_auction_recovery = self._should_attempt_auction_recovery(
            phase=phase,
            now=request.now,
            auction_anchor_ready=request.auction_anchor_ready,
        )
        should_refresh_hot_plates = self._should_refresh_hot_plates(
            phase=phase,
            hot_plates_ready=request.hot_plates_ready,
            hot_plates_live_fresh=request.hot_plates_live_fresh,
        )
        should_refresh_yest_limit_pool = self._should_refresh_yest_limit_pool(
            phase=phase,
            yest_limit_pool_ready=request.yest_limit_pool_ready,
        )
        offline_context_date = self._market_runtime_summary_offline_context_date(
            phase=phase,
            request=request,
            report=report,
        )
        service = self._market_runtime_summary_service or MarketRuntimeSummaryService(redis_client=self._intraday_hub.redis)
        cached_market_runtime_summary = service.load_cached(
            request.trade_date,
            offline_context_date=offline_context_date,
            max_age_seconds=self._market_runtime_summary_max_age_seconds(
                phase,
                live_target_session=request.live_target_session,
            ),
        )
        market_runtime_summary_cache_hit = cached_market_runtime_summary is not None
        should_refresh_market_runtime_summary = self._should_refresh_market_runtime_summary(
            phase=phase,
            market_runtime_summary_cache_hit=market_runtime_summary_cache_hit,
            should_attempt_auction_recovery=should_attempt_auction_recovery,
            should_refresh_hot_plates=should_refresh_hot_plates,
            should_refresh_yest_limit_pool=should_refresh_yest_limit_pool,
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
        elif market_runtime_summary_cache_hit:
            notes.append("Market runtime summary cache is fresh enough; startup can reuse it without rebuilding.")
        if phase == RunPhase.INTRADAY and (should_refresh_hot_plates or should_refresh_yest_limit_pool or should_attempt_auction_recovery):
            notes.append("Intraday startup repairs stay on lightweight Kaipan/Redis paths; heavy offline sync remains deferred.")
        if sync_pipeline_targets:
            notes.append(
                "Offline sync scope audited before execution: "
                f"pipeline={sync_pipeline_targets}, network={sync_network_targets}, "
                f"analytics={sync_analytics_targets}, factor_cache_gap={sync_factor_cache_gaps}."
            )
        return StartupCoordinationPlan(
            phase=phase,
            report=report,
            offline_decision=offline_decision,
            should_attempt_auction_recovery=should_attempt_auction_recovery,
            should_refresh_hot_plates=should_refresh_hot_plates,
            should_refresh_yest_limit_pool=should_refresh_yest_limit_pool,
            should_refresh_market_runtime_summary=should_refresh_market_runtime_summary,
            should_run_postmarket_recap=should_run_postmarket_recap,
            sync_pipeline_targets=sync_pipeline_targets,
            sync_network_targets=sync_network_targets,
            sync_analytics_targets=sync_analytics_targets,
            sync_factor_cache_gaps=sync_factor_cache_gaps,
            market_runtime_summary_cache_hit=market_runtime_summary_cache_hit,
            previous_settlement_payload=request.previous_settlement_payload,
            notes=tuple(note for note in notes if note),
        )

    def _render_previous_settlement_line(self, payload: dict[str, object] | None) -> str | None:
        if not payload:
            return None
        trade_date = str(payload.get("trade_date") or "").strip()
        if not trade_date:
            return None
        base = (
            "prev_settlement "
            f"| trade_date={trade_date} "
            f"| targets={int(payload.get('effective_targets', 0) or 0)} "
            f"| results={int(payload.get('result_count', 0) or 0)}"
        )
        quality_keys = (
            "full_ready_count",
            "partial_ready_count",
            "failed_count",
            "short_history_count",
            "factor_cache_gap_count",
        )
        if not any(key in payload for key in quality_keys):
            return f"{base} | quality=legacy_payload"
        return (
            f"{base} "
            f"| full={int(payload.get('full_ready_count', 0) or 0)} "
            f"| partial={int(payload.get('partial_ready_count', 0) or 0)} "
            f"| failed={int(payload.get('failed_count', 0) or 0)} "
            f"| short_history={int(payload.get('short_history_count', 0) or 0)} "
            f"| factor_cache_gap={int(payload.get('factor_cache_gap_count', 0) or 0)}"
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

    def execute_allowed_repairs(
        self,
        request: StartupCoordinatorRequest,
        *,
        plan: StartupCoordinationPlan | None = None,
    ) -> StartupExecutionBundle:
        plan = plan or self.build_plan(request)
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
            offline_context_date = self._market_runtime_summary_offline_context_date(
                phase=phase,
                request=request,
                report=plan.report,
            )
            market_runtime_summary_result = service.get_or_build(
                request.trade_date,
                offline_context_date=offline_context_date,
                max_age_seconds=self._market_runtime_summary_max_age_seconds(
                    phase,
                    live_target_session=request.live_target_session,
                ),
                force_rebuild=True,
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
            f"| trade_date={report.target_trade_date} "
            f"| offline_formal={report.formal_offline_date} "
            f"| kline_gap={daily_kline.missing_count}/{daily_kline.total_count} "
            f"| factor_formal_gap={daily_factors.missing_count}/{daily_factors.total_count}"
        )
        line2 = (
            f"fast_repair "
            f"| yest_limit={'ok' if yest_limit.ready else 'repair'} "
            f"| hot_plates={'ok' if hot_plates.ready else 'repair'} "
            f"| auction={auction_state} "
            f"| market_summary={'refresh' if plan.should_refresh_market_runtime_summary else 'skip'}"
        )
        line3 = "actions | " + " | ".join(report.recommended_actions[:4]) if report.recommended_actions else "actions | none"
        details: list[str] = [line1, line2, line3]
        if plan.market_runtime_summary_cache_hit:
            details.append("cache_hit | market_runtime_summary=fresh")
        if any(
            (
                plan.sync_pipeline_targets,
                plan.sync_network_targets,
                plan.sync_analytics_targets,
                plan.sync_factor_cache_gaps,
            )
        ):
            details.append(
                "sync_scope "
                f"| pipe={plan.sync_pipeline_targets} "
                f"| net={plan.sync_network_targets} "
                f"| calc={plan.sync_analytics_targets} "
                f"| factor_cache_gap={plan.sync_factor_cache_gaps}"
            )
        if daily_kline.dead_symbol_count or daily_kline.actionable_missing_count != daily_kline.missing_count:
            details.append(
                "gap_breakdown "
                f"| daily_kline.actionable={daily_kline.actionable_missing_count} "
                f"| daily_kline.dead={daily_kline.dead_symbol_count}"
            )
        if any(
            (
                daily_factors.actionable_missing_count != daily_factors.missing_count,
                daily_factors.structural_gap_count,
                daily_factors.cache_gap_count,
                daily_factors.dead_symbol_count,
                daily_factors.current_trade_ready_count,
            )
        ):
            details.append(
                "gap_breakdown "
                f"| daily_factors.actionable={daily_factors.actionable_missing_count} "
                f"| daily_factors.structural={daily_factors.structural_gap_count} "
                f"| daily_factors.cache_gap={daily_factors.cache_gap_count} "
                f"| daily_factors.dead={daily_factors.dead_symbol_count} "
                f"| daily_factors.current_trade_ready={daily_factors.current_trade_ready_count}"
            )
        previous_settlement_line = self._render_previous_settlement_line(plan.previous_settlement_payload)
        if previous_settlement_line:
            details.append(previous_settlement_line)
        return "\n".join(details)

    @staticmethod
    def _render_startup_digest(
        report: StartupSelfCheckReport,
        *,
        phase: RunPhase,
    ) -> tuple[str, ...]:
        status_map = report.by_dataset()
        daily_kline = status_map.get("daily_kline")
        daily_factors = status_map.get("daily_factors")
        chip_peaks = status_map.get("chip_peaks")
        daily_dde = status_map.get("daily_dde")
        yest_limit = status_map.get("yest_limit_pool")
        hot_plates = status_map.get("hot_plates")
        stock_plate = status_map.get("stock_plate_mapping")
        auction_anchor = status_map.get("auction_anchor")

        missing_parts: list[str] = []
        if daily_kline is not None and daily_kline.missing_count > 0:
            missing_parts.append(f"日线缺{daily_kline.missing_count}只")
        if daily_factors is not None and daily_factors.missing_count > 0:
            missing_parts.append(f"因子缺{daily_factors.missing_count}只")
        if chip_peaks is not None and chip_peaks.missing_count > 0:
            missing_parts.append(f"筹码缓存缺{chip_peaks.missing_count}只")
        if daily_dde is not None and daily_dde.missing_count > 0:
            missing_parts.append(f"DDE缺{daily_dde.missing_count}只")

        ready_parts: list[str] = []
        if yest_limit is not None and yest_limit.ready:
            ready_parts.append("昨日涨停池已就绪")
        if hot_plates is not None and hot_plates.ready:
            ready_parts.append("热板缓存已就绪")
        if stock_plate is not None and stock_plate.ready:
            ready_parts.append("个股板块映射已就绪")

        tail_parts: list[str] = []
        if auction_anchor is not None:
            if auction_anchor.ready:
                tail_parts.append("竞价锚点已就绪")
            elif phase == RunPhase.PREMARKET:
                tail_parts.append("竞价锚点待竞价生成")
            else:
                tail_parts.append("竞价锚点待修复")

        status_line = (
            "状态摘要 | "
            f"当前处于{phase.value}自检，目标交易日={report.target_trade_date}，"
            f"正式离线日={report.formal_offline_date}，readiness={report.readiness.value}。"
        )

        if missing_parts:
            gap_lead = f"{report.formal_offline_date} 链路仍有缺口："
            gap_body = "，".join(missing_parts)
        else:
            gap_lead = f"{report.formal_offline_date} 正式离线链路已齐。"
            gap_body = ""
        runtime_ready_text = "；".join(ready_parts) if ready_parts else "运行时快缓存仍需继续补齐"
        tail_text = "；".join(tail_parts) if tail_parts else ""
        data_gap_parts = [part for part in (gap_lead + gap_body, runtime_ready_text, tail_text) if part]
        data_gap_line = "数据缺口 | " + "；".join(data_gap_parts)

        factor_text = ""
        if daily_factors is not None and daily_factors.missing_count > 0:
            factor_text = (
                f"因子缺口{daily_factors.missing_count}只"
                f"（可补{daily_factors.actionable_missing_count}，"
                f"结构性{daily_factors.structural_gap_count}，"
                f"缓存{daily_factors.cache_gap_count}，"
                f"dead {daily_factors.dead_symbol_count}）"
            )
        premarket_kaipan_ready = (
            phase == RunPhase.PREMARKET
            and daily_dde is not None
            and daily_dde.action == StartupAction.HEAVY_SYNC_ALLOWED
        )
        impact_parts: list[str] = []
        if missing_parts:
            impact_parts.append("当前更适合先看历史上下文和缺口修复")
        else:
            impact_parts.append("正式离线链路已基本可用")
        if premarket_kaipan_ready:
            impact_parts.append("当前处于00:00-09:00启动修复窗，Kaipan历史接口已可请求，DDE/热板/昨日涨停允许继续补数")
        if factor_text:
            impact_parts.append(factor_text)
        if auction_anchor is not None and not auction_anchor.ready and phase == RunPhase.PREMARKET:
            impact_parts.append("竞价锚点要等真实竞价后才会转成可执行盘前视图")
        impact_line = "影响判断 | " + "；".join(impact_parts) + "。"
        return (status_line, data_gap_line, impact_line)

    @staticmethod
    def _render_factor_digest(report: StartupSelfCheckReport) -> str | None:
        factor_status = report.by_dataset().get("daily_factors")
        if factor_status is None:
            return None
        if not any(
            (
                factor_status.missing_count,
                factor_status.structural_gap_count,
                factor_status.cache_gap_count,
                factor_status.dead_symbol_count,
                factor_status.current_trade_ready_count,
            )
        ):
            return None
        if factor_status.missing_count <= 0:
            return None
        return (
            "因子说明 | "
            f"因子缺口共{factor_status.missing_count}只，"
            f"其中可补{factor_status.actionable_missing_count}只，"
            f"结构性缺口{factor_status.structural_gap_count}只，"
            f"纯缓存缺口{factor_status.cache_gap_count}只，"
            f"dead 缺口{factor_status.dead_symbol_count}只，"
            f"当日已具备 current_trade_ready 的有{factor_status.current_trade_ready_count}只。"
        )

    def render_execution_summary(self, bundle: StartupExecutionBundle) -> tuple[str, ...]:
        details: list[str] = []
        report = bundle.plan.report
        details.extend(self._render_startup_digest(report, phase=bundle.plan.phase))
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
        if any(
            (
                bundle.plan.sync_pipeline_targets,
                bundle.plan.sync_network_targets,
                bundle.plan.sync_analytics_targets,
                bundle.plan.sync_factor_cache_gaps,
            )
        ):
            details.append(
                "self_check.sync_scope "
                f"| pipe={bundle.plan.sync_pipeline_targets} "
                f"| net={bundle.plan.sync_network_targets} "
                f"| calc={bundle.plan.sync_analytics_targets} "
                f"| factor_cache_gap={bundle.plan.sync_factor_cache_gaps}"
            )
        factor_status = report.by_dataset().get("daily_factors")
        if factor_status is not None and any(
            (
                factor_status.actionable_missing_count != factor_status.missing_count,
                factor_status.structural_gap_count,
                factor_status.cache_gap_count,
                factor_status.dead_symbol_count,
                factor_status.current_trade_ready_count,
            )
        ):
            details.append(
                "self_check.factor_breakdown "
                f"| actionable={factor_status.actionable_missing_count} "
                f"| structural={factor_status.structural_gap_count} "
                f"| cache_gap={factor_status.cache_gap_count} "
                f"| dead={factor_status.dead_symbol_count} "
                f"| current_trade_ready={factor_status.current_trade_ready_count}"
            )
        previous_settlement_line = self._render_previous_settlement_line(bundle.plan.previous_settlement_payload)
        if previous_settlement_line:
            details.append(previous_settlement_line)
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
