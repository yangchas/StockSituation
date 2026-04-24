from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time

from engine_next.contracts.offline_sync_contracts import WatermarkSnapshot
from engine_next.domain.enums import RunPhase, StartupAction, StartupReadinessLevel
from engine_next.domain.models import StartupDatasetStatus, StartupSelfCheckReport

PREMARKET_HEAVY_SYNC_CUTOFF = dt_time(9, 0)


def infer_run_phase(now: datetime) -> RunPhase:
    current = now.time()
    if dt_time(0, 0) <= current < dt_time(9, 15):
        return RunPhase.PREMARKET
    if dt_time(9, 15) <= current < dt_time(9, 30):
        return RunPhase.AUCTION
    if dt_time(9, 30) <= current <= dt_time(15, 0):
        return RunPhase.INTRADAY
    if dt_time(15, 0) < current < dt_time(23, 59, 59):
        return RunPhase.POSTMARKET
    return RunPhase.NIGHT


@dataclass(frozen=True)
class StartupSelfCheckRequest:
    now: datetime
    trade_date: str
    previous_trade_date: str
    symbol_count: int
    watermark_snapshot: WatermarkSnapshot
    yest_limit_pool_ready: bool = False
    hot_plates_ready: bool = False
    stock_plate_mapping_ready: bool = False
    auction_anchor_ready: bool = False
    redis_factor_ready_count: int = 0
    redis_chip_ready_count: int = 0
    redis_dde_ready_count: int = 0
    small_repair_limit: int = 500


class StartupSelfCheckService:
    """
    Time-aware startup readiness service aligned with engine_v2.

    The goal is not to hard fail on every missing dataset. Instead we return
    an explicit degradation level plus the actions still allowed in the
    current time window.
    """

    def build_report(self, request: StartupSelfCheckRequest) -> StartupSelfCheckReport:
        phase = infer_run_phase(request.now)
        formal_offline_date = self._formal_offline_date(request)

        kline_missing = self._missing_count(
            request.symbol_count,
            request.watermark_snapshot.kline_latest_dates,
            formal_offline_date,
        )
        factor_missing = max(request.symbol_count - request.redis_factor_ready_count, 0)
        chip_missing = max(request.symbol_count - request.redis_chip_ready_count, 0)
        dde_missing = max(request.symbol_count - request.redis_dde_ready_count, 0)

        statuses = (
            self._build_heavy_status("daily_kline", phase, formal_offline_date, request, kline_missing),
            self._build_heavy_status("daily_factors", phase, formal_offline_date, request, factor_missing),
            self._build_heavy_status("chip_peaks", phase, formal_offline_date, request, chip_missing),
            self._build_heavy_status("daily_dde", phase, formal_offline_date, request, dde_missing),
            self._build_fast_status(
                dataset="yest_limit_pool",
                phase=phase,
                target_date=request.previous_trade_date,
                ready=request.yest_limit_pool_ready,
                source="kaipan",
                repair_note="Fast startup repair is allowed via Kaipan delayed source.",
            ),
            self._build_fast_status(
                dataset="hot_plates",
                phase=phase,
                target_date=request.previous_trade_date if phase == RunPhase.PREMARKET else request.trade_date,
                ready=request.hot_plates_ready,
                source="kaipan",
                repair_note="Kaipan hot plates can be refreshed in small batches and cached by trade_date.",
            ),
            self._build_fast_status(
                dataset="stock_plate_mapping",
                phase=phase,
                target_date=request.trade_date,
                ready=request.stock_plate_mapping_ready,
                source="csv+redis",
                repair_note="CSV plate-stock mapping should be reloaded on startup when missing.",
            ),
            self._build_auction_status(phase, request),
        )

        readiness = self._derive_readiness(phase, statuses)
        recommended_actions = tuple(self._collect_actions(phase, statuses))
        notes = self._build_notes(phase, formal_offline_date, request)

        return StartupSelfCheckReport(
            phase=phase,
            readiness=readiness,
            target_trade_date=request.trade_date,
            formal_offline_date=formal_offline_date,
            statuses=statuses,
            recommended_actions=recommended_actions,
            notes=notes,
        )

    def _formal_offline_date(self, request: StartupSelfCheckRequest) -> str:
        current = request.now.time()
        if current >= dt_time(17, 30):
            return request.trade_date
        return request.previous_trade_date

    def _missing_count(self, total_count: int, latest_dates: dict[str, str], target_date: str) -> int:
        ready_count = sum(1 for latest in latest_dates.values() if latest == target_date)
        return max(total_count - ready_count, 0)

    def _build_heavy_status(
        self,
        dataset: str,
        phase: RunPhase,
        target_date: str,
        request: StartupSelfCheckRequest,
        missing_count: int,
    ) -> StartupDatasetStatus:
        ready = missing_count == 0
        action = StartupAction.NOOP
        notes = []
        severity = "info"

        if ready:
            notes.append("Watermark/cache audit shows this heavy dataset is ready.")
        elif phase == RunPhase.PREMARKET and request.now.time() < PREMARKET_HEAVY_SYNC_CUTOFF:
            action = StartupAction.HEAVY_SYNC_ALLOWED
            severity = "warn"
            notes.append("Before 09:00 premarket, heavy repair is still allowed.")
        elif phase == RunPhase.PREMARKET and missing_count <= request.small_repair_limit:
            action = StartupAction.HEAVY_SYNC_LIMITED
            severity = "warn"
            notes.append("After 09:00 premarket, only small-scope repair should run.")
        elif phase == RunPhase.PREMARKET:
            action = StartupAction.HEAVY_SYNC_BLOCKED
            severity = "error"
            notes.append("After 09:00 premarket, large heavy sync is blocked to avoid colliding with the open.")
        elif phase in (RunPhase.AUCTION, RunPhase.INTRADAY):
            action = StartupAction.HEAVY_SYNC_BLOCKED
            severity = "error"
            notes.append("During auction/intraday, heavy sync must be audited but not fully executed.")
        elif phase == RunPhase.POSTMARKET:
            action = StartupAction.HEAVY_SYNC_ALLOWED
            severity = "warn"
            notes.append("Postmarket is the formal recovery window for heavy offline datasets.")
        else:
            action = StartupAction.HEAVY_SYNC_ALLOWED
            severity = "warn"
            notes.append("Night window allows formal offline recovery.")

        notes.append(f"Missing count: {missing_count}/{request.symbol_count}.")
        return StartupDatasetStatus(
            dataset=dataset,
            phase=phase,
            target_date=target_date,
            required=dataset in {"daily_kline", "daily_factors"},
            ready=ready,
            source="tdengine+redis",
            action=action,
            missing_count=missing_count,
            total_count=request.symbol_count,
            freshness_date=target_date if ready else "",
            severity=severity,
            notes=tuple(notes),
        )

    def _build_fast_status(
        self,
        *,
        dataset: str,
        phase: RunPhase,
        target_date: str,
        ready: bool,
        source: str,
        repair_note: str,
    ) -> StartupDatasetStatus:
        return StartupDatasetStatus(
            dataset=dataset,
            phase=phase,
            target_date=target_date,
            required=dataset != "hot_plates",
            ready=ready,
            source=source,
            action=StartupAction.NOOP if ready else StartupAction.FAST_REPAIR_ALLOWED,
            severity="info" if ready else "warn",
            notes=((("Ready in cache/view.",) if ready else (repair_note, "This dataset is lightweight enough for startup repair."))),
        )

    def _build_auction_status(self, phase: RunPhase, request: StartupSelfCheckRequest) -> StartupDatasetStatus:
        if phase not in (RunPhase.AUCTION, RunPhase.INTRADAY):
            required = False
        else:
            required = True

        ready = request.auction_anchor_ready
        notes = []
        if ready:
            notes.append("Auction anchor archive is available.")
            action = StartupAction.NOOP
            severity = "info"
        else:
            action = StartupAction.AUCTION_FALLBACK_RECOVERY if phase in (RunPhase.AUCTION, RunPhase.INTRADAY) else StartupAction.PRELOAD_ONLY
            severity = "warn" if phase == RunPhase.AUCTION else "error" if phase == RunPhase.INTRADAY else "info"
            notes.append("Use Redis anchor -> Redis 0925 -> TDengine -> Wencai fallback chain.")
            if phase == RunPhase.INTRADAY:
                notes.append("If startup is after 09:30, recovered results should be written back to Redis.")
            if phase == RunPhase.AUCTION:
                notes.append("If 09:25 anchor is still missing near 09:30, prefer fast fallback instead of waiting for offline repair.")

        return StartupDatasetStatus(
            dataset="auction_anchor",
            phase=phase,
            target_date=request.trade_date,
            required=required,
            ready=ready,
            source="redis/tdengine/wencai",
            action=action,
            severity=severity,
            notes=tuple(notes),
        )

    def _derive_readiness(
        self,
        phase: RunPhase,
        statuses: tuple[StartupDatasetStatus, ...],
    ) -> StartupReadinessLevel:
        status_map = {status.dataset: status for status in statuses}
        heavy_required = ("daily_kline", "daily_factors")
        heavy_all_ready = all(status_map[name].ready for name in heavy_required)
        heavy_blocked_missing = any(
            (not status_map[name].ready) and status_map[name].action == StartupAction.HEAVY_SYNC_BLOCKED
            for name in heavy_required
        )
        lightweight_statuses = (
            status_map["yest_limit_pool"],
            status_map["stock_plate_mapping"],
            status_map["hot_plates"],
        )
        lightweight_repairable = all(
            status.ready or status.action == StartupAction.FAST_REPAIR_ALLOWED
            for status in lightweight_statuses
        )
        auction_ok = status_map["auction_anchor"].ready or phase not in (RunPhase.AUCTION, RunPhase.INTRADAY)

        if phase == RunPhase.POSTMARKET and not heavy_all_ready:
            return StartupReadinessLevel.POSTMARKET_ONLY
        if heavy_blocked_missing:
            return StartupReadinessLevel.OBSERVE_ONLY
        if heavy_all_ready and lightweight_repairable and auction_ok:
            return StartupReadinessLevel.FULL_READY
        if phase == RunPhase.PREMARKET and lightweight_repairable:
            return StartupReadinessLevel.TRADE_READY_DEGRADED
        if phase in (RunPhase.AUCTION, RunPhase.INTRADAY) and lightweight_repairable and auction_ok:
            return StartupReadinessLevel.TRADE_READY_DEGRADED
        return StartupReadinessLevel.OBSERVE_ONLY

    def _collect_actions(self, phase: RunPhase, statuses: tuple[StartupDatasetStatus, ...]) -> list[str]:
        actions: list[str] = []
        for status in statuses:
            if status.ready:
                continue
            if status.action == StartupAction.HEAVY_SYNC_ALLOWED:
                actions.append(f"{status.dataset}: run formal integrated sync for {status.target_date}.")
            elif status.action == StartupAction.HEAVY_SYNC_LIMITED:
                actions.append(f"{status.dataset}: only repair small missing set, avoid full-market sync near open.")
            elif status.action == StartupAction.HEAVY_SYNC_BLOCKED:
                actions.append(f"{status.dataset}: keep missingness visible but defer heavy repair to postmarket/night.")
            elif status.action == StartupAction.FAST_REPAIR_ALLOWED:
                actions.append(f"{status.dataset}: fast startup repair is allowed now.")
            elif status.action == StartupAction.AUCTION_FALLBACK_RECOVERY:
                actions.append("auction_anchor: recover via Redis -> TDengine -> Wencai and write back to Redis.")

        if phase == RunPhase.POSTMARKET:
            actions.append("Evaluate whether recap should run after truth datasets are confirmed ready.")
        return actions

    def _build_notes(
        self,
        phase: RunPhase,
        formal_offline_date: str,
        request: StartupSelfCheckRequest,
    ) -> tuple[str, ...]:
        notes = [
            f"Phase inferred from runtime clock: {phase.value}.",
            f"Formal offline target date resolved to {formal_offline_date}.",
            "Heavy datasets are audited by bulk watermark snapshot first; do not replace with per-symbol TDengine latest queries.",
        ]
        if phase == RunPhase.PREMARKET and request.now.time() >= PREMARKET_HEAVY_SYNC_CUTOFF:
            notes.append("After 09:00, startup should avoid large formal sync jobs that may collide with the open.")
            notes.append("Only missing subsets below the configured small repair limit may continue.")
        if phase in (RunPhase.AUCTION, RunPhase.INTRADAY):
            notes.append("Intraday startup should still audit full missingness, but prefer Redis-first small repairs.")
            notes.append("If auction anchor is missing after 09:30, Wencai fallback is allowed and should be written back to Redis.")
        return tuple(notes)


class PremarketReadinessService:
    """Thin wrapper for callers that want an explicit premarket-only check."""

    def __init__(self) -> None:
        self._startup = StartupSelfCheckService()

    def build_report(self, request: StartupSelfCheckRequest) -> StartupSelfCheckReport:
        report = self._startup.build_report(request)
        if report.phase != RunPhase.PREMARKET:
            statuses = report.statuses + (
                StartupDatasetStatus(
                    dataset="premarket_window",
                    phase=report.phase,
                    target_date=request.trade_date,
                    required=False,
                    ready=False,
                    source="runtime_clock",
                    action=StartupAction.DEFER_TO_POSTMARKET,
                    severity="warn",
                    notes=("Premarket readiness was requested outside the premarket window.",),
                ),
            )
            return StartupSelfCheckReport(
                phase=report.phase,
                readiness=StartupReadinessLevel.OBSERVE_ONLY,
                target_trade_date=report.target_trade_date,
                formal_offline_date=report.formal_offline_date,
                statuses=statuses,
                recommended_actions=report.recommended_actions + ("Wait for the next valid premarket window or use startup self-check instead.",),
                notes=report.notes,
            )
        return report
