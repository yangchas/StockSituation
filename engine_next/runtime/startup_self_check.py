from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time
from functools import lru_cache

from engine_next.contracts.offline_sync_contracts import DatasetGapMatrix, WatermarkSnapshot
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
    symbols: tuple[str, ...]
    symbol_count: int
    watermark_snapshot: WatermarkSnapshot
    redis_factor_cache_ready: dict[str, bool] | None = None
    current_trade_factor_cache_ready: dict[str, bool] | None = None
    current_trade_chip_cache_ready: dict[str, bool] | None = None
    yest_limit_pool_ready: bool = False
    hot_plates_ready: bool = False
    hot_plates_today_ready: bool = False
    hot_plates_effective_ready: bool = False
    hot_plates_effective_trade_date: str = ""
    stock_plate_mapping_ready: bool = False
    auction_anchor_ready: bool = False
    redis_factor_ready_count: int = 0
    redis_chip_ready_count: int = 0
    redis_dde_ready_count: int = 0
    listing_dates: dict[str, str] | None = None
    kline_row_counts: dict[str, int] | None = None
    cached_structural_factor_gap: dict[str, bool] | None = None
    dataset_gap_matrix: DatasetGapMatrix | None = None
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
        effective_hot_plate_ready = bool(request.hot_plates_effective_ready or request.hot_plates_ready)
        effective_hot_plate_trade_date = str(
            request.hot_plates_effective_trade_date
            or (request.previous_trade_date if phase == RunPhase.PREMARKET else request.trade_date)
        ).strip()
        hot_plate_notes: tuple[str, ...]
        if effective_hot_plate_ready:
            note_list = ["Ready in cache/view."]
            if not request.hot_plates_today_ready and effective_hot_plate_trade_date == request.previous_trade_date:
                note_list.append("Today hot plates are not ready yet; startup is falling back to previous-trade-date cache.")
            elif effective_hot_plate_trade_date and effective_hot_plate_trade_date != request.trade_date:
                note_list.append(f"Effective hot plate cache date: {effective_hot_plate_trade_date}.")
            hot_plate_notes = tuple(note_list)
        else:
            note_list = [
                "Kaipan hot plates can be refreshed in small batches and cached by trade_date.",
                "This dataset is lightweight enough for startup repair.",
            ]
            if not request.hot_plates_today_ready:
                note_list.append("Neither today's nor fallback hot plate cache is ready.")
            hot_plate_notes = tuple(note_list)

        kline_missing, kline_dead = self._classify_kline_missing(
            request.symbols,
            request.watermark_snapshot.kline_latest_dates,
            formal_offline_date,
        )
        kline_actionable_missing = max(kline_missing - kline_dead, 0)
        factor_counts = self._classify_factor_missing(request)
        factor_missing = factor_counts["missing"]
        chip_missing = max(request.symbol_count - request.redis_chip_ready_count, 0)
        dde_missing = max(request.symbol_count - request.redis_dde_ready_count, 0)

        statuses = (
            self._build_heavy_status(
                "daily_kline",
                phase,
                formal_offline_date,
                request,
                kline_missing,
                actionable_missing_count=kline_actionable_missing,
                dead_symbol_count=kline_dead,
            ),
            self._build_heavy_status(
                "daily_factors",
                phase,
                formal_offline_date,
                request,
                factor_missing,
                actionable_missing_count=factor_counts["actionable_missing"],
                structural_gap_count=factor_counts["structural_gap"],
                cache_gap_count=factor_counts["cache_gap"],
                dead_symbol_count=factor_counts["dead_symbol_gap"],
                current_trade_ready_count=factor_counts["current_trade_ready"],
            ),
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
                target_date=effective_hot_plate_trade_date,
                ready=effective_hot_plate_ready,
                source="kaipan",
                repair_note="Kaipan hot plates can be refreshed in small batches and cached by trade_date.",
                notes=hot_plate_notes,
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

    def _classify_kline_missing(
        self,
        symbols: tuple[str, ...],
        latest_dates: dict[str, str],
        target_date: str,
    ) -> tuple[int, int]:
        missing_count = 0
        dead_symbol_count = 0
        for symbol in symbols:
            latest = latest_dates.get(symbol)
            if latest and latest >= target_date:
                continue
            missing_count += 1
            if not latest:
                dead_symbol_count += 1
        return missing_count, dead_symbol_count

    def _classify_factor_missing(self, request: StartupSelfCheckRequest) -> dict[str, int]:
        prev_ready_map = request.redis_factor_cache_ready or {}
        current_factor_ready_map = request.current_trade_factor_cache_ready or {}
        current_chip_ready_map = request.current_trade_chip_cache_ready or {}
        kline_dates = request.watermark_snapshot.kline_latest_dates
        factor_dates = request.watermark_snapshot.factor_latest_dates
        listing_dates = self._resolve_listing_dates(request)
        kline_row_counts = self._resolve_kline_row_counts(request)
        cached_structural_factor_gap = request.cached_structural_factor_gap or {}
        factor_watermark_pending = set(request.dataset_gap_matrix.symbols_for("daily_factors")) if request.dataset_gap_matrix else set()
        factor_cache_pending = set(request.dataset_gap_matrix.symbols_for("factor_cache")) if request.dataset_gap_matrix else set()

        missing = 0
        structural_gap = 0
        cache_gap = 0
        dead_symbol_gap = 0
        current_trade_ready = 0

        for symbol in request.symbols:
            prev_ready = bool(prev_ready_map.get(symbol))
            current_factor_ready = bool(current_factor_ready_map.get(symbol))
            current_chip_ready = bool(current_chip_ready_map.get(symbol))
            latest_kline = kline_dates.get(symbol)
            latest_factor = factor_dates.get(symbol)

            if current_factor_ready or (latest_factor and latest_factor >= request.trade_date):
                current_trade_ready += 1
            factor_missing_by_matrix = symbol in factor_watermark_pending or symbol in factor_cache_pending
            if not factor_missing_by_matrix and prev_ready:
                continue

            missing += 1
            if symbol in factor_cache_pending and symbol not in factor_watermark_pending:
                cache_gap += 1
            elif cached_structural_factor_gap.get(symbol) is True:
                structural_gap += 1
            elif self._is_structural_factor_gap(
                symbol=symbol,
                trade_date=request.trade_date,
                latest_kline=latest_kline,
                current_chip_ready=current_chip_ready,
                listing_dates=listing_dates,
                kline_row_counts=kline_row_counts,
            ):
                structural_gap += 1
            elif not latest_kline:
                dead_symbol_gap += 1

        actionable_missing = max(missing - structural_gap - cache_gap - dead_symbol_gap, 0)
        return {
            "missing": missing,
            "actionable_missing": actionable_missing,
            "structural_gap": structural_gap,
            "cache_gap": cache_gap,
            "dead_symbol_gap": dead_symbol_gap,
            "current_trade_ready": current_trade_ready,
        }

    def _resolve_listing_dates(self, request: StartupSelfCheckRequest) -> dict[str, str]:
        if request.listing_dates is None:
            return {}
        return {
            str(symbol): str(listing_date or "").strip()
            for symbol, listing_date in request.listing_dates.items()
            if str(symbol or "").strip() and str(listing_date or "").strip()
        }

    def _resolve_kline_row_counts(self, request: StartupSelfCheckRequest) -> dict[str, int]:
        if request.kline_row_counts is None:
            return {}
        return {
            str(symbol): int(count or 0)
            for symbol, count in request.kline_row_counts.items()
            if str(symbol or "").strip()
        }

    def _is_structural_factor_gap(
        self,
        *,
        symbol: str,
        trade_date: str,
        latest_kline: str | None,
        current_chip_ready: bool,
        listing_dates: dict[str, str],
        kline_row_counts: dict[str, int],
    ) -> bool:
        if not latest_kline or latest_kline < trade_date:
            return False
        if current_chip_ready:
            return True
        kline_row_count = int(kline_row_counts.get(symbol, 0) or 0)
        if 0 < kline_row_count < 35:
            return True
        listing_date = str(listing_dates.get(symbol) or "").strip()
        if not listing_date:
            return False
        cutoff_date = self._factor_structural_cutoff_date(trade_date)
        return bool(cutoff_date and listing_date >= cutoff_date)

    @staticmethod
    @lru_cache(maxsize=64)
    def _factor_structural_cutoff_date(trade_date: str, required_trade_days: int = 35) -> str:
        try:
            from web.services.trading_calendar_service import TradingCalendarService

            calendar = TradingCalendarService()
            cutoff = trade_date
            for _ in range(max(required_trade_days - 1, 0)):
                cutoff = calendar.get_previous_trading_day(cutoff)
            return cutoff
        except Exception:
            return ""

    def _build_heavy_status(
        self,
        dataset: str,
        phase: RunPhase,
        target_date: str,
        request: StartupSelfCheckRequest,
        missing_count: int,
        *,
        actionable_missing_count: int | None = None,
        structural_gap_count: int = 0,
        cache_gap_count: int = 0,
        dead_symbol_count: int = 0,
        current_trade_ready_count: int = 0,
    ) -> StartupDatasetStatus:
        effective_missing_count = missing_count if actionable_missing_count is None else actionable_missing_count
        ready = effective_missing_count == 0
        action = StartupAction.NOOP
        notes = []
        severity = "info"

        if ready:
            notes.append("Watermark/cache audit shows this heavy dataset is ready.")
        elif phase == RunPhase.PREMARKET and request.now.time() < PREMARKET_HEAVY_SYNC_CUTOFF:
            action = StartupAction.HEAVY_SYNC_ALLOWED
            severity = "warn"
            notes.append("Before 09:00 premarket, heavy repair is still allowed.")
        elif phase == RunPhase.PREMARKET and effective_missing_count <= request.small_repair_limit:
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
        if effective_missing_count != missing_count:
            notes.append(f"Actionable missing count: {effective_missing_count}.")
        if structural_gap_count:
            notes.append(f"Structural gap count: {structural_gap_count}.")
        if cache_gap_count:
            notes.append(f"Cache gap count: {cache_gap_count}.")
        if dead_symbol_count:
            notes.append(f"Dead-symbol gap count: {dead_symbol_count}.")
        if current_trade_ready_count:
            notes.append(f"Current-trade ready count: {current_trade_ready_count}.")
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
            actionable_missing_count=effective_missing_count,
            structural_gap_count=structural_gap_count,
            cache_gap_count=cache_gap_count,
            dead_symbol_count=dead_symbol_count,
            current_trade_ready_count=current_trade_ready_count,
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
        notes: tuple[str, ...] | None = None,
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
            notes=notes or (
                ("Ready in cache/view.",)
                if ready
                else (repair_note, "This dataset is lightweight enough for startup repair.")
            ),
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
            (
                "Offline formal target date resolved to "
                f"{formal_offline_date}, while runtime trade date is {request.trade_date}."
            ),
            "Heavy datasets are audited by bulk watermark snapshot first; do not replace with per-symbol TDengine latest queries.",
        ]
        if phase == RunPhase.PREMARKET and request.now.time() >= PREMARKET_HEAVY_SYNC_CUTOFF:
            notes.append("After 09:00, startup should avoid large formal sync jobs that may collide with the open.")
            notes.append("Only missing subsets below the configured small repair limit may continue.")
        if request.hot_plates_effective_ready and not request.hot_plates_today_ready:
            effective_trade_date = str(request.hot_plates_effective_trade_date or request.previous_trade_date).strip()
            notes.append(
                "Hot plates are currently using fallback cache "
                f"from {effective_trade_date}; today's hot plate truth is not ready yet."
            )
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
