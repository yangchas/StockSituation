from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from engine_next.connectors.baostock_connector import BaostockConnector, BaostockDailyBar
from engine_next.contracts.baostock_contracts import (
    BaostockAvailabilityResult,
    BaostockDailyKlineRequest,
    check_baostock_daily_kline_availability,
)
from engine_next.contracts.offline_sync_contracts import GapFillPlan, build_gap_fill_plan
from engine_next.domain.enums import ExecutionEnvironment
from engine_next.domain.models import PersistenceWritePlan, RedisViewMaterialization
from engine_next.contracts.offline_sync_contracts import IntegratedSyncResult, WatermarkSnapshot
from engine_next.offline import (
    IntegratedSyncExecutor,
    RedisViewBuilder,
    TdenginePersistenceAdapter,
)
from engine_next.offline.kline_factor_pipeline import FACTOR_FIELDS, KLINE_FACTOR_PIPELINE
from engine_next.runtime.execution_profile import get_default_execution_profile


def _is_offline_sync_symbol(value: str) -> bool:
    code = str(value or "").strip()
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


@dataclass(frozen=True)
class OfflineSyncRequest:
    now: datetime
    target_date: str
    previous_trade_date: str
    symbols: tuple[str, ...]
    kline_watermarks: Dict[str, str]
    factor_watermarks: Dict[str, str]
    redis_factor_cache_ready: Dict[str, bool]
    override_date: Optional[str] = None
    environment: ExecutionEnvironment = ExecutionEnvironment.SERVER


@dataclass(frozen=True)
class OfflineSyncDecision:
    allowed: bool
    target_date: str
    formal_kline_date: str
    availability: BaostockAvailabilityResult
    missing_kline_symbols: tuple[str, ...]
    missing_factor_symbols: tuple[str, ...]
    kline_gap_plan: Optional[GapFillPlan]
    factor_gap_plan: Optional[GapFillPlan]
    stage_names: tuple[str, ...]
    notes: str


@dataclass(frozen=True)
class OfflineKlineSyncPlan:
    decision: OfflineSyncDecision
    fetch_requests: tuple[BaostockDailyKlineRequest, ...]
    normalized_rows: tuple[dict[str, Any], ...]
    tdengine_write_plan: Optional[PersistenceWritePlan]
    redis_materialization: Optional[RedisViewMaterialization]
    redis_payload: Dict[str, Dict[str, Any]]
    notes: tuple[str, ...]


class ServerOnlyOfflineSyncExecutor:
    """
    Server-side planner for the offline kline + factor pipeline.

    This executor does not perform network or database writes yet.
    It converts runtime state into an execution decision that later
    worker implementations can consume.
    """

    def __init__(self) -> None:
        self._baostock = BaostockConnector()
        self._tdengine = TdenginePersistenceAdapter()
        self._redis_views = RedisViewBuilder()
        self._integrated_sync = IntegratedSyncExecutor(environment=ExecutionEnvironment.SERVER)

    def build_decision(self, request: OfflineSyncRequest) -> OfflineSyncDecision:
        profile = get_default_execution_profile(request.environment)
        availability = check_baostock_daily_kline_availability(
            now=request.now,
            target_date=request.target_date,
            previous_trade_date=request.previous_trade_date,
        )

        if not profile.allow_runtime_jobs:
            return OfflineSyncDecision(
                allowed=False,
                target_date=request.target_date,
                formal_kline_date=availability.fallback_trade_date or request.target_date,
                availability=availability,
                missing_kline_symbols=(),
                missing_factor_symbols=(),
                kline_gap_plan=None,
                factor_gap_plan=None,
                stage_names=tuple(stage.name for stage in KLINE_FACTOR_PIPELINE),
                notes="Offline sync is server-only. Local Windows must not run formal sync jobs.",
            )

        formal_kline_date = (
            request.override_date
            or request.target_date
            if availability.ready
            else availability.fallback_trade_date or request.previous_trade_date
        )

        missing_kline_symbols = tuple(
            symbol
            for symbol in request.symbols
            if request.kline_watermarks.get(symbol) != formal_kline_date
        )
        missing_factor_symbols = tuple(
            symbol
            for symbol in request.symbols
            if request.factor_watermarks.get(symbol) != formal_kline_date
        )

        kline_gap_plan = build_gap_fill_plan(
            target_date=formal_kline_date,
            missing_symbols=list(missing_kline_symbols),
            reason="daily_kline watermark missing or stale",
        )
        factor_gap_plan = build_gap_fill_plan(
            target_date=formal_kline_date,
            missing_symbols=list(missing_factor_symbols),
            reason="daily_factors watermark missing or stale",
            lookback_days=60 if missing_factor_symbols else 0,
        )

        notes: List[str] = [
            availability.reason,
            f"Factor fields expected: {', '.join(FACTOR_FIELDS)}.",
        ]
        if missing_kline_symbols:
            notes.append(f"Kline gap fill required for {len(missing_kline_symbols)} symbols.")
        if missing_factor_symbols:
            notes.append(f"Factor gap fill required for {len(missing_factor_symbols)} symbols.")
        if not missing_kline_symbols and not missing_factor_symbols:
            notes.append("All tracked symbols are ready for the target formal date.")

        return OfflineSyncDecision(
            allowed=True,
            target_date=request.target_date,
            formal_kline_date=formal_kline_date,
            availability=availability,
            missing_kline_symbols=missing_kline_symbols,
            missing_factor_symbols=missing_factor_symbols,
            kline_gap_plan=kline_gap_plan,
            factor_gap_plan=factor_gap_plan,
            stage_names=tuple(stage.name for stage in KLINE_FACTOR_PIPELINE),
            notes=" ".join(notes),
        )

    def build_kline_sync_plan(
        self,
        request: OfflineSyncRequest,
        fetched_rows_by_symbol: Optional[Dict[str, List[BaostockDailyBar]]] = None,
        perform_fetch: bool = False,
    ) -> OfflineKlineSyncPlan:
        decision = self.build_decision(request)
        fetch_requests = tuple(
            BaostockDailyKlineRequest(symbol=symbol, trade_date=decision.formal_kline_date)
            for symbol in decision.missing_kline_symbols
        )

        if not decision.allowed or not fetch_requests:
            return OfflineKlineSyncPlan(
                decision=decision,
                fetch_requests=fetch_requests,
                normalized_rows=(),
                tdengine_write_plan=None,
                redis_materialization=None,
                redis_payload={},
                notes=("No kline fetch work required.",),
            )

        rows_by_symbol = dict(fetched_rows_by_symbol or {})
        if perform_fetch and not rows_by_symbol:
            for req in fetch_requests:
                rows_by_symbol[req.symbol] = self._baostock.fetch_daily_kline(req)

        normalized_rows: List[dict[str, Any]] = []
        notes: List[str] = []
        for req in fetch_requests:
            bars = rows_by_symbol.get(req.symbol, [])
            if not bars:
                notes.append(f"No fetched bars available yet for {req.symbol}.")
                continue
            if not self._baostock.validate_daily_kline(bars):
                notes.append(f"Validation failed for {req.symbol}.")
                continue
            normalized_rows.extend(self._tdengine.prepare_rows(self._baostock.normalize_daily_kline(bars), source="baostock"))

        if not normalized_rows:
            return OfflineKlineSyncPlan(
                decision=decision,
                fetch_requests=fetch_requests,
                normalized_rows=(),
                tdengine_write_plan=None,
                redis_materialization=None,
                redis_payload={},
                notes=tuple(notes or ("Fetch requests prepared; waiting for fetched bars.",)),
            )

        tdengine_write_plan = self._tdengine.build_write_plan(
            dataset="daily_kline",
            rows=normalized_rows,
            trade_date=decision.formal_kline_date,
        )
        redis_materialization, redis_payload = self._redis_views.materialize(
            dataset="daily_kline",
            trade_date=decision.formal_kline_date,
            rows=normalized_rows,
        )
        notes.append(f"Prepared {len(normalized_rows)} normalized daily_kline rows.")

        return OfflineKlineSyncPlan(
            decision=decision,
            fetch_requests=fetch_requests,
            normalized_rows=tuple(normalized_rows),
            tdengine_write_plan=tdengine_write_plan,
            redis_materialization=redis_materialization,
            redis_payload=redis_payload,
            notes=tuple(notes),
        )

    def preload_watermark_snapshot(self, request: OfflineSyncRequest) -> WatermarkSnapshot:
        decision = self.build_decision(request)
        return self._integrated_sync.watermark_audit_service.preload_all_watermarks(decision.formal_kline_date)

    def _safe_redis_ping(self) -> bool:
        try:
            return bool(self._integrated_sync.redis.ping())
        except Exception:
            return False

    def _safe_redis_hlen(self, key: str) -> int:
        try:
            return int(self._integrated_sync.redis.hlen(key) or 0)
        except Exception:
            return 0

    def _safe_redis_hmget_count(self, key: str, fields: tuple[str, ...]) -> int:
        if not fields:
            return 0
        try:
            values = self._integrated_sync.redis.hmget(key, list(fields))
            return sum(1 for value in values if value)
        except Exception:
            return 0

    @staticmethod
    def _safe_ratio(ready: int, total: int) -> str:
        return f"{ready}/{total}" if total > 0 else "0/0"

    def _build_settlement_audit_lines(
        self,
        *,
        request: OfflineSyncRequest,
        decision: OfflineSyncDecision,
        snapshot: WatermarkSnapshot,
        pipeline_symbols: tuple[str, ...],
        network_symbols: tuple[str, ...],
        analytics_symbols: tuple[str, ...],
    ) -> tuple[str, ...]:
        symbol_total = len(request.symbols)
        formal_date = decision.formal_kline_date
        symbol_universe = tuple(str(symbol) for symbol in request.symbols if str(symbol))
        kline_ready = sum(1 for symbol in request.symbols if snapshot.kline_latest_dates.get(symbol) == formal_date)
        dde_ready = sum(1 for symbol in request.symbols if snapshot.dde_latest_dates.get(symbol) == formal_date)
        factor_ready = sum(1 for symbol in request.symbols if snapshot.factor_latest_dates.get(symbol) == formal_date)
        factor_cache_ready = self._safe_redis_hmget_count(f"cache:stock_extra:{formal_date}", symbol_universe)
        chip_cache_ready = self._safe_redis_hmget_count(f"cache:chip_peaks:{formal_date}", symbol_universe)
        dde_cache_ready = self._safe_redis_hmget_count(f"cache:dde_ready:{formal_date}", symbol_universe)
        redis_ok = self._safe_redis_ping()
        tdengine_ok = bool(snapshot.kline_latest_dates or snapshot.dde_latest_dates or snapshot.factor_latest_dates)

        baostock_state = BaostockConnector.status_summary()
        if decision.availability.ready:
            availability_state = "ready"
            if baostock_state == "idle":
                baostock_state = "ready"
        else:
            fallback_date = decision.availability.fallback_trade_date or request.previous_trade_date
            availability_state = "formal_target_locked" if fallback_date == formal_date else f"fallback->{fallback_date}"
            if baostock_state == "idle":
                baostock_state = "formal_locked"

        lines = [
            (
                f"[settlement] audit | formal={formal_date} | source={availability_state} | "
                f"pipeline={len(pipeline_symbols)} | network={len(network_symbols)} | analytics={len(analytics_symbols)}"
            ),
            (
                f"[settlement] missing | "
                f"kline={symbol_total - kline_ready}/{symbol_total} | "
                f"dde={symbol_total - dde_ready}/{symbol_total} | "
                f"factor={symbol_total - factor_ready}/{symbol_total}"
            ),
            (
                f"[settlement] cache | "
                f"factor={self._safe_ratio(factor_cache_ready, symbol_total)} | "
                f"chip={self._safe_ratio(chip_cache_ready, symbol_total)} | "
                f"dde={self._safe_ratio(dde_cache_ready, symbol_total)}"
            ),
            (
                f"[settlement] source_check | "
                f"tdengine={'ok' if tdengine_ok else 'missing'} | "
                f"redis={'ok' if redis_ok else 'fail'} | "
                f"baostock={baostock_state} | "
                f"f10=lazy | dde_source=on_demand"
            ),
        ]

        if decision.notes:
            lines.append(f"[settlement] decision | {decision.notes}")
        return tuple(lines)

    def resolve_effective_target_symbols(
        self,
        request: OfflineSyncRequest,
        decision: OfflineSyncDecision | None = None,
        snapshot: WatermarkSnapshot | None = None,
    ) -> tuple[str, ...]:
        effective_decision = decision or self.build_decision(request)
        if snapshot is None:
            target_symbols = tuple(
                dict.fromkeys(
                    effective_decision.missing_kline_symbols
                    + effective_decision.missing_factor_symbols
                )
            )
            return tuple(symbol for symbol in target_symbols if _is_offline_sync_symbol(symbol))
        return self.resolve_pipeline_target_symbols(
            request,
            snapshot,
            effective_decision,
        )

    def resolve_network_target_symbols(
        self,
        request: OfflineSyncRequest,
        snapshot: WatermarkSnapshot,
        decision: OfflineSyncDecision | None = None,
    ) -> tuple[str, ...]:
        effective_decision = decision or self.build_decision(request)
        target_symbols = tuple(
            dict.fromkeys(
                tuple(
                    symbol
                    for symbol in request.symbols
                    if snapshot.kline_latest_dates.get(symbol) != effective_decision.formal_kline_date
                    or snapshot.dde_latest_dates.get(symbol) != effective_decision.formal_kline_date
                )
            )
        )
        return tuple(symbol for symbol in target_symbols if _is_offline_sync_symbol(symbol))

    def resolve_analytics_target_symbols(
        self,
        request: OfflineSyncRequest,
        network_symbols: tuple[str, ...],
        decision: OfflineSyncDecision | None = None,
    ) -> tuple[str, ...]:
        effective_decision = decision or self.build_decision(request)
        target_symbols = tuple(
            dict.fromkeys(
                effective_decision.missing_factor_symbols + network_symbols
            )
        )
        return tuple(symbol for symbol in target_symbols if _is_offline_sync_symbol(symbol))

    def resolve_pipeline_target_symbols(
        self,
        request: OfflineSyncRequest,
        snapshot: WatermarkSnapshot,
        decision: OfflineSyncDecision | None = None,
    ) -> tuple[str, ...]:
        effective_decision = decision or self.build_decision(request)
        network_symbols = self.resolve_network_target_symbols(request, snapshot, effective_decision)
        analytics_symbols = self.resolve_analytics_target_symbols(request, network_symbols, effective_decision)
        target_symbols = tuple(dict.fromkeys(network_symbols + analytics_symbols))
        return tuple(symbol for symbol in target_symbols if _is_offline_sync_symbol(symbol))

    def execute_integrated_sync(
        self,
        request: OfflineSyncRequest,
        watermark_snapshot: WatermarkSnapshot | None = None,
    ) -> list[IntegratedSyncResult]:
        decision = self.build_decision(request)
        if not decision.allowed:
            return [
                IntegratedSyncResult(
                    symbol=symbol,
                    target_date=decision.formal_kline_date,
                    kline_ready=False,
                    dde_ready=False,
                    factor_ready=False,
                    chip_ready=False,
                    redis_cache_ready=False,
                    wrote_tdengine=(),
                    wrote_redis=(),
                    notes=(decision.notes,),
                )
                for symbol in request.symbols
            ]

        effective_snapshot = watermark_snapshot or self.preload_watermark_snapshot(request)
        target_symbols = self.resolve_effective_target_symbols(request, decision, effective_snapshot)
        network_symbols = self.resolve_network_target_symbols(request, effective_snapshot, decision)
        analytics_symbols = self.resolve_analytics_target_symbols(request, network_symbols, decision)
        target_symbols = self.resolve_pipeline_target_symbols(request, effective_snapshot, decision)
        for line in self._build_settlement_audit_lines(
                request=request,
                decision=decision,
                snapshot=effective_snapshot,
                pipeline_symbols=target_symbols,
                network_symbols=network_symbols,
                analytics_symbols=analytics_symbols,
            ):
            print(line)
        if not target_symbols:
            return [
                IntegratedSyncResult(
                    symbol=symbol,
                    target_date=decision.formal_kline_date,
                    kline_ready=True,
                    dde_ready=bool(
                        effective_snapshot.dde_latest_dates.get(symbol)
                        and effective_snapshot.dde_latest_dates.get(symbol) >= decision.formal_kline_date
                    ),
                    factor_ready=True,
                    chip_ready=True,
                    redis_cache_ready=True,
                    wrote_tdengine=(),
                    wrote_redis=(),
                    notes=("All tracked datasets are already ready for the formal target date.",),
                )
                for symbol in request.symbols
            ]

        print(
            f"[settlement] pipeline symbols={len(target_symbols)} | "
            f"network={len(network_symbols)} | analytics={len(analytics_symbols)} | "
            f"target_date={decision.formal_kline_date}"
        )
        return self._integrated_sync.sync_pipeline(
            target_symbols=target_symbols,
            network_symbols=network_symbols,
            analytics_symbols=analytics_symbols,
            target_date=decision.formal_kline_date,
            watermark_snapshot=effective_snapshot,
            analytics_workers=4,
        )
