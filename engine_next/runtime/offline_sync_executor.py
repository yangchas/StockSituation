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
from engine_next.contracts.offline_sync_contracts import DatasetGapEntry, DatasetGapMatrix, GapFillPlan, build_gap_fill_plan
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
    dataset_gap_matrix: DatasetGapMatrix | None = None


@dataclass(frozen=True)
class OfflineKlineSyncPlan:
    decision: OfflineSyncDecision
    fetch_requests: tuple[BaostockDailyKlineRequest, ...]
    normalized_rows: tuple[dict[str, Any], ...]
    tdengine_write_plan: Optional[PersistenceWritePlan]
    redis_materialization: Optional[RedisViewMaterialization]
    redis_payload: Dict[str, Dict[str, Any]]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class OfflineSyncScope:
    target_symbols: tuple[str, ...]
    network_symbols: tuple[str, ...]
    analytics_symbols: tuple[str, ...]
    factor_cache_gap_count: int = 0
    load_units: int = 0

    @property
    def pipeline_count(self) -> int:
        return len(self.target_symbols)

    @property
    def network_count(self) -> int:
        return len(self.network_symbols)

    @property
    def analytics_count(self) -> int:
        return len(self.analytics_symbols)


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
                dataset_gap_matrix=None,
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
            if not self._watermark_ready(request.kline_watermarks.get(symbol), formal_kline_date)
        )
        missing_factor_watermark_symbols = tuple(
            symbol
            for symbol in request.symbols
            if not self._watermark_ready(request.factor_watermarks.get(symbol), formal_kline_date)
        )
        missing_factor_cache_symbols = tuple(
            symbol
            for symbol in request.symbols
            if not bool(request.redis_factor_cache_ready.get(symbol))
        )
        missing_factor_symbols = tuple(
            dict.fromkeys(missing_factor_watermark_symbols + missing_factor_cache_symbols)
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
        if missing_factor_cache_symbols:
            notes.append(f"Factor cache rebuild required for {len(missing_factor_cache_symbols)} symbols.")
        if not missing_kline_symbols and not missing_factor_symbols:
            notes.append("All tracked symbols are ready for the target formal date.")

        gap_entries: list[DatasetGapEntry] = []
        for symbol in request.symbols:
            kline_ready = self._watermark_ready(request.kline_watermarks.get(symbol), formal_kline_date)
            factor_watermark_ready = self._watermark_ready(request.factor_watermarks.get(symbol), formal_kline_date)
            factor_cache_ready = bool(request.redis_factor_cache_ready.get(symbol))
            gap_entries.extend(
                (
                    DatasetGapEntry(
                        dataset="daily_kline",
                        symbol=symbol,
                        ready=kline_ready,
                        gap_type="ready" if kline_ready else "watermark_stale",
                        reason="" if kline_ready else "daily_kline watermark missing or stale",
                    ),
                    DatasetGapEntry(
                        dataset="daily_factors",
                        symbol=symbol,
                        ready=factor_watermark_ready,
                        gap_type="ready" if factor_watermark_ready else "watermark_stale",
                        reason="" if factor_watermark_ready else "daily_factors watermark missing or stale",
                    ),
                    DatasetGapEntry(
                        dataset="factor_cache",
                        symbol=symbol,
                        ready=factor_cache_ready,
                        gap_type="ready" if factor_cache_ready else "cache_missing",
                        reason="" if factor_cache_ready else "cache:stock_extra runtime view missing",
                    ),
                )
            )
        dataset_gap_matrix = DatasetGapMatrix(
            target_date=formal_kline_date,
            entries=tuple(gap_entries),
        )

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
            dataset_gap_matrix=dataset_gap_matrix,
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

    @staticmethod
    def _watermark_ready(latest_date: str | None, target_date: str) -> bool:
        return bool(latest_date and latest_date >= target_date)

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
        kline_ready = sum(
            1 for symbol in request.symbols if self._watermark_ready(snapshot.kline_latest_dates.get(symbol), formal_date)
        )
        dde_ready = sum(
            1 for symbol in request.symbols if self._watermark_ready(snapshot.dde_latest_dates.get(symbol), formal_date)
        )
        factor_ready = sum(
            1 for symbol in request.symbols if self._watermark_ready(snapshot.factor_latest_dates.get(symbol), formal_date)
        )
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

        kline_missing = symbol_total - kline_ready
        dde_missing = symbol_total - dde_ready
        factor_missing = symbol_total - factor_ready
        factor_cache_gap = max(0, symbol_total - factor_cache_ready)
        chip_cache_gap = max(0, symbol_total - chip_cache_ready)
        dde_cache_gap = max(0, symbol_total - dde_cache_ready)
        matrix_kline_gap = decision.dataset_gap_matrix.pending_count("daily_kline") if decision.dataset_gap_matrix else 0
        matrix_factor_gap = decision.dataset_gap_matrix.pending_count("daily_factors") if decision.dataset_gap_matrix else 0
        matrix_factor_cache_gap = decision.dataset_gap_matrix.pending_count("factor_cache") if decision.dataset_gap_matrix else 0
        source_text = {
            "ready": "当日正式可用",
            "formal_target_locked": "正式日锁定到上一交易日",
            "formal_locked": "正式日锁定到上一交易日",
        }.get(availability_state, availability_state)
        source_check_bits = [
            f"TDengine{'正常' if tdengine_ok else '缺失'}",
            f"Redis{'正常' if redis_ok else '异常'}",
            f"Baostock={baostock_state}",
        ]

        lines = [
            (
                f"[settlement] 启动摘要 | 交易日={request.target_date} | 正式数据日={formal_date} | 数据来源={source_text} | "
                f"待补={len(pipeline_symbols)} | 联网={len(network_symbols)} | 重算={len(analytics_symbols)}"
            ),
            (
                f"[settlement] 缺口判断 | "
                f"日线缺{kline_missing}只 | 因子缺{factor_missing}只 | DDE缺{dde_missing}只 | "
                f"因子缓存缺{factor_cache_gap}只 | 筹码缓存缺{chip_cache_gap}只 | DDE缓存缺{dde_cache_gap}只"
            ),
            (
                f"[settlement] 补数计划 | "
                f"日线待补{matrix_kline_gap}只 | 因子待补{matrix_factor_gap}只 | 因子缓存待回补{matrix_factor_cache_gap}只 | "
                f"{'；'.join(source_check_bits)}"
            ),
        ]

        if decision.notes:
            lines.append(f"[settlement] 说明 | {decision.notes}")
        return tuple(lines)

    def resolve_effective_target_symbols(
        self,
        request: OfflineSyncRequest,
        decision: OfflineSyncDecision | None = None,
        snapshot: WatermarkSnapshot | None = None,
    ) -> tuple[str, ...]:
        effective_decision = decision or self.build_decision(request)
        if snapshot is None:
            matrix_symbols = effective_decision.dataset_gap_matrix.symbols_for(
                "daily_kline",
                "daily_factors",
                "factor_cache",
            ) if effective_decision.dataset_gap_matrix is not None else ()
            target_symbols = tuple(
                dict.fromkeys(
                    matrix_symbols
                    or (
                        effective_decision.missing_kline_symbols
                        + effective_decision.missing_factor_symbols
                    )
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
        matrix_network_symbols = effective_decision.dataset_gap_matrix.symbols_for("daily_kline") if effective_decision.dataset_gap_matrix else ()
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
        if matrix_network_symbols:
            target_symbols = tuple(dict.fromkeys(matrix_network_symbols + target_symbols))
        return tuple(symbol for symbol in target_symbols if _is_offline_sync_symbol(symbol))

    def resolve_analytics_target_symbols(
        self,
        request: OfflineSyncRequest,
        network_symbols: tuple[str, ...],
        decision: OfflineSyncDecision | None = None,
    ) -> tuple[str, ...]:
        effective_decision = decision or self.build_decision(request)
        matrix_analytics_symbols = effective_decision.dataset_gap_matrix.symbols_for("daily_factors", "factor_cache") if effective_decision.dataset_gap_matrix else ()
        target_symbols = tuple(
            dict.fromkeys(
                matrix_analytics_symbols + effective_decision.missing_factor_symbols + network_symbols
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

    def build_sync_scope(
        self,
        request: OfflineSyncRequest,
        snapshot: WatermarkSnapshot,
        decision: OfflineSyncDecision | None = None,
    ) -> OfflineSyncScope:
        effective_decision = decision or self.build_decision(request)
        network_symbols = self.resolve_network_target_symbols(request, snapshot, effective_decision)
        analytics_symbols = self.resolve_analytics_target_symbols(request, network_symbols, effective_decision)
        target_symbols = tuple(dict.fromkeys(network_symbols + analytics_symbols))
        factor_cache_gap_count = (
            effective_decision.dataset_gap_matrix.pending_count("factor_cache")
            if effective_decision.dataset_gap_matrix is not None
            else 0
        )
        load_units = len(network_symbols) * 4 + len(analytics_symbols)
        return OfflineSyncScope(
            target_symbols=target_symbols,
            network_symbols=network_symbols,
            analytics_symbols=analytics_symbols,
            factor_cache_gap_count=factor_cache_gap_count,
            load_units=load_units,
        )

    def execute_integrated_sync(
        self,
        request: OfflineSyncRequest,
        watermark_snapshot: WatermarkSnapshot | None = None,
        sync_scope: OfflineSyncScope | None = None,
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
        scope = sync_scope or self.build_sync_scope(request, effective_snapshot, decision)
        target_symbols = scope.target_symbols
        network_symbols = scope.network_symbols
        analytics_symbols = scope.analytics_symbols
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
