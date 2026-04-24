from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from engine_next.contracts.offline_sync_contracts import WatermarkSnapshot
from engine_next.domain.enums import ExecutionEnvironment
from engine_next.runtime.offline_sync_executor import OfflineSyncRequest, ServerOnlyOfflineSyncExecutor
from engine_next.runtime.startup_runtime_coordinator import (
    RuntimeStartupCoordinator,
    StartupCoordinatorRequest,
    StartupExecutionBundle,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StartupBootstrapRequest:
    now: datetime
    trade_date: str
    previous_trade_date: str
    symbols: tuple[str, ...]
    offline_context_date: str
    environment: ExecutionEnvironment
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


@dataclass(frozen=True)
class StartupBootstrapResult:
    startup_bundle: StartupExecutionBundle
    watermark_snapshot: WatermarkSnapshot
    runtime_readiness: dict[str, object]
    lifecycle_audit_ran: bool
    used_cached_startup_state: bool


class StartupBootstrapController:
    """
    Owns startup/bootstrap audit state and caching.

    This isolates startup readiness, watermark preload, and startup repair
    from the phase-neutral app loop so later auction/intraday controllers
    can stop inheriting startup semantics as their default identity.
    """

    def __init__(
        self,
        *,
        startup_coordinator: RuntimeStartupCoordinator | None = None,
        offline_executor: ServerOnlyOfflineSyncExecutor | None = None,
        runtime_readiness_loader: Callable[..., dict[str, object]] | None = None,
    ) -> None:
        self._startup_coordinator = startup_coordinator or RuntimeStartupCoordinator()
        self._offline_executor = offline_executor or ServerOnlyOfflineSyncExecutor()
        self._runtime_readiness_loader = runtime_readiness_loader
        self._last_audit_token: str | None = None
        self._last_audit_trade_date: str | None = None
        self._cached_startup_bundle: StartupExecutionBundle | None = None
        self._cached_watermark_snapshot: WatermarkSnapshot | None = None
        self._cached_runtime_readiness: dict[str, object] | None = None

    @property
    def last_audit_token(self) -> str | None:
        return self._last_audit_token

    @property
    def last_audit_trade_date(self) -> str | None:
        return self._last_audit_trade_date

    def refresh_cached_state(
        self,
        *,
        trade_date: str,
        startup_bundle: StartupExecutionBundle,
        watermark_snapshot: WatermarkSnapshot,
        runtime_readiness: dict[str, object],
    ) -> None:
        self._last_audit_trade_date = trade_date
        self._cached_startup_bundle = startup_bundle
        self._cached_watermark_snapshot = watermark_snapshot
        self._cached_runtime_readiness = runtime_readiness

    def execute(
        self,
        request: StartupBootstrapRequest,
        *,
        should_run_lifecycle_audit: bool,
        audit_token: str | None,
    ) -> StartupBootstrapResult:
        effective_audit_token = audit_token or f"startup:{request.trade_date}:{request.now.strftime('%H:%M')}"
        if should_run_lifecycle_audit:
            logger.debug(
                "lifecycle audit decision | run=%s | token=%s | cached=%s",
                True,
                effective_audit_token,
                self._cached_startup_bundle is not None,
            )
            if self._runtime_readiness_loader is None:
                raise RuntimeError("StartupBootstrapController requires runtime_readiness_loader for audit path.")
            logger.debug(
                "runtime readiness audit start | symbols=%s | offline_context_date=%s",
                len(request.symbols),
                request.offline_context_date,
            )
            runtime_readiness = self._runtime_readiness_loader(
                now=request.now,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                offline_context_date=request.offline_context_date,
                symbols=request.symbols,
            )
            preload_request = OfflineSyncRequest(
                now=request.now,
                target_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                symbols=request.symbols,
                kline_watermarks=request.kline_watermarks or {},
                factor_watermarks=request.factor_watermarks or {},
                redis_factor_cache_ready=request.redis_factor_cache_ready or runtime_readiness["redis_factor_cache_ready"],
                environment=request.environment,
            )
            logger.debug("watermark preload start | target_date=%s", request.trade_date)
            watermark_snapshot = request.watermark_snapshot or self._offline_executor.preload_watermark_snapshot(preload_request)
            logger.debug(
                "watermark preload done | kline=%s | dde=%s | factor=%s",
                len(watermark_snapshot.kline_latest_dates),
                len(watermark_snapshot.dde_latest_dates),
                len(watermark_snapshot.factor_latest_dates),
            )
            startup_request = StartupCoordinatorRequest(
                now=request.now,
                trade_date=request.trade_date,
                previous_trade_date=request.previous_trade_date,
                symbols=request.symbols,
                kline_watermarks=request.kline_watermarks or watermark_snapshot.kline_latest_dates,
                factor_watermarks=request.factor_watermarks or watermark_snapshot.factor_latest_dates,
                redis_factor_cache_ready=request.redis_factor_cache_ready or runtime_readiness["redis_factor_cache_ready"],
                yest_limit_pool_ready=request.yest_limit_pool_ready or bool(runtime_readiness["yest_limit_pool_ready"]),
                hot_plates_ready=request.hot_plates_ready or bool(runtime_readiness["hot_plates_ready"]),
                stock_plate_mapping_ready=request.stock_plate_mapping_ready or bool(runtime_readiness["stock_plate_mapping_ready"]),
                auction_anchor_ready=request.auction_anchor_ready or bool(runtime_readiness["auction_anchor_ready"]),
                redis_chip_ready_count=max(request.redis_chip_ready_count, int(runtime_readiness["redis_chip_ready_count"])),
                redis_dde_ready_count=max(request.redis_dde_ready_count, int(runtime_readiness["redis_dde_ready_count"])),
                watermark_snapshot=watermark_snapshot,
                environment=request.environment,
            )
            logger.debug("startup repair phase start")
            startup_bundle = self._startup_coordinator.execute_allowed_repairs(startup_request)
            logger.debug(
                "startup repair phase done | phase=%s | readiness=%s",
                startup_bundle.plan.phase.value,
                startup_bundle.plan.report.readiness.value,
            )
            self._last_audit_token = effective_audit_token
            self._last_audit_trade_date = request.trade_date
            self._cached_startup_bundle = startup_bundle
            self._cached_watermark_snapshot = watermark_snapshot
            self._cached_runtime_readiness = runtime_readiness
            return StartupBootstrapResult(
                startup_bundle=startup_bundle,
                watermark_snapshot=watermark_snapshot,
                runtime_readiness=runtime_readiness,
                lifecycle_audit_ran=True,
                used_cached_startup_state=False,
            )

        startup_bundle = self._cached_startup_bundle
        watermark_snapshot = request.watermark_snapshot or self._cached_watermark_snapshot
        runtime_readiness = self._cached_runtime_readiness
        if startup_bundle is None or watermark_snapshot is None or runtime_readiness is None:
            logger.debug("lifecycle audit cache missing; forcing startup audit path")
            self._last_audit_token = None
            self._cached_startup_bundle = None
            self._cached_watermark_snapshot = None
            self._cached_runtime_readiness = None
            return self.execute(
                request,
                should_run_lifecycle_audit=True,
                audit_token=effective_audit_token,
            )
        return StartupBootstrapResult(
            startup_bundle=startup_bundle,
            watermark_snapshot=watermark_snapshot,
            runtime_readiness=runtime_readiness,
            lifecycle_audit_ran=False,
            used_cached_startup_state=True,
        )
