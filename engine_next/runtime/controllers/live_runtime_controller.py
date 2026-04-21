from __future__ import annotations

import logging
from dataclasses import dataclass

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import IntradayContext
from engine_next.runtime.intraday_context_builder import (
    IntradayContextBuilder,
    IntradayContextRequest,
    PrimedIntradayRuntimeState,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveRuntimeRequest:
    phase: RunPhase
    trade_date: str
    previous_trade_date: str
    offline_context_date: str
    symbols: tuple[str, ...]
    minute_index: int | None = None
    require_auction_recovery: bool = False


@dataclass(frozen=True)
class LiveRuntimeResult:
    intraday_context: IntradayContext | None
    primed_runtime_state: PrimedIntradayRuntimeState | None
    rebuilt_context: bool


class LiveRuntimeController:
    """
    Owns runtime priming and intraday-context cache reuse.

    This is the first extraction step away from app_main so startup/bootstrap
    and live runtime stop sharing one giant orchestration branch.
    """

    ACTIVE_PHASES = (
        RunPhase.PREMARKET,
        RunPhase.AUCTION,
        RunPhase.INTRADAY,
        RunPhase.POSTMARKET,
    )

    def __init__(
        self,
        *,
        intraday_context_builder: IntradayContextBuilder | None = None,
    ) -> None:
        self._intraday_context_builder = intraday_context_builder or IntradayContextBuilder()
        self._cached_intraday_context: IntradayContext | None = None

    def execute(
        self,
        request: LiveRuntimeRequest,
        *,
        lifecycle_audit_ran: bool,
        scheduled_event_executed: bool,
        should_render_cycle: bool,
    ) -> LiveRuntimeResult:
        intraday_context = self._cached_intraday_context
        primed_runtime_state: PrimedIntradayRuntimeState | None = None

        if request.phase not in self.ACTIVE_PHASES:
            return LiveRuntimeResult(
                intraday_context=intraday_context,
                primed_runtime_state=None,
                rebuilt_context=False,
            )

        context_request = IntradayContextRequest(
            phase=request.phase,
            trade_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            offline_context_date=request.offline_context_date,
            symbols=request.symbols,
            require_auction_recovery=request.require_auction_recovery,
            minute_index=request.minute_index,
        )
        primed_runtime_state = self._intraday_context_builder.prime_runtime_state(context_request)
        should_rebuild_context = (
            lifecycle_audit_ran
            or scheduled_event_executed
            or self._cached_intraday_context is None
            or should_render_cycle
        )

        if lifecycle_audit_ran or scheduled_event_executed:
            logger.info(
                "runtime prime | phase=%s | symbols=%s | quotes=%s | rust=%s",
                request.phase.value,
                len(request.symbols),
                len(primed_runtime_state.quote_rows),
                primed_runtime_state.rust_ingested,
            )

        if should_rebuild_context:
            if lifecycle_audit_ran or scheduled_event_executed:
                logger.info(
                    "runtime context build start | phase=%s | symbols=%s",
                    request.phase.value,
                    len(request.symbols),
                )
            intraday_context = self._intraday_context_builder.build_from_primed(primed_runtime_state)
            self._cached_intraday_context = intraday_context
            if lifecycle_audit_ran or scheduled_event_executed:
                logger.info(
                    "runtime context build done | snapshots=%s",
                    len(intraday_context.stock_snapshots) if intraday_context is not None else 0,
                )

        return LiveRuntimeResult(
            intraday_context=intraday_context,
            primed_runtime_state=primed_runtime_state,
            rebuilt_context=should_rebuild_context,
        )
