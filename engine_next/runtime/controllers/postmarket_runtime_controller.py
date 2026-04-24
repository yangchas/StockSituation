from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from engine_next.domain.enums import RunPhase
from engine_next.runtime.intraday_data_hub import IntradayDataHub, IntradayFetchResult
from engine_next.runtime.market_runtime_summary import MarketRuntimeSummaryResult, MarketRuntimeSummaryService


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostmarketEventResult:
    executed: bool
    notes: tuple[str, ...] = ()
    yest_limit_result: IntradayFetchResult | None = None
    hot_plate_result: IntradayFetchResult | None = None
    market_runtime_summary_result: MarketRuntimeSummaryResult | None = None


class PostmarketRuntimeController:
    """Owns postmarket close-marker and settlement-window event behavior."""

    def __init__(
        self,
        *,
        intraday_hub: IntradayDataHub | None = None,
        market_runtime_summary_service: MarketRuntimeSummaryService | None = None,
        state_writer: Callable[[str, str], bool] | None = None,
    ) -> None:
        self._intraday_hub = intraday_hub or IntradayDataHub()
        self._market_runtime_summary_service = market_runtime_summary_service or MarketRuntimeSummaryService(
            redis_client=self._intraday_hub.redis
        )
        self._state_writer = state_writer

    def execute_close_marker(self, *, trade_date: str) -> PostmarketEventResult:
        logger.info("scheduled event execute | name=market_close_1505")
        phase_written = self._state_writer("market:state:last_phase", "postmarket") if self._state_writer else False
        date_written = self._state_writer("market:state:last_close_trade_date", trade_date) if self._state_writer else False
        return PostmarketEventResult(
            executed=True,
            notes=(
                f"15:05 close marker persisted | phase_key={'ok' if phase_written else 'skip'} | date_key={'ok' if date_written else 'skip'}",
            ),
        )

    def execute_settlement_window(
        self,
        *,
        trade_date: str,
        previous_trade_date: str,
        offline_context_date: str,
    ) -> PostmarketEventResult:
        logger.info("scheduled event execute | name=postmarket_settlement_1740")
        hot_plate_result = self._intraday_hub.fetch_hot_plates(trade_date, RunPhase.POSTMARKET, today_mode=True)
        yest_limit_result = self._intraday_hub.fetch_yest_limit_pool(previous_trade_date, RunPhase.POSTMARKET)
        market_runtime_summary_result = self._market_runtime_summary_service.build_and_write(
            trade_date,
            offline_context_date=offline_context_date,
        )
        return PostmarketEventResult(
            executed=True,
            notes=("17:40 settlement event refreshed postmarket hot plates, yesterday limit pool, and market runtime summary.",),
            yest_limit_result=yest_limit_result,
            hot_plate_result=hot_plate_result,
            market_runtime_summary_result=market_runtime_summary_result,
        )
