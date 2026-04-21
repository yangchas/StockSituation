from __future__ import annotations

import logging
from dataclasses import dataclass

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import IntradayContext
from engine_next.runtime.intraday_data_hub import IntradayDataHub, IntradayFetchResult
from engine_next.runtime.market_runtime_summary import MarketRuntimeSummaryResult, MarketRuntimeSummaryService


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuctionReplayResult:
    executed: bool
    notes: tuple[str, ...] = ()
    yest_limit_result: IntradayFetchResult | None = None
    auction_result: IntradayFetchResult | None = None
    market_runtime_summary_result: MarketRuntimeSummaryResult | None = None


class AuctionRuntimeController:
    """Owns auction-specific replay and summary behavior."""

    def __init__(
        self,
        *,
        intraday_hub: IntradayDataHub | None = None,
        market_runtime_summary_service: MarketRuntimeSummaryService | None = None,
    ) -> None:
        self._intraday_hub = intraday_hub or IntradayDataHub()
        self._market_runtime_summary_service = market_runtime_summary_service or MarketRuntimeSummaryService(
            redis_client=self._intraday_hub.redis
        )

    def execute_replay_0926(
        self,
        *,
        trade_date: str,
        previous_trade_date: str,
        offline_context_date: str,
    ) -> AuctionReplayResult:
        logger.info("scheduled event execute | name=auction_replay_0926")
        yest_limit_result = self._intraday_hub.fetch_yest_limit_pool(previous_trade_date, RunPhase.AUCTION)
        auction_result = self._intraday_hub.recover_auction_anchor(trade_date, RunPhase.AUCTION)
        market_runtime_summary_result = self._market_runtime_summary_service.build_and_write(
            trade_date,
            offline_context_date=offline_context_date,
        )
        return AuctionReplayResult(
            executed=True,
            notes=("09:26 event refreshed yesterday limit pool, auction anchor, and market runtime summary.",),
            yest_limit_result=yest_limit_result,
            auction_result=auction_result,
            market_runtime_summary_result=market_runtime_summary_result,
        )

    def render_auction_view(self, intraday_context: IntradayContext | None) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        summary = intraday_context.market_summary
        leaders = ",".join(summary.top_turnover_symbols[:3]) or "-"
        return (
            (
                f"auction_leaders | top_turnover={leaders} "
                f"| mainline={summary.mainline_sector or summary.top_plate_name or '-'} "
                f"| switch={'yes' if summary.mainline_switch else 'no'}"
            ),
            (
                f"auction_risk | battle={summary.battle_status or '-'} "
                f"| resonance={summary.resonance_score:.2f} "
                f"| red_green={summary.red_green_ratio:.2f}"
            ),
        )
