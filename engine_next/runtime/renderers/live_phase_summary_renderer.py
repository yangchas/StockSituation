from __future__ import annotations

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import IntradayContext
from engine_next.runtime.intraday_context_builder import PrimedIntradayRuntimeState


class LivePhaseSummaryRenderer:
    """Builds operator-facing runtime summaries per phase."""

    def render(
        self,
        *,
        phase: RunPhase,
        intraday_context: IntradayContext | None,
        primed_runtime_state: PrimedIntradayRuntimeState | None,
        runtime_readiness_label: str,
        symbol_count: int,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return (
                f"runtime_readiness={runtime_readiness_label}",
                f"runtime_symbols={symbol_count}",
                "runtime_context=warming",
            )

        market_summary = intraday_context.market_summary
        quote_count = len(primed_runtime_state.quote_rows) if primed_runtime_state is not None else 0
        rust_count = int(primed_runtime_state.rust_ingested) if primed_runtime_state is not None else 0
        coverage_ratio = (quote_count / symbol_count) if symbol_count else 0.0

        header = (
            f"runtime_readiness={runtime_readiness_label} "
            f"| quotes={quote_count}/{symbol_count} "
            f"| rust={rust_count}"
        )
        coverage = f"runtime_coverage={coverage_ratio:.1%} | snapshots={len(intraday_context.stock_snapshots)}"

        if phase == RunPhase.AUCTION:
            return (
                header,
                (
                    f"auction_focus | top_plate={market_summary.top_plate_name or '-'} "
                    f"| hot={market_summary.hot_plate_count} "
                    f"| avg_bid={market_summary.avg_bid_amt / 1e8:.2f}y "
                    f"| auc_amt={market_summary.context_auc_amt / 1e8:.2f}y"
                ),
                (
                    f"auction_view | sentiment={market_summary.sentiment_score:.1f} "
                    f"| red_open={market_summary.red_open_rate:.1%} "
                    f"| promotion={market_summary.promotion_rate:.1%} "
                    f"| battle={market_summary.battle_status or '-'}"
                ),
                coverage,
            )

        if phase == RunPhase.POSTMARKET:
            return (
                header,
                (
                    f"settlement_wait | top_plate={market_summary.top_plate_name or '-'} "
                    f"| hot={market_summary.hot_plate_count} "
                    f"| battle={market_summary.battle_status or '-'}"
                ),
                (
                    f"market_close_state | sentiment={market_summary.sentiment_score:.1f} "
                    f"| promotion={market_summary.promotion_rate:.1%} "
                    f"| headshot={market_summary.headshot_rate:.1%}"
                ),
                coverage,
            )

        return (
            header,
            (
                f"market_pulse | top_plate={market_summary.top_plate_name or '-'} "
                f"| strength={market_summary.top_plate_strength:.2f} "
                f"| hot={market_summary.hot_plate_count} "
                f"| battle={market_summary.battle_status or '-'}"
            ),
            (
                f"shortline_sentiment | score={market_summary.sentiment_score:.1f} "
                f"| promotion={market_summary.promotion_rate:.1%} "
                f"| red_open={market_summary.red_open_rate:.1%} "
                f"| headshot={market_summary.headshot_rate:.1%}"
            ),
            coverage,
        )
