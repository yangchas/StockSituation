from __future__ import annotations

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import IntradayContext
from engine_next.runtime.intraday_context_builder import PrimedIntradayRuntimeState


def _native_ingested_count(primed_runtime_state: PrimedIntradayRuntimeState | None) -> int:
    if primed_runtime_state is None:
        return 0
    value = getattr(primed_runtime_state, "native_ingested", None)
    if value is None:
        value = getattr(primed_runtime_state, "rust_ingested", 0)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def render_quote_freshness_line(
    primed_runtime_state: PrimedIntradayRuntimeState | None,
    *,
    symbol_count: int,
) -> str | None:
    if primed_runtime_state is None:
        return None

    latest = primed_runtime_state.latest_quote_time or "-"
    latest_age = primed_runtime_state.latest_quote_age_seconds
    lag = "-" if latest_age is None else f"{latest_age}s"
    stale_or_cached = primed_runtime_state.quote_stale_count
    return (
        "quote_freshness "
        f"| fresh={primed_runtime_state.quote_fresh_count}/{symbol_count} "
        f"| stale={stale_or_cached} "
        f"| cache_only={stale_or_cached} "
        f"| missing={primed_runtime_state.quote_missing_count} "
        f"| latest={latest} "
        f"| lag={lag} "
        f"| tol={primed_runtime_state.quote_stale_threshold_seconds}s"
    )


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
        native_count = _native_ingested_count(primed_runtime_state)
        coverage_ratio = (quote_count / symbol_count) if symbol_count else 0.0
        fresh_line = render_quote_freshness_line(
            primed_runtime_state,
            symbol_count=symbol_count,
        )

        header = (
            f"runtime_readiness={runtime_readiness_label} "
            f"| quotes={quote_count}/{symbol_count} "
            f"| native={native_count}"
        )
        quote_fresh_ratio = primed_runtime_state.quote_fresh_ratio if primed_runtime_state is not None else 0.0
        coverage = (
            f"runtime_coverage={coverage_ratio:.1%} "
            f"| quote_fresh={quote_fresh_ratio:.1%} "
            f"| snapshots={len(intraday_context.stock_snapshots)}"
        )

        if phase == RunPhase.AUCTION:
            lines = [
                header,
                (
                    f"auction_focus | top_plate={market_summary.top_plate_name or '-'} "
                    f"| hot={market_summary.hot_plate_count} "
                    f"| avg_yest_auc={market_summary.avg_bid_amt / 1e8:.2f}y "
                    f"| auc_amt={market_summary.context_auc_amt / 1e8:.2f}y"
                ),
                (
                    f"auction_view | sentiment={market_summary.sentiment_score:.1f} "
                    f"| red_open={market_summary.red_open_rate:.1%} "
                    f"| promotion={market_summary.promotion_rate:.1%} "
                    f"| battle={market_summary.battle_status or '-'}"
                ),
            ]
            if fresh_line:
                lines.append(fresh_line)
            lines.append(coverage)
            return tuple(lines)

        if phase == RunPhase.POSTMARKET:
            lines = [
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
            ]
            if fresh_line:
                lines.append(fresh_line)
            lines.append(coverage)
            return tuple(lines)

        lines = [
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
        ]
        if fresh_line:
            lines.append(fresh_line)
        lines.append(coverage)
        return tuple(lines)
