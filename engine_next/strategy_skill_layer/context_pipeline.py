from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from engine_next.domain.models import AuctionLadderDecision, IntradayContext, StockProfileAssessment
from engine_next.strategy_skill_layer.auction_and_ladder import build_auction_and_ladder_decision
from engine_next.strategy_skill_layer.stock_profile import assess_stock_profile


@dataclass(frozen=True)
class ContextStrategyBundle:
    context: IntradayContext
    profiles: tuple[StockProfileAssessment, ...]
    decisions: tuple[AuctionLadderDecision, ...]
    focus_symbols: tuple[str, ...]
    notes: tuple[str, ...] = ()


def build_context_strategy_bundle(context: IntradayContext) -> ContextStrategyBundle:
    return build_context_strategy_bundle_for_symbols(context, symbols=None)


def build_context_strategy_bundle_for_symbols(
    context: IntradayContext,
    *,
    symbols: Iterable[str] | None,
) -> ContextStrategyBundle:
    symbol_filter = {str(symbol) for symbol in symbols or () if str(symbol)}
    selected_snapshots = tuple(
        snapshot for snapshot in context.stock_snapshots if not symbol_filter or snapshot.symbol in symbol_filter
    )
    profiles = tuple(assess_stock_profile(snapshot) for snapshot in selected_snapshots)
    decisions = tuple(build_auction_and_ladder_decision(snapshot) for snapshot in selected_snapshots)

    ranked = sorted(
        decisions,
        key=lambda decision: (
            decision.action == "dragon_early_board",
            decision.action == "early_boarding_candidate",
            decision.action == "hold_only",
            decision.confidence,
            decision.profile.continuation_score,
            decision.risk_reward_ratio,
        ),
        reverse=True,
    )
    focus_symbols = tuple(decision.symbol for decision in ranked[:10])
    notes = (
        f"mainline_sector={context.market_summary.mainline_sector or 'N/A'}",
        f"top_turnover_count={len(context.market_summary.top_turnover_symbols)}",
        f"decision_count={len(decisions)}",
        f"selected_snapshot_count={len(selected_snapshots)}",
    )
    return ContextStrategyBundle(
        context=context,
        profiles=profiles,
        decisions=ranked,
        focus_symbols=focus_symbols,
        notes=notes,
    )


def filter_trade_candidates(
    bundle: ContextStrategyBundle,
    *,
    min_confidence: int = 60,
) -> tuple[AuctionLadderDecision, ...]:
    candidates = []
    for decision in bundle.decisions:
        if decision.confidence < min_confidence:
            continue
        if decision.action in ("observe_only", "avoid_after_failed_promotion", "do_not_chase"):
            continue
        candidates.append(decision)
    return tuple(candidates)
