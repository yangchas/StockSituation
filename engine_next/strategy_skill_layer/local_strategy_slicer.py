from __future__ import annotations

from engine_next.domain.local_strategy_models import (
    LocalSignal,
    LocalStrategyEvidencePack,
    LocalStrategyGraph,
    LocalStrategyScopeSummary,
)


def _dedupe_scope_summaries(
    graph: LocalStrategyGraph,
    signals: tuple[LocalSignal, ...],
    *,
    limit: int,
) -> tuple[LocalStrategyScopeSummary, ...]:
    seen: set[tuple[str, str]] = set()
    rows: list[LocalStrategyScopeSummary] = []
    for signal in signals:
        key = (signal.scope_type, signal.scope)
        if key in seen:
            continue
        seen.add(key)
        rows.append(graph.scope_summary(signal.scope_type, signal.scope))
        if len(rows) >= limit:
            break
    return tuple(rows)


def build_local_strategy_evidence_pack(
    graph: LocalStrategyGraph,
    *,
    theme_limit: int = 5,
    stock_limit: int = 8,
    risk_limit: int = 8,
) -> LocalStrategyEvidencePack:
    """Slice local strategy graph into reusable evidence groups for the global layer."""

    theme_opportunity_signals = graph.top_signals(
        scope_type="theme",
        node_id="theme_relative",
        action_hints=("probe", "support"),
        limit=theme_limit * 3,
    )
    theme_risk_signals = graph.top_signals(
        scope_type="theme",
        action_hints=("avoid", "avoid_chase"),
        limit=risk_limit * 3,
    )
    stock_alignment_signals = graph.top_signals(
        scope_type="stock",
        node_id="theme_stock_bridge",
        states=("theme_stock_aligned", "theme_stock_pressure_repair"),
        limit=stock_limit * 2,
    )
    stock_risk_signals = graph.top_signals(
        scope_type="stock",
        node_id="theme_stock_bridge",
        action_hints=("avoid", "avoid_chase"),
        limit=risk_limit * 2,
    )
    high_pressure_signals = graph.top_signals(
        scope_type="theme",
        node_id="theme_high_focus_impact",
        states=("theme_high_group_pressure", "theme_high_individual_fail", "theme_high_promotion"),
        limit=theme_limit * 2,
    )
    emotion_signals = graph.top_signals(
        scope_type="emotion_bucket",
        action_hints=("support", "avoid", "avoid_chase"),
        limit=6,
    )
    theme_opportunities = _dedupe_scope_summaries(graph, theme_opportunity_signals, limit=theme_limit)
    theme_risks = _dedupe_scope_summaries(graph, theme_risk_signals, limit=risk_limit)
    high_pressure_alerts = _dedupe_scope_summaries(graph, high_pressure_signals, limit=theme_limit)
    stock_alignments = _dedupe_scope_summaries(graph, stock_alignment_signals, limit=stock_limit)
    stock_risks = _dedupe_scope_summaries(graph, stock_risk_signals, limit=risk_limit)
    emotion_alerts = _dedupe_scope_summaries(graph, emotion_signals, limit=6)
    return LocalStrategyEvidencePack(
        theme_opportunities=theme_opportunities,
        theme_risks=theme_risks,
        high_pressure_alerts=high_pressure_alerts,
        stock_alignments=stock_alignments,
        stock_risks=stock_risks,
        emotion_alerts=emotion_alerts,
        notes=(
            f"theme_opportunities={len(theme_opportunities)}",
            f"theme_risks={len(theme_risks)}",
            f"high_pressure={len(high_pressure_alerts)}",
            f"stock_alignments={len(stock_alignments)}",
            f"stock_risks={len(stock_risks)}",
            f"emotion_alerts={len(emotion_alerts)}",
            f"dependency_issues={len(graph.dependency_issues)}",
        ),
    )
