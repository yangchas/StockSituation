from __future__ import annotations

from engine_next.domain.models import StockSelectionContext, StockStateSnapshot, ThemeSelectionContext


def classify_conclusion_permission(conclusion: str) -> str:
    text = str(conclusion or "").strip()
    if text in {"switch_expansion_confirmed", "old_mainline_strong_continue"}:
        return "promote"
    if text in {"switch_partially_confirmed", "old_mainline_weak_continue"}:
        return "limited"
    if text in {"leader_only_alive"}:
        return "leader_only"
    return "deny"


def stock_passes_conclusion_gate(
    selection: StockSelectionContext | None,
    snapshot: StockStateSnapshot | None,
    theme_context: ThemeSelectionContext | None,
) -> bool:
    if selection is None or theme_context is None:
        return True

    permission = classify_conclusion_permission(theme_context.trade_conclusion)
    if permission == "deny":
        return False
    if permission == "leader_only":
        if selection.is_true_leader:
            return True
        return bool(
            selection.is_front_row
            and selection.theme_tradable
            and selection.theme_core_score >= 7.2
            and selection.shape_quality_score >= 6.4
            and selection.execution_quality_score >= 6.2
            and selection.open_undertake_score >= 5.8
            and (selection.hot_rank <= 80 or selection.total_score >= 8.0)
        )
    if permission == "limited":
        if selection.is_true_leader:
            return True
        if selection.leader_bucket not in {"front_row", "leader", "front_core"} and not selection.is_front_row:
            return False
        if selection.open_undertake_score < 5.4 and selection.execution_quality_score < 5.8:
            return False
        return True
    if permission == "promote":
        if selection.is_true_leader or selection.is_front_row:
            return True
        if snapshot is not None and snapshot.leader_rank_in_theme <= 3:
            return True
        if selection.open_undertake_score >= 6.0 and selection.execution_quality_score >= 6.0:
            return True
        return False
    return True
