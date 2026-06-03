from __future__ import annotations

from engine_next.domain.models import AuctionLadderDecision, StockSelectionContext, StockStateSnapshot


def classify_playbook_theme_tier(
    selection: StockSelectionContext | None,
    snapshot: StockStateSnapshot | None,
) -> str:
    if selection is None:
        if snapshot is not None and snapshot.leader_rank_in_theme <= 1:
            return "dragon"
        if snapshot is not None and (snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1):
            return "front_core"
        if snapshot is not None and snapshot.leader_rank_in_theme <= 6:
            return "front_follow"
        return "back_noise"
    if selection.is_true_leader:
        return "dragon"
    if selection.is_front_row and selection.open_follow_state in {"confirmed", "repair_strength"}:
        return "front_core"
    if selection.is_front_row or selection.leader_bucket == "front_row":
        return "front_follow"
    return "back_noise"


def build_playbook_rank_key(
    decision: AuctionLadderDecision,
    *,
    selection: StockSelectionContext | None,
    display_code: str,
    opening_confirmed: bool,
) -> tuple[int, int, int, int, int, float, int]:
    """Rank playbook rows with discrete evidence, not mega-scores."""
    action_rank_map = {
        "dragon_board": 0,
        "theme_first_board": 1,
        "leader_hold": 2,
        "watch_only": 3,
        "failed_promo_guard": 8,
        "do_not_chase": 9,
    }
    action_rank = action_rank_map.get(display_code, 5)
    leader_rank = 0 if (selection is not None and selection.is_true_leader) else 1
    front_rank = 0 if (selection is not None and selection.is_front_row) else 1
    follow_rank = (
        0
        if (
            selection is not None
            and selection.open_follow_state in {"confirmed", "repair_strength"}
        )
        else 1
    )
    risk_rank = (
        1
        if (
            selection is not None
            and selection.open_follow_state in {"faded", "weak_follow"}
        )
        else 0
    )
    return (
        action_rank,
        -int(opening_confirmed),
        leader_rank,
        front_rank,
        follow_rank,
        -float(decision.confidence),
        risk_rank,
    )
