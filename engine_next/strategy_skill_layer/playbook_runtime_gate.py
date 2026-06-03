from __future__ import annotations

from dataclasses import dataclass

from engine_next.domain.models import StockSelectionContext, StockStateSnapshot


@dataclass(frozen=True)
class PlaybookGateContext:
    preferred_plates: tuple[str, ...]
    execution_themes: tuple[str, ...]
    mode_name: str


def is_playbook_stock_auction_fakeout(
    snapshot: StockStateSnapshot | None,
    selection: StockSelectionContext | None,
    *,
    phase_label: str,
    front_weak: bool,
    front_strong: bool,
) -> bool:
    if snapshot is None:
        return False
    if phase_label not in {"auction", "opening", "open_confirm"}:
        return False
    weak_two_minute_follow = (
        snapshot.auction_amount > 0
        and snapshot.amount_2m > 0
        and snapshot.amount_2m < snapshot.auction_amount * 0.75
        and snapshot.speed_1m <= 0.006
    )
    if snapshot.open_pct >= 0.07 and weak_two_minute_follow:
        return True
    if front_weak and snapshot.open_pct >= 0.05 and weak_two_minute_follow:
        return True
    if front_strong and snapshot.open_pct >= 0.06 and weak_two_minute_follow:
        return True
    if (
        selection is not None
        and snapshot.auction_amount >= 40_000_000
        and snapshot.leader_rank_in_theme > 3
        and not selection.is_true_leader
        and not selection.is_front_row
        and selection.open_follow_state in {"weak_follow", "faded"}
    ):
        return True
    if selection is not None and selection.kline_pattern in {"high_open_then_weak", "explosive_failed_board"}:
        return True
    return False


def has_playbook_non_hot_strength(
    selection: StockSelectionContext,
    snapshot: StockStateSnapshot | None,
    *,
    low_open_rebound: bool,
) -> bool:
    if snapshot is None:
        return False
    if low_open_rebound:
        return True
    front_row_non_hot_start = (
        selection.hot_rank > 80
        and (selection.is_front_row or snapshot.leader_rank_in_theme <= 3)
        and snapshot.auction_amount >= 15_000_000
        and snapshot.amount_2m >= 28_000_000
        and selection.open_follow_state in {"confirmed", "repair_strength"}
    )
    if front_row_non_hot_start:
        return True
    if (
        snapshot.leader_rank_in_theme <= 3
        and snapshot.amount_2m >= 35_000_000
        and selection.open_follow_state in {"confirmed", "repair_strength"}
    ):
        return True
    if (
        snapshot.auction_amount > 0
        and snapshot.amount_2m >= snapshot.auction_amount * 1.3
        and selection.open_follow_state != "faded"
    ):
        return True
    if (
        snapshot.speed_1m >= 0.01
        and selection.kline_pattern in {"low_open_strength", "pullback_repair", "breakout", "platform_breakout"}
        and selection.open_follow_state in {"confirmed", "repair_strength"}
    ):
        return True
    if (
        selection.hot_rank > 80
        and selection.is_front_row
        and selection.open_follow_state in {"confirmed", "repair_strength"}
        and snapshot.open_pct <= 0.04
    ):
        return True
    return False


def is_playbook_hard_blocked(
    *,
    phase_label: str,
    decision_action: str,
    open_follow_state: str,
    high_dayk_trap: bool,
    auction_fakeout: bool,
) -> bool:
    if high_dayk_trap or auction_fakeout:
        return True
    if phase_label in {"opening", "open_confirm"} and open_follow_state == "weak_follow":
        return True
    if decision_action in {"dragon_early_board", "early_boarding_candidate"} and open_follow_state == "faded":
        return True
    return False


def is_playbook_behavior_blocked(
    *,
    decision_action: str,
    is_true_leader: bool,
    is_active_pool: bool,
    strong_non_hot_signal: bool,
    open_follow_state: str,
    auction_open_bucket: str,
    theme_x_score: float,
    hot_rank: int,
    repair_probe_exception: bool,
    priority_front_row_exception: bool,
    lb_days: int,
    leader_rank_in_theme: int,
    auction_amount: float,
    amount_2m: float,
) -> bool:
    if (
        not is_true_leader
        and not is_active_pool
        and not strong_non_hot_signal
        and open_follow_state not in {"confirmed", "repair_strength"}
        and not repair_probe_exception
        and not priority_front_row_exception
    ):
        return True
    if (
        not is_true_leader
        and open_follow_state in {"weak_follow", "faded"}
        and not strong_non_hot_signal
        and not repair_probe_exception
        and not priority_front_row_exception
    ):
        return True
    if (
        decision_action == "hold_only"
        and auction_open_bucket == "near_limit_open"
        and open_follow_state != "confirmed"
        and not is_true_leader
    ):
        return True
    if (
        decision_action == "hold_only"
        and auction_open_bucket == "overheat_high_open"
        and open_follow_state == "weak_follow"
        and not is_true_leader
    ):
        return True
    if not is_true_leader and theme_x_score >= 5.6 and open_follow_state != "confirmed":
        return True
    if not is_true_leader and hot_rank > 120 and open_follow_state != "confirmed" and not strong_non_hot_signal:
        return True
    if (
        lb_days >= 1
        and not is_true_leader
        and hot_rank > 100
        and open_follow_state not in {"confirmed", "repair_strength"}
        and not strong_non_hot_signal
    ):
        return True
    if (
        lb_days >= 1
        and not is_true_leader
        and leader_rank_in_theme > 3
        and auction_amount < 20_000_000
        and amount_2m < 25_000_000
        and open_follow_state != "confirmed"
    ):
        return True
    return False


def is_playbook_theme_judge_blocked(
    *,
    decision_action: str,
    tier: str,
    mode_name: str,
    allowed_count: int,
    allowed_tiers: frozenset[str],
    action_class: str,
    validation_state: str,
    is_true_leader: bool,
    open_follow_state: str,
    theme_x_score: float,
    repair_probe_exception: bool,
    opening_phase_weak: bool,
    strong_non_hot_signal: bool,
) -> bool:
    if decision_action != "hold_only" and allowed_count <= 0:
        return True
    if decision_action != "hold_only" and allowed_tiers and tier not in allowed_tiers and not repair_probe_exception:
        return True
    if action_class in {"observe", "trap_avoid"} and not is_true_leader and not repair_probe_exception:
        return True
    if action_class == "anchor_only" and not is_true_leader and not repair_probe_exception:
        return True
    if validation_state == "falsified" and (decision_action != "hold_only" or not is_true_leader):
        return True
    if (
        action_class in {"observe", "trap_avoid"}
        and decision_action == "hold_only"
        and (not is_true_leader or open_follow_state in {"weak_follow", "faded"} or theme_x_score >= 5.6)
    ):
        return True
    if mode_name == "leader_only" and not is_true_leader and open_follow_state != "confirmed" and not repair_probe_exception:
        return True
    if opening_phase_weak and (decision_action == "hold_only" or not is_true_leader or not strong_non_hot_signal):
        return True
    return False


def is_playbook_priority_plate_blocked(
    *,
    phase_label: str,
    has_preferred_plates: bool,
    decision_hits_preferred_plate: bool,
    is_true_leader: bool,
    strong_non_hot_signal: bool,
    repair_probe_exception: bool,
    priority_front_row_exception: bool,
) -> bool:
    return bool(
        phase_label in {"open_confirm", "intraday"}
        and has_preferred_plates
        and not decision_hits_preferred_plate
        and not is_true_leader
        and not strong_non_hot_signal
        and not repair_probe_exception
        and not priority_front_row_exception
    )


def is_playbook_early_promote_eligible(
    *,
    phase_label: str,
    matched_plate: str | None,
    matched_plate_in_execution: bool,
    action_class: str,
    is_front_row: bool,
    strong_non_hot_signal: bool,
    decision_action: str,
) -> bool:
    return bool(
        phase_label in {"auction", "opening", "open_confirm"}
        and matched_plate
        and matched_plate_in_execution
        and action_class in {"main_attack", "front_row_confirm"}
        and is_front_row
        and strong_non_hot_signal
        and decision_action != "hold_only"
    )


def evaluate_playbook_gate_outcome(
    *,
    phase_label: str,
    decision_action: str,
    # hard block
    open_follow_state: str,
    high_dayk_trap: bool,
    auction_fakeout: bool,
    # priority plate
    has_preferred_plates: bool,
    decision_hits_preferred_plate: bool,
    # common identity
    is_true_leader: bool,
    is_front_row: bool,
    is_active_pool: bool,
    strong_non_hot_signal: bool,
    repair_probe_exception: bool,
    priority_front_row_exception: bool,
    # theme judge (optional)
    has_judge: bool,
    tier: str = "",
    mode_name: str = "",
    allowed_count: int = 0,
    allowed_tiers: frozenset[str] = frozenset(),
    action_class: str = "",
    validation_state: str = "",
    theme_x_score: float = 0.0,
    opening_phase_weak: bool = False,
    matched_plate: str | None = None,
    matched_plate_in_execution: bool = False,
    # behavior
    auction_open_bucket: str = "",
    hot_rank: int = 999,
    lb_days: int = 0,
    leader_rank_in_theme: int = 999,
    auction_amount: float = 0.0,
    amount_2m: float = 0.0,
) -> str:
    if is_playbook_hard_blocked(
        phase_label=phase_label,
        decision_action=decision_action,
        open_follow_state=open_follow_state,
        high_dayk_trap=high_dayk_trap,
        auction_fakeout=auction_fakeout,
    ):
        return "reject"
    if is_playbook_priority_plate_blocked(
        phase_label=phase_label,
        has_preferred_plates=has_preferred_plates,
        decision_hits_preferred_plate=decision_hits_preferred_plate,
        is_true_leader=is_true_leader,
        strong_non_hot_signal=strong_non_hot_signal,
        repair_probe_exception=repair_probe_exception,
        priority_front_row_exception=priority_front_row_exception,
    ):
        return "reject"
    if has_judge and is_playbook_early_promote_eligible(
        phase_label=phase_label,
        matched_plate=matched_plate,
        matched_plate_in_execution=matched_plate_in_execution,
        action_class=action_class,
        is_front_row=is_front_row,
        strong_non_hot_signal=strong_non_hot_signal,
        decision_action=decision_action,
    ):
        return "promote"
    if has_judge and is_playbook_theme_judge_blocked(
        decision_action=decision_action,
        tier=tier,
        mode_name=mode_name,
        allowed_count=allowed_count,
        allowed_tiers=allowed_tiers,
        action_class=action_class,
        validation_state=validation_state,
        is_true_leader=is_true_leader,
        open_follow_state=open_follow_state,
        theme_x_score=theme_x_score,
        repair_probe_exception=repair_probe_exception,
        opening_phase_weak=opening_phase_weak,
        strong_non_hot_signal=strong_non_hot_signal,
    ):
        return "reject"
    if is_playbook_behavior_blocked(
        decision_action=decision_action,
        is_true_leader=is_true_leader,
        is_active_pool=is_active_pool,
        strong_non_hot_signal=strong_non_hot_signal,
        open_follow_state=open_follow_state,
        auction_open_bucket=auction_open_bucket,
        theme_x_score=theme_x_score,
        hot_rank=hot_rank,
        repair_probe_exception=repair_probe_exception,
        priority_front_row_exception=priority_front_row_exception,
        lb_days=lb_days,
        leader_rank_in_theme=leader_rank_in_theme,
        auction_amount=auction_amount,
        amount_2m=amount_2m,
    ):
        return "reject"
    return "pass"
