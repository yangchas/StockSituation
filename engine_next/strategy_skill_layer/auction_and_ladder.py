from __future__ import annotations

from engine_next.domain.enums import (
    FeedbackState,
    LeaderTier,
    StockArchetype,
    StockStage,
    TradeWindowState,
)
from engine_next.domain.models import AuctionLadderDecision, StockStateSnapshot
from engine_next.strategy_skill_layer.stock_profile import assess_stock_profile


def _clamp(value: float, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(round(value))))


def _calc_kelly_position(win_rate: float, avg_win_pct: float = 0.10, avg_loss_pct: float = 0.03) -> float:
    if win_rate <= 0 or win_rate >= 1:
        win_rate = 0.45
    b = avg_win_pct / avg_loss_pct
    q = 1.0 - win_rate
    f_star = (win_rate * b - q) / b
    return round(max(0.02, min(0.20, f_star * 0.5)), 4)


def _risk_reward_ratio(snapshot: StockStateSnapshot, profile: object) -> float:
    target_pct = 0.10
    if getattr(profile, "leader_tier", None) == LeaderTier.ABSOLUTE:
        target_pct = 0.12
    loss_pct = max(0.02, abs(snapshot.resistance_gap) * 1.5 if snapshot.resistance_gap else 0.02)
    return round(max(0.0, (target_pct - snapshot.current_pct) / loss_pct), 1)


def _estimate_win_rate(snapshot: StockStateSnapshot, profile: object) -> float:
    win_rate = 0.48
    if profile.archetype == StockArchetype.DRAGON_LEADER:
        win_rate += 0.08
    if profile.stage in (StockStage.CONFIRMATION, StockStage.MAIN_RISE):
        win_rate += 0.05
    if profile.stage == StockStage.ICE_POINT_REBOUND:
        win_rate -= 0.06
    if profile.feedback_state == FeedbackState.NEGATIVE:
        win_rate -= 0.10
    if profile.trade_window == TradeWindowState.EARLY_BOARDING:
        win_rate += 0.04
    if snapshot.plate_persistence_score >= 1.5 or snapshot.hot_plate_days >= 2:
        win_rate += 0.04
    return max(0.25, min(0.75, win_rate))


def build_auction_and_ladder_decision(snapshot: StockStateSnapshot) -> AuctionLadderDecision:
    profile = assess_stock_profile(snapshot)
    reasons: list[str] = list(profile.notes)
    confidence = 35
    setup_id = "observe_only"
    action = "observe_only"

    if profile.feedback_state == FeedbackState.NEGATIVE:
        confidence = 25
        setup_id = "failed_promotion_guard"
        action = "avoid_after_failed_promotion"
        reasons.append("recent failed promotion dominates next-day handling")
    elif profile.archetype == StockArchetype.DRAGON_LEADER and profile.trade_window == TradeWindowState.EARLY_BOARDING:
        confidence = 78
        setup_id = "dragon_early_board"
        action = "dragon_early_board"
        reasons.append("theme leader still inside short boarding window")
    elif profile.archetype == StockArchetype.DRAGON_LEADER and profile.trade_window == TradeWindowState.HOLD_ONLY:
        confidence = 68
        setup_id = "dragon_hold_only"
        action = "hold_only"
        reasons.append("leader remains strong but boarding window is already short")
    elif profile.stage == StockStage.ICE_POINT_REBOUND and profile.trade_window == TradeWindowState.EARLY_BOARDING:
        confidence = 55
        setup_id = "ice_rebound_probe"
        action = "small_probe_only"
        reasons.append("rebound is present but should be treated as limited-size probe")
    elif profile.trade_window == TradeWindowState.EARLY_BOARDING:
        confidence = 62
        setup_id = "early_boarding_candidate"
        action = "early_boarding_candidate"
        reasons.append("still inside early boarding window")
    elif profile.trade_window == TradeWindowState.CHASE_RISK:
        confidence = 40
        setup_id = "chase_risk_guard"
        action = "do_not_chase"
        reasons.append("exposure is already high and chase risk dominates")

    if profile.leader_tier == LeaderTier.ABSOLUTE:
        confidence += 8
        reasons.append("market or theme absolute leader gets leadership premium")
    elif profile.leader_tier == LeaderTier.CORE:
        confidence += 4
        reasons.append("core leader inside theme gets smaller premium")

    if snapshot.plate_persistence_score >= 1.5 or snapshot.hot_plate_days >= 2:
        confidence += 6
        reasons.append("plate persistence improves continuation odds")

    risk_reward = _risk_reward_ratio(snapshot, profile)
    if risk_reward < 2.0 and action not in ("hold_only", "observe_only", "avoid_after_failed_promotion", "do_not_chase"):
        action = "observe_only"
        setup_id = "risk_reward_filtered"
        reasons.append("risk-reward is below the minimum threshold")

    confidence = _clamp(confidence)
    win_rate = _estimate_win_rate(snapshot, profile)
    kelly_position = _calc_kelly_position(win_rate=win_rate)

    if action in ("avoid_after_failed_promotion", "do_not_chase", "observe_only"):
        kelly_position = 0.02

    return AuctionLadderDecision(
        symbol=snapshot.symbol,
        setup_id=setup_id,
        action=action,
        confidence=confidence,
        kelly_position_pct=kelly_position,
        risk_reward_ratio=risk_reward,
        profile=profile,
        reasons=tuple(reasons),
    )
