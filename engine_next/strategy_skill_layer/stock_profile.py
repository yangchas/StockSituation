from __future__ import annotations

from engine_next.domain.enums import (
    ExposureState,
    FailedPromotionType,
    FeedbackState,
    LeaderTier,
    OperatorStyleHint,
    StockArchetype,
    StockStage,
    TradeWindowState,
)
from engine_next.domain.models import StockProfileAssessment, StockStateSnapshot


def _clamp_score(value: float, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(round(value))))


def _infer_leader_tier(snapshot: StockStateSnapshot) -> tuple[LeaderTier, list[str]]:
    notes: list[str] = []
    if snapshot.leader_rank_in_theme <= 1 and snapshot.lb_days >= 3:
        notes.append("highest board count inside the theme defines the strongest leader")
        return LeaderTier.ABSOLUTE, notes
    if snapshot.leader_rank_in_theme <= 1 and snapshot.lb_days >= 2:
        notes.append("highest board count inside the theme defines the leader")
        return LeaderTier.CORE, notes
    if snapshot.leader_rank_in_theme <= 3 and snapshot.lb_days >= 1:
        notes.append("within first echelon of the theme")
        return LeaderTier.CORE, notes
    if snapshot.leader_rank_in_theme <= 5 or snapshot.lb_days >= 1:
        notes.append("secondary participant inside theme")
        return LeaderTier.SECONDARY, notes
    if snapshot.plate:
        notes.append("theme participant but not leader")
        return LeaderTier.FOLLOWER, notes
    return LeaderTier.UNKNOWN, notes


def _infer_failed_promotion_type(snapshot: StockStateSnapshot) -> tuple[FailedPromotionType, list[str]]:
    notes: list[str] = []
    if snapshot.yday_broken_board and snapshot.day_before_limit_up:
        notes.append("matches yesterday break-board after prior-day limit-up")
        return FailedPromotionType.YDAY_BREAK_AFTER_LIMIT, notes
    if snapshot.touched_limit_today and not snapshot.is_locked:
        notes.append("touched limit today but failed to relock")
        return FailedPromotionType.INTRADAY_RELOCK_FAIL, notes
    if snapshot.t2_lb_days >= 1 and snapshot.t2_pct < -0.05:
        notes.append("recent promotion failed and turned into weak continuation")
        return FailedPromotionType.WEAK_CONTINUATION, notes
    return FailedPromotionType.NONE, notes


def _infer_operator_style(snapshot: StockStateSnapshot) -> tuple[OperatorStyleHint, list[str]]:
    notes: list[str] = []
    if snapshot.market_cap_yi >= 300 or snapshot.amount_day_yi >= 40:
        if snapshot.lb_days <= 2:
            notes.append("larger cap and turnover fit institutional style")
            return OperatorStyleHint.INSTITUTION, notes
        notes.append("large turnover but still active on board ladder")
        return OperatorStyleHint.MIXED, notes
    if snapshot.lb_days >= 1 and 5 <= snapshot.amount_day_yi <= 35:
        notes.append("board preference plus medium turnover fits hot-money style")
        return OperatorStyleHint.HOT_MONEY, notes
    if snapshot.market_cap_yi > 0 or snapshot.amount_day_yi > 0:
        notes.append("size and turnover suggest mixed style")
        return OperatorStyleHint.MIXED, notes
    return OperatorStyleHint.UNKNOWN, notes


def _infer_archetype(snapshot: StockStateSnapshot) -> tuple[StockArchetype, list[str]]:
    notes: list[str] = []
    if snapshot.t2_lb_days >= 1 and snapshot.t2_pct <= -0.03 and snapshot.current_pct > 0:
        notes.append("reversal against recent failure")
        return StockArchetype.CONTRARIAN_STRENGTH, notes
    if snapshot.leader_rank_in_theme <= 1 and snapshot.lb_days >= 2 and snapshot.resonance_factor >= 1.0:
        notes.append("top theme rank with board height support")
        return StockArchetype.DRAGON_LEADER, notes
    if snapshot.lb_days >= 4 and snapshot.resonance_factor >= 1.2:
        notes.append("high board height and strong resonance")
        return StockArchetype.DRAGON_LEADER, notes
    if snapshot.lb_days >= 2 and snapshot.amount_2m >= 8_000_000 and snapshot.vol_ratio >= 1.5:
        notes.append("board strength plus amount support")
        return StockArchetype.CORE_TREND, notes
    if snapshot.vector_5m > 0.025 and snapshot.amount_5m >= 20_000_000:
        notes.append("multi-minute pulse confirms ongoing short-term trend")
        return StockArchetype.CORE_TREND, notes
    if (
        snapshot.market_cap_yi >= 300
        or snapshot.amount_day_yi >= 40
        or (
            snapshot.profit_ratio <= 0.20
            and snapshot.concentration <= 0.20
            and snapshot.bias_20 > -0.05
        )
    ):
        notes.append("lower overhang and tighter concentration profile")
        return StockArchetype.INSTITUTIONAL_TREND, notes
    if snapshot.auction_amount >= 30_000_000 and snapshot.speed_1m > 0.01:
        notes.append("aggressive early capital behavior")
        return StockArchetype.STRONG_OPERATOR, notes
    if snapshot.lb_days <= 1 and snapshot.resonance_factor >= 1.0:
        notes.append("likely theme follower rather than clear leader")
        return StockArchetype.FOLLOWER, notes
    return StockArchetype.UNKNOWN, notes


def _infer_stage(snapshot: StockStateSnapshot) -> tuple[StockStage, list[str]]:
    notes: list[str] = []
    if snapshot.t2_lb_days >= 1 and snapshot.t2_pct < -0.05 and snapshot.current_pct > 0:
        notes.append("ice-point rebound after failed promotion")
        return StockStage.ICE_POINT_REBOUND, notes
    if snapshot.t2_lb_days >= 1 and snapshot.t2_pct < -0.05 and snapshot.current_pct < 0:
        notes.append("failed promotion turning into negative feedback")
        return StockStage.FAILED_PROMOTION, notes
    if snapshot.lb_days == 0 and snapshot.current_pct > 0 and snapshot.vol_ratio > 2.0:
        notes.append("fresh breakout confirmation")
        return StockStage.CONFIRMATION, notes
    if 1 <= snapshot.lb_days <= 2 and snapshot.current_pct > 0.03:
        notes.append("early trend continuation")
        return StockStage.MAIN_RISE, notes
    if snapshot.lb_days >= 3 and snapshot.current_pct > 0.08:
        notes.append("high-position acceleration")
        return StockStage.HIGH_ACCELERATION, notes
    if snapshot.lb_days >= 3 and snapshot.current_pct < 0.03:
        notes.append("high-position divergence")
        return StockStage.HIGH_DIVERGENCE, notes
    if snapshot.current_pct > 0 and snapshot.bias_20 < 0:
        notes.append("trend repair after pullback")
        return StockStage.TREND_REPAIR, notes
    if snapshot.current_pct <= 0 and snapshot.lb_days == 0:
        notes.append("pre-breakout seed stage")
        return StockStage.SEED, notes
    return StockStage.UNKNOWN, notes


def _infer_feedback(
    snapshot: StockStateSnapshot,
    stage: StockStage,
    failed_promotion_type: FailedPromotionType,
) -> tuple[FeedbackState, list[str]]:
    notes: list[str] = []
    if (
        stage == StockStage.FAILED_PROMOTION
        or failed_promotion_type != FailedPromotionType.NONE
        or (snapshot.t2_lb_days >= 1 and snapshot.t2_pct < -0.05)
    ):
        notes.append("recent failed promotion implies next-day negative feedback risk")
        return FeedbackState.NEGATIVE, notes
    if snapshot.current_pct > 0.05 or snapshot.lb_days >= 2:
        notes.append("positive continuation footprint")
        return FeedbackState.POSITIVE, notes
    return FeedbackState.NEUTRAL, notes


def _infer_exposure(snapshot: StockStateSnapshot) -> tuple[ExposureState, int, int, list[str]]:
    notes: list[str] = []
    exposure_score = 0.0
    exposure_score += max(snapshot.lb_days - 1, 0) * 12
    exposure_score += max(snapshot.current_pct, 0.0) * 180
    exposure_score += max(snapshot.auction_amount / 100_000_000, 0.0) * 15
    exposure_score += max(snapshot.amount_2m / 100_000_000, 0.0) * 10
    exposure_score += max(snapshot.amount_5m / 100_000_000, 0.0) * 5
    exposure_score += max(snapshot.speed_1m, 0.0) * 800
    exposure_score += max(snapshot.vector_3m, 0.0) * 300
    exposure_score += max(snapshot.plate_persistence_score, 0.0) * 10
    if snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank > 0:
        exposure_score += max(0, 35 - snapshot.ths_hot_rank)

    score = _clamp_score(exposure_score)
    retail_attention_proxy = score
    if snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank > 0:
        retail_attention_proxy = _clamp_score(100 - min(snapshot.ths_hot_rank, 100))
    if score >= 70:
        notes.append("high attention / dark-forest exposure")
        return ExposureState.OVEREXPOSED, score, retail_attention_proxy, notes
    if score <= 30:
        notes.append("still under the crowd radar")
        return ExposureState.UNDEREXPOSED, score, retail_attention_proxy, notes
    return ExposureState.BALANCED, score, retail_attention_proxy, notes


def _infer_trade_window(
    snapshot: StockStateSnapshot,
    archetype: StockArchetype,
    leader_tier: LeaderTier,
    stage: StockStage,
    feedback_state: FeedbackState,
    exposure_state: ExposureState,
) -> tuple[TradeWindowState, int, list[str]]:
    notes: list[str] = []
    continuation_score = 0.0
    continuation_score += max(snapshot.resonance_factor - 1.0, 0.0) * 30
    continuation_score += max(snapshot.vol_ratio - 1.0, 0.0) * 15
    continuation_score += max(snapshot.speed_1m, 0.0) * 600
    continuation_score += max(snapshot.vector_3m, 0.0) * 260
    continuation_score += max(snapshot.vector_5m, 0.0) * 220
    continuation_score += max(snapshot.current_pct, 0.0) * 120
    if archetype == StockArchetype.DRAGON_LEADER:
        continuation_score += 15
    if leader_tier == LeaderTier.ABSOLUTE:
        continuation_score += 10
    if feedback_state == FeedbackState.NEGATIVE:
        continuation_score -= 30
    if exposure_state == ExposureState.OVEREXPOSED:
        continuation_score -= 15
    if snapshot.plate_persistence_score >= 1.5 or snapshot.hot_plate_days >= 2:
        continuation_score += 12

    score = _clamp_score(continuation_score)
    if feedback_state == FeedbackState.NEGATIVE:
        notes.append("negative feedback dominates entry timing")
        return TradeWindowState.AVOID, score, notes
    if (
        stage in (StockStage.CONFIRMATION, StockStage.MAIN_RISE, StockStage.ICE_POINT_REBOUND)
        and exposure_state != ExposureState.OVEREXPOSED
        and snapshot.lb_days <= 2
    ):
        notes.append("boarding window is short and mainly concentrated in the first one to two days")
        return TradeWindowState.EARLY_BOARDING, score, notes
    if stage in (StockStage.HIGH_ACCELERATION, StockStage.HIGH_DIVERGENCE) and archetype == StockArchetype.DRAGON_LEADER:
        notes.append("leader may remain strong but boarding window is short")
        return TradeWindowState.HOLD_ONLY, score, notes
    if exposure_state == ExposureState.OVEREXPOSED:
        notes.append("attention is too high, chase risk dominates")
        return TradeWindowState.CHASE_RISK, score, notes
    return TradeWindowState.AVOID, score, notes


def assess_stock_profile(snapshot: StockStateSnapshot) -> StockProfileAssessment:
    leader_tier, leader_notes = _infer_leader_tier(snapshot)
    failed_promotion_type, failed_notes = _infer_failed_promotion_type(snapshot)
    operator_style_hint, operator_notes = _infer_operator_style(snapshot)
    archetype, archetype_notes = _infer_archetype(snapshot)
    stage, stage_notes = _infer_stage(snapshot)
    feedback_state, feedback_notes = _infer_feedback(snapshot, stage, failed_promotion_type)
    exposure_state, exposure_score, retail_attention_proxy, exposure_notes = _infer_exposure(snapshot)
    trade_window, continuation_score, trade_notes = _infer_trade_window(
        snapshot=snapshot,
        archetype=archetype,
        leader_tier=leader_tier,
        stage=stage,
        feedback_state=feedback_state,
        exposure_state=exposure_state,
    )
    notes = tuple(
        leader_notes
        + failed_notes
        + operator_notes
        + archetype_notes
        + stage_notes
        + feedback_notes
        + exposure_notes
        + trade_notes
    )
    return StockProfileAssessment(
        symbol=snapshot.symbol,
        archetype=archetype,
        leader_tier=leader_tier,
        stage=stage,
        failed_promotion_type=failed_promotion_type,
        operator_style_hint=operator_style_hint,
        feedback_state=feedback_state,
        exposure_state=exposure_state,
        trade_window=trade_window,
        darkness_exposure_score=exposure_score,
        continuation_score=continuation_score,
        retail_attention_proxy=retail_attention_proxy,
        notes=notes,
    )
