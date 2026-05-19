from __future__ import annotations

from engine_next.domain.enums import (
    FeedbackState,
    LeaderTier,
    StockArchetype,
    StockStage,
    TradeWindowState,
)
from engine_next.domain.models import (
    AuctionLadderDecision,
    IntradayMarketSummary,
    StockProfileAssessment,
    StockSelectionContext,
    StockStateSnapshot,
    ThemeSelectionContext,
)
from engine_next.strategy_skill_layer.entry_strategy_matrix import evaluate_entry_strategy_matrix
from engine_next.strategy_skill_layer.stock_profile import assess_stock_profile
from engine_next.strategy_skill_layer.trap_guards import is_high_dayk_weak_trap


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


_BULLISH_KLINE_PATTERNS = {"platform_breakout", "low_open_strength", "n_rebound", "breakout", "pullback_repair"}
_BEARISH_KLINE_PATTERNS = {"high_open_then_weak", "volume_up_price_flat", "high_divergence", "explosive_failed_board"}
_ENTRY_VETO_KLINE_PATTERNS = {"high_open_then_weak", "volume_up_price_flat", "explosive_failed_board"}


def build_auction_and_ladder_decision(
    snapshot: StockStateSnapshot,
    *,
    profile: StockProfileAssessment | None = None,
    stock_selection: StockSelectionContext | None = None,
    theme_selection: ThemeSelectionContext | None = None,
    market_summary: IntradayMarketSummary | None = None,
) -> AuctionLadderDecision:
    profile = profile or assess_stock_profile(snapshot)
    reasons: list[str] = list(profile.notes)
    confidence = 35
    setup_id = "observe_only"
    action = "observe_only"
    if theme_selection is not None:
        reasons.append(
            f"theme_ctx tradable={int(theme_selection.tradable)} fakeout={theme_selection.fakeout_level} x={theme_selection.x_score:.1f}"
        )
    if stock_selection is not None:
        reasons.append(
            f"stock_ctx leader={stock_selection.leader_bucket} kline={stock_selection.kline_pattern} auction={stock_selection.auction_score:.1f}"
        )

    if theme_selection is not None and not theme_selection.tradable:
        strong_watch_candidate = bool(
            stock_selection is not None
            and (
                stock_selection.is_true_leader
                or (
                    stock_selection.is_front_row
                    and stock_selection.theme_core_score >= 7.0
                    and stock_selection.execution_quality_score >= 5.8
                    and stock_selection.open_undertake_score >= 5.6
                )
            )
        )
        if strong_watch_candidate:
            confidence = 64 if stock_selection is not None and stock_selection.is_true_leader else 60
            setup_id = "theme_not_tradable_watch"
            action = "observe_only"
            reasons.append("theme is not tradable yet, keep only the leader/front-row on watchlist")
        else:
            confidence = 20
            setup_id = "theme_not_tradable_guard"
            action = "observe_only"
            reasons.append("theme is not tradable, so stock-level strength is not enough")
    elif theme_selection is not None and theme_selection.open_confirm_state == "falsified":
        confidence = 18
        setup_id = "open_confirm_falsified_guard"
        action = "observe_only"
        reasons.append("open confirmation falsified the theme, so entries are blocked")
    elif (
        theme_selection is not None
        and theme_selection.fakeout_level in {"high", "extreme"}
        and not (stock_selection and stock_selection.is_true_leader)
    ):
        confidence = 22
        setup_id = "theme_fakeout_guard"
        action = "observe_only"
        reasons.append("theme fakeout risk blocks non-leader participation")

    elif profile.feedback_state == FeedbackState.NEGATIVE:
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

    if stock_selection is not None:
        if stock_selection.is_active_pool:
            confidence += 6
            reasons.append("active-pool stock has real attention and liquidity support")
        else:
            confidence -= 8
            reasons.append("non-active stock lacks enough attention for short-term execution")
        if stock_selection.theme_core_score >= 8.0:
            confidence += 8
            reasons.append("theme-core score marks this as a true core opportunity inside the theme")
        elif stock_selection.theme_core_score >= 6.0:
            confidence += 4
            reasons.append("theme-core score keeps the stock in the first attack echelon")
        elif stock_selection.theme_core_score < 4.0:
            confidence -= 6
            reasons.append("theme-core score is too weak, likely only a follow-up participant")
        if stock_selection.is_true_leader:
            confidence += 10
            reasons.append("true leader inside tradable theme gets clear premium")
        elif stock_selection.is_front_row:
            confidence += 5
            reasons.append("front-row stock inside theme gets entry priority")
        else:
            confidence -= 6
            reasons.append("back-row participant gets downgraded")

        confidence += _clamp((stock_selection.total_score - 5.0) * 3.0, minimum=-12, maximum=15)
        confidence += _clamp((stock_selection.activity_score - 5.0) * 2.0, minimum=-8, maximum=10)
        if stock_selection.kline_score < 4.0:
            confidence -= 5
            reasons.append("kline structure is not supportive enough")
        if stock_selection.structure_score >= 7.0:
            confidence += 5
            reasons.append("multi-day structure and chip factors support continuation")
        elif stock_selection.structure_score < 4.0:
            confidence -= 6
            reasons.append("multi-day structure is weak and lowers continuation quality")
        if stock_selection.kline_pattern in _BULLISH_KLINE_PATTERNS:
            confidence += 6
            reasons.append(f"kline pattern {stock_selection.kline_pattern} supports active continuation")
        elif stock_selection.kline_pattern in _BEARISH_KLINE_PATTERNS:
            confidence -= 10
            reasons.append(f"kline pattern {stock_selection.kline_pattern} warns of weak follow-through")
        if theme_selection is not None and theme_selection.market_regime == "defense":
            if stock_selection.kline_pattern in {"platform_breakout", "breakout"}:
                confidence -= 6
                reasons.append("defensive tape discounts breakout style entries")
            elif stock_selection.kline_pattern in {"low_open_strength", "pullback_repair", "n_rebound"}:
                confidence += 3
                reasons.append("defensive tape prefers repair and low-open strength")
        elif theme_selection is not None and theme_selection.market_regime == "attack":
            if stock_selection.kline_pattern in {"platform_breakout", "breakout", "n_rebound"}:
                confidence += 3
                reasons.append("attack tape rewards expansion-style entries")
        if stock_selection.auction_score >= 7.0:
            confidence += 4
            reasons.append("auction quality confirms active participation")
        if stock_selection.activity_score >= 7.0:
            confidence += 4
            reasons.append("activity score confirms short-term capital focus")
        if stock_selection.timing_score < 4.0 and action not in ("hold_only", "observe_only"):
            confidence -= 8
            reasons.append("timing quality is weak and needs confirmation")
        if (
            stock_selection.kline_pattern in _ENTRY_VETO_KLINE_PATTERNS
            and action not in ("hold_only", "observe_only", "avoid_after_failed_promotion", "do_not_chase")
        ):
            action = "observe_only"
            setup_id = "weak_shape_guard"
            reasons.append("weak opening shape blocks active entry until structure repairs")
        elif (
            stock_selection.kline_pattern == "high_divergence"
            and action in ("dragon_early_board", "early_boarding_candidate", "small_probe_only")
        ):
            action = "small_probe_only"
            setup_id = "high_divergence_probe_only"
            reasons.append("high divergence only allows reduced-size probe")

    if theme_selection is not None:
        if theme_selection.cohesion_level == "strong":
            confidence += 6
            reasons.append("theme cohesion supports continuation")
        elif theme_selection.cohesion_level == "weak":
            confidence -= 4
            reasons.append("theme cohesion is weak, reduce aggression")
        if theme_selection.open_confirm_state == "strengthened":
            confidence += 5
            reasons.append("open confirmation strengthened the theme")
        elif theme_selection.open_confirm_state == "maintained":
            confidence += 1
        elif theme_selection.open_confirm_state == "falsified":
            confidence -= 10
        if theme_selection.x_score >= 4.5 and action not in ("hold_only", "observe_only", "avoid_after_failed_promotion"):
            action = "small_probe_only"
            setup_id = "theme_risk_probe_only"
            reasons.append("theme risk is elevated, only probe after confirmation")
        if theme_selection.fakeout_level in {"medium", "high"} and action == "early_boarding_candidate":
            action = "small_probe_only"
            setup_id = "fakeout_probe_only"
            reasons.append("theme fakeout pressure downgrades early attack to probe only")
        if (
            action == "hold_only"
            and theme_selection.fakeout_level in {"high", "extreme"}
            and stock_selection is not None
            and stock_selection.open_follow_state in {"weak_follow", "faded"}
        ):
            action = "observe_only"
            confidence -= 14
            setup_id = "fakeout_hold_downgrade"
            reasons.append("fakeout-heavy theme cannot keep weak hold-only leader in core action")

    matrix_outcome = evaluate_entry_strategy_matrix(
        snapshot,
        stock_selection=stock_selection,
        theme_selection=theme_selection,
        market_summary=market_summary,
    )
    if matrix_outcome.label != "neutral":
        confidence += int(matrix_outcome.confidence_delta)
        reasons.extend(matrix_outcome.notes)
        if (
            matrix_outcome.action_override is not None
            and action not in {"hold_only", "avoid_after_failed_promotion", "do_not_chase"}
        ):
            action = matrix_outcome.action_override
            setup_id = matrix_outcome.label

    if is_high_dayk_weak_trap(snapshot, stock_selection, theme_selection):
        if action == "hold_only":
            action = "observe_only"
            setup_id = "high_dayk_weak_hold_guard"
            confidence -= 16
            reasons.append("high dayK leader with weak follow-through should exit core hold recommendation")
        elif action not in {"observe_only", "avoid_after_failed_promotion", "do_not_chase"}:
            action = "do_not_chase"
            setup_id = "high_dayk_weak_entry_guard"
            confidence -= 12
            reasons.append("high dayK weak opening structure blocks new entry")

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
    if stock_selection is not None and not stock_selection.theme_tradable:
        kelly_position = 0.02
    else:
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
