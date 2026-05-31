from __future__ import annotations

from dataclasses import dataclass

from engine_next.domain.models import StockSelectionContext, StockStateSnapshot, ThemeSelectionContext
from engine_next.strategy_skill_layer.slice_comparison import build_opening_2m_slice_comparison
from engine_next.strategy_skill_layer.market_regime_resolver import market_is_defense, market_is_attack


_REPAIR_PATTERNS = {"low_open_strength", "pullback_repair", "n_rebound"}
_ATTACK_PATTERNS = {"platform_breakout", "breakout", "n_rebound"}


@dataclass(frozen=True)
class EntryStrategyMatrixOutcome:
    label: str = "neutral"
    confidence_delta: int = 0
    action_override: str | None = None
    notes: tuple[str, ...] = ()


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def _market_is_defense(theme_selection: ThemeSelectionContext | None, market_summary: object | None) -> bool:
    if theme_selection is not None and str(theme_selection.market_regime or "") == "defense":
        return True
    return market_is_defense(market_summary)


def _market_is_attack(theme_selection: ThemeSelectionContext | None, market_summary: object | None) -> bool:
    if theme_selection is not None and str(theme_selection.market_regime or "") == "attack":
        return True
    return market_is_attack(market_summary)


def _is_true_core(stock_selection: StockSelectionContext) -> bool:
    return bool(
        stock_selection.is_true_leader
        or (
            stock_selection.is_front_row
            and stock_selection.stock_amount_2m_rank_in_theme_pct <= 0.25
            and stock_selection.open_follow_state in {"confirmed", "repair_strength"}
        )
    )


def evaluate_entry_strategy_matrix(
    snapshot: StockStateSnapshot,
    *,
    stock_selection: StockSelectionContext | None,
    theme_selection: ThemeSelectionContext | None,
    market_summary: object | None,
) -> EntryStrategyMatrixOutcome:
    if stock_selection is None or theme_selection is None:
        return EntryStrategyMatrixOutcome()

    open2m = build_opening_2m_slice_comparison(market_summary)
    market_defense = _market_is_defense(theme_selection, market_summary)
    market_attack = _market_is_attack(theme_selection, market_summary)
    mainline_switch = bool(getattr(market_summary, "mainline_switch", False))
    amount_ratio_2m = (snapshot.amount_2m / snapshot.auction_amount) if snapshot.auction_amount > 0 else 0.0
    is_true_core = _is_true_core(stock_selection)

    migrating_out = getattr(market_summary, "migrating_out_plates", ())
    is_flow_withdrawing = theme_selection.plate_name in migrating_out

    if is_flow_withdrawing and not stock_selection.is_true_leader:
        return EntryStrategyMatrixOutcome(
            label="sector_flow_withdrawal_trap",
            confidence_delta=-20,
            action_override="observe_only",
            notes=("matrix=sector_flow_withdrawal_trap net_inflow_dropping_sharply",),
        )

    if (
        stock_selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"}
        and stock_selection.open_follow_state in {"weak_follow", "faded"}
        and (
            stock_selection.stock_amount_ratio_2m_rank_in_theme_pct >= 0.65
            or (snapshot.auction_amount > 0 and snapshot.amount_2m < snapshot.auction_amount * 0.85)
        )
        and (market_defense or theme_selection.x_score >= 4.5 or theme_selection.fakeout_level in {"medium", "high"})
    ):
        return EntryStrategyMatrixOutcome(
            label="high_open_distribution_trap",
            confidence_delta=-12,
            action_override="observe_only",
            notes=("matrix=high_open_distribution_trap high_open_fade 2m_follow_not_enough",),
        )

    if (
        theme_selection.tradable
        and (theme_selection.theme_trade_label == "switch_candidate" or mainline_switch)
        and theme_selection.plate_delta_rank_pct <= 0.35
        and theme_selection.plate_follow_through_score >= 4.5
        and theme_selection.plate_resistance_score <= 5.8
        and stock_selection.is_front_row
        and stock_selection.open_follow_state == "confirmed"
        and stock_selection.daily_height_bucket != "high"
        and stock_selection.stock_amount_2m_rank_in_theme_pct <= 0.35
        and stock_selection.stock_amount_ratio_2m_rank_in_theme_pct <= 0.45
        and stock_selection.kline_pattern in _ATTACK_PATTERNS
        and (market_attack or open2m.is_strong or amount_ratio_2m >= 1.2)
    ):
        return EntryStrategyMatrixOutcome(
            label="switch_front_row_attack",
            confidence_delta=16,
            action_override="early_boarding_candidate",
            notes=("matrix=switch_front_row_attack switch_front_row_with_2m_confirmation",),
        )

    if (
        theme_selection.tradable
        and not mainline_switch
        and theme_selection.plate_role in {"leader", "chaser"}
        and theme_selection.rotation_bias in {"inflow", "neutral"}
        and theme_selection.plate_follow_through_score >= 4.2
        and theme_selection.plate_resistance_score <= 6.2
        and stock_selection.is_front_row
        and stock_selection.open_follow_state == "confirmed"
        and stock_selection.stock_amount_ratio_2m_rank_in_theme_pct <= 0.45
        and stock_selection.kline_pattern in _ATTACK_PATTERNS
        and (
            stock_selection.daily_height_bucket in {"low", "mid"}
            or (stock_selection.daily_height_bucket == "high" and stock_selection.is_true_leader)
        )
        and (market_attack or amount_ratio_2m >= 1.05 or open2m.is_strong)
    ):
        return EntryStrategyMatrixOutcome(
            label="mainline_continuation_attack",
            confidence_delta=10,
            action_override="early_boarding_candidate",
            notes=("matrix=mainline_continuation_attack mainline_follow_through_confirmed",),
        )

    if (
        market_defense
        and theme_selection.tradable
        and not is_true_core
        and stock_selection.is_front_row
        and stock_selection.open_follow_state in {"weak_follow", "confirmed", "repair_strength"}
        and not (
            stock_selection.kline_pattern in _REPAIR_PATTERNS
            and stock_selection.auction_open_bucket in {"deep_low_open", "low_open", "flat_open"}
            and stock_selection.daily_height_bucket in {"low", "mid"}
        )
        and (
            theme_selection.plate_role in {"chaser", "neutral", "laggard"}
            or theme_selection.plate_resistance_score >= 5.8
            or stock_selection.daily_height_bucket == "high"
        )
    ):
        return EntryStrategyMatrixOutcome(
            label="weak_market_non_core_filter",
            confidence_delta=-10,
            action_override="observe_only",
            notes=("matrix=weak_market_non_core_filter weak_tape_non_core_stand_aside",),
        )

    if (
        theme_selection.tradable
        and market_defense
        and theme_selection.plate_resistance_score <= 5.6
        and theme_selection.rotation_bias in {"repair", "neutral", "inflow"}
        and stock_selection.open_follow_state in {"confirmed", "repair_strength"}
        and stock_selection.daily_height_bucket in {"low", "mid"}
        and stock_selection.stock_amount_ratio_2m_rank_in_theme_pct <= 0.45
        and stock_selection.kline_pattern in _REPAIR_PATTERNS
        and stock_selection.auction_open_bucket in {"deep_low_open", "low_open", "flat_open"}
        and (
            snapshot.amount_2m >= max(snapshot.auction_amount * 0.9, 18_000_000)
            or stock_selection.stock_amount_2m_rank_in_theme_pct <= 0.35
        )
    ):
        return EntryStrategyMatrixOutcome(
            label="defensive_absorb_repair",
            confidence_delta=12,
            action_override="small_probe_only",
            notes=("matrix=defensive_absorb_repair low_open_absorb_with_repair",),
        )

    if (
        theme_selection.tradable
        and market_defense
        and is_true_core
        and theme_selection.plate_role in {"leader", "defensive_holder", "neutral"}
        and theme_selection.plate_resistance_score <= 5.8
        and stock_selection.open_follow_state in {"confirmed", "repair_strength"}
        and stock_selection.stock_amount_ratio_2m_rank_in_theme_pct <= 0.35
        and (
            stock_selection.daily_height_bucket in {"low", "mid"}
            or (stock_selection.daily_height_bucket == "high" and stock_selection.is_true_leader)
        )
        and (snapshot.amount_2m >= max(snapshot.auction_amount * 0.9, 20_000_000) or open2m.is_strong)
    ):
        return EntryStrategyMatrixOutcome(
            label="weak_market_true_core_only",
            confidence_delta=9,
            action_override="small_probe_only",
            notes=("matrix=weak_market_true_core_only weak_tape_only_true_core_can_trade",),
        )

    if (
        theme_selection.tradable
        and market_defense
        and theme_selection.rotation_bias in {"repair", "inflow"}
        and stock_selection.is_front_row
        and stock_selection.open_follow_state in {"confirmed", "repair_strength"}
        and stock_selection.daily_height_bucket in {"low", "mid"}
        and snapshot.t2_pct <= -0.04
        and snapshot.current_pct > 0
        and (
            snapshot.amount_2m >= max(snapshot.auction_amount, 20_000_000)
            or stock_selection.stock_amount_2m_rank_in_theme_pct <= 0.35
        )
    ):
        return EntryStrategyMatrixOutcome(
            label="selloff_repair_reversal",
            confidence_delta=11,
            action_override="small_probe_only",
            notes=("matrix=selloff_repair_reversal selloff_then_repair_with_returning_flow",),
        )

    return EntryStrategyMatrixOutcome()
