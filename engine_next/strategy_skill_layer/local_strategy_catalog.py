from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LocalStrategySpec:
    node_id: str
    layer: str
    responsibility: str
    not_responsible_for: tuple[str, ...]
    required_inputs: tuple[str, ...]
    output_states: tuple[str, ...]
    depends_on: tuple[str, ...] = ()


LOCAL_STRATEGY_SPECS: tuple[LocalStrategySpec, ...] = (
    LocalStrategySpec(
        node_id="stock_microstructure",
        layer="stock",
        responsibility="classify current stock-level auction/opening behavior from open pct, 1m speed, rolling 2m amount, amount-vs-auction, and front-row role",
        not_responsible_for=("market regime", "theme migration verdict", "final buy decision"),
        required_inputs=("open_pct", "current_pct", "auction_amount", "amount_2m", "amount_2m_rank_pct", "speed_1m", "leader_rank_in_theme"),
        output_states=("micro_attack", "micro_repair", "micro_distribution", "micro_weak_follow", "micro_watch"),
    ),
    LocalStrategySpec(
        node_id="stock_profile",
        layer="stock",
        responsibility="classify whether daily shape, chip cleanliness, resistance, market cap, and liquidity support or block the micro signal",
        not_responsible_for=("intraday attack timing", "theme strength verdict", "global risk appetite"),
        required_inputs=("daily_height_bucket", "resistance_gap", "shape_chip_cleanliness", "market_cap_yi", "amount_day_yi"),
        output_states=("profile_support", "profile_high_risk", "profile_neutral"),
        depends_on=("stock_microstructure",),
    ),
    LocalStrategySpec(
        node_id="stock_capital_profile",
        layer="stock",
        responsibility="classify stock-level capital/chip/factor support from DDE, profit ratio, concentration, bias, RSI, and chip cleanliness",
        not_responsible_for=("theme migration verdict", "market-wide risk appetite", "final buy decision"),
        required_inputs=("ddx", "ddy", "ddz", "ddje", "profit_ratio", "concentration", "bias_20", "rsi_6", "shape_chip_cleanliness"),
        output_states=("capital_support", "capital_divergence", "capital_overheat", "capital_watch"),
        depends_on=("stock_microstructure", "stock_profile"),
    ),
    LocalStrategySpec(
        node_id="weak_to_strong_repair",
        layer="stock",
        responsibility="classify low-open/deep-water repair candidates from auction amount, rolling 2m amount, 1m speed, open-to-current repair, and daily height",
        not_responsible_for=("declaring the theme tradable", "final low-buy decision", "position sizing"),
        required_inputs=("open_pct", "current_pct", "auction_amount", "amount_2m", "amount_2m_rank_pct", "speed_1m", "daily_height_bucket"),
        output_states=("repair_confirmed", "repair_wait", "repair_failed", "repair_watch"),
        depends_on=("stock_microstructure", "stock_profile", "stock_capital_profile"),
    ),
    LocalStrategySpec(
        node_id="high_focus",
        layer="high_focus",
        responsibility="classify high-attention stocks and high-board feedback as promotion, neutral, or negative pressure",
        not_responsible_for=("selecting low-level rotation stocks", "declaring a theme tradable by itself"),
        required_inputs=("lb_days", "is_yest_limit", "leader_rank_in_theme", "open_pct", "current_pct", "amount_2m", "auction_amount"),
        output_states=("high_focus_promotion", "high_focus_negative", "high_focus_watch"),
    ),
    LocalStrategySpec(
        node_id="theme_high_focus_impact",
        layer="theme",
        responsibility="map high-attention stock feedback into theme-level promotion, individual failure, group pressure, or high-board drag",
        not_responsible_for=("stock entry timing", "final theme tradability", "position sizing"),
        required_inputs=("high_focus signals", "stock theme set", "theme symbol_count", "lb_days", "open_pct", "current_pct", "amount_2m"),
        output_states=("theme_high_promotion", "theme_high_individual_fail", "theme_high_group_pressure", "theme_high_drag_watch"),
        depends_on=("high_focus", "stock_microstructure"),
    ),
    LocalStrategySpec(
        node_id="auction_bucket_local",
        layer="theme",
        responsibility="summarize auction buckets by plate without deciding final tradability: auction amount, red/green breadth, leaders, yesterday-limit hits, and open/current drift",
        not_responsible_for=("stock entry", "post-open validation", "position sizing"),
        required_inputs=("auction_amount", "red_count", "green_count", "leader_count", "yest_limit_count", "avg_open_pct", "avg_current_pct"),
        output_states=("auction_bucket_concentrated", "auction_bucket_breadth", "auction_bucket_fade", "auction_bucket_watch"),
        depends_on=("stock_microstructure",),
    ),
    LocalStrategySpec(
        node_id="hot_plate_context",
        layer="theme_context",
        responsibility="describe hot-plate continuity and day-over-day pressure from today's and yesterday's hot board facts",
        not_responsible_for=("intraday stock timing", "declaring a new theme confirmed"),
        required_inputs=("hot_plate_today", "hot_plate_yesterday", "plate_migration", "change_pct", "net_inflow_yi", "rank"),
        output_states=("hot_plate_continuation", "hot_plate_new_attack", "hot_plate_fading", "hot_plate_watch"),
    ),
    LocalStrategySpec(
        node_id="yesterday_limit_pool",
        layer="emotion",
        responsibility="summarize yesterday limit-up pool feedback by high/mid/first-board buckets for relay emotion and risk",
        not_responsible_for=("choosing the best stock inside a theme", "theme migration verdict"),
        required_inputs=("is_yest_limit", "lb_days", "open_pct", "current_pct", "auction_amount", "amount_2m", "touched_limit_today"),
        output_states=("yest_limit_relay_ok", "yest_limit_divergent", "yest_limit_negative", "yest_limit_watch"),
        depends_on=("stock_microstructure", "high_focus"),
    ),
    LocalStrategySpec(
        node_id="theme_internal",
        layer="theme",
        responsibility="classify one theme's internal behavior: front-row carry, middle spread, repair, distribution, or fake amount",
        not_responsible_for=("choosing between themes", "global market script", "stock final action"),
        required_inputs=("yest_hot_rank", "yest_limit_count", "auction_amount", "amount_2m_sum", "front_row_2m_pass_count", "expansion_count", "high_open_fail_count", "low_open_repair_count"),
        output_states=("theme_extension", "theme_rotation", "theme_repair", "theme_distribution", "theme_fakeout_watch", "theme_watch"),
        depends_on=("stock_microstructure", "stock_profile", "high_focus"),
    ),
    LocalStrategySpec(
        node_id="theme_opening_validation",
        layer="theme",
        responsibility="convert OpeningValidationBundle into local theme evidence: confirmed, falsified, watch, tradable level, and validation reason",
        not_responsible_for=("rebuilding opening validation", "stock-level entry timing"),
        required_inputs=("opening_validation_bundle", "validation_state", "tradable_level", "front_row_confirmed", "mid_follow_confirmed"),
        output_states=("opening_confirmed", "opening_probe", "opening_falsified", "opening_watch"),
        depends_on=("theme_internal", "auction_bucket_local"),
    ),
    LocalStrategySpec(
        node_id="theme_relative",
        layer="theme_relative",
        responsibility="compare themes horizontally and classify leading, rising, fading, fake rotation, and migration paths",
        not_responsible_for=("stock-level entry timing", "order placement", "position sizing"),
        required_inputs=("theme_internal signals", "migrating_in_plates", "migrating_out_plates"),
        output_states=("relative_leading", "relative_rotation", "relative_migrating_in", "relative_fading", "relative_fake_rotation", "relative_watch"),
        depends_on=("theme_internal", "hot_plate_context", "theme_opening_validation", "theme_high_focus_impact", "high_focus"),
    ),
    LocalStrategySpec(
        node_id="theme_stock_bridge",
        layer="stock_theme_bridge",
        responsibility="link local theme path signals with stock micro/profile/capital evidence and label alignment, wait-trigger, stock-only, theme-risk, or fakeout risk",
        not_responsible_for=("final buy decision", "position sizing", "market regime override"),
        required_inputs=("theme_relative signals", "stock_microstructure signals", "stock_profile signals", "stock_capital_profile signals", "weak_to_strong_repair signals", "stock theme set"),
        output_states=("theme_stock_aligned", "theme_stock_pressure_repair", "theme_stock_wait_trigger", "theme_stock_stock_only", "theme_stock_theme_risk", "theme_stock_fakeout_risk"),
        depends_on=("theme_relative", "stock_microstructure", "stock_profile", "stock_capital_profile", "weak_to_strong_repair"),
    ),
)


LOCAL_STRATEGY_SPEC_MAP: dict[str, LocalStrategySpec] = {item.node_id: item for item in LOCAL_STRATEGY_SPECS}
