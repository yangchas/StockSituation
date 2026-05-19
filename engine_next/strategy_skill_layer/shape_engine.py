from __future__ import annotations

from dataclasses import replace

from engine_next.domain.models import IntradayContext, StockSelectionContext, StockStateSnapshot, ThemeSelectionContext
from engine_next.runtime.theme_name_resolver import resolve_primary_theme_name
from engine_next.strategy_skill_layer.theme_selection_context_factory import (
    build_theme_selection_context,
    resolve_theme_trade_conclusion,
)
from engine_next.strategy_skill_layer.theme_trade_facts import build_theme_trade_fact_map
from engine_next.strategy_skill_layer.theme_trade_labels import classify_theme_trade_label


def _safe_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _clamp_score(value: float, minimum: float = 0.0, maximum: float = 10.0) -> float:
    return max(minimum, min(maximum, value))


def _neutral_shape_score(value: object, default: float = 5.0) -> float:
    if value in (None, ""):
        return default
    score = _safe_float(value)
    return _clamp_score(score)


def _is_hot_rank_active(hot_rank: int, hot_heat: float) -> bool:
    return hot_rank <= 50 or hot_heat >= 50_000


def _rank_pct_desc(value: float, ordered_values: list[float]) -> float:
    if not ordered_values:
        return 1.0
    try:
        index = ordered_values.index(value)
    except ValueError:
        index = len(ordered_values) - 1
    if len(ordered_values) == 1:
        return 0.0
    return round(index / (len(ordered_values) - 1), 4)


def _daily_height_bucket(snapshot: StockStateSnapshot) -> str:
    profit_ratio = _safe_float(getattr(snapshot, "profit_ratio", 0.0))
    bias_20 = _safe_float(getattr(snapshot, "bias_20", 0.0))
    overheat_risk = _safe_float(getattr(snapshot, "shape_overheat_risk", 0.0))
    if (
        snapshot.lb_days >= 2
        or profit_ratio >= 0.75
        or bias_20 >= 0.18
        or overheat_risk >= 7.0
    ):
        return "high"
    if (
        snapshot.lb_days == 0
        and profit_ratio <= 0.45
        and bias_20 <= 0.08
        and overheat_risk < 6.0
    ):
        return "low"
    return "mid"


def _factor_edge_score(snapshot: StockStateSnapshot) -> float:
    score = 5.0
    if snapshot.profit_ratio <= 0.20:
        score += 1.2
    elif snapshot.profit_ratio <= 0.35:
        score += 0.6
    elif snapshot.profit_ratio >= 0.82:
        score -= 1.0
    elif snapshot.profit_ratio >= 0.68:
        score -= 0.5

    if snapshot.concentration <= 0.20:
        score += 1.0
    elif snapshot.concentration <= 0.30:
        score += 0.4
    elif snapshot.concentration >= 0.60:
        score -= 0.9
    elif snapshot.concentration >= 0.45:
        score -= 0.4

    if -0.04 <= snapshot.bias_20 <= 0.12:
        score += 0.9
    elif snapshot.bias_20 > 0.18:
        score -= 1.2
    elif snapshot.bias_20 > 0.12:
        score -= 0.5
    elif snapshot.bias_20 < -0.12:
        score -= 0.7

    if 45.0 <= snapshot.rsi_6 <= 72.0:
        score += 0.8
    elif 72.0 < snapshot.rsi_6 <= 82.0:
        score += 0.2
    elif snapshot.rsi_6 > 86.0:
        score -= 1.0
    elif snapshot.rsi_6 < 24.0:
        score -= 0.6
    return _clamp_score(score)


def _dde_flow_score(snapshot: StockStateSnapshot) -> float:
    score = 5.0
    ddje = float(snapshot.ddje or 0.0)
    ddx = float(snapshot.ddx or 0.0)
    ddy = float(snapshot.ddy or 0.0)
    ddz = float(snapshot.ddz or 0.0)

    if ddje >= 80_000_000:
        score += 2.0
    elif ddje >= 30_000_000:
        score += 1.2
    elif ddje > 0:
        score += 0.5
    elif ddje <= -80_000_000:
        score -= 2.2
    elif ddje <= -30_000_000:
        score -= 1.4
    elif ddje < 0:
        score -= 0.6

    if ddx >= 0.60:
        score += 1.6
    elif ddx >= 0.20:
        score += 0.9
    elif ddx > 0:
        score += 0.4
    elif ddx <= -0.60:
        score -= 1.8
    elif ddx <= -0.20:
        score -= 1.0
    elif ddx < 0:
        score -= 0.4

    if ddz >= 8.0:
        score += 1.2
    elif ddz >= 3.0:
        score += 0.6
    elif ddz <= -8.0:
        score -= 1.4
    elif ddz <= -3.0:
        score -= 0.7

    if ddy > 0 and ddx > 0:
        score += 0.4
    elif ddy < 0 and ddx < 0:
        score -= 0.5
    return _clamp_score(score)


def _auction_open_bucket(snapshot: StockStateSnapshot) -> str:
    open_pct = _safe_float(getattr(snapshot, "open_pct", 0.0))
    if open_pct <= -0.03:
        return "deep_low_open"
    if open_pct < 0.0:
        return "low_open"
    if open_pct <= 0.02:
        return "flat_open"
    if open_pct <= 0.06:
        return "healthy_high_open"
    if open_pct < 0.095:
        return "overheat_high_open"
    return "near_limit_open"


def _open_follow_state(snapshot: StockStateSnapshot) -> str:
    open_pct = _safe_float(getattr(snapshot, "open_pct", 0.0))
    current_pct = _safe_float(getattr(snapshot, "current_pct", 0.0))
    auction_amount = max(_safe_float(getattr(snapshot, "auction_amount", 0.0)), 0.0)
    amount_2m = max(_safe_float(getattr(snapshot, "amount_2m", 0.0)), 0.0)
    speed_1m = _safe_float(getattr(snapshot, "speed_1m", 0.0))
    delta = current_pct - open_pct
    amount_ratio_2m = (amount_2m / auction_amount) if auction_amount > 0 else 0.0

    if (
        open_pct <= 0.01
        and current_pct >= max(0.03, open_pct + 0.02)
        and amount_2m >= max(auction_amount, 20_000_000)
        and speed_1m > 0
    ):
        return "repair_strength"
    if (
        delta >= -0.005
        and (amount_ratio_2m >= 1.0 or amount_2m >= 25_000_000 or speed_1m > 0.008)
    ):
        return "confirmed"
    if delta <= -0.03 or (amount_ratio_2m < 0.75 and speed_1m <= 0):
        return "faded"
    return "weak_follow"


def resolve_theme_name(snapshot: StockStateSnapshot) -> str:
    return resolve_primary_theme_name(snapshot)


def should_evaluate_stock_shape_fast(snapshot: StockStateSnapshot) -> bool:
    return bool(
        snapshot.is_yest_limit
        or snapshot.lb_days >= 1
        or snapshot.t2_lb_days >= 1
        or snapshot.leader_rank_in_theme <= 5
        or (snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank <= 100)
        or snapshot.ths_hot_heat >= 30_000
        or snapshot.auction_amount >= 20_000_000
        or snapshot.amount_2m >= 20_000_000
        or snapshot.amount_day_yi >= 8
        or snapshot.vol_ratio >= 1.8
        or snapshot.current_pct >= 0.03
        or snapshot.current_pct <= -0.04
        or snapshot.structure_score_base >= 6.0
        or snapshot.theme_core_base >= 5.5
    )


def filter_shape_eval_scope(
    snapshots: tuple[StockStateSnapshot, ...] | list[StockStateSnapshot],
    *,
    max_count: int | None = None,
) -> tuple[str, ...]:
    ranked = sorted(
        (snapshot for snapshot in snapshots if should_evaluate_stock_shape_fast(snapshot)),
        key=lambda snapshot: (
            snapshot.theme_core_base,
            snapshot.structure_score_base,
            snapshot.auction_amount,
            snapshot.amount_2m,
            snapshot.amount_day_yi,
            snapshot.vol_ratio,
            -(snapshot.ths_hot_rank or 999),
            snapshot.ths_hot_heat,
            snapshot.current_pct,
            -snapshot.leader_rank_in_theme,
            snapshot.is_yest_limit,
            snapshot.lb_days,
        ),
        reverse=True,
    )
    symbols = tuple(snapshot.symbol for snapshot in ranked if snapshot.symbol)
    if max_count is not None and max_count > 0:
        return symbols[:max_count]
    return symbols


def market_regime_from_context(context: IntradayContext) -> str:
    summary = context.market_summary
    sentiment = _safe_float(getattr(summary, "sentiment_score", 0.0))
    promotion_rate = _safe_float(getattr(summary, "promotion_rate", 0.0))
    red_open_rate = _safe_float(getattr(summary, "red_open_rate", 0.0))
    battle_status = str(getattr(summary, "battle_status", "") or "").lower()
    if sentiment >= 6.5 or battle_status in {"bullish", "attack"} or promotion_rate >= 0.32:
        return "attack"
    if sentiment <= 4.2 or battle_status in {"bearish", "defense", "frozen"} or red_open_rate <= 0.42:
        return "defense"
    return "neutral"


def build_theme_context_map(
    context: IntradayContext,
    snapshots: tuple[StockStateSnapshot, ...],
) -> dict[str, ThemeSelectionContext]:
    market_regime = market_regime_from_context(context)
    trade_fact_map = build_theme_trade_fact_map(context, snapshots, resolve_theme_name=resolve_theme_name)
    grouped: dict[str, list[StockStateSnapshot]] = {}
    for snapshot in snapshots:
        plate_name = resolve_theme_name(snapshot)
        if not plate_name:
            continue
        grouped.setdefault(plate_name, []).append(snapshot)

    context_map: dict[str, ThemeSelectionContext] = {}
    raw_plate_scores: dict[str, dict[str, float | str]] = {}
    for plate_name, plate_snapshots in grouped.items():
        trade_fact = trade_fact_map.get(plate_name)
        hot_row = context.hot_plate_map.get(plate_name, {})
        yest_hot_row = context.yesterday_hot_plate_map.get(plate_name, {})
        yest_limit_count = sum(1 for snapshot in plate_snapshots if snapshot.is_yest_limit)
        auction_symbol_count = sum(1 for snapshot in plate_snapshots if snapshot.auction_amount > 0)
        red_count = sum(1 for snapshot in plate_snapshots if snapshot.open_pct > 0)
        avg_open_pct = (
            sum(snapshot.open_pct for snapshot in plate_snapshots) / len(plate_snapshots)
            if plate_snapshots
            else 0.0
        )
        auction_amount = sum(snapshot.auction_amount for snapshot in plate_snapshots)
        leader_count = sum(1 for snapshot in plate_snapshots if snapshot.leader_rank_in_theme <= 3)
        front_row_snapshots = [snapshot for snapshot in plate_snapshots if snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1]
        front_row_count = len(front_row_snapshots)
        front_row_red_count = sum(1 for snapshot in front_row_snapshots if snapshot.open_pct > 0)
        front_row_avg_open_pct = (
            sum(snapshot.open_pct for snapshot in front_row_snapshots) / front_row_count
            if front_row_count
            else 0.0
        )
        hot_rank = _safe_int(hot_row.get("rank", 999))
        yest_hot_rank = _safe_int(yest_hot_row.get("rank", 999))
        strength = _safe_float(hot_row.get("strength", hot_row.get("score", 0.0)))
        change_pct = _safe_float(hot_row.get("change_pct", hot_row.get("pct_chg", 0.0)))
        net_inflow = _safe_float(hot_row.get("net_inflow_yi", hot_row.get("net_amount", 0.0)))
        strength_score = min(max(strength, 0.0) / 800.0, 2.5)
        positive_flow_score = min(max(net_inflow, 0.0), 20.0) / 10.0
        negative_flow_score = min(abs(min(net_inflow, 0.0)), 20.0) / 10.0
        positive_change_score = min(max(change_pct, 0.0), 4.0) / 2.0
        negative_change_score = min(abs(min(change_pct, 0.0)), 4.0) / 2.0
        trade_label = classify_theme_trade_label(trade_fact) if trade_fact is not None else "unknown"
        persistence_avg = (
            sum(snapshot.plate_persistence_score for snapshot in plate_snapshots) / len(plate_snapshots)
            if plate_snapshots
            else 0.0
        )
        hot_days_max = max((snapshot.hot_plate_days for snapshot in plate_snapshots), default=0)

        e_score = 0.0
        if yest_hot_rank <= 10:
            e_score += 4.0
        elif yest_hot_rank <= 20:
            e_score += 2.0
        e_score += min(yest_limit_count, 3) * 1.2
        e_score += min(max(persistence_avg, 0.0), 2.0) * 1.2
        if hot_days_max >= 2:
            e_score += 1.5
        e_score += strength_score * 0.8
        e_score += positive_flow_score * 0.6
        e_score -= negative_flow_score * 0.4
        if trade_label == "old_mainline":
            e_score += 0.5

        a_score = 0.0
        a_score += min(auction_amount / 100_000_000, 4.0)
        a_score += min(auction_symbol_count, 4) * 0.8
        a_score += min(leader_count, 3) * 0.7
        if red_count >= max(2, len(plate_snapshots) // 2):
            a_score += 1.0
        if avg_open_pct > 0.01:
            a_score += 1.0
        a_score += strength_score * 0.9
        a_score += positive_flow_score * 0.8
        a_score += positive_change_score * 0.7
        if net_inflow > 0 and change_pct <= 0:
            a_score += 0.3
        if trade_label == "switch_candidate":
            a_score += 0.4

        x_score = 0.0
        if avg_open_pct < -0.01:
            x_score += 2.0
        if red_count * 2 < len(plate_snapshots):
            x_score += 1.5
        if front_row_count >= 2 and front_row_red_count * 2 < front_row_count:
            x_score += 1.4
        if auction_amount >= 100_000_000 and change_pct <= 0:
            x_score += 1.5
        if front_row_count >= 1 and front_row_avg_open_pct < -0.01:
            x_score += 1.2
        if hot_rank <= 10 and change_pct < 0:
            x_score += 1.0
        x_score += negative_flow_score * 0.9
        x_score += negative_change_score * 0.8
        if net_inflow < 0 and change_pct > 0:
            x_score += 0.9
        if trade_label == "high_event":
            x_score += 0.6
        elif trade_label == "independent_hug":
            x_score += 0.8

        fakeout_level = "low"
        if (
            auction_amount >= 100_000_000
            and change_pct <= 0
            and red_count * 2 < len(plate_snapshots)
            and (front_row_count == 0 or front_row_red_count * 2 < front_row_count)
        ):
            fakeout_level = "high"
        elif auction_amount >= 50_000_000 and (change_pct <= 0 or front_row_avg_open_pct <= 0):
            fakeout_level = "medium"
        if trade_label == "high_event" and leader_count <= 1:
            fakeout_level = "high"
        elif trade_label == "independent_hug" and fakeout_level == "low":
            fakeout_level = "medium"

        cohesion_level = "weak"
        if (
            leader_count >= 2
            and auction_symbol_count >= 3
            and red_count * 2 >= len(plate_snapshots)
            and (front_row_count == 0 or front_row_red_count * 2 >= front_row_count)
        ):
            cohesion_level = "strong"
        elif leader_count >= 1 and auction_symbol_count >= 2:
            cohesion_level = "medium"

        if market_regime == "defense":
            if fakeout_level == "medium":
                fakeout_level = "high"
            x_score += 0.6
        elif market_regime == "attack":
            a_score += 0.4

        tradable = (
            (e_score >= 4.5 or a_score >= 4.0 or hot_rank <= 15)
            and x_score < 4.5
            and fakeout_level != "high"
        )
        if trade_label == "high_event" and leader_count <= 1:
            tradable = False
        bias_action = "observe_only"
        if tradable and cohesion_level == "strong":
            bias_action = "front_row_confirm"
        elif tradable:
            bias_action = "small_probe_only"
        if trade_label == "high_event":
            bias_action = "observe_only"
        elif trade_label == "independent_hug" and tradable:
            bias_action = "front_row_watch"

        trade_conclusion = resolve_theme_trade_conclusion(
            theme_trade_label=trade_label,
            open_confirm_state="unknown",
            fakeout_level=fakeout_level,
            leader_count=leader_count,
            yest_limit_count=yest_limit_count,
        )
        breadth_score = _clamp_score(
            (
                min(red_count / max(len(plate_snapshots), 1), 1.0) * 4.0
                + min(front_row_red_count / max(front_row_count, 1), 1.0) * 3.0
                + min(leader_count, 3) * 0.9
            )
        )
        follow_through_score = _clamp_score(
            min(auction_amount / 120_000_000, 4.0)
            + min(front_row_count, 3) * 1.0
            + min(front_row_red_count, 3) * 0.8
            + max(front_row_avg_open_pct, 0.0) * 25.0
        )
        resistance_score = _clamp_score(
            x_score * 0.9
            + max(front_row_count - front_row_red_count, 0) * 0.8
            + (1.5 if fakeout_level == "high" else 0.8 if fakeout_level == "medium" else 0.0)
            + max(hot_days_max - 1, 0) * 0.5
        )
        raw_strength_score = float(a_score * 0.55 + e_score * 0.30 + breadth_score * 0.15 - x_score * 0.20)
        raw_delta_score = float(a_score - x_score)
        raw_plate_scores[plate_name] = {
            "raw_strength": raw_strength_score,
            "raw_delta": raw_delta_score,
            "breadth_score": breadth_score,
            "follow_through_score": follow_through_score,
            "resistance_score": resistance_score,
        }
        context_map[plate_name] = build_theme_selection_context(
            plate_name=plate_name,
            e_score=e_score,
            a_score=a_score,
            x_score=x_score,
            market_regime=market_regime,
            theme_trade_label=trade_label,
            trade_conclusion=trade_conclusion,
            open_confirm_state="unknown",
            fakeout_level=fakeout_level,
            cohesion_level=cohesion_level,
            tradable=tradable,
            bias_action=bias_action,
            leader_count=leader_count,
            yest_limit_count=yest_limit_count,
            notes=(
                f"trade_label={trade_label}",
                f"trade_conclusion={trade_conclusion}",
                f"hot_rank={hot_rank}",
                f"yest_hot_rank={yest_hot_rank}",
                f"strength={strength:.1f}",
                f"net_inflow={net_inflow:.2f}yi",
                f"auction={auction_amount / 1e8:.2f}yi",
                f"front_row_red={front_row_red_count}/{front_row_count}",
            ),
        )
    ordered_strength = sorted(
        [float(item["raw_strength"]) for item in raw_plate_scores.values()],
        reverse=True,
    )
    ordered_delta = sorted(
        [float(item["raw_delta"]) for item in raw_plate_scores.values()],
        reverse=True,
    )
    for plate_name, theme_context in tuple(context_map.items()):
        raw = raw_plate_scores.get(plate_name, {})
        strength_rank_pct = _rank_pct_desc(float(raw.get("raw_strength", 0.0) or 0.0), ordered_strength)
        delta_rank_pct = _rank_pct_desc(float(raw.get("raw_delta", 0.0) or 0.0), ordered_delta)
        breadth_score = float(raw.get("breadth_score", 0.0) or 0.0)
        follow_score = float(raw.get("follow_through_score", 0.0) or 0.0)
        resistance_score = float(raw.get("resistance_score", 0.0) or 0.0)
        if strength_rank_pct <= 0.2 and follow_score >= 5.0:
            plate_role = "leader"
        elif delta_rank_pct <= 0.3 and strength_rank_pct <= 0.5:
            plate_role = "chaser"
        elif strength_rank_pct >= 0.7 and delta_rank_pct <= 0.4:
            plate_role = "laggard"
        elif strength_rank_pct <= 0.45 and resistance_score <= 4.8 and breadth_score >= 4.5:
            plate_role = "defensive_holder"
        else:
            plate_role = "neutral"
        if delta_rank_pct <= 0.25 and resistance_score <= 5.2:
            rotation_bias = "inflow"
        elif strength_rank_pct >= 0.7 and delta_rank_pct <= 0.4 and resistance_score <= 5.8:
            rotation_bias = "repair"
        elif delta_rank_pct >= 0.7 or resistance_score >= 6.0:
            rotation_bias = "outflow"
        else:
            rotation_bias = "neutral"
        context_map[plate_name] = replace(
            theme_context,
            plate_strength_rank_pct=strength_rank_pct,
            plate_delta_rank_pct=delta_rank_pct,
            plate_breadth_score=round(breadth_score, 2),
            plate_follow_through_score=round(follow_score, 2),
            plate_resistance_score=round(resistance_score, 2),
            plate_role=plate_role,
            rotation_bias=rotation_bias,
            notes=tuple(
                list(theme_context.notes)
                + [
                    f"plate_strength_rank_pct={strength_rank_pct:.3f}",
                    f"plate_delta_rank_pct={delta_rank_pct:.3f}",
                    f"plate_role={plate_role}",
                    f"rotation_bias={rotation_bias}",
                ]
            ),
        )
    return context_map


def build_stock_selection_context(
    snapshot: StockStateSnapshot,
    theme_context: ThemeSelectionContext | None,
) -> StockSelectionContext:
    plate_name = resolve_theme_name(snapshot)
    auction_open_bucket = _auction_open_bucket(snapshot)
    open_follow_state = _open_follow_state(snapshot)
    hot_rank = snapshot.ths_hot_rank if snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank > 0 else 999
    hot_heat = max(float(snapshot.ths_hot_heat or 0.0), 0.0)
    is_true_leader = snapshot.leader_rank_in_theme <= 1 and snapshot.lb_days >= 1
    is_front_row = snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1
    factor_edge_score = _factor_edge_score(snapshot)
    dde_flow_score = _dde_flow_score(snapshot)
    leader_bucket = "follower"
    if is_true_leader:
        leader_bucket = "true_leader"
    elif is_front_row:
        leader_bucket = "front_row"
    market_regime = theme_context.market_regime if theme_context is not None else "neutral"
    heat_flow_multiplier = 1.0
    turnover_quality_multiplier = 1.0
    if market_regime == "defense":
        heat_flow_multiplier = 0.72
        turnover_quality_multiplier = 1.08
    elif market_regime == "attack":
        heat_flow_multiplier = 1.10
        turnover_quality_multiplier = 1.02

    heat_flow_score = 0.0
    if hot_rank <= 10:
        heat_flow_score += 2.2
    elif hot_rank <= 30:
        heat_flow_score += 1.6
    elif hot_rank <= 50:
        heat_flow_score += 1.1
    elif hot_rank <= 100:
        heat_flow_score += 0.4
    if hot_heat >= 100_000:
        heat_flow_score += 2.0
    elif hot_heat >= 50_000:
        heat_flow_score += 1.4
    elif hot_heat >= 20_000:
        heat_flow_score += 0.7
    if snapshot.vol_ratio >= 3.0:
        heat_flow_score += 1.2
    elif snapshot.vol_ratio >= 2.0:
        heat_flow_score += 0.8
    if 8 <= snapshot.amount_day_yi <= 80:
        heat_flow_score += 0.8
    elif snapshot.amount_day_yi > 80:
        heat_flow_score += 0.2
    if snapshot.current_pct >= 0.02:
        heat_flow_score += 0.6
    elif snapshot.current_pct <= -0.03:
        heat_flow_score -= 0.6
    heat_flow_score = _clamp_score(heat_flow_score * heat_flow_multiplier)

    turnover_quality_score = 0.0
    if 8 <= snapshot.amount_day_yi <= 50:
        turnover_quality_score += 2.2
    elif 5 <= snapshot.amount_day_yi < 8:
        turnover_quality_score += 1.0
    elif snapshot.amount_day_yi > 80:
        turnover_quality_score -= 0.4
    if snapshot.vol_ratio >= 2.2:
        turnover_quality_score += 1.4
    elif snapshot.vol_ratio >= 1.5:
        turnover_quality_score += 0.8
    if snapshot.auction_amount >= 20_000_000:
        turnover_quality_score += 0.8
    if snapshot.amount_2m >= 20_000_000:
        turnover_quality_score += 0.8
    if 40.0 <= snapshot.rsi_6 <= 78.0:
        turnover_quality_score += 0.6
    turnover_quality_score = _clamp_score(turnover_quality_score * turnover_quality_multiplier)

    activity_score = 0.0
    if hot_rank <= 10:
        activity_score += 2.4
    elif hot_rank <= 30:
        activity_score += 1.7
    elif hot_rank <= 100:
        activity_score += 0.7
    if hot_heat >= 100_000:
        activity_score += 2.2
    elif hot_heat >= 30_000:
        activity_score += 1.4
    if snapshot.is_yest_limit and (
        snapshot.auction_amount >= 10_000_000
        or snapshot.amount_2m >= 10_000_000
        or hot_heat >= 20_000
    ):
        activity_score += 0.4
    if snapshot.lb_days >= 1 and (
        snapshot.amount_day_yi >= 5
        or snapshot.auction_amount >= 10_000_000
        or snapshot.amount_2m >= 10_000_000
    ):
        activity_score += min(snapshot.lb_days, 2) * 0.2
    if snapshot.t2_lb_days >= 1:
        activity_score += 0.4
    if snapshot.leader_rank_in_theme <= 3:
        activity_score += 2.0
    elif snapshot.leader_rank_in_theme <= 5:
        activity_score += 1.0
    if snapshot.auction_amount >= 50_000_000:
        activity_score += 1.8
    elif snapshot.auction_amount >= 20_000_000:
        activity_score += 1.1
    if snapshot.amount_2m >= 50_000_000:
        activity_score += 1.8
    elif snapshot.amount_2m >= 20_000_000:
        activity_score += 1.2
    if snapshot.vol_ratio >= 2.5:
        activity_score += 1.4
    elif snapshot.vol_ratio >= 1.8:
        activity_score += 0.9
    if snapshot.amount_day_yi >= 20:
        activity_score += 1.4
    elif snapshot.amount_day_yi >= 8:
        activity_score += 0.8
    if theme_context is not None and theme_context.tradable:
        activity_score += 1.0
    activity_score += min(heat_flow_score, 4.0) * 0.45
    activity_score += min(turnover_quality_score, 4.0) * 0.25
    if (
        snapshot.lb_days >= 1
        and hot_rank > 100
        and hot_heat < 20_000
        and snapshot.leader_rank_in_theme > 5
        and snapshot.auction_amount < 10_000_000
        and snapshot.amount_2m < 10_000_000
    ):
        activity_score -= 1.2
    activity_score = min(activity_score, 10.0)
    is_active_pool = bool(
        hot_rank <= 100
        or hot_heat >= 30_000
        or snapshot.leader_rank_in_theme <= 3
        or snapshot.auction_amount >= 20_000_000
        or snapshot.amount_2m >= 20_000_000
        or snapshot.vol_ratio >= 2.0
        or snapshot.amount_day_yi >= 10
        or _is_hot_rank_active(hot_rank, hot_heat)
        or (
            snapshot.lb_days >= 1
            and (
                snapshot.amount_day_yi >= 8
                or snapshot.auction_amount >= 15_000_000
                or snapshot.amount_2m >= 15_000_000
            )
        )
        or (
            snapshot.is_yest_limit
            and (
                snapshot.auction_amount >= 15_000_000
                or snapshot.amount_2m >= 15_000_000
                or hot_rank <= 80
            )
        )
    )

    theme_core_score = float(snapshot.theme_core_base or 0.0)
    if is_true_leader:
        theme_core_score += 4.0
    elif is_front_row:
        theme_core_score += 2.6
    if snapshot.leader_rank_in_theme <= 2:
        theme_core_score += 1.8
    elif snapshot.leader_rank_in_theme <= 5:
        theme_core_score += 0.9
    if hot_rank <= 30:
        theme_core_score += 1.2
    elif hot_rank <= 100:
        theme_core_score += 0.6
    if snapshot.auction_amount >= 30_000_000:
        theme_core_score += 0.9
    if snapshot.amount_2m >= 20_000_000:
        theme_core_score += 0.9
    if theme_context is not None and theme_context.cohesion_level == "strong":
        theme_core_score += 0.8
    if theme_context is not None and theme_context.tradable:
        theme_core_score += 0.6
        if float(theme_context.a_score or 0.0) >= 6.0:
            theme_core_score += 0.8
        elif float(theme_context.a_score or 0.0) >= 4.5:
            theme_core_score += 0.3
    if theme_context is not None and float(theme_context.x_score or 0.0) >= 6.0:
        theme_core_score -= 1.2
    elif theme_context is not None and float(theme_context.x_score or 0.0) >= 4.5:
        theme_core_score -= 0.5
    if is_front_row and theme_context is not None and theme_context.tradable:
        theme_core_score += 0.4
    theme_core_score += min(heat_flow_score, 4.0) * 0.22
    theme_core_score += min(turnover_quality_score, 4.0) * 0.16
    theme_core_score += (factor_edge_score - 5.0) * 0.18
    theme_core_score += (dde_flow_score - 5.0) * 0.10
    if (
        snapshot.lb_days >= 1
        and snapshot.leader_rank_in_theme > 5
        and hot_rank > 100
        and snapshot.auction_amount < 12_000_000
        and snapshot.amount_2m < 12_000_000
    ):
        theme_core_score -= 1.2
    theme_core_score = min(theme_core_score, 10.0)

    if snapshot.t2_lb_days >= 1 and snapshot.t2_pct < -0.05 and snapshot.current_pct > 0:
        kline_pattern = "pullback_repair"
        kline_score = 6.5
    elif (
        snapshot.open_pct < 0
        and snapshot.current_pct > max(0.01, abs(snapshot.open_pct) * 0.5)
        and snapshot.amount_2m >= max(snapshot.auction_amount, 15_000_000)
        and snapshot.speed_1m > 0.005
    ):
        kline_pattern = "low_open_strength"
        kline_score = 8.0
    elif (
        snapshot.lb_days == 0
        and snapshot.current_pct > 0.03
        and snapshot.vol_ratio >= 1.8
        and snapshot.amount_day_yi >= 8
        and snapshot.bias_20 > -0.03
    ):
        kline_pattern = "platform_breakout"
        kline_score = 8.2
    elif (
        snapshot.t2_lb_days >= 1
        and -0.08 <= snapshot.t2_pct <= -0.02
        and snapshot.current_pct > 0.02
        and snapshot.speed_1m > 0
    ):
        kline_pattern = "n_rebound"
        kline_score = 7.2
    elif (
        snapshot.open_pct > 0.04
        and snapshot.current_pct <= max(snapshot.open_pct - 0.03, 0.0)
        and snapshot.amount_2m >= max(snapshot.auction_amount, 20_000_000)
    ):
        kline_pattern = "high_open_then_weak"
        kline_score = 2.2
    elif (
        snapshot.amount_2m >= max(snapshot.auction_amount * 1.2, 25_000_000)
        and snapshot.speed_1m <= 0.002
        and snapshot.current_pct <= 0.02
    ):
        kline_pattern = "volume_up_price_flat"
        kline_score = 2.8
    elif snapshot.lb_days >= 2 and snapshot.current_pct > 0.03:
        kline_pattern = "breakout"
        kline_score = 7.5
    elif snapshot.lb_days >= 3 and snapshot.current_pct < 0.03:
        kline_pattern = "high_divergence"
        kline_score = 3.0
    elif snapshot.touched_limit_today and not snapshot.is_locked:
        kline_pattern = "explosive_failed_board"
        kline_score = 2.5
    else:
        kline_pattern = "range_consolidation"
        kline_score = 5.0

    if kline_pattern == "platform_breakout":
        if auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
            kline_score -= 1.0
        elif auction_open_bucket == "flat_open":
            kline_score += 0.4
    elif kline_pattern == "breakout":
        if auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
            kline_score -= 0.8
        elif auction_open_bucket in {"flat_open", "healthy_high_open"}:
            kline_score += 0.3
    elif kline_pattern in {"pullback_repair", "low_open_strength", "n_rebound"}:
        if auction_open_bucket in {"deep_low_open", "low_open", "flat_open"}:
            kline_score += 0.4
        elif auction_open_bucket == "near_limit_open":
            kline_score -= 0.8
    elif kline_pattern in {"high_open_then_weak", "volume_up_price_flat", "explosive_failed_board"}:
        if auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
            kline_score -= 0.6

    platform_ready = _neutral_shape_score(snapshot.shape_platform_ready)
    breakout_ready = _neutral_shape_score(snapshot.shape_breakout_ready)
    repair_ready = _neutral_shape_score(snapshot.shape_repair_ready)
    overheat_risk = _neutral_shape_score(snapshot.shape_overheat_risk)
    chip_cleanliness = _neutral_shape_score(snapshot.shape_chip_cleanliness)
    trend_health = _neutral_shape_score(snapshot.shape_trend_health)
    t2_repair_bias = _neutral_shape_score(snapshot.shape_t2_repair_bias)

    if kline_pattern == "platform_breakout":
        kline_score += (platform_ready - 5.0) * 0.24
        kline_score += (breakout_ready - 5.0) * 0.12
        kline_score -= max(overheat_risk - 5.5, 0.0) * 0.28
    elif kline_pattern == "breakout":
        kline_score += (breakout_ready - 5.0) * 0.26
        kline_score += (trend_health - 5.0) * 0.12
        kline_score -= max(overheat_risk - 5.2, 0.0) * 0.32
    elif kline_pattern == "pullback_repair":
        kline_score += (repair_ready - 5.0) * 0.24
        kline_score += (t2_repair_bias - 5.0) * 0.16
        kline_score += (trend_health - 5.0) * 0.08
        kline_score -= max(overheat_risk - 6.5, 0.0) * 0.14
    elif kline_pattern == "n_rebound":
        kline_score += (repair_ready - 5.0) * 0.20
        kline_score += (t2_repair_bias - 5.0) * 0.20
        kline_score += (chip_cleanliness - 5.0) * 0.08
        kline_score -= max(overheat_risk - 6.2, 0.0) * 0.18
    elif kline_pattern == "low_open_strength":
        kline_score += (repair_ready - 5.0) * 0.10
        kline_score += (trend_health - 5.0) * 0.14
        kline_score -= max(overheat_risk - 7.0, 0.0) * 0.12
    elif kline_pattern == "range_consolidation":
        kline_score += (platform_ready - 5.0) * 0.10
        kline_score += (chip_cleanliness - 5.0) * 0.08
        kline_score += (trend_health - 5.0) * 0.06
        kline_score -= max(overheat_risk - 6.0, 0.0) * 0.10
    elif kline_pattern in {"high_open_then_weak", "volume_up_price_flat", "high_divergence", "explosive_failed_board"}:
        kline_score -= max(overheat_risk - 5.0, 0.0) * 0.18

    kline_score = _clamp_score(kline_score)

    chip_score = 5.0
    if snapshot.profit_ratio <= 0.20:
        chip_score += 1.5
    if snapshot.concentration <= 0.20:
        chip_score += 1.5
    if snapshot.bias_20 > -0.05:
        chip_score += 1.0
    if 5 <= snapshot.amount_day_yi <= 40:
        chip_score += 1.0
    chip_score += (chip_cleanliness - 5.0) * 0.22
    chip_score += (factor_edge_score - 5.0) * 0.18
    chip_score -= max(overheat_risk - 6.0, 0.0) * 0.12
    chip_score = _clamp_score(chip_score)

    structure_score = float(snapshot.structure_score_base or 0.0) if snapshot.structure_score_base > 0 else 4.0
    if snapshot.vector_5m > 0.015:
        structure_score += 0.7
    elif snapshot.vector_5m < -0.01:
        structure_score -= 0.7
    if snapshot.amount_day_yi < 3 and snapshot.amount_day_yi > 0:
        structure_score -= 0.8
    elif 8 <= snapshot.amount_day_yi <= 60:
        structure_score += 0.4
    structure_score += (chip_cleanliness - 5.0) * 0.18
    structure_score += (trend_health - 5.0) * 0.24
    structure_score += (platform_ready - 5.0) * 0.08
    structure_score += (factor_edge_score - 5.0) * 0.22
    if kline_pattern in {"pullback_repair", "n_rebound"}:
        structure_score += (repair_ready - 5.0) * 0.10
    structure_score -= max(overheat_risk - 5.0, 0.0) * 0.18
    structure_score = _clamp_score(structure_score)

    auction_score = 0.0
    auction_score += min(snapshot.auction_amount / 50_000_000, 4.0)
    if 0.0 <= snapshot.open_pct <= 0.07:
        auction_score += 2.0
    elif snapshot.open_pct > 0.07:
        auction_score += 0.5
    if snapshot.amount_2m >= snapshot.auction_amount > 0:
        auction_score += 2.0
    if snapshot.speed_1m > 0.01:
        auction_score += 1.5
    if _is_hot_rank_active(hot_rank, hot_heat) and snapshot.open_pct <= 0.07:
        auction_score += 0.6
    auction_score = min(auction_score, 10.0)

    timing_score = 5.0
    if snapshot.speed_1m > 0.01:
        timing_score += 1.5
    if snapshot.amount_2m >= 20_000_000:
        timing_score += 1.0
    if heat_flow_score >= 4.0 and snapshot.current_pct >= 0:
        timing_score += 0.8
    if snapshot.open_pct > 0.08:
        timing_score -= 1.5
    if snapshot.amount_2m < snapshot.auction_amount and snapshot.speed_1m <= 0:
        timing_score -= 1.5
    if open_follow_state == "confirmed":
        timing_score += 0.6
    elif open_follow_state == "repair_strength":
        timing_score += 1.0
    elif open_follow_state == "faded":
        timing_score -= 1.4
    timing_score = max(0.0, min(timing_score, 10.0))

    open_undertake_score = 0.0
    amount_ratio_2m = (snapshot.amount_2m / snapshot.auction_amount) if snapshot.auction_amount > 0 else 0.0
    if amount_ratio_2m >= 1.8:
        open_undertake_score += 3.2
    elif amount_ratio_2m >= 1.25:
        open_undertake_score += 2.4
    elif amount_ratio_2m >= 0.95:
        open_undertake_score += 1.4
    elif snapshot.auction_amount > 0 and amount_ratio_2m < 0.60:
        open_undertake_score -= 1.6
    if snapshot.amount_2m >= 50_000_000:
        open_undertake_score += 2.3
    elif snapshot.amount_2m >= 30_000_000:
        open_undertake_score += 1.5
    elif snapshot.amount_2m >= 15_000_000:
        open_undertake_score += 0.8
    if snapshot.current_pct >= snapshot.open_pct - 0.01:
        open_undertake_score += 1.2
    elif snapshot.current_pct <= snapshot.open_pct - 0.035:
        open_undertake_score -= 1.4
    if snapshot.speed_1m > 0.01:
        open_undertake_score += 1.0
    elif snapshot.speed_1m < 0 and snapshot.amount_2m < snapshot.auction_amount:
        open_undertake_score -= 1.0
    if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
        open_undertake_score += 0.8
    if open_follow_state == "confirmed":
        open_undertake_score += 0.6
    elif open_follow_state == "repair_strength":
        open_undertake_score += 1.0
    elif open_follow_state == "faded":
        open_undertake_score -= 1.2
    open_undertake_score += (dde_flow_score - 5.0) * 0.18
    open_undertake_score = _clamp_score(open_undertake_score)

    theme_tradable = bool(theme_context.tradable) if theme_context is not None else False
    theme_x_score = float(theme_context.x_score) if theme_context is not None else 0.0
    theme_trade_label = theme_context.theme_trade_label if theme_context is not None else "unknown"
    theme_fakeout_level = theme_context.fakeout_level if theme_context is not None else "unknown"
    open_confirm_state = theme_context.open_confirm_state if theme_context is not None else "unknown"
    daily_height_bucket = _daily_height_bucket(snapshot)
    if market_regime == "attack":
        if kline_pattern in {"platform_breakout", "breakout", "n_rebound"}:
            kline_score = min(10.0, kline_score + 0.6)
        elif kline_pattern in {"high_open_then_weak", "volume_up_price_flat"}:
            kline_score = max(0.0, kline_score - 0.2)
    elif market_regime == "defense":
        if kline_pattern in {"low_open_strength", "pullback_repair", "n_rebound"}:
            kline_score = min(10.0, kline_score + 0.6)
        elif kline_pattern in {"platform_breakout", "breakout", "high_open_then_weak", "volume_up_price_flat"}:
            kline_score = max(0.0, kline_score - 0.8)
    shape_quality_score = _clamp_score(
        kline_score * 0.42
        + structure_score * 0.30
        + chip_score * 0.18
        + max(0.0, 10.0 - overheat_risk) * 0.10
    )

    execution_quality_score = _clamp_score(
        auction_score * 0.38
        + timing_score * 0.24
        + open_undertake_score * 0.26
        + turnover_quality_score * 0.22
        + min(heat_flow_score, 8.0) * 0.10
        + (dde_flow_score - 5.0) * 0.18
    )

    quality_gate_count = sum(
        (
            activity_score >= 6.5,
            theme_core_score >= 6.5,
            kline_score >= 6.2,
            structure_score >= 6.0,
            chip_score >= 6.0,
            auction_score >= 6.0,
            open_undertake_score >= 6.0,
            shape_quality_score >= 6.3,
            execution_quality_score >= 6.2,
        )
    )
    if quality_gate_count >= 5:
        quality_gate_bonus = 1.4
    elif quality_gate_count >= 4:
        quality_gate_bonus = 0.5
    elif quality_gate_count <= 2:
        quality_gate_bonus = -1.6
    else:
        quality_gate_bonus = -0.4

    non_hot_front_row_strength = bool(
        hot_rank > 80
        and (
            (
                is_front_row
                and snapshot.amount_2m >= 28_000_000
                and open_undertake_score >= 5.0
                and execution_quality_score >= 5.4
            )
            or (
                snapshot.leader_rank_in_theme <= 3
                and snapshot.amount_2m >= 35_000_000
                and open_undertake_score >= 5.2
                and execution_quality_score >= 5.6
            )
            or (
                snapshot.auction_amount > 0
                and snapshot.amount_2m >= snapshot.auction_amount * 1.3
                and shape_quality_score >= 6.0
                and open_follow_state in {"confirmed", "repair_strength"}
            )
        )
    )

    plain_promotion_penalty = 0.0
    if (
        snapshot.lb_days >= 1
        and not is_true_leader
        and hot_rank > 100
        and hot_heat < 20_000
        and snapshot.leader_rank_in_theme > 5
        and theme_core_score < 6.5
        and activity_score < 6.0
        and structure_score < 6.0
    ):
        plain_promotion_penalty = -2.2
    if (
        not is_true_leader
        and snapshot.lb_days >= 1
        and hot_rank > 80
        and hot_heat < 25_000
        and turnover_quality_score < 5.2
        and shape_quality_score < 5.8
        and not non_hot_front_row_strength
    ):
        plain_promotion_penalty -= 1.6
    if (
        not is_true_leader
        and snapshot.leader_rank_in_theme > 5
        and snapshot.auction_amount < 15_000_000
        and snapshot.amount_2m < 15_000_000
        and execution_quality_score < 5.5
    ):
        plain_promotion_penalty -= 1.2
    if (
        snapshot.lb_days >= 1
        and not is_true_leader
        and hot_rank > 60
        and heat_flow_score < 5.0
        and open_undertake_score < 5.6
        and not non_hot_front_row_strength
    ):
        plain_promotion_penalty -= 2.4
    if (
        snapshot.lb_days >= 1
        and not is_true_leader
        and snapshot.leader_rank_in_theme > 3
        and snapshot.auction_amount < 20_000_000
        and snapshot.amount_2m < 25_000_000
        and execution_quality_score < 6.0
        and not non_hot_front_row_strength
    ):
        plain_promotion_penalty -= 1.8
    if (
        snapshot.lb_days >= 1
        and not is_true_leader
        and kline_pattern in {"high_open_then_weak", "volume_up_price_flat", "explosive_failed_board"}
        and execution_quality_score < 6.0
    ):
        plain_promotion_penalty -= 2.2
    hot_execution_bonus = 0.0
    if hot_rank <= 30 and heat_flow_score >= 5.5 and open_undertake_score >= 5.8:
        hot_execution_bonus += 1.2
    elif hot_rank <= 50 and heat_flow_score >= 5.0 and open_undertake_score >= 5.4:
        hot_execution_bonus += 0.6
    elif non_hot_front_row_strength:
        hot_execution_bonus += 0.9
    total_score = round(
        activity_score * 0.18
        + theme_core_score * 0.14
        + kline_score * 0.18
        + structure_score * 0.16
        + chip_score * 0.12
        + auction_score * 0.08
        + timing_score * 0.06
        + open_undertake_score * 0.14
        + shape_quality_score * 0.16
        + execution_quality_score * 0.14
        + (1.0 if is_active_pool else -1.0)
        + (1.0 if is_front_row else 0.0)
        + (1.0 if theme_tradable else -1.0)
        + quality_gate_bonus
        + plain_promotion_penalty
        + hot_execution_bonus,
        2,
    )

    notes = [
        f"active_pool={int(is_active_pool)}",
        f"activity={activity_score:.1f}",
        f"core={theme_core_score:.1f}",
        f"leader_bucket={leader_bucket}",
        f"kline={kline_pattern}",
        f"auction_open={auction_open_bucket}",
        f"open_follow={open_follow_state}",
        f"structure={structure_score:.1f}",
        f"shape_platform={platform_ready:.1f}",
        f"shape_repair={repair_ready:.1f}",
        f"shape_overheat={overheat_risk:.1f}",
        f"heat_flow={heat_flow_score:.1f}",
        f"turnover_quality={turnover_quality_score:.1f}",
        f"open_undertake={open_undertake_score:.1f}",
        f"shape_quality={shape_quality_score:.1f}",
        f"execution_quality={execution_quality_score:.1f}",
        f"factor_edge={factor_edge_score:.1f}",
        f"dde_flow={dde_flow_score:.1f}",
        f"quality_gates={quality_gate_count}",
        f"theme_tradable={int(theme_tradable)}",
    ]
    if hot_rank < 999:
        notes.append(f"hot_rank={hot_rank}")
    if hot_heat > 0:
        notes.append(f"hot_heat={hot_heat:.0f}")
    if theme_context is not None:
        notes.append(f"theme_fakeout={theme_fakeout_level}")
        notes.append(f"theme_label={theme_trade_label}")
        notes.append(f"theme_bias={theme_context.bias_action}")
        notes.append(f"market_regime={market_regime}")

    return StockSelectionContext(
        symbol=snapshot.symbol,
        plate_name=plate_name,
        theme_trade_label=theme_trade_label,
        hot_rank=hot_rank,
        hot_heat=round(hot_heat, 2),
        is_active_pool=is_active_pool,
        is_true_leader=is_true_leader,
        is_front_row=is_front_row,
        leader_bucket=leader_bucket,
        heat_flow_score=round(heat_flow_score, 2),
        turnover_quality_score=round(turnover_quality_score, 2),
        activity_score=round(activity_score, 2),
        theme_core_score=round(theme_core_score, 2),
        kline_pattern=kline_pattern,
        auction_open_bucket=auction_open_bucket,
        open_follow_state=open_follow_state,
        kline_score=round(kline_score, 2),
        structure_score=round(structure_score, 2),
        chip_score=round(chip_score, 2),
        auction_score=round(auction_score, 2),
        timing_score=round(timing_score, 2),
        open_undertake_score=round(open_undertake_score, 2),
        shape_quality_score=round(shape_quality_score, 2),
        execution_quality_score=round(execution_quality_score, 2),
        theme_tradable=theme_tradable,
        theme_fakeout_level=theme_fakeout_level,
        theme_x_score=round(theme_x_score, 2),
        open_confirm_state=open_confirm_state,
        daily_height_bucket=daily_height_bucket,
        total_score=total_score,
        notes=tuple(notes),
    )
