from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from engine_next.domain.local_strategy_models import (
    LocalMetric,
    LocalSignal,
    LocalStrategyGraph,
    LocalStrategyNodeResult,
)
from engine_next.domain.models import IntradayContext, StockSelectionContext, StockStateSnapshot, ThemeTradeFact
from engine_next.runtime.theme_name_resolver import resolve_theme_names
from engine_next.strategy_skill_layer.auction_plate_buckets import build_auction_plate_bucket_stats
from engine_next.strategy_skill_layer.local_strategy_catalog import LOCAL_STRATEGY_SPEC_MAP
from engine_next.strategy_skill_layer.relative_amount import relative_amount_floor, top_symbols_by_amount
from engine_next.strategy_skill_layer.stock_behavior import classify_opening_entry_behavior, stock_focus_evidence_labels


def _phase_name(context: IntradayContext) -> str:
    return str(getattr(context.phase, "value", context.phase) or "")


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if float(denominator or 0.0) > 0.0 else 0.0


def _primary_theme(snapshot: StockStateSnapshot) -> str:
    names = resolve_theme_names(snapshot)
    if names:
        return names[0]
    return str(snapshot.plate or "")


def _theme_names(snapshot: StockStateSnapshot) -> tuple[str, ...]:
    names = tuple(name for name in resolve_theme_names(snapshot) if name)
    if names:
        return names
    plate = str(snapshot.plate or "")
    return (plate,) if plate else ()


def _signal_action_rank(signal: LocalSignal | None) -> int:
    if signal is None:
        return 9
    if signal.action_hint in {"support", "probe"}:
        return 0
    if signal.action_hint == "watch":
        return 1
    if signal.action_hint in {"avoid", "avoid_chase"}:
        return 2
    return 5


def _best_theme_signal(
    theme_by_name: dict[str, LocalSignal],
    theme_names: tuple[str, ...],
) -> tuple[str, LocalSignal | None]:
    matches = tuple((name, theme_by_name.get(name)) for name in theme_names if name in theme_by_name)
    if not matches:
        return (theme_names[0] if theme_names else "", None)
    return sorted(matches, key=lambda item: (_signal_action_rank(item[1]), item[0]))[0]


def _stock_scope(
    snapshots: tuple[StockStateSnapshot, ...],
    selection_map: dict[str, StockSelectionContext],
    *,
    max_symbols: int,
) -> tuple[StockStateSnapshot, ...]:
    amount_top = top_symbols_by_amount(snapshots, "amount_2m", top_n=max(120, max_symbols // 2))
    auction_top = top_symbols_by_amount(snapshots, "auction_amount", top_n=max(120, max_symbols // 2))
    rows = [
        snapshot
        for snapshot in snapshots
        if snapshot.symbol in selection_map
        or snapshot.symbol in amount_top
        or snapshot.symbol in auction_top
        or snapshot.lb_days >= 1
        or snapshot.is_yest_limit
        or snapshot.leader_rank_in_theme <= 3
    ]
    rows.sort(
        key=lambda snapshot: (
            snapshot.symbol not in selection_map,
            int(snapshot.leader_rank_in_theme or 999),
            -int(snapshot.lb_days or 0),
            -float(snapshot.amount_2m or 0.0),
            -float(snapshot.auction_amount or 0.0),
        )
    )
    return tuple(rows[:max_symbols])


def _metric(name: str, value: float | int | str, unit: str = "", rank_pct: float | None = None, relation: str = "") -> LocalMetric:
    return LocalMetric(name=name, value=value, unit=unit, rank_pct=rank_pct, relation=relation)


def _hot_value(payload: object, field: str, default: float = 0.0) -> float:
    if isinstance(payload, dict):
        value = payload.get(field, default)
    else:
        value = getattr(payload, field, default)
    try:
        return float(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _hot_rank(payload: object) -> int:
    try:
        return int(_hot_value(payload, "rank", 999.0) or 999)
    except (TypeError, ValueError):
        return 999


def _hot_name(payload: object) -> str:
    if isinstance(payload, dict):
        return str(payload.get("plate_name") or payload.get("name") or payload.get("plate") or "")
    return str(getattr(payload, "plate_name", "") or "")


def build_stock_microstructure_node(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 260,
) -> LocalStrategyNodeResult:
    snapshots = tuple(context.stock_snapshots)
    selection_map = {selection.symbol: selection for selection in selection_contexts}
    scope = _stock_scope(snapshots, selection_map, max_symbols=max_symbols)
    amount_2m_floor = relative_amount_floor(snapshots, "amount_2m", top_n=160, fallback=20_000_000)
    signals: list[LocalSignal] = []
    for snapshot in scope:
        behavior = classify_opening_entry_behavior(snapshot, amount_2m_floor=amount_2m_floor)
        amount_ratio = _ratio(snapshot.amount_2m, snapshot.auction_amount)
        state = "micro_watch"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        if behavior in {"volume_confirm", "limit_attack"}:
            state = "micro_attack"
            action = "probe"
            strength = "strong"
        elif behavior == "low_open_repair":
            state = "micro_repair"
            action = "probe"
            strength = "repair"
        elif behavior == "high_open_distribution":
            state = "micro_distribution"
            action = "avoid"
            strength = "risk"
            risks.append("high_open_distribution")
        elif behavior == "weak_follow":
            state = "micro_weak_follow"
            risks.append("weak_2m_follow")
        if snapshot.open_pct >= 0.095 and snapshot.current_pct < snapshot.open_pct - 0.02:
            risks.append("one_word_fade")
        evidence = stock_focus_evidence_labels(
            snapshot,
            phase_label=_phase_name(context),
            selection=selection_map.get(snapshot.symbol),
            all_snapshots=snapshots,
        )
        signals.append(
            LocalSignal(
                signal_id=f"stock_micro:{snapshot.symbol}",
                node_id="stock_microstructure",
                scope_type="stock",
                scope=snapshot.symbol,
                state=state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("open_pct", round(snapshot.open_pct, 4), "pct"),
                    _metric("current_pct", round(snapshot.current_pct, 4), "pct"),
                    _metric("auction_amount", round(snapshot.auction_amount, 2), "yuan"),
                    _metric("amount_2m", round(snapshot.amount_2m, 2), "yuan", snapshot.amount_2m_rank_pct),
                    _metric("amount_2m_vs_auction", round(amount_ratio, 3), "ratio"),
                    _metric("speed_1m", round(snapshot.speed_1m, 5), "pct"),
                    _metric("leader_rank", int(snapshot.leader_rank_in_theme or 999)),
                ),
                evidence=evidence,
                risk_tags=tuple(risks),
            )
        )
    attack_count = sum(1 for item in signals if item.action_hint == "probe")
    risk_count = sum(1 for item in signals if item.action_hint == "avoid")
    summary = "micro_attack_seen" if attack_count else ("micro_risk_seen" if risk_count else "micro_watch")
    return LocalStrategyNodeResult(
        node_id="stock_microstructure",
        layer="stock",
        summary_state=summary,
        action_hint="probe" if attack_count else "watch",
        signals=tuple(signals),
        evidence=(f"signals={len(signals)}", f"probe={attack_count}", f"avoid={risk_count}"),
    )


def build_stock_profile_node(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 260,
) -> LocalStrategyNodeResult:
    snapshots = tuple(context.stock_snapshots)
    selection_map = {selection.symbol: selection for selection in selection_contexts}
    scope = _stock_scope(snapshots, selection_map, max_symbols=max_symbols)
    signals: list[LocalSignal] = []
    for snapshot in scope:
        selection = selection_map.get(snapshot.symbol)
        height = selection.daily_height_bucket if selection is not None else "unknown"
        profile_state = "profile_neutral"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        if height == "high" and snapshot.leader_rank_in_theme > 1:
            profile_state = "profile_high_risk"
            risks.append("high_dayk_non_leader")
        elif height in {"low", "mid"} and snapshot.resistance_gap <= 0.08 and snapshot.shape_chip_cleanliness >= 5.0:
            profile_state = "profile_support"
            action = "support"
            strength = "supportive"
        if snapshot.resistance_gap > 0.12:
            risks.append("large_resistance_gap")
        if snapshot.market_cap_yi >= 300 and snapshot.amount_day_yi < 20:
            risks.append("large_cap_liquidity_mismatch")
        signals.append(
            LocalSignal(
                signal_id=f"stock_profile:{snapshot.symbol}",
                node_id="stock_profile",
                scope_type="stock",
                scope=snapshot.symbol,
                state=profile_state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("daily_height", height),
                    _metric("resistance_gap", round(snapshot.resistance_gap, 4), "pct"),
                    _metric("chip_cleanliness", round(snapshot.shape_chip_cleanliness, 2)),
                    _metric("market_cap_yi", round(snapshot.market_cap_yi, 2), "yi"),
                    _metric("amount_day_yi", round(snapshot.amount_day_yi, 2), "yi"),
                ),
                evidence=(f"dayK={height}", f"resistance={snapshot.resistance_gap:.3f}", f"chip={snapshot.shape_chip_cleanliness:.1f}"),
                risk_tags=tuple(risks),
                depends_on=(f"stock_micro:{snapshot.symbol}",),
            )
        )
    support_count = sum(1 for item in signals if item.action_hint == "support")
    risk_count = sum(1 for item in signals if item.risk_tags)
    return LocalStrategyNodeResult(
        node_id="stock_profile",
        layer="stock",
        summary_state="profile_support_seen" if support_count else ("profile_risk_seen" if risk_count else "profile_neutral"),
        action_hint="support" if support_count else "watch",
        signals=tuple(signals),
        evidence=(f"signals={len(signals)}", f"support={support_count}", f"risk={risk_count}"),
        depends_on=("stock_microstructure",),
    )


def build_stock_capital_profile_node(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 260,
) -> LocalStrategyNodeResult:
    snapshots = tuple(context.stock_snapshots)
    selection_map = {selection.symbol: selection for selection in selection_contexts}
    scope = _stock_scope(snapshots, selection_map, max_symbols=max_symbols)
    signals: list[LocalSignal] = []
    for snapshot in scope:
        state = "capital_watch"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        dde_support = snapshot.ddx > 0 and snapshot.ddy > 0 and snapshot.ddz > 0
        chip_support = snapshot.shape_chip_cleanliness >= 5.0 and snapshot.concentration <= 0.25
        overheat = snapshot.rsi_6 >= 82 or snapshot.bias_20 >= 0.18 or snapshot.profit_ratio >= 0.9
        if overheat and snapshot.leader_rank_in_theme > 1:
            state = "capital_overheat"
            action = "avoid"
            strength = "risk"
            risks.append("capital_overheat_non_leader")
        elif dde_support and chip_support:
            state = "capital_support"
            action = "support"
            strength = "supportive"
        elif snapshot.current_pct > 0.03 and not dde_support:
            state = "capital_divergence"
            risks.append("price_without_dde")
        if snapshot.ddje < 0 and snapshot.current_pct > 0:
            risks.append("dde_outflow_red_price")
        signals.append(
            LocalSignal(
                signal_id=f"stock_capital:{snapshot.symbol}",
                node_id="stock_capital_profile",
                scope_type="stock",
                scope=snapshot.symbol,
                state=state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("ddx", round(snapshot.ddx, 4)),
                    _metric("ddy", round(snapshot.ddy, 4)),
                    _metric("ddz", round(snapshot.ddz, 4)),
                    _metric("ddje", round(snapshot.ddje, 2), "yuan"),
                    _metric("profit_ratio", round(snapshot.profit_ratio, 4), "ratio"),
                    _metric("concentration", round(snapshot.concentration, 4), "ratio"),
                    _metric("bias_20", round(snapshot.bias_20, 4), "pct"),
                    _metric("rsi_6", round(snapshot.rsi_6, 2)),
                    _metric("chip_cleanliness", round(snapshot.shape_chip_cleanliness, 2)),
                ),
                evidence=(
                    f"dde={'support' if dde_support else 'weak'}",
                    f"chip={'support' if chip_support else 'normal'}",
                    f"overheat={overheat}",
                ),
                risk_tags=tuple(risks),
                depends_on=(f"stock_micro:{snapshot.symbol}", f"stock_profile:{snapshot.symbol}"),
            )
        )
    support = sum(1 for item in signals if item.action_hint == "support")
    avoid = sum(1 for item in signals if item.action_hint == "avoid")
    divergence = sum(1 for item in signals if item.state == "capital_divergence")
    return LocalStrategyNodeResult(
        node_id="stock_capital_profile",
        layer="stock",
        summary_state="capital_support_seen" if support else ("capital_risk_seen" if avoid or divergence else "capital_watch"),
        action_hint="support" if support else "watch",
        signals=tuple(signals),
        evidence=(f"signals={len(signals)}", f"support={support}", f"avoid={avoid}", f"divergence={divergence}"),
        depends_on=("stock_microstructure", "stock_profile"),
    )


def build_weak_to_strong_repair_node(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 260,
) -> LocalStrategyNodeResult:
    snapshots = tuple(context.stock_snapshots)
    selection_map = {selection.symbol: selection for selection in selection_contexts}
    scope = _stock_scope(snapshots, selection_map, max_symbols=max_symbols)
    amount_2m_floor = relative_amount_floor(snapshots, "amount_2m", top_n=160, fallback=20_000_000)
    auction_floor = relative_amount_floor(snapshots, "auction_amount", top_n=160, fallback=10_000_000)
    signals: list[LocalSignal] = []
    for snapshot in scope:
        selection = selection_map.get(snapshot.symbol)
        height = selection.daily_height_bucket if selection is not None else "unknown"
        open_to_current = float(snapshot.current_pct or 0.0) - float(snapshot.open_pct or 0.0)
        auction_ok = float(snapshot.auction_amount or 0.0) >= auction_floor
        amount_ok = float(snapshot.amount_2m or 0.0) >= amount_2m_floor or float(snapshot.amount_2m_rank_pct or 1.0) <= 0.25
        speed_ok = float(snapshot.speed_1m or 0.0) >= 0.006
        low_open = float(snapshot.open_pct or 0.0) <= -0.015
        deep_open = float(snapshot.open_pct or 0.0) <= -0.035
        repaired = open_to_current >= 0.035 and float(snapshot.current_pct or 0.0) >= -0.005
        state = "repair_watch"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        if height == "high" and snapshot.leader_rank_in_theme > 1:
            risks.append("high_dayk_non_leader_repair")
        if low_open and auction_ok and amount_ok and speed_ok and repaired and "high_dayk_non_leader_repair" not in risks:
            state = "repair_confirmed"
            action = "probe"
            strength = "repair"
        elif low_open and auction_ok and amount_ok:
            state = "repair_wait"
            strength = "watch_repair"
        elif deep_open and open_to_current < 0.015:
            state = "repair_failed"
            action = "avoid"
            strength = "risk"
            risks.append("deep_open_no_repair")
        signals.append(
            LocalSignal(
                signal_id=f"weak_to_strong:{snapshot.symbol}",
                node_id="weak_to_strong_repair",
                scope_type="stock",
                scope=snapshot.symbol,
                state=state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("open_pct", round(snapshot.open_pct, 4), "pct"),
                    _metric("current_pct", round(snapshot.current_pct, 4), "pct"),
                    _metric("open_to_current", round(open_to_current, 4), "pct"),
                    _metric("auction_amount", round(snapshot.auction_amount, 2), "yuan"),
                    _metric("amount_2m", round(snapshot.amount_2m, 2), "yuan", snapshot.amount_2m_rank_pct),
                    _metric("speed_1m", round(snapshot.speed_1m, 5), "pct"),
                    _metric("daily_height", height),
                ),
                evidence=(
                    f"low_open={low_open}",
                    f"auction_ok={auction_ok}",
                    f"amount_ok={amount_ok}",
                    f"repaired={repaired}",
                ),
                risk_tags=tuple(risks),
                depends_on=(f"stock_micro:{snapshot.symbol}", f"stock_profile:{snapshot.symbol}", f"stock_capital:{snapshot.symbol}"),
            )
        )
    confirmed = sum(1 for item in signals if item.state == "repair_confirmed")
    waiting = sum(1 for item in signals if item.state == "repair_wait")
    failed = sum(1 for item in signals if item.state == "repair_failed")
    return LocalStrategyNodeResult(
        node_id="weak_to_strong_repair",
        layer="stock",
        summary_state="repair_confirmed_seen" if confirmed else ("repair_wait_seen" if waiting else ("repair_failed_seen" if failed else "repair_watch")),
        action_hint="probe" if confirmed else "watch",
        signals=tuple(signals),
        evidence=(f"signals={len(signals)}", f"confirmed={confirmed}", f"wait={waiting}", f"failed={failed}"),
        depends_on=("stock_microstructure", "stock_profile", "stock_capital_profile"),
    )


def build_high_focus_local_node(context: IntradayContext) -> LocalStrategyNodeResult:
    high_rows = tuple(
        snapshot
        for snapshot in context.stock_snapshots
        if snapshot.lb_days >= 2 or snapshot.leader_rank_in_theme <= 1 or snapshot.is_yest_limit
    )
    signals: list[LocalSignal] = []
    for snapshot in high_rows[:80]:
        theme = _primary_theme(snapshot)
        amount_ratio = _ratio(snapshot.amount_2m, snapshot.auction_amount)
        state = "high_focus_watch"
        action = "watch"
        risks: list[str] = []
        if snapshot.current_pct >= 0.095 or snapshot.is_locked or snapshot.touched_limit_today:
            state = "high_focus_promotion"
            action = "support"
        elif snapshot.current_pct <= -0.03 or (snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.04):
            state = "high_focus_negative"
            action = "avoid"
            risks.append("high_focus_negative")
        signals.append(
            LocalSignal(
                signal_id=f"high_focus:{snapshot.symbol}",
                node_id="high_focus",
                scope_type="stock",
                scope=snapshot.symbol,
                state=state,
                action_hint=action,
                strength_bucket="strong" if action == "support" else ("risk" if action == "avoid" else "normal"),
                metrics=(
                    _metric("theme", theme),
                    _metric("lb_days", int(snapshot.lb_days or 0)),
                    _metric("open_pct", round(snapshot.open_pct, 4), "pct"),
                    _metric("current_pct", round(snapshot.current_pct, 4), "pct"),
                    _metric("amount_2m_vs_auction", round(amount_ratio, 3), "ratio"),
                ),
                evidence=(f"theme={theme}", f"lb={snapshot.lb_days}", f"ratio2m={amount_ratio:.2f}"),
                risk_tags=tuple(risks),
            )
        )
    positive = sum(1 for item in signals if item.action_hint == "support")
    negative = sum(1 for item in signals if item.action_hint == "avoid")
    state = "high_focus_positive" if positive > negative else ("high_focus_negative" if negative else "high_focus_neutral")
    return LocalStrategyNodeResult(
        node_id="high_focus",
        layer="high_focus",
        summary_state=state,
        action_hint="avoid_chase" if negative > positive else "watch",
        signals=tuple(signals),
        evidence=(f"high={len(signals)}", f"positive={positive}", f"negative={negative}"),
    )


def build_theme_high_focus_impact_node(
    context: IntradayContext,
    high_focus_node: LocalStrategyNodeResult,
) -> LocalStrategyNodeResult:
    snapshot_map = {snapshot.symbol: snapshot for snapshot in context.stock_snapshots}
    theme_fact_map = context.session_facts.theme_fact_map or {}
    buckets: dict[str, dict[str, object]] = {}
    for signal in high_focus_node.signals:
        snapshot = snapshot_map.get(signal.scope)
        if snapshot is None:
            continue
        for theme_name in _theme_names(snapshot):
            bucket = buckets.setdefault(
                theme_name,
                {
                    "signals": [],
                    "positive": 0,
                    "negative": 0,
                    "high_count": 0,
                    "amount_2m": 0.0,
                    "samples": [],
                },
            )
            cast_signals = bucket["signals"]
            if isinstance(cast_signals, list):
                cast_signals.append(signal)
            if signal.action_hint == "support":
                bucket["positive"] = int(bucket["positive"]) + 1
            if signal.action_hint in {"avoid", "avoid_chase"}:
                bucket["negative"] = int(bucket["negative"]) + 1
            if snapshot.lb_days >= 2 or snapshot.t2_lb_days >= 2:
                bucket["high_count"] = int(bucket["high_count"]) + 1
            bucket["amount_2m"] = float(bucket["amount_2m"]) + float(snapshot.amount_2m or 0.0)
            cast_samples = bucket["samples"]
            if isinstance(cast_samples, list) and snapshot.symbol not in cast_samples and len(cast_samples) < 4:
                cast_samples.append(snapshot.symbol)
    signals: list[LocalSignal] = []
    for theme_name, bucket in sorted(
        buckets.items(),
        key=lambda item: (
            -int(item[1]["negative"]),
            -int(item[1]["positive"]),
            -float(item[1]["amount_2m"]),
            item[0],
        ),
    )[:40]:
        positive = int(bucket["positive"])
        negative = int(bucket["negative"])
        high_count = int(bucket["high_count"])
        amount_2m = float(bucket["amount_2m"])
        samples = tuple(str(item) for item in bucket["samples"]) if isinstance(bucket["samples"], list) else ()
        raw_signals = tuple(bucket["signals"]) if isinstance(bucket["signals"], list) else ()
        high_attention_count = max(len(raw_signals), 1)
        theme_fact = theme_fact_map.get(theme_name)
        theme_symbol_count = int(getattr(theme_fact, "symbol_count", 0) or 0)
        normalized_base = max(min(theme_symbol_count, high_attention_count * 3), high_attention_count, 1)
        negative_rate = negative / normalized_base
        positive_rate = positive / normalized_base
        state = "theme_high_drag_watch"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        group_pressure = negative >= 2 and negative >= positive and (negative_rate >= 0.34 or negative >= max(2, high_attention_count // 2))
        individual_fail = negative >= 1 and not group_pressure and positive <= negative
        if group_pressure:
            state = "theme_high_group_pressure"
            action = "avoid"
            strength = "risk"
            risks.append("high_focus_group_pressure")
        elif individual_fail:
            state = "theme_high_individual_fail"
            strength = "watch_fail"
            risks.append("high_focus_individual_fail")
        elif positive >= 1 and negative == 0:
            state = "theme_high_promotion"
            action = "support"
            strength = "supportive"
        signals.append(
            LocalSignal(
                signal_id=f"theme_high_focus:{theme_name}",
                node_id="theme_high_focus_impact",
                scope_type="theme",
                scope=theme_name,
                state=state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("positive_high_focus", positive),
                    _metric("negative_high_focus", negative),
                    _metric("negative_rate", round(negative_rate, 4), "ratio"),
                    _metric("positive_rate", round(positive_rate, 4), "ratio"),
                    _metric("high_count", high_count),
                    _metric("high_attention_count", high_attention_count),
                    _metric("theme_symbol_count", theme_symbol_count),
                    _metric("amount_2m_sum", round(amount_2m, 2), "yuan"),
                    _metric("samples", ",".join(samples)),
                ),
                evidence=(
                    f"positive={positive}",
                    f"negative={negative}",
                    f"neg_rate={negative_rate:.2f}",
                    f"base={normalized_base}",
                    f"high_count={high_count}",
                    f"samples={','.join(samples) or '-'}",
                ),
                risk_tags=tuple(risks),
                depends_on=tuple(signal.signal_id for signal in raw_signals[:6]),
            )
        )
    support = sum(1 for item in signals if item.action_hint == "support")
    avoid = sum(1 for item in signals if item.action_hint == "avoid")
    watch_fail = sum(1 for item in signals if item.state == "theme_high_individual_fail")
    return LocalStrategyNodeResult(
        node_id="theme_high_focus_impact",
        layer="theme",
        summary_state="theme_high_group_pressure_seen" if avoid else ("theme_high_promotion_seen" if support else "theme_high_watch"),
        action_hint="avoid" if avoid else ("support" if support else "watch"),
        signals=tuple(signals),
        evidence=(f"themes={len(signals)}", f"support={support}", f"avoid={avoid}", f"individual_fail={watch_fail}"),
        depends_on=("high_focus", "stock_microstructure"),
    )


def build_auction_bucket_local_node(context: IntradayContext, *, top_n: int = 30) -> LocalStrategyNodeResult:
    stats = build_auction_plate_bucket_stats(context, top_n=top_n)
    signals: list[LocalSignal] = []
    for row in stats[:top_n]:
        red_green_ratio = _ratio(row.red_count, row.green_count)
        drift = float(row.avg_current_pct or 0.0) - float(row.avg_open_pct or 0.0)
        state = "auction_bucket_watch"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        if row.auction_amount > 0 and row.leader_count >= 2 and row.yest_limit_count >= 1:
            state = "auction_bucket_concentrated"
            action = "probe"
            strength = "strong"
        elif row.red_count >= row.green_count * 2 and row.leader_count >= 1:
            state = "auction_bucket_breadth"
            action = "probe"
            strength = "breadth"
        if drift <= -0.015 and row.avg_open_pct > 0.01:
            state = "auction_bucket_fade"
            action = "avoid"
            strength = "risk"
            risks.append("auction_open_fade")
        if row.generic:
            risks.append("generic_plate")
        signals.append(
            LocalSignal(
                signal_id=f"auction_bucket:{row.plate_name}",
                node_id="auction_bucket_local",
                scope_type="theme",
                scope=row.plate_name,
                state=state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("auction_amount", row.auction_amount, "yuan"),
                    _metric("symbol_count", row.symbol_count),
                    _metric("leader_count", row.leader_count),
                    _metric("yest_limit_count", row.yest_limit_count),
                    _metric("red_green_ratio", round(red_green_ratio, 3), "ratio"),
                    _metric("avg_open_pct", round(row.avg_open_pct, 4), "pct"),
                    _metric("avg_current_pct", round(row.avg_current_pct, 4), "pct"),
                    _metric("open_to_current_drift", round(drift, 4), "pct"),
                ),
                evidence=(
                    f"expectation={row.expectation}",
                    f"auc={row.auction_amount:.0f}",
                    f"leaders={row.leader_count}",
                    f"rg={row.red_count}:{row.green_count}",
                ),
                risk_tags=tuple(risks),
                depends_on=("stock_microstructure",),
            )
        )
    probe = sum(1 for item in signals if item.action_hint == "probe")
    avoid = sum(1 for item in signals if item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="auction_bucket_local",
        layer="theme",
        summary_state="auction_bucket_probe_seen" if probe else ("auction_bucket_risk_seen" if avoid else "auction_bucket_watch"),
        action_hint="probe" if probe else "watch",
        signals=tuple(signals),
        evidence=(f"buckets={len(signals)}", f"probe={probe}", f"avoid={avoid}"),
        depends_on=("stock_microstructure",),
    )


def build_hot_plate_context_node(context: IntradayContext, *, top_n: int = 40) -> LocalStrategyNodeResult:
    today_map = context.session_facts.hot_plate_today_map or context.hot_plate_map or {}
    yesterday_map = context.session_facts.hot_plate_yesterday_map or context.yesterday_hot_plate_map or {}
    migration_map = context.session_facts.plate_migration_map or {}
    plate_names = set(today_map) | set(yesterday_map) | set(migration_map)
    ranked_names = sorted(
        plate_names,
        key=lambda name: (
            _hot_rank(today_map.get(name, {})),
            _hot_rank(yesterday_map.get(name, {})),
            name,
        ),
    )[:top_n]
    signals: list[LocalSignal] = []
    migrating_in = set(getattr(context.market_summary, "migrating_in_plates", ()) or ())
    migrating_out = set(getattr(context.market_summary, "migrating_out_plates", ()) or ())
    for plate_name in ranked_names:
        today = today_map.get(plate_name, {})
        yesterday = yesterday_map.get(plate_name, {})
        migration = migration_map.get(plate_name)
        today_rank = _hot_rank(today)
        yesterday_rank = _hot_rank(yesterday)
        change_pct = _hot_value(today, "change_pct")
        net_inflow = _hot_value(today, "net_inflow_yi")
        strength = _hot_value(today, "strength", _hot_value(today, "hot"))
        delta_inflow = float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0) if migration is not None else 0.0
        state = "hot_plate_watch"
        action = "watch"
        risks: list[str] = []
        if plate_name in migrating_out or delta_inflow <= -10:
            state = "hot_plate_fading"
            action = "avoid"
            risks.append("hot_plate_fading")
        elif today_rank <= 20 and yesterday_rank <= 50:
            state = "hot_plate_continuation"
            action = "support"
        elif today_rank <= 20 or plate_name in migrating_in or delta_inflow >= 10:
            state = "hot_plate_new_attack"
            action = "probe"
        signals.append(
            LocalSignal(
                signal_id=f"hot_plate:{plate_name}",
                node_id="hot_plate_context",
                scope_type="theme",
                scope=plate_name,
                state=state,
                action_hint=action,
                strength_bucket="strong" if action in {"support", "probe"} else ("risk" if action == "avoid" else "normal"),
                metrics=(
                    _metric("today_hot_rank", today_rank),
                    _metric("yesterday_hot_rank", yesterday_rank),
                    _metric("hot_change_pct", round(change_pct, 4), "pct"),
                    _metric("hot_net_inflow_yi", round(net_inflow, 2), "yi"),
                    _metric("hot_strength", round(strength, 2)),
                    _metric("net_inflow_delta_yi", round(delta_inflow, 2), "yi"),
                ),
                evidence=(f"today_rank={today_rank}", f"yday_rank={yesterday_rank}", f"delta_in={delta_inflow:.1f}"),
                risk_tags=tuple(risks),
            )
        )
    active = sum(1 for item in signals if item.action_hint in {"support", "probe"})
    fading = sum(1 for item in signals if item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="hot_plate_context",
        layer="theme_context",
        summary_state="hot_plate_active" if active else ("hot_plate_fading" if fading else "hot_plate_watch"),
        action_hint="probe" if active else "watch",
        signals=tuple(signals),
        evidence=(f"plates={len(signals)}", f"active={active}", f"fading={fading}"),
    )


def build_yesterday_limit_pool_node(context: IntradayContext) -> LocalStrategyNodeResult:
    rows = tuple(snapshot for snapshot in context.stock_snapshots if snapshot.is_yest_limit)
    buckets = {
        "high": tuple(snapshot for snapshot in rows if snapshot.lb_days >= 3 or snapshot.t2_lb_days >= 3),
        "mid": tuple(snapshot for snapshot in rows if 1 <= snapshot.lb_days < 3 or 1 <= snapshot.t2_lb_days < 3),
        "first": tuple(snapshot for snapshot in rows if snapshot.lb_days <= 0 and snapshot.t2_lb_days <= 0),
    }
    signals: list[LocalSignal] = []
    for bucket_name, bucket_rows in buckets.items():
        total = len(bucket_rows)
        if total == 0:
            continue
        red = sum(1 for snapshot in bucket_rows if snapshot.open_pct > 0)
        promoted = sum(1 for snapshot in bucket_rows if snapshot.current_pct >= 0.095 or snapshot.touched_limit_today or snapshot.is_locked)
        deep_negative = sum(1 for snapshot in bucket_rows if snapshot.current_pct <= -0.05 or snapshot.open_pct <= -0.05)
        amount_2m = sum(float(snapshot.amount_2m or 0.0) for snapshot in bucket_rows)
        state = "yest_limit_watch"
        action = "watch"
        risks: list[str] = []
        if deep_negative >= max(1, total // 3):
            state = "yest_limit_negative"
            action = "avoid"
            risks.append("limit_pool_negative_feedback")
        elif promoted >= max(1, total // 3) and red >= max(1, total // 2):
            state = "yest_limit_relay_ok"
            action = "support"
        elif red > 0 or promoted > 0:
            state = "yest_limit_divergent"
            action = "watch"
        signals.append(
            LocalSignal(
                signal_id=f"yest_limit_pool:{bucket_name}",
                node_id="yesterday_limit_pool",
                scope_type="emotion_bucket",
                scope=bucket_name,
                state=state,
                action_hint=action,
                strength_bucket="strong" if action == "support" else ("risk" if action == "avoid" else "normal"),
                metrics=(
                    _metric("sample_count", total),
                    _metric("red_open_count", red),
                    _metric("promotion_count", promoted),
                    _metric("deep_negative_count", deep_negative),
                    _metric("amount_2m_sum", round(amount_2m, 2), "yuan"),
                ),
                evidence=(f"sample={total}", f"red={red}", f"promoted={promoted}", f"deep_neg={deep_negative}"),
                risk_tags=tuple(risks),
                depends_on=("stock_microstructure", "high_focus"),
            )
        )
    support = sum(1 for item in signals if item.action_hint == "support")
    avoid = sum(1 for item in signals if item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="yesterday_limit_pool",
        layer="emotion",
        summary_state="yest_limit_relay_ok" if support else ("yest_limit_negative" if avoid else "yest_limit_watch"),
        action_hint="support" if support else "watch",
        signals=tuple(signals),
        evidence=(f"buckets={len(signals)}", f"support={support}", f"avoid={avoid}"),
        depends_on=("stock_microstructure", "high_focus"),
    )


def _theme_spread_level(fact: ThemeTradeFact) -> str:
    if fact.front_row_2m_pass_count >= 2 and fact.expansion_count >= 2:
        return "strong"
    if fact.front_row_2m_pass_count >= 1 or fact.expansion_count >= 1:
        return "normal"
    if fact.front_row_count > 0:
        return "weak"
    return "none"


def build_theme_internal_node(context: IntradayContext) -> LocalStrategyNodeResult:
    facts = tuple(context.session_facts.theme_trade_facts or ())
    signals: list[LocalSignal] = []
    for fact in sorted(
        facts,
        key=lambda item: (
            item.yest_hot_rank,
            -float(item.amount_2m_sum or 0.0),
            -float(item.auction_amount or 0.0),
            item.plate_name,
        ),
    )[:40]:
        amount_ratio = _ratio(fact.amount_2m_sum, fact.auction_amount)
        spread = _theme_spread_level(fact)
        state = "theme_watch"
        action = "watch"
        risks: list[str] = []
        if fact.high_open_fail_count >= max(1, fact.front_row_count):
            state = "theme_distribution"
            action = "avoid"
            risks.append("front_row_distribution")
        elif fact.low_open_repair_count > 0 and amount_ratio >= 0.8:
            state = "theme_repair"
            action = "probe"
        elif spread in {"strong", "normal"} and fact.amount_2m_sum > 0:
            state = "theme_extension" if fact.yest_hot_rank <= 50 else "theme_rotation"
            action = "probe"
        elif fact.amount_2m_sum > 0:
            state = "theme_fakeout_watch"
            risks.append("amount_without_spread")
        signals.append(
            LocalSignal(
                signal_id=f"theme_internal:{fact.plate_name}",
                node_id="theme_internal",
                scope_type="theme",
                scope=fact.plate_name,
                state=state,
                action_hint=action,
                strength_bucket=spread,
                metrics=(
                    _metric("yest_hot_rank", int(fact.yest_hot_rank or 999)),
                    _metric("yest_limit_count", int(fact.yest_limit_count or 0)),
                    _metric("auction_amount", round(fact.auction_amount, 2), "yuan"),
                    _metric("amount_2m_sum", round(fact.amount_2m_sum, 2), "yuan"),
                    _metric("amount_2m_vs_auction", round(amount_ratio, 3), "ratio"),
                    _metric("front_row_count", int(fact.front_row_count or 0)),
                    _metric("front_row_2m_pass", int(fact.front_row_2m_pass_count or 0)),
                    _metric("expansion_count", int(fact.expansion_count or 0)),
                ),
                evidence=(f"spread={spread}", f"ratio2m={amount_ratio:.2f}", f"yhot={fact.yest_hot_rank}"),
                risk_tags=tuple(risks),
                depends_on=("stock_microstructure", "high_focus"),
            )
        )
    probe = sum(1 for item in signals if item.action_hint == "probe")
    avoid = sum(1 for item in signals if item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="theme_internal",
        layer="theme",
        summary_state="theme_probe_seen" if probe else ("theme_risk_seen" if avoid else "theme_watch"),
        action_hint="probe" if probe else "watch",
        signals=tuple(signals),
        evidence=(f"themes={len(signals)}", f"probe={probe}", f"avoid={avoid}"),
        depends_on=("stock_microstructure", "stock_profile", "high_focus"),
    )


def build_theme_opening_validation_node(context: IntradayContext) -> LocalStrategyNodeResult:
    bundle = context.opening_validation_bundle
    if bundle is None:
        return LocalStrategyNodeResult(
            node_id="theme_opening_validation",
            layer="theme",
            summary_state="opening_validation_missing",
            action_hint="watch",
            evidence=("bundle=missing",),
            depends_on=("theme_internal", "auction_bucket_local"),
        )
    rows = []
    for mapping_name, mapping in (
        ("confirmed", bundle.confirmed_themes),
        ("falsified", bundle.falsified_themes),
        ("watch", bundle.watch_themes),
    ):
        for plate_name, item in mapping.items():
            rows.append((mapping_name, plate_name, item))
    signals: list[LocalSignal] = []
    for mapping_name, plate_name, item in rows[:60]:
        state = "opening_watch"
        action = "watch"
        risks: list[str] = []
        if mapping_name == "confirmed" and item.tradable_level == "attack":
            state = "opening_confirmed"
            action = "probe"
        elif mapping_name == "confirmed":
            state = "opening_probe"
            action = "watch"
        elif mapping_name == "falsified":
            state = "opening_falsified"
            action = "avoid"
            risks.append(item.invalid_reason or "opening_falsified")
        signals.append(
            LocalSignal(
                signal_id=f"opening_validation:{plate_name}",
                node_id="theme_opening_validation",
                scope_type="theme",
                scope=plate_name,
                state=state,
                action_hint=action,
                strength_bucket=item.tradable_level or "watch",
                metrics=(
                    _metric("validation_state", item.validation_state),
                    _metric("tradable_level", item.tradable_level),
                    _metric("amount_2m_rank_pct", round(item.amount_2m_rank_pct, 4), "rank_pct"),
                    _metric("amount_2m_ratio_vs_auction", round(item.amount_2m_ratio_vs_auction, 3), "ratio"),
                    _metric("net_inflow_delta_yi", round(item.net_inflow_delta_yi, 2), "yi"),
                    _metric("front_row_confirmed", int(bool(item.front_row_confirmed))),
                    _metric("mid_follow_confirmed", int(bool(item.mid_follow_confirmed))),
                ),
                evidence=item.evidence or (f"script={item.predicted_script}", f"state={item.validation_state}", f"level={item.tradable_level}"),
                risk_tags=tuple(risks),
                depends_on=("theme_internal", "auction_bucket_local"),
            )
        )
    confirmed = sum(1 for item in signals if item.state in {"opening_confirmed", "opening_probe"})
    falsified = sum(1 for item in signals if item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="theme_opening_validation",
        layer="theme",
        summary_state="opening_confirmed_seen" if confirmed else ("opening_falsified_seen" if falsified else "opening_watch"),
        action_hint="probe" if confirmed else "watch",
        signals=tuple(signals),
        evidence=(f"main={bundle.main_validated_theme or '-'}", f"confirmed={confirmed}", f"falsified={falsified}"),
        depends_on=("theme_internal", "auction_bucket_local"),
    )


def build_theme_relative_node(
    context: IntradayContext,
    theme_node: LocalStrategyNodeResult,
    *,
    hot_plate_node: LocalStrategyNodeResult | None = None,
    opening_node: LocalStrategyNodeResult | None = None,
    high_impact_node: LocalStrategyNodeResult | None = None,
) -> LocalStrategyNodeResult:
    theme_signals = tuple(item for item in theme_node.signals if item.scope_type == "theme")
    migrating_in = tuple(getattr(context.market_summary, "migrating_in_plates", ()) or ())
    migrating_out = tuple(getattr(context.market_summary, "migrating_out_plates", ()) or ())
    hot_by_theme = {item.scope: item for item in (hot_plate_node.signals if hot_plate_node is not None else ())}
    opening_by_theme = {item.scope: item for item in (opening_node.signals if opening_node is not None else ())}
    high_impact_by_theme = {item.scope: item for item in (high_impact_node.signals if high_impact_node is not None else ())}
    signals: list[LocalSignal] = []
    for signal in theme_signals:
        hot_signal = hot_by_theme.get(signal.scope)
        opening_signal = opening_by_theme.get(signal.scope)
        high_signal = high_impact_by_theme.get(signal.scope)
        state = "relative_watch"
        action = "watch"
        risks: list[str] = []
        if high_signal is not None and high_signal.state == "theme_high_group_pressure":
            state = "relative_fading"
            action = "avoid"
            risks.append("high_focus_group_pressure")
        elif signal.scope in migrating_out or signal.state == "theme_distribution":
            state = "relative_fading"
            action = "avoid"
            risks.append("relative_fading")
        elif opening_signal is not None and opening_signal.state == "opening_confirmed":
            state = "relative_leading"
            action = "probe"
        elif opening_signal is not None and opening_signal.state == "opening_falsified":
            state = "relative_fake_rotation"
            action = "avoid"
            risks.append("opening_falsified")
        elif signal.scope in migrating_in:
            state = "relative_migrating_in"
            action = "probe" if signal.action_hint == "probe" else "watch"
        elif hot_signal is not None and hot_signal.state in {"hot_plate_continuation", "hot_plate_new_attack"} and signal.action_hint == "probe":
            state = "relative_leading" if hot_signal.state == "hot_plate_continuation" else "relative_rotation"
            action = "probe"
        elif high_signal is not None and high_signal.state == "theme_high_promotion" and signal.action_hint == "probe":
            state = "relative_leading"
            action = "probe"
        elif signal.state in {"theme_extension", "theme_repair"}:
            state = "relative_leading"
            action = "probe"
        elif signal.state == "theme_rotation":
            state = "relative_rotation"
            action = "probe"
        elif "amount_without_spread" in signal.risk_tags:
            state = "relative_fake_rotation"
            risks.append("fake_rotation")
        signals.append(
            LocalSignal(
                signal_id=f"theme_relative:{signal.scope}",
                node_id="theme_relative",
                scope_type="theme",
                scope=signal.scope,
                state=state,
                action_hint=action,
                strength_bucket=signal.strength_bucket,
                metrics=signal.metrics,
                evidence=signal.evidence
                + (
                    f"hot={hot_signal.state if hot_signal is not None else '-'}",
                    f"opening={opening_signal.state if opening_signal is not None else '-'}",
                    f"high={high_signal.state if high_signal is not None else '-'}",
                    f"migrating_in={signal.scope in migrating_in}",
                    f"migrating_out={signal.scope in migrating_out}",
                ),
                risk_tags=tuple(risks),
                depends_on=tuple(
                    item
                    for item in (
                        signal.signal_id,
                        hot_signal.signal_id if hot_signal is not None else "",
                        opening_signal.signal_id if opening_signal is not None else "",
                        high_signal.signal_id if high_signal is not None else "",
                    )
                    if item
                ),
            )
        )
    probe = sum(1 for item in signals if item.action_hint == "probe")
    risk = sum(1 for item in signals if item.risk_tags or item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="theme_relative",
        layer="theme_relative",
        summary_state="relative_path_found" if probe else ("relative_risk_first" if risk else "relative_watch"),
        action_hint="probe" if probe else "watch",
        signals=tuple(signals),
        evidence=(f"themes={len(signals)}", f"probe={probe}", f"risk={risk}"),
        depends_on=("theme_internal", "hot_plate_context", "theme_opening_validation", "theme_high_focus_impact", "high_focus"),
    )


def build_theme_stock_bridge_node(
    context: IntradayContext,
    *,
    stock_micro_node: LocalStrategyNodeResult,
    stock_profile_node: LocalStrategyNodeResult,
    stock_capital_node: LocalStrategyNodeResult,
    repair_node: LocalStrategyNodeResult,
    theme_relative_node: LocalStrategyNodeResult,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 260,
) -> LocalStrategyNodeResult:
    snapshots = tuple(context.stock_snapshots)
    selection_map = {selection.symbol: selection for selection in selection_contexts}
    scope = _stock_scope(snapshots, selection_map, max_symbols=max_symbols)
    micro_by_stock = {signal.scope: signal for signal in stock_micro_node.signals if signal.scope_type == "stock"}
    profile_by_stock = {signal.scope: signal for signal in stock_profile_node.signals if signal.scope_type == "stock"}
    capital_by_stock = {signal.scope: signal for signal in stock_capital_node.signals if signal.scope_type == "stock"}
    repair_by_stock = {signal.scope: signal for signal in repair_node.signals if signal.scope_type == "stock"}
    theme_by_name = {signal.scope: signal for signal in theme_relative_node.signals if signal.scope_type == "theme"}
    signals: list[LocalSignal] = []
    for snapshot in scope:
        candidate_themes = _theme_names(snapshot)
        theme, theme_signal = _best_theme_signal(theme_by_name, candidate_themes)
        micro_signal = micro_by_stock.get(snapshot.symbol)
        profile_signal = profile_by_stock.get(snapshot.symbol)
        capital_signal = capital_by_stock.get(snapshot.symbol)
        repair_signal = repair_by_stock.get(snapshot.symbol)
        theme_action = theme_signal.action_hint if theme_signal is not None else "watch"
        micro_action = micro_signal.action_hint if micro_signal is not None else "watch"
        profile_action = profile_signal.action_hint if profile_signal is not None else "watch"
        capital_action = capital_signal.action_hint if capital_signal is not None else "watch"
        repair_action = repair_signal.action_hint if repair_signal is not None else "watch"
        leader_rank = int(snapshot.leader_rank_in_theme or 999)
        stock_risk = any(
            signal is not None and signal.action_hint in {"avoid", "avoid_chase"}
            for signal in (micro_signal, profile_signal, capital_signal, repair_signal)
        )
        stock_support = any(
            signal is not None and signal.action_hint in {"support", "probe"}
            for signal in (micro_signal, profile_signal, capital_signal, repair_signal)
        )
        state = "theme_stock_stock_only" if stock_support else "theme_stock_wait_trigger"
        action = "watch"
        strength = "normal"
        risks: list[str] = []
        pressure_repair = (
            theme_signal is not None
            and "high_focus_group_pressure" in theme_signal.risk_tags
            and repair_action == "probe"
            and leader_rank <= 1
            and capital_action in {"support", "watch"}
            and not stock_risk
        )
        if stock_risk:
            state = "theme_stock_fakeout_risk"
            action = "avoid"
            strength = "risk"
            risks.append("stock_local_risk")
        elif pressure_repair:
            state = "theme_stock_pressure_repair"
            action = "probe"
            strength = "pressure_repair"
        elif theme_action in {"avoid", "avoid_chase"}:
            state = "theme_stock_theme_risk"
            action = "avoid"
            strength = "risk"
            risks.append("theme_relative_risk")
        elif theme_action in {"probe", "support"} and micro_action == "probe" and capital_action in {"support", "watch"}:
            state = "theme_stock_aligned"
            action = "probe"
            strength = "aligned"
        elif theme_action in {"probe", "support"} and repair_action == "probe" and capital_action in {"support", "watch"}:
            state = "theme_stock_aligned"
            action = "probe"
            strength = "repair_aligned"
        elif theme_action in {"probe", "support"}:
            state = "theme_stock_wait_trigger"
            action = "watch"
            strength = "theme_ready"
        elif stock_support:
            state = "theme_stock_stock_only"
            action = "watch"
            strength = "stock_only"
        depends = tuple(
            item
            for item in (
                theme_signal.signal_id if theme_signal is not None else "",
                micro_signal.signal_id if micro_signal is not None else "",
                profile_signal.signal_id if profile_signal is not None else "",
                capital_signal.signal_id if capital_signal is not None else "",
                repair_signal.signal_id if repair_signal is not None else "",
            )
            if item
        )
        signals.append(
            LocalSignal(
                signal_id=f"theme_stock_bridge:{snapshot.symbol}",
                node_id="theme_stock_bridge",
                scope_type="stock",
                scope=snapshot.symbol,
                state=state,
                action_hint=action,
                strength_bucket=strength,
                metrics=(
                    _metric("theme", theme),
                    _metric("theme_candidates", ",".join(candidate_themes[:5])),
                    _metric("theme_action", theme_action),
                    _metric("micro_action", micro_action),
                    _metric("profile_action", profile_action),
                    _metric("capital_action", capital_action),
                    _metric("repair_action", repair_action),
                    _metric("leader_rank", leader_rank),
                    _metric("amount_2m", round(snapshot.amount_2m, 2), "yuan", snapshot.amount_2m_rank_pct),
                    _metric("speed_1m", round(snapshot.speed_1m, 5), "pct"),
                ),
                evidence=(
                    f"theme={theme}:{theme_signal.state if theme_signal is not None else '-'}",
                    f"micro={micro_signal.state if micro_signal is not None else '-'}",
                    f"profile={profile_signal.state if profile_signal is not None else '-'}",
                    f"capital={capital_signal.state if capital_signal is not None else '-'}",
                    f"repair={repair_signal.state if repair_signal is not None else '-'}",
                ),
                risk_tags=tuple(risks),
                depends_on=depends,
            )
        )
    aligned = sum(1 for item in signals if item.state in {"theme_stock_aligned", "theme_stock_pressure_repair"})
    wait = sum(1 for item in signals if item.state == "theme_stock_wait_trigger")
    risk = sum(1 for item in signals if item.action_hint == "avoid")
    return LocalStrategyNodeResult(
        node_id="theme_stock_bridge",
        layer="stock_theme_bridge",
        summary_state="theme_stock_aligned_seen" if aligned else ("theme_stock_risk_seen" if risk else "theme_stock_wait"),
        action_hint="probe" if aligned else "watch",
        signals=tuple(signals),
        evidence=(f"signals={len(signals)}", f"aligned={aligned}", f"wait={wait}", f"risk={risk}"),
        depends_on=("theme_relative", "stock_microstructure", "stock_profile", "stock_capital_profile", "weak_to_strong_repair"),
    )


def _validate_local_strategy_graph(nodes: tuple[LocalStrategyNodeResult, ...], signal_index: dict[str, LocalSignal]) -> tuple[str, ...]:
    node_ids = {node.node_id for node in nodes}
    spec_ids = set(LOCAL_STRATEGY_SPEC_MAP)
    issues: list[str] = []
    for missing in sorted(spec_ids - node_ids):
        issues.append(f"missing_node:{missing}")
    for extra in sorted(node_ids - spec_ids):
        issues.append(f"unknown_node:{extra}")
    for node in nodes:
        spec = LOCAL_STRATEGY_SPEC_MAP.get(node.node_id)
        expected_deps = set(spec.depends_on if spec is not None else ())
        actual_deps = set(node.depends_on)
        for dep in sorted(expected_deps - node_ids):
            issues.append(f"{node.node_id}.spec_unknown_dep:{dep}")
        for dep in sorted(actual_deps - node_ids):
            issues.append(f"{node.node_id}.unknown_dep:{dep}")
        if expected_deps and not expected_deps.issubset(actual_deps):
            missing = ",".join(sorted(expected_deps - actual_deps))
            if missing:
                issues.append(f"{node.node_id}.missing_declared_dep:{missing}")
        if spec is not None:
            allowed_states = set(spec.output_states)
            for signal in node.signals:
                if signal.state not in allowed_states:
                    issues.append(f"{node.node_id}.unknown_state:{signal.state}")
        for signal in node.signals:
            for dep in signal.depends_on:
                if dep not in node_ids and dep not in signal_index:
                    issues.append(f"{signal.signal_id}.unknown_dep:{dep}")
    return tuple(issues[:40])


def build_local_strategy_graph(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 260,
) -> LocalStrategyGraph:
    stock_micro = build_stock_microstructure_node(context, selection_contexts=selection_contexts, max_symbols=max_symbols)
    stock_profile = build_stock_profile_node(context, selection_contexts=selection_contexts, max_symbols=max_symbols)
    stock_capital = build_stock_capital_profile_node(context, selection_contexts=selection_contexts, max_symbols=max_symbols)
    weak_repair = build_weak_to_strong_repair_node(context, selection_contexts=selection_contexts, max_symbols=max_symbols)
    high_focus = build_high_focus_local_node(context)
    high_impact = build_theme_high_focus_impact_node(context, high_focus)
    auction_bucket = build_auction_bucket_local_node(context)
    hot_plate = build_hot_plate_context_node(context)
    yest_limit = build_yesterday_limit_pool_node(context)
    theme_internal = build_theme_internal_node(context)
    opening_validation = build_theme_opening_validation_node(context)
    theme_relative = build_theme_relative_node(
        context,
        theme_internal,
        hot_plate_node=hot_plate,
        opening_node=opening_validation,
        high_impact_node=high_impact,
    )
    theme_stock_bridge = build_theme_stock_bridge_node(
        context,
        stock_micro_node=stock_micro,
        stock_profile_node=stock_profile,
        stock_capital_node=stock_capital,
        repair_node=weak_repair,
        theme_relative_node=theme_relative,
        selection_contexts=selection_contexts,
        max_symbols=max_symbols,
    )
    nodes = (
        stock_micro,
        stock_profile,
        stock_capital,
        weak_repair,
        high_focus,
        high_impact,
        auction_bucket,
        hot_plate,
        yest_limit,
        theme_internal,
        opening_validation,
        theme_relative,
        theme_stock_bridge,
    )
    node_index = {node.node_id: node for node in nodes}
    signal_index = {signal.signal_id: signal for node in nodes for signal in node.signals}
    scope_index: dict[str, list[str]] = defaultdict(list)
    state_index: dict[str, list[str]] = defaultdict(list)
    for signal in signal_index.values():
        scope_index[f"{signal.scope_type}:{signal.scope}"].append(signal.signal_id)
        state_index[signal.state].append(signal.signal_id)
    dependency_issues = _validate_local_strategy_graph(nodes, signal_index)
    notes = tuple(f"{node.node_id}={node.summary_state}:{len(node.signals)}" for node in nodes) + tuple(
        f"dependency_issues={len(dependency_issues)}" for _ in (0,) if dependency_issues
    )
    return LocalStrategyGraph(
        trade_date=str(context.trade_date or ""),
        phase=_phase_name(context),
        nodes=nodes,
        node_index=node_index,
        signal_index=signal_index,
        scope_signal_index={key: tuple(value) for key, value in scope_index.items()},
        state_signal_index={key: tuple(value) for key, value in state_index.items()},
        dependency_issues=dependency_issues,
        notes=notes,
    )
