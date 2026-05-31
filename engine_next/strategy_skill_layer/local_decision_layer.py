from __future__ import annotations

from collections import Counter
from typing import Iterable

from engine_next.domain.decision_models import (
    DecisionBundle,
    DecisionTrace,
    HighFocusDecision,
    StockLocalDecision,
    ThemeLocalDecision,
    ThemeRelativeDecision,
)
from engine_next.domain.models import IntradayContext, StockSelectionContext, StockStateSnapshot, ThemeTradeFact
from engine_next.runtime.theme_name_resolver import resolve_theme_names
from engine_next.strategy_skill_layer.relative_amount import relative_amount_floor, top_symbols_by_amount
from engine_next.strategy_skill_layer.local_strategy_framework import build_local_strategy_graph
from engine_next.strategy_skill_layer.local_strategy_slicer import build_local_strategy_evidence_pack
from engine_next.strategy_skill_layer.stock_behavior import (
    classify_opening_entry_behavior,
    dedupe_text_items,
    opening_entry_behavior_label,
    stock_focus_evidence_labels,
)


def _phase_name(context: IntradayContext) -> str:
    return str(getattr(context.phase, "value", context.phase) or "")


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if float(denominator or 0.0) > 0.0 else 0.0


def _decision_id(*parts: str) -> str:
    return ":".join(str(part or "-") for part in parts)


def _trace(
    *,
    decision_id: str,
    decision_type: str,
    scope: str,
    context: IntradayContext,
    state: str,
    action_hint: str = "watch",
    confidence_bucket: str = "unknown",
    evidence_refs: tuple[str, ...] = (),
    lower_decision_refs: tuple[str, ...] = (),
    reason_codes: tuple[str, ...] = (),
    risk_tags: tuple[str, ...] = (),
    reject_reason: str = "",
    invalidation_points: tuple[str, ...] = (),
    evidence_summary: tuple[str, ...] = (),
) -> DecisionTrace:
    return DecisionTrace(
        decision_id=decision_id,
        decision_type=decision_type,
        scope=scope,
        phase=_phase_name(context),
        trade_date=str(context.trade_date or ""),
        state=state,
        action_hint=action_hint,
        confidence_bucket=confidence_bucket,
        evidence_refs=evidence_refs,
        lower_decision_refs=lower_decision_refs,
        reason_codes=reason_codes,
        risk_tags=risk_tags,
        reject_reason=reject_reason,
        invalidation_points=invalidation_points,
        evidence_summary=evidence_summary,
    )


def _primary_theme(snapshot: StockStateSnapshot) -> str:
    names = resolve_theme_names(snapshot)
    if names:
        return names[0]
    return str(snapshot.plate or "")


def _stock_role(snapshot: StockStateSnapshot, selection: StockSelectionContext | None = None) -> str:
    if selection is not None:
        if selection.is_true_leader:
            return "true_leader"
        if selection.is_front_row:
            return "front_row"
    if snapshot.leader_rank_in_theme <= 1 and snapshot.lb_days >= 1:
        return "true_leader"
    if snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1:
        return "front_row"
    if snapshot.leader_rank_in_theme <= 6:
        return "mid_follow"
    return "back_noise"


def _high_focus_scope(snapshots: Iterable[StockStateSnapshot]) -> tuple[StockStateSnapshot, ...]:
    rows = [
        snapshot
        for snapshot in snapshots
        if snapshot.lb_days >= 2
        or (snapshot.is_yest_limit and snapshot.lb_days >= 1)
        or snapshot.leader_rank_in_theme <= 1
    ]
    rows.sort(
        key=lambda snapshot: (
            -int(snapshot.lb_days or 0),
            int(snapshot.leader_rank_in_theme or 999),
            -float(snapshot.auction_amount or 0.0),
            -float(snapshot.amount_2m or 0.0),
        )
    )
    return tuple(rows[:30])


def build_stock_local_decisions(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
    max_symbols: int = 240,
) -> tuple[StockLocalDecision, ...]:
    """Build lightweight per-stock local roles for current candidate-like scope."""

    selection_map = {selection.symbol: selection for selection in selection_contexts}
    all_snapshots = tuple(context.stock_snapshots)
    amount_top_n = max(80, max_symbols // 2)
    amount_2m_top_symbols = top_symbols_by_amount(all_snapshots, "amount_2m", top_n=amount_top_n)
    auction_top_symbols = top_symbols_by_amount(all_snapshots, "auction_amount", top_n=amount_top_n)
    amount_2m_floor = relative_amount_floor(all_snapshots, "amount_2m", top_n=amount_top_n, fallback=0.0)
    scoped = [
        snapshot
        for snapshot in all_snapshots
        if snapshot.symbol in selection_map
        or snapshot.is_yest_limit
        or snapshot.lb_days >= 1
        or snapshot.leader_rank_in_theme <= 3
        or snapshot.symbol in amount_2m_top_symbols
        or snapshot.symbol in auction_top_symbols
    ]
    scoped.sort(
        key=lambda snapshot: (
            snapshot.symbol not in selection_map,
            int(snapshot.leader_rank_in_theme or 999),
            -int(snapshot.lb_days or 0),
            -float(snapshot.amount_2m or 0.0),
            -float(snapshot.auction_amount or 0.0),
        )
    )
    decisions: list[StockLocalDecision] = []
    for snapshot in scoped[:max_symbols]:
        selection = selection_map.get(snapshot.symbol)
        theme_name = selection.plate_name if selection is not None and selection.plate_name else _primary_theme(snapshot)
        role = _stock_role(snapshot, selection)
        behavior = classify_opening_entry_behavior(snapshot, amount_2m_floor=amount_2m_floor)
        evidence_labels = stock_focus_evidence_labels(
            snapshot,
            phase_label=_phase_name(context),
            selection=selection,
            all_snapshots=all_snapshots,
        )
        evidence_text = dedupe_text_items(evidence_labels, limit=4) or "仅有基础观察信号"
        state = "watch"
        action_hint = "watch"
        reason_codes: list[str] = [f"role_{role}", f"behavior_{behavior}"]
        risk_tags: list[str] = []
        if role in {"true_leader", "front_row"} and behavior in {"limit_attack", "volume_confirm", "low_open_repair"}:
            state = "candidate"
            action_hint = "probe"
        if behavior == "high_open_distribution":
            state = "risk"
            action_hint = "avoid"
            risk_tags.append("high_open_distribution")
        if selection is not None and selection.daily_height_bucket == "high" and role != "true_leader":
            risk_tags.append("high_dayk_risk")
        amount_ratio = _ratio(snapshot.amount_2m, snapshot.auction_amount)
        trace = _trace(
            decision_id=_decision_id("stock_local", snapshot.symbol, theme_name, _phase_name(context)),
            decision_type="stock_local",
            scope=snapshot.symbol,
            context=context,
            state=state,
            action_hint=action_hint,
            confidence_bucket="medium" if state == "candidate" else "low",
            evidence_refs=(
                f"stock.{snapshot.symbol}.amount_2m",
                f"stock.{snapshot.symbol}.speed_1m",
                f"stock.{snapshot.symbol}.auction_amount",
                f"stock.{snapshot.symbol}.leader_rank_in_theme",
                f"stock.{snapshot.symbol}.daily_height_bucket",
            ),
            reason_codes=tuple(reason_codes),
            risk_tags=tuple(risk_tags),
            reject_reason="risk_behavior" if action_hint == "avoid" else "",
            invalidation_points=("amount_2m_fades", "theme_fails_to_confirm") if state == "candidate" else (),
            evidence_summary=(
                f"amount_2m={snapshot.amount_2m:.0f}",
                f"speed_1m={snapshot.speed_1m:.4f}",
                f"amount_2m_vs_auction={amount_ratio:.2f}",
                f"rank={snapshot.leader_rank_in_theme}",
            ),
        )
        decisions.append(
            StockLocalDecision(
                trace=trace,
                symbol=snapshot.symbol,
                theme_name=theme_name,
                role_hint=role,
                entry_behavior=behavior,
                entry_behavior_label=opening_entry_behavior_label(behavior),
                evidence_text=evidence_text,
                evidence_labels=evidence_labels,
                local_rank=int(snapshot.leader_rank_in_theme or 999),
            )
        )
    return tuple(decisions)


def build_high_focus_decision(context: IntradayContext) -> HighFocusDecision:
    """Summarize high-level feedback without deciding the global script."""

    high_rows = _high_focus_scope(context.stock_snapshots)
    if not high_rows:
        return HighFocusDecision(
            trace=_trace(
                decision_id=_decision_id("high_focus", _phase_name(context)),
                decision_type="high_focus",
                scope="market",
                context=context,
                state="empty",
                action_hint="watch",
                reason_codes=("no_high_focus_scope",),
                evidence_summary=("no high-focus rows",),
            )
        )

    positive = [
        row
        for row in high_rows
        if row.current_pct >= 0.03
        and (row.amount_2m >= max(row.auction_amount * 0.8, 10_000_000) or row.is_locked or row.touched_limit_today)
    ]
    negative = [
        row
        for row in high_rows
        if row.current_pct <= -0.03
        or (row.open_pct >= 0.03 and row.current_pct <= row.open_pct - 0.04)
    ]
    promoted = [row for row in high_rows if row.is_locked or row.touched_limit_today or row.current_pct >= 0.095]
    failed_themes = tuple(sorted({_primary_theme(row) for row in negative if _primary_theme(row)}))
    drive_themes = tuple(sorted({_primary_theme(row) for row in positive if _primary_theme(row)}))
    feedback_state = "neutral"
    risk_spread = "none"
    action_hint = "watch"
    reason_codes: list[str] = []
    risk_tags: list[str] = []
    if len(positive) >= max(1, len(high_rows) // 3):
        feedback_state = "positive"
        action_hint = "watch"
        reason_codes.append("high_focus_positive")
    if len(negative) >= max(1, len(high_rows) // 3):
        feedback_state = "negative"
        risk_spread = "mild" if len(negative) < len(high_rows) // 2 else "heavy"
        action_hint = "avoid_chase"
        reason_codes.append("high_focus_negative")
        risk_tags.append("high_focus_risk_spread")
    promotion_quality = "weak"
    if len(promoted) >= max(1, len(high_rows) // 3):
        promotion_quality = "strong"
    elif promoted:
        promotion_quality = "normal"
    trace = _trace(
        decision_id=_decision_id("high_focus", _phase_name(context)),
        decision_type="high_focus",
        scope="market",
        context=context,
        state=feedback_state,
        action_hint=action_hint,
        confidence_bucket="medium",
        evidence_refs=(
            "high_focus.lb_days",
            "high_focus.auction_amount",
            "high_focus.current_pct",
            "high_focus.amount_2m",
            "high_focus.speed_1m",
        ),
        reason_codes=tuple(reason_codes or ("high_focus_mixed",)),
        risk_tags=tuple(risk_tags),
        invalidation_points=("high_leader_fades", "same_theme_spread_fails") if feedback_state == "positive" else (),
        evidence_summary=(
            f"high_count={len(high_rows)}",
            f"positive={len(positive)}",
            f"negative={len(negative)}",
            f"promoted={len(promoted)}",
        ),
    )
    return HighFocusDecision(
        trace=trace,
        feedback_state=feedback_state,
        promotion_quality=promotion_quality,
        risk_spread_level=risk_spread,
        leader_drive_themes=drive_themes[:5],
        failed_high_themes=failed_themes[:5],
    )


def _theme_candidates_for_fact(
    stock_decisions: tuple[StockLocalDecision, ...],
    theme_name: str,
) -> tuple[str, ...]:
    rows = [
        row
        for row in stock_decisions
        if row.theme_name == theme_name and row.trace.state == "candidate" and row.role_hint in {"true_leader", "front_row"}
    ]
    rows.sort(key=lambda row: (row.local_rank, row.symbol))
    return tuple(row.symbol for row in rows[:3])


def _theme_spread_level(fact: ThemeTradeFact) -> str:
    if fact.front_row_2m_pass_count >= 2 and fact.expansion_count >= 2:
        return "strong"
    if fact.front_row_2m_pass_count >= 1 or fact.expansion_count >= 1:
        return "normal"
    if fact.front_row_count > 0:
        return "weak"
    return "none"


def build_theme_local_decisions(
    context: IntradayContext,
    *,
    stock_decisions: tuple[StockLocalDecision, ...] = (),
    max_themes: int = 20,
) -> tuple[ThemeLocalDecision, ...]:
    """Build per-theme local decisions from existing session facts."""

    facts = tuple(context.session_facts.theme_trade_facts or ())
    if not facts:
        return ()
    hot_today_map = context.session_facts.hot_plate_today_map
    migration_map = context.session_facts.plate_migration_map
    sorted_facts = sorted(
        facts,
        key=lambda fact: (
            fact.yest_hot_rank,
            -float(fact.amount_2m_sum or 0.0),
            -float(fact.auction_amount or 0.0),
            -int(fact.yest_limit_count or 0),
            fact.plate_name,
        ),
    )
    decisions: list[ThemeLocalDecision] = []
    for fact in sorted_facts[:max_themes]:
        hot_fact = hot_today_map.get(fact.plate_name)
        migration = migration_map.get(fact.plate_name)
        amount_ratio = _ratio(fact.amount_2m_sum, fact.auction_amount)
        spread_level = _theme_spread_level(fact)
        risk_tags: list[str] = []
        reason_codes: list[str] = []
        local_script = "mixed"
        validation = "watch_like"
        action_hint = "watch"
        if fact.high_open_fail_count >= max(1, fact.front_row_count):
            local_script = "distribution"
            validation = "falsified_like"
            action_hint = "avoid"
            risk_tags.append("front_row_distribution")
            reason_codes.append("high_open_fail")
        elif fact.low_open_repair_count > 0 and amount_ratio >= 0.8:
            local_script = "repair"
            validation = "confirmed_like"
            action_hint = "probe"
            reason_codes.append("low_open_repair")
        elif spread_level in {"strong", "normal"} and fact.amount_2m_sum > 0:
            local_script = "extension" if fact.yest_hot_rank <= 50 else "rotation_candidate"
            validation = "confirmed_like"
            action_hint = "probe"
            reason_codes.append("front_spread")
        elif fact.amount_2m_sum > 0 and spread_level in {"none", "weak"}:
            local_script = "fakeout"
            validation = "watch_like"
            action_hint = "watch"
            risk_tags.append("weak_spread")
            reason_codes.append("amount_without_spread")
        if migration is not None and migration.net_inflow_yi_delta < -3.0:
            risk_tags.append("net_inflow_withdrawing")
        candidates = _theme_candidates_for_fact(stock_decisions, fact.plate_name)
        drive_type = "leader_only" if fact.leader_count > 0 and fact.expansion_count <= 0 else "group_spread"
        trace = _trace(
            decision_id=_decision_id("theme_local", fact.plate_name, _phase_name(context)),
            decision_type="theme_local",
            scope=fact.plate_name,
            context=context,
            state=validation,
            action_hint=action_hint,
            confidence_bucket="medium" if validation == "confirmed_like" else "low",
            evidence_refs=(
                f"theme.{fact.plate_name}.auction_amount",
                f"theme.{fact.plate_name}.amount_2m_sum",
                f"theme.{fact.plate_name}.front_row_2m_pass_count",
                f"theme.{fact.plate_name}.expansion_count",
                f"theme.{fact.plate_name}.yest_hot_rank",
            ),
            lower_decision_refs=tuple(f"stock_local:{symbol}" for symbol in candidates),
            reason_codes=tuple(reason_codes or ("theme_mixed",)),
            risk_tags=tuple(risk_tags),
            reject_reason="weak_spread" if local_script == "fakeout" else "",
            invalidation_points=("front_row_fades", "mid_follow_missing") if validation == "confirmed_like" else (),
            evidence_summary=(
                f"amount_2m_sum={fact.amount_2m_sum:.0f}",
                f"amount_2m_vs_auction={amount_ratio:.2f}",
                f"spread={spread_level}",
                f"yest_hot_rank={fact.yest_hot_rank}",
                f"hot_rank={hot_fact.rank if hot_fact else 999}",
            ),
        )
        decisions.append(
            ThemeLocalDecision(
                trace=trace,
                theme_name=fact.plate_name,
                local_script_hint=local_script,
                local_validation_hint=validation,
                spread_level=spread_level,
                leader_drive_type=drive_type,
                top_local_candidates=candidates,
            )
        )
    return tuple(decisions)


def _ordered_existing(names: Iterable[str], existing: set[str]) -> tuple[str, ...]:
    output: list[str] = []
    for name in names:
        text = str(name or "")
        if text and text in existing and text not in output:
            output.append(text)
    return tuple(output)


def build_theme_relative_decision(
    context: IntradayContext,
    *,
    theme_decisions: tuple[ThemeLocalDecision, ...] = (),
) -> ThemeRelativeDecision:
    """Compare local theme decisions horizontally before global hypothesis selection."""

    existing = {decision.theme_name for decision in theme_decisions if decision.theme_name}
    migrating_in = _ordered_existing(getattr(context.market_summary, "migrating_in_plates", ()) or (), existing)
    migrating_out = _ordered_existing(getattr(context.market_summary, "migrating_out_plates", ()) or (), existing)
    confirmed = [
        decision
        for decision in theme_decisions
        if decision.local_validation_hint == "confirmed_like"
    ]
    confirmed.sort(
        key=lambda decision: (
            decision.theme_name not in migrating_in,
            decision.local_script_hint not in {"extension", "rotation_candidate", "repair"},
            decision.spread_level != "strong",
            decision.leader_drive_type == "leader_only",
            decision.theme_name,
        )
    )
    leading = tuple(decision.theme_name for decision in confirmed if decision.local_script_hint == "extension")[:5]
    rising_base = [
        decision.theme_name
        for decision in confirmed
        if decision.local_script_hint in {"rotation_candidate", "repair"} and decision.theme_name not in leading
    ]
    rising = tuple(dict.fromkeys((*migrating_in, *rising_base)))[:5]
    fading = tuple(
        decision.theme_name
        for decision in theme_decisions
        if decision.local_script_hint == "distribution" or decision.theme_name in migrating_out
    )[:5]
    fake_rotation = tuple(
        decision.theme_name
        for decision in theme_decisions
        if decision.local_script_hint == "fakeout"
        or ("weak_spread" in decision.trace.risk_tags and decision.theme_name not in rising)
    )[:5]
    mainline_candidates = tuple(dict.fromkeys((*leading, *(name for name in rising if name not in fake_rotation))))[:5]
    rotation_candidates = tuple(name for name in rising if name not in fading and name not in fake_rotation)[:5]
    risk_themes = tuple(dict.fromkeys((*fading, *fake_rotation, *migrating_out)))[:8]
    state = "observe"
    action_hint = "watch"
    reason_codes: tuple[str, ...] = ("relative_no_confirmed_theme",)
    risk_tags: tuple[str, ...] = ()
    if mainline_candidates:
        state = "theme_path_found"
        action_hint = "probe"
        reason_codes = ("relative_mainline_or_rotation",)
    if risk_themes and not mainline_candidates:
        state = "risk_first"
        action_hint = "avoid_chase"
        reason_codes = ("relative_risk_first",)
        risk_tags = ("theme_relative_risk",)
    trace = _trace(
        decision_id=_decision_id("theme_relative", _phase_name(context)),
        decision_type="theme_relative",
        scope="market",
        context=context,
        state=state,
        action_hint=action_hint,
        confidence_bucket="medium" if mainline_candidates else "low",
        evidence_refs=tuple(decision.trace.decision_id for decision in theme_decisions[:5]),
        lower_decision_refs=tuple(decision.trace.decision_id for decision in theme_decisions[:8]),
        reason_codes=reason_codes,
        risk_tags=risk_tags,
        reject_reason="no_relative_theme_path" if not mainline_candidates else "",
        invalidation_points=("relative_leader_fades", "migration_reverses") if mainline_candidates else (),
        evidence_summary=(
            f"leading={','.join(leading[:3]) or '-'}",
            f"rising={','.join(rising[:3]) or '-'}",
            f"fading={','.join(fading[:3]) or '-'}",
            f"fake_rotation={','.join(fake_rotation[:3]) or '-'}",
            f"migrating_in={','.join(migrating_in[:3]) or '-'}",
        ),
    )
    return ThemeRelativeDecision(
        trace=trace,
        leading_themes=leading,
        rising_themes=rising,
        fading_themes=fading,
        fake_rotation_themes=fake_rotation,
        migration_candidates=migrating_in,
        mainline_candidates=mainline_candidates,
        rotation_candidates=rotation_candidates,
        risk_themes=risk_themes,
    )


def build_local_decision_bundle(
    context: IntradayContext,
    *,
    selection_contexts: Iterable[StockSelectionContext] = (),
) -> DecisionBundle:
    """Build local evidence and decisions for the playbook-first recommendation path."""

    local_strategy_graph = build_local_strategy_graph(context, selection_contexts=selection_contexts)
    local_evidence_pack = build_local_strategy_evidence_pack(local_strategy_graph)
    stock_decisions = build_stock_local_decisions(context, selection_contexts=selection_contexts)
    high_focus = build_high_focus_decision(context)
    theme_decisions = build_theme_local_decisions(context, stock_decisions=stock_decisions)
    theme_relative = build_theme_relative_decision(context, theme_decisions=theme_decisions)
    theme_counter = Counter(decision.local_validation_hint for decision in theme_decisions)
    local_probe_themes = tuple(signal.scope for signal in local_strategy_graph.top_signals(scope_type="theme", action_hints=("probe", "support"), limit=5))
    local_risk_themes = tuple(signal.scope for signal in local_strategy_graph.top_signals(scope_type="theme", action_hints=("avoid",), limit=5))
    local_aligned_stocks = tuple(
        summary.scope for summary in local_evidence_pack.stock_alignments[:5]
    )
    local_stock_risks = tuple(
        summary.scope for summary in local_evidence_pack.stock_risks[:5]
    )
    notes = (
        f"stock_local={len(stock_decisions)}",
        f"theme_local={len(theme_decisions)}",
        f"theme_confirmed_like={theme_counter.get('confirmed_like', 0)}",
        f"theme_relative={theme_relative.trace.state}",
        f"local_strategy_nodes={len(local_strategy_graph.nodes)}",
        f"local_probe_themes={','.join(local_probe_themes) or '-'}",
        f"local_risk_themes={','.join(local_risk_themes) or '-'}",
        f"local_aligned_stocks={','.join(local_aligned_stocks) or '-'}",
        f"local_stock_risks={','.join(local_stock_risks) or '-'}",
        f"local_dependency_issues={len(local_strategy_graph.dependency_issues)}",
        f"local_evidence_pack={';'.join(local_evidence_pack.notes[:4])}",
    )
    return DecisionBundle(
        stock_local_decisions=stock_decisions,
        local_strategy_graph=local_strategy_graph,
        local_strategy_evidence_pack=local_evidence_pack,
        high_focus_decision=high_focus,
        theme_local_decisions=theme_decisions,
        theme_relative_decision=theme_relative,
        notes=notes,
    )
