from __future__ import annotations

from dataclasses import replace

from engine_next.domain.local_strategy_models import LocalMetric, LocalSignal, LocalStrategyScopeSummary
from engine_next.domain.decision_models import (
    DecisionTrace,
    DecisionBundle,
    FinalCandidateDecision,
    GlobalMarketDecision,
    HypothesisValidation,
    MarketHypothesis,
    PlaybookControlMatrix,
    PlaybookControlRow,
    ShadowTakeoverDecision,
    StockLocalDecision,
    ThemeLocalDecision,
)
from engine_next.domain.models import IntradayContext
from engine_next.strategy_skill_layer.playbook_control import playbook_for_candidate_path


def _phase_name(context: IntradayContext) -> str:
    return str(getattr(context.phase, "value", context.phase) or "")


def _hypothesis_id(script: str, scope: str, context: IntradayContext) -> str:
    return f"hypothesis:{script}:{scope or 'market'}:{_phase_name(context)}"


def _theme_ref(theme: ThemeLocalDecision) -> str:
    return theme.trace.decision_id


def _playbook_profile(script: str) -> tuple[str, str, str, str]:
    profiles = {
        "mainline_extension": (
            "hot-sector persistence",
            "herding/FOMO after confirmed spread",
            "only front-row or true leader; avoid late high-open followers",
            "front-row 2m amount and mid-follow spread must hold",
        ),
        "capital_rotation": (
            "sector rotation",
            "capital seeks lower resistance after old path weakens",
            "probe only; reject if old mainline reclaims or new spread fades",
            "migrating-in theme plus 2m amount expansion",
        ),
        "high_level_distribution": (
            "dragon-head risk control",
            "loss aversion and break-even selling from high-level failure",
            "risk-off; no chasing until leader repair is verified",
            "high-focus stocks stop falling or repair with volume",
        ),
        "fakeout_pulse": (
            "auction strength miss detection",
            "FOMO trap with amount but no group behavior",
            "observe only unless spread upgrades to confirmed",
            "front-row follow and theme spread must appear together",
        ),
        "local_pack_theme_opportunity": (
            "local evidence pack mainline",
            "capital follows the clearest local evidence cluster",
            "only act when stock bridge also aligns; otherwise watch",
            "theme relative path plus stock 2m/profile/capital alignment",
        ),
        "local_pack_theme_risk": (
            "local evidence pack risk",
            "capital avoids fading/fakeout paths before they spread",
            "no chasing in risk theme unless absolute true leader repairs",
            "theme risk signal must recede before re-entry",
        ),
        "local_pack_high_pressure": (
            "dragon-head feedback risk",
            "high-board failure can trigger loss aversion and group deleveraging",
            "block same-theme attack unless pressure repairs or absolute leader reclaims",
            "high-focus pressure rate and sample symbols must improve",
        ),
        "local_pack_high_pressure_repair": (
            "dragon-head pressure repair",
            "absolute leader repair can pull risk appetite back from panic",
            "probe only; reject if 2m repair fades or breadth fails",
            "absolute front-row repair plus 2m amount and capital profile not vetoing",
        ),
    }
    return profiles.get(
        script,
        (
            "unknown",
            "unknown",
            "watch only until classified",
            "needs local evidence",
        ),
    )


def _build_hypothesis(
    *,
    context: IntradayContext,
    script: str,
    theme: ThemeLocalDecision | None,
    claim: str,
    required_validations: tuple[str, ...],
    invalidation_points: tuple[str, ...],
    trigger_refs: tuple[str, ...] = (),
    extra_local_refs: tuple[str, ...] = (),
) -> MarketHypothesis:
    scope = theme.theme_name if theme is not None else "market"
    local_refs = ((_theme_ref(theme),) if theme is not None else ()) + extra_local_refs
    evidence_summary = theme.trace.evidence_summary if theme is not None else ()
    playbook, psychology, risk_constraint, microstructure = _playbook_profile(script)
    return MarketHypothesis(
        hypothesis_id=_hypothesis_id(script, scope, context),
        script=script,
        claim=claim,
        phase=_phase_name(context),
        scope=scope,
        playbook=playbook,
        psychology=psychology,
        risk_constraint=risk_constraint,
        microstructure_requirement=microstructure,
        trigger_refs=trigger_refs or (local_refs[:1] if local_refs else ()),
        source_local_decision_refs=local_refs,
        required_validations=required_validations,
        invalidation_points=invalidation_points,
        evidence_summary=evidence_summary,
    )


def _validate_hypothesis(
    hypothesis: MarketHypothesis,
    *,
    theme: ThemeLocalDecision | None,
    high_focus_state: str,
) -> HypothesisValidation:
    passed: list[str] = []
    failed: list[str] = []
    missing: list[str] = []
    if theme is None:
        missing.append("theme_local_decision")
    else:
        if theme.local_validation_hint == "confirmed_like":
            passed.append("theme_local_confirmed")
        elif theme.local_validation_hint == "falsified_like":
            failed.append("theme_local_falsified")
        else:
            missing.append("theme_local_pending")
        if theme.spread_level in {"strong", "normal"}:
            passed.append("theme_spread")
        else:
            failed.append("theme_spread")
        if theme.top_local_candidates:
            passed.append("profit_center_candidate")
        else:
            missing.append("profit_center_candidate")

    if hypothesis.script in {"mainline_extension", "capital_rotation"}:
        if high_focus_state == "negative":
            failed.append("high_focus_feedback")
        elif high_focus_state in {"positive", "neutral"}:
            passed.append("high_focus_feedback")
        else:
            missing.append("high_focus_feedback")
    if hypothesis.script == "high_level_distribution" and high_focus_state == "negative":
        passed.append("high_focus_distribution")
    if hypothesis.script == "fakeout_pulse" and theme is not None:
        if theme.local_script_hint in {"fakeout", "distribution"}:
            passed.append("fakeout_or_distribution")
        else:
            failed.append("fakeout_or_distribution")

    result = "pending"
    next_action = "watch"
    if failed:
        result = "rejected" if hypothesis.script in {"mainline_extension", "capital_rotation"} else "partial"
        next_action = "avoid_chase"
    elif passed and not missing:
        result = "confirmed"
        next_action = "probe" if hypothesis.script in {"mainline_extension", "capital_rotation"} else "watch"
    elif passed:
        result = "partial"
        next_action = "watch"
    return HypothesisValidation(
        hypothesis_id=hypothesis.hypothesis_id,
        result=result,
        passed_checks=tuple(passed),
        failed_checks=tuple(failed),
        missing_checks=tuple(missing),
        evidence_refs=hypothesis.trigger_refs,
        lower_decision_refs=hypothesis.source_local_decision_refs,
        next_action_hint=next_action,
    )


def _theme_order_index(theme_name: str, ordered_names: tuple[str, ...]) -> int:
    try:
        return ordered_names.index(theme_name)
    except ValueError:
        return 999


def _summary_ref(prefix: str, summary: LocalStrategyScopeSummary) -> str:
    return f"{prefix}:{summary.scope_type}:{summary.scope}"


def _metric_value(metrics: tuple[LocalMetric, ...], name: str, default: str = "") -> str:
    for metric in metrics:
        if metric.name == name:
            return str(metric.value)
    return default


def _bridge_signal_for_symbol(decision_bundle: DecisionBundle, symbol: str) -> LocalSignal | None:
    graph = decision_bundle.local_strategy_graph
    if graph is None:
        return None
    for signal in graph.signals_for_scope("stock", symbol):
        if signal.node_id == "theme_stock_bridge":
            return signal
    return None


def _build_local_pack_hypotheses(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
) -> tuple[tuple[MarketHypothesis, HypothesisValidation], ...]:
    pack = decision_bundle.local_strategy_evidence_pack
    if pack is None:
        return ()
    rows: list[tuple[MarketHypothesis, HypothesisValidation]] = []
    aligned_themes: set[str] = set()
    aligned_theme_risks: dict[str, tuple[str, ...]] = {}
    pressure_repair_themes: set[str] = set()
    graph = decision_bundle.local_strategy_graph
    if graph is not None:
        for summary in pack.stock_alignments:
            bridge = _bridge_signal_for_symbol(decision_bundle, summary.scope)
            theme_name = _metric_value(bridge.metrics, "theme") if bridge is not None else ""
            if theme_name:
                aligned_themes.add(theme_name)
                if bridge is not None and bridge.state == "theme_stock_pressure_repair":
                    pressure_repair_themes.add(theme_name)
                if bridge is not None and bridge.risk_tags:
                    aligned_theme_risks[theme_name] = bridge.risk_tags
    theme_risk_scopes = {summary.scope for summary in pack.theme_risks}
    high_pressure_scopes = {
        summary.scope
        for summary in pack.high_pressure_alerts
        if summary.action_hint in {"avoid", "avoid_chase"} or summary.avoid_count > 0
    }
    emotion_risk = any(summary.action_hint in {"avoid", "avoid_chase"} or summary.avoid_count > 0 for summary in pack.emotion_alerts)
    for summary in pack.theme_opportunities[:3]:
        has_stock_alignment = summary.scope in aligned_themes
        blocked_by_theme_risk = summary.scope in theme_risk_scopes
        blocked_by_high_pressure = summary.scope in high_pressure_scopes and summary.scope not in pressure_repair_themes
        blocked_by_emotion = emotion_risk
        bridge_risks = aligned_theme_risks.get(summary.scope, ())
        playbook, psychology, risk_constraint, microstructure = _playbook_profile("local_pack_theme_opportunity")
        hypothesis = MarketHypothesis(
            hypothesis_id=_hypothesis_id("local_pack_theme_opportunity", summary.scope, context),
            script="local_pack_theme_opportunity",
            claim=f"{summary.scope} has local strategy opportunity evidence",
            phase=_phase_name(context),
            scope=summary.scope,
            playbook=playbook,
            psychology=psychology,
            risk_constraint=risk_constraint,
            microstructure_requirement=microstructure,
            trigger_refs=(_summary_ref("local_theme", summary),),
            source_local_decision_refs=tuple(summary.states),
            required_validations=("local_theme_opportunity", "stock_bridge_alignment"),
            invalidation_points=("local_theme_fades", "aligned_stock_fades"),
            evidence_summary=summary.evidence,
        )
        result = (
            "confirmed"
            if has_stock_alignment
            and summary.action_hint in {"probe", "support"}
            and not blocked_by_theme_risk
            and not blocked_by_high_pressure
            and not blocked_by_emotion
            and not bridge_risks
            else "partial"
        )
        missing_checks: list[str] = []
        failed_checks: list[str] = []
        if not has_stock_alignment:
            missing_checks.append("stock_bridge_alignment")
        if blocked_by_theme_risk:
            failed_checks.append("same_theme_local_risk")
        if blocked_by_high_pressure:
            failed_checks.append("high_focus_pressure")
        if blocked_by_emotion:
            failed_checks.append("emotion_bucket_risk")
        if bridge_risks:
            failed_checks.append("stock_bridge_risk")
        validation = HypothesisValidation(
            hypothesis_id=hypothesis.hypothesis_id,
            result=result,
            passed_checks=("local_theme_opportunity", "stock_bridge_alignment") if has_stock_alignment else ("local_theme_opportunity",),
            failed_checks=tuple(failed_checks),
            missing_checks=tuple(missing_checks),
            evidence_refs=hypothesis.trigger_refs,
            lower_decision_refs=hypothesis.source_local_decision_refs,
            next_action_hint="probe" if result == "confirmed" else "watch",
        )
        rows.append((hypothesis, validation))
    for summary in pack.high_pressure_alerts[:3]:
        has_pressure_repair = summary.scope in pressure_repair_themes
        if has_pressure_repair:
            playbook, psychology, risk_constraint, microstructure = _playbook_profile("local_pack_high_pressure_repair")
            hypothesis = MarketHypothesis(
                hypothesis_id=_hypothesis_id("local_pack_high_pressure_repair", summary.scope, context),
                script="local_pack_high_pressure_repair",
                claim=f"{summary.scope} has absolute-leader repair against high-focus pressure",
                phase=_phase_name(context),
                scope=summary.scope,
                playbook=playbook,
                psychology=psychology,
                risk_constraint=risk_constraint,
                microstructure_requirement=microstructure,
                trigger_refs=(_summary_ref("high_pressure_repair", summary),),
                source_local_decision_refs=tuple(summary.states),
                required_validations=("high_focus_pressure", "absolute_leader_reclaims", "stock_bridge_alignment"),
                invalidation_points=("repair_2m_fades", "pressure_spreads_again"),
                evidence_summary=summary.evidence,
            )
            validation = HypothesisValidation(
                hypothesis_id=hypothesis.hypothesis_id,
                result="confirmed",
                passed_checks=("high_focus_pressure", "absolute_leader_reclaims", "stock_bridge_alignment"),
                evidence_refs=hypothesis.trigger_refs,
                lower_decision_refs=hypothesis.source_local_decision_refs,
                next_action_hint="probe",
            )
            rows.append((hypothesis, validation))
            continue
        if summary.action_hint not in {"avoid", "avoid_chase"} and summary.avoid_count <= 0:
            continue
        playbook, psychology, risk_constraint, microstructure = _playbook_profile("local_pack_high_pressure")
        hypothesis = MarketHypothesis(
            hypothesis_id=_hypothesis_id("local_pack_high_pressure", summary.scope, context),
            script="local_pack_high_pressure",
            claim=f"{summary.scope} has high-focus pressure that can block same-theme attack",
            phase=_phase_name(context),
            scope=summary.scope,
            playbook=playbook,
            psychology=psychology,
            risk_constraint=risk_constraint,
            microstructure_requirement=microstructure,
            trigger_refs=(_summary_ref("high_pressure", summary),),
            source_local_decision_refs=tuple(summary.states),
            required_validations=("high_focus_pressure",),
            invalidation_points=("high_pressure_repairs", "absolute_leader_reclaims"),
            evidence_summary=summary.evidence,
        )
        validation = HypothesisValidation(
            hypothesis_id=hypothesis.hypothesis_id,
            result="rejected",
            failed_checks=("high_focus_pressure",),
            evidence_refs=hypothesis.trigger_refs,
            lower_decision_refs=hypothesis.source_local_decision_refs,
            next_action_hint="avoid",
        )
        rows.append((hypothesis, validation))
    for summary in pack.theme_risks[:3]:
        playbook, psychology, risk_constraint, microstructure = _playbook_profile("local_pack_theme_risk")
        hypothesis = MarketHypothesis(
            hypothesis_id=_hypothesis_id("local_pack_theme_risk", summary.scope, context),
            script="local_pack_theme_risk",
            claim=f"{summary.scope} has local strategy risk evidence",
            phase=_phase_name(context),
            scope=summary.scope,
            playbook=playbook,
            psychology=psychology,
            risk_constraint=risk_constraint,
            microstructure_requirement=microstructure,
            trigger_refs=(_summary_ref("local_theme_risk", summary),),
            source_local_decision_refs=tuple(summary.states),
            required_validations=("local_theme_risk",),
            invalidation_points=("risk_theme_repairs",),
            evidence_summary=summary.evidence,
        )
        validation = HypothesisValidation(
            hypothesis_id=hypothesis.hypothesis_id,
            result="rejected",
            failed_checks=("local_theme_risk",),
            evidence_refs=hypothesis.trigger_refs,
            lower_decision_refs=hypothesis.source_local_decision_refs,
            next_action_hint="avoid",
        )
        rows.append((hypothesis, validation))
    return tuple(rows)


def _build_global_decision(
    context: IntradayContext,
    hypotheses: tuple[MarketHypothesis, ...],
    validations: tuple[HypothesisValidation, ...],
    high_focus_state: str,
    theme_relative_trace: DecisionTrace | None = None,
    relative_risk_themes: tuple[str, ...] = (),
) -> GlobalMarketDecision:
    validation_map = {validation.hypothesis_id: validation for validation in validations}
    confirmed_attack: list[MarketHypothesis] = []
    partial_watch: list[MarketHypothesis] = []
    rejected_risk: list[MarketHypothesis] = []
    for hypothesis in hypotheses:
        validation = validation_map.get(hypothesis.hypothesis_id)
        if validation is None:
            continue
        if validation.result == "confirmed" and hypothesis.script in {
            "mainline_extension",
            "capital_rotation",
            "local_pack_theme_opportunity",
            "local_pack_high_pressure_repair",
        }:
            confirmed_attack.append(hypothesis)
        elif validation.result in {"partial", "pending"}:
            partial_watch.append(hypothesis)
        elif validation.result == "rejected":
            rejected_risk.append(hypothesis)

    main_attack = confirmed_attack[0].scope if confirmed_attack else ""
    pressure_repair_attack = any(item.script == "local_pack_high_pressure_repair" for item in confirmed_attack)
    secondary = tuple(item.scope for item in confirmed_attack[1:3] if item.scope and item.scope != "market")
    watch = tuple(item.scope for item in partial_watch[:5] if item.scope and item.scope != "market")
    avoid = tuple(item.scope for item in rejected_risk[:5] if item.scope and item.scope != "market")
    market_script = "observe"
    action_hint = "watch"
    position_cap = 0.0
    reason_codes: tuple[str, ...] = ("no_confirmed_hypothesis",)
    risk_tags: tuple[str, ...] = ()
    migrating_in = tuple(getattr(context.market_summary, "migrating_in_plates", ()) or ())
    migrating_out = tuple(getattr(context.market_summary, "migrating_out_plates", ()) or ())
    if main_attack:
        market_script = "attack_confirmed"
        action_hint = "probe"
        position_cap = 0.12 if pressure_repair_attack else (0.2 if high_focus_state == "negative" else 0.35)
        reason_list = ["validated_attack_hypothesis"]
        risk_list: list[str] = []
        if pressure_repair_attack:
            reason_list.append("pressure_repair_probe")
            risk_list.append("risk_capped_pressure_repair")
        if main_attack in migrating_in:
            reason_list.append("sector_flow_migrating_in")
        if main_attack in migrating_out:
            risk_list.append("sector_flow_migrating_out")
        if main_attack in relative_risk_themes:
            risk_list.append("relative_risk_theme")
        reason_codes = tuple(reason_list)
        risk_tags = tuple(risk_list)
    elif high_focus_state == "negative":
        market_script = "risk_off"
        action_hint = "avoid_chase"
        position_cap = 0.0
        reason_codes = ("high_focus_negative",)
        risk_tags = ("high_focus_risk",)
    elif watch:
        market_script = "watch_validation"
        action_hint = "watch"
        position_cap = 0.1
        reason_codes = ("partial_hypothesis",)
    elif relative_risk_themes:
        market_script = "risk_off"
        action_hint = "avoid_chase"
        position_cap = 0.0
        reason_codes = ("relative_risk_first",)
        risk_tags = ("theme_relative_risk",)

    lower_refs = tuple(
        ref
        for hypothesis in hypotheses[:4]
        for ref in (hypothesis.hypothesis_id,)
        if ref
    )
    if theme_relative_trace is not None:
        lower_refs = (theme_relative_trace.decision_id, *lower_refs)
    trace = DecisionTrace(
        decision_id=f"global_market:{context.trade_date}:{_phase_name(context)}",
        decision_type="global_market",
        scope="market",
        phase=_phase_name(context),
        trade_date=str(context.trade_date or ""),
        state=market_script,
        action_hint=action_hint,
        confidence_bucket="medium" if main_attack else "low",
        evidence_refs=tuple(hypothesis.trigger_refs[0] for hypothesis in hypotheses[:5] if hypothesis.trigger_refs),
        lower_decision_refs=lower_refs,
        reason_codes=reason_codes,
        risk_tags=risk_tags,
        reject_reason="no_validated_attack" if not main_attack else "",
        invalidation_points=("confirmed_theme_fades", "risk_spread_expands") if main_attack else (),
        evidence_summary=(
            f"confirmed_attack={len(confirmed_attack)}",
            f"partial_watch={len(partial_watch)}",
            f"rejected_risk={len(rejected_risk)}",
            f"high_focus={high_focus_state}",
            f"pressure_repair={pressure_repair_attack}",
            f"migrating_in={','.join(migrating_in[:3]) or '-'}",
            f"migrating_out={','.join(migrating_out[:3]) or '-'}",
            f"relative_risk={','.join(relative_risk_themes[:3]) or '-'}",
        ),
    )
    return GlobalMarketDecision(
        trace=trace,
        market_script=market_script,
        main_attack_theme=main_attack,
        secondary_themes=secondary,
        watch_themes=watch,
        avoid_themes=avoid,
        position_cap=position_cap,
    )


def _build_final_candidates(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
    global_decision: GlobalMarketDecision,
) -> tuple[FinalCandidateDecision, ...]:
    theme_relative = decision_bundle.theme_relative_decision
    mainline_order = theme_relative.mainline_candidates if theme_relative is not None else ()
    rotation_order = theme_relative.rotation_candidates if theme_relative is not None else ()
    risk_themes = theme_relative.risk_themes if theme_relative is not None else ()
    pack = decision_bundle.local_strategy_evidence_pack
    local_pack_themes = tuple(summary.scope for summary in pack.theme_opportunities) if pack is not None else ()
    local_pack_risk_themes = tuple(summary.scope for summary in pack.theme_risks) if pack is not None else ()
    local_aligned_symbols = {summary.scope for summary in pack.stock_alignments} if pack is not None else set()
    target_theme_items: list[str] = []
    if global_decision.main_attack_theme:
        target_theme_items.append(global_decision.main_attack_theme)
    target_theme_items.extend(mainline_order)
    target_theme_items.extend(rotation_order)
    target_theme_items.extend(local_pack_themes)
    target_themes = tuple(dict.fromkeys(target_theme_items))
    if not target_themes:
        return ()
    raw_candidates: list[StockLocalDecision] = [
        decision
        for decision in decision_bundle.stock_local_decisions
        if decision.theme_name in target_themes
        and (decision.trace.state == "candidate" or decision.symbol in local_aligned_symbols)
        and decision.role_hint in {"true_leader", "front_row"}
        and decision.entry_behavior not in {"high_open_distribution", "weak_follow"}
    ]
    if not raw_candidates:
        raw_candidates = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if decision.theme_name in target_themes
            and decision.role_hint in {"true_leader", "front_row"}
            and decision.entry_behavior != "high_open_distribution"
        ]
    ranked: list[tuple[tuple[int, int, int, int, str], StockLocalDecision, str, str, str]] = []
    for decision in raw_candidates:
        bridge_signal = _bridge_signal_for_symbol(decision_bundle, decision.symbol)
        is_local_aligned = decision.symbol in local_aligned_symbols
        bridge_state = bridge_signal.state if bridge_signal is not None else ""
        if decision.theme_name in risk_themes + local_pack_risk_themes and decision.role_hint != "true_leader":
            path_type = "risk_theme_watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "elevated"
            path_rank = 8
        elif is_local_aligned and bridge_state == "theme_stock_pressure_repair":
            path_type = "local_pack_pressure_repair"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "elevated"
            path_rank = 1
        elif is_local_aligned and decision.theme_name == global_decision.main_attack_theme:
            path_type = "local_pack_main_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 0
        elif is_local_aligned and decision.theme_name in local_pack_themes:
            path_type = "local_pack_aligned"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 2 + _theme_order_index(decision.theme_name, local_pack_themes)
        elif decision.theme_name == global_decision.main_attack_theme:
            path_type = "main_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 0
        elif decision.theme_name in mainline_order:
            path_type = "mainline_follow"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 1 + _theme_order_index(decision.theme_name, mainline_order)
        elif decision.theme_name in rotation_order:
            path_type = "rotation_probe"
            action = "probe"
            bucket = "shadow_rotation"
            risk_level = "normal"
            path_rank = 20 + _theme_order_index(decision.theme_name, rotation_order)
        else:
            path_type = "watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal"
            path_rank = 50
        if global_decision.market_script == "risk_off":
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "elevated"
            path_rank += 100
            if path_type in {"main_attack", "mainline_follow", "rotation_probe"}:
                path_type = f"{path_type}_risk_off"
        elif decision.trace.state != "candidate":
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal"
            if path_type in {"main_attack", "mainline_follow", "rotation_probe"} and not is_local_aligned:
                path_type = f"{path_type}_unconfirmed"
        if bridge_signal is not None and bridge_signal.action_hint in {"avoid", "avoid_chase"}:
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "elevated"
            path_type = f"{path_type}_bridge_risk"
        behavior_rank = 0 if decision.entry_behavior in {"volume_confirm", "low_open_repair", "limit_attack"} else 1
        role_rank = 0 if decision.role_hint == "true_leader" else 1
        ranked.append(
            (
                (
                    path_rank,
                    role_rank,
                    behavior_rank,
                    decision.local_rank,
                    decision.symbol,
                ),
                decision,
                path_type,
                action,
                risk_level,
            )
        )
    ranked.sort(key=lambda item: item[0])
    selected: list[tuple[StockLocalDecision, str, str, str, str, int]] = []
    seen_symbols: set[str] = set()
    per_theme_count: dict[str, int] = {}
    for sort_key, decision, path_type, action, risk_level in ranked:
        if decision.symbol in seen_symbols:
            continue
        theme_count = per_theme_count.get(decision.theme_name, 0)
        if theme_count >= 2:
            continue
        bucket = "shadow_rotation" if path_type == "rotation_probe" else ("shadow_watch" if action == "watch" else "shadow_attack")
        selected.append((decision, path_type, action, risk_level, bucket, sort_key[0]))
        seen_symbols.add(decision.symbol)
        per_theme_count[decision.theme_name] = theme_count + 1
        if len(selected) >= 5:
            break
    selected.sort(
        key=lambda item: (
            item[5],
            item[0].role_hint != "true_leader",
            item[0].local_rank,
            item[0].symbol,
        )
    )
    final_candidates: list[FinalCandidateDecision] = []
    for priority_rank, (stock_decision, path_type, action, risk_level, bucket, _path_rank) in enumerate(selected[:5], start=1):
        reason_codes = (
            "validated_theme_profit_center",
            path_type,
            stock_decision.role_hint,
            stock_decision.entry_behavior,
        )
        risk_tag_items = list(stock_decision.trace.risk_tags)
        if stock_decision.theme_name in risk_themes + local_pack_risk_themes:
            risk_tag_items.append("relative_risk_theme")
        bridge_signal = _bridge_signal_for_symbol(decision_bundle, stock_decision.symbol)
        if bridge_signal is not None:
            risk_tag_items.extend(bridge_signal.risk_tags)
        risk_tags = tuple(dict.fromkeys(risk_tag_items))
        trace = DecisionTrace(
            decision_id=f"final_candidate:{stock_decision.symbol}:{_phase_name(context)}",
            decision_type="final_candidate",
            scope=stock_decision.symbol,
            phase=_phase_name(context),
            trade_date=str(context.trade_date or ""),
            state=bucket,
            action_hint=action,
            confidence_bucket="medium",
            lower_decision_refs=(global_decision.trace.decision_id, stock_decision.trace.decision_id),
            evidence_refs=(
                *stock_decision.trace.evidence_refs,
                *((bridge_signal.signal_id,) if bridge_signal is not None else ()),
            ),
            reason_codes=reason_codes,
            risk_tags=risk_tags,
            invalidation_points=("theme_global_fades", "stock_2m_fades"),
            evidence_summary=(
                f"path={path_type}",
                f"bridge={bridge_signal.state if bridge_signal is not None else '-'}",
                stock_decision.evidence_text,
                *stock_decision.trace.evidence_summary,
            ),
        )
        final_candidates.append(
            FinalCandidateDecision(
                trace=trace,
                symbol=stock_decision.symbol,
                theme_name=stock_decision.theme_name,
                bucket=bucket,
                action=action,
                path_type=path_type,
                playbook=playbook_for_candidate_path(path_type),
                priority_rank=priority_rank,
                risk_level=risk_level,
            )
        )
    return tuple(final_candidates)


def _build_shadow_takeover_decision(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
    global_decision: GlobalMarketDecision,
    final_candidates: tuple[FinalCandidateDecision, ...],
    validations: tuple[HypothesisValidation, ...],
) -> ShadowTakeoverDecision:
    probe_candidates = tuple(
        item
        for item in final_candidates
        if item.action == "probe"
        and item.risk_level == "normal"
        and "relative_risk_theme" not in item.trace.risk_tags
    )
    risk_capped_candidates = tuple(
        item
        for item in final_candidates
        if item.action == "probe"
        and item.path_type == "local_pack_pressure_repair"
        and item.risk_level == "elevated"
    )
    pressure_bridge_mode = not probe_candidates and bool(risk_capped_candidates)
    eligible_candidates = probe_candidates or risk_capped_candidates
    non_pressure_global_risks = tuple(
        tag for tag in global_decision.trace.risk_tags if tag != "risk_capped_pressure_repair"
    )
    confirmed_attack_count = sum(
        1
        for item in validations
        if item.result == "confirmed" and item.next_action_hint == "probe"
    )
    block_reasons: list[str] = []
    if global_decision.market_script == "risk_off":
        block_reasons.append("global_risk_off")
    if non_pressure_global_risks:
        block_reasons.append("global_risk_tags")
    if not global_decision.main_attack_theme:
        block_reasons.append("no_main_attack_theme")
    if confirmed_attack_count <= 0:
        block_reasons.append("no_confirmed_attack_hypothesis")
    if not eligible_candidates:
        block_reasons.append("no_probe_candidate")
    high_focus = decision_bundle.high_focus_decision
    if high_focus is not None and high_focus.feedback_state == "negative" and not pressure_bridge_mode:
        block_reasons.append("high_focus_negative")
    allowed = not block_reasons
    state = "ready_to_shadow_takeover" if allowed else "blocked"
    action_hint = "shadow_can_rank" if allowed else "shadow_only"
    primary_symbols = tuple(item.symbol for item in eligible_candidates[:3])
    mode = "risk_capped_bridge" if allowed and pressure_bridge_mode else ("rank_bridge" if allowed else "shadow_only")
    trace = DecisionTrace(
        decision_id=f"shadow_takeover:{context.trade_date}:{_phase_name(context)}",
        decision_type="shadow_takeover",
        scope="market",
        phase=_phase_name(context),
        trade_date=str(context.trade_date or ""),
        state=state,
        action_hint=action_hint,
        confidence_bucket="medium" if allowed and not pressure_bridge_mode else ("low" if allowed else "low"),
        evidence_refs=tuple(item.trace.decision_id for item in eligible_candidates[:3]),
        lower_decision_refs=(
            global_decision.trace.decision_id,
            *(item.trace.decision_id for item in eligible_candidates[:3]),
        ),
        reason_codes=("shadow_takeover_gate",),
        risk_tags=tuple(block_reasons[:3]),
        reject_reason=";".join(block_reasons) if block_reasons else "",
        invalidation_points=("shadow_candidate_fades", "theme_path_fails") if allowed else (),
        evidence_summary=(
            f"allowed={allowed}",
            f"probe_candidates={len(probe_candidates)}",
            f"risk_capped_candidates={len(risk_capped_candidates)}",
            f"confirmed_attack={confirmed_attack_count}",
            f"primary={','.join(primary_symbols) or '-'}",
            f"blocks={','.join(block_reasons[:3]) or '-'}",
        ),
    )
    return ShadowTakeoverDecision(
        trace=trace,
        allowed=allowed,
        mode=mode,
        primary_symbols=primary_symbols,
        block_reasons=tuple(block_reasons),
    )


def _build_playbook_control_matrix(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
    global_decision: GlobalMarketDecision,
    final_candidates: tuple[FinalCandidateDecision, ...],
    validations: tuple[HypothesisValidation, ...],
    takeover: ShadowTakeoverDecision,
) -> PlaybookControlMatrix:
    phase = _phase_name(context)
    validation_by_id = {item.hypothesis_id: item for item in validations}
    hypothesis_by_id = {item.hypothesis_id: item for item in decision_bundle.hypotheses}
    confirmed_scripts = {
        hypothesis_by_id[item.hypothesis_id].script
        for item in validations
        if item.result == "confirmed" and item.hypothesis_id in hypothesis_by_id
    }
    rejected_scripts = {
        hypothesis_by_id[item.hypothesis_id].script
        for item in validations
        if item.result == "rejected" and item.hypothesis_id in hypothesis_by_id
    }
    candidate_paths = {item.path_type for item in final_candidates}
    candidate_playbooks = {item.playbook for item in final_candidates}
    candidate_refs_by_playbook: dict[str, tuple[str, ...]] = {}
    for item in final_candidates:
        refs = candidate_refs_by_playbook.get(item.playbook, ())
        if len(refs) < 3:
            candidate_refs_by_playbook[item.playbook] = refs + (item.trace.decision_id,)
    pack = decision_bundle.local_strategy_evidence_pack
    high_pressure_count = len(pack.high_pressure_alerts) if pack is not None else 0
    emotion_risk = any(item.action_hint in {"avoid", "avoid_chase"} or item.avoid_count > 0 for item in (pack.emotion_alerts if pack is not None else ()))
    risk_tags = tuple(global_decision.trace.risk_tags)
    rows: list[PlaybookControlRow] = []

    def add_row(
        playbook: str,
        *,
        enabled: bool,
        action_hint: str,
        cap: float,
        reason: str,
        risks: tuple[str, ...] = (),
        refs: tuple[str, ...] = (),
    ) -> None:
        rows.append(
            PlaybookControlRow(
                playbook=playbook,
                enabled=enabled,
                action_hint=action_hint,
                cap=cap,
                phase=phase,
                reason=reason,
                risk_tags=risks,
                evidence_refs=refs,
            )
        )

    mainline_enabled = (
        global_decision.market_script == "attack_confirmed"
        and "mainline_attack" in candidate_playbooks
        and "risk_capped_pressure_repair" not in risk_tags
    )
    add_row(
        "mainline_attack",
        enabled=mainline_enabled,
        action_hint="probe" if mainline_enabled else "watch",
        cap=global_decision.position_cap if mainline_enabled else 0.0,
        reason="confirmed local/theme path" if mainline_enabled else "no clean mainline candidate",
        risks=() if mainline_enabled else tuple(risk_tags),
        refs=candidate_refs_by_playbook.get("mainline_attack", ()),
    )

    pressure_enabled = "dragon_pressure_repair" in candidate_playbooks and takeover.mode == "risk_capped_bridge"
    add_row(
        "dragon_pressure_repair",
        enabled=pressure_enabled,
        action_hint="probe" if pressure_enabled else "watch",
        cap=min(global_decision.position_cap, 0.12) if pressure_enabled else 0.0,
        reason="absolute leader repair under high pressure" if pressure_enabled else "pressure repair not confirmed",
        risks=("risk_capped_pressure_repair",) if pressure_enabled else (),
        refs=candidate_refs_by_playbook.get("dragon_pressure_repair", ()),
    )

    high_pressure_block = high_pressure_count > 0 and not pressure_enabled
    add_row(
        "dragon_head_risk_control",
        enabled=high_pressure_block,
        action_hint="avoid_chase" if high_pressure_block else "watch",
        cap=0.0,
        reason="high-focus pressure blocks same-theme attack" if high_pressure_block else "no active high-pressure block",
        risks=("high_focus_pressure",) if high_pressure_block else (),
        refs=tuple(summary.scope for summary in (pack.high_pressure_alerts[:3] if pack is not None else ())),
    )

    rotation_confirmed = "capital_rotation" in confirmed_scripts
    rotation_partial = any(
        hypothesis_by_id.get(item.hypothesis_id) is not None
        and hypothesis_by_id[item.hypothesis_id].script == "capital_rotation"
        and item.result in {"partial", "pending"}
        for item in validations
    )
    add_row(
        "sector_rotation",
        enabled=rotation_confirmed,
        action_hint="probe" if rotation_confirmed else ("watch" if rotation_partial else "disabled"),
        cap=min(global_decision.position_cap, 0.18) if rotation_confirmed else (0.05 if rotation_partial else 0.0),
        reason="rotation confirmed" if rotation_confirmed else ("rotation needs validation" if rotation_partial else "no rotation path"),
        risks=("rotation_unconfirmed",) if rotation_partial and not rotation_confirmed else (),
        refs=candidate_refs_by_playbook.get("sector_rotation", ()),
    )

    weak_repair_enabled = pressure_enabled or "dragon_pressure_repair" in candidate_playbooks
    add_row(
        "weak_to_strong_repair",
        enabled=weak_repair_enabled,
        action_hint="probe" if weak_repair_enabled else "watch",
        cap=min(global_decision.position_cap, 0.12) if weak_repair_enabled else 0.0,
        reason="low-open repair with 2m confirmation" if weak_repair_enabled else "no confirmed weak-to-strong bridge",
        risks=("elevated_repair_risk",) if weak_repair_enabled else (),
        refs=candidate_refs_by_playbook.get("dragon_pressure_repair", ()),
    )

    relay_blocked = emotion_risk or "local_pack_theme_risk" in rejected_scripts or high_pressure_count > 0
    add_row(
        "yesterday_limit_relay",
        enabled=not relay_blocked and global_decision.market_script == "attack_confirmed",
        action_hint="probe" if not relay_blocked and global_decision.market_script == "attack_confirmed" else "watch",
        cap=min(global_decision.position_cap, 0.15) if not relay_blocked and global_decision.market_script == "attack_confirmed" else 0.0,
        reason="relay allowed by emotion" if not relay_blocked and global_decision.market_script == "attack_confirmed" else "relay blocked by emotion/local risk",
        risks=("relay_emotion_risk",) if relay_blocked else (),
    )

    active = tuple(row.playbook for row in rows if row.enabled and row.action_hint not in {"avoid", "avoid_chase", "disabled"})
    blocked = tuple(row.playbook for row in rows if row.action_hint in {"avoid", "avoid_chase", "disabled"} or row.risk_tags)
    max_cap = max((row.cap for row in rows if row.enabled), default=0.0)
    return PlaybookControlMatrix(
        phase=phase,
        rows=tuple(rows),
        active_playbooks=active,
        blocked_playbooks=blocked,
        max_cap=max_cap,
        notes=(
            f"active={','.join(active) or '-'}",
            f"blocked={','.join(blocked) or '-'}",
            f"max_cap={max_cap:.0%}",
            f"takeover={takeover.mode}",
            f"global={global_decision.market_script}",
        ),
    )


def build_hypothesis_decision_bundle(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
) -> DecisionBundle:
    """Attach market hypotheses and final playbook candidates for the active path."""

    themes = tuple(decision_bundle.theme_local_decisions or ())
    high_focus = decision_bundle.high_focus_decision
    high_focus_state = high_focus.feedback_state if high_focus is not None else "unknown"
    high_focus_ref = (high_focus.trace.decision_id,) if high_focus is not None else ()
    theme_relative = decision_bundle.theme_relative_decision
    relative_mainline_order = theme_relative.mainline_candidates if theme_relative is not None else ()
    relative_rotation_order = theme_relative.rotation_candidates if theme_relative is not None else ()
    relative_risk_order = theme_relative.risk_themes if theme_relative is not None else ()
    relative_ref = (theme_relative.trace.decision_id,) if theme_relative is not None else ()

    hypothesis_items: list[tuple[MarketHypothesis, ThemeLocalDecision | None]] = []
    validations: list[HypothesisValidation] = []

    extension_candidates = [
        theme
        for theme in themes
        if theme.local_script_hint == "extension" and theme.local_validation_hint == "confirmed_like"
    ]
    extension_candidates.sort(
        key=lambda theme: (
            _theme_order_index(theme.theme_name, relative_mainline_order),
            theme.spread_level != "strong",
            theme.leader_drive_type == "leader_only",
            theme.theme_name,
        )
    )
    rotation_candidates = [
        theme
        for theme in themes
        if theme.local_script_hint in {"rotation_candidate", "repair"}
        and theme.local_validation_hint == "confirmed_like"
    ]
    rotation_candidates.sort(
        key=lambda theme: (
            _theme_order_index(theme.theme_name, relative_rotation_order),
            theme.spread_level != "strong",
            theme.leader_drive_type == "leader_only",
            theme.theme_name,
        )
    )
    fakeout_candidates = [
        theme
        for theme in themes
        if theme.local_script_hint in {"fakeout", "distribution"}
        or theme.local_validation_hint == "falsified_like"
    ]
    fakeout_candidates.sort(
        key=lambda theme: (
            _theme_order_index(theme.theme_name, relative_risk_order),
            theme.theme_name,
        )
    )

    if extension_candidates:
        theme = extension_candidates[0]
        hypothesis_items.append(
            (
                _build_hypothesis(
                context=context,
                script="mainline_extension",
                theme=theme,
                claim=f"{theme.theme_name} may extend if front row and spread keep confirming",
                required_validations=("theme_local_confirmed", "theme_spread", "profit_center_candidate"),
                invalidation_points=("front_row_fades", "mid_follow_missing"),
                extra_local_refs=high_focus_ref + relative_ref,
                ),
                theme,
            )
        )
    if rotation_candidates:
        theme = rotation_candidates[0]
        hypothesis_items.append(
            (
                _build_hypothesis(
                context=context,
                script="capital_rotation",
                theme=theme,
                claim=f"{theme.theme_name} may be the lower-resistance rotation path",
                required_validations=("theme_local_confirmed", "theme_spread", "high_focus_feedback"),
                invalidation_points=("rotation_volume_fades", "old_mainline_reclaims"),
                extra_local_refs=high_focus_ref + relative_ref,
                ),
                theme,
            )
        )
    if high_focus_state == "negative":
        hypothesis_items.append(
            (
                _build_hypothesis(
                context=context,
                script="high_level_distribution",
                theme=None,
                claim="high-focus stocks are spreading negative feedback",
                required_validations=("high_focus_distribution",),
                invalidation_points=("leader_repair", "risk_spread_recedes"),
                trigger_refs=high_focus_ref,
                extra_local_refs=high_focus_ref + relative_ref,
                ),
                None,
            )
        )
    if fakeout_candidates:
        theme = fakeout_candidates[0]
        hypothesis_items.append(
            (
                _build_hypothesis(
                context=context,
                script="fakeout_pulse",
                theme=theme,
                claim=f"{theme.theme_name} has amount without enough tradable spread",
                required_validations=("fakeout_or_distribution", "theme_spread"),
                invalidation_points=("front_row_reclaims", "spread_expands"),
                extra_local_refs=high_focus_ref + relative_ref,
                ),
                theme,
            )
        )

    hypothesis_items = hypothesis_items[:4]
    for hypothesis, theme in hypothesis_items:
        validations.append(
            _validate_hypothesis(
                hypothesis,
                theme=theme,
                high_focus_state=high_focus_state,
            )
        )

    pack_pairs = _build_local_pack_hypotheses(context, decision_bundle)
    seen_hypothesis_ids = {hypothesis.hypothesis_id for hypothesis, _ in hypothesis_items}
    for hypothesis, validation in pack_pairs:
        if hypothesis.hypothesis_id in seen_hypothesis_ids:
            continue
        hypothesis_items.append((hypothesis, None))
        validations.append(validation)
        seen_hypothesis_ids.add(hypothesis.hypothesis_id)

    final_hypotheses = tuple(hypothesis for hypothesis, _ in hypothesis_items)
    final_validations = tuple(validations)
    global_decision = _build_global_decision(
        context,
        final_hypotheses,
        final_validations,
        high_focus_state,
        theme_relative.trace if theme_relative is not None else None,
        relative_risk_order,
    )
    final_candidates = _build_final_candidates(context, decision_bundle, global_decision)
    shadow_takeover = _build_shadow_takeover_decision(
        context,
        decision_bundle,
        global_decision,
        final_candidates,
        final_validations,
    )
    playbook_matrix = _build_playbook_control_matrix(
        context,
        decision_bundle,
        global_decision,
        final_candidates,
        final_validations,
        shadow_takeover,
    )
    final_bucket_counts: dict[str, int] = {}
    for item in final_candidates:
        final_bucket_counts[item.bucket] = final_bucket_counts.get(item.bucket, 0) + 1
    bucket_summary = ",".join(f"{key}:{value}" for key, value in sorted(final_bucket_counts.items())) or "-"
    notes = tuple(decision_bundle.notes) + (
        f"hypotheses={len(hypothesis_items)}",
        f"local_pack_hypotheses={len(pack_pairs)}",
        f"hypothesis_confirmed={sum(1 for item in validations if item.result == 'confirmed')}",
        f"global_script={global_decision.market_script}",
        f"final_candidates={len(final_candidates)}",
        f"final_buckets={bucket_summary}",
        f"shadow_takeover={shadow_takeover.trace.state}",
        f"playbook_active={','.join(playbook_matrix.active_playbooks) or '-'}",
        f"playbook_cap={playbook_matrix.max_cap:.0%}",
    )
    return replace(
        decision_bundle,
        hypotheses=final_hypotheses,
        hypothesis_validations=final_validations,
        global_decision=global_decision,
        final_candidates=final_candidates,
        shadow_takeover_decision=shadow_takeover,
        playbook_control_matrix=playbook_matrix,
        notes=notes,
    )
