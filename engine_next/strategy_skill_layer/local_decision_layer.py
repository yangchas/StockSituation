from __future__ import annotations

import json
from collections import Counter, deque
from typing import Iterable

from engine_next.domain.decision_models import (
    DecisionBundle,
    DecisionTrace,
    FocusAssetStressDecision,
    HighFocusDecision,
    HotPlateAnchorDecision,
    HotPlateMetricLine,
    StockLocalDecision,
    TemporalMemoryLine,
    TemporalMigrationDecision,
    ThemeLocalDecision,
    ThemeRelativeDecision,
    TimeframeEvidence,
)
from engine_next.domain.models import IntradayContext, StockSelectionContext, StockStateSnapshot, ThemeTradeFact
from engine_next.runtime.intraday_data_hub import IntradayDataHub
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


_TEMPORAL_MEMORY_MAX_SAMPLES = 6
_TEMPORAL_MEMORY_TTL_SECONDS = 24 * 60 * 60
_TEMPORAL_MEMORY: dict[str, deque[dict[str, tuple[str, int, float, float, float]]]] = {}


def _temporal_memory_redis_key(trade_date: str) -> str:
    return f"cache:temporal_memory:{trade_date or 'unknown_trade_date'}"


def _encode_temporal_memory(history: deque[dict[str, tuple[str, int, float, float, float]]]) -> str:
    rows: list[dict[str, list[object]]] = []
    for snapshot in history:
        rows.append(
            {
                theme: [
                    str(values[0]),
                    int(values[1]),
                    float(values[2]),
                    float(values[3]),
                    float(values[4]),
                ]
                for theme, values in snapshot.items()
            }
        )
    return json.dumps(rows[-_TEMPORAL_MEMORY_MAX_SAMPLES:], ensure_ascii=False, separators=(",", ":"))


def _decode_temporal_memory(raw: object) -> deque[dict[str, tuple[str, int, float, float, float]]]:
    history: deque[dict[str, tuple[str, int, float, float, float]]] = deque(maxlen=_TEMPORAL_MEMORY_MAX_SAMPLES)
    if raw in (None, ""):
        return history
    try:
        payload = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        return history
    if not isinstance(payload, list):
        return history
    for item in payload[-_TEMPORAL_MEMORY_MAX_SAMPLES:]:
        if not isinstance(item, dict):
            continue
        snapshot: dict[str, tuple[str, int, float, float, float]] = {}
        for theme, values in item.items():
            if not isinstance(values, list) or len(values) < 5:
                continue
            try:
                snapshot[str(theme)] = (
                    str(values[0]),
                    int(float(values[1])),
                    float(values[2]),
                    float(values[3]),
                    float(values[4]),
                )
            except (TypeError, ValueError):
                continue
        if snapshot:
            history.append(snapshot)
    return history


def _load_persisted_temporal_memory(memory_key: str) -> deque[dict[str, tuple[str, int, float, float, float]]]:
    try:
        raw = IntradayDataHub().redis.get(_temporal_memory_redis_key(memory_key))
    except Exception:
        return deque(maxlen=_TEMPORAL_MEMORY_MAX_SAMPLES)
    return _decode_temporal_memory(raw)


def _persist_temporal_memory(memory_key: str, history: deque[dict[str, tuple[str, int, float, float, float]]]) -> None:
    if not history:
        return
    try:
        redis = IntradayDataHub().redis
        redis.set(
            _temporal_memory_redis_key(memory_key),
            _encode_temporal_memory(history),
            ex=_TEMPORAL_MEMORY_TTL_SECONDS,
        )
    except Exception:
        return


def _context_note_value(context: IntradayContext, key: str) -> str:
    prefix = f"{key}="
    for note in tuple(getattr(context, "notes", ()) or ()):
        text = str(note or "")
        if text.startswith(prefix):
            return text.split("=", 1)[1]
    return ""


def _temporal_memory_write_allowed(context: IntradayContext) -> bool:
    flag = _context_note_value(context, "temporal_memory_write")
    return flag.lower() not in {"0", "false", "no", "disabled"}


def _temporal_sample_key(context: IntradayContext, snapshot: dict[str, tuple[str, int, float, float, float]]) -> str:
    quote_ts = int(getattr(context, "latest_quote_timestamp_ms", 0) or 0)
    if quote_ts > 0:
        return f"quote_min:{quote_ts // 60_000}"
    phase = _context_note_value(context, "temporal_sample_phase") or _phase_name(context)
    minute = _context_note_value(context, "temporal_sample_minute") or "-"
    compact = "|".join(
        f"{theme}:{values[0]}:{values[1]}:{values[2]:.0f}:{values[4]:.2f}"
        for theme, values in sorted(snapshot.items())[:8]
    )
    return f"fallback:{phase}:{minute}:{compact}"


def _snapshot_sample_key(snapshot: dict[str, tuple[str, int, float, float, float]]) -> str:
    values = snapshot.get("__sample__")
    if values is None:
        return ""
    return str(values[0] or "")


def _phase_name(context: IntradayContext) -> str:
    return str(getattr(context.phase, "value", context.phase) or "")


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if float(denominator or 0.0) > 0.0 else 0.0


def _trace_metric_value(trace: DecisionTrace | None, name: str, default: float = 0.0) -> float:
    if trace is None:
        return default
    for metric_name, value in trace.metric_values:
        if metric_name == name:
            return float(value)
    return default


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
    metrics: tuple[str, ...] = (),
    metric_values: tuple[tuple[str, float], ...] = (),
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
        metrics=metrics,
        metric_values=metric_values,
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
        amount_ratio = _ratio(snapshot.amount_2m, snapshot.auction_amount)
        state = "watch"
        action_hint = "watch"
        reason_codes: list[str] = [f"role_{role}", f"behavior_{behavior}"]
        risk_tags: list[str] = []
        shape_bucket = selection.shape_bucket if selection is not None else "unknown"
        execution_bucket = selection.execution_bucket if selection is not None else "unknown"
        undertake_bucket = selection.undertake_bucket if selection is not None else "unknown"
        risk_bucket = selection.risk_bucket if selection is not None else "unknown"
        is_strict_candidate = role in {"true_leader", "front_row"} and behavior in {
            "limit_attack",
            "volume_confirm",
            "low_open_repair",
        }
        is_amount_confirmed = (
            snapshot.amount_2m >= max(amount_2m_floor, 20_000_000)
            or amount_ratio >= 0.85
            or snapshot.speed_1m >= 0.008
        )
        is_relative_candidate = (
            role in {"true_leader", "front_row"}
            and behavior not in {"high_open_distribution", "weak_follow"}
            and is_amount_confirmed
            and snapshot.current_pct >= snapshot.open_pct - 0.02
        )
        is_mid_repair_candidate = (
            role == "mid_follow"
            and behavior in {"repair_strength", "confirmed", "low_open_repair"}
            and is_amount_confirmed
            and snapshot.current_pct >= -0.01
            and snapshot.leader_rank_in_theme <= 6
        )
        if is_strict_candidate or is_relative_candidate or is_mid_repair_candidate:
            state = "candidate"
            action_hint = "probe"
            if is_relative_candidate and not is_strict_candidate:
                reason_codes.append("relative_amount_candidate")
            if is_mid_repair_candidate:
                reason_codes.append("mid_follow_repair_candidate")
        if behavior == "high_open_distribution":
            state = "risk"
            action_hint = "avoid"
            risk_tags.append("high_open_distribution")
        if selection is not None and selection.daily_height_bucket == "high" and role != "true_leader":
            risk_tags.append("high_dayk_risk")
        if selection is not None and selection.risk_bucket in {"elevated", "high"}:
            risk_tags.append(f"shape_risk_{selection.risk_bucket}")
        if risk_bucket == "high" and role != "true_leader":
            state = "watch"
            action_hint = "watch"
            reason_codes.append("high_risk_bucket_downgrade")
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
            metrics=(
                f"amount_2m={snapshot.amount_2m:.0f}",
                f"speed_1m={snapshot.speed_1m:.4f}",
                f"auction_amount={snapshot.auction_amount:.0f}",
                f"amount_2m_vs_auction={amount_ratio:.2f}",
                f"open_pct={snapshot.open_pct:.4f}",
                f"current_pct={snapshot.current_pct:.4f}",
                f"lb_days={snapshot.lb_days}",
                f"leader_rank={snapshot.leader_rank_in_theme}",
            ),
            metric_values=(
                ("amount_2m", float(snapshot.amount_2m or 0.0)),
                ("speed_1m", float(snapshot.speed_1m or 0.0)),
                ("auction_amount", float(snapshot.auction_amount or 0.0)),
                ("amount_2m_vs_auction", amount_ratio),
                ("open_pct", float(snapshot.open_pct or 0.0)),
                ("current_pct", float(snapshot.current_pct or 0.0)),
                ("lb_days", float(snapshot.lb_days or 0)),
                ("leader_rank", float(snapshot.leader_rank_in_theme or 999)),
            ),
            evidence_summary=(
                f"amount_2m={snapshot.amount_2m:.0f}",
                f"speed_1m={snapshot.speed_1m:.4f}",
                f"amount_2m_vs_auction={amount_ratio:.2f}",
                f"rank={snapshot.leader_rank_in_theme}",
                f"shape={shape_bucket}/exec={execution_bucket}/undertake={undertake_bucket}/risk={risk_bucket}",
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
        metrics=(
            f"high_count={len(high_rows)}",
            f"positive_count={len(positive)}",
            f"negative_count={len(negative)}",
            f"promoted_count={len(promoted)}",
            f"positive_rate={_ratio(len(positive), len(high_rows)):.2f}",
            f"negative_rate={_ratio(len(negative), len(high_rows)):.2f}",
            f"promoted_rate={_ratio(len(promoted), len(high_rows)):.2f}",
        ),
        metric_values=(
            ("high_count", float(len(high_rows))),
            ("positive_count", float(len(positive))),
            ("negative_count", float(len(negative))),
            ("promoted_count", float(len(promoted))),
            ("positive_rate", _ratio(len(positive), len(high_rows))),
            ("negative_rate", _ratio(len(negative), len(high_rows))),
            ("promoted_rate", _ratio(len(promoted), len(high_rows))),
        ),
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


def _focus_asset_scope(context: IntradayContext) -> tuple[StockStateSnapshot, ...]:
    all_snapshots = tuple(context.stock_snapshots or ())
    top_turnover_symbols = tuple(getattr(context.market_summary, "top_turnover_symbols", ()) or ())[:40]
    top_turnover_set = set(top_turnover_symbols)
    yest_hot_themes = {
        str(getattr(item, "plate_name", "") or "")
        for item in tuple(getattr(context.session_facts, "hot_plate_yesterday", ()) or ())[:8]
        if str(getattr(item, "plate_name", "") or "")
    }
    today_hot_themes = {
        str(getattr(item, "plate_name", "") or "")
        for item in tuple(getattr(context.session_facts, "hot_plate_today", ()) or ())[:8]
        if str(getattr(item, "plate_name", "") or "")
    }
    rows = [
        snapshot
        for snapshot in all_snapshots
        if snapshot.symbol in top_turnover_set
        or snapshot.lb_days >= 2
        or snapshot.is_yest_limit
        or (snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank <= 30)
        or _primary_theme(snapshot) in yest_hot_themes
        or _primary_theme(snapshot) in today_hot_themes
    ]
    rows.sort(
        key=lambda snapshot: (
            -int(snapshot.lb_days or 0),
            0 if snapshot.symbol in top_turnover_set else 1,
            int(snapshot.leader_rank_in_theme or 999),
            int(snapshot.ths_hot_rank or 999),
            -float(snapshot.amount_2m or 0.0),
            -float(snapshot.auction_amount or 0.0),
            snapshot.symbol,
        )
    )
    deduped: list[StockStateSnapshot] = []
    seen_symbols: set[str] = set()
    for row in rows:
        if row.symbol in seen_symbols:
            continue
        deduped.append(row)
        seen_symbols.add(row.symbol)
        if len(deduped) >= 60:
            break
    return tuple(deduped)


def build_focus_asset_stress_decision(context: IntradayContext) -> FocusAssetStressDecision:
    rows = _focus_asset_scope(context)
    if not rows:
        trace = _trace(
            decision_id=_decision_id("focus_asset_stress", _phase_name(context)),
            decision_type="focus_asset_stress",
            scope="market",
            context=context,
            state="empty",
            action_hint="watch",
            confidence_bucket="low",
            reject_reason="no_focus_assets",
            metrics=("scope_count=0",),
            metric_values=(("scope_count", 0.0),),
            evidence_summary=("focus_assets=0",),
        )
        return FocusAssetStressDecision(trace=trace, stress_state="empty", spread_level="none")

    theme_front_totals: Counter[str] = Counter()
    theme_front_failures: Counter[str] = Counter()
    stressed_themes_counter: Counter[str] = Counter()
    positive_focus_count: Counter[str] = Counter()
    weak_focus_count: Counter[str] = Counter()
    yest_hot_theme_map = context.session_facts.hot_plate_yesterday_map or {}
    today_hot_theme_map = context.session_facts.hot_plate_today_map or {}
    top_turnover_set = set(tuple(getattr(context.market_summary, "top_turnover_symbols", ()) or ())[:40])
    leader_retreat_symbols: list[str] = []
    core_drop_symbols: list[str] = []
    core_stall_symbols: list[str] = []
    dragon_positive_symbols: list[str] = []
    focus_symbols: list[str] = []
    high_board_break_count = 0
    limit_down_or_near_count = 0
    yest_hot_core_fail_count = 0
    retreat_amount_2m_sum = 0.0
    retreat_speed_1m_sum = 0.0
    retreat_net_inflow_sum = 0.0
    stressed_themes_seen: set[str] = set()

    for row in rows:
        theme_name = _primary_theme(row)
        focus_symbols.append(row.symbol)
        hot_fact = today_hot_theme_map.get(theme_name)
        net_inflow_yi = float(getattr(hot_fact, "net_inflow_yi", 0.0) or 0.0) if hot_fact is not None else 0.0
        is_front = row.leader_rank_in_theme <= 3 or row.lb_days >= 1 or row.symbol in top_turnover_set
        if is_front:
            theme_front_totals[theme_name] += 1
        is_retreat = (
            row.current_pct <= -0.03
            or (row.open_pct >= 0.03 and row.current_pct <= row.open_pct - 0.04)
            or (row.touched_limit_today and not row.is_locked and row.current_pct <= 0.01)
        )
        is_core_drop = row.symbol in top_turnover_set and row.current_pct <= -0.04
        is_core_stall = (
            row.symbol in top_turnover_set
            and row.auction_amount >= 30_000_000
            and row.amount_2m <= row.auction_amount * 0.75
            and row.current_pct <= max(row.open_pct - 0.02, 0.005)
            and not row.is_locked
        )
        is_high_board_break = (
            row.lb_days >= 2
            and row.touched_limit_today
            and not row.is_locked
            and row.current_pct <= max(row.open_pct - 0.05, 0.0)
        )
        is_limit_down_or_near = row.current_pct <= -0.08
        if is_retreat or is_core_drop or is_core_stall or is_high_board_break or is_limit_down_or_near:
            stressed_themes_counter[theme_name] += 1
            stressed_themes_seen.add(theme_name)
            retreat_amount_2m_sum += float(row.amount_2m or 0.0)
            retreat_speed_1m_sum += float(row.speed_1m or 0.0)
            retreat_net_inflow_sum += net_inflow_yi
        if is_front and (is_retreat or is_core_drop or is_core_stall or is_high_board_break):
            theme_front_failures[theme_name] += 1
        if is_retreat and row.leader_rank_in_theme <= 2:
            leader_retreat_symbols.append(row.symbol)
        if is_core_drop:
            core_drop_symbols.append(row.symbol)
        if is_core_stall:
            core_stall_symbols.append(row.symbol)
        if is_high_board_break:
            high_board_break_count += 1
        if is_limit_down_or_near:
            limit_down_or_near_count += 1
        if theme_name in yest_hot_theme_map and row.symbol in top_turnover_set and (is_retreat or is_core_drop or is_core_stall):
            yest_hot_core_fail_count += 1
        if row.current_pct >= 0.03 or row.is_locked or row.touched_limit_today:
            positive_focus_count[theme_name] += 1
            if row.leader_rank_in_theme <= 2:
                dragon_positive_symbols.append(row.symbol)
        if is_retreat or is_core_drop or is_core_stall:
            weak_focus_count[theme_name] += 1

    stressed_themes = tuple(theme for theme, _ in stressed_themes_counter.most_common(6))
    dragon_alone_themes = tuple(
        theme
        for theme in positive_focus_count
        if positive_focus_count[theme] >= 1 and weak_focus_count.get(theme, 0) >= 2
    )[:6]
    theme_pressure_count = sum(1 for theme in stressed_themes if stressed_themes_counter[theme] >= 2)
    spread_level = "none"
    if len(stressed_themes_seen) >= 3:
        spread_level = "cross_theme"
    elif theme_pressure_count >= 1:
        spread_level = "theme"
    elif stressed_themes_seen:
        spread_level = "individual"
    dragon_alone_count = len(dragon_alone_themes)
    stress_state = "observe"
    if spread_level == "cross_theme" and (high_board_break_count > 0 or limit_down_or_near_count > 0 or yest_hot_core_fail_count > 0):
        stress_state = "market_risk_spread"
    elif dragon_alone_count > 0 and theme_pressure_count > 0:
        stress_state = "dragon_alone"
    elif theme_pressure_count > 0 or yest_hot_core_fail_count > 0:
        stress_state = "theme_pressure"
    elif leader_retreat_symbols or core_drop_symbols or core_stall_symbols:
        stress_state = "individual_retreat"
    style_switch_warning = 1.0 if stress_state in {"theme_pressure", "market_risk_spread", "dragon_alone"} and leader_retreat_symbols else 0.0
    front_follow_fail_rate = 0.0
    front_denominator = sum(theme_front_totals.values())
    if front_denominator > 0:
        front_follow_fail_rate = float(sum(theme_front_failures.values())) / float(front_denominator)
    stress_spread_level_num = {"none": 0.0, "individual": 1.0, "theme": 2.0, "cross_theme": 3.0}.get(spread_level, 0.0)
    evidence_summary = (
        f"stress={stress_state}",
        f"spread={spread_level}",
        f"themes={','.join(stressed_themes[:3]) or '-'}",
        f"dragon_alone={','.join(dragon_alone_themes[:3]) or '-'}",
        f"retreat={','.join((leader_retreat_symbols + core_drop_symbols + core_stall_symbols)[:3]) or '-'}",
    )
    trace = _trace(
        decision_id=_decision_id("focus_asset_stress", _phase_name(context)),
        decision_type="focus_asset_stress",
        scope="market",
        context=context,
        state=stress_state,
        action_hint="avoid_chase" if stress_state in {"theme_pressure", "market_risk_spread", "dragon_alone"} else "watch",
        confidence_bucket="medium" if spread_level in {"theme", "cross_theme"} else "low",
        evidence_refs=tuple(
            ref
            for ref in (
                "market.top_turnover_symbols",
                *(f"theme.{theme}.hot_plate_today" for theme in stressed_themes[:2]),
                *(f"theme.{theme}.hot_plate_yesterday" for theme in stressed_themes[:2] if theme in yest_hot_theme_map),
            )
            if ref
        ),
        reason_codes=("focus_asset_stress", stress_state, spread_level),
        risk_tags=("focus_asset_distribution",) if stress_state != "observe" else (),
        reject_reason="focus_asset_stress_spread" if stress_state == "market_risk_spread" else "",
        invalidation_points=("leader_repair", "mid_core_reclaims") if stress_state != "observe" else (),
        metrics=(
            f"scope_count={len(rows)}",
            f"leader_retreat_count={len(leader_retreat_symbols)}",
            f"high_board_break_count={high_board_break_count}",
            f"limit_down_or_near_count={limit_down_or_near_count}",
            f"core_mid_drop_count={len(core_drop_symbols)}",
            f"core_mid_stall_count={len(core_stall_symbols)}",
            f"yest_hot_core_fail_count={yest_hot_core_fail_count}",
            f"front_follow_fail_rate={front_follow_fail_rate:.2f}",
            f"stress_theme_count={len(stressed_themes_seen)}",
            f"dragon_alone_count={dragon_alone_count}",
            f"retreat_amount_2m={retreat_amount_2m_sum:.0f}",
            f"retreat_net_inflow={retreat_net_inflow_sum:.2f}",
        ),
        metric_values=(
            ("scope_count", float(len(rows))),
            ("leader_retreat_count", float(len(leader_retreat_symbols))),
            ("high_board_break_count", float(high_board_break_count)),
            ("limit_down_or_near_count", float(limit_down_or_near_count)),
            ("core_mid_drop_count", float(len(core_drop_symbols))),
            ("core_mid_stall_count", float(len(core_stall_symbols))),
            ("yest_hot_core_fail_count", float(yest_hot_core_fail_count)),
            ("front_follow_fail_rate", front_follow_fail_rate),
            ("stress_theme_count", float(len(stressed_themes_seen))),
            ("stress_spread_level_num", stress_spread_level_num),
            ("dragon_alone_count", float(dragon_alone_count)),
            ("retreat_amount_2m_yuan", float(retreat_amount_2m_sum)),
            ("retreat_speed_1m_pct", float(retreat_speed_1m_sum)),
            ("retreat_net_inflow_yi", float(retreat_net_inflow_sum)),
            ("style_switch_warning", style_switch_warning),
        ),
        evidence_summary=evidence_summary,
    )
    return FocusAssetStressDecision(
        trace=trace,
        stress_state=stress_state,
        spread_level=spread_level,
        stressed_themes=stressed_themes,
        dragon_alone_themes=dragon_alone_themes,
        retreat_symbols=tuple(dict.fromkeys((*leader_retreat_symbols, *core_drop_symbols, *core_stall_symbols)))[:8],
        core_symbols=tuple(dict.fromkeys((*focus_symbols[:4], *dragon_positive_symbols[:4])))[:8],
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


def _hot_rank_map(context: IntradayContext) -> dict[str, int]:
    return {
        fact.plate_name: int(fact.rank or 999)
        for fact in tuple(getattr(context.session_facts, "hot_plate_today", ()) or ())
        if fact.plate_name
    }


def _rank_pct_desc_values(values_by_name: dict[str, float]) -> dict[str, float]:
    rows = sorted(
        ((name, float(value or 0.0)) for name, value in values_by_name.items()),
        key=lambda item: (-item[1], item[0]),
    )
    total = max(len(rows), 1)
    return {name: (index + 1) / total for index, (name, _value) in enumerate(rows)}


def _hot_anchor_score(
    *,
    rank: int,
    strength_rank_pct: float,
    change_rank_pct: float,
    inflow_rank_pct: float,
    hot_rank_pct: float,
    amount_2m_rank_pct: float,
    front_2m_count: int,
    spread_level: str,
    high_open_fail_count: int,
    net_inflow_yi: float,
    net_inflow_yi_delta: float,
) -> float:
    score = 0.0
    score += max(0.0, 1.10 - float(strength_rank_pct or 1.0)) * 28.0
    score += max(0.0, 1.10 - float(change_rank_pct or 1.0)) * 18.0
    score += max(0.0, 1.10 - float(inflow_rank_pct or 1.0)) * 24.0
    score += max(0.0, 1.10 - float(hot_rank_pct or 1.0)) * 16.0
    score += max(0.0, 1.10 - float(amount_2m_rank_pct or 1.0)) * 14.0
    score += 6.0 if int(rank or 999) <= 3 else (3.0 if int(rank or 999) <= 8 else 0.0)
    score += 6.0 if spread_level == "strong" else (3.0 if spread_level == "normal" else 0.0)
    score += min(max(int(front_2m_count or 0), 0), 3) * 4.0
    if float(net_inflow_yi or 0.0) > 0:
        score += 4.0
    if float(net_inflow_yi_delta or 0.0) > 0:
        score += 4.0
    score -= min(max(int(high_open_fail_count or 0), 0), 3) * 5.0
    return round(score, 4)


def _ordered_hot_names(
    names: list[str] | tuple[str, ...],
    *,
    anchor_scores: dict[str, float],
    metric_lines: list[HotPlateMetricLine],
    limit: int,
) -> tuple[str, ...]:
    if not names or limit <= 0:
        return ()
    rank_map = {line.plate_name: int(line.rank or 999) for line in metric_lines if line.plate_name}
    ordered = sorted(
        dict.fromkeys(name for name in names if name),
        key=lambda name: (
            -float(anchor_scores.get(name, 0.0)),
            int(rank_map.get(name, 999)),
            name,
        ),
    )
    return tuple(ordered[:limit])


def build_hot_plate_anchor_decision(context: IntradayContext) -> HotPlateAnchorDecision:
    """Use hot plates as the first market-direction anchor before stock picking."""

    today = tuple(getattr(context.session_facts, "hot_plate_today", ()) or ())
    yesterday_map = context.session_facts.hot_plate_yesterday_map or {}
    trade_fact_map = context.session_facts.theme_trade_fact_map or {}
    migration_map = context.session_facts.plate_migration_map or {}
    hot_rows = tuple(hot for hot in today if hot.plate_name)
    strength_rank_pct = _rank_pct_desc_values({hot.plate_name: hot.strength for hot in hot_rows})
    change_rank_pct = _rank_pct_desc_values({hot.plate_name: hot.change_pct for hot in hot_rows})
    inflow_rank_pct = _rank_pct_desc_values({hot.plate_name: hot.net_inflow_yi for hot in hot_rows})
    hot_rank_pct = _rank_pct_desc_values({hot.plate_name: hot.hot for hot in hot_rows})
    amount_2m_rank_pct = _rank_pct_desc_values(
        {
            name: float(getattr(fact, "amount_2m_sum", 0.0) or 0.0)
            for name, fact in trade_fact_map.items()
        }
    )
    primary: list[str] = []
    continuation: list[str] = []
    rotation: list[str] = []
    fading: list[str] = []
    fakeout: list[str] = []
    evidence: list[str] = []
    metric_lines: list[HotPlateMetricLine] = []
    anchor_scores: dict[str, float] = {}

    for hot in today[:12]:
        name = hot.plate_name
        if not name:
            continue
        fact = trade_fact_map.get(name)
        migration = migration_map.get(name)
        yest = yesterday_map.get(name)
        spread = _theme_spread_level(fact) if fact is not None else "none"
        amount_2m = float(getattr(fact, "amount_2m_sum", 0.0) or 0.0) if fact is not None else 0.0
        front_2m = int(getattr(fact, "front_row_2m_pass_count", 0) or 0) if fact is not None else 0
        high_open_fail = int(getattr(fact, "high_open_fail_count", 0) or 0) if fact is not None else 0
        net_delta = float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0) if migration is not None else 0.0
        strength_pct = strength_rank_pct.get(name, 1.0)
        change_pct_rank = change_rank_pct.get(name, 1.0)
        inflow_pct = inflow_rank_pct.get(name, 1.0)
        hot_pct = hot_rank_pct.get(name, 1.0)
        amount_2m_pct = amount_2m_rank_pct.get(name, 1.0)
        trade_strength = strength_pct <= 0.35 or hot_pct <= 0.35
        height_strength = change_pct_rank <= 0.35 or hot.change_pct >= 1.0
        main_force_strength = inflow_pct <= 0.35 or hot.net_inflow_yi > 0 or net_delta > 0
        amount_confirmed = amount_2m_pct <= 0.35 or front_2m > 0
        spread_confirmed = spread in {"strong", "normal"} or front_2m > 0
        force_votes = int(trade_strength) + int(height_strength) + int(main_force_strength) + int(amount_confirmed or spread_confirmed)
        yest_rank = int(getattr(yest, "rank", 999) if yest is not None else 999)
        is_continuation = bool(
            yest is not None
            and hot.rank <= 50
            and force_votes >= 2
            and (spread_confirmed or amount_confirmed or net_delta >= 0)
        )
        is_rotation = bool(
            yest is None
            and hot.rank <= 50
            and (trade_strength or height_strength)
            and main_force_strength
            and force_votes >= 3
            and (spread_confirmed or amount_confirmed)
        )
        is_fading = bool(
            net_delta < -3.0
            or (yest is not None and hot.rank > 50 and not main_force_strength)
            or (yest is not None and yest_rank <= 30 and hot.rank > 30 and not spread_confirmed)
        )
        is_fakeout = bool(
            (amount_2m > 0 or height_strength)
            and spread in {"none", "weak"}
            and (high_open_fail > 0 or not main_force_strength)
        )
        if is_continuation:
            continuation.append(name)
        if is_rotation:
            rotation.append(name)
        if is_fading:
            fading.append(name)
        if is_fakeout:
            fakeout.append(name)
        anchor_score = _hot_anchor_score(
            rank=int(hot.rank or 999),
            strength_rank_pct=strength_pct,
            change_rank_pct=change_pct_rank,
            inflow_rank_pct=inflow_pct,
            hot_rank_pct=hot_pct,
            amount_2m_rank_pct=amount_2m_pct,
            front_2m_count=front_2m,
            spread_level=spread,
            high_open_fail_count=high_open_fail,
            net_inflow_yi=float(hot.net_inflow_yi or 0.0),
            net_inflow_yi_delta=net_delta,
        )
        anchor_scores[name] = anchor_score
        is_anchor_primary = (
            not is_fading
            and not is_fakeout
            and int(hot.rank or 999) <= 8
            and force_votes >= 3
            and (
                is_continuation
                or is_rotation
                or (front_2m > 0 and anchor_score >= 56.0)
                or anchor_score >= 64.0
            )
        )
        if is_anchor_primary:
            primary.append(name)
        line_state = "observe"
        if is_fading:
            line_state = "fading"
        elif is_fakeout:
            line_state = "fakeout"
        elif is_rotation:
            line_state = "rotation"
        elif is_continuation:
            line_state = "continuation"
        elif front_2m > 0:
            line_state = "front_2m_watch"
        metric_lines.append(
            HotPlateMetricLine(
                plate_name=name,
                rank=int(hot.rank or 999),
                yest_rank=yest_rank,
                change_pct=float(hot.change_pct or 0.0),
                strength=float(hot.strength or 0.0),
                net_inflow_yi=float(hot.net_inflow_yi or 0.0),
                hot_value=float(hot.hot or 0.0),
                amount_2m=amount_2m,
                front_2m_count=front_2m,
                high_open_fail_count=high_open_fail,
                net_inflow_yi_delta=net_delta,
                spread_level=spread,
                strength_rank_pct=strength_pct,
                change_rank_pct=change_pct_rank,
                inflow_rank_pct=inflow_pct,
                hot_rank_pct=hot_pct,
                amount_2m_rank_pct=amount_2m_pct,
                state=line_state,
            )
        )
        evidence.append(
            f"{name}:rank={hot.rank}/yest={getattr(yest, 'rank', 999) if yest is not None else 999}/"
            f"chg={hot.change_pct:.2f}/strength={hot.strength:.0f}/inflow={hot.net_inflow_yi:.2f}/"
            f"spread={spread}/2m={amount_2m:.0f}/front2m={front_2m}/net_delta={net_delta:.2f}/"
            f"rank_pct=s{strength_pct:.2f},h{hot_pct:.2f},chg{change_pct_rank:.2f},in{inflow_pct:.2f},2m{amount_2m_pct:.2f}/"
            f"votes={force_votes}/anchor_score={anchor_score:.1f}"
        )

    primary_themes = _ordered_hot_names(
        primary or continuation or rotation,
        anchor_scores=anchor_scores,
        metric_lines=metric_lines,
        limit=3,
    )
    continuation_themes = _ordered_hot_names(
        continuation,
        anchor_scores=anchor_scores,
        metric_lines=metric_lines,
        limit=3,
    )
    rotation_themes = _ordered_hot_names(
        rotation,
        anchor_scores=anchor_scores,
        metric_lines=metric_lines,
        limit=3,
    )
    fading_themes = _ordered_hot_names(
        fading,
        anchor_scores=anchor_scores,
        metric_lines=metric_lines,
        limit=4,
    )
    fakeout_themes = _ordered_hot_names(
        fakeout,
        anchor_scores=anchor_scores,
        metric_lines=metric_lines,
        limit=4,
    )
    anchor_state = "observe"
    action_hint = "watch"
    risk_tags: tuple[str, ...] = ()
    if primary_themes and rotation_themes:
        anchor_state = "hot_rotation"
        action_hint = "probe"
    elif primary_themes and continuation_themes:
        anchor_state = "hot_continuation"
        action_hint = "probe"
    elif primary_themes:
        anchor_state = "hot_probe"
        action_hint = "probe"
    elif fading_themes or fakeout_themes:
        anchor_state = "hot_risk"
        action_hint = "avoid_chase"
        risk_tags = ("hot_plate_risk",)
    top_metric = metric_lines[0] if metric_lines else HotPlateMetricLine(plate_name="-")
    trace = _trace(
        decision_id=_decision_id("hot_plate_anchor", _phase_name(context)),
        decision_type="hot_plate_anchor",
        scope="market",
        context=context,
        state=anchor_state,
        action_hint=action_hint,
        confidence_bucket="medium" if primary_themes else "low",
        evidence_refs=tuple(f"hot_plate.{name}" for name in primary_themes[:5]),
        reason_codes=("hot_plate_first_anchor", anchor_state),
        risk_tags=risk_tags,
        reject_reason="no_hot_plate_anchor" if not primary_themes else "",
        invalidation_points=("hot_rank_fades", "front_row_2m_fades") if primary_themes else (),
        metrics=tuple(evidence[:5]),
        metric_values=(
            ("primary_theme_count", float(len(primary_themes))),
            ("continuation_theme_count", float(len(continuation_themes))),
            ("rotation_theme_count", float(len(rotation_themes))),
            ("fading_theme_count", float(len(fading_themes))),
            ("fakeout_theme_count", float(len(fakeout_themes))),
            ("main_hot_theme_count", float(min(len(primary_themes), 1))),
            ("secondary_hot_theme_count", float(max(len(primary_themes) - 1, 0))),
            ("top_hot_rank", float(top_metric.rank)),
            ("top_hot_change_pct", float(top_metric.change_pct)),
            ("top_hot_strength", float(top_metric.strength)),
            ("top_hot_net_inflow_yi", float(top_metric.net_inflow_yi)),
            ("top_hot_amount_2m", float(top_metric.amount_2m)),
        ),
        evidence_summary=tuple(evidence[:5]),
    )
    return HotPlateAnchorDecision(
        trace=trace,
        anchor_state=anchor_state,
        primary_themes=primary_themes,
        continuation_themes=continuation_themes,
        rotation_themes=rotation_themes,
        fading_themes=fading_themes,
        fakeout_themes=fakeout_themes,
        hot_evidence=tuple(evidence[:8]),
        metric_lines=tuple(metric_lines[:12]),
    )


def _ranked_theme_facts(context: IntradayContext, *, limit: int = 24) -> tuple[ThemeTradeFact, ...]:
    hot_rank = _hot_rank_map(context)
    facts = tuple(context.session_facts.theme_trade_facts or ())
    return tuple(
        sorted(
            facts,
            key=lambda fact: (
                hot_rank.get(fact.plate_name, 999),
                fact.yest_hot_rank,
                -float(fact.amount_5m_sum or 0.0),
                -float(fact.amount_2m_sum or 0.0),
                -int(fact.front_row_2m_pass_count or 0),
                fact.plate_name,
            ),
        )[:limit]
    )


def _timeframe_evidence_for_theme(
    context: IntradayContext,
    fact: ThemeTradeFact,
    *,
    hot_rank: int,
) -> tuple[TimeframeEvidence, ...]:
    migration = context.session_facts.plate_migration_map.get(fact.plate_name)
    amount_ratio_2m = _ratio(fact.amount_2m_sum, fact.auction_amount)
    amount_ratio_5m = _ratio(fact.amount_5m_sum, fact.auction_amount)
    spread_level = _theme_spread_level(fact)
    t1_state = "attack" if fact.front_row_2m_pass_count >= 1 and fact.amount_2m_sum > 0 else "quiet"
    t2_state = "spread" if spread_level in {"strong", "normal"} and fact.amount_5m_sum > 0 else "thin"
    t3_state = "neutral"
    t3_risk: tuple[str, ...] = ()
    if migration is not None:
        if migration.net_inflow_yi_delta > 0 or fact.plate_name in getattr(context.market_summary, "migrating_in_plates", ()):
            t3_state = "migrating_in"
        elif migration.net_inflow_yi_delta < 0 or fact.plate_name in getattr(context.market_summary, "migrating_out_plates", ()):
            t3_state = "withdrawing"
            t3_risk = ("migration_withdrawing",)
    hot_state = "hot_anchor" if hot_rank <= 20 else ("hot_watch" if hot_rank <= 50 else "not_hot")
    return (
        TimeframeEvidence(
            timeframe="T1_2m",
            scope_type="theme",
            scope=fact.plate_name,
            state=t1_state,
            action_hint="probe" if t1_state == "attack" else "watch",
            rank=hot_rank,
            metrics=(
                f"amount_2m_sum={fact.amount_2m_sum:.0f}",
                f"amount_2m_vs_auction={amount_ratio_2m:.2f}",
                f"front_row_2m={fact.front_row_2m_pass_count}",
            ),
            metric_values=(
                ("amount_2m_sum", float(fact.amount_2m_sum or 0.0)),
                ("amount_2m_vs_auction", float(amount_ratio_2m)),
                ("front_row_2m_pass_count", float(fact.front_row_2m_pass_count or 0)),
                ("hot_rank", float(hot_rank)),
            ),
            evidence_refs=(
                f"theme.{fact.plate_name}.amount_2m_sum",
                f"theme.{fact.plate_name}.front_row_2m_pass_count",
            ),
        ),
        TimeframeEvidence(
            timeframe="T2_5m",
            scope_type="theme",
            scope=fact.plate_name,
            state=t2_state,
            action_hint="probe" if t2_state == "spread" else "watch",
            rank=hot_rank,
            metrics=(
                f"amount_5m_sum={fact.amount_5m_sum:.0f}",
                f"amount_5m_vs_auction={amount_ratio_5m:.2f}",
                f"spread={spread_level}",
                f"expansion={fact.expansion_count}",
            ),
            metric_values=(
                ("amount_5m_sum", float(fact.amount_5m_sum or 0.0)),
                ("amount_5m_vs_auction", float(amount_ratio_5m)),
                ("expansion_count", float(fact.expansion_count or 0)),
                ("red_open_count", float(fact.red_open_count or 0)),
                ("front_row_count", float(fact.front_row_count or 0)),
                ("yest_limit_count", float(fact.yest_limit_count or 0)),
            ),
            evidence_refs=(
                f"theme.{fact.plate_name}.amount_5m_sum",
                f"theme.{fact.plate_name}.expansion_count",
            ),
        ),
        TimeframeEvidence(
            timeframe="T3_15m",
            scope_type="theme",
            scope=fact.plate_name,
            state=t3_state,
            action_hint="avoid_chase" if t3_state == "withdrawing" else ("probe" if t3_state == "migrating_in" else "watch"),
            rank=hot_rank,
            metrics=(
                f"net_inflow_delta={float(getattr(migration, 'net_inflow_yi_delta', 0.0) or 0.0):.2f}",
                f"strength_delta={float(getattr(migration, 'strength_delta', 0.0) or 0.0):.2f}",
            ),
            metric_values=(
                ("net_inflow_yi_delta", float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0)),
                ("strength_delta", float(getattr(migration, "strength_delta", 0.0) or 0.0)),
                ("hot_rank", float(hot_rank)),
            ),
            evidence_refs=(f"theme.{fact.plate_name}.plate_migration",),
            risk_tags=t3_risk,
        ),
        TimeframeEvidence(
            timeframe="T4_hot",
            scope_type="theme",
            scope=fact.plate_name,
            state=hot_state,
            action_hint="support" if hot_state == "hot_anchor" else "watch",
            rank=hot_rank,
            metrics=(f"hot_rank={hot_rank}", f"yest_hot_rank={fact.yest_hot_rank}"),
            metric_values=(
                ("hot_rank", float(hot_rank)),
                ("yest_hot_rank", float(fact.yest_hot_rank or 999)),
                ("hot_rank_change", float((fact.yest_hot_rank or 999) - hot_rank)),
            ),
            evidence_refs=(f"theme.{fact.plate_name}.hot_plate_today",),
        ),
    )


def _temporal_transition_state(
    *,
    current_process: str,
    previous_process: str,
    hot_rank: int,
    previous_hot_rank: int,
    amount_2m: float,
    previous_amount_2m: float,
    net_delta: float,
    previous_net_delta: float,
) -> str:
    if not previous_process:
        return "init_sample"
    rank_improves = hot_rank < previous_hot_rank
    rank_fades = previous_hot_rank < 999 and hot_rank > previous_hot_rank + 10
    amount_accelerates = amount_2m > max(previous_amount_2m * 1.25, previous_amount_2m + 10_000_000)
    amount_fades = previous_amount_2m > 0 and amount_2m < previous_amount_2m * 0.6
    flow_improves = net_delta > max(previous_net_delta, 0.0)
    flow_fades = previous_net_delta > 0 and net_delta < previous_net_delta - 1.0
    if current_process in {"rotation_attack", "mainline_extend", "hot_persistence"} and (rank_improves or amount_accelerates or flow_improves):
        return "accelerating"
    if current_process == "withdrawal" or rank_fades or amount_fades or flow_fades:
        return "withdrawing"
    if current_process == "fake_breakout" and previous_process in {"rotation_attack", "mainline_extend", "hot_persistence"}:
        return "failed_after_attack"
    if current_process == "rebound_repair" and previous_process in {"withdrawal", "fake_breakout", "observe"}:
        return "rebound_repair"
    if current_process != previous_process and previous_process:
        return f"{previous_process}_to_{current_process}"
    return "steady"


def _temporal_macro_confirmation(
    *,
    process_state: str,
    hot_rank: int,
    amount_5m: float,
    previous_amount_5m: float,
    net_delta: float,
    previous_net_delta: float,
) -> tuple[bool, int]:
    if process_state not in {"rotation_attack", "mainline_extend", "hot_persistence", "rebound_repair"}:
        return False, 0
    score = 0
    if hot_rank <= 30:
        score += 1
    if amount_5m > max(previous_amount_5m * 1.15, previous_amount_5m + 20_000_000):
        score += 1
    if net_delta > max(previous_net_delta, 0.0):
        score += 1
    if process_state == "rotation_attack":
        score += 1
    return score >= 2, score


def _build_temporal_memory_lines(
    context: IntradayContext,
    *,
    facts_by_theme: dict[str, ThemeTradeFact],
    process_by_theme: dict[str, str],
    hot_rank_map: dict[str, int],
) -> tuple[TemporalMemoryLine, ...]:
    memory_key = str(context.trade_date or "unknown_trade_date")
    write_allowed = _temporal_memory_write_allowed(context)
    if write_allowed:
        history = _TEMPORAL_MEMORY.get(memory_key)
        if history is None:
            persisted = _load_persisted_temporal_memory(memory_key)
            history = persisted if persisted else deque(maxlen=_TEMPORAL_MEMORY_MAX_SAMPLES)
            _TEMPORAL_MEMORY[memory_key] = history
    else:
        history = deque(maxlen=_TEMPORAL_MEMORY_MAX_SAMPLES)
    previous = history[-1] if history else {}
    migration_map = context.session_facts.plate_migration_map or {}
    snapshot: dict[str, tuple[str, int, float, float, float]] = {}
    lines: list[TemporalMemoryLine] = []
    for theme_name, process_state in list(process_by_theme.items())[:16]:
        fact = facts_by_theme.get(theme_name)
        migration = migration_map.get(theme_name)
        hot_rank = int(hot_rank_map.get(theme_name, 999))
        amount_2m = float(getattr(fact, "amount_2m_sum", 0.0) or 0.0) if fact is not None else 0.0
        amount_5m = float(getattr(fact, "amount_5m_sum", 0.0) or 0.0) if fact is not None else 0.0
        net_delta = float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0) if migration is not None else 0.0
        prev_process, prev_hot_rank, prev_amount_2m, prev_amount_5m, prev_net_delta = previous.get(
            theme_name,
            ("", 999, 0.0, 0.0, 0.0),
        )
        transition = _temporal_transition_state(
            current_process=process_state,
            previous_process=prev_process,
            hot_rank=hot_rank,
            previous_hot_rank=int(prev_hot_rank),
            amount_2m=amount_2m,
            previous_amount_2m=float(prev_amount_2m),
            net_delta=net_delta,
            previous_net_delta=float(prev_net_delta),
        )
        macro_confirmed, macro_score = _temporal_macro_confirmation(
            process_state=process_state,
            hot_rank=hot_rank,
            amount_5m=amount_5m,
            previous_amount_5m=float(prev_amount_5m),
            net_delta=net_delta,
            previous_net_delta=float(prev_net_delta),
        )
        snapshot[theme_name] = (process_state, hot_rank, amount_2m, amount_5m, net_delta)
        lines.append(
            TemporalMemoryLine(
                plate_name=theme_name,
                process_state=process_state,
                previous_process_state=str(prev_process or ""),
                transition_state=transition,
                sample_count=len(history) + 1,
                hot_rank=hot_rank,
                previous_hot_rank=int(prev_hot_rank),
                amount_2m=amount_2m,
                previous_amount_2m=float(prev_amount_2m),
                amount_5m=amount_5m,
                previous_amount_5m=float(prev_amount_5m),
                net_inflow_yi_delta=net_delta,
                previous_net_inflow_yi_delta=float(prev_net_delta),
                macro_confirmed=macro_confirmed,
                macro_score=macro_score,
            )
        )
    sample_key = _temporal_sample_key(context, snapshot)
    previous_sample_key = _snapshot_sample_key(previous)
    if snapshot:
        snapshot["__sample__"] = (sample_key, 0, 0.0, 0.0, 0.0)
    if write_allowed and snapshot and sample_key != previous_sample_key:
        history.append(snapshot)
        _persist_temporal_memory(memory_key, history)
    return tuple(lines)


def build_temporal_migration_decision(
    context: IntradayContext,
    *,
    theme_decisions: tuple[ThemeLocalDecision, ...] = (),
) -> TemporalMigrationDecision:
    """Separate time-grain evidence from final theme judgment."""

    hot_rank_map = _hot_rank_map(context)
    facts = _ranked_theme_facts(context)
    facts_by_theme = {fact.plate_name: fact for fact in facts if fact.plate_name}
    evidence: list[TimeframeEvidence] = []
    for fact in facts[:16]:
        evidence.extend(
            _timeframe_evidence_for_theme(
                context,
                fact,
                hot_rank=hot_rank_map.get(fact.plate_name, 999),
            )
        )
    by_theme: dict[str, list[TimeframeEvidence]] = {}
    for item in evidence:
        by_theme.setdefault(item.scope, []).append(item)
    confirmed_like = {
        item.theme_name
        for item in theme_decisions
        if item.local_validation_hint == "confirmed_like"
    }
    target_scores: list[tuple[tuple[int, int, int, int, str], str]] = []
    fading: list[str] = []
    source: list[str] = []
    process_by_theme: dict[str, str] = {}
    current_macro_confirmed: set[str] = set()
    for theme_name, rows in by_theme.items():
        states = {item.timeframe: item.state for item in rows}
        hot_rank = min((item.rank for item in rows), default=999)
        t1_ok = states.get("T1_2m") == "attack"
        t2_ok = states.get("T2_5m") == "spread"
        t3_state = states.get("T3_15m", "neutral")
        hot_ok = hot_rank <= 50
        macro_ok = t2_ok and (t3_state == "migrating_in" or hot_rank <= 30)
        process_state = "observe"
        if t1_ok and t2_ok and t3_state == "migrating_in":
            process_state = "rotation_attack"
        elif t1_ok and t2_ok and hot_ok:
            process_state = "mainline_extend"
        elif t3_state == "withdrawing":
            process_state = "withdrawal"
        elif t1_ok and not t2_ok:
            process_state = "fake_breakout"
        elif t1_ok and theme_name in confirmed_like:
            process_state = "rebound_repair"
        elif hot_ok and t2_ok:
            process_state = "hot_persistence"
        process_by_theme[theme_name] = process_state
        if t3_state == "withdrawing" or (states.get("T2_5m") == "thin" and theme_name in confirmed_like):
            fading.append(theme_name)
        if t3_state == "migrating_in":
            source.append(theme_name)
        if macro_ok:
            current_macro_confirmed.add(theme_name)
        if t1_ok and macro_ok and t3_state != "withdrawing":
            target_scores.append(
                (
                    (
                        0 if theme_name in confirmed_like else 1,
                        0 if t3_state == "migrating_in" else 1,
                        0 if hot_ok else 1,
                        hot_rank,
                        theme_name,
                    ),
                    theme_name,
                )
            )
    memory_lines = _build_temporal_memory_lines(
        context,
        facts_by_theme=facts_by_theme,
        process_by_theme=process_by_theme,
        hot_rank_map=hot_rank_map,
    )
    macro_confirmed_memory = tuple(line.plate_name for line in memory_lines if line.macro_confirmed)
    accelerating_memory = tuple(
        line.plate_name
        for line in memory_lines
        if line.transition_state == "accelerating" and line.macro_confirmed
    )
    micro_accelerating_memory = tuple(
        line.plate_name
        for line in memory_lines
        if line.transition_state == "accelerating" and not line.macro_confirmed
    )
    withdrawing_memory = tuple(line.plate_name for line in memory_lines if line.transition_state in {"withdrawing", "failed_after_attack"})
    rebound_memory = tuple(line.plate_name for line in memory_lines if line.transition_state == "rebound_repair")
    macro_target_scores = tuple(
        theme
        for _, theme in sorted(target_scores)
        if theme in macro_confirmed_memory
        or theme in current_macro_confirmed
    )
    target_themes = tuple(dict.fromkeys((*accelerating_memory, *macro_target_scores)))[:6]
    fading = list(dict.fromkeys((*fading, *withdrawing_memory)))
    fake_breakout_themes = tuple(theme for theme, process in process_by_theme.items() if process == "fake_breakout")[:6]
    exchange_state = "observe"
    if accelerating_memory and withdrawing_memory:
        exchange_state = "rolling_rotation_exchange"
    elif accelerating_memory and not target_themes:
        exchange_state = "rolling_acceleration"
    elif withdrawing_memory and not target_themes:
        exchange_state = "rolling_withdrawal"
    elif rebound_memory and not target_themes:
        exchange_state = "rolling_rebound_repair"
    elif target_themes and fading:
        exchange_state = "rotation_exchange"
    elif any(process_by_theme.get(theme) == "rotation_attack" for theme in target_themes):
        exchange_state = "rotation_attack"
    elif any(process_by_theme.get(theme) == "mainline_extend" for theme in target_themes):
        exchange_state = "mainline_extend"
    elif target_themes:
        exchange_state = "timeframe_aligned"
    elif micro_accelerating_memory:
        exchange_state = "micro_noise_watch"
    elif fake_breakout_themes:
        exchange_state = "fake_breakout"
    elif fading:
        exchange_state = "risk_rotation"
    hot_anchor = min(hot_rank_map.items(), key=lambda item: item[1])[0] if hot_rank_map else ""
    chain_summary = tuple(
        f"{theme}:process={process_by_theme.get(theme, 'observe')}/macro={int(theme in current_macro_confirmed)}/T1={states.get('T1_2m','-')}/T2={states.get('T2_5m','-')}/T3={states.get('T3_15m','-')}/hot={min((item.rank for item in rows), default=999)}"
        for theme, rows in list(by_theme.items())[:6]
        for states in ({item.timeframe: item.state for item in rows},)
    )
    memory_summary = tuple(
        f"{line.plate_name}:rolling={line.transition_state}/macro={int(line.macro_confirmed)}:{line.macro_score}/prev={line.previous_process_state or '-'}/now={line.process_state}/2m={line.previous_amount_2m:.0f}->{line.amount_2m:.0f}/5m={line.previous_amount_5m:.0f}->{line.amount_5m:.0f}/hot={line.previous_hot_rank}->{line.hot_rank}/flow={line.previous_net_inflow_yi_delta:.2f}->{line.net_inflow_yi_delta:.2f}"
        for line in memory_lines[:6]
        if line.transition_state != "steady"
    )
    chain_summary = (*memory_summary[:4], *chain_summary)[:8]
    trace = _trace(
        decision_id=_decision_id("temporal_migration", _phase_name(context)),
        decision_type="temporal_migration",
        scope="market",
        context=context,
        state=exchange_state,
        action_hint="probe" if target_themes else ("avoid_chase" if fading else "watch"),
        confidence_bucket="medium" if target_themes else "low",
        evidence_refs=tuple(item.evidence_refs[0] for item in evidence[:5] if item.evidence_refs),
        lower_decision_refs=tuple(item.trace.decision_id for item in theme_decisions[:8]),
        reason_codes=("timeframe_chain", exchange_state),
        risk_tags=("migration_fading",) if fading and not target_themes else (),
        reject_reason="no_timeframe_aligned_theme" if not target_themes else "",
        invalidation_points=("T1_2m_fades", "T2_spread_breaks", "T3_withdraws") if target_themes else (),
        metrics=(
            f"timeframe_evidence_count={len(evidence)}",
            f"theme_count={len(by_theme)}",
            f"target_count={len(target_themes)}",
            f"source_count={len(source)}",
            f"fading_count={len(fading)}",
            f"fake_breakout_count={len(fake_breakout_themes)}",
            f"rolling_accelerating_count={len(accelerating_memory)}",
            f"micro_accelerating_count={len(micro_accelerating_memory)}",
            f"macro_confirmed_count={len(macro_confirmed_memory)}",
            f"current_macro_confirmed_count={len(current_macro_confirmed)}",
            f"rolling_withdrawing_count={len(withdrawing_memory)}",
            f"rolling_rebound_count={len(rebound_memory)}",
            f"temporal_memory_write={_context_note_value(context, 'temporal_memory_write') or 'default'}",
            f"temporal_sample_key={_temporal_sample_key(context, {})}",
            f"hot_anchor_rank={hot_rank_map.get(hot_anchor, 999) if hot_anchor else 999}",
        ),
        metric_values=(
            ("timeframe_evidence_count", float(len(evidence))),
            ("theme_count", float(len(by_theme))),
            ("target_count", float(len(target_themes))),
            ("source_count", float(len(source))),
            ("fading_count", float(len(fading))),
            ("fake_breakout_count", float(len(fake_breakout_themes))),
            ("rolling_accelerating_count", float(len(accelerating_memory))),
            ("micro_accelerating_count", float(len(micro_accelerating_memory))),
            ("macro_confirmed_count", float(len(macro_confirmed_memory))),
            ("current_macro_confirmed_count", float(len(current_macro_confirmed))),
            ("rolling_withdrawing_count", float(len(withdrawing_memory))),
            ("rolling_rebound_count", float(len(rebound_memory))),
            ("temporal_memory_write", 1.0 if _temporal_memory_write_allowed(context) else 0.0),
            ("hot_anchor_rank", float(hot_rank_map.get(hot_anchor, 999) if hot_anchor else 999)),
        ),
        evidence_summary=chain_summary[:5],
    )
    return TemporalMigrationDecision(
        trace=trace,
        hot_plate_anchor=hot_anchor,
        exchange_state=exchange_state,
        target_themes=target_themes,
        source_themes=tuple(dict.fromkeys(source))[:6],
        fading_themes=tuple(dict.fromkeys(fading))[:6],
        timeframe_evidence=tuple(evidence),
        chain_summary=chain_summary,
        memory_lines=memory_lines,
    )


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
            metrics=(
                f"auction_amount={fact.auction_amount:.0f}",
                f"amount_2m_sum={fact.amount_2m_sum:.0f}",
                f"amount_5m_sum={fact.amount_5m_sum:.0f}",
                f"amount_2m_vs_auction={amount_ratio:.2f}",
                f"front_row_2m_pass_count={fact.front_row_2m_pass_count}",
                f"expansion_count={fact.expansion_count}",
                f"yest_hot_rank={fact.yest_hot_rank}",
                f"hot_rank={hot_fact.rank if hot_fact else 999}",
                f"hot_change_pct={float(getattr(hot_fact, 'change_pct', 0.0) or 0.0):.2f}",
                f"hot_strength={float(getattr(hot_fact, 'strength', 0.0) or 0.0):.0f}",
                f"hot_net_inflow_yi={float(getattr(hot_fact, 'net_inflow_yi', 0.0) or 0.0):.2f}",
            ),
            metric_values=(
                ("auction_amount", float(fact.auction_amount or 0.0)),
                ("amount_2m_sum", float(fact.amount_2m_sum or 0.0)),
                ("amount_5m_sum", float(fact.amount_5m_sum or 0.0)),
                ("amount_2m_vs_auction", amount_ratio),
                ("front_row_2m_pass_count", float(fact.front_row_2m_pass_count or 0)),
                ("expansion_count", float(fact.expansion_count or 0)),
                ("yest_hot_rank", float(fact.yest_hot_rank or 999)),
                ("hot_rank", float(hot_fact.rank if hot_fact else 999)),
                ("hot_change_pct", float(getattr(hot_fact, "change_pct", 0.0) or 0.0)),
                ("hot_strength", float(getattr(hot_fact, "strength", 0.0) or 0.0)),
                ("hot_net_inflow_yi", float(getattr(hot_fact, "net_inflow_yi", 0.0) or 0.0)),
                ("net_inflow_yi_delta", float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0)),
            ),
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


def _hot_metric_by_theme(hot_plate_anchor: HotPlateAnchorDecision | None) -> dict[str, HotPlateMetricLine]:
    if hot_plate_anchor is None:
        return {}
    return {
        line.plate_name: line
        for line in tuple(getattr(hot_plate_anchor, "metric_lines", ()) or ())
        if line.plate_name
    }


def _hot_race_sort_key(
    theme_name: str,
    *,
    hot_metric_map: dict[str, HotPlateMetricLine],
    hot_primary: tuple[str, ...],
    hot_risk: tuple[str, ...],
) -> tuple[float, float, float, float, float, float, float, str]:
    line = hot_metric_map.get(theme_name)
    if line is None:
        return (1.0, 1.0, 1.0, 1.0, 1.0, 999.0, 9.0, theme_name)
    state_rank = {
        "continuation": 0.0,
        "rotation": 0.2,
        "front_2m_watch": 0.5,
        "observe": 1.0,
        "fakeout": 4.0,
        "fading": 5.0,
    }.get(line.state, 2.0)
    if theme_name in hot_primary:
        state_rank -= 0.5
    if theme_name in hot_risk:
        state_rank += 3.0
    # Lower is better. Rank percentiles keep the comparison relative and TopN-friendly.
    return (
        state_rank,
        float(line.hot_rank_pct),
        float(line.change_rank_pct),
        float(line.strength_rank_pct),
        float(line.inflow_rank_pct),
        float(line.amount_2m_rank_pct),
        float(line.rank or 999),
        theme_name,
    )


def _hot_race_line(theme_name: str, line: HotPlateMetricLine | None) -> str:
    if line is None:
        return f"{theme_name}:hot=-"
    return (
        f"{theme_name}:hot_rank={line.rank}/chg={line.change_pct:.2f}/"
        f"strength={line.strength:.0f}/inflow={line.net_inflow_yi:.2f}/"
        f"2m={line.amount_2m:.0f}/state={line.state}"
    )


def build_theme_relative_decision(
    context: IntradayContext,
    *,
    theme_decisions: tuple[ThemeLocalDecision, ...] = (),
    temporal_migration: TemporalMigrationDecision | None = None,
    hot_plate_anchor: HotPlateAnchorDecision | None = None,
) -> ThemeRelativeDecision:
    """Compare local theme decisions horizontally before global hypothesis selection."""

    existing = {decision.theme_name for decision in theme_decisions if decision.theme_name}
    migrating_in = _ordered_existing(getattr(context.market_summary, "migrating_in_plates", ()) or (), existing)
    migrating_out = _ordered_existing(getattr(context.market_summary, "migrating_out_plates", ()) or (), existing)
    hot_primary = _ordered_existing(hot_plate_anchor.primary_themes if hot_plate_anchor is not None else (), existing)
    hot_risk = _ordered_existing(
        (
            *(hot_plate_anchor.fading_themes if hot_plate_anchor is not None else ()),
            *(hot_plate_anchor.fakeout_themes if hot_plate_anchor is not None else ()),
        ),
        existing,
    )
    hot_metric_map = _hot_metric_by_theme(hot_plate_anchor)
    confirmed = [
        decision
        for decision in theme_decisions
        if decision.local_validation_hint == "confirmed_like"
    ]
    confirmed.sort(
        key=lambda decision: (
            decision.theme_name not in hot_primary,
            decision.theme_name not in migrating_in,
            decision.local_script_hint not in {"extension", "rotation_candidate", "repair"},
            decision.spread_level != "strong",
            decision.leader_drive_type == "leader_only",
            *_hot_race_sort_key(
                decision.theme_name,
                hot_metric_map=hot_metric_map,
                hot_primary=hot_primary,
                hot_risk=hot_risk,
            ),
            -_trace_metric_value(decision.trace, "amount_2m_sum"),
            decision.theme_name,
        )
    )
    leading = tuple(decision.theme_name for decision in confirmed if decision.local_script_hint == "extension")[:5]
    rising_base = [
        decision.theme_name
        for decision in confirmed
        if decision.local_script_hint in {"rotation_candidate", "repair"} and decision.theme_name not in leading
    ]
    temporal_targets = temporal_migration.target_themes if temporal_migration is not None else ()
    temporal_fading = temporal_migration.fading_themes if temporal_migration is not None else ()
    rising_pool = tuple(dict.fromkeys((*hot_primary, *migrating_in, *temporal_targets, *rising_base)))
    rising = tuple(
        sorted(
            rising_pool,
            key=lambda name: _hot_race_sort_key(
                name,
                hot_metric_map=hot_metric_map,
                hot_primary=hot_primary,
                hot_risk=hot_risk,
            ),
        )
    )[:5]
    fading = tuple(
        decision.theme_name
        for decision in theme_decisions
        if decision.local_script_hint == "distribution"
        or decision.theme_name in migrating_out
        or decision.theme_name in temporal_fading
        or decision.theme_name in hot_risk
    )[:5]
    fake_rotation = tuple(
        decision.theme_name
        for decision in theme_decisions
        if decision.local_script_hint == "fakeout"
        or ("weak_spread" in decision.trace.risk_tags and decision.theme_name not in rising)
    )[:5]
    mainline_pool = tuple(dict.fromkeys((*hot_primary, *leading, *(name for name in rising if name not in fake_rotation))))
    mainline_candidates = tuple(
        sorted(
            mainline_pool,
            key=lambda name: _hot_race_sort_key(
                name,
                hot_metric_map=hot_metric_map,
                hot_primary=hot_primary,
                hot_risk=hot_risk,
            ),
        )
    )[:5]
    rotation_candidates = tuple(
        sorted(
            (name for name in rising if name not in fading and name not in fake_rotation),
            key=lambda name: _hot_race_sort_key(
                name,
                hot_metric_map=hot_metric_map,
                hot_primary=hot_primary,
                hot_risk=hot_risk,
            ),
        )
    )[:5]
    risk_themes = tuple(dict.fromkeys((*fading, *fake_rotation, *migrating_out, *temporal_fading, *hot_risk)))[:8]
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
    hot_race_leaders = tuple(
        sorted(
            (name for name in dict.fromkeys((*mainline_candidates, *rising, *hot_primary)) if name),
            key=lambda name: _hot_race_sort_key(
                name,
                hot_metric_map=hot_metric_map,
                hot_primary=hot_primary,
                hot_risk=hot_risk,
            ),
        )
    )[:5]
    top_hot_line = hot_metric_map.get(hot_race_leaders[0]) if hot_race_leaders else None
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
        metrics=(
            f"confirmed_count={len(confirmed)}",
            f"leading_count={len(leading)}",
            f"rising_count={len(rising)}",
            f"fading_count={len(fading)}",
            f"fake_rotation_count={len(fake_rotation)}",
            f"risk_theme_count={len(risk_themes)}",
            f"migrating_in_count={len(migrating_in)}",
            f"temporal_target_count={len(temporal_targets)}",
            f"hot_primary_count={len(hot_primary)}",
            f"hot_risk_count={len(hot_risk)}",
            f"hot_race={';'.join(_hot_race_line(name, hot_metric_map.get(name)) for name in hot_race_leaders[:3]) or '-'}",
        ),
        metric_values=(
            ("confirmed_count", float(len(confirmed))),
            ("leading_count", float(len(leading))),
            ("rising_count", float(len(rising))),
            ("fading_count", float(len(fading))),
            ("fake_rotation_count", float(len(fake_rotation))),
            ("risk_theme_count", float(len(risk_themes))),
            ("migrating_in_count", float(len(migrating_in))),
            ("temporal_target_count", float(len(temporal_targets))),
            ("hot_primary_count", float(len(hot_primary))),
            ("hot_risk_count", float(len(hot_risk))),
            ("top_hot_rank", float(top_hot_line.rank if top_hot_line is not None else 999)),
            ("top_hot_change_pct", float(top_hot_line.change_pct if top_hot_line is not None else 0.0)),
            ("top_hot_strength", float(top_hot_line.strength if top_hot_line is not None else 0.0)),
            ("top_hot_net_inflow_yi", float(top_hot_line.net_inflow_yi if top_hot_line is not None else 0.0)),
            ("top_hot_amount_2m", float(top_hot_line.amount_2m if top_hot_line is not None else 0.0)),
        ),
        evidence_summary=(
            f"leading={','.join(leading[:3]) or '-'}",
            f"rising={','.join(rising[:3]) or '-'}",
            f"fading={','.join(fading[:3]) or '-'}",
            f"fake_rotation={','.join(fake_rotation[:3]) or '-'}",
            f"migrating_in={','.join(migrating_in[:3]) or '-'}",
            f"hot_primary={','.join(hot_primary[:3]) or '-'}",
            f"hot_race={','.join(hot_race_leaders[:3]) or '-'}",
            f"temporal={temporal_migration.exchange_state if temporal_migration is not None else '-'}",
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
    focus_asset_stress = build_focus_asset_stress_decision(context)
    theme_decisions = build_theme_local_decisions(context, stock_decisions=stock_decisions)
    hot_plate_anchor = build_hot_plate_anchor_decision(context)
    temporal_migration = build_temporal_migration_decision(context, theme_decisions=theme_decisions)
    theme_relative = build_theme_relative_decision(
        context,
        theme_decisions=theme_decisions,
        temporal_migration=temporal_migration,
        hot_plate_anchor=hot_plate_anchor,
    )
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
        f"focus_asset_stress={focus_asset_stress.stress_state}",
        f"focus_asset_themes={','.join(focus_asset_stress.stressed_themes[:3]) or '-'}",
        f"focus_asset_dragons={','.join(focus_asset_stress.dragon_alone_themes[:3]) or '-'}",
        f"hot_anchor={hot_plate_anchor.anchor_state}",
        f"hot_primary={','.join(hot_plate_anchor.primary_themes[:3]) or '-'}",
        f"hot_rotation={','.join(hot_plate_anchor.rotation_themes[:3]) or '-'}",
        f"hot_fading={','.join(hot_plate_anchor.fading_themes[:3]) or '-'}",
        f"theme_relative={theme_relative.trace.state}",
        f"temporal_migration={temporal_migration.exchange_state}",
        f"temporal_targets={','.join(temporal_migration.target_themes[:3]) or '-'}",
        f"temporal_fading={','.join(temporal_migration.fading_themes[:3]) or '-'}",
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
        focus_asset_stress_decision=focus_asset_stress,
        theme_local_decisions=theme_decisions,
        hot_plate_anchor_decision=hot_plate_anchor,
        temporal_migration_decision=temporal_migration,
        theme_relative_decision=theme_relative,
        notes=notes,
    )
