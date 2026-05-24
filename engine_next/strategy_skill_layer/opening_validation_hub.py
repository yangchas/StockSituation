from __future__ import annotations

from engine_next.domain.models import (
    IntradayContext,
    OpeningValidationBundle,
    PlateMigrationFact,
    StockStateSnapshot,
    ThemeOpeningValidation,
    ThemeSelectionContext,
    ThemeTradeFact,
)
from engine_next.runtime.plate_mapping_registry import normalize_plate_name
from engine_next.strategy_skill_layer.shape_engine import resolve_theme_name


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return default


def iter_opening_validation_plate_names(
    *,
    snapshot: StockStateSnapshot | None = None,
    selection=None,
    extra_plate_names: tuple[str, ...] | list[str] = (),
) -> tuple[str, ...]:
    ordered: list[str] = []

    def _push(value: object) -> None:
        normalized = normalize_plate_name(value)
        if normalized and normalized != "-" and normalized not in ordered:
            ordered.append(normalized)

    for plate_name in extra_plate_names:
        _push(plate_name)
    if snapshot is not None:
        _push(resolve_theme_name(snapshot))
        _push(getattr(snapshot, "plate", ""))
    if selection is not None:
        _push(getattr(selection, "plate_name", ""))
    return tuple(ordered)


def match_opening_validation(
    bundle: OpeningValidationBundle | None,
    *,
    snapshot: StockStateSnapshot | None = None,
    selection=None,
    extra_plate_names: tuple[str, ...] | list[str] = (),
):
    if bundle is None:
        return None
    for plate_name in iter_opening_validation_plate_names(
        snapshot=snapshot,
        selection=selection,
        extra_plate_names=extra_plate_names,
    ):
        for mapping in (
            getattr(bundle, "confirmed_themes", {}) or {},
            getattr(bundle, "watch_themes", {}) or {},
            getattr(bundle, "falsified_themes", {}) or {},
        ):
            item = mapping.get(plate_name)
            if item is not None:
                return item
    return None


def _rank_pct_desc(pairs: list[tuple[str, float]]) -> dict[str, float]:
    ranked = sorted(pairs, key=lambda pair: pair[1], reverse=True)
    if not ranked:
        return {}
    if len(ranked) == 1:
        return {ranked[0][0]: 0.0}
    return {
        plate: round(index / (len(ranked) - 1), 4)
        for index, (plate, _value) in enumerate(ranked)
    }


def _theme_predicted_script(theme_context: ThemeSelectionContext | None) -> str:
    if theme_context is None:
        return "unknown"
    trade_label = str(theme_context.theme_trade_label or "").strip()
    trade_conclusion = str(theme_context.trade_conclusion or "").strip()
    if trade_label == "old_mainline" or trade_conclusion.startswith("old_mainline_"):
        if "distribution" in trade_conclusion:
            return "distribution"
        return "extension"
    if trade_label == "switch_candidate" or trade_conclusion.startswith("switch_"):
        return "rotation"
    if trade_conclusion in {"leader_only_alive", "high_event_self_excited", "rotation_noise"}:
        return "distribution"
    return "unknown"


def _front_row_snapshots(snapshots: list[StockStateSnapshot]) -> list[StockStateSnapshot]:
    return [
        snapshot
        for snapshot in snapshots
        if snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1
    ]


def _front_row_confirmed_count(front_row: list[StockStateSnapshot]) -> int:
    count = 0
    for snapshot in front_row:
        auction_amount = _safe_float(snapshot.auction_amount)
        amount_2m = _safe_float(snapshot.amount_2m)
        if (
            amount_2m >= max(auction_amount * 0.8, 20_000_000.0)
            and _safe_float(snapshot.current_pct) >= _safe_float(snapshot.open_pct) - 0.015
            and _safe_float(snapshot.speed_1m) > -0.002
        ):
            count += 1
    return count


def _mid_follow_confirmed_count(snapshots: list[StockStateSnapshot]) -> int:
    count = 0
    for snapshot in snapshots:
        if snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1:
            continue
        if (
            _safe_float(snapshot.amount_2m) >= 15_000_000.0
            and _safe_float(snapshot.current_pct) >= max(_safe_float(snapshot.open_pct), 0.0)
            and _safe_float(snapshot.speed_1m) >= 0.0
        ):
            count += 1
    return count


def _resolve_high_level_feedback(front_row: list[StockStateSnapshot]) -> str:
    if not front_row:
        return "unknown"
    highest = max(front_row, key=lambda item: (_safe_int(item.lb_days), -_safe_int(item.leader_rank_in_theme)))
    current_pct = _safe_float(highest.current_pct)
    open_pct = _safe_float(highest.open_pct)
    if current_pct >= max(open_pct - 0.01, 0.02):
        return "strong"
    if current_pct <= open_pct - 0.03 or current_pct < -0.01:
        return "weak"
    return "mixed"


def _build_theme_opening_metrics(
    context: IntradayContext,
    theme_context_map: dict[str, ThemeSelectionContext],
) -> dict[str, dict[str, object]]:
    plate_snapshots: dict[str, list[StockStateSnapshot]] = {plate: [] for plate in theme_context_map}
    for snapshot in context.stock_snapshots:
        plate_name = resolve_theme_name(snapshot)
        if plate_name in plate_snapshots:
            plate_snapshots[plate_name].append(snapshot)

    amount_pairs: list[tuple[str, float]] = []
    metrics_map: dict[str, dict[str, object]] = {}
    theme_trade_fact_map = dict(getattr(context.session_facts, "theme_trade_fact_map", {}) or {})
    migration_map = dict(getattr(context.session_facts, "plate_migration_map", {}) or {})
    for plate_name, snapshots in plate_snapshots.items():
        front_row = _front_row_snapshots(snapshots)
        front_confirmed_count = _front_row_confirmed_count(front_row)
        mid_confirmed_count = _mid_follow_confirmed_count(snapshots)
        amount_2m_sum = sum(_safe_float(snapshot.amount_2m) for snapshot in snapshots)
        auction_amount_sum = sum(_safe_float(snapshot.auction_amount) for snapshot in snapshots)
        amount_pairs.append((plate_name, amount_2m_sum))
        theme_trade_fact = theme_trade_fact_map.get(plate_name)
        migration = migration_map.get(plate_name)
        metrics_map[plate_name] = {
            "front_row_count": len(front_row),
            "front_row_confirmed_count": front_confirmed_count,
            "mid_follow_confirmed_count": mid_confirmed_count,
            "high_level_feedback": _resolve_high_level_feedback(front_row),
            "amount_2m_sum": amount_2m_sum,
            "auction_amount_sum": auction_amount_sum,
            "amount_2m_ratio_vs_auction": (amount_2m_sum / auction_amount_sum) if auction_amount_sum > 0 else 0.0,
            "theme_trade_fact": theme_trade_fact,
            "net_inflow_delta_yi": _safe_float(getattr(migration, "net_inflow_yi_delta", 0.0)) if migration else 0.0,
            "is_migrating_in": plate_name in getattr(context.market_summary, "migrating_in_plates", ()),
            "is_migrating_out": plate_name in getattr(context.market_summary, "migrating_out_plates", ()),
            "red_count": sum(1 for snapshot in snapshots if _safe_float(snapshot.current_pct) >= 0.0),
            "green_count": sum(1 for snapshot in snapshots if _safe_float(snapshot.current_pct) < 0.0),
        }
    rank_pct_map = _rank_pct_desc(amount_pairs)
    for plate_name, metrics in metrics_map.items():
        metrics["amount_2m_rank_pct"] = float(rank_pct_map.get(plate_name, 1.0))
    return metrics_map


def _validate_extension(
    plate_name: str,
    metrics: dict[str, object],
) -> ThemeOpeningValidation:
    front_row_count = _safe_int(metrics.get("front_row_count"))
    front_row_confirmed_count = _safe_int(metrics.get("front_row_confirmed_count"))
    mid_follow_confirmed_count = _safe_int(metrics.get("mid_follow_confirmed_count"))
    high_level_feedback = str(metrics.get("high_level_feedback") or "unknown")
    amount_rank_pct = _safe_float(metrics.get("amount_2m_rank_pct"), 1.0)
    amount_ratio = _safe_float(metrics.get("amount_2m_ratio_vs_auction"))
    net_inflow_delta = _safe_float(metrics.get("net_inflow_delta_yi"))
    evidence: list[str] = []
    hits = 0
    if front_row_count > 0 and front_row_confirmed_count * 2 >= front_row_count:
        hits += 1
        evidence.append("前排承接成立")
    if mid_follow_confirmed_count >= 1:
        hits += 1
        evidence.append("中位扩散成立")
    if high_level_feedback != "weak":
        hits += 1
        evidence.append("高位反馈未转弱")
    if amount_rank_pct <= 0.35 or amount_ratio >= 0.8:
        hits += 1
        evidence.append("2m量能达标")
    if net_inflow_delta >= -2.0:
        hits += 1
        evidence.append("资金未明显抽离")
    if hits >= 4:
        return ThemeOpeningValidation(
            plate_name=plate_name,
            predicted_script="extension",
            validation_state="confirmed",
            tradable_level="attack",
            front_row_confirmed=front_row_count > 0 and front_row_confirmed_count * 2 >= front_row_count,
            mid_follow_confirmed=mid_follow_confirmed_count >= 1,
            high_level_feedback=high_level_feedback,
            amount_2m_rank_pct=amount_rank_pct,
            amount_2m_ratio_vs_auction=amount_ratio,
            net_inflow_delta_yi=net_inflow_delta,
            is_migrating_in=bool(metrics.get("is_migrating_in")),
            is_migrating_out=bool(metrics.get("is_migrating_out")),
            evidence=tuple(evidence),
        )
    invalid_reason = "前排承接不足或中位扩散不够"
    if front_row_count > 0 and front_row_confirmed_count == 0:
        invalid_reason = "前排承接不足"
    elif high_level_feedback == "weak":
        invalid_reason = "高位反馈转弱"
    return ThemeOpeningValidation(
        plate_name=plate_name,
        predicted_script="extension",
        validation_state="falsified" if hits <= 1 else "watch",
        tradable_level="avoid" if hits <= 1 else "watch",
        front_row_confirmed=front_row_count > 0 and front_row_confirmed_count * 2 >= front_row_count,
        mid_follow_confirmed=mid_follow_confirmed_count >= 1,
        high_level_feedback=high_level_feedback,
        amount_2m_rank_pct=amount_rank_pct,
        amount_2m_ratio_vs_auction=amount_ratio,
        net_inflow_delta_yi=net_inflow_delta,
        is_migrating_in=bool(metrics.get("is_migrating_in")),
        is_migrating_out=bool(metrics.get("is_migrating_out")),
        evidence=tuple(evidence),
        invalid_reason=invalid_reason,
    )


def _validate_distribution(
    plate_name: str,
    metrics: dict[str, object],
) -> ThemeOpeningValidation:
    front_row_count = _safe_int(metrics.get("front_row_count"))
    front_row_confirmed_count = _safe_int(metrics.get("front_row_confirmed_count"))
    mid_follow_confirmed_count = _safe_int(metrics.get("mid_follow_confirmed_count"))
    high_level_feedback = str(metrics.get("high_level_feedback") or "unknown")
    amount_rank_pct = _safe_float(metrics.get("amount_2m_rank_pct"), 1.0)
    amount_ratio = _safe_float(metrics.get("amount_2m_ratio_vs_auction"))
    net_inflow_delta = _safe_float(metrics.get("net_inflow_delta_yi"))
    evidence: list[str] = []
    hits = 0
    if front_row_count > 0 and front_row_confirmed_count * 2 < front_row_count:
        hits += 1
        evidence.append("前排承接偏弱")
    if mid_follow_confirmed_count == 0:
        hits += 1
        evidence.append("中位不跟")
    if high_level_feedback == "weak":
        hits += 1
        evidence.append("高位负反馈")
    if amount_rank_pct > 0.55 and amount_ratio < 0.75:
        hits += 1
        evidence.append("2m量能掉队")
    if net_inflow_delta <= -2.0 or bool(metrics.get("is_migrating_out")):
        hits += 1
        evidence.append("资金抽离")
    state = "confirmed" if hits >= 3 else "watch"
    tradable_level = "avoid" if state == "confirmed" else "watch"
    return ThemeOpeningValidation(
        plate_name=plate_name,
        predicted_script="distribution",
        validation_state=state,
        tradable_level=tradable_level,
        front_row_confirmed=front_row_count > 0 and front_row_confirmed_count * 2 >= front_row_count,
        mid_follow_confirmed=mid_follow_confirmed_count >= 1,
        high_level_feedback=high_level_feedback,
        amount_2m_rank_pct=amount_rank_pct,
        amount_2m_ratio_vs_auction=amount_ratio,
        net_inflow_delta_yi=net_inflow_delta,
        is_migrating_in=bool(metrics.get("is_migrating_in")),
        is_migrating_out=bool(metrics.get("is_migrating_out")),
        evidence=tuple(evidence),
        invalid_reason="" if state != "confirmed" else "开盘兑现确认",
    )


def _validate_rotation(
    plate_name: str,
    metrics: dict[str, object],
) -> ThemeOpeningValidation:
    front_row_count = _safe_int(metrics.get("front_row_count"))
    front_row_confirmed_count = _safe_int(metrics.get("front_row_confirmed_count"))
    mid_follow_confirmed_count = _safe_int(metrics.get("mid_follow_confirmed_count"))
    high_level_feedback = str(metrics.get("high_level_feedback") or "unknown")
    amount_rank_pct = _safe_float(metrics.get("amount_2m_rank_pct"), 1.0)
    amount_ratio = _safe_float(metrics.get("amount_2m_ratio_vs_auction"))
    net_inflow_delta = _safe_float(metrics.get("net_inflow_delta_yi"))
    evidence: list[str] = []
    hits = 0
    if front_row_confirmed_count >= 1:
        hits += 1
        evidence.append("前排先转强")
    if mid_follow_confirmed_count >= 1:
        hits += 1
        evidence.append("中位开始扩散")
    if amount_rank_pct <= 0.4 or amount_ratio >= 0.9:
        hits += 1
        evidence.append("2m量能进入前列")
    if net_inflow_delta >= 0.0 or bool(metrics.get("is_migrating_in")):
        hits += 1
        evidence.append("资金回流/流入")
    if high_level_feedback != "weak":
        hits += 1
        evidence.append("高位未拖累")
    if hits >= 4:
        state = "confirmed"
        tradable_level = "attack"
    elif hits >= 2:
        state = "watch"
        tradable_level = "probe"
    else:
        state = "falsified"
        tradable_level = "avoid"
    return ThemeOpeningValidation(
        plate_name=plate_name,
        predicted_script="rotation",
        validation_state=state,
        tradable_level=tradable_level,
        front_row_confirmed=front_row_confirmed_count >= 1,
        mid_follow_confirmed=mid_follow_confirmed_count >= 1,
        high_level_feedback=high_level_feedback,
        amount_2m_rank_pct=amount_rank_pct,
        amount_2m_ratio_vs_auction=amount_ratio,
        net_inflow_delta_yi=net_inflow_delta,
        is_migrating_in=bool(metrics.get("is_migrating_in")),
        is_migrating_out=bool(metrics.get("is_migrating_out")),
        evidence=tuple(evidence),
        invalid_reason="切换量能与扩散不足" if state == "falsified" else "",
    )


def build_opening_validation_bundle(
    context: IntradayContext,
    theme_context_map: dict[str, ThemeSelectionContext],
    *,
    phase_label: str,
) -> OpeningValidationBundle:
    metrics_map = _build_theme_opening_metrics(context, theme_context_map)
    confirmed: dict[str, ThemeOpeningValidation] = {}
    falsified: dict[str, ThemeOpeningValidation] = {}
    watch: dict[str, ThemeOpeningValidation] = {}
    for plate_name, theme_context in theme_context_map.items():
        predicted_script = _theme_predicted_script(theme_context)
        metrics = metrics_map.get(plate_name, {})
        if predicted_script == "extension":
            result = _validate_extension(plate_name, metrics)
        elif predicted_script == "distribution":
            result = _validate_distribution(plate_name, metrics)
        elif predicted_script == "rotation":
            result = _validate_rotation(plate_name, metrics)
        else:
            result = ThemeOpeningValidation(
                plate_name=plate_name,
                predicted_script=predicted_script,
                validation_state="watch",
                tradable_level="watch",
                front_row_confirmed=False,
                mid_follow_confirmed=False,
                high_level_feedback=str(metrics.get("high_level_feedback") or "unknown"),
                amount_2m_rank_pct=_safe_float(metrics.get("amount_2m_rank_pct"), 1.0),
                amount_2m_ratio_vs_auction=_safe_float(metrics.get("amount_2m_ratio_vs_auction")),
                net_inflow_delta_yi=_safe_float(metrics.get("net_inflow_delta_yi")),
                is_migrating_in=bool(metrics.get("is_migrating_in")),
                is_migrating_out=bool(metrics.get("is_migrating_out")),
                evidence=(),
                invalid_reason="预判剧本不明确",
            )
        if result.validation_state == "confirmed":
            confirmed[plate_name] = result
        elif result.validation_state == "falsified":
            falsified[plate_name] = result
        else:
            watch[plate_name] = result

    confirmed_sorted = sorted(
        confirmed.values(),
        key=lambda item: (
            item.tradable_level == "attack",
            -item.amount_2m_rank_pct,
            item.front_row_confirmed,
            item.mid_follow_confirmed,
        ),
        reverse=True,
    )
    notes = (
        f"confirmed={len(confirmed)}",
        f"falsified={len(falsified)}",
        f"watch={len(watch)}",
    )
    return OpeningValidationBundle(
        trade_date=context.trade_date,
        phase=phase_label,
        confirmed_themes=confirmed,
        falsified_themes=falsified,
        watch_themes=watch,
        main_validated_theme=confirmed_sorted[0].plate_name if confirmed_sorted else "",
        backup_validated_theme=confirmed_sorted[1].plate_name if len(confirmed_sorted) > 1 else "",
        notes=notes,
    )
