from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable

from engine_next.domain.models import (
    AuctionLadderDecision,
    IntradayContext,
    StockProfileAssessment,
    StockSelectionContext,
    ThemeSelectionContext,
)
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.strategy_skill_layer.auction_and_ladder import build_auction_and_ladder_decision
from engine_next.strategy_skill_layer.opening_validation_hub import match_opening_validation
from engine_next.strategy_skill_layer.shape_engine import (
    build_stock_selection_context,
    build_theme_context_map,
    resolve_theme_name,
    should_evaluate_stock_shape_fast,
)
from engine_next.strategy_skill_layer.slice_comparison import (
    build_market_topn_slice_comparison,
    topn_expansion_factor,
)
from engine_next.strategy_skill_layer.stock_profile import assess_stock_profile
from engine_next.strategy_skill_layer.trade_conclusion_gate import stock_passes_conclusion_gate


@dataclass(frozen=True)
class ContextStrategyBundle:
    context: IntradayContext
    profiles: tuple[StockProfileAssessment, ...]
    theme_context_map: dict[str, ThemeSelectionContext]
    stock_selection_contexts: tuple[StockSelectionContext, ...]
    decisions: tuple[AuctionLadderDecision, ...]
    focus_symbols: tuple[str, ...]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class _CachedStockSelectionEntry:
    signature: tuple[object, ...]
    context: StockSelectionContext


_STOCK_SELECTION_CACHE: dict[tuple[str, str], _CachedStockSelectionEntry] = {}
_THEME_CONCLUSION_CACHE_TTL_SECONDS = 300


@dataclass(frozen=True)
class _RelativeStrengthProfile:
    amount_2m_top: frozenset[str]
    amount_ratio_2m_top: frozenset[str]
    open_undertake_top: frozenset[str]
    execution_quality_top: frozenset[str]
    shape_quality_top: frozenset[str]
    turnover_quality_top: frozenset[str]
    theme_core_top: frozenset[str]


def _decision_action_priority(action: str) -> int:
    priority_map = {
        "dragon_early_board": 5,
        "early_boarding_candidate": 4,
        "confirm_then_go": 3,
        "small_probe_only": 2,
        "leader_watch": 2,
        "front_row_watch": 1,
        "n_rebound": 1,
    }
    return priority_map.get(str(action or ""), 0)


def _rank_pct_desc(pairs: list[tuple[str, float]]) -> dict[str, float]:
    ranked = sorted(pairs, key=lambda pair: pair[1], reverse=True)
    if not ranked:
        return {}
    if len(ranked) == 1:
        return {ranked[0][0]: 0.0}
    return {
        symbol: round(index / (len(ranked) - 1), 4)
        for index, (symbol, _value) in enumerate(ranked)
    }


def _enrich_stock_relative_rank_pcts(
    snapshots: tuple,
    selections: tuple[StockSelectionContext, ...],
) -> tuple[StockSelectionContext, ...]:
    snapshot_map = {item.symbol: item for item in snapshots}
    selection_map = {item.symbol: item for item in selections}
    grouped_symbols: dict[str, list[str]] = {}
    for selection in selections:
        grouped_symbols.setdefault(selection.plate_name or "", []).append(selection.symbol)

    enriched: list[StockSelectionContext] = []
    for selection in selections:
        theme_symbols = grouped_symbols.get(selection.plate_name or "", [])
        amount_pairs: list[tuple[str, float]] = []
        amount_ratio_pairs: list[tuple[str, float]] = []
        execution_pairs: list[tuple[str, float]] = []
        shape_pairs: list[tuple[str, float]] = []
        for symbol in theme_symbols:
            snapshot = snapshot_map.get(symbol)
            matched = selection_map.get(symbol)
            if snapshot is None or matched is None:
                continue
            auction_amount = float(getattr(snapshot, "auction_amount", 0.0) or 0.0)
            amount_2m = float(getattr(snapshot, "amount_2m", 0.0) or 0.0)
            amount_pairs.append((symbol, amount_2m))
            amount_ratio_pairs.append((symbol, (amount_2m / auction_amount) if auction_amount > 0 else 0.0))
            execution_pairs.append((symbol, float(matched.execution_quality_score or 0.0)))
            shape_pairs.append((symbol, float(matched.shape_quality_score or 0.0)))
        amount_rank_pct = _rank_pct_desc(amount_pairs)
        amount_ratio_rank_pct = _rank_pct_desc(amount_ratio_pairs)
        execution_rank_pct = _rank_pct_desc(execution_pairs)
        shape_rank_pct = _rank_pct_desc(shape_pairs)
        enriched.append(
            replace(
                selection,
                stock_amount_2m_rank_in_theme_pct=float(amount_rank_pct.get(selection.symbol, 1.0)),
                stock_amount_ratio_2m_rank_in_theme_pct=float(amount_ratio_rank_pct.get(selection.symbol, 1.0)),
                stock_execution_rank_in_theme_pct=float(execution_rank_pct.get(selection.symbol, 1.0)),
                stock_shape_rank_in_theme_pct=float(shape_rank_pct.get(selection.symbol, 1.0)),
                notes=tuple(
                    list(selection.notes)
                    + [
                        f"amount_2m_rank_pct={float(amount_rank_pct.get(selection.symbol, 1.0)):.3f}",
                        f"amount_ratio_2m_rank_pct={float(amount_ratio_rank_pct.get(selection.symbol, 1.0)):.3f}",
                        f"execution_rank_pct={float(execution_rank_pct.get(selection.symbol, 1.0)):.3f}",
                        f"shape_rank_pct={float(shape_rank_pct.get(selection.symbol, 1.0)):.3f}",
                        f"daily_height={selection.daily_height_bucket}",
                    ]
                ),
            )
        )
    return tuple(enriched)


def _prune_stock_selection_cache(current_trade_date: str) -> None:
    stale_keys = [key for key in _STOCK_SELECTION_CACHE if key[0] != current_trade_date]
    for key in stale_keys:
        _STOCK_SELECTION_CACHE.pop(key, None)


def _theme_context_signature(theme_context: ThemeSelectionContext | None) -> tuple[object, ...]:
    if theme_context is None:
        return ("none",)
    return (
        theme_context.plate_name,
        round(float(theme_context.e_score or 0.0), 3),
        round(float(theme_context.a_score or 0.0), 3),
        round(float(theme_context.x_score or 0.0), 3),
        str(theme_context.market_regime or ""),
        str(theme_context.theme_trade_label or ""),
        str(theme_context.trade_conclusion or ""),
        str(theme_context.fakeout_level or ""),
        str(theme_context.cohesion_level or ""),
        bool(theme_context.tradable),
        str(theme_context.bias_action or ""),
        str(theme_context.open_confirm_state or ""),
        round(float(theme_context.phase_priority_bias or 0.0), 3),
    )


def _stock_selection_signature(
    snapshot,
    theme_context: ThemeSelectionContext | None,
) -> tuple[object, ...]:
    return (
        round(float(snapshot.current_pct or 0.0), 5),
        round(float(snapshot.open_pct or 0.0), 5),
        round(float(snapshot.auction_amount or 0.0), 2),
        round(float(snapshot.amount_2m or 0.0), 2),
        round(float(snapshot.speed_1m or 0.0), 5),
        round(float(snapshot.vol_ratio or 0.0), 4),
        int(snapshot.ths_hot_rank or 999),
        round(float(snapshot.ths_hot_heat or 0.0), 2),
        int(snapshot.leader_rank_in_theme or 999),
        int(snapshot.lb_days or 0),
        _theme_context_signature(theme_context),
    )


def _persist_theme_conclusions(
    context: IntradayContext,
    theme_context_map: dict[str, ThemeSelectionContext],
) -> None:
    conclusions = {
        plate: str(getattr(theme_context, "trade_conclusion", "") or "").strip()
        for plate, theme_context in theme_context_map.items()
        if str(getattr(theme_context, "trade_conclusion", "") or "").strip()
        and str(getattr(theme_context, "trade_conclusion", "") or "").strip() != "unknown"
    }
    if not conclusions:
        return
    try:
        hub = IntradayDataHub()
        redis_key = f"cache:theme_conclusions:{context.trade_date}"
        hub.redis.hset(redis_key, mapping=conclusions)
        hub.redis.expire(redis_key, _THEME_CONCLUSION_CACHE_TTL_SECONDS)
    except Exception:
        return


def build_context_strategy_bundle(context: IntradayContext) -> ContextStrategyBundle:
    return build_context_strategy_bundle_for_symbols(context, symbols=None)


def build_context_strategy_bundle_for_symbols(
    context: IntradayContext,
    *,
    symbols: Iterable[str] | None,
    theme_context_map: dict[str, ThemeSelectionContext] | None = None,
) -> ContextStrategyBundle:
    symbol_filter = {str(symbol) for symbol in symbols or () if str(symbol)}
    total_snapshot_count = len(context.stock_snapshots)
    if symbol_filter:
        selected_snapshots = tuple(snapshot for snapshot in context.stock_snapshots if snapshot.symbol in symbol_filter)
    else:
        selected_snapshots = tuple(
            snapshot for snapshot in context.stock_snapshots if should_evaluate_stock_shape_fast(snapshot)
        )
    selected_snapshot_count = len(selected_snapshots)
    compression_ratio = (
        round(1.0 - (selected_snapshot_count / total_snapshot_count), 4)
        if total_snapshot_count > 0
        else 0.0
    )
    resolved_theme_context_map = theme_context_map or build_theme_context_map(context, tuple(context.stock_snapshots))
    _persist_theme_conclusions(context, resolved_theme_context_map)
    profiles = tuple(assess_stock_profile(snapshot) for snapshot in selected_snapshots)
    _prune_stock_selection_cache(context.trade_date)
    stock_selection_context_list: list[StockSelectionContext] = []
    stock_ctx_recomputed = 0
    stock_ctx_reused = 0
    for snapshot in selected_snapshots:
        theme_ctx = resolved_theme_context_map.get(resolve_theme_name(snapshot))
        cache_key = (context.trade_date, snapshot.symbol)
        signature = _stock_selection_signature(snapshot, theme_ctx)
        cached = _STOCK_SELECTION_CACHE.get(cache_key)
        if cached is not None and cached.signature == signature:
            stock_selection_context_list.append(cached.context)
            stock_ctx_reused += 1
            continue
        selection = build_stock_selection_context(snapshot, theme_ctx)
        _STOCK_SELECTION_CACHE[cache_key] = _CachedStockSelectionEntry(
            signature=signature,
            context=selection,
        )
        stock_selection_context_list.append(selection)
        stock_ctx_recomputed += 1
    stock_selection_contexts = _enrich_stock_relative_rank_pcts(
        selected_snapshots,
        tuple(stock_selection_context_list),
    )
    decisions = tuple(
        build_auction_and_ladder_decision(
            snapshot,
            profile=profile,
            stock_selection=selection,
            theme_selection=resolved_theme_context_map.get(selection.plate_name),
            market_summary=context.market_summary,
        )
        for snapshot, profile, selection in zip(selected_snapshots, profiles, stock_selection_contexts)
    )

    pre_rank_bundle = ContextStrategyBundle(
        context=context,
        profiles=profiles,
        theme_context_map=resolved_theme_context_map,
        stock_selection_contexts=stock_selection_contexts,
        decisions=decisions,
        focus_symbols=(),
        notes=(),
    )
    upgraded_bundle = _upgrade_bundle_decisions_by_opening_validation(pre_rank_bundle)
    upgraded_decisions = upgraded_bundle.decisions

    ranked = sorted(
        zip(upgraded_decisions, stock_selection_contexts),
        key=lambda pair: (
            pair[1].theme_tradable,
            _decision_action_priority(pair[0].action),
            pair[1].is_true_leader,
            pair[1].is_front_row,
            pair[0].confidence,
            -pair[1].hot_rank,
            pair[1].is_active_pool,
            pair[0].risk_reward_ratio,
        ),
        reverse=True,
    )
    ranked_decisions = tuple(decision for decision, _ in ranked)
    focus_symbols = tuple(decision.symbol for decision in ranked_decisions[:10])
    summary = context.market_summary
    notes = (
        f"mainline_sector={summary.mainline_sector or 'N/A'}",
        f"top_turnover_count={len(summary.top_turnover_symbols)}",
        f"theme_context_count={len(resolved_theme_context_map)}",
        f"decision_count={len(ranked_decisions)}",
        f"selected_snapshot_count={selected_snapshot_count}",
        f"total_snapshot_count={total_snapshot_count}",
        f"shape_scope_mode={'explicit_symbols' if symbol_filter else 'fast_active_prefilter'}",
        f"shape_prefilter_compression_ratio={compression_ratio:.4f}",
        f"auction_top10_vs_prev_ratio={float(getattr(summary, 'auction_top10_vs_prev_ratio', 1.0) or 1.0):.3f}",
        f"auction_top20_vs_prev_ratio={float(getattr(summary, 'auction_top20_vs_prev_ratio', 1.0) or 1.0):.3f}",
        f"stock_ctx_recomputed={stock_ctx_recomputed}",
        f"stock_ctx_reused={stock_ctx_reused}",
    )
    bundle = ContextStrategyBundle(
        context=context,
        profiles=profiles,
        theme_context_map=resolved_theme_context_map,
        stock_selection_contexts=stock_selection_contexts,
        decisions=ranked_decisions,
        focus_symbols=focus_symbols,
        notes=notes,
    )
    return bundle


def _passes_trade_conclusion_gate(selection, snapshot, theme_context) -> bool:
    if not stock_passes_conclusion_gate(selection, snapshot, theme_context):
        return False
    if selection is not None and selection.theme_trade_label == "high_event" and not selection.is_true_leader:
        return False
    if selection is not None and selection.open_confirm_state == "falsified" and not selection.is_true_leader:
        return False
    if selection is not None and not selection.theme_tradable:
        return False
    if selection is not None and selection.theme_fakeout_level in {"high", "extreme"} and not selection.is_true_leader:
        return False
    if selection is not None and selection.theme_x_score >= 5.6 and not selection.is_true_leader:
        return False
    if (
        selection is not None
        and theme_context is not None
        and theme_context.trade_conclusion == "leader_only_alive"
        and not selection.is_true_leader
    ):
        front_row_override = bool(
            selection.is_front_row
            and selection.theme_tradable
            and selection.hot_rank <= 80
        )
        if not front_row_override:
            return False
    if (
        selection is not None
        and theme_context is not None
        and theme_context.bias_action in {"observe_only", "avoid_after_open_confirm"}
        and theme_context.open_confirm_state in {"maintained", "falsified"}
        and not selection.is_true_leader
        and float(getattr(theme_context, "phase_priority_bias", 0.0) or 0.0) <= 0.0
    ):
        return False
    return True


def _resolve_top_n(size: int, *, minimum: int, ratio: float, maximum: int | None = None) -> int:
    if size <= 0:
        return 0
    top_n = max(minimum, int(round(size * ratio)))
    if maximum is not None:
        top_n = min(top_n, maximum)
    return max(1, min(size, top_n))


def _front_strength_expansion_factor(context: IntradayContext) -> float:
    return topn_expansion_factor(build_market_topn_slice_comparison(getattr(context, "market_summary", None)))


def _top_symbols_by_metric(
    symbol_metric_pairs: list[tuple[str, float]],
    *,
    minimum: int,
    ratio: float,
    maximum: int | None = None,
) -> frozenset[str]:
    valid_pairs = [(symbol, float(value)) for symbol, value in symbol_metric_pairs if symbol]
    if not valid_pairs:
        return frozenset()
    ranked = sorted(valid_pairs, key=lambda pair: pair[1], reverse=True)
    limit = _resolve_top_n(len(ranked), minimum=minimum, ratio=ratio, maximum=maximum)
    return frozenset(symbol for symbol, _ in ranked[:limit])


def _build_relative_strength_profile(bundle: ContextStrategyBundle) -> _RelativeStrengthProfile:
    stock_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
    snapshot_map = {item.symbol: item for item in bundle.context.stock_snapshots}
    symbols = [decision.symbol for decision in bundle.decisions if decision.symbol in stock_context_map and decision.symbol in snapshot_map]
    expansion_factor = _front_strength_expansion_factor(bundle.context)

    def metric_pairs_from_snapshot(attr: str) -> list[tuple[str, float]]:
        return [(symbol, float(getattr(snapshot_map[symbol], attr, 0.0) or 0.0)) for symbol in symbols]

    def metric_pairs_from_selection(attr: str) -> list[tuple[str, float]]:
        return [(symbol, float(getattr(stock_context_map[symbol], attr, 0.0) or 0.0)) for symbol in symbols]

    amount_ratio_pairs: list[tuple[str, float]] = []
    for symbol in symbols:
        snapshot = snapshot_map[symbol]
        auction_amount = float(getattr(snapshot, "auction_amount", 0.0) or 0.0)
        amount_2m = float(getattr(snapshot, "amount_2m", 0.0) or 0.0)
        ratio_2m = (amount_2m / auction_amount) if auction_amount > 0 else 0.0
        amount_ratio_pairs.append((symbol, ratio_2m))

    return _RelativeStrengthProfile(
        amount_2m_top=_top_symbols_by_metric(metric_pairs_from_snapshot("amount_2m"), minimum=10, ratio=0.12 * expansion_factor, maximum=24),
        amount_ratio_2m_top=_top_symbols_by_metric(amount_ratio_pairs, minimum=10, ratio=0.14 * expansion_factor, maximum=28),
        open_undertake_top=_top_symbols_by_metric(metric_pairs_from_selection("open_undertake_score"), minimum=12, ratio=0.16 * expansion_factor, maximum=28),
        execution_quality_top=_top_symbols_by_metric(metric_pairs_from_selection("execution_quality_score"), minimum=12, ratio=0.16 * expansion_factor, maximum=28),
        shape_quality_top=_top_symbols_by_metric(metric_pairs_from_selection("shape_quality_score"), minimum=12, ratio=0.18 * expansion_factor, maximum=30),
        turnover_quality_top=_top_symbols_by_metric(metric_pairs_from_selection("turnover_quality_score"), minimum=12, ratio=0.18 * expansion_factor, maximum=30),
        theme_core_top=_top_symbols_by_metric(metric_pairs_from_selection("theme_core_score"), minimum=14, ratio=0.20 * expansion_factor, maximum=36),
    )


def _has_non_hot_front_row_strength(selection, snapshot, relative_profile: _RelativeStrengthProfile | None = None) -> bool:
    if selection is None or snapshot is None:
        return False
    if selection.hot_rank <= 80:
        return False
    symbol = str(getattr(snapshot, "symbol", "") or "")
    relative_hit = False
    if relative_profile is not None and symbol:
        relative_hit = (
            symbol in relative_profile.amount_2m_top
            and symbol in relative_profile.open_undertake_top
            and symbol in relative_profile.execution_quality_top
        )
        if (
            not relative_hit
            and snapshot.leader_rank_in_theme <= 3
            and symbol in relative_profile.amount_2m_top
            and symbol in relative_profile.shape_quality_top
            and symbol in relative_profile.turnover_quality_top
        ):
            relative_hit = True
        if (
            not relative_hit
            and symbol in relative_profile.amount_ratio_2m_top
            and symbol in relative_profile.shape_quality_top
            and selection.open_follow_state in {"confirmed", "repair_strength"}
        ):
            relative_hit = True
        if (
            not relative_hit
            and selection.is_front_row
            and symbol in relative_profile.theme_core_top
            and symbol in relative_profile.execution_quality_top
            and symbol in relative_profile.shape_quality_top
        ):
            relative_hit = True
    if relative_hit:
        return True
    if (
        selection.is_front_row
        and snapshot.amount_2m >= 28_000_000
        and selection.open_undertake_score >= 5.0
        and selection.execution_quality_score >= 5.4
    ):
        return True
    if (
        snapshot.leader_rank_in_theme <= 3
        and snapshot.amount_2m >= 35_000_000
        and selection.open_undertake_score >= 5.2
        and selection.execution_quality_score >= 5.6
    ):
        return True
    if (
        snapshot.auction_amount > 0
        and snapshot.amount_2m >= snapshot.auction_amount * 1.3
        and selection.shape_quality_score >= 6.0
        and selection.open_follow_state in {"confirmed", "repair_strength"}
    ):
        return True
    return False


def _passes_shape_quality_gate(selection, snapshot, relative_profile: _RelativeStrengthProfile | None = None) -> bool:
    if selection is None:
        return True
    strong_non_hot_signal = _has_non_hot_front_row_strength(selection, snapshot, relative_profile)
    if not selection.is_active_pool and selection.theme_core_score < 6.5 and not strong_non_hot_signal:
        return False
    if selection.kline_pattern in {"high_open_then_weak", "volume_up_price_flat"}:
        return False
    if selection.open_follow_state == "faded" and selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
        return False
    if (
        not selection.is_true_leader
        and selection.auction_open_bucket == "near_limit_open"
        and selection.open_follow_state != "confirmed"
    ):
        return False
    if selection.structure_score < 4.0 and not selection.is_true_leader:
        return False
    if not selection.is_true_leader and selection.shape_quality_score < 5.5 and selection.execution_quality_score < 5.2:
        return False
    if not selection.is_true_leader and selection.open_undertake_score < 4.8 and selection.execution_quality_score < 5.4:
        return False
    if not selection.is_front_row and not selection.is_true_leader:
        return False
    if (
        not selection.is_true_leader
        and not selection.is_active_pool
        and selection.theme_core_score < 7.0
        and not strong_non_hot_signal
    ):
        return False
    if (
        not selection.is_true_leader
        and selection.is_front_row
        and selection.activity_score < 6.0
        and selection.theme_core_score < 6.5
        and selection.structure_score < 6.0
        and not strong_non_hot_signal
    ):
        return False
    return True


def _passes_heat_and_board_gate(selection, snapshot, relative_profile: _RelativeStrengthProfile | None = None) -> bool:
    if selection is None:
        return True
    strong_non_hot_signal = _has_non_hot_front_row_strength(selection, snapshot, relative_profile)
    if (
        not selection.is_true_leader
        and selection.auction_open_bucket == "overheat_high_open"
        and selection.open_follow_state == "weak_follow"
        and selection.open_undertake_score < 5.8
    ):
        return False
    if (
        not selection.is_true_leader
        and selection.hot_rank > 120
        and selection.turnover_quality_score < 5.0
        and selection.shape_quality_score < 6.0
        and not strong_non_hot_signal
    ):
        return False
    if (
        snapshot is not None
        and snapshot.lb_days >= 1
        and not selection.is_true_leader
        and selection.hot_rank > 100
        and selection.heat_flow_score < 5.0
        and selection.open_undertake_score < 5.6
        and not strong_non_hot_signal
    ):
        return False
    if (
        snapshot is not None
        and snapshot.lb_days >= 1
        and not selection.is_true_leader
        and snapshot.leader_rank_in_theme > 3
        and snapshot.auction_amount < 20_000_000
        and snapshot.amount_2m < 25_000_000
        and selection.execution_quality_score < 6.0
        and not strong_non_hot_signal
    ):
        return False
    return True


def _passes_action_gate(
    bundle: ContextStrategyBundle,
    decision,
    selection,
    snapshot,
    min_confidence: int,
) -> bool:
    if selection is not None and decision.action in {"dragon_early_board", "early_boarding_candidate"}:
        if selection.timing_score < 4.5 and not selection.is_true_leader:
            return False
    if decision.confidence < min_confidence:
        return False
    if decision.action in ("avoid_after_failed_promotion", "do_not_chase"):
        return False
    if decision.action == "observe_only":
        validation = _opening_validation_for_selection(bundle, selection, snapshot)
        if (
            validation is not None
            and selection is not None
            and str(getattr(validation, "validation_state", "") or "") == "confirmed"
            and str(getattr(validation, "tradable_level", "") or "") in {"attack", "probe"}
        ):
            if selection.is_true_leader:
                return True
            return bool(
                selection.is_front_row
                and selection.open_follow_state not in {"weak_follow", "faded"}
                and selection.open_undertake_score >= 5.8
                and selection.execution_quality_score >= 5.8
            )
        return False
    return True


def _passes_watch_action_gate(decision, selection, min_confidence: int) -> bool:
    if decision.confidence < min_confidence:
        return False
    if decision.action in ("avoid_after_failed_promotion", "do_not_chase"):
        return False
    if selection is None:
        return False
    if decision.action == "observe_only":
        if selection.is_true_leader:
            return True
        if selection.is_front_row and (
            selection.theme_core_score >= 6.6
            or selection.execution_quality_score >= 6.0
            or selection.open_undertake_score >= 5.8
            or selection.activity_score >= 6.8
        ):
            return True
        return False
    return True


def _passes_watch_trade_conclusion_gate(selection, snapshot, theme_context) -> bool:
    if selection is None:
        return False
    if selection.open_confirm_state == "falsified" and not selection.is_true_leader:
        return False
    if selection.theme_fakeout_level == "extreme" and not selection.is_true_leader:
        return False
    if selection.theme_tradable:
        return _passes_trade_conclusion_gate(selection, snapshot, theme_context)
    return bool(
        selection.is_true_leader
        or (
            selection.is_front_row
            and selection.theme_core_score >= 6.8
            and selection.execution_quality_score >= 5.8
            and selection.open_undertake_score >= 5.6
        )
    )


def _passes_watch_shape_quality_gate(selection, snapshot, relative_profile: _RelativeStrengthProfile | None = None) -> bool:
    if selection is None:
        return False
    if _passes_shape_quality_gate(selection, snapshot, relative_profile):
        return True
    strong_non_hot_signal = _has_non_hot_front_row_strength(selection, snapshot, relative_profile)
    if selection.kline_pattern in {"high_open_then_weak", "volume_up_price_flat"}:
        return False
    if selection.open_follow_state == "faded" and selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
        return False
    return bool(
        (selection.is_true_leader or selection.is_front_row or strong_non_hot_signal)
        and selection.execution_quality_score >= 5.4
        and selection.open_undertake_score >= 5.0
    )


def _passes_watch_heat_and_board_gate(selection, snapshot, relative_profile: _RelativeStrengthProfile | None = None) -> bool:
    if selection is None:
        return False
    if _passes_heat_and_board_gate(selection, snapshot, relative_profile):
        return True
    strong_non_hot_signal = _has_non_hot_front_row_strength(selection, snapshot, relative_profile)
    return bool(
        (selection.is_true_leader or selection.is_front_row or strong_non_hot_signal)
        and selection.execution_quality_score >= 5.4
        and selection.open_undertake_score >= 5.0
        and (selection.hot_rank <= 160 or strong_non_hot_signal)
    )


def _opening_validation_for_selection(
    bundle: ContextStrategyBundle,
    selection: StockSelectionContext | None,
    snapshot=None,
):
    return match_opening_validation(
        getattr(bundle.context, "opening_validation_bundle", None),
        snapshot=snapshot,
        selection=selection,
    )


def _upgrade_decision_action_by_opening_validation(
    bundle: ContextStrategyBundle,
    decision: AuctionLadderDecision,
    selection: StockSelectionContext | None,
    snapshot,
) -> AuctionLadderDecision:
    if selection is None or snapshot is None or decision.action != "observe_only":
        return decision
    validation = _opening_validation_for_selection(bundle, selection, snapshot)
    if validation is None:
        return decision
    if str(getattr(validation, "validation_state", "") or "") != "confirmed":
        return decision
    if str(getattr(validation, "tradable_level", "") or "") not in {"attack", "probe"}:
        return decision
    if selection.is_true_leader:
        return replace(
            decision,
            action="leader_watch",
            setup_id="opening_validation_leader_watch",
            confidence=max(int(getattr(decision, "confidence", 0) or 0), 72),
            kelly_position_pct=max(float(getattr(decision, "kelly_position_pct", 0.02) or 0.02), 0.03),
            reasons=tuple(decision.reasons) + ("opening validation confirmed the theme leader",),
        )
    if (
        selection.is_front_row
        and selection.open_follow_state in {"confirmed", "repair_strength"}
        and selection.open_undertake_score >= 5.8
        and selection.execution_quality_score >= 5.8
    ):
        return replace(
            decision,
            action="confirm_then_go",
            setup_id="opening_validation_confirm_then_go",
            confidence=max(int(getattr(decision, "confidence", 0) or 0), 68),
            kelly_position_pct=max(float(getattr(decision, "kelly_position_pct", 0.02) or 0.02), 0.04),
            reasons=tuple(decision.reasons) + ("opening validation confirmed a front-row continuation candidate",),
        )
    if (
        selection.is_front_row
        and selection.open_follow_state not in {"weak_follow", "faded"}
        and selection.open_undertake_score >= 5.6
        and selection.execution_quality_score >= 5.6
    ):
        return replace(
            decision,
            action="front_row_watch",
            setup_id="opening_validation_front_row_watch",
            confidence=max(int(getattr(decision, "confidence", 0) or 0), 64),
            kelly_position_pct=max(float(getattr(decision, "kelly_position_pct", 0.02) or 0.02), 0.03),
            reasons=tuple(decision.reasons) + ("opening validation kept the front row tradable after the open",),
        )
    return decision


def _upgrade_bundle_decisions_by_opening_validation(
    bundle: ContextStrategyBundle,
) -> ContextStrategyBundle:
    stock_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
    snapshot_map = {item.symbol: item for item in bundle.context.stock_snapshots}
    upgraded = tuple(
        _upgrade_decision_action_by_opening_validation(
            bundle,
            decision,
            stock_context_map.get(decision.symbol),
            snapshot_map.get(decision.symbol),
        )
        for decision in bundle.decisions
    )
    if upgraded == bundle.decisions:
        return bundle
    return replace(bundle, decisions=upgraded)


def _opening_validation_trade_conclusion_override(
    bundle: ContextStrategyBundle,
    selection: StockSelectionContext | None,
    snapshot,
) -> bool:
    validation = _opening_validation_for_selection(bundle, selection, snapshot)
    if validation is None or selection is None or snapshot is None:
        return False
    state = str(getattr(validation, "validation_state", "") or "")
    tradable_level = str(getattr(validation, "tradable_level", "") or "")
    if state != "confirmed" or tradable_level not in {"attack", "probe"}:
        return False
    if selection.is_true_leader:
        return True
    if not selection.is_front_row:
        return False
    if selection.open_follow_state in {"weak_follow", "faded"}:
        return False
    if float(getattr(snapshot, "amount_2m", 0.0) or 0.0) <= 0.0:
        return False
    return bool(
        selection.open_undertake_score >= 5.6
        and selection.execution_quality_score >= 5.6
    )


def _passes_opening_validation_trade_gate(
    bundle: ContextStrategyBundle,
    decision: AuctionLadderDecision,
    selection: StockSelectionContext | None,
    snapshot=None,
) -> bool:
    validation = _opening_validation_for_selection(bundle, selection, snapshot)
    if validation is None or selection is None:
        return True
    state = str(getattr(validation, "validation_state", "") or "")
    tradable_level = str(getattr(validation, "tradable_level", "") or "")
    if state == "confirmed":
        if tradable_level == "attack":
            return True
        if tradable_level == "probe":
            return bool(
                selection.is_true_leader
                or selection.is_front_row
                or decision.action in {"small_probe_only", "leader_watch", "front_row_watch", "confirm_then_go"}
            )
        return selection.is_true_leader
    if state == "watch":
        return bool(
            selection.is_true_leader
            or (
                selection.is_front_row
                and selection.open_undertake_score >= 5.8
                and selection.execution_quality_score >= 5.8
            )
        )
    if state == "falsified" or tradable_level == "avoid":
        return selection.is_true_leader
    return True


def _passes_opening_validation_watch_gate(
    bundle: ContextStrategyBundle,
    selection: StockSelectionContext | None,
    snapshot=None,
) -> bool:
    validation = _opening_validation_for_selection(bundle, selection, snapshot)
    if validation is None or selection is None:
        return True
    state = str(getattr(validation, "validation_state", "") or "")
    tradable_level = str(getattr(validation, "tradable_level", "") or "")
    if state in {"confirmed", "watch"}:
        return True
    if state == "falsified" or tradable_level == "avoid":
        return selection.is_true_leader
    return True


def filter_trade_candidates(
    bundle: ContextStrategyBundle,
    *,
    min_confidence: int = 60,
) -> tuple[AuctionLadderDecision, ...]:
    candidates = []
    relative_profile = _build_relative_strength_profile(bundle)
    stock_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
    snapshot_map = {item.symbol: item for item in bundle.context.stock_snapshots}
    for decision in bundle.decisions:
        selection = stock_context_map.get(decision.symbol)
        snapshot = snapshot_map.get(decision.symbol)
        theme_context_map = getattr(bundle, "theme_context_map", None)
        theme_context = theme_context_map.get(selection.plate_name) if isinstance(theme_context_map, dict) and selection is not None else None
        if (
            not _passes_trade_conclusion_gate(selection, snapshot, theme_context)
            and not _opening_validation_trade_conclusion_override(bundle, selection, snapshot)
        ):
            continue
        if not _passes_opening_validation_trade_gate(bundle, decision, selection, snapshot):
            continue
        if not _passes_shape_quality_gate(selection, snapshot, relative_profile):
            continue
        if not _passes_heat_and_board_gate(selection, snapshot, relative_profile):
            continue
        if not _passes_action_gate(bundle, decision, selection, snapshot, min_confidence):
            continue
        candidates.append(decision)
    return tuple(candidates)


def filter_watch_candidates(
    bundle: ContextStrategyBundle,
    *,
    min_confidence: int = 60,
) -> tuple[AuctionLadderDecision, ...]:
    candidates = []
    relative_profile = _build_relative_strength_profile(bundle)
    stock_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
    snapshot_map = {item.symbol: item for item in bundle.context.stock_snapshots}
    for decision in bundle.decisions:
        selection = stock_context_map.get(decision.symbol)
        snapshot = snapshot_map.get(decision.symbol)
        theme_context_map = getattr(bundle, "theme_context_map", None)
        theme_context = theme_context_map.get(selection.plate_name) if isinstance(theme_context_map, dict) and selection is not None else None
        if not _passes_watch_trade_conclusion_gate(selection, snapshot, theme_context):
            continue
        if not _passes_opening_validation_watch_gate(bundle, selection, snapshot):
            continue
        if not _passes_watch_shape_quality_gate(selection, snapshot, relative_profile):
            continue
        if not _passes_watch_heat_and_board_gate(selection, snapshot, relative_profile):
            continue
        if not _passes_watch_action_gate(decision, selection, min_confidence):
            continue
        candidates.append(decision)
    return tuple(candidates)
