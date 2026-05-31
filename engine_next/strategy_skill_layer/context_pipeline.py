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
from engine_next.domain.decision_models import DecisionBundle
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.strategy_skill_layer.hypothesis_engine import build_hypothesis_decision_bundle
from engine_next.strategy_skill_layer.local_decision_layer import build_local_decision_bundle
from engine_next.strategy_skill_layer.opening_validation_hub import match_opening_validation
from engine_next.strategy_skill_layer.relative_amount import enrich_snapshot_amount_rank_pcts
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


@dataclass(frozen=True)
class ContextStrategyBundle:
    context: IntradayContext
    profiles: tuple[StockProfileAssessment, ...]
    theme_context_map: dict[str, ThemeSelectionContext]
    stock_selection_contexts: tuple[StockSelectionContext, ...]
    decisions: tuple[AuctionLadderDecision, ...]
    focus_symbols: tuple[str, ...]
    decision_bundle: DecisionBundle | None = None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class _CachedStockSelectionEntry:
    signature: tuple[object, ...]
    context: StockSelectionContext


_STOCK_SELECTION_CACHE: dict[tuple[str, str], _CachedStockSelectionEntry] = {}
_THEME_CONCLUSION_CACHE_TTL_SECONDS = 300


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


def _local_candidate_action_priority(action: str) -> int:
    priority_map = {
        "probe": 4,
        "shadow_can_rank": 3,
        "watch": 2,
        "avoid_chase": 1,
        "avoid": 0,
        "disabled": 0,
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
        for symbol in theme_symbols:
            snapshot = snapshot_map.get(symbol)
            if snapshot is None:
                continue
            auction_amount = float(getattr(snapshot, "auction_amount", 0.0) or 0.0)
            amount_2m = float(getattr(snapshot, "amount_2m", 0.0) or 0.0)
            amount_pairs.append((symbol, amount_2m))
            amount_ratio_pairs.append((symbol, (amount_2m / auction_amount) if auction_amount > 0 else 0.0))
        amount_rank_pct = _rank_pct_desc(amount_pairs)
        amount_ratio_rank_pct = _rank_pct_desc(amount_ratio_pairs)
        enriched.append(
            replace(
                selection,
                stock_amount_2m_rank_in_theme_pct=float(amount_rank_pct.get(selection.symbol, 1.0)),
                stock_amount_ratio_2m_rank_in_theme_pct=float(amount_ratio_rank_pct.get(selection.symbol, 1.0)),
                notes=tuple(
                    list(selection.notes)
                    + [
                        f"amount_2m_rank_pct={float(amount_rank_pct.get(selection.symbol, 1.0)):.3f}",
                        f"amount_ratio_2m_rank_pct={float(amount_ratio_rank_pct.get(selection.symbol, 1.0)):.3f}",
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
    enriched_snapshots = enrich_snapshot_amount_rank_pcts(context.stock_snapshots)
    if enriched_snapshots is not context.stock_snapshots:
        context = replace(context, stock_snapshots=enriched_snapshots)
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
    decision_bundle: DecisionBundle | None = None
    decision_notes: tuple[str, ...] = ()
    try:
        decision_bundle = build_local_decision_bundle(
            context,
            selection_contexts=stock_selection_contexts,
        )
        decision_bundle = build_hypothesis_decision_bundle(context, decision_bundle)
        playbook_matrix = decision_bundle.playbook_control_matrix
        decision_notes = (
            tuple(f"local_decision_{note}" for note in decision_bundle.notes)
            + (
                f"playbook_final_candidates={len(decision_bundle.final_candidates)}",
                f"playbook_global_script={decision_bundle.global_decision.market_script if decision_bundle.global_decision is not None else 'missing'}",
                f"playbook_main_theme={decision_bundle.global_decision.main_attack_theme if decision_bundle.global_decision is not None else '-'}",
                f"playbook_active={','.join(playbook_matrix.active_playbooks) if playbook_matrix is not None and playbook_matrix.active_playbooks else '-'}",
                f"playbook_blocked={','.join(playbook_matrix.blocked_playbooks) if playbook_matrix is not None and playbook_matrix.blocked_playbooks else '-'}",
            )
        )
    except Exception as exc:
        decision_notes = (f"local_decision_error={type(exc).__name__}",)
    final_candidate_rank = {
        item.symbol: item
        for item in (decision_bundle.final_candidates if decision_bundle is not None else ())
    }
    candidate_pool = tuple(candidates or watch_candidates)
    if candidate_pool:
        ranked = sorted(
            candidate_pool,
            key=lambda decision: (
                decision.symbol in final_candidate_rank,
                -int(final_candidate_rank[decision.symbol].priority_rank) if decision.symbol in final_candidate_rank else -999,
                _local_candidate_action_priority(final_candidate_rank[decision.symbol].action) if decision.symbol in final_candidate_rank else 0,
                1 if decision.symbol in final_candidate_rank and final_candidate_rank[decision.symbol].risk_level != "high" else 0,
                str(getattr(decision.profile.leader_tier, "value", decision.profile.leader_tier)) == "absolute",
                _decision_action_priority(decision.action),
                str(getattr(decision.profile.trade_window, "value", decision.profile.trade_window)) == "early_boarding",
                str(getattr(decision.profile.archetype, "value", decision.profile.archetype)) == "dragon_leader",
                -decision.kelly_position_pct,
            ),
            reverse=True,
        )
        ranked_decisions = tuple(ranked)
    elif decision_bundle is not None and decision_bundle.final_candidates:
        ranked_decisions = tuple(candidates)
    else:
        ranked_decisions = ()
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
        "legacy_candidate_fallback=removed",
    ) + decision_notes
    bundle = ContextStrategyBundle(
        context=context,
        profiles=profiles,
        theme_context_map=resolved_theme_context_map,
        stock_selection_contexts=stock_selection_contexts,
        decisions=ranked_decisions,
        focus_symbols=focus_symbols,
        decision_bundle=decision_bundle,
        notes=notes,
    )
    return bundle


def _passes_watch_trade_conclusion_gate(selection, snapshot, theme_context) -> bool:
    if selection is None:
        return False
    if selection.open_confirm_state == "falsified" and not selection.is_true_leader:
        return False
    if selection.theme_fakeout_level == "extreme" and not selection.is_true_leader:
        return False
    if selection.theme_x_score >= 5.6 and not selection.is_true_leader:
        return False
    if selection.theme_tradable and theme_context is not None:
        if getattr(theme_context, "trade_conclusion", "") == "leader_only_alive" and not selection.is_true_leader:
            return bool(
                selection.is_front_row
                and selection.open_follow_state not in {"weak_follow", "faded"}
                and float(getattr(snapshot, "amount_2m", 0.0) or 0.0) >= 15_000_000
            )
    return bool(
        selection.is_true_leader
        or (
            selection.is_front_row
            and selection.open_follow_state not in {"weak_follow", "faded"}
        )
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
                and selection.open_follow_state not in {"weak_follow", "faded"}
                and float(getattr(snapshot, "amount_2m", 0.0) or 0.0) >= 15_000_000
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


def _passes_hard_trade_veto_only(
    bundle: ContextStrategyBundle,
    decision: AuctionLadderDecision,
    selection: StockSelectionContext | None,
    snapshot,
) -> bool:
    if decision.action in {"avoid_after_failed_promotion", "do_not_chase"}:
        return False
    if not _passes_opening_validation_trade_gate(bundle, decision, selection, snapshot):
        return False
    return True


def _final_candidate_is_tradeable_probe(final_candidate) -> bool:
    if final_candidate is None:
        return False
    if str(getattr(final_candidate, "action", "") or "") != "probe":
        return False
    if str(getattr(final_candidate, "risk_level", "") or "") == "high":
        return False
    risk_tags = set(getattr(getattr(final_candidate, "trace", None), "risk_tags", ()) or ())
    if "relative_risk_theme" in risk_tags:
        return False
    return True


def _final_candidate_is_watchable(final_candidate) -> bool:
    if final_candidate is None:
        return False
    if str(getattr(final_candidate, "risk_level", "") or "") == "high":
        return False
    return str(getattr(final_candidate, "action", "") or "") in {"probe", "watch"}


def _append_playbook_final_trade_candidates(
    bundle: ContextStrategyBundle,
    candidates: list[AuctionLadderDecision],
) -> list[AuctionLadderDecision]:
    decision_bundle = bundle.decision_bundle
    if decision_bundle is None or not decision_bundle.final_candidates:
        return candidates
    decision_map = {item.symbol: item for item in bundle.decisions}
    stock_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
    snapshot_map = {item.symbol: item for item in bundle.context.stock_snapshots}
    existing = {item.symbol for item in candidates}
    for final_candidate in sorted(
        decision_bundle.final_candidates,
        key=lambda item: (
            item.action == "probe",
            item.risk_level != "high",
            -int(item.priority_rank),
        ),
        reverse=True,
    ):
        if final_candidate.symbol in existing:
            continue
        if not _final_candidate_is_tradeable_probe(final_candidate):
            continue
        decision = decision_map.get(final_candidate.symbol)
        selection = stock_context_map.get(final_candidate.symbol)
        snapshot = snapshot_map.get(final_candidate.symbol)
        if decision is None or selection is None or snapshot is None:
            continue
        if not _passes_hard_trade_veto_only(bundle, decision, selection, snapshot):
            continue
        if selection.kline_pattern in {"high_open_then_weak", "volume_up_price_flat", "explosive_failed_board"}:
            continue
        if selection.open_follow_state == "faded":
            continue
        if selection.daily_height_bucket == "high" and not selection.is_true_leader:
            continue
        candidates.append(decision)
        existing.add(decision.symbol)
    return candidates


def _append_playbook_final_watch_candidates(
    bundle: ContextStrategyBundle,
    candidates: list[AuctionLadderDecision],
) -> list[AuctionLadderDecision]:
    decision_bundle = bundle.decision_bundle
    if decision_bundle is None or not decision_bundle.final_candidates:
        return candidates
    decision_map = {item.symbol: item for item in bundle.decisions}
    stock_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
    snapshot_map = {item.symbol: item for item in bundle.context.stock_snapshots}
    theme_context_map = getattr(bundle, "theme_context_map", None)
    existing = {item.symbol for item in candidates}
    for final_candidate in sorted(
        decision_bundle.final_candidates,
        key=lambda item: (
            item.action in {"probe", "watch"},
            item.risk_level != "high",
            -int(item.priority_rank),
        ),
        reverse=True,
    ):
        if final_candidate.symbol in existing:
            continue
        if not _final_candidate_is_watchable(final_candidate):
            continue
        decision = decision_map.get(final_candidate.symbol)
        selection = stock_context_map.get(final_candidate.symbol)
        snapshot = snapshot_map.get(final_candidate.symbol)
        if decision is None or selection is None or snapshot is None:
            continue
        theme_context = theme_context_map.get(selection.plate_name) if isinstance(theme_context_map, dict) else None
        if not _passes_opening_validation_watch_gate(bundle, selection, snapshot):
            continue
        if final_candidate.action == "watch" and theme_context is not None and not _passes_watch_trade_conclusion_gate(selection, snapshot, theme_context):
            continue
        candidates.append(decision)
        existing.add(decision.symbol)
    return candidates


def filter_trade_candidates(
    bundle: ContextStrategyBundle,
    *,
    min_confidence: int = 60,
) -> tuple[AuctionLadderDecision, ...]:
    candidates = _append_playbook_final_trade_candidates(bundle, [])
    return tuple(candidates)


def filter_watch_candidates(
    bundle: ContextStrategyBundle,
    *,
    min_confidence: int = 60,
) -> tuple[AuctionLadderDecision, ...]:
    candidates = _append_playbook_final_watch_candidates(bundle, [])
    if candidates:
        return tuple(candidates)
    return ()
