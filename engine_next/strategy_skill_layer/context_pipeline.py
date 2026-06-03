from __future__ import annotations

from dataclasses import dataclass, replace
import logging
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

logger = logging.getLogger(__name__)


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
        logger.exception(
            "context pipeline decision bundle failed | trade_date=%s | phase=%s | selected=%s",
            getattr(context, "trade_date", ""),
            getattr(getattr(context, "phase", None), "value", getattr(context, "phase", "")),
            len(selected_snapshots),
        )
        decision_notes = (f"local_decision_error={type(exc).__name__}",)
    final_candidate_rank = {
        item.symbol: item
        for item in (decision_bundle.final_candidates if decision_bundle is not None else ())
    }
    profile_map = {profile.symbol: profile for profile in profiles}
    seeded_decisions: list[AuctionLadderDecision] = []
    missing_profile_count = 0
    for final_candidate in sorted(
        tuple(final_candidate_rank.values()),
        key=lambda item: (
            _local_candidate_action_priority(str(getattr(item, "action", "") or "")),
            int(getattr(item, "priority_rank", 999) or 999) * -1,
        ),
        reverse=True,
    ):
        symbol = str(getattr(final_candidate, "symbol", "") or "")
        if not symbol:
            continue
        profile = profile_map.get(symbol)
        if profile is None:
            missing_profile_count += 1
            continue
        raw_action = str(getattr(final_candidate, "action", "") or "")
        risk_level = str(getattr(final_candidate, "risk_level", "") or "")
        if raw_action in {"avoid", "avoid_chase", "disabled"} or risk_level == "high":
            continue
        mapped_action = "small_probe_only" if raw_action == "probe" else "observe_only"
        priority_rank = int(getattr(final_candidate, "priority_rank", 999) or 999)
        confidence = max(52, min(95, 100 - priority_rank))
        if mapped_action == "observe_only":
            confidence = min(confidence, 65)
        seeded_decisions.append(
            AuctionLadderDecision(
                symbol=symbol,
                setup_id=f"playbook_{str(getattr(final_candidate, 'playbook', '') or 'watch')}",
                action=mapped_action,
                confidence=confidence,
                kelly_position_pct=0.10 if mapped_action == "small_probe_only" else 0.0,
                risk_reward_ratio=1.6 if mapped_action == "small_probe_only" else 1.0,
                profile=profile,
                reasons=(
                    f"final_candidate={raw_action}",
                    f"path={str(getattr(final_candidate, 'path_type', '') or '-')}",
                    f"rank={priority_rank}",
                ),
            )
        )
    ranked_decisions = tuple(seeded_decisions)
    logger.info(
        "context pipeline seed | selected=%s | final_candidates=%s | seeded=%s | missing_profile=%s",
        len(selected_snapshots),
        len(final_candidate_rank),
        len(ranked_decisions),
        missing_profile_count,
    )
    focus_symbols = tuple(decision.symbol for decision in ranked_decisions[:10])
    summary = context.market_summary

    context_notes = tuple(str(note) for note in tuple(getattr(context, "notes", ()) or ()) if str(note))
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
    ) + context_notes + decision_notes
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
