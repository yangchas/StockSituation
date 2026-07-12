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
from engine_next.domain.decision_models import DecisionBundle, MarketTranslationSummary
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
_FUNNEL_TRACE_LOG_LIMIT = 20


def build_empty_funnel_summary() -> dict[str, object]:
    return {
        "raw": 0,
        "shape": 0,
        "local": 0,
        "global": 0,
        "executable": 0,
        "profit_center": 0,
        "backup_watch": 0,
        "blocked": 0,
        "invalidated": 0,
        "blocked_by_theme": 0,
        "blocked_by_buy_point": 0,
        "blocked_by_data": 0,
        "controller_downgrade": 0,
        "why_no_profit_center": "unknown",
    }


def _derive_why_no_profit_center(summary: dict[str, object]) -> str:
    def _num(key: str) -> int:
        try:
            return int(float(summary.get(key, 0) or 0))
        except (TypeError, ValueError):
            return 0

    if _num("raw") == 0:
        return "no_candidate"
    if _num("local") == 0:
        return "no_local_signal"
    if _num("blocked_by_data") > 0 and _num("executable") == 0:
        return "quote_stale_block_live_trade"
    if _num("blocked_by_theme") > 0 and _num("global") == 0:
        return "theme_not_tradeable"
    if _num("blocked_by_buy_point") > 0 and _num("executable") == 0:
        return "buy_point_not_confirmed"
    if _num("controller_downgrade") > 0 and _num("profit_center") == 0:
        return "controller_downgraded"
    if _num("profit_center") == 0:
        return "unknown"
    return ""


def _finalize_funnel_summary(
    decision_bundle: DecisionBundle | None,
    *,
    context: IntradayContext,
    raw_count: int,
    shape_count: int,
) -> DecisionBundle | None:
    if decision_bundle is None:
        return None
    summary = build_empty_funnel_summary()
    summary.update(dict(getattr(decision_bundle, "funnel_summary", {}) or {}))
    summary["raw"] = int(raw_count)
    summary["shape"] = int(shape_count)
    if int(summary.get("global", 0) or 0) <= 0:
        summary["global"] = len(tuple(getattr(decision_bundle, "final_candidates", ()) or ()))
    if int(summary.get("profit_center", 0) or 0) <= 0:
        slice_obj = getattr(decision_bundle, "playbook_candidate_slice", None)
        if slice_obj is not None:
            summary["profit_center"] = len(tuple(getattr(slice_obj, "primary", ()) or ()))
            summary["backup_watch"] = len(tuple(getattr(slice_obj, "watch", ()) or ())) + len(tuple(getattr(slice_obj, "inactive", ()) or ()))
            summary["blocked"] = len(tuple(getattr(slice_obj, "blocked", ()) or ()))
            summary["executable"] = len(tuple(getattr(slice_obj, "primary", ()) or ()))
    summary["why_no_profit_center"] = _derive_why_no_profit_center(summary) or ""
    updated_bundle = replace(
        decision_bundle,
        funnel_summary=summary,
        funnel_traces=_enrich_funnel_trace_names(
            context,
            traces=tuple(getattr(decision_bundle, "funnel_traces", ()) or ()),
        ),
    )
    return replace(
        updated_bundle,
        market_translation_summary=_build_market_translation_summary(updated_bundle),
    )


def _format_funnel_summary(summary: dict[str, object]) -> str:
    return (
        f"raw={summary.get('raw', 0)} | shape={summary.get('shape', 0)} | "
        f"local={summary.get('local', 0)} | global={summary.get('global', 0)} | "
        f"executable={summary.get('executable', 0)} | profit_center={summary.get('profit_center', 0)} | "
        f"backup={summary.get('backup_watch', 0)} | blocked={summary.get('blocked', 0)} | "
        f"invalidated={summary.get('invalidated', 0)} | blocked_theme={summary.get('blocked_by_theme', 0)} | "
        f"blocked_buy_point={summary.get('blocked_by_buy_point', 0)} | blocked_data={summary.get('blocked_by_data', 0)} | "
        f"controller_downgrade={summary.get('controller_downgrade', 0)} | "
        f"why_no_profit_center={summary.get('why_no_profit_center', 'unknown') or 'none'}"
    )


def _summary_int(summary: dict[str, object], key: str, default: int = 0) -> int:
    try:
        return int(float(summary.get(key, default) or default))
    except (TypeError, ValueError):
        return default


def _theme_name_from_signal(signal: object) -> str:
    return str(getattr(signal, "theme", "") or "").strip()


def _build_market_translation_summary(decision_bundle: DecisionBundle | None) -> MarketTranslationSummary | None:
    """Translate existing migration/funnel facts into operator-facing market language.

    This summary is explanatory only. It must not affect candidate buckets, ranking, actions, or caps.
    """

    if decision_bundle is None:
        return None
    signals = tuple(getattr(decision_bundle, "market_migration_signals", ()) or ())
    summary = dict(getattr(decision_bundle, "funnel_summary", {}) or {})
    if not signals and not summary:
        return None

    executable = _summary_int(summary, "executable")
    profit_center = _summary_int(summary, "profit_center")
    backup = _summary_int(summary, "backup_watch")
    blocked_buy_point = _summary_int(summary, "blocked_by_buy_point")
    local = _summary_int(summary, "local")

    watch_themes: list[str] = []
    cashout_themes: list[str] = []
    risk_themes: list[str] = []
    evidence: list[str] = []
    validation_by_theme = {
        str(getattr(item, "theme", "") or ""): str(getattr(item, "validation_state", "") or "")
        for item in tuple(getattr(decision_bundle, "mainline_validation_states", ()) or ())
        if str(getattr(item, "theme", "") or "")
    }

    for signal in signals[:8]:
        theme = _theme_name_from_signal(signal)
        if not theme:
            continue
        money_state = str(getattr(signal, "money_state", "") or "")
        validation_state = validation_by_theme.get(theme, "")
        rank = int(getattr(signal, "rank", 999) or 999)
        change_pct = float(getattr(signal, "change_pct", 0.0) or 0.0)
        net_inflow_yi = float(getattr(signal, "net_inflow_yi", 0.0) or 0.0)
        evidence_axes = tuple(getattr(signal, "evidence_axes", ()) or ())

        hot_but_pressure = (
            rank <= 5
            and (net_inflow_yi <= -20.0 or change_pct <= -1.0)
            and money_state in {"money_in", "money_rotation_in", "attention_only", "money_out", "fake_hot"}
        )
        if hot_but_pressure:
            cashout_themes.append(theme)
            evidence.append(f"{theme}=热度{rank}/流向{net_inflow_yi:.1f}亿/涨跌{change_pct:.1f}%")
            continue
        if money_state in {"money_out", "fake_hot"}:
            risk_themes.append(theme)
            evidence.append(f"{theme}={money_state}")
            continue
        if money_state == "style_risk_line":
            risk_themes.append(theme)
            evidence.append(f"{theme}=风格风险线")
            continue
        if money_state in {"money_in", "money_rotation_in"}:
            if validation_state in {"auction_candidate", "open_watch"}:
                watch_themes.append(f"{theme}(待验证)")
            else:
                watch_themes.append(theme)
            if len(evidence_axes) >= 3:
                evidence.append(f"{theme}={money_state}/{validation_state}")
        elif money_state == "attention_only":
            risk_themes.append(f"{theme}(仅热度)")

    has_buy_point_block = blocked_buy_point >= max(20, local // 2 if local else 20)
    if profit_center > 0 or executable > 0:
        market_mode = "开盘验证候选"
    elif cashout_themes and watch_themes and has_buy_point_block:
        market_mode = "低位试错/热板兑现"
    elif cashout_themes and has_buy_point_block:
        market_mode = "热板兑现观察"
    elif watch_themes and has_buy_point_block:
        market_mode = "低位试错"
    elif risk_themes and not watch_themes:
        market_mode = "防守观察"
    else:
        market_mode = "轮动观察"

    if has_buy_point_block and watch_themes:
        profit_style = "有方向无确认买点"
    elif cashout_themes:
        profit_style = "热板承压兑现"
    elif executable > 0:
        profit_style = "前排确认候选"
    elif backup > 0:
        profit_style = "等待前排换手"
    else:
        profit_style = "无明确赚钱方式"

    mainline_text = "-"
    if watch_themes:
        mainline_text = " / ".join(dict.fromkeys(watch_themes[:3]))
    elif cashout_themes:
        mainline_text = f"{cashout_themes[0]}承压"
    elif risk_themes:
        mainline_text = "风险优先"

    if has_buy_point_block:
        evidence.append(f"买点未确认={blocked_buy_point}")
    if executable == 0 and local > 0:
        evidence.append("有候选但无可执行买点")

    return MarketTranslationSummary(
        market_mode=market_mode,
        profit_style=profit_style,
        mainline_text=mainline_text,
        watch_themes=tuple(dict.fromkeys(watch_themes)),
        cashout_themes=tuple(dict.fromkeys(cashout_themes)),
        risk_themes=tuple(dict.fromkeys(risk_themes)),
        evidence=tuple(dict.fromkeys(evidence)),
    )


def _format_funnel_trace(trace: dict[str, object]) -> str:
    symbol = str(trace.get("symbol", "") or "unknown")
    name = str(trace.get("name", "") or "unknown")
    theme = str(trace.get("theme", "") or "unknown")
    source = ",".join(str(item) for item in tuple(trace.get("source", []) or []) if str(item)) or "unknown"
    buy_point = str(trace.get("buy_point", "") or "unknown")
    status = str(trace.get("pass_or_block", "") or "unknown")
    reason = ",".join(str(item) for item in tuple(trace.get("reason", []) or []) if str(item)) or "unknown"
    metrics = trace.get("key_metrics", {})
    if isinstance(metrics, dict):
        metric_text = ",".join(f"{key}:{value}" for key, value in list(metrics.items())[:5]) or "-"
    else:
        metric_text = "-"
    return (
        f"{symbol} | name={name} | theme={theme} | source={source} | "
        f"buy_point={buy_point} | status={status} | reason={reason} | metrics={metric_text}"
    )


def _format_migration_shadow(signal: object, validation: object | None) -> str:
    theme = str(getattr(signal, "theme", "") or "-")
    tags = ",".join(str(item) for item in tuple(getattr(signal, "money_tags", ()) or ()) if str(item)) or "-"
    axes = ",".join(str(item) for item in tuple(getattr(signal, "evidence_axes", ()) or ()) if str(item)) or "-"
    validation_state = str(getattr(validation, "validation_state", "") or "-") if validation is not None else "-"
    invalidations = (
        ",".join(str(item) for item in tuple(getattr(validation, "invalidations", ()) or ()) if str(item))
        if validation is not None
        else ""
    ) or "-"
    phase = str(getattr(validation, "phase", "") or "-") if validation is not None else "-"
    return (
        f"phase={phase} | theme={theme} | rank={getattr(signal, 'rank', 999)} | "
        f"rank_delta_prev={getattr(signal, 'rank_delta_prev', 0)} | "
        f"rank_delta_5m={getattr(signal, 'rank_delta_5m', 0)} | "
        f"rank_delta_yday={getattr(signal, 'rank_delta_yday', 0)} | "
        f"strength={float(getattr(signal, 'strength', 0.0) or 0.0):.2f} | "
        f"change_pct={float(getattr(signal, 'change_pct', 0.0) or 0.0):.3f} | "
        f"net_inflow_yi={float(getattr(signal, 'net_inflow_yi', 0.0) or 0.0):.2f} | "
        f"net_inflow_yi_delta_prev={float(getattr(signal, 'net_inflow_yi_delta_prev', 0.0) or 0.0):.2f} | "
        f"net_inflow_yi_delta_5m={float(getattr(signal, 'net_inflow_yi_delta_5m', 0.0) or 0.0):.2f} | "
        f"net_inflow_yi_delta_yday={float(getattr(signal, 'net_inflow_yi_delta_yday', 0.0) or 0.0):.2f} | "
        f"money_state={str(getattr(signal, 'money_state', '') or 'unknown')} | "
        f"money_tags={tags} | validation_state={validation_state} | evidence_axes={axes} | "
        f"source_freshness={str(getattr(signal, 'source_freshness', '') or 'unknown')} | "
        f"confidence={str(getattr(signal, 'confidence', '') or 'low')} | invalidations={invalidations}"
    )


def _format_stable_trading_plan(plan: object) -> str:
    money_to = ",".join(str(item) for item in tuple(getattr(plan, "money_to", ()) or ()) if str(item)) or "-"
    money_from = ",".join(str(item) for item in tuple(getattr(plan, "money_from", ()) or ()) if str(item)) or "-"
    risk_or_noise = ",".join(str(item) for item in tuple(getattr(plan, "risk_or_noise", ()) or ()) if str(item)) or "-"
    money_to_metrics = ";".join(str(item) for item in tuple(getattr(plan, "money_to_metrics", ()) or ()) if str(item)) or "-"
    money_from_metrics = ";".join(str(item) for item in tuple(getattr(plan, "money_from_metrics", ()) or ()) if str(item)) or "-"
    risk_or_noise_metrics = ";".join(str(item) for item in tuple(getattr(plan, "risk_or_noise_metrics", ()) or ()) if str(item)) or "-"
    tactic_reason = ",".join(str(item) for item in tuple(getattr(plan, "tactic_reason", ()) or ()) if str(item)) or "-"
    confirm = ",".join(str(item) for item in tuple(getattr(plan, "confirm_conditions", ()) or ()) if str(item)) or "-"
    invalidation = ",".join(str(item) for item in tuple(getattr(plan, "invalidation_points", ()) or ()) if str(item)) or "-"
    candidates = ",".join(str(getattr(item, "symbol", "") or "") for item in tuple(getattr(plan, "candidates", ()) or ()) if str(getattr(item, "symbol", "") or "")) or "-"
    return (
        f"phase={str(getattr(plan, 'phase', '') or 'unknown')} | "
        f"money_to={money_to} | money_to_metrics={money_to_metrics} | "
        f"money_from={money_from} | money_from_metrics={money_from_metrics} | "
        f"risk_or_noise={risk_or_noise} | risk_or_noise_metrics={risk_or_noise_metrics} | "
        f"best_tactic={str(getattr(plan, 'best_tactic', '') or 'watch_only')} | "
        f"tactic_reason={tactic_reason} | candidates={candidates} | "
        f"confirm={confirm} | invalidation={invalidation} | "
        f"why_no_candidate={str(getattr(plan, 'why_no_candidate', '') or '-')}"
    )


def _format_stable_trading_candidate(candidate: object) -> str:
    confirm = ",".join(str(item) for item in tuple(getattr(candidate, "confirm_condition", ()) or ()) if str(item)) or "-"
    invalidation = ",".join(str(item) for item in tuple(getattr(candidate, "invalidation_points", ()) or ()) if str(item)) or "-"
    evidence = ",".join(str(item) for item in tuple(getattr(candidate, "evidence_summary", ()) or ()) if str(item)) or "-"
    return (
        f"{str(getattr(candidate, 'symbol', '') or '-')} | "
        f"theme={str(getattr(candidate, 'theme_name', '') or '-')} | "
        f"bucket={str(getattr(candidate, 'source_bucket', '') or '-')} | "
        f"state={str(getattr(candidate, 'candidate_state', '') or 'watch_only')} | "
        f"strategy={str(getattr(candidate, 'strategy_id', '') or '-')} | "
        f"buy_point={str(getattr(candidate, 'buy_point', '') or 'unknown')} | "
        f"role={str(getattr(candidate, 'role', '') or 'unknown')} | "
        f"score={float(getattr(candidate, 'setup_score', 0.0) or 0.0):.2f} | "
        f"confirm={confirm} | invalidation={invalidation} | evidence={evidence}"
    )


def _format_theme_process_board(board: object) -> str:
    summary = ";".join(str(item) for item in tuple(getattr(board, "process_summary", ()) or ()) if str(item)) or "-"
    return (
        f"main={str(getattr(board, 'current_mainline', '') or '-')} | "
        f"recheck={1 if bool(getattr(board, 'recheck_required', False)) else 0} | "
        f"focus={str(getattr(board, 'execution_focus_candidate', '') or '-')} | "
        f"reason={str(getattr(board, 'recheck_reason', '') or '-')} | "
        f"rows={summary}"
    )


def _format_theme_process_row(row: object) -> str:
    candidates = ",".join(str(item) for item in tuple(getattr(row, "top_candidates", ()) or ()) if str(item)) or "-"
    axes = ",".join(str(item) for item in tuple(getattr(row, "evidence_axes", ()) or ()) if str(item)) or "-"
    invalidation = ",".join(str(item) for item in tuple(getattr(row, "invalidation_points", ()) or ())[:3] if str(item)) or "-"
    votes = ",".join(
        f"{str(getattr(vote, 'strategy_id', '') or '-')}:{int(getattr(vote, 'count', 0) or 0)}"
        for vote in tuple(getattr(row, "strategy_votes", ()) or ())[:4]
    ) or "-"
    return (
        f"theme={str(getattr(row, 'theme', '') or '-')} | "
        f"hot_rank={int(getattr(row, 'hot_rank', 999) or 999)} | "
        f"money_state={str(getattr(row, 'money_state', '') or 'unknown')} | "
        f"validation={str(getattr(row, 'validation_state', '') or 'unknown')} | "
        f"flow={float(getattr(row, 'net_inflow_yi', 0.0) or 0.0):.2f} | "
        f"amount_2m={float(getattr(row, 'amount_2m_sum', 0.0) or 0.0):.0f} | "
        f"front_2m={int(getattr(row, 'front_2m_count', 0) or 0)} | "
        f"strong_weak={float(getattr(row, 'strong_weak_ratio', 0.0) or 0.0):.2f} | "
        f"local={int(getattr(row, 'local_candidate_count', 0) or 0)} | "
        f"best_strategy={str(getattr(row, 'best_strategy', '') or '-')} | "
        f"votes={votes} | candidates={candidates} | "
        f"process={str(getattr(row, 'process_state', '') or 'unknown')} | "
        f"opportunity={str(getattr(row, 'opportunity_tag', '') or 'observe')} | "
        f"axes={axes} | "
        f"state={str(getattr(row, 'state_label', '') or 'unknown')} | "
        f"action={str(getattr(row, 'action_hint', '') or 'observe')} | "
        f"invalidation={invalidation} | "
        f"reject={str(getattr(row, 'reject_reason', '') or '-')} | "
        f"mismatch={str(getattr(row, 'mismatch_reason', '') or '-')}"
    )


def _enrich_funnel_trace_names(
    context: IntradayContext,
    traces: tuple[dict[str, object], ...],
) -> tuple[dict[str, object], ...]:
    if not traces:
        return ()
    name_map = {
        str(getattr(snapshot, "symbol", "") or ""): str(getattr(snapshot, "name", "") or "")
        for snapshot in tuple(getattr(context, "stock_snapshots", ()) or ())
        if str(getattr(snapshot, "symbol", "") or "")
    }
    enriched: list[dict[str, object]] = []
    for trace in traces:
        if not isinstance(trace, dict):
            continue
        symbol = str(trace.get("symbol", "") or "")
        name = str(trace.get("name", "") or "")
        if symbol and (not name or name == "unknown"):
            resolved_name = name_map.get(symbol, "")
            if resolved_name:
                updated = dict(trace)
                updated["name"] = resolved_name
                enriched.append(updated)
                continue
        enriched.append(trace)
    return tuple(enriched)


def _log_funnel_debug(
    decision_bundle: DecisionBundle | None,
    *,
    raw_count: int = 0,
    shape_count: int = 0,
) -> None:
    if decision_bundle is None:
        summary = build_empty_funnel_summary()
        summary["raw"] = int(raw_count)
        summary["shape"] = int(shape_count)
        summary["why_no_profit_center"] = _derive_why_no_profit_center(summary)
        logger.info("funnel.summary | %s", _format_funnel_summary(summary))
        return
    summary = dict(getattr(decision_bundle, "funnel_summary", {}) or build_empty_funnel_summary())
    logger.info("funnel.summary | %s", _format_funnel_summary(summary))
    for trace in tuple(getattr(decision_bundle, "funnel_traces", ()) or ())[:_FUNNEL_TRACE_LOG_LIMIT]:
        if isinstance(trace, dict):
            logger.info("funnel.trace | %s", _format_funnel_trace(trace))
    validations = {
        str(getattr(item, "theme", "") or ""): item
        for item in tuple(getattr(decision_bundle, "mainline_validation_states", ()) or ())
        if str(getattr(item, "theme", "") or "")
    }
    for signal in tuple(getattr(decision_bundle, "market_migration_signals", ()) or ())[:8]:
        logger.info(
            "hot_board.migration_shadow | %s",
            _format_migration_shadow(signal, validations.get(str(getattr(signal, "theme", "") or ""))),
        )
    stable_plan = getattr(decision_bundle, "stable_trading_plan", None)
    if stable_plan is not None:
        logger.info("stable_trade.plan | %s", _format_stable_trading_plan(stable_plan))
        for candidate in tuple(getattr(stable_plan, "candidates", ()) or ())[:3]:
            logger.info("stable_trade.candidate | %s", _format_stable_trading_candidate(candidate))
    theme_process_board = getattr(decision_bundle, "theme_process_board", None)
    if theme_process_board is not None:
        logger.info("theme_process.board | %s", _format_theme_process_board(theme_process_board))
        for row in tuple(getattr(theme_process_board, "rows", ()) or ())[:6]:
            logger.info("theme_process.row | %s", _format_theme_process_row(row))


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
                f"playbook_battlefield={decision_bundle.temporal_migration_decision.main_battlefield_theme if decision_bundle.temporal_migration_decision is not None else '-'}",
                f"playbook_battlefield_state={decision_bundle.temporal_migration_decision.battlefield_state if decision_bundle.temporal_migration_decision is not None else '-'}",
                f"playbook_handoff={(decision_bundle.temporal_migration_decision.handoff_from if decision_bundle.temporal_migration_decision is not None else '-') + '->' + (decision_bundle.temporal_migration_decision.handoff_to if decision_bundle.temporal_migration_decision is not None else '-')}",
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
        decision_bundle = None
        decision_notes = (f"local_decision_error={type(exc).__name__}",)
    decision_bundle = _finalize_funnel_summary(
        decision_bundle,
        context=context,
        raw_count=selected_snapshot_count,
        shape_count=len(stock_selection_contexts),
    )
    _log_funnel_debug(
        decision_bundle,
        raw_count=selected_snapshot_count,
        shape_count=len(stock_selection_contexts),
    )
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
        "context pipeline seed | selected=%s | final_candidates=%s | seeded=%s | missing_profile=%s | battlefield=%s | battlefield_state=%s | handoff=%s",
        len(selected_snapshots),
        len(final_candidate_rank),
        len(ranked_decisions),
        missing_profile_count,
        decision_bundle.temporal_migration_decision.main_battlefield_theme if decision_bundle is not None and decision_bundle.temporal_migration_decision is not None else "-",
        decision_bundle.temporal_migration_decision.battlefield_state if decision_bundle is not None and decision_bundle.temporal_migration_decision is not None else "-",
        (
            (decision_bundle.temporal_migration_decision.handoff_from or "-")
            + "->"
            + (decision_bundle.temporal_migration_decision.handoff_to or "-")
        )
        if decision_bundle is not None and decision_bundle.temporal_migration_decision is not None
        else "-",
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
