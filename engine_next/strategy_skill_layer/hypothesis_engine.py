from __future__ import annotations

from dataclasses import replace
import logging

from engine_next.domain.local_strategy_models import LocalMetric, LocalSignal, LocalStrategyScopeSummary
from engine_next.domain.decision_models import (
    CandidateFunnelSummary,
    DecisionTrace,
    DecisionBundle,
    FinalCandidateDecision,
    FocusAssetStressDecision,
    GlobalMarketDecision,
    HotPlateAnchorDecision,
    HypothesisValidation,
    LocalStrategyCandidateRow,
    MarketHypothesis,
    PlaybookControlMatrix,
    PlaybookControlRow,
    PlaybookOutputSummary,
    ShadowTakeoverDecision,
    StableTradingPlan,
    StableTradingPlanCandidate,
    StockLocalDecision,
    TemporalMigrationDecision,
    ThemeProcessBoard,
    ThemeProcessRow,
    ThemeStrategyVote,
    ThemeLocalDecision,
)
from engine_next.domain.models import IntradayContext
from engine_next.strategy_skill_layer.playbook_control import playbook_for_candidate_path, playbook_row
from engine_next.strategy_skill_layer.playbook_decision_adapter import (
    build_playbook_decision_view,
    slice_playbook_decision_views,
)

logger = logging.getLogger(__name__)

FINAL_CANDIDATE_BUFFER_LIMIT = 10
FINAL_CANDIDATE_DISPLAY_LIMIT = 5
ACTIONABLE_BUY_POINTS = {
    "turnover_confirm",
    "front_turnover",
    "low_open_repair",
    "halfway_momentum",
    "rotation_first_confirm",
    "mid_trend_support",
    "capacity_trend_support",
    "yest_core_relay",
    "dragon_divergence",
    "chip_breakout_support",
    "index_rebound_resonance",
    "trend_pullback_support",
    "capital_repair_support",
    "same_theme_arbitrage",
}
ACTIONABLE_BUY_GATES = {
    "action_not_probe",
    "rotation_wait_confirm",
    "turnover_ready",
    "front_turnover_ready",
    "low_open_repair",
    "rotation_amount_ready",
    "mid_trend_amount_ready",
    "capacity_trend_ready",
    "yest_core_relay_ready",
    "leader_divergence_ready",
    "intraday_push_ready",
    "chip_breakout_ready",
    "index_rebound_ready",
    "trend_pullback_ready",
    "capital_repair_ready",
    "same_theme_arbitrage_ready",
}
REPAIR_ACTIONABLE_BUY_POINTS = {
    "low_open_repair",
    "chip_breakout_support",
    "index_rebound_resonance",
    "trend_pullback_support",
    "capital_repair_support",
}


def _phase_name(context: IntradayContext) -> str:
    return str(getattr(context.phase, "value", context.phase) or "")


def _metric_value(trace: DecisionTrace | None, name: str, default: float = 0.0) -> float:
    if trace is None:
        return default
    for metric_name, value in trace.metric_values:
        if metric_name == name:
            return float(value)
    return default


def _effective_temporal_battlefield_state(
    temporal: TemporalMigrationDecision | None,
    state: str | None = None,
) -> str:
    raw_state = str(state if state is not None else getattr(temporal, "battlefield_state", "") or "")
    trace = getattr(temporal, "trace", None)
    guard_blocked = int(_metric_value(trace, "battlefield_guard_blocked", 0.0))
    if raw_state in {"extend", "handoff_confirmed"} and guard_blocked > 0:
        return "handoff_attempt" if raw_state == "handoff_confirmed" else "observe"
    if raw_state != "handoff_confirmed":
        return raw_state
    evidence_count = int(_metric_value(trace, "handoff_evidence_count", 0.0))
    persistence_ok = int(_metric_value(trace, "handoff_persistence_ok", 0.0))
    if evidence_count < 3 or persistence_ok < 1:
        return "handoff_attempt"
    return raw_state


def _fmt_amount_yuan(value: float) -> str:
    number = float(value or 0.0)
    if abs(number) >= 100_000_000:
        return f"{number / 100_000_000:.2f}e"
    if abs(number) >= 10_000:
        return f"{number / 10_000:.0f}w"
    return f"{number:.0f}"


def _fmt_metric_pct(value: float) -> str:
    return f"{float(value or 0.0) * 100:.1f}%"


def _fmt_plain_pct(value: float) -> str:
    return f"{float(value or 0.0):.2f}%"


def _hot_state_label(state: str) -> str:
    return {
        "tradable_repair": "低开修复可试错",
        "repair_confirming": "修复确认中",
        "panic_exposure": "风险释放中",
        "overheat_cashout": "高开兑现不追",
        "continuation": "热板延续",
        "rotation": "新热板轮动",
        "front_2m_watch": "前排承接观察",
        "fakeout": "假强不追",
        "fading": "热板退潮",
        "observe": "观察",
    }.get(str(state or "").strip(), str(state or "").strip() or "-")


def _hot_style_label(style: str) -> str:
    return {
        "shortline_tradable": "短线赚钱热板",
        "shortline_watch": "短线观察热板",
        "index_defense": "指数护盘热板",
        "cashout_risk": "兑现风险热板",
        "heat_only": "纯热度观察",
        "mixed_watch": "混合观察",
        "data_missing": "数据不足",
        "unknown": "未知",
    }.get(str(style or "").strip(), str(style or "").strip() or "-")


def _hot_metric_line_text(line) -> str:
    return (
        f"hot_metric:{line.plate_name};rank={line.rank};yest={line.yest_rank};"
        f"chg={_fmt_plain_pct(line.change_pct)};strength={line.strength:.0f};"
        f"inflow={line.net_inflow_yi:.2f}e;2m={_fmt_amount_yuan(line.amount_2m)};"
        f"front2m={line.front_2m_count};low_repair={int(getattr(line, 'low_open_repair_count', 0) or 0)};"
        f"high_fail={int(getattr(line, 'high_open_fail_count', 0) or 0)};delta={line.net_inflow_yi_delta:.2f}e;"
        f"bucket={int(getattr(line, 'big_rise_count', 0) or 0)}/"
        f"{int(getattr(line, 'strong_rise_count', 0) or 0)}/"
        f"{int(getattr(line, 'slight_rise_count', 0) or 0)}/"
        f"{int(getattr(line, 'slight_fall_count', 0) or 0)}/"
        f"{int(getattr(line, 'strong_fall_count', 0) or 0)}/"
        f"{int(getattr(line, 'big_fall_count', 0) or 0)};"
        f"sw={float(getattr(line, 'strong_weak_ratio', 0.0) or 0.0):.2f};"
        f"state={line.state};label={_hot_state_label(line.state)};"
        f"style={getattr(line, 'style', 'unknown')};style_label={_hot_style_label(getattr(line, 'style', 'unknown'))};"
        f"pct=s{line.strength_rank_pct:.2f},"
        f"chg{line.change_rank_pct:.2f},in{line.inflow_rank_pct:.2f},2m{line.amount_2m_rank_pct:.2f}"
    )


def _hot_quant_summary(hot_anchor: HotPlateAnchorDecision | None) -> str:
    if hot_anchor is None:
        return "hot:state=-"
    base = (
        f"hot:state={hot_anchor.anchor_state};primary={len(hot_anchor.primary_themes)};"
        f"continue={len(hot_anchor.continuation_themes)};rotate={len(hot_anchor.rotation_themes)};"
        f"risk={len(hot_anchor.fading_themes) + len(hot_anchor.fakeout_themes)}"
    )
    if not hot_anchor.metric_lines:
        return f"{base};top=-;top_chg=-;top_inflow=-"
    top = hot_anchor.metric_lines[0]
    return f"{base};top={top.plate_name};top_chg={_fmt_plain_pct(top.change_pct)};top_inflow={top.net_inflow_yi:.2f}e"


def _hot_metric_state_map(hot_anchor: HotPlateAnchorDecision | None) -> dict[str, str]:
    if hot_anchor is None:
        return {}
    return {
        line.plate_name: str(line.state or "observe")
        for line in tuple(getattr(hot_anchor, "metric_lines", ()) or ())
        if line.plate_name
    }


def _hot_metric_style_map(hot_anchor: HotPlateAnchorDecision | None) -> dict[str, str]:
    if hot_anchor is None:
        return {}
    return {
        line.plate_name: str(getattr(line, "style", "") or "unknown")
        for line in tuple(getattr(hot_anchor, "metric_lines", ()) or ())
        if line.plate_name
    }


def _hot_theme_is_hard_risk(theme_name: str, hot_anchor: HotPlateAnchorDecision | None) -> bool:
    if not theme_name or hot_anchor is None:
        return False
    state = _hot_metric_state_map(hot_anchor).get(theme_name, "")
    return theme_name in tuple(hot_anchor.fading_themes) or theme_name in tuple(hot_anchor.fakeout_themes) or state in {"fading", "fakeout", "overheat_cashout"}


def _hot_theme_is_index_defense(theme_name: str, hot_anchor: HotPlateAnchorDecision | None) -> bool:
    if not theme_name or hot_anchor is None:
        return False
    return _hot_metric_style_map(hot_anchor).get(theme_name, "") == "index_defense"


def _evidence_value(items: tuple[str, ...], key: str, default: str = "") -> str:
    prefix = f"{key}="
    for item in tuple(items or ()):
        text = str(item or "")
        if text.startswith(prefix):
            return text.removeprefix(prefix).strip()
    return default


def _candidate_buy_point_type(
    stock_decision: StockLocalDecision,
    *,
    path_type: str,
    action: str,
    risk_level: str,
    global_decision: GlobalMarketDecision,
    hot_anchor: HotPlateAnchorDecision | None,
) -> tuple[str, str, str]:
    """Translate existing local facts into a human-readable buy-point label.

    This is intentionally explanatory only. It must not change candidate ranking,
    theme ownership, action, or cap; the global/playbook layers still decide that.
    """

    behavior = str(stock_decision.entry_behavior or "")
    role = str(stock_decision.role_hint or "")
    path = str(path_type or "")
    current_pct = _metric_value(stock_decision.trace, "current_pct")
    open_pct = _metric_value(stock_decision.trace, "open_pct")
    amount_2m = _metric_value(stock_decision.trace, "amount_2m")
    amount_5m = _metric_value(stock_decision.trace, "amount_5m")
    amount_5m_rank_pct = _metric_value(stock_decision.trace, "amount_5m_rank_pct", 1.0)
    volume_intensity = _metric_value(stock_decision.trace, "volume_intensity")
    is_yest_limit = _metric_value(stock_decision.trace, "is_yest_limit") >= 0.5
    auction_amount = _metric_value(stock_decision.trace, "auction_amount")
    speed_1m = _metric_value(stock_decision.trace, "speed_1m")
    amount_2m_vs_auction = _metric_value(stock_decision.trace, "amount_2m_vs_auction")
    resistance_gap = _metric_value(stock_decision.trace, "resistance_gap", 1.0)
    chip_cleanliness = _metric_value(stock_decision.trace, "chip_cleanliness")
    trend_health = _metric_value(stock_decision.trace, "trend_health")
    concentration = _metric_value(stock_decision.trace, "concentration", 1.0)
    ddje = _metric_value(stock_decision.trace, "ddje")
    ddx = _metric_value(stock_decision.trace, "ddx")
    ddy = _metric_value(stock_decision.trace, "ddy")
    ddz = _metric_value(stock_decision.trace, "ddz")
    bias_20 = _metric_value(stock_decision.trace, "bias_20")
    rsi_6 = _metric_value(stock_decision.trace, "rsi_6")
    kline_score = _metric_value(stock_decision.trace, "kline_score")
    chip_score = _metric_value(stock_decision.trace, "chip_score")
    shape_quality_score = _metric_value(stock_decision.trace, "shape_quality_score")
    open_undertake_score = _metric_value(stock_decision.trace, "open_undertake_score")
    execution_quality_score = _metric_value(stock_decision.trace, "execution_quality_score")
    daily_height_low = _metric_value(stock_decision.trace, "daily_height_low") >= 0.5
    daily_height_mid = _metric_value(stock_decision.trace, "daily_height_mid") >= 0.5
    daily_height_high = _metric_value(stock_decision.trace, "daily_height_high") >= 0.5
    market_rising_rate = _metric_value(global_decision.trace, "market_rising_rate")
    market_strong_weak_ratio = _metric_value(global_decision.trace, "market_strong_weak_ratio")
    market_downside_pressure_rate = _metric_value(global_decision.trace, "market_downside_pressure_rate")
    amount_ready = amount_2m >= 20_000_000 or auction_amount >= 20_000_000 or speed_1m >= 0.008
    capacity_ready = amount_5m >= 50_000_000 or amount_5m_rank_pct <= 0.20 or volume_intensity >= 2.5
    hard_hot_risk = _hot_theme_is_hard_risk(stock_decision.theme_name, hot_anchor)
    near_limit = current_pct >= 0.095 or (open_pct >= 0.09 and current_pct >= 0.08)
    confirmed_high_open_structure = (
        role in {"true_leader", "front_row"}
        and behavior in {"volume_confirm", "confirmed", "repair_strength", "limit_attack"}
        and amount_ready
        and current_pct <= 0.09
        and current_pct >= open_pct - 0.006
        and not hard_hot_risk
    )
    high_open_chase = (
        open_pct >= 0.06
        and current_pct >= 0.06
        and behavior != "low_open_repair"
        and not confirmed_high_open_structure
    )
    market_allows_probe = (
        action == "probe"
        and risk_level == "normal"
        and global_decision.market_script in {"attack_confirmed", "pressure_validation"}
    )
    market_allows_shape = (
        risk_level == "normal"
        and global_decision.market_script in {"attack_confirmed", "pressure_validation"}
    )
    market_allows_repair_shape = (
        risk_level in {"normal", "elevated"}
        and global_decision.market_script in {"attack_confirmed", "pressure_validation", "watch_validation"}
    )
    open_support_ready = current_pct >= max(open_pct - 0.006, -0.005) and amount_2m >= max(auction_amount * 0.70, 12_000_000)
    chip_breakout_ready = (
        market_allows_repair_shape
        and auction_amount >= 18_000_000
        and open_support_ready
        and 0.0 <= open_pct <= 0.065
        and current_pct >= 0.018
        and resistance_gap <= 0.045
        and (chip_cleanliness >= 5.5 or chip_score >= 5.8)
        and amount_2m_vs_auction >= 0.65
    )
    index_rebound_ready = (
        market_allows_repair_shape
        and (market_rising_rate >= 0.55 or market_strong_weak_ratio >= 1.20)
        and market_downside_pressure_rate <= 0.28
        and -0.025 <= open_pct <= 0.035
        and current_pct >= max(open_pct + 0.012, 0.012)
        and amount_ready
        and (daily_height_low or daily_height_mid)
    )
    trend_pullback_ready = (
        market_allows_repair_shape
        and trend_health >= 6.3
        and (kline_score >= 5.8 or shape_quality_score >= 6.0)
        and -0.035 <= open_pct <= 0.025
        and current_pct >= max(open_pct - 0.004, -0.004)
        and amount_ready
        and not daily_height_high
        and bias_20 <= 0.16
        and rsi_6 <= 82
    )
    capital_repair_ready = (
        market_allows_repair_shape
        and ddje < 0
        and (ddx > 0 or ddy > 0 or ddz > 0 or auction_amount >= 20_000_000)
        and open_pct >= -0.035
        and current_pct >= max(open_pct + 0.015, 0.015)
        and (amount_2m >= max(auction_amount * 0.85, 15_000_000) or speed_1m >= 0.006)
        and (chip_cleanliness >= 4.8 or concentration <= 0.30)
    )
    same_theme_arbitrage_ready = (
        market_allows_probe
        and global_decision.market_script == "attack_confirmed"
        and str(stock_decision.theme_name or "").strip() == str(global_decision.main_attack_theme or "").strip()
        and role == "mid_follow"
        and behavior in {"volume_confirm", "confirmed", "repair_strength"}
        and amount_ready
        and -0.025 <= open_pct <= 0.04
        and 0.006 <= current_pct <= 0.075
        and current_pct >= open_pct - 0.006
        and not daily_height_high
        and not hard_hot_risk
    )
    intraday_push_ready = (
        amount_2m >= 15_000_000
        and speed_1m >= 0.006
        and 0.025 <= current_pct <= 0.085
        and current_pct >= max(open_pct + 0.008, 0.025)
    )
    turnover_ready = (
        0.015 <= open_pct <= 0.07
        and 0.02 <= current_pct <= 0.09
        and current_pct >= max(open_pct - 0.008, 0.0)
        and amount_ready
    )
    def _authorization_gate() -> str:
        if action == "probe":
            return ""
        if "_off_mainline" in path:
            return "off_mainline_watch"
        if "focus_stress" in path:
            return "focus_stress_watch"
        if "dragon_alone" in path:
            return "dragon_alone_watch"
        if path.startswith(("hot_plate_anchor_watch", "timeframe_watch", "battlefield_handoff_attack")):
            return "rotation_wait_confirm"
        if global_decision.market_script not in {"attack_confirmed", "pressure_validation"} or path.endswith("_risk_off"):
            return "market_not_probe"
        if path == "watch":
            return "path_watch_only"
        return "action_not_probe"

    auth_gate = _authorization_gate()

    def _ready_gate(gate: str) -> str:
        return auth_gate or gate

    def _repair_ready_gate(gate: str) -> str:
        if auth_gate == "market_not_probe" and global_decision.market_script == "watch_validation":
            return gate
        return _ready_gate(gate)

    repair_shape_ready = (
        chip_breakout_ready
        or index_rebound_ready
        or trend_pullback_ready
        or capital_repair_ready
    )

    if hard_hot_risk:
        return ("avoid_chase", "禁止追高", "hot_theme_hard_risk")
    if near_limit and role != "true_leader":
        return ("avoid_chase", "禁止追高", "near_limit_non_leader")
    if behavior == "high_open_distribution":
        return ("avoid_chase", "禁止追高", "high_open_distribution")
    if high_open_chase:
        return ("avoid_chase", "禁止追高", "high_open_chase")
    if near_limit and behavior == "limit_attack":
        return ("strength_only", "强度代表", "near_limit_strength_only")
    if risk_level not in {"normal", "elevated"}:
        return ("watch_only", "观察承接", "risk_level_block")
    if behavior == "low_open_repair" or (open_pct < 0 and current_pct >= 0.02):
        return ("low_open_repair", "低开转强", _repair_ready_gate("low_open_repair"))
    if chip_breakout_ready:
        return ("chip_breakout_support", "筹码突破承接", _repair_ready_gate("chip_breakout_ready"))
    if index_rebound_ready:
        return ("index_rebound_resonance", "大盘修复共振", _repair_ready_gate("index_rebound_ready"))
    if trend_pullback_ready:
        return ("trend_pullback_support", "趋势回踩承接", _repair_ready_gate("trend_pullback_ready"))
    if capital_repair_ready:
        return ("capital_repair_support", "资金修复承接", _repair_ready_gate("capital_repair_ready"))
    if action != "probe" and risk_level != "normal" and not repair_shape_ready:
        return ("watch_only", "观察承接", "risk_elevated_watch")
    if role in {"true_leader", "front_row"} and turnover_ready and not high_open_chase:
        return ("turnover_confirm", "换手确认", _ready_gate("turnover_ready"))
    if market_allows_probe and intraday_push_ready and role in {"front_row", "mid_follow"}:
        return ("halfway_momentum", "半路放量", "intraday_push_ready")
    if is_yest_limit and role in {"true_leader", "front_row"} and amount_ready and 0.015 <= current_pct <= 0.09:
        return ("yest_core_relay", "昨日核心接力", _ready_gate("yest_core_relay_ready"))
    if role == "true_leader" and behavior in {"volume_confirm", "confirmed", "repair_strength", "limit_attack"}:
        return ("dragon_divergence", "龙头分歧", _ready_gate("leader_divergence_ready"))
    if (
        role == "front_row"
        and behavior in {"volume_confirm", "confirmed", "repair_strength"}
        and amount_ready
        and not high_open_chase
    ):
        return ("front_turnover", "前排换手承接", _ready_gate("front_turnover_ready"))
    if (
        ("handoff" in path or path.startswith("timeframe_aligned") or path.startswith("hot_plate_anchor_attack"))
        and market_allows_shape
        and amount_ready
    ):
        return ("rotation_first_confirm", "切换首确认", _ready_gate("rotation_amount_ready"))
    if same_theme_arbitrage_ready:
        return ("same_theme_arbitrage", "同题材套利", _ready_gate("same_theme_arbitrage_ready"))
    if role == "mid_follow" and behavior in {"repair_strength", "confirmed", "low_open_repair"} and amount_ready:
        return ("mid_trend_support", "中军趋势承接", _ready_gate("mid_trend_amount_ready"))
    if (
        role == "mid_follow"
        and market_allows_shape
        and capacity_ready
        and 0.01 <= current_pct <= 0.07
        and current_pct >= open_pct - 0.01
    ):
        return ("capacity_trend_support", "容量趋势承接", _ready_gate("capacity_trend_ready"))
    if action != "probe":
        return ("watch_only", "观察承接", "action_not_probe")
    if action == "probe" and risk_level == "elevated":
        return ("watch_only", "观察承接", "risk_elevated_watch")
    if not market_allows_probe:
        return ("watch_only", "观察承接", "market_not_probe")
    if not amount_ready and not capacity_ready:
        return ("watch_only", "观察承接", "amount_not_ready")
    return ("watch_only", "观察承接", "shape_not_ready")


def _buy_point_code(candidate: FinalCandidateDecision | None) -> str:
    if candidate is None:
        return ""
    return _evidence_value(tuple(candidate.trace.evidence_summary or ()), "buy_point", "")


def _buy_gate_code(candidate: FinalCandidateDecision | None) -> str:
    if candidate is None:
        return ""
    return _evidence_value(tuple(candidate.trace.evidence_summary or ()), "buy_gate", "")


def _candidate_has_actionable_buy_gate(candidate: FinalCandidateDecision) -> bool:
    buy_point = _buy_point_code(candidate) or ""
    risk_tags = tuple(candidate.trace.risk_tags or ())
    risk_allowed = candidate.risk_level == "normal" or (
        candidate.risk_level == "elevated"
        and buy_point in REPAIR_ACTIONABLE_BUY_POINTS
    )
    relative_risk_allowed = (
        "relative_risk_theme" not in risk_tags
        or buy_point in REPAIR_ACTIONABLE_BUY_POINTS
    )
    return (
        buy_point in ACTIONABLE_BUY_POINTS
        and (_buy_gate_code(candidate) or "") in ACTIONABLE_BUY_GATES
        and risk_allowed
        and relative_risk_allowed
        and "hot_plate_hard_risk" not in risk_tags
        and "index_defense_hot_plate" not in risk_tags
        and "focus_asset_stress" not in risk_tags
        and "dragon_alone_risk" not in risk_tags
    )


def _candidate_is_direct_probe_buy(candidate: FinalCandidateDecision) -> bool:
    """Actionable buy point that is already authorized as a probe candidate."""

    return _candidate_has_actionable_buy_gate(candidate) and candidate.action == "probe"


def _stock_metric_values_for_feed(stock_decision: StockLocalDecision) -> tuple[tuple[str, float], ...]:
    names = (
        "auction_amount",
        "amount_2m",
        "amount_5m",
        "amount_2m_vs_auction",
        "speed_1m",
        "open_pct",
        "current_pct",
        "high_pct",
        "low_pct",
        "leader_rank_in_theme",
        "is_yest_limit",
        "lb_days",
        "trend_health",
        "resistance_gap",
        "chip_cleanliness",
        "ddje",
    )
    return tuple((name, _metric_value(stock_decision.trace, name)) for name in names)


def _build_local_candidate_feeds(
    decision_bundle: DecisionBundle,
    global_decision: GlobalMarketDecision,
    *,
    temporal: TemporalMigrationDecision | None,
    hot_anchor: HotPlateAnchorDecision | None,
) -> tuple[LocalStrategyCandidateRow, ...]:
    stock_decisions = tuple(decision_bundle.stock_local_decisions or ())
    if not stock_decisions:
        return ()
    main_attack_theme = str(global_decision.main_attack_theme or "")
    temporal_targets = set(tuple(getattr(temporal, "target_themes", ()) or ()))
    hot_primary = set(tuple(getattr(hot_anchor, "primary_themes", ()) or ()))

    def _fact_tags(stock_decision: StockLocalDecision) -> tuple[str, ...]:
        tags: list[str] = []
        open_pct = _metric_value(stock_decision.trace, "open_pct")
        current_pct = _metric_value(stock_decision.trace, "current_pct")
        amount_2m = _metric_value(stock_decision.trace, "amount_2m")
        auction_amount = _metric_value(stock_decision.trace, "auction_amount")
        if open_pct < 0:
            tags.append("low_open")
        elif open_pct > 0.03:
            tags.append("high_open")
        if current_pct >= max(open_pct + 0.015, 0.015):
            tags.append("open_repair")
        if amount_2m >= max(auction_amount * 0.80, 20_000_000):
            tags.append("volume_expand")
        if stock_decision.role_hint == "front_row":
            tags.append("front_row")
        if stock_decision.role_hint == "mid_follow":
            tags.append("mid_follow")
        if _metric_value(stock_decision.trace, "is_yest_limit") >= 0.5:
            tags.append("yest_limit_core")
        if current_pct >= 0.095:
            tags.append("near_limit")
        if _metric_value(stock_decision.trace, "trend_health") >= 6.3:
            tags.append("trend_hold")
        if _metric_value(stock_decision.trace, "resistance_gap", 1.0) <= 0.045:
            tags.append("pullback_hold")
        if _metric_value(stock_decision.trace, "chip_cleanliness") >= 5.5:
            tags.append("chip_breakout_like")
        return tuple(dict.fromkeys(tags))

    def _top_rows(
        strategy_id: str,
        rows: list[StockLocalDecision],
        *,
        sort_key,
        limit: int = 3,
    ) -> list[LocalStrategyCandidateRow]:
        output: list[LocalStrategyCandidateRow] = []
        for rank, stock_decision in enumerate(sorted(rows, key=sort_key)[:limit], start=1):
            output.append(
                LocalStrategyCandidateRow(
                    strategy_id=strategy_id,
                    symbol=stock_decision.symbol,
                    theme_name=stock_decision.theme_name,
                    rank_in_strategy=rank,
                    metric_values=_stock_metric_values_for_feed(stock_decision),
                    fact_tags=_fact_tags(stock_decision),
                    evidence_refs=stock_decision.trace.evidence_refs,
                )
            )
        return output

    mainline_rows = [
        item
        for item in stock_decisions
        if item.theme_name == main_attack_theme and item.role_hint in {"true_leader", "front_row"}
    ]
    rotation_rows = [
        item
        for item in stock_decisions
        if item.theme_name
        and item.theme_name != main_attack_theme
        and (item.theme_name in temporal_targets or item.theme_name in hot_primary)
    ]
    repair_rows = [
        item
        for item in stock_decisions
        if item.entry_behavior == "low_open_repair"
        or (
            _metric_value(item.trace, "open_pct") < 0
            and _metric_value(item.trace, "current_pct") >= max(_metric_value(item.trace, "open_pct") + 0.015, 0.015)
        )
    ]
    trend_rows = [
        item
        for item in stock_decisions
        if _metric_value(item.trace, "trend_health") >= 6.0
        and item.entry_behavior in {"confirmed", "volume_confirm", "repair_strength", "low_open_repair"}
    ]
    arbitrage_rows = [
        item
        for item in stock_decisions
        if item.theme_name == main_attack_theme
        and item.role_hint == "mid_follow"
        and item.entry_behavior in {"confirmed", "volume_confirm", "repair_strength"}
    ]

    feeds: list[LocalStrategyCandidateRow] = []
    feeds.extend(
        _top_rows(
            "mainline_local",
            mainline_rows,
            sort_key=lambda item: (
                int(item.role_hint != "true_leader"),
                int(_metric_value(item.trace, "leader_rank_in_theme", 999)),
                -_metric_value(item.trace, "amount_2m"),
                -_metric_value(item.trace, "speed_1m"),
                item.symbol,
            ),
        )
    )
    feeds.extend(
        _top_rows(
            "rotation_local",
            rotation_rows,
            sort_key=lambda item: (
                -_metric_value(item.trace, "amount_2m_vs_auction"),
                -_metric_value(item.trace, "amount_2m"),
                -(_metric_value(item.trace, "current_pct") - _metric_value(item.trace, "open_pct")),
                item.symbol,
            ),
        )
    )
    feeds.extend(
        _top_rows(
            "repair_local",
            repair_rows,
            sort_key=lambda item: (
                -(_metric_value(item.trace, "current_pct") - _metric_value(item.trace, "open_pct")),
                -_metric_value(item.trace, "amount_2m_vs_auction"),
                -_metric_value(item.trace, "amount_2m"),
                item.symbol,
            ),
        )
    )
    feeds.extend(
        _top_rows(
            "trend_local",
            trend_rows,
            sort_key=lambda item: (
                -_metric_value(item.trace, "trend_health"),
                -_metric_value(item.trace, "current_pct"),
                -_metric_value(item.trace, "amount_2m"),
                _metric_value(item.trace, "resistance_gap", 1.0),
                item.symbol,
            ),
        )
    )
    feeds.extend(
        _top_rows(
            "arbitrage_local",
            arbitrage_rows,
            sort_key=lambda item: (
                -_metric_value(item.trace, "amount_2m"),
                -_metric_value(item.trace, "speed_1m"),
                -_metric_value(item.trace, "current_pct"),
                item.symbol,
            ),
        )
    )
    return tuple(feeds)


def _local_strategy_pack_gate(
    global_decision: GlobalMarketDecision,
    *,
    temporal: TemporalMigrationDecision | None,
    hot_anchor: HotPlateAnchorDecision | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Choose which local strategy packs may supplement final candidates.

    Local strategies are responsible for finding stocks and exposing numeric
    facts. The global layer should pick suitable strategy packs for the current
    money path, not consume every local candidate row as a flat metric pool.
    """

    market_script = str(getattr(global_decision, "market_script", "") or "")
    main_attack_theme = str(getattr(global_decision, "main_attack_theme", "") or "")
    temporal_state = _effective_temporal_battlefield_state(
        temporal,
        str(getattr(temporal, "battlefield_state", "") or "") if temporal is not None else "",
    )
    temporal_targets = set(tuple(getattr(temporal, "target_themes", ()) or ())) if temporal is not None else set()
    temporal_handoff_to = str(getattr(temporal, "handoff_to", "") or "") if temporal is not None else ""
    temporal_rising_hot = set(tuple(getattr(temporal, "rising_hot_themes", ()) or ())) if temporal is not None else set()
    hot_primary = set(tuple(getattr(hot_anchor, "primary_themes", ()) or ())) if hot_anchor is not None else set()
    hot_rotation = set(tuple(getattr(hot_anchor, "rotation_themes", ()) or ())) if hot_anchor is not None else set()
    hot_fading = set(tuple(getattr(hot_anchor, "fading_themes", ()) or ())) if hot_anchor is not None else set()
    hot_fakeout = set(tuple(getattr(hot_anchor, "fakeout_themes", ()) or ())) if hot_anchor is not None else set()
    has_hot_risk = bool(hot_fading or hot_fakeout)
    risk_off = market_script == "risk_off"
    allowed: list[str] = []
    rejected: list[str] = []

    def _allow(strategy_id: str, condition: bool, reject_reason: str) -> None:
        if condition:
            allowed.append(strategy_id)
        else:
            rejected.append(f"{strategy_id}:{reject_reason}")

    mainline_context = bool(
        main_attack_theme
        and market_script in {"attack_confirmed", "pressure_validation", "watch_validation"}
        and (
            temporal_state in {"extend", "handoff_confirmed"}
            or main_attack_theme in hot_primary
            or main_attack_theme in temporal_targets
            or main_attack_theme in temporal_rising_hot
        )
    )
    rotation_context = bool(
        not risk_off
        and (
            temporal_state in {"handoff_attempt", "handoff_confirmed", "rotation_attack", "rotation_exchange"}
            or bool(temporal_handoff_to)
            or bool(hot_rotation)
            or bool(hot_primary.intersection(temporal_targets))
        )
    )
    repair_context = bool(
        not risk_off
        and (
            market_script in {"watch_validation", "pressure_validation", "attack_confirmed"}
            or has_hot_risk
            or temporal_state in {"handoff_attempt", "observe", "mixed", "rotation_exchange"}
        )
    )
    trend_context = bool(
        not risk_off
        and market_script in {"attack_confirmed", "pressure_validation", "watch_validation"}
    )
    arbitrage_context = bool(
        main_attack_theme
        and market_script == "attack_confirmed"
        and temporal_state in {"extend", "handoff_confirmed"}
        and not has_hot_risk
    )

    _allow("mainline_local", mainline_context, "mainline_not_confirmed")
    _allow("rotation_local", rotation_context, "rotation_not_active")
    _allow("repair_local", repair_context, "repair_not_supported")
    _allow("trend_local", trend_context, "trend_context_not_allowed")
    _allow("arbitrage_local", arbitrage_context, "arbitrage_requires_clean_attack")
    return (tuple(dict.fromkeys(allowed)), tuple(rejected))


def _local_feed_metric_value(row: LocalStrategyCandidateRow | None, name: str, default: float = 0.0) -> float:
    if row is None:
        return default
    for metric_name, value in tuple(row.metric_values or ()):
        if metric_name == name:
            return float(value)
    return default


def _local_strategy_pack_quality(
    strategy_id: str,
    row: LocalStrategyCandidateRow,
) -> tuple[bool, tuple[str, ...]]:
    tags = set(tuple(row.fact_tags or ()))
    amount_2m = _local_feed_metric_value(row, "amount_2m")
    auction_amount = _local_feed_metric_value(row, "auction_amount")
    amount_ratio = _local_feed_metric_value(row, "amount_2m_vs_auction")
    speed_1m = _local_feed_metric_value(row, "speed_1m")
    open_pct = _local_feed_metric_value(row, "open_pct")
    current_pct = _local_feed_metric_value(row, "current_pct")
    trend_health = _local_feed_metric_value(row, "trend_health")
    resistance_gap = _local_feed_metric_value(row, "resistance_gap", 1.0)
    chip_cleanliness = _local_feed_metric_value(row, "chip_cleanliness")
    reasons: list[str] = []

    def _ok(condition: bool, reason: str) -> None:
        if condition:
            reasons.append(reason)

    amount_ready = amount_2m >= 20_000_000 or auction_amount >= 30_000_000
    undertake_ready = amount_ratio >= 0.80 or speed_1m >= 0.006
    not_chase_zone = current_pct < 0.095 or "yest_limit_core" in tags
    if strategy_id == "mainline_local":
        _ok("front_row" in tags or "yest_limit_core" in tags, "role_front_or_yest_core")
        _ok(amount_ready or undertake_ready, "amount_or_undertake_ready")
        _ok(not_chase_zone, "not_nonleader_chase")
    elif strategy_id == "rotation_local":
        _ok(undertake_ready, "rotation_undertake")
        _ok(current_pct >= max(open_pct - 0.006, -0.01), "price_not_fading")
        _ok("near_limit" not in tags or "yest_limit_core" in tags, "not_blind_near_limit")
    elif strategy_id == "repair_local":
        _ok("open_repair" in tags or current_pct >= max(open_pct + 0.015, 0.015), "repair_price")
        _ok(amount_ready or undertake_ready, "repair_amount")
        _ok(open_pct <= 0.03, "not_high_open_repair")
    elif strategy_id == "trend_local":
        _ok(trend_health >= 6.0 or "trend_hold" in tags, "trend_health")
        _ok(amount_ready or undertake_ready, "trend_amount")
        _ok(resistance_gap <= 0.08 or chip_cleanliness >= 5.0, "resistance_or_chip_ok")
    elif strategy_id == "arbitrage_local":
        _ok("mid_follow" in tags, "mid_follow")
        _ok(amount_ready or undertake_ready, "spillover_amount")
        _ok(0.0 <= current_pct <= 0.085, "not_late_spillover")
    else:
        _ok(amount_ready or undertake_ready, "generic_amount")
    return (len(reasons) >= 2, tuple(reasons))


def _log_local_strategy_feed_audit(
    context: IntradayContext,
    local_candidate_feeds: tuple[LocalStrategyCandidateRow, ...],
    *,
    global_decision: GlobalMarketDecision,
    temporal: TemporalMigrationDecision | None,
    hot_anchor: HotPlateAnchorDecision | None,
) -> None:
    rows = tuple(local_candidate_feeds or ())
    if not rows:
        logger.info(
            "local strategy feed audit | phase=%s | total=0 | market=%s | main=%s | battlefield=%s/%s | hot=%s",
            _phase_name(context),
            global_decision.market_script or "-",
            global_decision.main_attack_theme or "-",
            str(getattr(temporal, "main_battlefield_theme", "") or "-") if temporal is not None else "-",
            str(getattr(temporal, "battlefield_state", "") or "-") if temporal is not None else "-",
            ",".join(tuple(getattr(hot_anchor, "primary_themes", ()) or ())[:3]) if hot_anchor is not None else "-",
        )
        return
    by_strategy: dict[str, list[LocalStrategyCandidateRow]] = {}
    for row in rows:
        by_strategy.setdefault(row.strategy_id or "unknown", []).append(row)
    for strategy_id, strategy_rows in sorted(by_strategy.items()):
        ordered = sorted(
            strategy_rows,
            key=lambda item: (
                int(item.rank_in_strategy or 999),
                -_local_feed_metric_value(item, "amount_2m"),
                item.symbol,
            ),
        )
        themes = tuple(dict.fromkeys(row.theme_name for row in ordered if row.theme_name))[:4]
        sample_parts: list[str] = []
        for row in ordered[:3]:
            tags = ",".join(tuple(row.fact_tags or ())[:4]) or "-"
            sample_parts.append(
                (
                    f"{row.symbol}:{row.theme_name or '-'}"
                    f"/rank={int(row.rank_in_strategy or 999)}"
                    f"/2m={_local_feed_metric_value(row, 'amount_2m'):.0f}"
                    f"/auc={_local_feed_metric_value(row, 'auction_amount'):.0f}"
                    f"/open={_local_feed_metric_value(row, 'open_pct'):.3f}"
                    f"/now={_local_feed_metric_value(row, 'current_pct'):.3f}"
                    f"/tags={tags}"
                )
            )
        logger.info(
            "local strategy feed audit | phase=%s | strategy=%s | rows=%s | themes=%s | sample=%s | market=%s | main=%s | battlefield=%s/%s | hot=%s",
            _phase_name(context),
            strategy_id,
            len(strategy_rows),
            ",".join(themes) or "-",
            ";".join(sample_parts) or "-",
            global_decision.market_script or "-",
            global_decision.main_attack_theme or "-",
            str(getattr(temporal, "main_battlefield_theme", "") or "-") if temporal is not None else "-",
            str(getattr(temporal, "battlefield_state", "") or "-") if temporal is not None else "-",
            ",".join(tuple(getattr(hot_anchor, "primary_themes", ()) or ())[:3]) if hot_anchor is not None else "-",
        )


def _log_strategy_pack_contract_audit(
    context: IntradayContext,
    local_candidate_feeds: tuple[LocalStrategyCandidateRow, ...],
    *,
    global_decision: GlobalMarketDecision,
    temporal: TemporalMigrationDecision | None,
    hot_anchor: HotPlateAnchorDecision | None,
    allowed_strategy_ids: tuple[str, ...],
    rejected_strategy_reasons: tuple[str, ...],
) -> None:
    allowed = set(tuple(allowed_strategy_ids or ()))
    rejected_reason = {
        item.split(":", 1)[0]: item.split(":", 1)[1]
        for item in tuple(rejected_strategy_reasons or ())
        if ":" in str(item)
    }
    rows_by_strategy: dict[str, list[LocalStrategyCandidateRow]] = {}
    for row in tuple(local_candidate_feeds or ()):
        rows_by_strategy.setdefault(row.strategy_id or "unknown", []).append(row)
    market_script = str(getattr(global_decision, "market_script", "") or "")
    main_attack = str(getattr(global_decision, "main_attack_theme", "") or "")
    temporal_state = _effective_temporal_battlefield_state(
        temporal,
        str(getattr(temporal, "battlefield_state", "") or "") if temporal is not None else "",
    )
    battlefield = str(getattr(temporal, "main_battlefield_theme", "") or "") if temporal is not None else ""
    handoff_to = str(getattr(temporal, "handoff_to", "") or "") if temporal is not None else ""
    hot_primary = tuple(getattr(hot_anchor, "primary_themes", ()) or ()) if hot_anchor is not None else ()
    hot_rotation = tuple(getattr(hot_anchor, "rotation_themes", ()) or ()) if hot_anchor is not None else ()
    hot_risk_count = (
        len(tuple(getattr(hot_anchor, "fading_themes", ()) or ()))
        + len(tuple(getattr(hot_anchor, "fakeout_themes", ()) or ()))
        if hot_anchor is not None
        else 0
    )
    contracts = {
        "mainline_local": (
            "mainline_extend",
            "main_attack+front_row+temporal_or_hot_support",
            "no_mainline_or_unconfirmed",
        ),
        "rotation_local": (
            "capital_rotation",
            "handoff_or_hot_rotation+non_risk_off",
            "no_rotation_evidence_or_risk_off",
        ),
        "repair_local": (
            "panic_repair",
            "repair_market_or_hot_risk_release+non_risk_off",
            "no_repair_context_or_risk_off",
        ),
        "trend_local": (
            "capacity_trend",
            "supportive_market+trend_health",
            "risk_off_or_market_not_supportive",
        ),
        "arbitrage_local": (
            "same_theme_spillover",
            "clean_attack+mainline_extend+mid_follow",
            "not_clean_attack_or_hot_risk",
        ),
    }
    for strategy_id, (setup, hard_condition, fail_condition) in contracts.items():
        rows = sorted(
            rows_by_strategy.get(strategy_id, []),
            key=lambda item: (
                int(item.rank_in_strategy or 999),
                -_local_feed_metric_value(item, "amount_2m"),
                item.symbol,
            ),
        )
        quality_rows = tuple((_local_strategy_pack_quality(strategy_id, row), row) for row in rows)
        quality_pass_count = sum(1 for (passed, _reasons), _row in quality_rows if passed)
        quality_block_count = len(rows) - quality_pass_count
        sample = "-"
        sample_quality = "-"
        if rows:
            row = rows[0]
            _passed, quality_reasons = _local_strategy_pack_quality(strategy_id, row)
            sample_quality = ",".join(quality_reasons[:4]) or "-"
            sample = (
                f"{row.symbol}:{row.theme_name or '-'}"
                f"/2m={_local_feed_metric_value(row, 'amount_2m'):.0f}"
                f"/now={_local_feed_metric_value(row, 'current_pct'):.3f}"
                f"/tags={','.join(tuple(row.fact_tags or ())[:4]) or '-'}"
            )
        logger.info(
            "strategy pack contract | phase=%s | strategy=%s | enabled=%s | setup=%s | hard=%s | fail=%s | reject=%s | rows=%s | quality_pass=%s | quality_block=%s | sample=%s | sample_quality=%s | market=%s | main=%s | battlefield=%s/%s | handoff_to=%s | hot=%s | hot_rotation=%s | hot_risk=%s",
            _phase_name(context),
            strategy_id,
            1 if strategy_id in allowed else 0,
            setup,
            hard_condition,
            fail_condition,
            rejected_reason.get(strategy_id, "-"),
            len(rows),
            quality_pass_count,
            quality_block_count,
            sample,
            sample_quality,
            market_script or "-",
            main_attack or "-",
            battlefield or "-",
            temporal_state or "-",
            handoff_to or "-",
            ",".join(hot_primary[:3]) or "-",
            ",".join(hot_rotation[:3]) or "-",
            hot_risk_count,
        )


def _build_candidate_funnel_summary(
    local_candidate_feeds: tuple[LocalStrategyCandidateRow, ...],
    final_candidates: tuple[FinalCandidateDecision, ...],
    candidate_slice,
) -> CandidateFunnelSummary:
    strategy_counts: dict[str, int] = {}
    strategy_samples: dict[str, str] = {}
    merged_symbols: set[str] = set()
    for row in local_candidate_feeds:
        strategy_counts[row.strategy_id] = strategy_counts.get(row.strategy_id, 0) + 1
        strategy_samples.setdefault(row.strategy_id, row.symbol)
        merged_symbols.add(row.symbol)
    gate_counts: dict[str, int] = {}
    for candidate in final_candidates:
        gate_code = _buy_gate_code(candidate) or "unknown"
        gate_counts[gate_code] = gate_counts.get(gate_code, 0) + 1
    return CandidateFunnelSummary(
        strategy_counts=tuple(f"{key}:{value}" for key, value in strategy_counts.items()),
        merged_count=len(merged_symbols),
        final_count=len(final_candidates),
        primary_count=len(tuple(candidate_slice.primary or ())),
        watch_count=len(tuple(candidate_slice.watch or ())),
        blocked_count=len(tuple(candidate_slice.blocked or ())),
        inactive_count=len(tuple(candidate_slice.inactive or ())),
        gate_reason_counts=tuple(
            f"{key}:{value}"
            for key, value in sorted(gate_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
        strategy_samples=tuple(f"{key}:{value}" for key, value in strategy_samples.items()),
    )


def _candidate_metric_values_dict(candidate: FinalCandidateDecision | None, *, limit: int = 8) -> dict[str, float]:
    if candidate is None:
        return {}
    output: dict[str, float] = {}
    for name, value in tuple(getattr(candidate.trace, "metric_values", ()) or ())[:limit]:
        key = str(name or "")
        if not key:
            continue
        try:
            output[key] = round(float(value), 4)
        except (TypeError, ValueError):
            continue
    return output


def _candidate_money_theme_aligned(theme: str, money_themes: tuple[str, ...]) -> bool:
    theme_text = str(theme or "")
    if not theme_text or not money_themes:
        return False
    for money_theme in money_themes:
        money_text = str(money_theme or "")
        if not money_text:
            continue
        if theme_text == money_text or theme_text in money_text or money_text in theme_text:
            return True
    return False


def _stable_plan_candidate_role(candidate: FinalCandidateDecision, row: LocalStrategyCandidateRow | None) -> str:
    tags = set(tuple(getattr(row, "fact_tags", ()) or ())) if row is not None else set()
    if "true_leader" in tags or "yest_limit_core" in tags:
        return "true_leader"
    if "front_row" in tags:
        return "front_row"
    amount_2m = _metric_value(candidate.trace, "amount_2m")
    auction_amount = _metric_value(candidate.trace, "auction_amount")
    if amount_2m >= 100_000_000 or auction_amount >= 100_000_000:
        return "capacity_core"
    if "mid_follow" in tags:
        return "follower"
    return "unknown"


def _stable_plan_candidate_evidence(candidate: FinalCandidateDecision, row: LocalStrategyCandidateRow | None) -> tuple[str, ...]:
    evidence = [
        f"2m={_fmt_amount_yuan(_metric_value(candidate.trace, 'amount_2m'))}",
        f"auction={_fmt_amount_yuan(_metric_value(candidate.trace, 'auction_amount'))}",
        f"now={_fmt_metric_pct(_metric_value(candidate.trace, 'current_pct'))}",
    ]
    if row is not None:
        evidence.append(f"strategy={row.strategy_id}")
        tags = ",".join(tuple(getattr(row, "fact_tags", ()) or ())[:3])
        if tags:
            evidence.append(f"tags={tags}")
    return tuple(evidence)


def _stable_plan_row_role(row: LocalStrategyCandidateRow) -> str:
    tags = set(tuple(getattr(row, "fact_tags", ()) or ()))
    if "true_leader" in tags or "yest_limit_core" in tags:
        return "true_leader"
    if "front_row" in tags:
        return "front_row"
    amount_2m = _local_feed_metric_value(row, "amount_2m")
    auction_amount = _local_feed_metric_value(row, "auction_amount")
    if amount_2m >= 100_000_000 or auction_amount >= 100_000_000:
        return "capacity_core"
    if "mid_follow" in tags:
        return "follower"
    return "unknown"


def _stable_plan_row_buy_point(row: LocalStrategyCandidateRow) -> str:
    tags = set(tuple(getattr(row, "fact_tags", ()) or ()))
    if "open_repair" in tags:
        return "low_open_repair"
    if "front_row" in tags and _local_feed_metric_value(row, "amount_2m") > 0:
        return "front_turnover"
    if _local_feed_metric_value(row, "current_pct") >= 0.095:
        return "avoid_chase"
    if "trend_hold" in tags:
        return "capacity_trend_support"
    return "wait_confirm"


def _stable_plan_row_evidence(row: LocalStrategyCandidateRow) -> tuple[str, ...]:
    tags = ",".join(tuple(getattr(row, "fact_tags", ()) or ())[:3]) or "-"
    return (
        f"2m={_fmt_amount_yuan(_local_feed_metric_value(row, 'amount_2m'))}",
        f"auction={_fmt_amount_yuan(_local_feed_metric_value(row, 'auction_amount'))}",
        f"now={_fmt_metric_pct(_local_feed_metric_value(row, 'current_pct'))}",
        f"strategy={row.strategy_id}",
        f"tags={tags}",
    )


def _stable_plan_setup_score(
    *,
    role: str,
    buy_point: str,
    amount_2m: float,
    auction_amount: float,
    current_pct: float,
) -> float:
    score = 0.0
    if role in {"true_leader", "front_row", "capacity_core"}:
        score += 2.0
    elif role == "follower":
        score += 0.5
    if buy_point in {"front_turnover", "turnover_confirm", "opening_confirm", "low_open_repair", "capital_repair"}:
        score += 2.0
    elif buy_point in {"capacity_trend_support", "wait_confirm"}:
        score += 1.0
    if amount_2m >= 100_000_000:
        score += 2.0
    elif amount_2m >= 20_000_000:
        score += 1.0
    if auction_amount >= 100_000_000:
        score += 1.0
    elif auction_amount >= 30_000_000:
        score += 0.5
    if current_pct >= 0.095:
        score -= 2.0
    elif -0.03 <= current_pct <= 0.085:
        score += 1.0
    return round(score, 2)


def _stable_plan_candidate_state(*, setup_score: float, buy_point: str, source_bucket: str) -> str:
    if buy_point in {"avoid_chase", "forbidden_chase"} or source_bucket in {"blocked", "invalidated", "inactive"}:
        return "risk_attention"
    if setup_score >= 5.0 and buy_point not in {"wait_confirm", "unknown"}:
        return "setup_ready"
    if setup_score >= 3.0:
        return "wait_confirm"
    return "watch_only"


def _stable_plan_signal_metric(signal: object) -> str:
    axes = tuple(getattr(signal, "evidence_axes", ()) or ())
    return (
        f"{str(getattr(signal, 'theme', '') or '-')}:"
        f"rank={int(getattr(signal, 'rank', 999) or 999)},"
        f"flow={float(getattr(signal, 'net_inflow_yi', 0.0) or 0.0):.1f}e,"
        f"chg={float(getattr(signal, 'change_pct', 0.0) or 0.0):.2f}%,"
        f"axes={len(axes)},"
        f"state={str(getattr(signal, 'money_state', '') or 'unknown')}"
    )


def _stable_plan_validation_blocks_candidate(validation_state: str) -> bool:
    return validation_state in {"failed", "withdrawal", "degraded", "invalidated"}


def _stable_plan_candidate_confirm_conditions(candidate: FinalCandidateDecision, row: LocalStrategyCandidateRow | None) -> tuple[str, ...]:
    buy_point = _buy_point_code(candidate) or "unknown"
    role = _stable_plan_candidate_role(candidate, row)
    base = [
        "money_theme_stays_front",
        "front_row_2m_or_5m_holds",
    ]
    if buy_point in {"front_turnover", "turnover_confirm", "opening_confirm"}:
        base.append("turnover_confirm_not_fade")
    elif buy_point in {"low_open_repair", "capital_repair"}:
        base.append("repair_reclaims_key_price")
    else:
        base.append("buy_point_confirmed")
    if role in {"true_leader", "front_row", "capacity_core"}:
        base.append(f"role={role}")
    return tuple(base)


def _build_stable_trading_plan(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
    global_decision: GlobalMarketDecision,
    final_candidates: tuple[FinalCandidateDecision, ...],
    local_candidate_feeds: tuple[LocalStrategyCandidateRow, ...],
) -> StableTradingPlan:
    """Answer the four trading questions in shadow mode without changing recommendations."""

    signals = tuple(getattr(decision_bundle, "market_migration_signals", ()) or ())
    validation_by_theme = {
        str(getattr(item, "theme", "") or ""): str(getattr(item, "validation_state", "") or "")
        for item in tuple(getattr(decision_bundle, "mainline_validation_states", ()) or ())
        if str(getattr(item, "theme", "") or "")
    }
    positive_signals = tuple(
        signal
        for signal in signals
        if str(getattr(signal, "money_state", "") or "") in {"money_rotation_in", "money_in"}
        and str(getattr(signal, "source_freshness", "") or "") in {"fresh", "cached"}
    )
    positive_signals = tuple(
        sorted(
            positive_signals,
            key=lambda item: (
                int(getattr(item, "rank", 999) or 999),
                -len(tuple(getattr(item, "evidence_axes", ()) or ())),
                -float(getattr(item, "net_inflow_yi", 0.0) or 0.0),
                str(getattr(item, "theme", "") or ""),
            ),
        )
    )
    money_out_signals = tuple(
        signal
        for signal in signals
        if str(getattr(signal, "money_state", "") or "") == "money_out"
    )
    risk_or_noise_signals = tuple(
        signal
        for signal in signals
        if str(getattr(signal, "money_state", "") or "") in {"fake_hot", "attention_only", "style_risk_line"}
    )
    money_to = tuple(dict.fromkeys(str(getattr(item, "theme", "") or "") for item in positive_signals if str(getattr(item, "theme", "") or "")))[:5]
    money_from = tuple(dict.fromkeys(str(getattr(item, "theme", "") or "") for item in money_out_signals if str(getattr(item, "theme", "") or "")))[:5]
    risk_or_noise = tuple(dict.fromkeys(str(getattr(item, "theme", "") or "") for item in risk_or_noise_signals if str(getattr(item, "theme", "") or "")))[:5]
    money_to_metrics = tuple(_stable_plan_signal_metric(item) for item in positive_signals[:5])
    money_from_metrics = tuple(_stable_plan_signal_metric(item) for item in money_out_signals[:5])
    risk_or_noise_metrics = tuple(_stable_plan_signal_metric(item) for item in risk_or_noise_signals[:5])

    top_signal = positive_signals[0] if positive_signals else None
    top_validation = validation_by_theme.get(str(getattr(top_signal, "theme", "") or ""), "") if top_signal is not None else ""
    top_axes = tuple(getattr(top_signal, "evidence_axes", ()) or ()) if top_signal is not None else ()
    top_tags = tuple(getattr(top_signal, "money_tags", ()) or ()) if top_signal is not None else ()
    if not top_signal:
        best_tactic = "watch_only"
        tactic_reason = ("no_money_rotation_signal",)
    elif _stable_plan_validation_blocks_candidate(top_validation):
        best_tactic = "risk_attention"
        tactic_reason = (f"money_to={getattr(top_signal, 'theme', '')}", f"validation={top_validation}")
    elif top_validation in {"auction_candidate", "open_watch"}:
        best_tactic = "wait_open_confirm"
        tactic_reason = (f"money_to={getattr(top_signal, 'theme', '')}", f"validation={top_validation}")
    elif "front_axis" in top_axes and "spread_axis" in top_axes:
        best_tactic = "front_row_turnover_confirm"
        tactic_reason = (f"money_to={getattr(top_signal, 'theme', '')}", "front_and_spread_axes")
    elif "expectation_gap" in top_tags or "rotation_axis" in top_axes:
        best_tactic = "rotation_front_row"
        tactic_reason = (f"money_to={getattr(top_signal, 'theme', '')}", "expectation_gap_or_rotation")
    else:
        best_tactic = "capacity_core_confirm"
        tactic_reason = (f"money_to={getattr(top_signal, 'theme', '')}", "flow_confirm")

    feed_by_symbol: dict[str, LocalStrategyCandidateRow] = {}
    for row in sorted(
        tuple(local_candidate_feeds or ()),
        key=lambda item: (int(item.rank_in_strategy or 999), item.strategy_id, item.symbol),
    ):
        if row.symbol:
            feed_by_symbol.setdefault(row.symbol, row)

    plan_candidates: list[StableTradingPlanCandidate] = []
    sorted_candidates = sorted(
        tuple(final_candidates or ()),
        key=lambda item: (
            0 if item.bucket in {"primary", "main", "profit_center"} else 1 if item.bucket == "watch" else 2,
            int(item.priority_rank or 999),
            -_metric_value(item.trace, "amount_2m"),
            -_metric_value(item.trace, "auction_amount"),
            item.symbol,
        ),
    )
    for candidate in sorted_candidates:
        buy_point_code = _buy_point_code(candidate) or "unknown"
        if candidate.bucket in {"blocked", "invalidated", "inactive"} or buy_point_code in {"avoid_chase", "forbidden_chase"}:
            continue
        if not _candidate_money_theme_aligned(candidate.theme_name, money_to):
            continue
        theme_validation = validation_by_theme.get(candidate.theme_name, "")
        if _stable_plan_validation_blocks_candidate(theme_validation):
            continue
        row = feed_by_symbol.get(candidate.symbol)
        role = _stable_plan_candidate_role(candidate, row)
        if role not in {"true_leader", "front_row", "capacity_core"} and candidate.bucket not in {"primary", "watch"}:
            continue
        amount_2m = _metric_value(candidate.trace, "amount_2m")
        auction_amount = _metric_value(candidate.trace, "auction_amount")
        current_pct = _metric_value(candidate.trace, "current_pct")
        setup_score = _stable_plan_setup_score(
            role=role,
            buy_point=buy_point_code,
            amount_2m=amount_2m,
            auction_amount=auction_amount,
            current_pct=current_pct,
        )
        candidate_state = _stable_plan_candidate_state(
            setup_score=setup_score,
            buy_point=buy_point_code,
            source_bucket=candidate.bucket,
        )
        plan_candidates.append(
            StableTradingPlanCandidate(
                symbol=candidate.symbol,
                theme_name=candidate.theme_name,
                source_bucket=candidate.bucket,
                candidate_state=candidate_state,
                strategy_id=str(getattr(row, "strategy_id", "") or candidate.playbook or ""),
                buy_point=buy_point_code,
                role=role,
                setup_score=setup_score,
                confirm_condition=_stable_plan_candidate_confirm_conditions(candidate, row),
                invalidation_points=tuple(candidate.trace.invalidation_points or ()) or ("theme_money_fades", "stock_2m_fades"),
                evidence_summary=_stable_plan_candidate_evidence(candidate, row),
            )
        )
        if len(plan_candidates) >= 3:
            break

    selected_symbols = {item.symbol for item in plan_candidates}
    if len(plan_candidates) < 3 and money_to:
        for row in sorted(
            tuple(local_candidate_feeds or ()),
            key=lambda item: (
                int(item.rank_in_strategy or 999),
                -_local_feed_metric_value(item, "amount_2m"),
                -_local_feed_metric_value(item, "auction_amount"),
                item.symbol,
            ),
        ):
            if not row.symbol or row.symbol in selected_symbols:
                continue
            if not _candidate_money_theme_aligned(row.theme_name, money_to):
                continue
            theme_validation = validation_by_theme.get(row.theme_name, "")
            if _stable_plan_validation_blocks_candidate(theme_validation):
                continue
            quality_passed, quality_reasons = _local_strategy_pack_quality(row.strategy_id, row)
            if not quality_passed:
                continue
            role = _stable_plan_row_role(row)
            if role not in {"true_leader", "front_row", "capacity_core"}:
                continue
            buy_point = _stable_plan_row_buy_point(row)
            if buy_point == "avoid_chase":
                continue
            amount_2m = _local_feed_metric_value(row, "amount_2m")
            auction_amount = _local_feed_metric_value(row, "auction_amount")
            current_pct = _local_feed_metric_value(row, "current_pct")
            setup_score = _stable_plan_setup_score(
                role=role,
                buy_point=buy_point,
                amount_2m=amount_2m,
                auction_amount=auction_amount,
                current_pct=current_pct,
            )
            candidate_state = _stable_plan_candidate_state(
                setup_score=setup_score,
                buy_point=buy_point,
                source_bucket="local_setup",
            )
            plan_candidates.append(
                StableTradingPlanCandidate(
                    symbol=row.symbol,
                    theme_name=row.theme_name,
                    source_bucket="local_setup",
                    candidate_state=candidate_state,
                    strategy_id=row.strategy_id,
                    buy_point=buy_point,
                    role=role,
                    setup_score=setup_score,
                    confirm_condition=(
                        "money_theme_stays_front",
                        "local_setup_needs_global_confirm",
                        "front_row_2m_or_5m_holds",
                    ),
                    invalidation_points=("theme_money_fades", "stock_2m_fades"),
                    evidence_summary=(*_stable_plan_row_evidence(row)[:4], f"quality={','.join(quality_reasons[:2]) or '-'}"),
                )
            )
            selected_symbols.add(row.symbol)
            if len(plan_candidates) >= 3:
                break
    state_priority = {
        "setup_ready": 0,
        "wait_confirm": 1,
        "watch_only": 2,
        "risk_attention": 3,
    }
    plan_candidates = sorted(
        plan_candidates,
        key=lambda item: (
            state_priority.get(item.candidate_state, 9),
            -float(item.setup_score or 0.0),
            item.symbol,
        ),
    )[:3]

    why_no_candidate = ""
    if not plan_candidates:
        if not money_to:
            why_no_candidate = "no_money_direction"
        elif top_validation and _stable_plan_validation_blocks_candidate(top_validation):
            why_no_candidate = "mainline_validation_failed"
        elif not final_candidates:
            why_no_candidate = "no_final_candidate"
        else:
            why_no_candidate = "candidate_theme_mismatch_money_flow"

    confirm_conditions = (
        "money_theme_rank_stays_front",
        "money_theme_flow_not_reverse",
        "front_row_2m_or_5m_holds",
        "candidate_buy_point_confirms",
    )
    invalidation_points = tuple(
        dict.fromkeys(
            (
                "money_theme_flow_reverses",
                "hot_rank_fades",
                "front_row_fades",
                "candidate_buy_point_fails",
                *tuple(global_decision.trace.invalidation_points or ()),
            )
        )
    )
    return StableTradingPlan(
        money_to=money_to,
        money_from=money_from,
        risk_or_noise=risk_or_noise,
        money_to_metrics=money_to_metrics,
        money_from_metrics=money_from_metrics,
        risk_or_noise_metrics=risk_or_noise_metrics,
        best_tactic=best_tactic,
        tactic_reason=tuple(tactic_reason),
        candidates=tuple(plan_candidates),
        confirm_conditions=confirm_conditions,
        invalidation_points=invalidation_points,
        why_no_candidate=why_no_candidate,
        phase=_phase_name(context),
    )


def _build_theme_process_board(
    context: IntradayContext,
    decision_bundle: DecisionBundle,
    global_decision: GlobalMarketDecision,
    final_candidates: tuple[FinalCandidateDecision, ...],
    local_candidate_feeds: tuple[LocalStrategyCandidateRow, ...],
    stable_plan: StableTradingPlan,
) -> ThemeProcessBoard:
    """Build a read-only theme process table from existing facts.

    This is a diagnostic bridge: it exposes theme fighting/strengthening and
    local-strategy concentration without changing candidate buckets.
    """

    signals = {
        str(getattr(signal, "theme", "") or ""): signal
        for signal in tuple(getattr(decision_bundle, "market_migration_signals", ()) or ())
        if str(getattr(signal, "theme", "") or "")
    }
    validation_by_theme = {
        str(getattr(item, "theme", "") or ""): str(getattr(item, "validation_state", "") or "")
        for item in tuple(getattr(decision_bundle, "mainline_validation_states", ()) or ())
        if str(getattr(item, "theme", "") or "")
    }
    hot_anchor = getattr(decision_bundle, "hot_plate_anchor_decision", None)
    hot_metric_by_theme = {
        str(getattr(line, "plate_name", "") or ""): line
        for line in tuple(getattr(hot_anchor, "metric_lines", ()) or ())
        if str(getattr(line, "plate_name", "") or "")
    }
    final_by_theme: dict[str, list[FinalCandidateDecision]] = {}
    for candidate in tuple(final_candidates or ()):
        theme = str(getattr(candidate, "theme_name", "") or "")
        if not theme:
            continue
        final_by_theme.setdefault(theme, []).append(candidate)

    feed_by_theme_strategy: dict[str, dict[str, list[LocalStrategyCandidateRow]]] = {}
    for row in tuple(local_candidate_feeds or ()):
        theme = str(getattr(row, "theme_name", "") or "")
        strategy_id = str(getattr(row, "strategy_id", "") or "")
        if not theme or not strategy_id:
            continue
        feed_by_theme_strategy.setdefault(theme, {}).setdefault(strategy_id, []).append(row)

    temporal = getattr(decision_bundle, "temporal_migration_decision", None)
    current_mainline = (
        str(getattr(global_decision, "main_attack_theme", "") or "")
        or str(getattr(temporal, "main_battlefield_theme", "") or "")
    )
    candidate_themes = tuple(
        dict.fromkeys(
            theme
            for theme in (
                *signals.keys(),
                *hot_metric_by_theme.keys(),
                *feed_by_theme_strategy.keys(),
                *final_by_theme.keys(),
                current_mainline,
                *tuple(getattr(stable_plan, "money_to", ()) or ()),
                *tuple(getattr(stable_plan, "risk_or_noise", ()) or ()),
            )
            if theme
        )
    )

    def _theme_strategy_votes(theme: str) -> tuple[ThemeStrategyVote, ...]:
        votes: list[ThemeStrategyVote] = []
        for strategy_id, rows in sorted(feed_by_theme_strategy.get(theme, {}).items()):
            sorted_rows = sorted(
                rows,
                key=lambda item: (
                    int(getattr(item, "rank_in_strategy", 999) or 999),
                    -_local_feed_metric_value(item, "amount_2m"),
                    -_local_feed_metric_value(item, "auction_amount"),
                    str(getattr(item, "symbol", "") or ""),
                ),
            )
            current_values = [_local_feed_metric_value(item, "current_pct") for item in sorted_rows]
            buy_points = tuple(dict.fromkeys(_stable_plan_row_buy_point(item) for item in sorted_rows if item.symbol))
            votes.append(
                ThemeStrategyVote(
                    strategy_id=strategy_id,
                    count=len(sorted_rows),
                    top_symbols=tuple(item.symbol for item in sorted_rows[:3] if item.symbol),
                    top_amount_2m=max((_local_feed_metric_value(item, "amount_2m") for item in sorted_rows), default=0.0),
                    top_auction_amount=max((_local_feed_metric_value(item, "auction_amount") for item in sorted_rows), default=0.0),
                    avg_current_pct=round(sum(current_values) / len(current_values), 4) if current_values else 0.0,
                    buy_points=buy_points,
                )
            )
        votes.sort(key=lambda item: (-item.count, -item.top_amount_2m, item.strategy_id))
        return tuple(votes[:5])

    def _row_state_label(money_state: str, validation_state: str, local_count: int, strong_weak_ratio: float, hot_rank: int) -> str:
        if money_state in {"fake_hot", "money_out"} or validation_state in {"failed", "withdrawal", "degraded"}:
            return "risk_or_fading"
        if money_state == "style_risk_line":
            return "style_risk_watch"
        if money_state in {"money_rotation_in", "money_in"} and validation_state in {"open_validated", "intraday_emerging"} and local_count > 0:
            return "strengthening_with_candidates"
        if money_state in {"money_rotation_in", "money_in"} and validation_state in {"auction_candidate", "open_watch"}:
            return "money_in_wait_confirm"
        if local_count > 0 and hot_rank > 8:
            return "local_strength_off_hot_board"
        if money_state == "attention_only":
            return "attention_only"
        if strong_weak_ratio > 1.5 and local_count > 0:
            return "profit_effect_watch"
        return "observe"

    def _row_action_hint(state_label: str) -> str:
        return {
            "strengthening_with_candidates": "setup_watch",
            "money_in_wait_confirm": "wait_open_or_front_confirm",
            "local_strength_off_hot_board": "mainline_recheck_watch",
            "profit_effect_watch": "watch_for_spread",
            "style_risk_watch": "risk_style_only",
            "risk_or_fading": "avoid_or_downgrade",
            "attention_only": "info_only",
        }.get(state_label, "observe")

    def _row_evidence_axes(
        *,
        money_state: str,
        validation_state: str,
        hot_rank: int,
        net_inflow_yi: float,
        front_2m_count: int,
        local_count: int,
        strong_weak_ratio: float,
    ) -> tuple[str, ...]:
        axes: list[str] = []
        if hot_rank <= 5:
            axes.append("hot_axis")
        if money_state in {"money_rotation_in", "money_in"} or net_inflow_yi > 0:
            axes.append("flow_axis")
        if front_2m_count > 0 or validation_state in {"open_validated", "intraday_emerging"}:
            axes.append("front_axis")
        if local_count > 0:
            axes.append("local_axis")
        if strong_weak_ratio >= 1.5:
            axes.append("spread_axis")
        if validation_state in {"failed", "withdrawal"} or money_state in {"money_out", "fake_hot"}:
            axes.append("risk_axis")
        return tuple(dict.fromkeys(axes))

    def _row_process_state(
        *,
        money_state: str,
        validation_state: str,
        hot_rank: int,
        net_inflow_yi: float,
        front_2m_count: int,
        local_count: int,
        strong_weak_ratio: float,
        evidence_axes: tuple[str, ...],
    ) -> str:
        axis_count = len(tuple(axis for axis in evidence_axes if axis != "risk_axis"))
        if validation_state in {"failed", "withdrawal"}:
            return "failed" if validation_state == "failed" else "cashout"
        if money_state == "fake_hot":
            return "fake_hot"
        if money_state == "money_out":
            return "cashout"
        if money_state == "style_risk_line":
            return "style_risk"
        if (
            money_state in {"money_rotation_in", "money_in"}
            and validation_state in {"open_validated", "intraday_emerging"}
            and local_count > 0
            and axis_count >= 3
        ):
            return "main_rising" if strong_weak_ratio >= 1.5 or front_2m_count >= 2 else "attack"
        if money_state in {"money_rotation_in", "money_in"} and validation_state in {"auction_candidate", "open_watch"}:
            return "validating"
        if money_state in {"money_rotation_in", "money_in"} and hot_rank <= 6 and axis_count >= 2:
            return "forming"
        if local_count > 0 and hot_rank > 8 and net_inflow_yi >= 0:
            return "counter_trend"
        if hot_rank <= 5 and money_state == "attention_only":
            return "attention"
        if local_count > 0 and strong_weak_ratio >= 1.5:
            return "divergence"
        return "observe"

    def _row_opportunity_tag(process_state: str, *, hot_rank: int, local_count: int, evidence_axes: tuple[str, ...]) -> str:
        axis_set = set(evidence_axes)
        if process_state in {"main_rising", "attack"} and {"flow_axis", "front_axis", "local_axis"}.issubset(axis_set):
            return "resonance"
        if process_state == "counter_trend":
            return "counter_trend"
        if process_state in {"forming", "validating"} and hot_rank <= 6 and local_count > 0:
            return "expectation_gap"
        if process_state in {"cashout", "fake_hot", "failed", "style_risk"}:
            return "risk"
        if process_state == "divergence":
            return "divergence"
        return "observe"

    def _row_invalidation_points(process_state: str, validation_state: str, money_state: str) -> tuple[str, ...]:
        points: list[str] = []
        if process_state in {"main_rising", "attack", "validating", "forming"}:
            points.extend(("theme_front_row_fades", "hot_rank_rolls_down", "flow_turns_out"))
        if process_state in {"counter_trend", "expectation_gap"}:
            points.extend(("no_open_validation", "stock_2m_fades"))
        if money_state in {"attention_only", "style_risk_line"}:
            points.append("spread_not_confirmed")
        if validation_state in {"failed", "withdrawal"}:
            points.append(f"validation_{validation_state}")
        return tuple(dict.fromkeys(points))

    rows: list[ThemeProcessRow] = []
    money_to_set = set(tuple(getattr(stable_plan, "money_to", ()) or ()))
    for theme in candidate_themes:
        signal = signals.get(theme)
        metric_line = hot_metric_by_theme.get(theme)
        votes = _theme_strategy_votes(theme)
        local_candidates = tuple(dict.fromkeys(symbol for vote in votes for symbol in vote.top_symbols if symbol))
        final_candidates_for_theme = sorted(
            final_by_theme.get(theme, ()),
            key=lambda item: (int(getattr(item, "priority_rank", 999) or 999), item.symbol),
        )
        top_candidates = tuple(
            dict.fromkeys(
                (
                    *(item.symbol for item in final_candidates_for_theme[:3] if item.symbol),
                    *local_candidates[:3],
                )
            )
        )
        hot_rank = int(getattr(signal, "rank", 999) or 999) if signal is not None else int(getattr(metric_line, "rank", 999) or 999)
        money_state = str(getattr(signal, "money_state", "") or "unknown") if signal is not None else "unknown"
        validation_state = validation_by_theme.get(theme, str(getattr(signal, "validation_state", "") or "unknown") if signal is not None else "unknown")
        amount_2m_sum = float(getattr(metric_line, "amount_2m", 0.0) or 0.0) if metric_line is not None else sum(vote.top_amount_2m for vote in votes)
        strong_weak_ratio = float(getattr(metric_line, "strong_weak_ratio", 0.0) or 0.0) if metric_line is not None else 0.0
        local_count = sum(vote.count for vote in votes)
        net_inflow_yi = float(getattr(signal, "net_inflow_yi", 0.0) or 0.0) if signal is not None else 0.0
        front_2m_count = int(getattr(metric_line, "front_2m_count", 0) or 0) if metric_line is not None else 0
        evidence_axes = _row_evidence_axes(
            money_state=money_state,
            validation_state=validation_state,
            hot_rank=hot_rank,
            net_inflow_yi=net_inflow_yi,
            front_2m_count=front_2m_count,
            local_count=local_count,
            strong_weak_ratio=strong_weak_ratio,
        )
        process_state = _row_process_state(
            money_state=money_state,
            validation_state=validation_state,
            hot_rank=hot_rank,
            net_inflow_yi=net_inflow_yi,
            front_2m_count=front_2m_count,
            local_count=local_count,
            strong_weak_ratio=strong_weak_ratio,
            evidence_axes=evidence_axes,
        )
        opportunity_tag = _row_opportunity_tag(
            process_state,
            hot_rank=hot_rank,
            local_count=local_count,
            evidence_axes=evidence_axes,
        )
        state_label = _row_state_label(money_state, validation_state, local_count, strong_weak_ratio, hot_rank)
        mismatch_reason = ""
        if local_count > 0 and theme not in money_to_set and theme != current_mainline:
            mismatch_reason = "local_strength_not_in_money_to"
        reject_reason = ""
        if money_state in {"fake_hot", "money_out", "style_risk_line"}:
            reject_reason = money_state
        elif validation_state in {"failed", "withdrawal", "degraded"}:
            reject_reason = f"validation_{validation_state}"
        rows.append(
            ThemeProcessRow(
                theme=theme,
                hot_rank=hot_rank,
                money_state=money_state,
                validation_state=validation_state,
                net_inflow_yi=net_inflow_yi,
                amount_2m_sum=amount_2m_sum,
                front_2m_count=front_2m_count,
                strong_weak_ratio=strong_weak_ratio,
                local_candidate_count=local_count,
                local_top_amount_2m=max((vote.top_amount_2m for vote in votes), default=0.0),
                best_strategy=votes[0].strategy_id if votes else "",
                top_candidates=top_candidates,
                strategy_votes=votes,
                state_label=state_label,
                process_state=process_state,
                opportunity_tag=opportunity_tag,
                evidence_axes=evidence_axes,
                invalidation_points=_row_invalidation_points(process_state, validation_state, money_state),
                action_hint=_row_action_hint(state_label),
                reject_reason=reject_reason,
                mismatch_reason=mismatch_reason,
            )
        )

    process_priority = {
        "main_rising": 0,
        "attack": 1,
        "validating": 2,
        "forming": 3,
        "counter_trend": 4,
        "divergence": 5,
        "attention": 6,
        "style_risk": 7,
        "cashout": 8,
        "fake_hot": 9,
        "failed": 10,
    }
    opportunity_priority = {
        "resonance": 0,
        "expectation_gap": 1,
        "counter_trend": 2,
        "divergence": 3,
        "observe": 4,
        "risk": 5,
    }
    rows.sort(
        key=lambda item: (
            process_priority.get(item.process_state, 20),
            opportunity_priority.get(item.opportunity_tag, 20),
            -int(item.local_candidate_count or 0),
            int(item.hot_rank or 999),
            -float(item.amount_2m_sum or 0.0),
            item.theme,
        )
    )
    current_rows = [row for row in rows if row.theme == current_mainline]
    current_local_count = current_rows[0].local_candidate_count if current_rows else 0
    challenger = next(
        (
            row
            for row in rows
            if row.theme != current_mainline
            and row.state_label in {"strengthening_with_candidates", "local_strength_off_hot_board", "money_in_wait_confirm"}
            and row.local_candidate_count >= max(2, current_local_count + 1)
            and row.local_top_amount_2m >= 20_000_000
        ),
        None,
    )
    recheck_required = bool(challenger is not None and current_mainline)
    process_summary = tuple(
        f"{row.theme}:{row.process_state}/{row.opportunity_tag}/hot={row.hot_rank}/local={row.local_candidate_count}/best={row.best_strategy or '-'}"
        for row in rows[:5]
    )
    return ThemeProcessBoard(
        rows=tuple(rows[:8]),
        recheck_required=recheck_required,
        recheck_reason="local_strategy_cluster_challenges_mainline" if recheck_required else "",
        current_mainline=current_mainline,
        execution_focus_candidate=challenger.theme if challenger is not None else "",
        process_summary=process_summary,
    )


def _candidate_trace_source(candidate: FinalCandidateDecision | None) -> tuple[str, ...]:
    if candidate is None:
        return ("unknown",)
    metrics = _candidate_metric_values_dict(candidate)
    sources: list[str] = ["stock_plate_mapping"]
    if float(metrics.get("auction_amount", 0.0) or 0.0) > 0.0:
        sources.append("auction_0925_top_amount")
    if float(metrics.get("amount_2m", 0.0) or 0.0) > 0.0:
        sources.append("live_quote_active")
        sources.append("q2_active")
    if float(metrics.get("is_yest_limit", 0.0) or 0.0) >= 0.5:
        sources.append("yest_limit_pool")
    sources.append("hypothesis_final_candidate")
    return tuple(dict.fromkeys(sources))


def _candidate_trace_reasons(candidate: FinalCandidateDecision | None, view=None) -> tuple[str, ...]:
    reasons: list[str] = []
    if view is not None:
        view_reason = str(getattr(view, "reason", "") or "")
        if view_reason:
            reasons.append(view_reason)
        reasons.extend(str(item) for item in tuple(getattr(view, "risk_tags", ()) or ()) if str(item))
    if candidate is not None:
        reasons.extend(str(item) for item in tuple(getattr(candidate.trace, "reason_codes", ()) or ()) if str(item))
        reasons.extend(str(item) for item in tuple(getattr(candidate.trace, "risk_tags", ()) or ()) if str(item))
    if not reasons:
        reasons.append("candidate_observed")
    return tuple(dict.fromkeys(reasons[:5]))


def _candidate_pass_or_block(candidate: FinalCandidateDecision | None, view=None) -> str:
    if view is not None:
        if bool(getattr(view, "blocked", False)):
            return "blocked"
        bucket = str(getattr(view, "display_bucket", "") or "")
        if bucket == "primary" or bool(getattr(view, "primary_allowed", False)):
            return "pass"
        if bucket in {"watch", "inactive"}:
            return "watch"
    if candidate is None:
        return "unknown"
    action = str(getattr(candidate, "action", "") or "")
    bucket = str(getattr(candidate, "bucket", "") or "")
    risk_level = str(getattr(candidate, "risk_level", "") or "")
    if action == "probe" and risk_level != "high":
        return "pass"
    if bucket in {"shadow_attack", "shadow_rotation"}:
        return "watch"
    if action in {"avoid", "avoid_chase", "disabled"} or risk_level == "high":
        return "blocked"
    return "watch"


def _build_hypothesis_funnel_traces(
    *,
    final_candidates: tuple[FinalCandidateDecision, ...],
    candidate_slice,
    limit: int = 20,
) -> tuple[dict[str, object], ...]:
    candidate_map = {candidate.symbol: candidate for candidate in final_candidates}
    ordered_views = (
        tuple(getattr(candidate_slice, "primary", ()) or ())
        + tuple(getattr(candidate_slice, "watch", ()) or ())
        + tuple(getattr(candidate_slice, "blocked", ()) or ())
        + tuple(getattr(candidate_slice, "inactive", ()) or ())
        + tuple(getattr(candidate_slice, "unclassified", ()) or ())
    )
    traces: list[dict[str, object]] = []
    seen: set[str] = set()
    for view in ordered_views:
        symbol = str(getattr(view, "symbol", "") or "")
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        candidate = candidate_map.get(symbol)
        traces.append(
            {
                "symbol": symbol,
                "name": "unknown",
                "theme": str(getattr(candidate, "theme_name", "") or "unknown"),
                "source": list(_candidate_trace_source(candidate)),
                "buy_point": _buy_point_code(candidate) or "unknown",
                "key_metrics": _candidate_metric_values_dict(candidate),
                "pass_or_block": _candidate_pass_or_block(candidate, view),
                "reason": list(_candidate_trace_reasons(candidate, view)),
            }
        )
        if len(traces) >= limit:
            return tuple(traces)
    for candidate in final_candidates:
        if candidate.symbol in seen:
            continue
        traces.append(
            {
                "symbol": candidate.symbol,
                "name": "unknown",
                "theme": candidate.theme_name or "unknown",
                "source": list(_candidate_trace_source(candidate)),
                "buy_point": _buy_point_code(candidate) or "unknown",
                "key_metrics": _candidate_metric_values_dict(candidate),
                "pass_or_block": _candidate_pass_or_block(candidate),
                "reason": list(_candidate_trace_reasons(candidate)),
            }
        )
        if len(traces) >= limit:
            break
    return tuple(traces)


def _merge_hypothesis_funnel_debug(
    decision_bundle: DecisionBundle,
    *,
    final_candidates: tuple[FinalCandidateDecision, ...],
    candidate_slice,
    candidate_funnel_summary: CandidateFunnelSummary,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    summary = dict(getattr(decision_bundle, "funnel_summary", {}) or {})
    summary.update(
        {
            "global": int(candidate_funnel_summary.final_count or 0),
            "executable": int(candidate_funnel_summary.primary_count or 0),
            "profit_center": int(candidate_funnel_summary.primary_count or 0),
            "backup_watch": int((candidate_funnel_summary.watch_count or 0) + (candidate_funnel_summary.inactive_count or 0)),
            "blocked": int(candidate_funnel_summary.blocked_count or 0),
            "invalidated": int(summary.get("invalidated", 0) or 0),
        }
    )
    gate_items = tuple(candidate_funnel_summary.gate_reason_counts or ())
    blocked_by_buy_point = 0
    blocked_by_data = 0
    blocked_by_theme = 0
    for raw in gate_items:
        text = str(raw or "")
        if ":" not in text:
            continue
        code, count_text = text.split(":", 1)
        try:
            count = int(float(count_text))
        except (TypeError, ValueError):
            count = 0
        if code in {"amount_not_ready", "shape_not_ready", "high_open_chase", "near_limit_non_leader", "buy_point_avoid_chase", "rotation_wait_confirm"}:
            blocked_by_buy_point += count
        if code in {"quote_stale", "data_missing", "amount_missing"}:
            blocked_by_data += count
        if code in {"market_not_probe", "theme_not_allowed", "hot_theme_hard_risk"}:
            blocked_by_theme += count
    summary["blocked_by_buy_point"] = int(summary.get("blocked_by_buy_point", 0) or 0) + blocked_by_buy_point
    summary["blocked_by_data"] = int(summary.get("blocked_by_data", 0) or 0) + blocked_by_data
    summary["blocked_by_theme"] = int(summary.get("blocked_by_theme", 0) or 0) + blocked_by_theme
    local_traces = tuple(getattr(decision_bundle, "funnel_traces", ()) or ())
    hypothesis_traces = _build_hypothesis_funnel_traces(
        final_candidates=final_candidates,
        candidate_slice=candidate_slice,
    )
    merged: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for item in (*hypothesis_traces, *local_traces):
        symbol = str(item.get("symbol", "") if isinstance(item, dict) else "")
        status = str(item.get("pass_or_block", "") if isinstance(item, dict) else "")
        key = f"{symbol}:{status}"
        if not symbol or key in seen_keys:
            continue
        seen_keys.add(key)
        merged.append(item)
        if len(merged) >= 20:
            break
    return summary, tuple(merged)


def _battle_mode_from_buy_points(
    *,
    market_script: str,
    primary_buy_points: tuple[str, ...],
    filtered_buy_points: dict[str, int],
    risk_tags: tuple[str, ...] = (),
    focus_stress_state: str,
    temporal_battlefield_state: str,
) -> str:
    if market_script == "risk_off":
        return "risk_defense"
    if focus_stress_state == "market_risk_spread" and not primary_buy_points:
        return "risk_defense"
    if "hot_plate_overheat_watch" in risk_tags:
        return "anti_chase_watch"
    if not primary_buy_points:
        if filtered_buy_points.get("avoid_chase", 0) or filtered_buy_points.get("strength_only", 0):
            return "anti_chase_watch"
        return "wait_confirm"
    if any(code in {"rotation_first_confirm"} for code in primary_buy_points) or temporal_battlefield_state == "handoff_confirmed":
        return "rotation_confirm"
    if any(code in {"dragon_divergence", "yest_core_relay"} for code in primary_buy_points):
        return "leader_core"
    if any(code in {"turnover_confirm", "front_turnover"} for code in primary_buy_points):
        return "front_turnover"
    if any(code in {"halfway_momentum"} for code in primary_buy_points):
        return "halfway_attack"
    if any(code in {"same_theme_arbitrage"} for code in primary_buy_points):
        return "same_theme_arbitrage"
    if any(code in {"capacity_trend_support", "mid_trend_support", "trend_pullback_support"} for code in primary_buy_points):
        return "capacity_trend"
    if any(code in {"chip_breakout_support", "capital_repair_support"} for code in primary_buy_points):
        return "repair_attack"
    if any(code in {"low_open_repair", "index_rebound_resonance"} for code in primary_buy_points):
        return "repair_attack"
    return "selective_probe"


def _tactic_family_from_battle_mode(mode: str) -> str:
    return {
        "leader_core": "core_confirm",
        "front_turnover": "core_confirm",
        "halfway_attack": "core_confirm",
        "repair_attack": "risk_release",
        "capacity_trend": "risk_release",
        "rotation_confirm": "capital_rotation",
        "same_theme_arbitrage": "capital_overflow",
        "anti_chase_watch": "defense_wait",
        "risk_defense": "defense_wait",
        "wait_confirm": "defense_wait",
        "selective_probe": "selective_probe",
    }.get(str(mode or "").strip(), "defense_wait")


def _is_style_risk_theme(theme_name: str) -> bool:
    normalized = str(theme_name or "").strip().upper()
    return normalized in {"ST", "*ST", "ST板块", "ST摘帽"}


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
    needs_theme = hypothesis.script in {"mainline_extension", "capital_rotation", "fakeout_pulse"}
    if theme is None and needs_theme:
        missing.append("theme_local_decision")
    elif theme is not None:
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
    if hypothesis.script == "focus_asset_stress":
        passed.append("focus_asset_stress")
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


def _metric_text_value(metrics: tuple[LocalMetric, ...], name: str, default: str = "") -> str:
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
            theme_name = _metric_text_value(bridge.metrics, "theme") if bridge is not None else ""
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
    focus_stress: FocusAssetStressDecision | None = None,
    theme_relative_trace: DecisionTrace | None = None,
    relative_risk_themes: tuple[str, ...] = (),
    temporal: TemporalMigrationDecision | None = None,
    temporal_trace: DecisionTrace | None = None,
    temporal_targets: tuple[str, ...] = (),
    temporal_fading: tuple[str, ...] = (),
    hot_anchor: HotPlateAnchorDecision | None = None,
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
            "local_pack_high_pressure_repair",
        }:
            confirmed_attack.append(hypothesis)
        elif validation.result == "confirmed" and hypothesis.script == "local_pack_theme_opportunity":
            partial_watch.append(hypothesis)
        elif validation.result in {"partial", "pending"}:
            partial_watch.append(hypothesis)
        elif validation.result == "rejected":
            rejected_risk.append(hypothesis)

    hypothesis_main_attack = confirmed_attack[0].scope if confirmed_attack else ""
    pressure_repair_attack = any(item.script == "local_pack_high_pressure_repair" for item in confirmed_attack)
    hypothesis_secondary = tuple(item.scope for item in confirmed_attack[1:3] if item.scope and item.scope != "market")
    hypothesis_watch = tuple(item.scope for item in partial_watch[:5] if item.scope and item.scope != "market")
    main_attack = hypothesis_main_attack
    secondary = hypothesis_secondary
    watch = hypothesis_watch
    avoid = tuple(item.scope for item in rejected_risk[:5] if item.scope and item.scope != "market")
    market_script = "observe"
    action_hint = "watch"
    position_cap = 0.0
    reason_codes: tuple[str, ...] = ("no_confirmed_hypothesis",)
    risk_tags: tuple[str, ...] = ()
    migrating_in = tuple(getattr(context.market_summary, "migrating_in_plates", ()) or ())
    migrating_out = tuple(getattr(context.market_summary, "migrating_out_plates", ()) or ())
    market_rising_count = int(getattr(context.market_summary, "market_rising_count", 0) or 0)
    market_falling_count = int(getattr(context.market_summary, "market_falling_count", 0) or 0)
    market_rising_rate = float(getattr(context.market_summary, "market_rising_rate", 0.0) or 0.0)
    market_breadth_ratio = float(getattr(context.market_summary, "market_breadth_ratio", 0.0) or 0.0)
    market_big_rise_count = int(getattr(context.market_summary, "market_big_rise_count", 0) or 0)
    market_strong_rise_count = int(getattr(context.market_summary, "market_strong_rise_count", 0) or 0)
    market_slight_rise_count = int(getattr(context.market_summary, "market_slight_rise_count", 0) or 0)
    market_slight_fall_count = int(getattr(context.market_summary, "market_slight_fall_count", 0) or 0)
    market_strong_fall_count = int(getattr(context.market_summary, "market_strong_fall_count", 0) or 0)
    market_big_fall_count = int(getattr(context.market_summary, "market_big_fall_count", 0) or 0)
    market_strong_weak_ratio = float(getattr(context.market_summary, "market_strong_weak_ratio", 0.0) or 0.0)
    top_turnover_strong_count = int(getattr(context.market_summary, "top_turnover_strong_count", 0) or 0)
    top_turnover_weak_count = int(getattr(context.market_summary, "top_turnover_weak_count", 0) or 0)
    top_turnover_strong_weak_ratio = float(getattr(context.market_summary, "top_turnover_strong_weak_ratio", 0.0) or 0.0)
    market_bucket_sample_count = max(1, market_rising_count + market_falling_count)
    market_downside_pressure_rate = (market_strong_fall_count + market_big_fall_count) / market_bucket_sample_count
    market_breadth_weak = bool(
        market_rising_count + market_falling_count > 0
        and (
            market_rising_rate <= 0.38
            or (0.0 < market_breadth_ratio <= 0.65)
            or market_downside_pressure_rate >= 0.32
            or (0.0 < market_strong_weak_ratio <= 0.60)
            or (top_turnover_strong_count + top_turnover_weak_count > 0 and top_turnover_strong_weak_ratio <= 0.70)
        )
    )
    market_pct_bucket_text = (
        f"{market_big_rise_count}/"
        f"{market_strong_rise_count}/"
        f"{market_slight_rise_count}/"
        f"{market_slight_fall_count}/"
        f"{market_strong_fall_count}/"
        f"{market_big_fall_count}"
    )
    market_breadth_strong = bool(
        market_rising_count + market_falling_count > 0
        and (
            market_rising_rate >= 0.58
            or market_breadth_ratio >= 1.35
            or market_strong_weak_ratio >= 1.50
            or top_turnover_strong_weak_ratio >= 1.50
        )
    )
    hot_primary = hot_anchor.primary_themes if hot_anchor is not None else ()
    hot_fading = hot_anchor.fading_themes if hot_anchor is not None else ()
    hot_fakeout = hot_anchor.fakeout_themes if hot_anchor is not None else ()
    hot_trace = hot_anchor.trace if hot_anchor is not None else None
    relative_trace = theme_relative_trace
    hot_main_attack = next(
        (
            theme
            for theme in hot_primary
            if theme
            and not _is_style_risk_theme(theme)
            and theme not in hot_fading
            and theme not in hot_fakeout
        ),
        "",
    )
    hot_secondary = tuple(theme for theme in hot_primary if theme and theme != hot_main_attack)[:3]
    temporal_main_attack = next(
        (
            theme
            for theme in temporal_targets
            if theme and not _is_style_risk_theme(theme) and (theme == hot_main_attack or theme in hot_primary)
        ),
        "",
    )
    temporal_battlefield_theme = str(getattr(temporal, "main_battlefield_theme", "") or "") if temporal is not None else ""
    temporal_battlefield_state = str(getattr(temporal, "battlefield_state", "") or "") if temporal is not None else ""
    temporal_handoff_to = str(getattr(temporal, "handoff_to", "") or "") if temporal is not None else ""
    temporal_handoff_from = str(getattr(temporal, "handoff_from", "") or "") if temporal is not None else ""
    temporal_battlefield_state_raw = temporal_battlefield_state
    temporal_battlefield_state = _effective_temporal_battlefield_state(temporal, temporal_battlefield_state)
    battlefield_main_attack = next(
        (
            theme
            for theme in (
                temporal_battlefield_theme,
                temporal_handoff_to,
                hot_main_attack,
                temporal_main_attack,
            )
            if theme and not _is_style_risk_theme(theme)
        ),
        "",
    )
    main_attack = hot_main_attack or ""
    if temporal_battlefield_state == "handoff_confirmed" and battlefield_main_attack:
        if battlefield_main_attack in hot_primary or battlefield_main_attack in temporal_targets:
            main_attack = battlefield_main_attack
    elif temporal_battlefield_state == "extend" and battlefield_main_attack:
        if battlefield_main_attack in hot_primary or battlefield_main_attack == hot_main_attack:
            main_attack = battlefield_main_attack
    if not main_attack and temporal_main_attack:
        main_attack = temporal_main_attack
    if not main_attack and hypothesis_main_attack:
        main_attack = hypothesis_main_attack

    if _is_style_risk_theme(main_attack):
        watch = tuple(dict.fromkeys((main_attack, *watch, *hot_primary)))[:4]
        main_attack = ""
    if main_attack:
        secondary_items: list[str] = []
        if (
            temporal_battlefield_state == "handoff_confirmed"
            and temporal_handoff_to
            and temporal_handoff_to != main_attack
        ):
            secondary_items.append(temporal_handoff_to)
        secondary_items.extend(theme for theme in hot_secondary if theme and theme != main_attack)
        secondary_items.extend(theme for theme in hypothesis_secondary if theme and theme != main_attack)
        if temporal_battlefield_state in {"extend", "handoff_confirmed"}:
            secondary_items.extend(theme for theme in temporal_targets if theme and theme != main_attack)
        secondary = tuple(dict.fromkeys(secondary_items))[:3]
        watch_items: list[str] = []
        if (
            temporal_battlefield_state == "handoff_attempt"
            and temporal_handoff_to
            and temporal_handoff_to != main_attack
            and temporal_handoff_to not in secondary
        ):
            watch_items.append(temporal_handoff_to)
        watch_items.extend(theme for theme in hypothesis_watch if theme and theme != main_attack and theme not in secondary)
        watch_items.extend(theme for theme in hot_primary if theme and theme != main_attack and theme not in secondary)
        watch_items.extend(
            item.scope
            for item in partial_watch[:5]
            if item.scope and item.scope != "market" and item.scope != main_attack and item.scope not in secondary
        )
        watch = tuple(dict.fromkeys(watch_items))[:4]
    elif not secondary and hot_primary:
        secondary = tuple(theme for theme in hot_primary if theme)[:3]
    elif not watch and hot_primary:
        watch = tuple(theme for theme in hot_primary[:4] if theme)
    focus_stress_trace = focus_stress.trace if focus_stress is not None else None
    focus_stress_state = focus_stress.stress_state if focus_stress is not None else "unknown"
    focus_stress_spread = focus_stress.spread_level if focus_stress is not None else "unknown"
    focus_stress_themes = focus_stress.stressed_themes if focus_stress is not None else ()
    dragon_alone_themes = focus_stress.dragon_alone_themes if focus_stress is not None else ()
    focus_stress_risk_count = len(focus_stress_themes)
    main_attack_hot_hard_risk = _hot_theme_is_hard_risk(main_attack, hot_anchor)
    main_attack_index_defense = _hot_theme_is_index_defense(main_attack, hot_anchor)
    main_attack_has_process_support = bool(
        main_attack
        and (main_attack in temporal_targets or main_attack in migrating_in or main_attack in hot_primary)
    )
    main_attack_market_risk_core_ready = bool(
        main_attack
        and main_attack_has_process_support
        and main_attack in hot_primary
        and main_attack not in dragon_alone_themes
        and not main_attack_hot_hard_risk
        and not main_attack_index_defense
    )
    hot_anchor_first = bool(
        hot_anchor is not None
        and hot_anchor.anchor_state in {"hot_rotation", "hot_continuation", "hot_probe"}
        and bool(main_attack)
        and not hypothesis_main_attack
    )
    if main_attack:
        market_script = "hot_risk_validation" if main_attack_hot_hard_risk else "attack_confirmed"
        action_hint = "watch" if main_attack_hot_hard_risk else "probe"
        position_cap = 0.05 if main_attack_hot_hard_risk else (0.12 if pressure_repair_attack else (0.2 if high_focus_state == "negative" else 0.35))
        reason_list = ["validated_attack_hypothesis"]
        if hot_anchor_first:
            reason_list = ["hot_plate_first_anchor"]
            market_script = "watch_validation"
            action_hint = "watch"
            position_cap = 0.1
            if hot_anchor is not None and hot_anchor.anchor_state in {"hot_rotation", "hot_continuation"}:
                market_script = "attack_confirmed"
                action_hint = "probe"
                position_cap = 0.12 if high_focus_state == "negative" else 0.18
        risk_list: list[str] = []
        if main_attack_hot_hard_risk:
            reason_list.append("hot_plate_hard_risk_validation")
            risk_list.append("hot_plate_hard_risk")
        if main_attack_index_defense:
            reason_list.append("index_defense_hot_plate_validation")
            risk_list.append("index_defense_hot_plate")
            market_script = "watch_validation"
            action_hint = "watch"
            position_cap = min(position_cap, 0.05)
        if pressure_repair_attack:
            reason_list.append("pressure_repair_probe")
            risk_list.append("risk_capped_pressure_repair")
        if main_attack in migrating_in:
            reason_list.append("sector_flow_migrating_in")
        if main_attack in temporal_targets:
            reason_list.append("timeframe_chain_confirmed")
        if main_attack in hot_primary:
            reason_list.append("hot_plate_anchor")
        if main_attack and main_attack == temporal_battlefield_theme:
            reason_list.append("battlefield_anchor")
        if (
            main_attack
            and main_attack == temporal_handoff_to
            and temporal_handoff_from
            and temporal_battlefield_state == "handoff_confirmed"
        ):
            reason_list.append("hot_handoff_target")
        elif temporal_battlefield_state_raw == "handoff_confirmed" and temporal_battlefield_state != "handoff_confirmed":
            reason_list.append("handoff_confirmation_downgraded")
        if main_attack in migrating_out:
            risk_list.append("sector_flow_migrating_out")
        if main_attack in temporal_fading and main_attack not in temporal_targets:
            risk_list.append("timeframe_chain_fading")
            if temporal_battlefield_state != "handoff_confirmed":
                market_script = "pressure_validation"
                action_hint = "watch"
                position_cap = min(position_cap, 0.08)
                reason_list.append("timeframe_chain_fading_pressure")
        if main_attack in hot_fading or main_attack in hot_fakeout:
            risk_list.append("hot_plate_anchor_risk")
        if main_attack in relative_risk_themes:
            risk_list.append("relative_risk_theme")
        if main_attack in focus_stress_themes:
            reason_list.append("focus_asset_stress_theme")
            risk_list.append("focus_asset_stress")
            position_cap = min(position_cap, 0.12)
        if main_attack in dragon_alone_themes:
            reason_list.append("dragon_alone_only")
            risk_list.append("dragon_alone_risk")
            position_cap = min(position_cap, 0.08)
            action_hint = "watch"
        if focus_stress_state == "market_risk_spread":
            if main_attack_market_risk_core_ready:
                market_script = "attack_confirmed"
                action_hint = "probe"
                position_cap = min(position_cap, 0.08)
                reason_list.append("focus_asset_market_risk_core_probe")
            else:
                market_script = "pressure_validation" if main_attack_has_process_support else "risk_validation"
                action_hint = "watch"
                position_cap = min(position_cap, 0.08 if main_attack_has_process_support else 0.05)
                reason_list.append("focus_asset_market_risk_spread")
            risk_list.append("focus_asset_market_risk")
        elif focus_stress_state == "theme_pressure" and main_attack in focus_stress_themes:
            market_script = "pressure_validation"
            action_hint = "watch"
            position_cap = min(position_cap, 0.08)
            reason_list.append("focus_asset_theme_pressure")
        if market_breadth_weak and not pressure_repair_attack:
            position_cap = min(position_cap, 0.12)
            reason_list.append("market_breadth_weak")
            risk_list.append("market_breadth_weak")
        elif market_breadth_strong:
            reason_list.append("market_breadth_support")
        reason_codes = tuple(reason_list)
        risk_tags = tuple(risk_list)
    elif focus_stress_state == "market_risk_spread":
        market_script = "risk_off"
        action_hint = "avoid_chase"
        position_cap = 0.0
        reason_codes = ("focus_asset_market_risk_spread",)
        risk_tags = ("focus_asset_market_risk",)
    elif high_focus_state == "negative" and not hot_primary:
        market_script = "risk_off"
        action_hint = "avoid_chase"
        position_cap = 0.0
        reason_codes = ("high_focus_negative",)
        risk_tags = ("high_focus_risk",)
    elif watch:
        market_script = "watch_validation"
        action_hint = "watch"
        position_cap = 0.1
        reason_codes = ("hot_plate_watch_validation",) if hot_primary else ("partial_hypothesis",)
    elif relative_risk_themes:
        market_script = "watch_validation"
        action_hint = "watch"
        position_cap = 0.05
        reason_codes = ("relative_risk_watch_validation",)
        risk_tags = ("theme_relative_risk",)

    lower_refs = tuple(
        ref
        for hypothesis in hypotheses[:4]
        for ref in (hypothesis.hypothesis_id,)
        if ref
    )
    if theme_relative_trace is not None:
        lower_refs = (theme_relative_trace.decision_id, *lower_refs)
    if temporal_trace is not None:
        lower_refs = (temporal_trace.decision_id, *lower_refs)
    if hot_anchor is not None:
        lower_refs = (hot_anchor.trace.decision_id, *lower_refs)
    if focus_stress is not None:
        lower_refs = (focus_stress.trace.decision_id, *lower_refs)
    invalidation_points: list[str] = []
    if main_attack:
        invalidation_points.extend(("confirmed_theme_fades", "risk_spread_expands"))
    if "focus_asset_market_risk" in risk_tags:
        invalidation_points.extend(("risk_spread_recedes", "front_row_reclaims", "mid_core_reclaims"))
    if "focus_asset_stress" in risk_tags or "dragon_alone_risk" in risk_tags:
        invalidation_points.extend(("leader_repair", "mid_core_reclaims"))
    if "timeframe_chain_fading" in risk_tags:
        invalidation_points.extend(("front_row_reclaims", "spread_expands"))
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
        invalidation_points=tuple(dict.fromkeys(invalidation_points)),
        metrics=(
            f"confirmed_attack_count={len(confirmed_attack)}",
            f"partial_watch_count={len(partial_watch)}",
            f"rejected_risk_count={len(rejected_risk)}",
            f"hot_primary_count={len(hot_primary)}",
            f"focus_stress_theme_count={focus_stress_risk_count}",
            f"market_breadth={market_rising_count}/{market_falling_count}/{market_rising_rate:.4f}",
            f"market_pct_bucket={market_pct_bucket_text}",
            f"market_downside_pressure_rate={market_downside_pressure_rate:.4f}",
            f"market_strong_weak_ratio={market_strong_weak_ratio:.3f}",
            f"top_turnover_sw={top_turnover_strong_count}/{top_turnover_weak_count}/{top_turnover_strong_weak_ratio:.3f}",
            f"position_cap={position_cap:.2f}",
        ),
        metric_values=(
            ("confirmed_attack_count", float(len(confirmed_attack))),
            ("partial_watch_count", float(len(partial_watch))),
            ("rejected_risk_count", float(len(rejected_risk))),
            ("hot_primary_count", _metric_value(hot_trace, "primary_theme_count", float(len(hot_primary)))),
            ("hot_risk_count", float(len(hot_fading) + len(hot_fakeout))),
            ("temporal_target_count", float(len(temporal_targets))),
            ("temporal_fading_count", float(len(temporal_fading))),
            ("focus_stress_theme_count", float(focus_stress_risk_count)),
            ("focus_stress_dragon_alone_count", float(len(dragon_alone_themes))),
            ("market_rising_count", float(market_rising_count)),
            ("market_falling_count", float(market_falling_count)),
            ("market_rising_rate", float(market_rising_rate)),
            ("market_breadth_ratio", float(market_breadth_ratio)),
            ("market_downside_pressure_rate", float(market_downside_pressure_rate)),
            ("market_strong_weak_ratio", float(market_strong_weak_ratio)),
            ("position_cap", float(position_cap)),
        ),
        evidence_summary=(
            f"confirmed_attack={len(confirmed_attack)}",
            f"partial_watch={len(partial_watch)}",
            f"rejected_risk={len(rejected_risk)}",
            f"high_focus={high_focus_state}",
            f"pressure_repair={pressure_repair_attack}",
            f"focus_stress={focus_stress_state}",
            f"focus_spread={focus_stress_spread}",
            f"migrating_in={','.join(migrating_in[:3]) or '-'}",
            f"migrating_out={','.join(migrating_out[:3]) or '-'}",
            f"relative_risk={','.join(relative_risk_themes[:3]) or '-'}",
            f"temporal_targets={','.join(temporal_targets[:3]) or '-'}",
            f"temporal_fading={','.join(temporal_fading[:3]) or '-'}",
            f"battlefield={temporal_battlefield_theme or '-'}",
            f"battlefield_state={temporal_battlefield_state or '-'}",
            f"handoff={(temporal_handoff_from or '-') + '->' + (temporal_handoff_to or '-')}",
            f"hot_anchor={hot_anchor.anchor_state if hot_anchor is not None else '-'}",
            f"hot_primary={','.join(hot_primary[:3]) or '-'}",
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
    local_candidate_feeds: tuple[LocalStrategyCandidateRow, ...] = (),
) -> tuple[FinalCandidateDecision, ...]:
    theme_relative = decision_bundle.theme_relative_decision
    mainline_order = theme_relative.mainline_candidates if theme_relative is not None else ()
    rotation_order = theme_relative.rotation_candidates if theme_relative is not None else ()
    risk_themes = theme_relative.risk_themes if theme_relative is not None else ()
    pack = decision_bundle.local_strategy_evidence_pack
    temporal = decision_bundle.temporal_migration_decision
    hot_anchor = decision_bundle.hot_plate_anchor_decision
    focus_stress = decision_bundle.focus_asset_stress_decision
    local_pack_themes = tuple(summary.scope for summary in pack.theme_opportunities) if pack is not None else ()
    local_pack_risk_themes = tuple(summary.scope for summary in pack.theme_risks) if pack is not None else ()
    local_aligned_symbols = {summary.scope for summary in pack.stock_alignments} if pack is not None else set()
    temporal_targets = temporal.target_themes if temporal is not None else ()
    temporal_fading = temporal.fading_themes if temporal is not None else ()
    temporal_battlefield_theme = str(getattr(temporal, "main_battlefield_theme", "") or "") if temporal is not None else ""
    temporal_battlefield_state = str(getattr(temporal, "battlefield_state", "") or "") if temporal is not None else ""
    temporal_handoff_from = str(getattr(temporal, "handoff_from", "") or "") if temporal is not None else ""
    temporal_handoff_to = str(getattr(temporal, "handoff_to", "") or "") if temporal is not None else ""
    temporal_rising_hot = tuple(getattr(temporal, "rising_hot_themes", ()) or ()) if temporal is not None else ()
    temporal_handoff_evidence_count = int(_metric_value(getattr(temporal, "trace", None), "handoff_evidence_count", 0.0)) if temporal is not None else 0
    temporal_handoff_persistence_ok = int(_metric_value(getattr(temporal, "trace", None), "handoff_persistence_ok", 0.0)) if temporal is not None else 0
    temporal_battlefield_candidate_count = int(_metric_value(getattr(temporal, "trace", None), "battlefield_candidate_count", 0.0)) if temporal is not None else 0
    temporal_battlefield_state_raw = temporal_battlefield_state
    temporal_battlefield_state = _effective_temporal_battlefield_state(temporal, temporal_battlefield_state)
    allowed_strategy_ids, rejected_strategy_reasons = _local_strategy_pack_gate(
        global_decision,
        temporal=temporal,
        hot_anchor=hot_anchor,
    )
    strategy_priority = {
        "mainline_local": 0,
        "rotation_local": 1,
        "repair_local": 2,
        "trend_local": 3,
        "arbitrage_local": 4,
    }
    allowed_local_feed_rows = tuple(
        row
        for row in tuple(local_candidate_feeds or ())
        if row.symbol and row.strategy_id in allowed_strategy_ids
    )
    best_local_feed_by_symbol: dict[str, LocalStrategyCandidateRow] = {}
    for row in sorted(
        allowed_local_feed_rows,
        key=lambda item: (
            strategy_priority.get(item.strategy_id, 99),
            int(item.rank_in_strategy or 999),
            item.symbol,
        ),
    ):
        best_local_feed_by_symbol.setdefault(row.symbol, row)
    local_feed_by_symbol = {
        row.symbol: row
        for row in best_local_feed_by_symbol.values()
        if row.symbol
    }
    _log_strategy_pack_contract_audit(
        context,
        local_candidate_feeds,
        global_decision=global_decision,
        temporal=temporal,
        hot_anchor=hot_anchor,
        allowed_strategy_ids=allowed_strategy_ids,
        rejected_strategy_reasons=rejected_strategy_reasons,
    )
    logger.info(
        "strategy pack gate | phase=%s | allowed=%s | rejected=%s | feeds=%s/%s | market=%s | battlefield=%s/%s | hot=%s",
        _phase_name(context),
        ",".join(allowed_strategy_ids) or "-",
        ",".join(rejected_strategy_reasons[:5]) or "-",
        len(allowed_local_feed_rows),
        len(tuple(local_candidate_feeds or ())),
        global_decision.market_script or "-",
        temporal_battlefield_theme or "-",
        temporal_battlefield_state or "-",
        ",".join(tuple(getattr(hot_anchor, "primary_themes", ()) or ())[:3]) if hot_anchor is not None else "-",
    )

    def _feed_metric(row: LocalStrategyCandidateRow | None, name: str, default: float = 0.0) -> float:
        if row is None:
            return default
        for metric_name, value in tuple(row.metric_values or ()):
            if metric_name == name:
                return float(value)
        return default

    def _local_feed_quant_ready(row: LocalStrategyCandidateRow | None) -> bool:
        if row is None:
            return False
        tags = set(tuple(row.fact_tags or ()))
        amount_2m = _feed_metric(row, "amount_2m")
        auction_amount = _feed_metric(row, "auction_amount")
        amount_ratio = _feed_metric(row, "amount_2m_vs_auction")
        speed_1m = _feed_metric(row, "speed_1m")
        current_pct = _feed_metric(row, "current_pct")
        if amount_2m >= 20_000_000 or auction_amount >= 30_000_000:
            return True
        if amount_ratio >= 0.85 or speed_1m >= 0.006:
            return True
        if tags.intersection({"volume_expand", "open_repair", "front_row", "trend_hold", "chip_breakout_like"}):
            return current_pct < 0.095 or "front_row" in tags
        return False

    local_feed_symbols = {
        symbol
        for symbol, row in local_feed_by_symbol.items()
        if _local_feed_quant_ready(row)
    }
    hot_primary = hot_anchor.primary_themes if hot_anchor is not None else ()
    hot_continuation = hot_anchor.continuation_themes if hot_anchor is not None else ()
    hot_rotation = hot_anchor.rotation_themes if hot_anchor is not None else ()
    hot_state_map = _hot_metric_state_map(hot_anchor)
    hot_risk = (
        tuple(hot_anchor.fading_themes) + tuple(hot_anchor.fakeout_themes)
        if hot_anchor is not None
        else ()
    )
    focus_stress_state = focus_stress.stress_state if focus_stress is not None else "unknown"
    focus_stress_themes = focus_stress.stressed_themes if focus_stress is not None else ()
    dragon_alone_themes = focus_stress.dragon_alone_themes if focus_stress is not None else ()
    core_target_theme_items: list[str] = []
    if temporal_battlefield_state == "extend":
        if temporal_battlefield_theme and not _is_style_risk_theme(temporal_battlefield_theme):
            core_target_theme_items.append(temporal_battlefield_theme)
    elif temporal_battlefield_state == "handoff_confirmed":
        if temporal_battlefield_theme and not _is_style_risk_theme(temporal_battlefield_theme):
            core_target_theme_items.append(temporal_battlefield_theme)
        if temporal_handoff_to and not _is_style_risk_theme(temporal_handoff_to):
            core_target_theme_items.append(temporal_handoff_to)
        core_target_theme_items.extend(temporal_rising_hot)
    core_target_theme_items.extend(hot_primary)
    if global_decision.main_attack_theme and not _is_style_risk_theme(global_decision.main_attack_theme):
        core_target_theme_items.append(global_decision.main_attack_theme)
    core_target_theme_items.extend(global_decision.secondary_themes)
    watch_target_theme_items: list[str] = []
    watch_target_theme_items.extend(hot_continuation)
    watch_target_theme_items.extend(hot_rotation)
    watch_target_theme_items.extend(global_decision.watch_themes)
    if temporal_battlefield_state == "handoff_attempt" and temporal_handoff_to:
        watch_target_theme_items.append(temporal_handoff_to)
    fallback_target_theme_items: list[str] = []
    fallback_target_theme_items.extend(mainline_order)
    fallback_target_theme_items.extend(rotation_order)
    if temporal_battlefield_state in {"extend", "handoff_confirmed"}:
        fallback_target_theme_items.extend(temporal_targets)
    fallback_target_theme_items.extend(local_pack_themes)
    target_themes = tuple(
        dict.fromkeys(theme for theme in (*core_target_theme_items, *watch_target_theme_items) if theme)
    )
    fallback_target_themes = tuple(
        dict.fromkeys(theme for theme in (*target_themes, *fallback_target_theme_items) if theme)
    )
    if temporal_battlefield_state == "extend":
        main_attack_theme = temporal_battlefield_theme or global_decision.main_attack_theme or ""
    elif temporal_battlefield_state == "handoff_confirmed":
        main_attack_theme = temporal_handoff_to or temporal_battlefield_theme or global_decision.main_attack_theme or ""
    else:
        main_attack_theme = global_decision.main_attack_theme or ""
    if _is_style_risk_theme(main_attack_theme):
        main_attack_theme = global_decision.main_attack_theme if not _is_style_risk_theme(global_decision.main_attack_theme) else ""
    secondary_theme_set = set(global_decision.secondary_themes)
    watch_theme_set = set(global_decision.watch_themes)
    battlefield_theme_set = (
        {
            theme
            for theme in (
                temporal_battlefield_theme,
                temporal_handoff_to if temporal_battlefield_state == "handoff_confirmed" else "",
            )
            if theme and not _is_style_risk_theme(theme)
        }
        if temporal_battlefield_state in {"extend", "handoff_confirmed"}
        else set()
    )
    stock_local_all = tuple(decision_bundle.stock_local_decisions)
    stock_local_candidate_count = sum(1 for decision in stock_local_all if decision.trace.state == "candidate")
    stock_local_front_count = sum(
        1 for decision in stock_local_all if decision.role_hint in {"true_leader", "front_row"}
    )
    stock_local_mid_count = sum(1 for decision in stock_local_all if decision.role_hint == "mid_follow")
    if not target_themes and not fallback_target_themes:
        target_themes = tuple(
            dict.fromkeys(
                decision.theme_name
                for decision in decision_bundle.stock_local_decisions
                if decision.theme_name
                and decision.role_hint in {"true_leader", "front_row"}
                and decision.trace.state == "candidate"
            )
        )[:8]
        fallback_target_themes = target_themes
    if not target_themes and not fallback_target_themes:
        target_themes = tuple(
            dict.fromkeys(
                decision.theme_name
                for decision in decision_bundle.stock_local_decisions
                if decision.theme_name and decision.role_hint in {"true_leader", "front_row"}
            )
        )[:8]
        fallback_target_themes = target_themes
    if not target_themes and not fallback_target_themes:
        logger.info(
            "final candidate debug | phase=%s | target_themes=0 | stock_local=%s | candidate=%s | front=%s | mid=%s | hot_primary=%s | temporal_targets=%s | local_pack_themes=%s",
            _phase_name(context),
            len(stock_local_all),
            stock_local_candidate_count,
            stock_local_front_count,
            stock_local_mid_count,
            len(hot_primary),
            len(temporal_targets),
            len(local_pack_themes),
        )
        return ()
    raw_candidates_strict: list[StockLocalDecision] = [
        decision
        for decision in decision_bundle.stock_local_decisions
        if decision.theme_name in target_themes
        and (decision.trace.state == "candidate" or decision.symbol in local_aligned_symbols)
        and decision.role_hint in {"true_leader", "front_row"}
        and decision.entry_behavior not in {"high_open_distribution", "weak_follow"}
    ]
    raw_candidates = raw_candidates_strict
    raw_candidates_relaxed_front: list[StockLocalDecision] = []
    raw_candidates_candidate_any: list[StockLocalDecision] = []
    raw_candidates_relaxed_mid: list[StockLocalDecision] = []
    raw_candidates_mid_supplement: list[StockLocalDecision] = []
    raw_candidates_repair_supplement: list[StockLocalDecision] = []
    raw_candidates_local_feed_supplement: list[StockLocalDecision] = []
    if not raw_candidates:
        raw_candidates_relaxed_front = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if decision.theme_name in target_themes
            and decision.role_hint in {"true_leader", "front_row"}
            and decision.entry_behavior != "high_open_distribution"
        ]
        raw_candidates = raw_candidates_relaxed_front
    if not raw_candidates and fallback_target_themes:
        raw_candidates_candidate_any = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if decision.trace.state == "candidate"
            and decision.theme_name in fallback_target_themes
            and decision.role_hint in {"true_leader", "front_row"}
            and decision.entry_behavior != "high_open_distribution"
        ]
        raw_candidates = raw_candidates_candidate_any
    if not raw_candidates and not fallback_target_themes:
        raw_candidates_candidate_any = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if decision.trace.state == "candidate"
            and decision.role_hint in {"true_leader", "front_row"}
            and decision.entry_behavior != "high_open_distribution"
        ]
        raw_candidates = raw_candidates_candidate_any
    if fallback_target_themes:
        existing_symbols = {decision.symbol for decision in raw_candidates}
        raw_candidates_local_feed_supplement = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if decision.symbol not in existing_symbols
            and decision.symbol in local_feed_symbols
            and decision.theme_name in fallback_target_themes
            and decision.role_hint in {"true_leader", "front_row", "mid_follow"}
            and decision.entry_behavior not in {"high_open_distribution", "weak_follow"}
            and (
                _metric_value(decision.trace, "current_pct") < 0.095
                or decision.role_hint == "true_leader"
            )
        ][:4]
        if raw_candidates_local_feed_supplement:
            raw_candidates = [*raw_candidates, *raw_candidates_local_feed_supplement]
    if not raw_candidates:
        raw_candidates_relaxed_mid = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if (
                decision.theme_name in fallback_target_themes
                or decision.symbol in local_aligned_symbols
                or (not fallback_target_themes and decision.trace.state == "candidate")
            )
            and decision.role_hint in {"true_leader", "front_row", "mid_follow"}
            and decision.entry_behavior not in {"high_open_distribution", "weak_follow"}
        ]
        raw_candidates = raw_candidates_relaxed_mid
    if raw_candidates and global_decision.market_script == "attack_confirmed":
        existing_symbols = {decision.symbol for decision in raw_candidates}
        mid_supplement_theme_set = {
            theme
            for theme in (
                main_attack_theme,
                *battlefield_theme_set,
                *hot_primary,
            )
            if theme and not _is_style_risk_theme(theme)
        }
        raw_candidates_mid_supplement = [
            decision
            for decision in decision_bundle.stock_local_decisions
            if decision.symbol not in existing_symbols
            and decision.trace.state == "candidate"
            and decision.theme_name in mid_supplement_theme_set
            and decision.role_hint == "mid_follow"
            and decision.entry_behavior in {"volume_confirm", "confirmed", "repair_strength", "low_open_repair"}
        ][:3]
        if raw_candidates_mid_supplement:
            raw_candidates = [*raw_candidates, *raw_candidates_mid_supplement]
    existing_symbols = {decision.symbol for decision in raw_candidates}
    repair_supplement_theme_set = {
        theme
        for theme in (
            main_attack_theme,
            *battlefield_theme_set,
            *hot_primary,
            *global_decision.secondary_themes,
            *global_decision.watch_themes,
        )
        if theme and not _is_style_risk_theme(theme)
    }
    raw_candidates_repair_supplement = [
        decision
        for decision in decision_bundle.stock_local_decisions
        if decision.symbol not in existing_symbols
        and decision.trace.state == "candidate"
        and (
            decision.theme_name in repair_supplement_theme_set
            or decision.symbol in local_aligned_symbols
        )
        and decision.role_hint in {"true_leader", "front_row", "mid_follow"}
        and decision.entry_behavior in {"low_open_repair", "repair_strength", "volume_confirm", "confirmed"}
        and (
            _metric_value(decision.trace, "amount_2m") >= 12_000_000
            or _metric_value(decision.trace, "speed_1m") >= 0.006
            or _metric_value(decision.trace, "auction_amount") >= 15_000_000
        )
        and (
            _metric_value(decision.trace, "current_pct") < 0.095
            or decision.role_hint == "true_leader"
        )
    ][:4]
    if raw_candidates_repair_supplement:
        raw_candidates = [*raw_candidates, *raw_candidates_repair_supplement]
    mid_supplement_themes = {decision.theme_name for decision in raw_candidates_mid_supplement}
    raw_main_attack = [
        decision
        for decision in raw_candidates
        if main_attack_theme and decision.theme_name == main_attack_theme
    ]
    if main_attack_theme:
        main_theme_all = [
            decision
            for decision in stock_local_all
            if decision.theme_name == main_attack_theme
        ]
        main_theme_candidates = [
            decision
            for decision in main_theme_all
            if decision.trace.state == "candidate"
        ]
        main_theme_front_candidates = [
            decision
            for decision in main_theme_candidates
            if decision.role_hint in {"true_leader", "front_row"}
        ]
        main_theme_mid_candidates = [
            decision
            for decision in main_theme_candidates
            if decision.role_hint == "mid_follow"
        ]

        def _mainline_sample(rows: list[StockLocalDecision]) -> str:
            ordered = sorted(
                rows,
                key=lambda item: (
                    item.trace.state != "candidate",
                    item.role_hint not in {"true_leader", "front_row"},
                    -_metric_value(item.trace, "amount_2m"),
                    -_metric_value(item.trace, "auction_amount"),
                    item.symbol,
                ),
            )
            parts: list[str] = []
            for item in ordered[:5]:
                parts.append(
                    (
                        f"{item.symbol}:{item.trace.state}/{item.role_hint}/{item.entry_behavior}"
                        f"/2m={_metric_value(item.trace, 'amount_2m'):.0f}"
                        f"/open={_metric_value(item.trace, 'open_pct'):.3f}"
                        f"/now={_metric_value(item.trace, 'current_pct'):.3f}"
                        f"/reject={item.trace.reject_reason or '-'}"
                    )
                )
            return ";".join(parts) or "-"

        logger.info(
            "mainline candidate audit | phase=%s | main=%s | local=%s | candidate=%s | front_candidate=%s | mid_candidate=%s | raw_main=%s | sample=%s",
            _phase_name(context),
            main_attack_theme,
            len(main_theme_all),
            len(main_theme_candidates),
            len(main_theme_front_candidates),
            len(main_theme_mid_candidates),
            len(raw_main_attack),
            _mainline_sample(main_theme_all),
        )
    raw_secondary = [
        decision
        for decision in raw_candidates
        if decision.theme_name in secondary_theme_set
    ]
    raw_watch = [
        decision
        for decision in raw_candidates
        if decision.theme_name in watch_theme_set
    ]
    main_attack_ready = any(
        decision.role_hint in {"true_leader", "front_row"} and decision.trace.state == "candidate"
        for decision in raw_main_attack
    )
    mainline_family_ready = bool(main_attack_ready or raw_secondary)
    focus_core_symbols = set(focus_stress.core_symbols if focus_stress is not None else ())
    market_spread_core_themes = {
        theme
        for theme in (
            main_attack_theme,
            *battlefield_theme_set,
            *hot_primary,
        )
        if theme and not _is_style_risk_theme(theme)
    }
    ranked: list[tuple[tuple[float, float, float, float, float, float, str], StockLocalDecision, str, str, str]] = []
    for decision in raw_candidates:
        bridge_signal = _bridge_signal_for_symbol(decision_bundle, decision.symbol)
        is_local_aligned = decision.symbol in local_aligned_symbols
        is_local_feed_quant = decision.symbol in local_feed_symbols
        bridge_state = bridge_signal.state if bridge_signal is not None else ""
        theme_tier = 3
        if decision.theme_name in battlefield_theme_set:
            theme_tier = 0
        elif main_attack_theme and decision.theme_name == main_attack_theme:
            theme_tier = 0
        elif decision.theme_name in secondary_theme_set:
            theme_tier = 1
        elif decision.theme_name in watch_theme_set:
            theme_tier = 2
        hot_hard_risk = _hot_theme_is_hard_risk(decision.theme_name, hot_anchor)
        style_risk = _is_style_risk_theme(decision.theme_name)
        if style_risk:
            path_type = "style_risk_watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "elevated"
            path_rank = 10 if decision.role_hint == "true_leader" else 22
        elif hot_hard_risk:
            path_type = "hot_plate_hard_risk_watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "elevated"
            path_rank = 7 if decision.role_hint == "true_leader" else 15
        elif decision.theme_name in risk_themes + local_pack_risk_themes + temporal_fading + hot_risk and decision.role_hint != "true_leader":
            path_type = "risk_theme_watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "elevated"
            path_rank = 8
        elif (
            decision.theme_name in battlefield_theme_set
            and temporal_battlefield_state in {"extend", "handoff_confirmed"}
            and is_local_aligned
        ):
            path_type = "battlefield_anchor_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = -2 if decision.theme_name == temporal_battlefield_theme else -1
        elif (
            decision.theme_name == temporal_handoff_to
            and temporal_handoff_from
            and temporal_battlefield_state == "handoff_confirmed"
        ):
            path_type = "battlefield_handoff_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = -1
        elif decision.theme_name in hot_primary and is_local_aligned:
            path_type = "hot_plate_anchor_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = _theme_order_index(decision.theme_name, hot_primary)
        elif decision.theme_name in hot_primary and decision.theme_name != main_attack_theme:
            path_type = "hot_plate_anchor_watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal"
            path_rank = 12 + _theme_order_index(decision.theme_name, hot_primary)
        elif decision.theme_name in temporal_targets and is_local_aligned:
            path_type = "timeframe_aligned_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 1 + _theme_order_index(decision.theme_name, temporal_targets)
        elif decision.theme_name in temporal_targets:
            path_type = "timeframe_watch"
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal"
            path_rank = 18 + _theme_order_index(decision.theme_name, temporal_targets)
        elif is_local_aligned and bridge_state == "theme_stock_pressure_repair":
            path_type = "local_pack_pressure_repair"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "elevated"
            path_rank = 1
        elif is_local_aligned and decision.theme_name == main_attack_theme:
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
            path_rank = (2 if decision.theme_name in secondary_theme_set else 6) + _theme_order_index(decision.theme_name, local_pack_themes)
        elif decision.theme_name == main_attack_theme:
            path_type = "main_attack"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 0
        elif decision.theme_name in secondary_theme_set:
            path_type = "secondary_follow"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = 3 + _theme_order_index(decision.theme_name, global_decision.secondary_themes)
        elif decision.theme_name in mainline_order:
            path_type = "mainline_follow"
            action = "probe"
            bucket = "shadow_attack"
            risk_level = "normal"
            path_rank = (4 if decision.theme_name in secondary_theme_set else 9) + _theme_order_index(decision.theme_name, mainline_order)
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
        market_spread_focus_core = (
            focus_stress_state == "market_risk_spread"
            and decision.role_hint in {"true_leader", "front_row"}
            and (
                decision.symbol in focus_core_symbols
                or decision.theme_name in market_spread_core_themes
            )
        )
        front_row_core_watch = (
            decision.role_hint == "front_row"
            and (
                decision.symbol in focus_core_symbols
                or decision.theme_name in market_spread_core_themes
            )
        )
        if (
            decision.theme_name in focus_stress_themes
            and decision.role_hint != "true_leader"
            and not market_spread_focus_core
        ):
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal" if front_row_core_watch else "elevated"
            path_rank = max(path_rank, 13 if front_row_core_watch else 14)
            path_type = f"{path_type}_focus_stress"
        if decision.theme_name in dragon_alone_themes and decision.role_hint != "true_leader":
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal" if front_row_core_watch else "elevated"
            path_rank = max(path_rank, 15 if front_row_core_watch else 16)
            path_type = f"{path_type}_dragon_alone"
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
        if (
            mainline_family_ready
            and theme_tier >= 2
            and path_type not in {"local_pack_pressure_repair"}
            and action == "probe"
        ):
            action = "watch"
            bucket = "shadow_watch"
            risk_level = "normal" if theme_tier == 2 else "elevated"
            path_rank += 18 if theme_tier == 2 else 28
            path_type = f"{path_type}_off_mainline"
        if is_local_feed_quant and "local_feed" not in path_type:
            path_type = f"{path_type}_local_feed_quant"
            if action != "probe":
                path_rank = min(path_rank, 16 if theme_tier <= 2 else 24)
        behavior_rank = 0 if decision.entry_behavior in {"volume_confirm", "low_open_repair", "limit_attack"} else 1
        role_rank = 0 if decision.role_hint == "true_leader" else 1
        amount_2m = _metric_value(decision.trace, "amount_2m")
        speed_1m = _metric_value(decision.trace, "speed_1m")
        auction_amount = _metric_value(decision.trace, "auction_amount")
        ranked.append(
            (
                (
                    float(path_rank),
                    float(theme_tier),
                    float(role_rank),
                    float(behavior_rank),
                    -amount_2m,
                    -speed_1m,
                    -auction_amount,
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
    blocked_market_risk = 0
    blocked_dragon_alone = 0
    blocked_per_theme_cap = 0
    released_market_spread_core = 0
    for sort_key, decision, path_type, action, risk_level in ranked:
        if decision.symbol in seen_symbols:
            continue
        market_spread_core_allowed = (
            focus_stress_state == "market_risk_spread"
            and action == "probe"
            and path_type != "local_pack_pressure_repair"
            and risk_level == "normal"
            and decision.role_hint in {"true_leader", "front_row"}
            and (
                decision.symbol in focus_core_symbols
                or decision.theme_name in market_spread_core_themes
            )
        )
        if (
            focus_stress_state == "market_risk_spread"
            and action == "probe"
            and path_type != "local_pack_pressure_repair"
            and not market_spread_core_allowed
        ):
            blocked_market_risk += 1
            continue
        if market_spread_core_allowed:
            released_market_spread_core += 1
        if decision.theme_name in dragon_alone_themes and decision.role_hint != "true_leader":
            blocked_dragon_alone += 1
            continue
        theme_count = per_theme_count.get(decision.theme_name, 0)
        max_per_theme = (
            1
            if decision.theme_name in dragon_alone_themes
            else (3 if decision.theme_name in mid_supplement_themes else 2)
        )
        if theme_count >= max_per_theme:
            blocked_per_theme_cap += 1
            continue
        bucket = "shadow_rotation" if path_type == "rotation_probe" else ("shadow_watch" if action == "watch" else "shadow_attack")
        selected.append((decision, path_type, action, risk_level, bucket, sort_key[0]))
        seen_symbols.add(decision.symbol)
        per_theme_count[decision.theme_name] = theme_count + 1
        if len(selected) >= FINAL_CANDIDATE_BUFFER_LIMIT:
            break
    empty_reason = "-"
    if not selected:
        if not ranked:
            empty_reason = "no_ranked_candidates"
        elif blocked_market_risk >= len(ranked):
            empty_reason = "market_risk_blocked_probe"
        elif blocked_dragon_alone >= len(ranked):
            empty_reason = "dragon_alone_filtered"
        elif raw_candidates and all(_is_style_risk_theme(decision.theme_name) for decision in raw_candidates):
            empty_reason = "style_risk_watch_only"
        else:
            empty_reason = "ranked_filtered"
    selected.sort(
        key=lambda item: (
            item[5],
            item[0].role_hint != "true_leader",
            -_metric_value(item[0].trace, "amount_2m"),
            -_metric_value(item[0].trace, "speed_1m"),
            item[0].symbol,
        )
    )
    logger.info(
        "final candidate debug | phase=%s | stock_local=%s | candidate=%s | front=%s | mid=%s | target_themes=%s | strict=%s | relaxed_front=%s | candidate_any=%s | relaxed_mid=%s | mid_supplement=%s | repair_supplement=%s | local_feed_supplement=%s | strategy_allowed=%s | strategy_rejected=%s | selected=%s | empty_reason=%s | ranked=%s | blocked_market=%s | released_market_core=%s | blocked_dragon=%s | blocked_cap=%s | main_ready=%s | second_ready=%s | hot_primary=%s | temporal_targets=%s | battlefield=%s | battlefield_state=%s | handoff=%s->%s | rising_hot=%s | handoff_evidence=%s | handoff_persist=%s | battlefield_candidates=%s | local_align=%s | risk_themes=%s",
        _phase_name(context),
        len(stock_local_all),
        stock_local_candidate_count,
        stock_local_front_count,
        stock_local_mid_count,
        len(target_themes),
        len(raw_candidates_strict),
        len(raw_candidates_relaxed_front),
        len(raw_candidates_candidate_any),
        len(raw_candidates_relaxed_mid),
        len(raw_candidates_mid_supplement),
        len(raw_candidates_repair_supplement),
        len(raw_candidates_local_feed_supplement),
        ",".join(allowed_strategy_ids) or "-",
        ",".join(rejected_strategy_reasons[:3]) or "-",
        len(selected),
        empty_reason,
        len(ranked),
        blocked_market_risk,
        released_market_spread_core,
        blocked_dragon_alone,
        blocked_per_theme_cap,
        len(raw_main_attack),
        len(raw_secondary),
        len(hot_primary),
        len(temporal_targets),
        temporal_battlefield_theme or "-",
        (
            f"{temporal_battlefield_state_raw}->{temporal_battlefield_state}"
            if temporal_battlefield_state_raw != temporal_battlefield_state
            else (temporal_battlefield_state or "-")
        ),
        temporal_handoff_from or "-",
        temporal_handoff_to or "-",
        len(temporal_rising_hot),
        temporal_handoff_evidence_count,
        temporal_handoff_persistence_ok,
        temporal_battlefield_candidate_count,
        len(local_aligned_symbols),
        len(risk_themes),
    )
    final_candidates: list[FinalCandidateDecision] = []
    for priority_rank, (stock_decision, path_type, action, risk_level, bucket, _path_rank) in enumerate(selected[:FINAL_CANDIDATE_BUFFER_LIMIT], start=1):
        reason_codes = (
            "validated_theme_profit_center",
            path_type,
            stock_decision.role_hint,
            stock_decision.entry_behavior,
        )
        risk_tag_items = list(stock_decision.trace.risk_tags)
        if stock_decision.theme_name in risk_themes + local_pack_risk_themes + temporal_fading + hot_risk:
            risk_tag_items.append("relative_risk_theme")
        if _hot_theme_is_hard_risk(stock_decision.theme_name, hot_anchor):
            risk_tag_items.append("hot_plate_hard_risk")
        if _hot_theme_is_index_defense(stock_decision.theme_name, hot_anchor):
            risk_tag_items.append("index_defense_hot_plate")
        if stock_decision.theme_name in hot_primary:
            risk_tag_items.append("hot_plate_anchor")
        if "local_feed_quant" in path_type:
            risk_tag_items.append("local_feed_quant_signal")
        if stock_decision.theme_name in focus_stress_themes:
            risk_tag_items.append("focus_asset_stress")
        if stock_decision.theme_name in dragon_alone_themes and stock_decision.role_hint != "true_leader":
            risk_tag_items.append("dragon_alone_risk")
        bridge_signal = _bridge_signal_for_symbol(decision_bundle, stock_decision.symbol)
        if bridge_signal is not None:
            risk_tag_items.extend(bridge_signal.risk_tags)
        risk_tags = tuple(dict.fromkeys(risk_tag_items))
        temporal_chain = ()
        if temporal is not None:
            temporal_chain = tuple(
                item
                for item in temporal.chain_summary
                if item.startswith(f"{stock_decision.theme_name}:")
            )[:2]
        buy_point_code, buy_point_label, buy_point_gate = _candidate_buy_point_type(
            stock_decision,
            path_type=path_type,
            action=action,
            risk_level=risk_level,
            global_decision=global_decision,
            hot_anchor=hot_anchor,
        )
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
            metrics=stock_decision.trace.metrics,
            metric_values=stock_decision.trace.metric_values,
            evidence_summary=(
                f"buy_point={buy_point_code}",
                f"buy_label={buy_point_label}",
                f"buy_gate={buy_point_gate}",
                f"path={path_type}",
                f"hot_state={hot_state_map.get(stock_decision.theme_name, '-')}",
                f"focus_stress={focus_stress_state if stock_decision.theme_name in focus_stress_themes else '-'}",
                f"bridge={bridge_signal.state if bridge_signal is not None else '-'}",
                *(temporal_chain or ("timeframe_chain=-",)),
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
    focus_stress = decision_bundle.focus_asset_stress_decision
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
        tag
        for tag in global_decision.trace.risk_tags
        if tag not in {"risk_capped_pressure_repair", "theme_relative_risk"}
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
    released_market_core_probe = any(
        item.action == "probe"
        and item.risk_level == "normal"
        and "focus_asset_stress" in tuple(item.trace.risk_tags or ())
        and "hot_plate_hard_risk" not in tuple(item.trace.risk_tags or ())
        and "dragon_alone_risk" not in tuple(item.trace.risk_tags or ())
        and "relative_risk_theme" not in tuple(item.trace.risk_tags or ())
        for item in eligible_candidates
    )
    if (
        focus_stress is not None
        and focus_stress.stress_state == "market_risk_spread"
        and not released_market_core_probe
    ):
        block_reasons.append("focus_asset_market_risk")
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
            f"released_market_core_probe={released_market_core_probe}",
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
    candidate_buy_points = {_buy_point_code(item) or "unknown" for item in final_candidates}
    actionable_buy_points = {
        _buy_point_code(item)
        for item in final_candidates
        if _candidate_is_direct_probe_buy(item)
    }
    repair_actionable_candidates = tuple(
        item
        for item in final_candidates
        if (_buy_point_code(item) or "") in REPAIR_ACTIONABLE_BUY_POINTS
        and (_buy_gate_code(item) or "") in ACTIONABLE_BUY_GATES
        and item.risk_level in {"normal", "elevated"}
        and "hot_plate_hard_risk" not in tuple(item.trace.risk_tags or ())
        and "index_defense_hot_plate" not in tuple(item.trace.risk_tags or ())
        and "focus_asset_stress" not in tuple(item.trace.risk_tags or ())
        and "dragon_alone_risk" not in tuple(item.trace.risk_tags or ())
    )
    same_theme_arbitrage_only = bool(actionable_buy_points) and actionable_buy_points.issubset({"same_theme_arbitrage"})
    actionable_shape_paths = {
        item.path_type
        for item in final_candidates
        if _candidate_has_actionable_buy_gate(item)
    }
    candidate_refs_by_playbook: dict[str, tuple[str, ...]] = {}
    for item in final_candidates:
        refs = candidate_refs_by_playbook.get(item.playbook, ())
        if len(refs) < 3:
            candidate_refs_by_playbook[item.playbook] = refs + (item.trace.decision_id,)
    pack = decision_bundle.local_strategy_evidence_pack
    high_pressure_count = len(pack.high_pressure_alerts) if pack is not None else 0
    emotion_risk = any(item.action_hint in {"avoid", "avoid_chase"} or item.avoid_count > 0 for item in (pack.emotion_alerts if pack is not None else ()))
    risk_tags = tuple(global_decision.trace.risk_tags)
    hot_hard_risk_global = global_decision.market_script == "hot_risk_validation" or "hot_plate_hard_risk" in risk_tags
    index_defense_global = "index_defense_hot_plate" in risk_tags
    hot_overheat_watch = bool(
        global_decision.market_script == "attack_confirmed"
        and final_candidates
        and candidate_buy_points
        and candidate_buy_points.issubset({"strength_only", "avoid_chase", "watch_only", "unknown"})
        and ("strength_only" in candidate_buy_points or "avoid_chase" in candidate_buy_points)
    )
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
        and not index_defense_global
        and not hot_overheat_watch
    )
    mainline_cap = min(global_decision.position_cap, 0.08) if same_theme_arbitrage_only else global_decision.position_cap
    mainline_reason = "same-theme arbitrage small probe" if mainline_enabled and same_theme_arbitrage_only else "confirmed local/theme path"
    add_row(
        "mainline_attack",
        enabled=mainline_enabled,
        action_hint="probe" if mainline_enabled else "watch",
        cap=mainline_cap if mainline_enabled else 0.0,
        reason=mainline_reason if mainline_enabled else "no clean mainline candidate",
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
    risk_control_enabled = high_pressure_block or hot_hard_risk_global or hot_overheat_watch
    add_row(
        "dragon_head_risk_control",
        enabled=risk_control_enabled,
        action_hint="avoid_chase" if risk_control_enabled else "watch",
        cap=0.0,
        reason=(
            "hot plate hard risk blocks sector chase"
            if hot_hard_risk_global
            else (
                "hot plate overheat has no tradable entry"
                if hot_overheat_watch
                else ("high-focus pressure blocks same-theme attack" if high_pressure_block else "no active high-pressure block")
            )
        ),
        risks=(
            ("hot_plate_hard_risk",)
            if hot_hard_risk_global
            else (("hot_plate_overheat_watch",) if hot_overheat_watch else (("high_focus_pressure",) if high_pressure_block else ()))
        ),
        refs=tuple(summary.scope for summary in (pack.high_pressure_alerts[:3] if pack is not None else ())),
    )

    global_attack_confirmed = global_decision.market_script == "attack_confirmed"
    timeframe_rotation_confirmed = (
        global_attack_confirmed
        and any(path.startswith("timeframe_aligned") for path in candidate_paths)
        and not hot_hard_risk_global
    )
    hot_anchor_confirmed = (
        global_attack_confirmed
        and any(path.startswith("hot_plate_anchor_attack") for path in candidate_paths)
        and not hot_hard_risk_global
    )
    hot_anchor_watch = any(path.startswith("hot_plate_anchor_watch") for path in candidate_paths)
    hot_anchor_actionable_watch = any(path.startswith("hot_plate_anchor_watch") for path in actionable_shape_paths)
    timeframe_actionable_watch = any(path.startswith("timeframe_watch") for path in actionable_shape_paths)
    rotation_confirmed = global_attack_confirmed and (
        "capital_rotation" in confirmed_scripts
        or timeframe_rotation_confirmed
        or hot_anchor_confirmed
        or hot_anchor_actionable_watch
        or timeframe_actionable_watch
    )
    rotation_partial = hot_hard_risk_global or any(
        hypothesis_by_id.get(item.hypothesis_id) is not None
        and hypothesis_by_id[item.hypothesis_id].script == "capital_rotation"
        and item.result in {"partial", "pending"}
        for item in validations
    ) or hot_anchor_watch or (
        not global_attack_confirmed and "capital_rotation" in confirmed_scripts
    ) or (
        not global_attack_confirmed
        and any(path.startswith(("timeframe_aligned", "hot_plate_anchor_attack")) for path in candidate_paths)
    )
    rotation_cap = 0.0
    if rotation_confirmed:
        rotation_cap = min(max(global_decision.position_cap, 0.08 if (hot_anchor_confirmed or hot_anchor_actionable_watch or timeframe_actionable_watch) else 0.0), 0.18)
    elif rotation_partial:
        rotation_cap = 0.05
    add_row(
        "sector_rotation",
        enabled=rotation_confirmed,
        action_hint="probe" if rotation_confirmed else ("watch" if rotation_partial else "disabled"),
        cap=rotation_cap,
        reason=(
            "timeframe rotation confirmed"
            if timeframe_rotation_confirmed
            else (
                "hot plate anchor confirmed"
                if hot_anchor_confirmed
                else (
                    "tradable hot plate buy point"
                    if hot_anchor_actionable_watch
                    else (
                        "tradable timeframe buy point"
                        if timeframe_actionable_watch
                        else ("hot plate hard risk validation" if hot_hard_risk_global else ("rotation confirmed" if rotation_confirmed else ("rotation needs validation" if rotation_partial else "no rotation path")))
                    )
                )
            )
        ),
        risks=("hot_plate_hard_risk",) if hot_hard_risk_global else (("rotation_unconfirmed",) if rotation_partial and not rotation_confirmed else ()),
        refs=candidate_refs_by_playbook.get("sector_rotation", ()),
    )

    repair_micro_allowed = bool(
        repair_actionable_candidates
        and global_decision.market_script != "risk_off"
        and not index_defense_global
    )
    weak_repair_enabled = bool(
        pressure_enabled
        or "dragon_pressure_repair" in candidate_playbooks
        or repair_micro_allowed
    )
    weak_repair_cap_limit = 0.08 if hot_hard_risk_global else 0.12
    weak_repair_cap = min(max(global_decision.position_cap, 0.05), weak_repair_cap_limit) if weak_repair_enabled else 0.0
    weak_repair_reason = (
        "repair buy point under hot-risk background"
        if repair_micro_allowed and hot_hard_risk_global and not pressure_enabled
        else "low-risk repair buy point"
        if repair_micro_allowed and not pressure_enabled
        else ("low-open repair with 2m confirmation" if weak_repair_enabled else "no confirmed weak-to-strong bridge")
    )
    add_row(
        "weak_to_strong_repair",
        enabled=weak_repair_enabled,
        action_hint="probe" if weak_repair_enabled else "watch",
        cap=weak_repair_cap,
        reason=weak_repair_reason,
        risks=tuple(
            item
            for item in (
                "elevated_repair_risk" if weak_repair_enabled else "",
                "hot_plate_hard_risk_background" if weak_repair_enabled and hot_hard_risk_global else "",
            )
            if item
        ),
        refs=tuple(
            dict.fromkeys(
                (
                    *candidate_refs_by_playbook.get("dragon_pressure_repair", ()),
                    *(item.trace.decision_id for item in repair_actionable_candidates[:3]),
                )
            )
        ),
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
    blocked = tuple(
        row.playbook
        for row in rows
        if row.action_hint in {"avoid", "avoid_chase", "disabled"} or (row.cap <= 0.0 and row.risk_tags)
    )
    watch_risk = tuple(
        row.playbook
        for row in rows
        if row.playbook not in blocked and row.risk_tags
    )
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
            f"watch_risk={','.join(watch_risk) or '-'}",
            f"max_cap={max_cap:.0%}",
            f"takeover={takeover.mode}",
            f"global={global_decision.market_script}",
        ),
    )


def _build_playbook_candidate_slice(
    final_candidates: tuple[FinalCandidateDecision, ...],
    matrix: PlaybookControlMatrix | None,
):
    views = []
    weak_repair_row = playbook_row(matrix, "weak_to_strong_repair")
    for candidate in sorted(
        final_candidates,
        key=lambda item: (
            int(item.priority_rank or 999),
            item.risk_level == "high",
            item.action != "probe",
            item.symbol,
        ),
    ):
        row = playbook_row(matrix, candidate.playbook)
        view = build_playbook_decision_view(
            symbol=candidate.symbol,
            raw_action=candidate.action,
            row=row,
            source="decision_bundle",
            playbook=candidate.playbook,
            path_type=candidate.path_type,
            action_hint=candidate.action,
            priority_rank=candidate.priority_rank,
            evidence_refs=(
                candidate.trace.decision_id,
                *candidate.trace.evidence_refs[:2],
            ),
        )
        buy_point = _buy_point_code(candidate)
        buy_gate = _buy_gate_code(candidate)
        repair_micro_probe_allowed = bool(
            buy_point in REPAIR_ACTIONABLE_BUY_POINTS
            and buy_gate in ACTIONABLE_BUY_GATES
            and weak_repair_row is not None
            and weak_repair_row.enabled
            and weak_repair_row.action_hint == "probe"
            and not view.blocked
            and candidate.risk_level in {"normal", "elevated"}
            and "hot_plate_hard_risk" not in tuple(candidate.trace.risk_tags or ())
            and "index_defense_hot_plate" not in tuple(candidate.trace.risk_tags or ())
            and "focus_asset_stress" not in tuple(candidate.trace.risk_tags or ())
            and "dragon_alone_risk" not in tuple(candidate.trace.risk_tags or ())
        )
        if buy_point == "avoid_chase":
            view = replace(
                view,
                action_hint="avoid_chase",
                display_bucket="blocked",
                primary_allowed=False,
                blocked=True,
                reason="buy_point_avoid_chase",
                risk_tags=tuple(dict.fromkeys((*view.risk_tags, "buy_point_avoid_chase"))),
            )
        elif repair_micro_probe_allowed:
            view = replace(
                view,
                action_hint="probe",
                display_bucket="primary",
                primary_allowed=True,
                cap=weak_repair_row.cap,
                reason="weak_repair_micro_probe",
                risk_tags=tuple(dict.fromkeys((*view.risk_tags, "repair_micro_probe"))),
            )
        elif (
            buy_point in ACTIONABLE_BUY_POINTS
            and buy_gate in {"action_not_probe", "rotation_wait_confirm"}
            and row is not None
            and row.enabled
            and row.action_hint == "probe"
            and not view.blocked
            and candidate.risk_level == "normal"
            and (
                buy_gate == "action_not_probe"
                or (
                    buy_gate == "rotation_wait_confirm"
                    and candidate.playbook == "sector_rotation"
                    and "hot_plate_hard_risk" not in tuple(candidate.trace.risk_tags or ())
                    and "focus_asset_stress" not in tuple(candidate.trace.risk_tags or ())
                    and "dragon_alone_risk" not in tuple(candidate.trace.risk_tags or ())
                    and "relative_risk_theme" not in tuple(candidate.trace.risk_tags or ())
                )
            )
        ):
            view = replace(
                view,
                action_hint="probe",
                display_bucket="primary",
                primary_allowed=True,
                reason=(
                    "playbook_authorized_rotation_micro_probe"
                    if buy_gate == "rotation_wait_confirm"
                    else "playbook_authorized_actionable_shape"
                ),
            )
        elif (
            buy_point in ACTIONABLE_BUY_POINTS
            and buy_gate in ACTIONABLE_BUY_GATES
            and not view.primary_allowed
            and not view.blocked
            and candidate.risk_level != "high"
        ):
            view = replace(
                view,
                action_hint="watch",
                display_bucket="watch",
                primary_allowed=False,
                reason="near_buy_wait_global_authorization",
                risk_tags=tuple(dict.fromkeys((*view.risk_tags, "near_buy_watch"))),
            )
        elif buy_point == "strength_only" and view.primary_allowed:
            view = replace(
                view,
                action_hint="watch",
                display_bucket="watch",
                primary_allowed=False,
                reason="buy_point_strength_only",
            )
        elif buy_point in {"watch_only", "unknown", ""} and view.primary_allowed:
            view = replace(
                view,
                action_hint="watch",
                display_bucket="watch",
                primary_allowed=False,
                reason="buy_point_watch_only",
            )
        views.append(view)
    return slice_playbook_decision_views(tuple(views))


def _build_playbook_output_summary(
    *,
    candidate_slice,
    final_candidates: tuple[FinalCandidateDecision, ...],
    global_decision: GlobalMarketDecision,
    matrix: PlaybookControlMatrix,
    temporal,
    hot_anchor: HotPlateAnchorDecision | None,
    candidate_funnel_summary: CandidateFunnelSummary | None = None,
) -> PlaybookOutputSummary:
    candidate_map = {item.symbol: item for item in final_candidates}

    def _candidate_line(view, *, bucket: str) -> str:
        candidate = candidate_map.get(view.symbol)
        theme = candidate.theme_name if candidate is not None else "-"
        if bucket == "primary" and view.primary_allowed and candidate is not None:
            action = "probe" if view.action_hint == "probe" else candidate.action
        elif bucket == "repair" and view.primary_allowed and candidate is not None:
            action = "probe" if view.action_hint == "probe" else candidate.action
        elif bucket == "avoid":
            action = "avoid_chase" if view.blocked else "avoid"
        else:
            action = "watch"
        risk = candidate.risk_level if candidate is not None else ("blocked" if view.blocked else "normal")
        block_note = ""
        if bucket == "avoid":
            risk = "blocked" if view.blocked or action in {"avoid", "avoid_chase"} else risk
            block_reason = view.reason or ",".join(view.risk_tags) or "global_or_playbook_block"
            block_note = f"block={block_reason}/"
        evidence = "-"
        invalidation = "-"
        quant = "-"
        buy_label = "-"
        if candidate is not None:
            buy_label = _evidence_value(candidate.trace.evidence_summary, "buy_label", "-") or "-"
            evidence_items = tuple(
                item
                for item in candidate.trace.evidence_summary
                if not str(item or "").startswith(("buy_point=", "buy_label="))
            )
            evidence = " / ".join(evidence_items[:3]) or "-"
            invalidation = ",".join(candidate.trace.invalidation_points[:2]) or "-"
            quant = (
                f"2m={_fmt_amount_yuan(_metric_value(candidate.trace, 'amount_2m'))},"
                f"1m={_fmt_metric_pct(_metric_value(candidate.trace, 'speed_1m'))},"
                f"auc={_fmt_amount_yuan(_metric_value(candidate.trace, 'auction_amount'))},"
                f"open={_fmt_metric_pct(_metric_value(candidate.trace, 'open_pct'))},"
                f"now={_fmt_metric_pct(_metric_value(candidate.trace, 'current_pct'))}"
            )
        action_text = {
            "probe": "试错",
            "watch": "观察",
            "avoid": "回避",
            "avoid_chase": "回避追高",
            "disabled": "禁做",
        }.get(action, action or "-")
        if bucket == "watch" and candidate is not None and _buy_point_code(candidate) in ACTIONABLE_BUY_POINTS and not view.blocked:
            action_text = "近买点观察"
        bucket_text = {
            "primary": "主攻",
            "watch": "观察",
            "repair": "修复",
            "avoid": "回避",
        }.get(bucket, bucket)
        playbook_text = {
            "mainline_attack": "主线进攻",
            "sector_rotation": "题材切换",
            "dragon_pressure_repair": "高标修复",
            "dragon_head_risk_control": "高标风控",
            "weak_to_strong_repair": "弱转强修复",
            "yesterday_limit_relay": "昨日涨停接力",
            "watch": "观察",
        }.get(view.playbook or "", view.playbook or "-")
        return (
            f"{view.symbol}={action_text}/{theme}/{playbook_text}/"
            f"{bucket_text}/买点={buy_label}/{block_note}risk={risk}/cap={view.cap:.0%}/"
            f"quant={quant}/"
            f"证据={evidence}/证伪={invalidation}"
        )

    def _view_is_true_avoid(view) -> bool:
        """Keep risk names separate from candidates merely stopped by global arbitration."""

        candidate = candidate_map.get(view.symbol)
        buy_point = _buy_point_code(candidate) if candidate is not None else ""
        buy_gate = _buy_gate_code(candidate) if candidate is not None else ""
        if buy_point == "avoid_chase":
            return True
        hard_gate_codes = {
            "hot_theme_hard_risk",
            "near_limit_non_leader",
            "high_open_chase",
            "high_open_distribution",
            "risk_level_block",
        }
        if buy_gate in hard_gate_codes:
            return True
        risk_tags = set(tuple(getattr(view, "risk_tags", ()) or ()))
        if candidate is not None:
            risk_tags.update(tuple(getattr(candidate.trace, "risk_tags", ()) or ()))
        true_avoid_tags = {
            "buy_point_avoid_chase",
            "hot_plate_hard_risk",
            "hot_theme_hard_risk",
            "risk_theme_watch",
            "relay_emotion_risk",
            "local_pack_theme_risk",
        }
        if risk_tags.intersection(true_avoid_tags):
            return True
        if "relative_risk_theme" in risk_tags and buy_point not in REPAIR_ACTIONABLE_BUY_POINTS:
            return True
        path = str(getattr(view, "path_type", "") or "")
        if path.startswith(("hot_plate_hard_risk", "risk_theme_watch")):
            return True
        reason = str(getattr(view, "reason", "") or "")
        if reason in {"buy_point_avoid_chase", "avoid", "avoid_chase"}:
            return True
        return False

    def _view_is_yest_high_focus_risk(view) -> bool:
        """Fold yesterday-limit/high-focus risk samples out of recommendation-like avoid lists."""

        candidate = candidate_map.get(view.symbol)
        if candidate is None:
            return False
        is_yest_limit = _metric_value(candidate.trace, "is_yest_limit") >= 0.5
        if not is_yest_limit:
            return False
        path = str(getattr(view, "path_type", "") or "")
        risk_tags = set(tuple(getattr(view, "risk_tags", ()) or ()))
        risk_tags.update(tuple(getattr(candidate.trace, "risk_tags", ()) or ()))
        high_focus_tags = {
            "hot_plate_hard_risk",
            "relative_risk_theme",
            "high_focus_pressure",
            "buy_point_avoid_chase",
        }
        return path.startswith(("hot_plate_hard_risk", "risk_theme_watch")) or bool(risk_tags.intersection(high_focus_tags))

    primary = tuple(view.symbol for view in candidate_slice.primary[:FINAL_CANDIDATE_DISPLAY_LIMIT])
    watch = tuple(view.symbol for view in candidate_slice.watch)
    inactive = tuple(view.symbol for view in candidate_slice.inactive)
    blocked = tuple(view.symbol for view in candidate_slice.blocked)
    primary_actions = tuple(_candidate_line(view, bucket="primary") for view in candidate_slice.primary[:FINAL_CANDIDATE_DISPLAY_LIMIT])
    watch_actions = tuple(
        _candidate_line(view, bucket="watch")
        for view in (candidate_slice.watch + candidate_slice.inactive)[:8]
    )
    repair_actions = tuple(
        _candidate_line(view, bucket="repair")
        for view in (candidate_slice.primary + candidate_slice.watch + candidate_slice.inactive)
        if "repair" in (view.playbook or "")
        or "repair" in (view.path_type or "")
        or "low_open_repair" in tuple(candidate_map.get(view.symbol).trace.reason_codes if candidate_map.get(view.symbol) is not None else ())
    )[:5]
    avoid_actions = tuple(
        _candidate_line(view, bucket="avoid")
        for view in tuple(
            view
            for view in (candidate_slice.blocked + candidate_slice.unclassified)
            if _view_is_true_avoid(view) and not _view_is_yest_high_focus_risk(view)
        )[:8]
    )
    primary_reasons = tuple(
        f"{view.symbol}=playbook:{view.playbook or '-'};path:{view.path_type or '-'};cap:{view.cap:.0%};reason:{view.reason or '-'}"
        for view in candidate_slice.primary[:5]
    )
    watch_reasons = tuple(
        f"{view.symbol}=watch;playbook:{view.playbook or '-'};bucket:{view.display_bucket};reason:{view.reason or '-'}"
        for view in (candidate_slice.watch + candidate_slice.inactive)[:5]
    )
    reject_reasons = tuple(
        f"{view.symbol}=blocked;playbook:{view.playbook or '-'};risk:{','.join(view.risk_tags) or view.reason or '-'}"
        f"{';fold=yest_high_focus_risk' if _view_is_yest_high_focus_risk(view) else ''}"
        for view in (candidate_slice.blocked + candidate_slice.unclassified)[:8]
    )
    risk_tags = tuple(
        dict.fromkeys(
            tuple(global_decision.trace.risk_tags)
            + tuple(tag for row in matrix.rows for tag in row.risk_tags)
            + tuple(tag for view in candidate_slice.blocked for tag in view.risk_tags)
        )
    )
    temporal_text = ""
    handoff_evidence_count = 0
    handoff_persistence_ok = 0
    if temporal is not None:
        handoff_evidence_count = int(_metric_value(temporal.trace, "handoff_evidence_count", 0.0))
        handoff_persistence_ok = int(_metric_value(temporal.trace, "handoff_persistence_ok", 0.0))
        temporal_text = (
            f"temporal={temporal.exchange_state};"
            f"battlefield={getattr(temporal, 'main_battlefield_theme', '') or '-'};"
            f"battlefield_state={getattr(temporal, 'battlefield_state', '') or '-'};"
            f"handoff={(getattr(temporal, 'handoff_from', '') or '-') + '->' + (getattr(temporal, 'handoff_to', '') or '-')};"
            f"handoff_evidence={handoff_evidence_count};"
            f"handoff_persist={handoff_persistence_ok};"
            f"targets={','.join(temporal.target_themes[:3]) or '-'};"
            f"fading={','.join(temporal.fading_themes[:3]) or '-'}"
        )
    focus_stress_state = next(
        (item.split("=", 1)[1] for item in tuple(global_decision.trace.evidence_summary or ()) if item.startswith("focus_stress=")),
        "-",
    )
    focus_spread = next(
        (item.split("=", 1)[1] for item in tuple(global_decision.trace.evidence_summary or ()) if item.startswith("focus_spread=")),
        "-",
    )
    temporal_battlefield_state = str(getattr(temporal, "battlefield_state", "") or "") if temporal is not None else ""
    temporal_battlefield_theme = str(getattr(temporal, "main_battlefield_theme", "") or "") if temporal is not None else ""
    temporal_handoff_to = str(getattr(temporal, "handoff_to", "") or "") if temporal is not None else ""
    temporal_battlefield_state_raw = temporal_battlefield_state
    temporal_battlefield_state = _effective_temporal_battlefield_state(temporal, temporal_battlefield_state)
    narrative_main = global_decision.main_attack_theme or "-"
    if temporal_battlefield_state == "handoff_confirmed" and (temporal_handoff_to or temporal_battlefield_theme):
        narrative_main = temporal_handoff_to or temporal_battlefield_theme
    elif temporal_battlefield_state in {"extend", "handoff_attempt", "observe"} and temporal_battlefield_theme:
        narrative_main = temporal_battlefield_theme
    narrative_secondary_items: list[str] = []
    if (
        temporal_handoff_to
        and temporal_handoff_to != narrative_main
        and temporal_battlefield_state in {"handoff_attempt", "handoff_confirmed"}
    ):
        narrative_secondary_items.append(temporal_handoff_to)
    narrative_secondary_items.extend(global_decision.secondary_themes[:3])
    narrative_secondary = ",".join(tuple(dict.fromkeys(theme for theme in narrative_secondary_items if theme))[:3]) or "-"
    hot_primary_text = ",".join(tuple(getattr(hot_anchor, "primary_themes", ()) or ())[:3]) if hot_anchor is not None else "-"
    handoff_to_text = temporal_handoff_to or "-"
    execution_text = "probe" if candidate_slice.primary else "watch"
    buy_point_by_symbol = {candidate.symbol: _buy_point_code(candidate) or "unknown" for candidate in final_candidates}
    buy_gate_by_symbol = {candidate.symbol: _buy_gate_code(candidate) or "unknown" for candidate in final_candidates}
    def _view_gate_code(view) -> str:
        code = buy_gate_by_symbol.get(view.symbol, "") or "unknown"
        if view.primary_allowed and code == "action_not_probe":
            return "playbook_authorized"
        return code

    buy_point_counts: dict[str, int] = {}
    for code in buy_point_by_symbol.values():
        buy_point_counts[code] = buy_point_counts.get(code, 0) + 1
    buy_gate_counts: dict[str, int] = {}
    for view in (
        *tuple(candidate_slice.primary or ()),
        *tuple(candidate_slice.watch or ()),
        *tuple(candidate_slice.inactive or ()),
        *tuple(candidate_slice.blocked or ()),
        *tuple(candidate_slice.unclassified or ()),
    ):
        code = _view_gate_code(view)
        buy_gate_counts[code] = buy_gate_counts.get(code, 0) + 1
    filtered_buy_points: dict[str, int] = {}
    for view in (*tuple(candidate_slice.watch or ()), *tuple(candidate_slice.blocked or ())):
        code = buy_point_by_symbol.get(view.symbol, "")
        if not code or code in {"watch_only", "unknown"}:
            continue
        filtered_buy_points[code] = filtered_buy_points.get(code, 0) + 1
    primary_buy_points = tuple(
        code
        for view in tuple(candidate_slice.primary or ())
        for code in (buy_point_by_symbol.get(view.symbol, ""),)
        if code
    )
    battle_mode = _battle_mode_from_buy_points(
        market_script=global_decision.market_script,
        primary_buy_points=primary_buy_points,
        filtered_buy_points=filtered_buy_points,
        risk_tags=risk_tags,
        focus_stress_state=focus_stress_state,
        temporal_battlefield_state=temporal_battlefield_state,
    )
    tactic_family = _tactic_family_from_battle_mode(battle_mode)
    buy_point_text = ",".join(f"{code}:{count}" for code, count in sorted(buy_point_counts.items())) or "-"
    buy_gate_text = ",".join(f"{code}:{count}" for code, count in sorted(buy_gate_counts.items())) or "-"
    filtered_buy_point_text = ",".join(f"{code}:{count}" for code, count in sorted(filtered_buy_points.items())) or "-"
    narrative_lines = (
        f"script={global_decision.market_script};main={narrative_main};"
        f"secondary={narrative_secondary};cap={global_decision.position_cap:.0%}",
        f"route_judge=hot_anchor_first;hot={hot_primary_text or '-'};"
        f"battlefield={temporal_battlefield_theme or '-'};state={temporal_battlefield_state or '-'};"
        f"raw_state={temporal_battlefield_state_raw or '-'};"
        f"handoff_to={handoff_to_text};handoff_evidence={handoff_evidence_count};"
        f"handoff_persist={handoff_persistence_ok};execution={execution_text};"
        f"battle_mode={battle_mode};tactic={tactic_family};primary={len(candidate_slice.primary)};watch={len(candidate_slice.watch)}",
        f"buy_points={buy_point_text};gates={buy_gate_text};filtered={filtered_buy_point_text};final={len(final_candidates)};"
        f"slice={len(candidate_slice.primary)}/{len(candidate_slice.watch)}/{len(candidate_slice.inactive)}/{len(candidate_slice.blocked)};"
        f"blocked={len(candidate_slice.blocked)};inactive={len(candidate_slice.inactive)}",
        f"playbook_active={','.join(matrix.active_playbooks) or '-'};"
        f"playbook_blocked={','.join(matrix.blocked_playbooks) or '-'};"
        f"playbook_watch_risk={next((note.split('=', 1)[1] for note in matrix.notes if note.startswith('watch_risk=')), '-')}",
        f"focus_stress={focus_stress_state};spread={focus_spread};themes={_metric_value(global_decision.trace, 'focus_stress_theme_count'):.0f};dragon_alone={_metric_value(global_decision.trace, 'focus_stress_dragon_alone_count'):.0f}",
        (
            f"hot_anchor={hot_anchor.anchor_state};primary={','.join(hot_anchor.primary_themes[:3]) or '-'};"
            f"continue={','.join(hot_anchor.continuation_themes[:3]) or '-'};"
            f"rotate={','.join(hot_anchor.rotation_themes[:3]) or '-'};"
            f"risk={','.join((hot_anchor.fading_themes + hot_anchor.fakeout_themes)[:3]) or '-'}"
            if hot_anchor is not None
            else "hot_anchor=-"
        ),
    )
    hot_metric_lines = tuple(
        _hot_metric_line_text(line) for line in tuple(getattr(hot_anchor, "metric_lines", ()) or ())[:4]
    ) if hot_anchor is not None else ()
    hot_lines = hot_metric_lines or (tuple(getattr(hot_anchor, "hot_evidence", ()) or ())[:4] if hot_anchor is not None else ())
    temporal_lines = tuple(getattr(temporal, "chain_summary", ()) or ()) if temporal is not None else ()
    hot_context_lines = tuple(
        f"{line.plate_name}:hot_rank={int(line.rank or 999)}/state={line.state}/"
        f"label={_hot_state_label(line.state)}/"
        f"style={getattr(line, 'style', 'unknown')}/"
        f"chg={float(line.change_pct or 0.0):.2f}/inflow={float(line.net_inflow_yi or 0.0):.2f}e/"
        f"2m={_fmt_amount_yuan(float(line.amount_2m or 0.0))}/front2m={int(line.front_2m_count or 0)}/"
        f"low_repair={int(getattr(line, 'low_open_repair_count', 0) or 0)}/"
        f"high_fail={int(getattr(line, 'high_open_fail_count', 0) or 0)}/"
        f"bucket={int(getattr(line, 'big_rise_count', 0) or 0)}/"
        f"{int(getattr(line, 'strong_rise_count', 0) or 0)}/"
        f"{int(getattr(line, 'slight_rise_count', 0) or 0)}/"
        f"{int(getattr(line, 'slight_fall_count', 0) or 0)}/"
        f"{int(getattr(line, 'strong_fall_count', 0) or 0)}/"
        f"{int(getattr(line, 'big_fall_count', 0) or 0)}/"
        f"sw={float(getattr(line, 'strong_weak_ratio', 0.0) or 0.0):.2f}"
        for line in tuple(getattr(hot_anchor, "metric_lines", ()) or ())[:3]
        if getattr(line, "plate_name", "")
    ) if hot_anchor is not None else ()
    battlefield_line = ""
    if temporal is not None:
        battlefield_main = str(getattr(temporal, "main_battlefield_theme", "") or "") or "-"
        battlefield_state_raw = str(getattr(temporal, "battlefield_state", "") or "") or "-"
        battlefield_state = _effective_temporal_battlefield_state(temporal, battlefield_state_raw) or "-"
        battlefield_handoff = (
            (str(getattr(temporal, "handoff_from", "") or "") or "-")
            + "->"
            + (str(getattr(temporal, "handoff_to", "") or "") or "-")
        )
        battlefield_line = (
            f"battlefield={battlefield_main};"
            f"state={battlefield_state};"
            f"raw_state={battlefield_state_raw};"
            f"handoff={battlefield_handoff};"
            f"rising={','.join(tuple(getattr(temporal, 'rising_hot_themes', ()) or ())[:3]) or '-'}"
        )
    migration_lines = ()
    if temporal_lines:
        migration_lines = ((battlefield_line,) if battlefield_line else ()) + temporal_lines[:5]
        if hot_context_lines:
            migration_lines = (*migration_lines, *hot_context_lines[:2])[:8]
    elif hot_context_lines:
        migration_lines = ((battlefield_line,) if battlefield_line else ()) + hot_context_lines[:3]
    else:
        migration_lines = ((battlefield_line,) if battlefield_line else ()) + hot_lines[:3]
    top_candidate_quant = tuple(
        (
            f"{candidate.symbol}:theme={candidate.theme_name};"
            f"2m={_fmt_amount_yuan(_metric_value(candidate.trace, 'amount_2m'))};"
            f"1m={_fmt_metric_pct(_metric_value(candidate.trace, 'speed_1m'))};"
            f"auction={_fmt_amount_yuan(_metric_value(candidate.trace, 'auction_amount'))};"
            f"open={_fmt_metric_pct(_metric_value(candidate.trace, 'open_pct'))};"
            f"now={_fmt_metric_pct(_metric_value(candidate.trace, 'current_pct'))}"
        )
        for candidate in final_candidates[:5]
    )
    pct_bucket_text = _evidence_value(tuple(global_decision.trace.metrics or ()), "market_pct_bucket", "-")
    top_turnover_sw_text = _evidence_value(tuple(global_decision.trace.metrics or ()), "top_turnover_sw", "-")
    quant_lines = (
        (
            f"global:confirmed={_metric_value(global_decision.trace, 'confirmed_attack_count'):.0f};"
            f"watch={_metric_value(global_decision.trace, 'partial_watch_count'):.0f};"
            f"risk={_metric_value(global_decision.trace, 'rejected_risk_count'):.0f};"
            f"hot_primary={_metric_value(global_decision.trace, 'hot_primary_count'):.0f};"
            f"focus_stress={_metric_value(global_decision.trace, 'focus_stress_theme_count'):.0f};"
            f"breadth={_metric_value(global_decision.trace, 'market_rising_count'):.0f}/"
            f"{_metric_value(global_decision.trace, 'market_falling_count'):.0f}/"
            f"{_metric_value(global_decision.trace, 'market_rising_rate'):.0%};"
            f"pct_bucket={pct_bucket_text};"
            f"strong_weak={_metric_value(global_decision.trace, 'market_strong_weak_ratio'):.2f};"
            f"turnover_sw={top_turnover_sw_text};"
            f"rel_hot_rank={_metric_value(global_decision.trace, 'relative_top_hot_rank', 999):.0f};"
            f"rel_hot_inflow={_metric_value(global_decision.trace, 'relative_top_hot_net_inflow_yi'):.2f}e;"
            f"cap={global_decision.position_cap:.0%}"
        ),
        _hot_quant_summary(hot_anchor),
        (
            f"time:state={temporal.exchange_state};targets={len(temporal.target_themes)};"
            f"battlefield={getattr(temporal, 'main_battlefield_theme', '') or '-'};"
            f"handoff_state={getattr(temporal, 'battlefield_state', '') or '-'};"
            f"fading={len(temporal.fading_themes)};"
            f"rolling_acc={_metric_value(temporal.trace, 'rolling_accelerating_count'):.0f};"
            f"rolling_out={_metric_value(temporal.trace, 'rolling_withdrawing_count'):.0f};"
            f"rolling_repair={_metric_value(temporal.trace, 'rolling_rebound_count'):.0f}"
            if temporal is not None
            else "time:state=-"
        ),
        *top_candidate_quant[:5],
    )
    invalidation_points = tuple(
        dict.fromkeys(
            point
            for point in (
                *tuple(global_decision.trace.invalidation_points or ()),
                *tuple(
                    point
                    for candidate in final_candidates
                    for point in tuple(candidate.trace.invalidation_points or ())
                ),
            )
        )
    )[:8]
    mode_note = (
        f"global={global_decision.market_script};"
        f"cap={global_decision.position_cap:.0%};"
        f"battle_mode={battle_mode};"
        f"tactic={tactic_family};"
        f"active={','.join(matrix.active_playbooks) or '-'};"
        f"blocked={','.join(matrix.blocked_playbooks) or '-'};"
        f"watch_risk={next((note.split('=', 1)[1] for note in matrix.notes if note.startswith('watch_risk=')), '-')}"
    )
    if temporal_text:
        mode_note = f"{mode_note};{temporal_text}"
    candidate_funnel_lines = ()
    if candidate_funnel_summary is not None:
        candidate_funnel_lines = (
            f"strategies={','.join(candidate_funnel_summary.strategy_counts) or '-'};"
            f"merged={candidate_funnel_summary.merged_count};"
            f"final={candidate_funnel_summary.final_count};"
            f"primary={candidate_funnel_summary.primary_count};"
            f"watch={candidate_funnel_summary.watch_count};"
            f"blocked={candidate_funnel_summary.blocked_count};"
            f"inactive={candidate_funnel_summary.inactive_count}",
            f"gates={','.join(candidate_funnel_summary.gate_reason_counts[:4]) or '-'};"
            f"samples={','.join(candidate_funnel_summary.strategy_samples[:4]) or '-'}",
        )
    return PlaybookOutputSummary(
        primary_symbols=primary,
        watch_symbols=watch,
        inactive_symbols=inactive,
        blocked_symbols=blocked,
        primary_actions=primary_actions,
        watch_actions=watch_actions,
        repair_actions=repair_actions,
        avoid_actions=avoid_actions,
        mode_note=mode_note,
        primary_reasons=primary_reasons,
        watch_reasons=watch_reasons,
        reject_reasons=reject_reasons,
        invalidation_points=invalidation_points,
        narrative_lines=narrative_lines,
        migration_lines=migration_lines,
        quant_lines=quant_lines,
        risk_tags=risk_tags,
        candidate_funnel_lines=candidate_funnel_lines,
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
    focus_stress = decision_bundle.focus_asset_stress_decision
    focus_stress_state = focus_stress.stress_state if focus_stress is not None else "unknown"
    focus_stress_ref = (focus_stress.trace.decision_id,) if focus_stress is not None else ()
    theme_relative = decision_bundle.theme_relative_decision
    temporal = decision_bundle.temporal_migration_decision
    hot_anchor = decision_bundle.hot_plate_anchor_decision
    relative_mainline_order = theme_relative.mainline_candidates if theme_relative is not None else ()
    relative_rotation_order = theme_relative.rotation_candidates if theme_relative is not None else ()
    relative_risk_order = theme_relative.risk_themes if theme_relative is not None else ()
    relative_ref = (theme_relative.trace.decision_id,) if theme_relative is not None else ()
    temporal_ref = (temporal.trace.decision_id,) if temporal is not None else ()
    temporal_targets = temporal.target_themes if temporal is not None else ()
    temporal_fading = temporal.fading_themes if temporal is not None else ()
    hot_ref = (hot_anchor.trace.decision_id,) if hot_anchor is not None else ()

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
                extra_local_refs=high_focus_ref + focus_stress_ref + relative_ref + temporal_ref + hot_ref,
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
                extra_local_refs=high_focus_ref + focus_stress_ref + relative_ref + temporal_ref + hot_ref,
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
                extra_local_refs=high_focus_ref + focus_stress_ref + relative_ref + temporal_ref + hot_ref,
                ),
                None,
            )
        )
    if focus_stress_state in {"theme_pressure", "market_risk_spread", "dragon_alone"}:
        hypothesis_items.append(
            (
                _build_hypothesis(
                    context=context,
                    script="focus_asset_stress",
                    theme=None,
                    claim="focus assets are retreating or only dragon survives, watch for rotation or risk spread",
                    required_validations=("focus_asset_stress",),
                    invalidation_points=("leader_repair", "mid_core_reclaims"),
                    trigger_refs=focus_stress_ref,
                    extra_local_refs=high_focus_ref + focus_stress_ref + relative_ref + temporal_ref + hot_ref,
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
                extra_local_refs=high_focus_ref + focus_stress_ref + relative_ref + temporal_ref + hot_ref,
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
        focus_stress,
        theme_relative.trace if theme_relative is not None else None,
        relative_risk_order,
        temporal,
        temporal.trace if temporal is not None else None,
        temporal_targets,
        temporal_fading,
        hot_anchor,
    )
    local_candidate_feeds = _build_local_candidate_feeds(
        decision_bundle,
        global_decision,
        temporal=temporal,
        hot_anchor=hot_anchor,
    )
    _log_local_strategy_feed_audit(
        context,
        local_candidate_feeds,
        global_decision=global_decision,
        temporal=temporal,
        hot_anchor=hot_anchor,
    )
    final_candidates = _build_final_candidates(
        context,
        decision_bundle,
        global_decision,
        local_candidate_feeds=local_candidate_feeds,
    )
    stable_trading_plan = _build_stable_trading_plan(
        context,
        decision_bundle,
        global_decision,
        final_candidates,
        local_candidate_feeds,
    )
    theme_process_board = _build_theme_process_board(
        context,
        decision_bundle,
        global_decision,
        final_candidates,
        local_candidate_feeds,
        stable_trading_plan,
    )
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
    playbook_candidate_slice = _build_playbook_candidate_slice(final_candidates, playbook_matrix)
    candidate_funnel_summary = _build_candidate_funnel_summary(
        local_candidate_feeds,
        final_candidates,
        playbook_candidate_slice,
    )
    funnel_summary, funnel_traces = _merge_hypothesis_funnel_debug(
        decision_bundle,
        final_candidates=final_candidates,
        candidate_slice=playbook_candidate_slice,
        candidate_funnel_summary=candidate_funnel_summary,
    )
    playbook_output_summary = _build_playbook_output_summary(
        candidate_slice=playbook_candidate_slice,
        final_candidates=final_candidates,
        global_decision=global_decision,
        matrix=playbook_matrix,
        temporal=temporal,
        hot_anchor=hot_anchor,
        candidate_funnel_summary=candidate_funnel_summary,
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
        f"focus_asset_stress={focus_stress_state}",
        f"focus_asset_themes={','.join(focus_stress.stressed_themes[:3]) if focus_stress is not None else '-'}",
        f"hot_anchor={hot_anchor.anchor_state if hot_anchor is not None else '-'}",
        f"hot_primary={','.join(hot_anchor.primary_themes[:3]) if hot_anchor is not None else '-'}",
        f"temporal_migration={temporal.exchange_state if temporal is not None else '-'}",
        f"temporal_targets={','.join(temporal_targets[:3]) or '-'}",
        f"final_candidates={len(final_candidates)}",
        f"final_buckets={bucket_summary}",
        f"stable_money_to={','.join(stable_trading_plan.money_to[:3]) or '-'}",
        f"stable_tactic={stable_trading_plan.best_tactic}",
        f"stable_candidates={','.join(item.symbol for item in stable_trading_plan.candidates) or '-'}",
        f"stable_no_candidate={stable_trading_plan.why_no_candidate or '-'}",
        f"theme_process_recheck={int(theme_process_board.recheck_required)}",
        f"theme_process_focus={theme_process_board.execution_focus_candidate or '-'}",
        f"candidate_funnel_strategies={','.join(candidate_funnel_summary.strategy_counts) or '-'}",
        f"candidate_funnel_merged={candidate_funnel_summary.merged_count}",
        f"candidate_funnel_gates={','.join(candidate_funnel_summary.gate_reason_counts[:4]) or '-'}",
        f"shadow_takeover={shadow_takeover.trace.state}",
        f"playbook_active={','.join(playbook_matrix.active_playbooks) or '-'}",
        f"playbook_cap={playbook_matrix.max_cap:.0%}",
        f"playbook_slice=primary:{len(playbook_candidate_slice.primary)},watch:{len(playbook_candidate_slice.watch)},inactive:{len(playbook_candidate_slice.inactive)},blocked:{len(playbook_candidate_slice.blocked)}",
        f"playbook_output=primary:{len(playbook_output_summary.primary_symbols)},watch:{len(playbook_output_summary.watch_symbols)},reject:{len(playbook_output_summary.reject_reasons)}",
    )
    return replace(
        decision_bundle,
        hypotheses=final_hypotheses,
        hypothesis_validations=final_validations,
        global_decision=global_decision,
        local_candidate_feeds=local_candidate_feeds,
        final_candidates=final_candidates,
        candidate_funnel_summary=candidate_funnel_summary,
        shadow_takeover_decision=shadow_takeover,
        playbook_control_matrix=playbook_matrix,
        playbook_candidate_slice=playbook_candidate_slice,
        playbook_output_summary=playbook_output_summary,
        stable_trading_plan=stable_trading_plan,
        theme_process_board=theme_process_board,
        funnel_summary=funnel_summary,
        funnel_traces=funnel_traces,
        notes=notes,
    )
