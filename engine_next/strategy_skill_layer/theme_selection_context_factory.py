from __future__ import annotations

from engine_next.domain.models import ThemeSelectionContext
from engine_next.strategy_skill_layer.theme_trade_conclusions import classify_theme_trade_conclusion


def normalize_theme_fakeout_level(raw_level: str) -> str:
    text = str(raw_level or "").strip().lower()
    if text in {"strong", "high"}:
        return "high"
    if text in {"warn", "medium"}:
        return "medium"
    if text in {"low"}:
        return "low"
    return text or "unknown"


def resolve_theme_trade_conclusion(
    *,
    theme_trade_label: str,
    open_confirm_state: str,
    fakeout_level: str,
    high_open_fail_count: int = 0,
    low_open_repair_count: int = 0,
    expansion_count: int = 0,
    leader_count: int = 0,
    yest_limit_count: int = 0,
) -> str:
    return classify_theme_trade_conclusion(
        theme_trade_label=theme_trade_label,
        open_confirm_state=open_confirm_state,
        fakeout_level=fakeout_level,
        high_open_fail_count=high_open_fail_count,
        low_open_repair_count=low_open_repair_count,
        expansion_count=expansion_count,
        leader_count=leader_count,
        yest_limit_count=yest_limit_count,
    )


def build_theme_selection_context(
    *,
    plate_name: str,
    e_score: float,
    a_score: float,
    x_score: float,
    market_regime: str = "neutral",
    theme_trade_label: str = "unknown",
    open_confirm_state: str = "unknown",
    fakeout_level: str = "unknown",
    cohesion_level: str = "unknown",
    tradable: bool = False,
    bias_action: str = "observe_only",
    plate_strength_rank_pct: float = 1.0,
    plate_delta_rank_pct: float = 1.0,
    plate_breadth_score: float = 0.0,
    plate_follow_through_score: float = 0.0,
    plate_resistance_score: float = 0.0,
    plate_role: str = "neutral",
    rotation_bias: str = "neutral",
    phase_priority_bias: float = 0.0,
    notes: tuple[str, ...] = (),
    trade_conclusion: str | None = None,
    high_open_fail_count: int = 0,
    low_open_repair_count: int = 0,
    expansion_count: int = 0,
    leader_count: int = 0,
    yest_limit_count: int = 0,
) -> ThemeSelectionContext:
    normalized_fakeout_level = normalize_theme_fakeout_level(fakeout_level)
    resolved_trade_conclusion = trade_conclusion or resolve_theme_trade_conclusion(
        theme_trade_label=theme_trade_label,
        open_confirm_state=open_confirm_state,
        fakeout_level=normalized_fakeout_level,
        high_open_fail_count=high_open_fail_count,
        low_open_repair_count=low_open_repair_count,
        expansion_count=expansion_count,
        leader_count=leader_count,
        yest_limit_count=yest_limit_count,
    )
    return ThemeSelectionContext(
        plate_name=plate_name,
        e_score=round(float(e_score or 0.0), 2),
        a_score=round(float(a_score or 0.0), 2),
        x_score=round(float(x_score or 0.0), 2),
        market_regime=market_regime,
        theme_trade_label=theme_trade_label,
        trade_conclusion=resolved_trade_conclusion,
        fakeout_level=normalized_fakeout_level,
        cohesion_level=cohesion_level,
        tradable=tradable,
        bias_action=bias_action,
        open_confirm_state=open_confirm_state,
        plate_strength_rank_pct=plate_strength_rank_pct,
        plate_delta_rank_pct=plate_delta_rank_pct,
        plate_breadth_score=plate_breadth_score,
        plate_follow_through_score=plate_follow_through_score,
        plate_resistance_score=plate_resistance_score,
        plate_role=plate_role,
        rotation_bias=rotation_bias,
        phase_priority_bias=round(float(phase_priority_bias or 0.0), 2),
        notes=notes,
    )
