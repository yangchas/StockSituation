from __future__ import annotations


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def resolve_market_regime(market_summary: object | None) -> str:
    """Resolve a simple market regime from existing summary fields.

    This module is intentionally small: it restores the shared source of truth
    needed by shape and entry logic without adding another scoring framework.
    """

    if market_summary is None:
        return "neutral"
    sentiment = _safe_float(getattr(market_summary, "sentiment_score", 0.0))
    promotion_rate = _safe_float(getattr(market_summary, "promotion_rate", 0.0))
    headshot_rate = _safe_float(getattr(market_summary, "headshot_rate", 0.0))
    red_open_rate = _safe_float(getattr(market_summary, "red_open_rate", 0.0))
    mainline_switch = bool(getattr(market_summary, "mainline_switch", False))
    attack_votes = 0
    defense_votes = 0
    if sentiment >= 5.8:
        attack_votes += 1
    elif sentiment <= 4.0:
        defense_votes += 1
    if promotion_rate >= 0.25:
        attack_votes += 1
    elif promotion_rate <= 0.10:
        defense_votes += 1
    if red_open_rate >= 0.58 and headshot_rate <= 0.05:
        attack_votes += 1
    elif red_open_rate <= 0.42 or headshot_rate >= 0.10:
        defense_votes += 1
    if mainline_switch and defense_votes == 0:
        attack_votes += 1
    if defense_votes >= 2:
        return "defense"
    if attack_votes >= 2:
        return "attack"
    return "neutral"


def market_is_defense(market_summary: object | None) -> bool:
    return resolve_market_regime(market_summary) == "defense"


def market_is_attack(market_summary: object | None) -> bool:
    return resolve_market_regime(market_summary) == "attack"
