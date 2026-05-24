from __future__ import annotations

def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default

def resolve_market_regime(market_summary: object) -> str:
    """
    Resolve the unified market regime from the market summary.
    Replaces the duplicated and conflicting logic in shape_engine and entry_strategy_matrix.
    Uses a 2-of-3 majority rule to determine attack/defense states,
    preventing over-sensitive flapping.
    """
    if market_summary is None:
        return "neutral"

    battle = str(getattr(market_summary, "battle_status", "") or "").lower()
    sentiment = _safe_float(getattr(market_summary, "sentiment_score", 0.0))
    promotion_rate = _safe_float(getattr(market_summary, "promotion_rate", 0.0))
    red_open_rate = _safe_float(getattr(market_summary, "red_open_rate", 0.0))
    headshot_rate = _safe_float(getattr(market_summary, "headshot_rate", 0.0))

    # 2-of-3 rule for defense
    defense_votes = 0
    if sentiment <= 4.5:
        defense_votes += 1
    if battle in {"bearish", "defense", "frozen"}:
        defense_votes += 1
    if red_open_rate <= 0.42 or headshot_rate >= 0.15:
        defense_votes += 1

    if defense_votes >= 2:
        return "defense"

    # 2-of-3 rule for attack
    attack_votes = 0
    if sentiment >= 6.2:
        attack_votes += 1
    if battle in {"bullish", "attack"}:
        attack_votes += 1
    if promotion_rate >= 0.32:
        attack_votes += 1

    if attack_votes >= 2:
        return "attack"

    return "neutral"

def market_is_defense(market_summary: object) -> bool:
    return resolve_market_regime(market_summary) == "defense"

def market_is_attack(market_summary: object) -> bool:
    return resolve_market_regime(market_summary) == "attack"
