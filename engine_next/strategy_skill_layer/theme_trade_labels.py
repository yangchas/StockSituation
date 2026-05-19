from __future__ import annotations

from typing import Any

from engine_next.domain.models import ThemeTradeFact

_HIGH_EVENT_KEYWORDS = (
    "股权转让",
    "实控人变更",
    "举牌",
    "并购重组",
)


def is_high_event_theme(plate_name: str) -> bool:
    text = str(plate_name or "").strip()
    return any(keyword in text for keyword in _HIGH_EVENT_KEYWORDS)


def classify_theme_trade_label(fact: ThemeTradeFact) -> str:
    if is_high_event_theme(fact.plate_name):
        return "high_event"
    if fact.yest_hot_rank <= 10 or fact.yest_limit_count >= 3 or fact.yest_high_board_count >= 1:
        return "old_mainline"
    if (
        fact.leader_count >= 1
        and fact.front_row_count >= 1
        and fact.expansion_count == 0
        and fact.front_row_2m_pass_count <= 1
        and fact.high_open_fail_count >= 1
    ):
        return "independent_hug"
    if (
        fact.auction_amount >= 50_000_000
        and fact.red_open_rate >= 0.35
        and fact.front_row_count >= 1
        and fact.amount_2m_sum <= 0
        and fact.amount_5m_sum <= 0
    ):
        return "switch_candidate"
    if (
        fact.auction_amount >= 50_000_000
        and fact.red_open_rate >= 0.35
        and (fact.front_row_count >= 2 or fact.leader_count >= 1)
        and (fact.front_row_2m_pass_count >= 1 or fact.expansion_count >= 1)
    ):
        return "switch_candidate"
    return "rotation_noise"


def classify_theme_trade_label_from_collision(
    plate_name: str,
    row: Any,
    *,
    yesterday_hot_rank: int = 999,
) -> str:
    if is_high_event_theme(plate_name):
        return "high_event"
    yest_limit_count = int(getattr(row, "yest_limit_count", 0) or 0)
    highest_lb_days = int(getattr(row, "highest_lb_days", 0) or 0)
    auction_amount = float(getattr(row, "auction_amount", 0.0) or 0.0)
    red_count = int(getattr(row, "red_count", 0) or 0)
    symbol_count = int(getattr(row, "symbol_count", 0) or 0)
    leader_count = int(getattr(row, "leader_count", 0) or 0)
    auction_symbol_count = int(getattr(row, "auction_symbol_count", 0) or 0)
    limit_up_count = int(getattr(row, "limit_up_count", 0) or 0)
    turn_strong_count = int(getattr(row, "turn_strong_count", 0) or 0)
    avg_open_pct = float(getattr(row, "avg_open_pct", 0.0) or 0.0)
    red_open_rate = (red_count / symbol_count) if symbol_count > 0 else 0.0

    if yesterday_hot_rank <= 10 or yest_limit_count >= 3 or highest_lb_days >= 3:
        return "old_mainline"
    if (
        leader_count >= 1
        and auction_symbol_count <= 2
        and limit_up_count <= 1
        and turn_strong_count <= 1
        and avg_open_pct <= 0.02
    ):
        return "independent_hug"
    if (
        auction_amount >= 50_000_000
        and (leader_count >= 1 or auction_symbol_count >= 2)
        and (red_open_rate >= 0.35 or avg_open_pct >= 0.0 or red_count == 0)
    ):
        return "switch_candidate"
    return "rotation_noise"
