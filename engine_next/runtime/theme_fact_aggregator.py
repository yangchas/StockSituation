from __future__ import annotations

from dataclasses import dataclass, field
from heapq import nsmallest
from typing import Iterable

from engine_next.domain.models import StockStateSnapshot, ThemeFact, ThemeTradeFact
from engine_next.runtime.theme_name_resolver import resolve_theme_names


def _is_front_row(snapshot: StockStateSnapshot) -> bool:
    return snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1


def _front_row_2m_pass(snapshot: StockStateSnapshot) -> bool:
    if not _is_front_row(snapshot):
        return False
    if snapshot.amount_2m >= 30_000_000:
        return True
    if snapshot.auction_amount > 0 and snapshot.amount_2m >= snapshot.auction_amount * 0.9:
        return snapshot.current_pct >= snapshot.open_pct - 0.02
    return False


def _high_open_fail(snapshot: StockStateSnapshot) -> bool:
    return snapshot.open_pct >= 0.05 and snapshot.current_pct <= snapshot.open_pct - 0.03


def _low_open_repair(snapshot: StockStateSnapshot) -> bool:
    return snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000


def _expansion_candidate(snapshot: StockStateSnapshot) -> bool:
    return (
        snapshot.leader_rank_in_theme > 3
        and snapshot.current_pct >= 0.03
        and (snapshot.amount_2m >= 20_000_000 or snapshot.speed_1m > 0.008)
    )


@dataclass
class _ThemeAggregateBucket:
    symbols: list[StockStateSnapshot] = field(default_factory=list)
    symbol_set: set[str] = field(default_factory=set)
    open_pct_sum: float = 0.0
    auction_amount: float = 0.0
    amount_2m_sum: float = 0.0
    amount_5m_sum: float = 0.0
    red_open_count: int = 0
    front_row_count: int = 0
    front_row_red_count: int = 0
    leader_count_le3: int = 0
    leader_count_le2: int = 0
    yest_limit_count: int = 0
    yest_high_board_count: int = 0
    front_row_2m_pass_count: int = 0
    high_open_fail_count: int = 0
    low_open_repair_count: int = 0
    expansion_count: int = 0


def build_theme_fact_outputs(
    snapshots: Iterable[StockStateSnapshot],
    *,
    yesterday_hot_rank_by_plate: dict[str, int] | None = None,
) -> tuple[tuple[ThemeFact, ...], dict[str, ThemeTradeFact]]:
    buckets: dict[str, _ThemeAggregateBucket] = {}
    for snapshot in snapshots:
        theme_names = resolve_theme_names(snapshot)
        if not theme_names:
            continue
        for plate_name in theme_names:
            bucket = buckets.setdefault(plate_name, _ThemeAggregateBucket())
            bucket.symbols.append(snapshot)
            if snapshot.symbol:
                bucket.symbol_set.add(snapshot.symbol)
            bucket.open_pct_sum += float(snapshot.open_pct or 0.0)
            bucket.auction_amount += float(snapshot.auction_amount or 0.0)
            bucket.amount_2m_sum += float(snapshot.amount_2m or 0.0)
            bucket.amount_5m_sum += float(snapshot.amount_5m or 0.0)
            if snapshot.open_pct > 0:
                bucket.red_open_count += 1
            if _is_front_row(snapshot):
                bucket.front_row_count += 1
                if snapshot.open_pct > 0:
                    bucket.front_row_red_count += 1
            if snapshot.leader_rank_in_theme <= 3:
                bucket.leader_count_le3 += 1
            if snapshot.leader_rank_in_theme <= 2:
                bucket.leader_count_le2 += 1
            if snapshot.is_yest_limit:
                bucket.yest_limit_count += 1
                if snapshot.lb_days >= 2 or snapshot.t2_lb_days >= 2:
                    bucket.yest_high_board_count += 1
            if _front_row_2m_pass(snapshot):
                bucket.front_row_2m_pass_count += 1
            if _high_open_fail(snapshot):
                bucket.high_open_fail_count += 1
            if _low_open_repair(snapshot):
                bucket.low_open_repair_count += 1
            if _expansion_candidate(snapshot):
                bucket.expansion_count += 1

    theme_facts: list[ThemeFact] = []
    theme_trade_fact_map: dict[str, ThemeTradeFact] = {}
    hot_rank_map = yesterday_hot_rank_by_plate or {}
    for plate_name, bucket in buckets.items():
        ranked = nsmallest(
            3,
            bucket.symbols,
            key=lambda snapshot: (
                -snapshot.lb_days,
                snapshot.leader_rank_in_theme,
                -snapshot.auction_amount,
                -snapshot.current_pct,
            ),
        )
        top3_symbols = tuple(snapshot.symbol for snapshot in ranked if snapshot.symbol)
        symbol_count = max(len(bucket.symbol_set), len(bucket.symbols), 1)
        theme_facts.append(
            ThemeFact(
                plate_name=plate_name,
                leader_symbol=top3_symbols[0] if top3_symbols else "",
                top3_symbols=top3_symbols,
                symbol_count=len(bucket.symbol_set) or len(bucket.symbols),
                auction_amount=round(bucket.auction_amount, 2),
                yest_limit_count=bucket.yest_limit_count,
                leader_count=bucket.leader_count_le3,
            )
        )
        theme_trade_fact_map[plate_name] = ThemeTradeFact(
            plate_name=plate_name,
            yest_hot_rank=int(hot_rank_map.get(plate_name, 999) or 999),
            yest_limit_count=bucket.yest_limit_count,
            yest_high_board_count=bucket.yest_high_board_count,
            auction_amount=round(bucket.auction_amount, 2),
            red_open_count=bucket.red_open_count,
            red_open_rate=round(bucket.red_open_count / symbol_count, 4),
            avg_open_pct=round(bucket.open_pct_sum / symbol_count, 4),
            front_row_count=bucket.front_row_count,
            front_row_red_count=bucket.front_row_red_count,
            leader_count=bucket.leader_count_le2,
            amount_2m_sum=round(bucket.amount_2m_sum, 2),
            amount_5m_sum=round(bucket.amount_5m_sum, 2),
            front_row_2m_pass_count=bucket.front_row_2m_pass_count,
            high_open_fail_count=bucket.high_open_fail_count,
            low_open_repair_count=bucket.low_open_repair_count,
            expansion_count=bucket.expansion_count,
        )
    theme_facts.sort(
        key=lambda item: (
            -item.yest_limit_count,
            -item.leader_count,
            -item.auction_amount,
            -item.symbol_count,
            item.plate_name,
        )
    )
    return tuple(theme_facts), theme_trade_fact_map
