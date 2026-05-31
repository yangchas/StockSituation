from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from engine_next.domain.models import StockStateSnapshot


def top_symbols_by_amount(
    snapshots: Iterable[StockStateSnapshot],
    attr_name: str,
    *,
    top_n: int,
) -> frozenset[str]:
    rows = [
        (snapshot.symbol, float(getattr(snapshot, attr_name, 0.0) or 0.0))
        for snapshot in snapshots
    ]
    rows = [row for row in rows if row[1] > 0.0]
    rows.sort(key=lambda row: row[1], reverse=True)
    return frozenset(symbol for symbol, _ in rows[:top_n])


def rank_pct_by_amount(
    snapshots: Iterable[StockStateSnapshot],
    attr_name: str,
) -> dict[str, float]:
    rows = [
        (snapshot.symbol, float(getattr(snapshot, attr_name, 0.0) or 0.0))
        for snapshot in snapshots
    ]
    rows = [row for row in rows if row[1] > 0.0]
    rows.sort(key=lambda row: row[1], reverse=True)
    if not rows:
        return {}
    if len(rows) == 1:
        return {rows[0][0]: 0.0}
    return {
        symbol: round(index / (len(rows) - 1), 4)
        for index, (symbol, _amount) in enumerate(rows)
    }


def enrich_snapshot_amount_rank_pcts(
    snapshots: Iterable[StockStateSnapshot],
) -> tuple[StockStateSnapshot, ...]:
    rows = tuple(snapshots)
    amount_2m_rank = rank_pct_by_amount(rows, "amount_2m")
    amount_5m_rank = rank_pct_by_amount(rows, "amount_5m")
    if not amount_2m_rank and not amount_5m_rank:
        return rows
    return tuple(
        replace(
            snapshot,
            amount_2m_rank_pct=float(amount_2m_rank.get(snapshot.symbol, 1.0)),
            amount_5m_rank_pct=float(amount_5m_rank.get(snapshot.symbol, 1.0)),
        )
        for snapshot in rows
    )


def relative_amount_floor(
    snapshots: Iterable[StockStateSnapshot],
    attr_name: str,
    *,
    top_n: int,
    fallback: float,
) -> float:
    values = [
        float(getattr(snapshot, attr_name, 0.0) or 0.0)
        for snapshot in snapshots
        if float(getattr(snapshot, attr_name, 0.0) or 0.0) > 0.0
    ]
    values.sort(reverse=True)
    if not values:
        return float(fallback or 0.0)
    index = min(max(top_n - 1, 0), len(values) - 1)
    return max(float(values[index] or 0.0), float(fallback or 0.0) * 0.25)


def is_relative_amount_top(
    snapshots: Iterable[StockStateSnapshot],
    snapshot: StockStateSnapshot,
    attr_name: str,
    *,
    top_n: int,
    fallback: float,
) -> bool:
    amount = float(getattr(snapshot, attr_name, 0.0) or 0.0)
    if amount <= 0.0:
        return False
    floor = relative_amount_floor(snapshots, attr_name, top_n=top_n, fallback=fallback)
    return amount >= floor


def is_relative_amount_top_or_fallback(
    snapshots: Iterable[StockStateSnapshot] | None,
    snapshot: StockStateSnapshot,
    attr_name: str,
    *,
    top_n: int,
    fallback: float,
) -> bool:
    amount = float(getattr(snapshot, attr_name, 0.0) or 0.0)
    if amount <= 0.0:
        return False
    if snapshots is None:
        return amount >= float(fallback or 0.0)
    return is_relative_amount_top(snapshots, snapshot, attr_name, top_n=top_n, fallback=fallback)


def snapshot_amount_2m_top(
    snapshot: StockStateSnapshot,
    *,
    max_rank_pct: float,
    fallback: float,
) -> bool:
    amount_2m = float(getattr(snapshot, "amount_2m", 0.0) or 0.0)
    if amount_2m <= 0.0:
        return False
    rank_pct = float(getattr(snapshot, "amount_2m_rank_pct", 1.0) or 1.0)
    if 0.0 <= rank_pct < 1.0:
        return rank_pct <= max_rank_pct
    return amount_2m >= float(fallback or 0.0)
