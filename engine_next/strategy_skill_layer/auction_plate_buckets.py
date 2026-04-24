from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from engine_next.domain.models import IntradayContext, StockStateSnapshot
from engine_next.runtime.plate_mapping_registry import is_generic_plate, split_plate_tokens


@dataclass(frozen=True)
class AuctionPlateBucketStat:
    plate_name: str
    weighted_score: float
    symbol_count: int
    auction_symbol_count: int
    auction_amount: float
    yest_limit_count: int
    leader_count: int
    hot_rank: int
    hot_change_pct: float
    hot_strength: float
    hot_net_inflow_yi: float
    hot_capital_behavior: float
    expectation: str
    sample_symbols: tuple[str, ...]
    generic: bool = False


def build_auction_plate_bucket_stats(
    context: IntradayContext,
    *,
    symbols: Iterable[str] | None = None,
    top_n: int = 5,
) -> tuple[AuctionPlateBucketStat, ...]:
    symbol_filter = {str(symbol) for symbol in symbols or () if str(symbol)}
    selected = [
        snapshot
        for snapshot in context.stock_snapshots
        if not symbol_filter or snapshot.symbol in symbol_filter
    ]
    if not selected:
        return ()

    hot_plate_map = context.session_facts.hot_plate_today_map or context.session_facts.hot_plate_yesterday_map
    if not hot_plate_map:
        hot_plate_map = context.hot_plate_map or context.yesterday_hot_plate_map
    bucket_payload: dict[str, dict[str, object]] = {}

    for snapshot in selected:
        theme_weights = _resolve_theme_weights(snapshot)
        if not theme_weights:
            continue
        for plate_name, weight in theme_weights:
            bucket = bucket_payload.setdefault(
                plate_name,
                {
                    "score": 0.0,
                    "symbols": set(),
                    "auction_symbols": set(),
                    "auction_amount": 0.0,
                    "yest_limit_count": 0,
                    "leader_count": 0,
                    "samples": [],
                },
            )
            bucket["score"] = float(bucket["score"]) + _score_snapshot(snapshot, weight, hot_plate_map.get(plate_name, {}))
            cast_symbols = bucket["symbols"]
            if isinstance(cast_symbols, set):
                cast_symbols.add(snapshot.symbol)
            if snapshot.auction_amount > 0:
                cast_auction_symbols = bucket["auction_symbols"]
                if isinstance(cast_auction_symbols, set):
                    cast_auction_symbols.add(snapshot.symbol)
                bucket["auction_amount"] = float(bucket["auction_amount"]) + snapshot.auction_amount * weight
            if snapshot.is_yest_limit:
                bucket["yest_limit_count"] = int(bucket["yest_limit_count"]) + 1
            if snapshot.leader_rank_in_theme <= 3:
                bucket["leader_count"] = int(bucket["leader_count"]) + 1
            sample_list = bucket["samples"]
            if isinstance(sample_list, list) and snapshot.symbol not in sample_list and len(sample_list) < 4:
                sample_list.append(snapshot.symbol)

    stats: list[AuctionPlateBucketStat] = []
    for plate_name, payload in bucket_payload.items():
        hot_payload = hot_plate_map.get(plate_name, {})
        hot_rank = int(_hot_field(hot_payload, "rank", default=999) or 999) if hot_payload else 999
        hot_change_pct = _hot_field(hot_payload, "change_pct") if hot_payload else 0.0
        hot_strength = _hot_strength(hot_payload) if hot_payload else 0.0
        hot_net_inflow_yi = _hot_field(hot_payload, "net_inflow_yi") if hot_payload else 0.0
        hot_capital_behavior = _hot_plate_capital_behavior_score(hot_change_pct, hot_net_inflow_yi)
        auction_amount = float(payload["auction_amount"])
        symbol_count = len(payload["symbols"]) if isinstance(payload["symbols"], set) else 0
        auction_symbol_count = len(payload["auction_symbols"]) if isinstance(payload["auction_symbols"], set) else 0
        yest_limit_count = int(payload["yest_limit_count"])
        leader_count = int(payload["leader_count"])
        generic = is_generic_plate(plate_name)
        expectation = _infer_expectation(
            generic=generic,
            hot_strength=hot_strength,
            hot_change_pct=hot_change_pct,
            hot_capital_behavior=hot_capital_behavior,
            auction_amount=auction_amount,
            symbol_count=symbol_count,
            auction_symbol_count=auction_symbol_count,
            yest_limit_count=yest_limit_count,
            leader_count=leader_count,
        )
        stats.append(
            AuctionPlateBucketStat(
                plate_name=plate_name,
                weighted_score=round(float(payload["score"]), 4),
                symbol_count=symbol_count,
                auction_symbol_count=auction_symbol_count,
                auction_amount=round(auction_amount, 2),
                yest_limit_count=yest_limit_count,
                leader_count=leader_count,
                hot_rank=hot_rank,
                hot_change_pct=round(hot_change_pct, 2),
                hot_strength=round(hot_strength, 2),
                hot_net_inflow_yi=round(hot_net_inflow_yi, 2),
                hot_capital_behavior=round(hot_capital_behavior, 4),
                expectation=expectation,
                sample_symbols=tuple(payload["samples"]) if isinstance(payload["samples"], list) else (),
                generic=generic,
            )
        )

    stats.sort(
        key=lambda item: (
            item.generic,
            -item.weighted_score,
            -item.hot_strength,
            -item.hot_capital_behavior,
            -item.hot_change_pct,
            -item.auction_amount,
            -item.symbol_count,
        )
    )
    return tuple(stats[:top_n])


def render_plate_bucket_summary(stats: Iterable[AuctionPlateBucketStat]) -> tuple[str, ...]:
    rows = list(stats)
    if not rows:
        return ("auction_plate_bucket | none",)
    top = rows[0]
    headline = (
        f"auction_plate_bucket | lead={top.plate_name}:{top.expectation} "
        f"| score={top.weighted_score:.2f} "
        f"| auc={top.auction_amount / 1e8:.2f}e "
        f"| hot_rank={top.hot_rank if top.hot_rank < 999 else '-'} "
        f"| symbols={top.symbol_count}"
    )
    detail_parts = []
    for row in rows[:3]:
        detail_parts.append(
            f"{row.plate_name}:{row.expectation}/auc={row.auction_amount / 1e8:.2f}e/n={row.symbol_count}"
        )
    return (
        headline,
        f"auction_plate_top3 | {' ; '.join(detail_parts)}",
    )


def _resolve_theme_weights(snapshot: StockStateSnapshot) -> tuple[tuple[str, float], ...]:
    candidates = []
    seen: set[str] = set()
    primary = ""
    if snapshot.plate:
        primary_tokens = split_plate_tokens(snapshot.plate)
        if primary_tokens:
            primary = primary_tokens[0]
    for raw in snapshot.real_plate_names:
        for token in split_plate_tokens(raw):
            if token in seen:
                continue
            seen.add(token)
            candidates.append(token)
    if primary and primary not in seen:
        candidates.insert(0, primary)
        seen.add(primary)
    elif primary:
        candidates = [primary] + [name for name in candidates if name != primary]

    if not candidates:
        return ()

    weights: list[tuple[str, float]] = []
    for idx, plate_name in enumerate(candidates[:3]):
        if idx == 0:
            weight = 1.0
        elif idx == 1:
            weight = 0.55
        else:
            weight = 0.3
        if is_generic_plate(plate_name):
            weight *= 0.18
        weights.append((plate_name, weight))
    return tuple(weights)


def _score_snapshot(
    snapshot: StockStateSnapshot,
    weight: float,
    hot_payload: Any,
) -> float:
    hot_strength = _hot_strength(hot_payload) if hot_payload else 0.0
    hot_change_pct = _hot_field(hot_payload, "change_pct") if hot_payload else 0.0
    hot_net_inflow_yi = _hot_field(hot_payload, "net_inflow_yi") if hot_payload else 0.0
    hot_bonus = min(hot_strength / 5000.0, 2.0) + _hot_plate_capital_behavior_score(hot_change_pct, hot_net_inflow_yi)
    leader_bonus = 0.8 if snapshot.leader_rank_in_theme <= 3 else 0.0
    yest_limit_bonus = 0.9 if snapshot.is_yest_limit else 0.0
    auction_score = min(snapshot.auction_amount / 80_000_000, 1.5)
    return weight * (auction_score + leader_bonus + yest_limit_bonus + hot_bonus)


def _infer_expectation(
    *,
    generic: bool,
    hot_strength: float,
    hot_change_pct: float,
    hot_capital_behavior: float,
    auction_amount: float,
    symbol_count: int,
    auction_symbol_count: int,
    yest_limit_count: int,
    leader_count: int,
) -> str:
    if generic:
        return "noise"
    if hot_strength >= 3000 and hot_capital_behavior >= 1.0 and auction_amount >= 80_000_000 and leader_count >= 1:
        return "mainline_attack"
    if hot_strength >= 1800 and hot_capital_behavior >= 0.4 and auction_symbol_count >= 2:
        return "hot_follow"
    if hot_capital_behavior <= -0.3 and hot_change_pct > 0:
        return "distribution"
    if yest_limit_count >= 2 and leader_count >= 1:
        return "ladder_extension"
    if symbol_count >= 3 and auction_symbol_count >= 2:
        return "cluster_move"
    return "observe"


def _hot_plate_capital_behavior_score(change_pct: float, net_inflow_yi: float) -> float:
    flow_signal = min(abs(net_inflow_yi), 20.0) / 20.0
    price_signal = min(abs(change_pct), 8.0) / 8.0
    if net_inflow_yi > 0 and change_pct > 0:
        score = 1.2 + flow_signal * 1.0 + price_signal * 0.6
    elif net_inflow_yi < 0 and change_pct > 0:
        score = -0.4 - flow_signal * 1.1 + price_signal * 0.2
    elif net_inflow_yi > 0 and change_pct <= 0:
        score = 0.35 + flow_signal * 0.85 - price_signal * 0.15
    elif net_inflow_yi < 0 and change_pct <= 0:
        score = -0.8 - flow_signal * 0.9 - price_signal * 0.3
    else:
        score = max(change_pct, 0.0) * 0.08
    return round(score, 4)


def _hot_field(payload: Any, field: str, *, default: float = 0.0) -> float:
    if isinstance(payload, dict):
        value = payload.get(field, default)
    else:
        value = getattr(payload, field, default)
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return float(default or 0.0)


def _hot_strength(payload: Any) -> float:
    strength = _hot_field(payload, "strength")
    if strength > 0.0:
        return strength
    return _hot_field(payload, "hot")
