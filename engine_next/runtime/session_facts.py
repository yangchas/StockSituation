from __future__ import annotations

from dataclasses import dataclass
from heapq import nsmallest
from typing import Any, Iterable

from engine_next.domain.models import (
    HotPlateFact,
    LadderFact,
    PlateMigrationFact,
    SessionFacts,
    StockStateSnapshot,
    ThemeFact,
)
from engine_next.runtime.plate_mapping_registry import is_generic_plate, split_plate_tokens


def session_facts_to_payload(facts: SessionFacts) -> dict[str, Any]:
    return {
        "fact_set_id": facts.fact_set_id,
        "hot_plate_today": [fact.__dict__ for fact in facts.hot_plate_today],
        "hot_plate_yesterday": [fact.__dict__ for fact in facts.hot_plate_yesterday],
        "plate_migration": [fact.__dict__ for fact in facts.plate_migration],
        "theme_facts": [fact.__dict__ for fact in facts.theme_facts],
        "ladder_facts": [fact.__dict__ for fact in facts.ladder_facts],
    }


def session_facts_from_payload(payload: dict[str, Any]) -> SessionFacts:
    hot_plate_today = tuple(HotPlateFact(**item) for item in payload.get("hot_plate_today", ()) if isinstance(item, dict))
    hot_plate_yesterday = tuple(
        HotPlateFact(**item) for item in payload.get("hot_plate_yesterday", ()) if isinstance(item, dict)
    )
    plate_migration = tuple(
        PlateMigrationFact(**item) for item in payload.get("plate_migration", ()) if isinstance(item, dict)
    )
    theme_facts = tuple(ThemeFact(**item) for item in payload.get("theme_facts", ()) if isinstance(item, dict))
    ladder_facts = tuple(LadderFact(**item) for item in payload.get("ladder_facts", ()) if isinstance(item, dict))
    return SessionFacts(
        fact_set_id=str(payload.get("fact_set_id") or ""),
        hot_plate_today=hot_plate_today,
        hot_plate_today_map={fact.plate_name: fact for fact in hot_plate_today},
        hot_plate_yesterday=hot_plate_yesterday,
        hot_plate_yesterday_map={fact.plate_name: fact for fact in hot_plate_yesterday},
        plate_migration=plate_migration,
        plate_migration_map={fact.plate_name: fact for fact in plate_migration},
        theme_facts=theme_facts,
        theme_fact_map={fact.plate_name: fact for fact in theme_facts},
        ladder_facts=ladder_facts,
        ladder_fact_map={fact.key: fact for fact in ladder_facts},
    )


def _hot_plate_strength(payload: dict[str, Any]) -> float:
    strength = float(payload.get("strength", 0.0) or 0.0)
    if strength > 0.0:
        return strength
    return float(payload.get("hot", 0.0) or 0.0)


def _hot_plate_rank(payload: dict[str, Any]) -> int:
    try:
        rank = int(payload.get("rank", 999) or 999)
    except (TypeError, ValueError):
        rank = 999
    return rank if rank > 0 else 999


def _hot_plate_sort_key(fact: HotPlateFact) -> tuple[float, float, float, float, int, str]:
    return (
        -fact.strength,
        -fact.change_pct,
        -fact.net_inflow_yi,
        -fact.hot,
        fact.rank,
        fact.plate_name,
    )


def _resolve_theme_names(snapshot: StockStateSnapshot) -> tuple[str, ...]:
    names: list[str] = []
    for raw_name in (snapshot.plate, *snapshot.real_plate_names):
        for token in split_plate_tokens(raw_name):
            name = str(token or "").strip()
            if not name or is_generic_plate(name) or name in names:
                continue
            names.append(name)
    return tuple(names)


@dataclass
class _ThemeAccumulator:
    symbols: list[StockStateSnapshot]
    auction_amount: float = 0.0
    yest_limit_count: int = 0
    leader_count: int = 0


@dataclass
class _LadderAccumulator:
    snapshots: list[StockStateSnapshot]
    red_open_count: int = 0
    promoted_count: int = 0


def build_session_facts(
    *,
    trade_date: str,
    phase_name: str,
    snapshots: Iterable[StockStateSnapshot],
    hot_plate_map: dict[str, dict[str, Any]],
    yesterday_hot_plate_map: dict[str, dict[str, Any]],
) -> SessionFacts:
    snapshots = tuple(snapshots)
    today_facts = _build_hot_plate_facts(hot_plate_map)
    yesterday_facts = _build_hot_plate_facts(yesterday_hot_plate_map)
    today_map = {fact.plate_name: fact for fact in today_facts}
    yesterday_map = {fact.plate_name: fact for fact in yesterday_facts}
    migration = _build_plate_migration_facts(today_map, yesterday_map)
    theme_facts = _build_theme_facts(snapshots)
    ladder_facts = _build_ladder_facts(snapshots)
    return SessionFacts(
        fact_set_id=f"{trade_date}:{phase_name}:h{len(today_facts)}:t{len(theme_facts)}:l{len(ladder_facts)}",
        hot_plate_today=today_facts,
        hot_plate_today_map=today_map,
        hot_plate_yesterday=yesterday_facts,
        hot_plate_yesterday_map=yesterday_map,
        plate_migration=migration,
        plate_migration_map={fact.plate_name: fact for fact in migration},
        theme_facts=theme_facts,
        theme_fact_map={fact.plate_name: fact for fact in theme_facts},
        ladder_facts=ladder_facts,
        ladder_fact_map={fact.key: fact for fact in ladder_facts},
    )


def _build_hot_plate_facts(source_map: dict[str, dict[str, Any]]) -> tuple[HotPlateFact, ...]:
    facts: list[HotPlateFact] = []
    for plate_name, payload in source_map.items():
        if not isinstance(payload, dict):
            continue
        facts.append(
            HotPlateFact(
                plate_name=str(plate_name or "").strip(),
                rank=_hot_plate_rank(payload),
                strength=round(_hot_plate_strength(payload), 2),
                change_pct=round(float(payload.get("change_pct", 0.0) or 0.0), 3),
                net_inflow_yi=round(float(payload.get("net_inflow_yi", 0.0) or 0.0), 2),
                hot=round(float(payload.get("hot", 0.0) or 0.0), 2),
            )
        )
    facts.sort(key=_hot_plate_sort_key)
    return tuple(facts)


def _build_plate_migration_facts(
    today_map: dict[str, HotPlateFact],
    yesterday_map: dict[str, HotPlateFact],
) -> tuple[PlateMigrationFact, ...]:
    rows: list[PlateMigrationFact] = []
    for plate_name in sorted(set(today_map) | set(yesterday_map)):
        today = today_map.get(plate_name)
        yesterday = yesterday_map.get(plate_name)
        rows.append(
            PlateMigrationFact(
                plate_name=plate_name,
                today_strength=today.strength if today else 0.0,
                yesterday_strength=yesterday.strength if yesterday else 0.0,
                strength_delta=round((today.strength if today else 0.0) - (yesterday.strength if yesterday else 0.0), 2),
                today_change_pct=today.change_pct if today else 0.0,
                yesterday_change_pct=yesterday.change_pct if yesterday else 0.0,
                change_pct_delta=round((today.change_pct if today else 0.0) - (yesterday.change_pct if yesterday else 0.0), 3),
                today_net_inflow_yi=today.net_inflow_yi if today else 0.0,
                yesterday_net_inflow_yi=yesterday.net_inflow_yi if yesterday else 0.0,
                net_inflow_yi_delta=round(
                    (today.net_inflow_yi if today else 0.0) - (yesterday.net_inflow_yi if yesterday else 0.0),
                    2,
                ),
                present_today=today is not None,
                present_yesterday=yesterday is not None,
            )
        )
    rows.sort(
        key=lambda item: (
            -(item.today_strength if item.present_today else item.yesterday_strength),
            -item.strength_delta,
            -item.change_pct_delta,
            -item.net_inflow_yi_delta,
            item.plate_name,
        )
    )
    return tuple(rows)


def _build_theme_facts(snapshots: Iterable[StockStateSnapshot]) -> tuple[ThemeFact, ...]:
    buckets: dict[str, _ThemeAccumulator] = {}
    for snapshot in snapshots:
        theme_names = _resolve_theme_names(snapshot)
        if not theme_names:
            continue
        for plate_name in theme_names:
            bucket = buckets.setdefault(plate_name, _ThemeAccumulator(symbols=[]))
            bucket.symbols.append(snapshot)
            bucket.auction_amount += float(snapshot.auction_amount or 0.0)
            bucket.yest_limit_count += int(bool(snapshot.is_yest_limit))
            if snapshot.leader_rank_in_theme <= 3:
                bucket.leader_count += 1

    facts: list[ThemeFact] = []
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
        top3_symbols = tuple(snapshot.symbol for snapshot in ranked)
        facts.append(
            ThemeFact(
                plate_name=plate_name,
                leader_symbol=top3_symbols[0] if top3_symbols else "",
                top3_symbols=top3_symbols,
                symbol_count=len({snapshot.symbol for snapshot in bucket.symbols}),
                auction_amount=round(bucket.auction_amount, 2),
                yest_limit_count=bucket.yest_limit_count,
                leader_count=bucket.leader_count,
            )
        )
    facts.sort(
        key=lambda item: (
            -item.yest_limit_count,
            -item.leader_count,
            -item.auction_amount,
            -item.symbol_count,
            item.plate_name,
        )
    )
    return tuple(facts)


def _build_ladder_facts(snapshots: Iterable[StockStateSnapshot]) -> tuple[LadderFact, ...]:
    transitions: dict[str, _LadderAccumulator] = {}
    fallback_groups: dict[str, _LadderAccumulator] = {}
    for snapshot in snapshots:
        if snapshot.is_yest_limit and snapshot.lb_days >= 1:
            key = f"{max(snapshot.lb_days - 1, 0)}B->{snapshot.lb_days}B"
            bucket = transitions.setdefault(key, _LadderAccumulator(snapshots=[]))
        elif snapshot.lb_days >= 2:
            key = f"{snapshot.lb_days}B"
            bucket = fallback_groups.setdefault(key, _LadderAccumulator(snapshots=[]))
        else:
            continue
        bucket.snapshots.append(snapshot)
        if snapshot.open_pct > 0:
            bucket.red_open_count += 1
        if snapshot.is_locked or snapshot.current_pct >= 0.098:
            bucket.promoted_count += 1

    groups = transitions or fallback_groups
    facts: list[LadderFact] = []
    for key, bucket in groups.items():
        rep = min(
            bucket.snapshots,
            key=lambda snapshot: (
                snapshot.leader_rank_in_theme,
                -snapshot.current_pct,
                -snapshot.auction_amount,
            ),
        )
        facts.append(
            LadderFact(
                key=key,
                total_count=len(bucket.snapshots),
                red_open_count=bucket.red_open_count,
                promoted_count=bucket.promoted_count,
                representative_symbol=rep.symbol,
            )
        )
    facts.sort(key=lambda item: (-_ladder_sort_value(item.key), -item.total_count, item.key))
    return tuple(facts)


def _ladder_sort_value(key: str) -> int:
    if "->" in key:
        start, _, end = key.partition("->")
        try:
            return int(end.replace("B", ""))
        except ValueError:
            return 0
    try:
        return int(key.replace("B", ""))
    except ValueError:
        return 0
