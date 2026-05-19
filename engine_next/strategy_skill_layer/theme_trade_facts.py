from __future__ import annotations

from engine_next.domain.models import IntradayContext, StockStateSnapshot, ThemeTradeFact
from engine_next.runtime.theme_fact_aggregator import build_theme_fact_outputs


def build_theme_trade_fact_map(
    context: IntradayContext,
    snapshots: tuple[StockStateSnapshot, ...],
    *,
    resolve_theme_name,
) -> dict[str, ThemeTradeFact]:
    cached_map = getattr(context.session_facts, "theme_trade_fact_map", None)
    if isinstance(cached_map, dict) and cached_map:
        context_symbols = tuple(snapshot.symbol for snapshot in context.stock_snapshots if snapshot.symbol)
        snapshot_symbols = tuple(snapshot.symbol for snapshot in snapshots if snapshot.symbol)
        if len(snapshot_symbols) == len(context_symbols) and snapshot_symbols == context_symbols:
            return dict(cached_map)

    yesterday_hot_rank_by_plate: dict[str, int] = {}
    for plate_name, payload in context.yesterday_hot_plate_map.items():
        if not isinstance(payload, dict):
            continue
        try:
            rank = int(payload.get("rank", 999) or 999)
        except (TypeError, ValueError):
            rank = 999
        yesterday_hot_rank_by_plate[str(plate_name or "").strip()] = rank if rank > 0 else 999
    _, fact_map = build_theme_fact_outputs(
        snapshots,
        yesterday_hot_rank_by_plate=yesterday_hot_rank_by_plate,
    )
    if resolve_theme_name is None:
        return fact_map
    filtered_map: dict[str, ThemeTradeFact] = {}
    for snapshot in snapshots:
        plate_name = str(resolve_theme_name(snapshot) or "").strip()
        if plate_name and plate_name in fact_map:
            filtered_map[plate_name] = fact_map[plate_name]
    return filtered_map
