from __future__ import annotations

from engine_next.domain.models import StockStateSnapshot
from engine_next.runtime.plate_mapping_registry import is_generic_plate, split_plate_tokens


def resolve_theme_names(snapshot: StockStateSnapshot) -> tuple[str, ...]:
    for raw_name in (snapshot.plate,):
        names = _resolve_names_from_raw(raw_name)
        if names:
            return names
    for raw_name in snapshot.real_plate_names:
        names = _resolve_names_from_raw(raw_name)
        if names:
            return names
    return ()


def resolve_primary_theme_name(snapshot: StockStateSnapshot) -> str:
    names = resolve_theme_names(snapshot)
    return names[0] if names else ""


def _resolve_names_from_raw(raw_name: object) -> tuple[str, ...]:
    names: list[str] = []
    for token in split_plate_tokens(raw_name):
        name = str(token or "").strip()
        if not name or is_generic_plate(name):
            continue
        if name not in names:
            names.append(name)
    return tuple(names)
