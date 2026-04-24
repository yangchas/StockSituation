from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable

from engine_next.runtime.rust_snapshot_bridge import RustSnapshotBridge


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


def _normalize_time_str(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "00:00:00"
    if len(text) == 5:
        return f"{text}:00"
    if len(text) == 6 and text.isdigit():
        return f"{text[:2]}:{text[2:4]}:{text[4:6]}"
    return text


def _normalize_quote_time_str(row: dict[str, Any]) -> str:
    timestamp_raw = row.get("timestamp", row.get("ts", 0))
    try:
        timestamp_ms = int(float(timestamp_raw or 0))
    except (TypeError, ValueError):
        timestamp_ms = 0
    if timestamp_ms > 0:
        return datetime.fromtimestamp(timestamp_ms / 1000.0).strftime("%H:%M:%S")
    return _normalize_time_str(row.get("time"))


class RustRuntimeFeed:
    """
    Runtime-side ingest helper for the engine_next Rust core.

    The feed keeps registration state local so the builder can push quote rows
    every cycle without repeatedly paying symbol/plate registration overhead.
    """

    def __init__(self, rust_bridge: RustSnapshotBridge | None = None) -> None:
        self._rust_bridge = rust_bridge or RustSnapshotBridge()
        self._registered_symbols: set[str] = set()
        self._plate_versions: dict[str, tuple[str, ...]] = {}

    @property
    def rust_bridge(self) -> RustSnapshotBridge:
        return self._rust_bridge

    def ingest_quotes(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        stock_plate_map: dict[str, str] | None = None,
    ) -> int:
        adapter = self._rust_bridge.adapter
        if not adapter or not getattr(adapter, "engine", None):
            return 0

        normalized_rows = []
        symbols_to_register: list[str] = []
        for row in rows:
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            normalized_rows.append((symbol, row))
            if symbol not in self._registered_symbols:
                self._registered_symbols.add(symbol)
                symbols_to_register.append(symbol)

        if symbols_to_register:
            adapter.register_symbols(symbols_to_register)

        if stock_plate_map:
            self._sync_plate_mapping(adapter, stock_plate_map, (symbol for symbol, _ in normalized_rows))

        pushed = 0
        for symbol, row in normalized_rows:
            try:
                adapter.push_tick_raw(
                    symbol,
                    float(row.get("price", 0.0) or 0.0),
                    float(row.get("amount", 0.0) or 0.0),
                    float(row.get("volume", 0.0) or 0.0),
                    _normalize_quote_time_str(row),
                    float(row.get("bid_amount", row.get("bid_amt", 0.0)) or 0.0),
                )
                pushed += 1
            except Exception:
                continue
        return pushed

    def _sync_plate_mapping(
        self,
        adapter: Any,
        stock_plate_map: dict[str, str],
        symbols: Iterable[str],
    ) -> None:
        grouped: dict[str, list[str]] = defaultdict(list)
        for symbol in symbols:
            plate = str(stock_plate_map.get(symbol) or "").strip()
            if not plate:
                continue
            grouped[plate].append(symbol)

        for plate, members in grouped.items():
            version = tuple(sorted(dict.fromkeys(members)))
            if self._plate_versions.get(plate) == version:
                continue
            try:
                adapter.register_plate_mapping(plate, list(version))
                self._plate_versions[plate] = version
            except Exception:
                continue
