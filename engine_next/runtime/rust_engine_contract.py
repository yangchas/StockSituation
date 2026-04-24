from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class RustEngineSource(str, Enum):
    ENGINE_NEXT = "engine_next"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class RustEngineStatus:
    source: RustEngineSource
    available: bool
    supports_tick_push: bool
    supports_symbol_registration: bool
    supports_plate_mapping: bool
    supports_snapshot: bool
    supports_market_extremes: bool
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RustSnapshotContract:
    symbol: str
    price: float
    amount: float
    speed_1m: float
    amount_2m: float
    amount_5m: float
    vector_3m: float
    vector_5m: float
    bid_amount: float
    max_price: float
    min_price: float
    p0920: float
    p0924: float
    p0925: float
    source: str


@runtime_checkable
class RustEngineAdapterProtocol(Protocol):
    engine: Any | None

    def get_snapshot(self) -> dict[str, Any]:
        ...

    def register_symbols(self, symbols: list[str]) -> None:
        ...

    def register_plate_mapping(self, plate_id: str, symbols: list[str]) -> None:
        ...

    def push_tick_raw(
        self,
        symbol: str,
        price: float,
        amount: float,
        volume: float,
        time_str: str = "00:00:00",
        bid_amount: float = 0.0,
    ) -> None:
        ...
