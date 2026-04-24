from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from engine_v3.contracts.quote_contract import QuoteContractV1


@dataclass(slots=True)
class HotWindowRequest:
    symbol_ids: Sequence[int]
    now_ts_ms: int


@dataclass(slots=True)
class HotFeatureSnapshot:
    symbol_id: int
    event_ts_ms: int
    price: float
    change_pct: float
    change_rate_1min: float
    amount_2min: float
    amount_5min: float


class QuoteSourceAdapter(Protocol):
    """
    Pull or receive canonical quotes from the external world.

    Implementations may be based on Redis, SHM, mmap, Arrow IPC, or replay files.
    """

    async def fetch_quotes(self, symbols: Sequence[str]) -> dict[str, QuoteContractV1]:
        ...


class HotStateStore(Protocol):
    """
    Owns the L1 hot state.

    This is the primary candidate for Rust/C++ ownership.
    """

    def upsert_quotes(self, quotes: Sequence[QuoteContractV1]) -> int:
        ...

    def compute_windows(self, request: HotWindowRequest) -> list[HotFeatureSnapshot]:
        ...


class PlateAggregator(Protocol):
    """
    Aggregates indexed stock-level state into plate-level state.
    """

    def update_membership(self, symbol_to_plates: dict[int, list[int]]) -> None:
        ...

    def calculate_spread(self, feature_rows: Sequence[HotFeatureSnapshot]) -> dict[int, float]:
        ...


class StrategyOrchestrator(Protocol):
    """
    Consumes stable hot features and warm context to produce business outputs.
    """

    async def on_feature_snapshot(self, features: Sequence[HotFeatureSnapshot]) -> None:
        ...
