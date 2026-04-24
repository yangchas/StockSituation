from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class QuoteContractV1:
    """
    Canonical hot quote contract for Engine V3.

    This contract exists to stop semantic drift in the current stack.
    It keeps 1m / 2m / 5m fields separate and never silently aliases them.
    """

    symbol: str
    event_ts_ms: int = 0
    ingest_ts_ms: int = 0

    price: float = 0.0
    pre_close: float = 0.0
    change_pct: float = 0.0
    volume: float = 0.0
    amount: float = 0.0
    large_net: float = 0.0

    change_rate_1min: float = 0.0
    amount_2min: float = 0.0

    change_5min: float = 0.0
    amount_5min: float = 0.0

    source: str = "unknown"
    contract_version: str = "quote.v1"
    quality_flags: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_mapping(
        cls,
        symbol: str,
        raw: Mapping[str, Any],
        *,
        source: str = "redis",
        ingest_ts_ms: int = 0,
    ) -> "QuoteContractV1":
        change_pct = _as_float(raw.get("change_pct", raw.get("change", 0.0)))
        event_ts_ms = _as_int(raw.get("timestamp", raw.get("ts", 0)))

        flags: list[str] = []
        if "amount_5min" in raw and "amount_2min" not in raw:
            flags.append("missing_amount_2min")
        if "change_5min" in raw and "change_rate_1min" not in raw:
            flags.append("missing_change_rate_1min")

        return cls(
            symbol=symbol,
            event_ts_ms=event_ts_ms,
            ingest_ts_ms=ingest_ts_ms,
            price=_as_float(raw.get("price", raw.get("lp", 0.0))),
            pre_close=_as_float(raw.get("pre_close", raw.get("close", raw.get("lc", 0.0)))),
            change_pct=change_pct,
            volume=_as_float(raw.get("volume", raw.get("v", 0.0))),
            amount=_as_float(raw.get("amount", raw.get("a", 0.0))),
            large_net=_as_float(raw.get("large_net", 0.0)),
            change_rate_1min=_as_float(raw.get("change_rate_1min", 0.0)),
            amount_2min=_as_float(raw.get("amount_2min", 0.0)),
            change_5min=_as_float(raw.get("change_5min", 0.0)),
            amount_5min=_as_float(raw.get("amount_5min", 0.0)),
            source=source,
            quality_flags=tuple(flags),
        )

    def to_redis_mapping(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "timestamp": str(self.event_ts_ms),
            "price": str(self.price),
            "pre_close": str(self.pre_close),
            "change_pct": str(self.change_pct),
            "volume": str(self.volume),
            "amount": str(self.amount),
            "large_net": str(self.large_net),
            "change_rate_1min": str(self.change_rate_1min),
            "amount_2min": str(self.amount_2min),
            "change_5min": str(self.change_5min),
            "amount_5min": str(self.amount_5min),
            "contract_version": self.contract_version,
            "source": self.source,
        }

    def effective_amount_2min(self) -> float:
        """
        Transitional compatibility helper.

        V3 keeps semantics separate, but for rollout we may temporarily fall
        back to the 5-minute amount when 2-minute amount is absent.
        """
        if self.amount_2min > 0:
            return self.amount_2min
        return self.amount_5min

    def effective_change_rate_1min(self) -> float:
        if self.change_rate_1min != 0:
            return self.change_rate_1min
        return self.change_5min
