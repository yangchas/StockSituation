from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


def _parse_timestamp_ms(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _minute_index_from_quote(*, timestamp_ms: Any = None, time_text: Any = None) -> int | None:
    parsed_timestamp_ms = _parse_timestamp_ms(timestamp_ms)
    if parsed_timestamp_ms > 0:
        dt = datetime.fromtimestamp(parsed_timestamp_ms / 1000.0)
        return dt.hour * 60 + dt.minute

    text = str(time_text or "").strip()
    if not text:
        return None
    if len(text) == 5:
        text = f"{text}:00"
    if len(text) == 6 and text.isdigit():
        text = f"{text[:2]}:{text[2:4]}:{text[4:6]}"
    parts = text.split(":")
    if len(parts) < 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour * 60 + minute


@dataclass(frozen=True)
class TickWindowMetrics:
    symbol: str
    minute_index: int
    last_price: float
    last_amount: float
    speed_1m: float
    amount_2m: float


class TickWindowTracker:
    """
    Lightweight minute-sliced tracker aligned with engine_v2.tick_history semantics.

    It stores one compact `(price, amount)` point per symbol per minute, keeps only
    the recent rolling window, and exposes `speed_1m` plus `amount_2m`.
    """

    def __init__(self, *, keep_minutes: int = 5) -> None:
        self._keep_minutes = max(keep_minutes, 3)
        self._history: dict[str, dict[int, tuple[float, float]]] = {}

    def ingest_quote(
        self,
        symbol: str,
        *,
        price: float,
        amount: float,
        timestamp_ms: Any = None,
        time_text: Any = None,
        minute_index: int | None = None,
    ) -> TickWindowMetrics | None:
        code = _normalize_symbol(symbol)
        if not code:
            return None
        try:
            price_f = float(price or 0.0)
            amount_f = float(amount or 0.0)
        except (TypeError, ValueError):
            return None
        if price_f <= 0.0 or amount_f < 0.0:
            return None

        minute = minute_index
        if minute is None:
            minute = _minute_index_from_quote(timestamp_ms=timestamp_ms, time_text=time_text)
        if minute is None:
            minute = int(time.time() // 60)
        bucket = self._history.setdefault(code, {})
        existing = bucket.get(minute)
        if existing is not None and existing[1] > amount_f:
            bucket[minute] = existing
        else:
            bucket[minute] = (price_f, amount_f)
        self._trim_bucket(bucket, minute)
        return self.get_metrics(code, minute_index=minute)

    def ingest_quotes(
        self,
        rows: list[dict[str, Any]],
        *,
        minute_index: int | None = None,
    ) -> dict[str, TickWindowMetrics]:
        metrics: dict[str, TickWindowMetrics] = {}
        for row in rows:
            metric = self.ingest_quote(
                str(row.get("symbol") or ""),
                price=row.get("price", 0.0),
                amount=row.get("amount", 0.0),
                timestamp_ms=row.get("timestamp"),
                time_text=row.get("time"),
                minute_index=minute_index,
            )
            if metric is not None:
                metrics[metric.symbol] = metric
        return metrics

    def get_metrics(self, symbol: str, *, minute_index: int | None = None) -> TickWindowMetrics | None:
        code = _normalize_symbol(symbol)
        bucket = self._history.get(code)
        if not bucket:
            return None
        minute = minute_index if minute_index is not None else max(bucket)
        m0 = bucket.get(minute)
        if not m0:
            return None
        m1 = bucket.get(minute - 1)
        m2 = bucket.get(minute - 2)

        speed_1m = 0.0
        amount_2m = 0.0
        if m1 and m1[0] > 0:
            speed_1m = (m0[0] - m1[0]) / m1[0]
        ref_bar = m2 or m1
        if ref_bar:
            amount_2m = max(m0[1] - ref_bar[1], 0.0)

        return TickWindowMetrics(
            symbol=code,
            minute_index=minute,
            last_price=m0[0],
            last_amount=m0[1],
            speed_1m=speed_1m,
            amount_2m=amount_2m,
        )

    def get_all_metrics(self, *, minute_index: int | None = None) -> dict[str, TickWindowMetrics]:
        metrics: dict[str, TickWindowMetrics] = {}
        for symbol in list(self._history.keys()):
            metric = self.get_metrics(symbol, minute_index=minute_index)
            if metric is not None:
                metrics[symbol] = metric
        return metrics

    def _trim_bucket(self, bucket: dict[int, tuple[float, float]], minute: int) -> None:
        expired = [key for key in bucket if key < minute - self._keep_minutes]
        for key in expired:
            bucket.pop(key, None)
