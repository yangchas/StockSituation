from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from engine_next.runtime.rust_engine_contract import RustEngineSource, RustEngineStatus


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _from_milli(value: Any) -> float:
    return _to_float(value) / 1_000.0


def _from_wan(value: Any) -> float:
    return _to_float(value) * 10_000.0


def _from_bp(value: Any) -> float:
    return _to_float(value) / 10_000.0


@dataclass(frozen=True)
class RustSnapshotRow:
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


class RustSnapshotBridge:
    """
    Thin runtime bridge around the native engine_next Rust adapter only.

    No fallback to engine_v2 is allowed. If the new core is unavailable,
    runtime code must surface that state explicitly instead of silently
    borrowing the old system.
    """

    def __init__(self, adapter: Any | None = None) -> None:
        self._adapter = adapter
        self._source = RustEngineSource.ENGINE_NEXT if adapter is not None else RustEngineSource.UNAVAILABLE

    @property
    def adapter(self) -> Any:
        if self._adapter is None:
            engine_next_adapter = self._try_load_engine_next_adapter()
            if engine_next_adapter is not None:
                self._adapter = engine_next_adapter
                self._source = RustEngineSource.ENGINE_NEXT
                return self._adapter
            self._adapter = None
            self._source = RustEngineSource.UNAVAILABLE
        return self._adapter

    @property
    def source(self) -> RustEngineSource:
        _ = self.adapter
        return self._source

    def _try_load_engine_next_adapter(self) -> Any | None:
        try:
            from engine_next.rust_core.python_adapter import engine_next_core_bridge

            return engine_next_core_bridge
        except Exception:
            return None

    def get_status(self) -> RustEngineStatus:
        adapter = self.adapter
        if not adapter or not getattr(adapter, "engine", None):
            return RustEngineStatus(
                source=self.source,
                available=False,
                supports_tick_push=False,
                supports_symbol_registration=False,
                supports_plate_mapping=False,
                supports_snapshot=False,
                supports_market_extremes=False,
                notes=(
                    "No Rust runtime adapter is currently available.",
                    "engine_next does not fall back to engine_v2 for Rust computation.",
                ),
            )
        return RustEngineStatus(
            source=self.source,
            available=True,
            supports_tick_push=bool(getattr(adapter, "push_tick_raw", None)),
            supports_symbol_registration=bool(getattr(adapter, "register_symbols", None)),
            supports_plate_mapping=bool(getattr(adapter, "register_plate_mapping", None)),
            supports_snapshot=bool(getattr(adapter, "get_snapshot", None)),
            supports_market_extremes=True,
            notes=(
                "Current bridge is snapshot-oriented and not yet the full engine_next Rust migration target.",
            ),
        )

    def get_raw_snapshot(self) -> dict[str, Any]:
        adapter = self.adapter
        if not adapter or not getattr(adapter, "engine", None):
            return {}
        try:
            snapshot = adapter.get_snapshot()
        except Exception:
            return {}
        return snapshot if isinstance(snapshot, dict) else {}

    def get_normalized_snapshot(self) -> dict[str, dict[str, float]]:
        raw = self.get_raw_snapshot()
        normalized: dict[str, dict[str, float]] = {}
        for symbol, payload in raw.items():
            code = _normalize_symbol(symbol)
            if not code or code == "MES_":
                continue
            if code.startswith("_"):
                continue
            if not isinstance(payload, dict):
                continue
            price = _from_milli(payload.get("price_milli")) if "price_milli" in payload else _to_float(payload.get("price"))
            amount = _from_wan(payload.get("amount_wan")) if "amount_wan" in payload else _to_float(payload.get("amount"))
            speed_1m = _from_bp(payload.get("speed_bp")) if "speed_bp" in payload else _to_float(payload.get("speed"))
            amount_2m = _from_wan(payload.get("amount_2m_wan")) if "amount_2m_wan" in payload else _to_float(payload.get("amount_2m"))
            amount_5m = _from_wan(payload.get("amount_5m_wan")) if "amount_5m_wan" in payload else _to_float(payload.get("amount_5m"))
            vector_3m = _from_bp(payload.get("vector_3m_bp")) if "vector_3m_bp" in payload else _to_float(payload.get("vector_3m"))
            vector_5m = _from_bp(payload.get("vector_5m_bp")) if "vector_5m_bp" in payload else _to_float(payload.get("vector_5m"))
            bid_amount = _from_wan(payload.get("bid_amt_wan")) if "bid_amt_wan" in payload else _to_float(payload.get("bid_amt"))
            max_price = _from_milli(payload.get("max_p_milli")) if "max_p_milli" in payload else _to_float(payload.get("max_p"))
            min_price = _from_milli(payload.get("min_p_milli")) if "min_p_milli" in payload else _to_float(payload.get("min_p"))
            p0920 = _from_milli(payload.get("p0920_milli")) if "p0920_milli" in payload else _to_float(payload.get("p0920"))
            p0924 = _from_milli(payload.get("p0924_milli")) if "p0924_milli" in payload else _to_float(payload.get("p0924"))
            p0925 = _from_milli(payload.get("p0925_milli")) if "p0925_milli" in payload else _to_float(payload.get("p0925"))
            normalized[code] = {
                "symbol": code,
                "price": price,
                "amount": amount,
                "speed_1m": speed_1m,
                "amount_2m": amount_2m,
                "amount_5m": amount_5m,
                "vector_3m": vector_3m,
                "vector_5m": vector_5m,
                "bid_amount": bid_amount,
                "max_price": max_price,
                "min_price": min_price,
                "p0920": p0920,
                "p0924": p0924,
                "p0925": p0925,
                "source": "rust_snapshot",
            }
        return normalized

    def get_market_extremes(self) -> dict[str, Any]:
        raw = self.get_raw_snapshot()
        payload = raw.get("_EXTREMES_")
        if not isinstance(payload, dict):
            return {}
        top_turnover = payload.get("top_turnover", [])
        return {
            "top_turnover_symbols": tuple(
                _normalize_symbol(symbol)
                for symbol in top_turnover
                if _normalize_symbol(symbol)
            )
        }
