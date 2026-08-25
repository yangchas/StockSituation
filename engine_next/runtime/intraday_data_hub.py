from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Sequence

from engine_next.connectors import KaipanConnector, ThsHotConnector, WencaiConnector
from engine_next.domain.enums import FetchIntent, RunPhase
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    RUNTIME_REASON_KEY,
    choose_primary_plate,
    choose_runtime_primary_plate,
    decode_theme_list,
    encode_theme_list,
    is_generic_plate,
    merge_theme_payload_prioritized,
    merge_theme_lists,
    normalize_plate_name,
    prioritize_core_themes,
)
from engine_next.source_policies.intraday_network_policy import allow_intraday_request


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_auction_pct_ratio(value: Any) -> float:
    """Normalize stock auction change values to ratio units.

    Legacy auction feeds have used three scales in Redis:
    ratio (0.0997), percent points (9.97), and occasionally basis points
    (997).  Strategy snapshots expect ratio units, while marginal deltas are
    converted back to percentage points at the delta boundary.
    """

    raw = _safe_float(value, 0.0)
    abs_raw = abs(raw)
    if abs_raw <= 0.35:
        return raw
    if abs_raw <= 30.0:
        return raw / 100.0
    return raw / 10000.0


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _milli_to_price(value: Any) -> float:
    return _safe_float(value) / 1000.0


def _is_q2_equity_quote(symbol: str, quote: dict[str, Any]) -> bool:
    market = str(quote.get("mk") or quote.get("market") or "").strip().lower()
    if market == "sz":
        return symbol.startswith(("000", "001", "002", "003", "300", "301"))
    if market == "kc":
        return symbol.startswith(("688", "689"))
    if market == "sh":
        return symbol.startswith(("600", "601", "603", "605", "688", "689"))
    price = _milli_to_price(quote.get("px"))
    if symbol.startswith(("000", "001", "002", "003", "300", "301")) and price >= 1000.0:
        return False
    return True


@dataclass(frozen=True)
class IntradayFetchResult:
    dataset: str
    trade_date: str
    rows: list[dict[str, Any]]
    source: str
    redis_keys_written: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


class IntradayDataHub:
    """Redis-first intraday hub aligned with engine_v2 acquisition -> processing -> flow -> consumption."""

    def __init__(
        self,
        *,
        redis_client: Any | None = None,
        kaipan_connector: KaipanConnector | None = None,
        ths_hot_connector: ThsHotConnector | None = None,
        wencai_connector: WencaiConnector | None = None,
        tdengine_auction_fetcher: Callable[[str], Sequence[dict[str, Any]]] | None = None,
        wencai_auction_fetcher: Callable[[str], Sequence[dict[str, Any]]] | None = None,
    ) -> None:
        self._redis = redis_client
        self._kaipan = kaipan_connector
        self._ths_hot = ths_hot_connector
        self._wencai = wencai_connector
        self._tdengine_auction_fetcher = tdengine_auction_fetcher
        self._wencai_auction_fetcher = wencai_auction_fetcher
        self._redis_q2_prefix = os.getenv("REDIS_Q2_PREFIX", "q2:")

    @property
    def redis(self) -> Any:
        if self._redis is None:
            import redis as redis_lib

            self._redis = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        return self._redis

    @property
    def kaipan(self) -> KaipanConnector:
        if self._kaipan is None:
            self._kaipan = KaipanConnector()
        return self._kaipan

    @property
    def wencai(self) -> WencaiConnector:
        if self._wencai is None:
            self._wencai = WencaiConnector()
        return self._wencai

    @property
    def ths_hot(self) -> ThsHotConnector:
        if self._ths_hot is None:
            self._ths_hot = ThsHotConnector()
        return self._ths_hot

    def _read_json_string_key(self, key: str) -> Any | None:
        raw = self.redis.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _batch_hgetall(self, keys: Sequence[str]) -> list[dict[str, Any]]:
        if not keys:
            return []
        pipeline_factory = getattr(self.redis, "pipeline", None)
        if callable(pipeline_factory):
            try:
                pipe = pipeline_factory()
                for key in keys:
                    pipe.hgetall(key)
                results = pipe.execute()
                return [result if isinstance(result, dict) else {} for result in results]
            except Exception:
                pass
        return [self.redis.hgetall(key) or {} for key in keys]

    def _batch_hmget(self, key: str, fields: Sequence[str]) -> list[Any]:
        if not fields:
            return []
        hmget = getattr(self.redis, "hmget", None)
        if callable(hmget):
            try:
                values = hmget(key, list(fields))
                if isinstance(values, list):
                    return values
            except Exception:
                pass
        return [self.redis.hget(key, field) for field in fields]

    @staticmethod
    def _standardize_legacy_quote(symbol: str, quote: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "name": quote.get("name", ""),
            "price": _safe_float(quote.get("price", 0.0)),
            "pre_close": _safe_float(quote.get("pre_close", 0.0)),
            "amount": _safe_float(quote.get("amount", 0.0)),
            "volume": _safe_float(quote.get("volume", 0.0)),
            "time": str(quote.get("time", "") or ""),
            "timestamp": _safe_int(quote.get("timestamp", quote.get("ts", 0))),
            "bid_amount": _safe_float(quote.get("bid_amount", quote.get("bid_amt", 0.0))),
            "auction_amount_yuan": _safe_float(quote.get("auction_amount_yuan", quote.get("amount", 0.0))),
            "bid_amount_yuan": _safe_float(quote.get("bid_amount_yuan", quote.get("bid_amount", quote.get("bid_amt", 0.0)))),
            "source": "redis_quote",
        }

    @staticmethod
    def _standardize_q2_quote(symbol: str, quote: dict[str, Any]) -> dict[str, Any]:
        price = _milli_to_price(quote.get("px"))
        pre_close = _milli_to_price(quote.get("pc"))
        phase = _safe_int(quote.get("ph", 0))
        is_auction = phase == 1
        auction_amount = _safe_float(quote.get("am", 0.0)) if is_auction else 0.0
        bid_amount = _safe_float(quote.get("br", 0.0)) if is_auction else 0.0
        q2_auction_amount = _safe_float(quote.get("am", 0.0))
        q2_auction_bid_amount = _safe_float(quote.get("br", 0.0))
        q2_auction_ask_amount = _safe_float(quote.get("ar", 0.0))
        speed_1m = _safe_int(quote.get("spd1m", 0)) / 10000.0
        return {
            "symbol": symbol,
            "market": str(quote.get("mk", "") or ""),
            "name": quote.get("name", ""),
            "price": price,
            "pre_close": pre_close,
            "amount": _safe_float(quote.get("amt", 0.0)),
            "volume": _safe_float(quote.get("vol", 0.0)),
            "time": str(quote.get("time", "") or ""),
            "timestamp": _safe_int(quote.get("ts", 0)),
            "bid_amount": bid_amount,
            "auction_amount_yuan": auction_amount,
            "bid_amount_yuan": bid_amount,
            "ask_amount_yuan": _safe_float(quote.get("ar", 0.0)) if is_auction else 0.0,
            "q2_a20_milli": _safe_int(quote.get("a20", 0)),
            "q2_a24_milli": _safe_int(quote.get("a24", 0)),
            "q2_a25_milli": _safe_int(quote.get("a25", 0)),
            "q2_auction_amount_yuan": q2_auction_amount,
            "q2_auction_bid_amount_yuan": q2_auction_bid_amount,
            "q2_auction_ask_amount_yuan": q2_auction_ask_amount,
            "instant_amount_yuan": _safe_float(quote.get("ia", 0.0)),
            "instant_volume": _safe_float(quote.get("iv", 0.0)),
            "large_net_yuan": _safe_float(quote.get("ln", 0.0)),
            "phase": phase,
            "limit_state": _safe_int(quote.get("ls", 0)),
            "change_rate_1min": speed_1m,
            "speed_1m": speed_1m,
            "speed_1m_bp": _safe_int(quote.get("spd1m", 0)),
            "amount_2m": _safe_float(quote.get("amt2m", 0.0)),
            "amount_2min": _safe_float(quote.get("amt2m", 0.0)),
            "amount_5m": _safe_float(quote.get("amt5m", 0.0)),
            "vector_3m": _safe_int(quote.get("vec3m", 0)) / 10000.0,
            "vector_5m": _safe_int(quote.get("vec5m", 0)) / 10000.0,
            "source": "redis_q2",
        }

    def _read_auction_hash_rows(self, key: str) -> list[dict[str, Any]]:
        raw = self.redis.hget(key, "top_amount")
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except Exception:
            return []
        return [row for row in payload if isinstance(row, dict)]

    def _read_auction_summary(self, key: str) -> dict[str, Any]:
        raw = self.redis.hget(key, "summary")
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _standardize_auction_snapshot_row(row: dict[str, Any], *, tag: str, summary: dict[str, Any]) -> dict[str, Any]:
        symbol = _normalize_symbol(row.get("symbol") or row.get("code"))
        ask_amount_present = any(
            key in row and row.get(key) is not None and str(row.get(key)).strip() != ""
            for key in ("ask_amount_yuan", "ask_amount", "ar")
        )
        return {
            "symbol": symbol,
            "name": str(row.get("name", row.get("stock_name", "")) or ""),
            "tag": tag,
            "timestamp": _safe_int(summary.get("ts", 0)),
            "price": _safe_float(row.get("price", row.get("open_price", 0.0))),
            "change_pct": normalize_auction_pct_ratio(row.get("change_pct", row.get("open_pct", 0.0))),
            "amount": _safe_float(row.get("auction_amount_yuan", row.get("amount", 0.0))),
            "auction_amount_yuan": _safe_float(row.get("auction_amount_yuan", row.get("amount", 0.0))),
            "bid_amount": _safe_float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0))),
            "bid_amount_yuan": _safe_float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0))),
            "ask_amount": _safe_float(row.get("ask_amount_yuan", row.get("ask_amount", row.get("ar", 0.0)))),
            "ask_amount_yuan": _safe_float(row.get("ask_amount_yuan", row.get("ask_amount", row.get("ar", 0.0)))),
            "ask_amount_present": ask_amount_present,
            "snapshot_total_stocks": _safe_int(summary.get("total_stocks", 0)),
            "snapshot_high_open_count": _safe_int(summary.get("high_open_count", 0)),
            "snapshot_low_open_count": _safe_int(summary.get("low_open_count", 0)),
            "snapshot_flat_open_count": _safe_int(summary.get("flat_open_count", 0)),
            "snapshot_limit_up_count": _safe_int(summary.get("limit_up_count", 0)),
            "snapshot_limit_down_count": _safe_int(summary.get("limit_down_count", 0)),
            "snapshot_total_auction_amount_yuan": _safe_float(summary.get("total_auction_amount_yuan", 0.0)),
            "snapshot_total_limit_up_bid_amount_yuan": _safe_float(
                summary.get("total_limit_up_bid_amount_yuan", 0.0)
            ),
            "source": f"redis_{tag}",
        }

    def _read_hash_payload(self, key: str) -> dict[str, Any]:
        raw = self.redis.hgetall(key) or {}
        return raw if isinstance(raw, dict) else {}

    def _write_hash_json(self, key: str, field: str, payload: dict[str, Any]) -> None:
        self.redis.hset(key, field, json.dumps(payload, ensure_ascii=False))

    def _write_hash_plain(self, key: str, field: str, value: str) -> None:
        self.redis.hset(key, field, value)

    def _merge_theme_hash_field(self, key: str, field: str, values: Iterable[str]) -> list[str]:
        existing_raw = self.redis.hget(key, field)
        merged, payload = merge_theme_payload_prioritized(existing_raw, values)
        if merged:
            self.redis.hset(key, field, payload)
        return merged

    def _should_refine_yest_limit_symbol(self, symbol: str, pool_plate: str) -> bool:
        normalized_pool_plate = str(pool_plate or "").strip()
        current_plate = str(self.redis.hget(RUNTIME_PRIMARY_PLATE_KEY, symbol) or "").strip()
        if not normalized_pool_plate or is_generic_plate(normalized_pool_plate):
            return True
        if not current_plate or is_generic_plate(current_plate):
            return True
        if normalize_plate_name(current_plate) != normalize_plate_name(normalized_pool_plate):
            return True
        return False

    def _decode_archived_anchor_rows(self, payload: Any) -> tuple[list[dict[str, Any]], bool]:
        if not isinstance(payload, dict):
            return [], False
        rows: list[dict[str, Any]] = []
        has_extended_fields = False
        for symbol, raw in payload.items():
            normalized_symbol = _normalize_symbol(symbol)
            if not normalized_symbol:
                continue
            if isinstance(raw, dict):
                amount = float(raw.get("amount", 0.0) or 0.0)
                bid_amount = float(raw.get("bid_amount", 0.0) or 0.0)
                ask_amount_present = any(
                    key in raw and raw.get(key) is not None and str(raw.get(key)).strip() != ""
                    for key in ("ask_amount_yuan", "ask_amount", "ar")
                )
                ask_amount = float(raw.get("ask_amount_yuan", raw.get("ask_amount", raw.get("ar", 0.0))) or 0.0)
                tag = str(raw.get("tag") or "").strip()
                source = str(raw.get("source") or "redis_anchor").strip() or "redis_anchor"
                rows.append(
                    {
                        "symbol": normalized_symbol,
                        "name": str(raw.get("name", raw.get("stock_name", "")) or ""),
                        "change_pct": normalize_auction_pct_ratio(raw.get("change_pct", 0.0)),
                        "amount": amount,
                        "bid_amount": bid_amount,
                        "ask_amount": ask_amount,
                        "ask_amount_yuan": ask_amount,
                        "ask_amount_present": ask_amount_present,
                        "tag": tag,
                        "source": source,
                    }
                )
                if amount > 0 or bid_amount > 0 or ask_amount > 0 or bool(tag):
                    has_extended_fields = True
                continue
            rows.append(
                {
                    "symbol": normalized_symbol,
                    "name": "",
                    "change_pct": normalize_auction_pct_ratio(raw),
                    "amount": 0.0,
                    "bid_amount": 0.0,
                    "ask_amount": 0.0,
                    "ask_amount_yuan": 0.0,
                    "ask_amount_present": False,
                    "source": "redis_anchor",
                }
            )
        return rows, has_extended_fields

    @staticmethod
    def _encode_anchor_archive(rows: Sequence[dict[str, Any]], *, source: str, tag: str) -> dict[str, dict[str, Any]]:
        payload: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            payload[symbol] = {
                "name": str(row.get("name", row.get("stock_name", "")) or ""),
                "change_pct": normalize_auction_pct_ratio(row.get("change_pct", 0.0)),
                "amount": float(row.get("amount", 0.0) or 0.0),
                "bid_amount": float(row.get("bid_amount", 0.0) or 0.0),
                "ask_amount": float(row.get("ask_amount", row.get("ask_amount_yuan", 0.0)) or 0.0),
                "ask_amount_yuan": float(row.get("ask_amount_yuan", row.get("ask_amount", 0.0)) or 0.0),
                "ask_amount_present": bool(row.get("ask_amount_present", False)),
                "tag": tag,
                "source": source,
            }
        return payload

    def recover_auction_anchor(self, trade_date: str, phase: RunPhase) -> IntradayFetchResult:
        decision = allow_intraday_request(FetchIntent.AUCTION_ANCHOR_RECOVERY, phase)
        if not decision.allowed:
            return IntradayFetchResult(
                dataset="auction_anchor",
                trade_date=trade_date,
                rows=[],
                source="blocked",
                notes=(decision.notes,),
            )

        tag = trade_date.replace("-", "")
        anchor_key = f"market:auction:anchor:{tag}"
        anchor_payload = self._read_json_string_key(anchor_key)
        archived_rows, archived_has_extended_fields = self._decode_archived_anchor_rows(anchor_payload)
        hash_rows = self._read_auction_hash_rows(f"market:auction:{tag}:0925")
        if archived_rows and archived_has_extended_fields and len(archived_rows) >= len(hash_rows):
            return IntradayFetchResult(
                dataset="auction_anchor",
                trade_date=trade_date,
                rows=archived_rows,
                source="redis_anchor",
                notes=("Recovered from full market:auction:anchor archive.",),
            )
        if hash_rows:
            rows = []
            for row in hash_rows:
                symbol = _normalize_symbol(row.get("symbol") or row.get("code"))
                if not symbol:
                    continue
                change_pct = normalize_auction_pct_ratio(row.get("change_pct", 0.0))
                rows.append(
                    {
                        "symbol": symbol,
                        "name": str(row.get("name", row.get("stock_name", "")) or ""),
                        "change_pct": change_pct,
                        "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                        "bid_amount": float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0)) or 0.0),
                        "ask_amount": float(row.get("ask_amount_yuan", row.get("ask_amount", row.get("ar", 0.0))) or 0.0),
                        "ask_amount_yuan": float(row.get("ask_amount_yuan", row.get("ask_amount", row.get("ar", 0.0))) or 0.0),
                        "ask_amount_present": any(
                            key in row and row.get(key) is not None and str(row.get(key)).strip() != ""
                            for key in ("ask_amount_yuan", "ask_amount", "ar")
                        ),
                        "source": "redis_0925",
                    }
                )
            if rows:
                archive_payload = self._encode_anchor_archive(rows, source="redis_0925", tag="0925")
                self.redis.set(anchor_key, json.dumps(archive_payload, ensure_ascii=False), ex=3 * 24 * 60 * 60)
            return IntradayFetchResult(
                dataset="auction_anchor",
                trade_date=trade_date,
                rows=rows,
                source="redis_0925",
                redis_keys_written=(anchor_key,),
                notes=("Recovered from market:auction:{date}:0925 and archived to anchor.",),
            )

        if archived_rows and archived_has_extended_fields:
            return IntradayFetchResult(
                dataset="auction_anchor",
                trade_date=trade_date,
                rows=archived_rows,
                source="redis_anchor",
                notes=("Recovered from market:auction:anchor archive.",),
            )

        if archived_rows:
            return IntradayFetchResult(
                dataset="auction_anchor",
                trade_date=trade_date,
                rows=archived_rows,
                source="redis_anchor",
                notes=("Recovered from legacy market:auction:anchor archive.",),
            )

        latest_key = f"market:auction:{tag}:latest"
        latest_payload = self._read_hash_payload(latest_key)
        latest_tag = str(latest_payload.get("tag") or "").strip()
        if latest_tag in {"0920", "0924"}:
            preview_rows = self._read_auction_hash_rows(f"market:auction:{tag}:{latest_tag}")
            if preview_rows:
                rows = []
                for row in preview_rows:
                    symbol = _normalize_symbol(row.get("symbol") or row.get("code"))
                    if not symbol:
                        continue
                    rows.append(
                        {
                            "symbol": symbol,
                            "name": str(row.get("name", row.get("stock_name", "")) or ""),
                            "change_pct": normalize_auction_pct_ratio(row.get("change_pct", 0.0)),
                            "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                            "bid_amount": float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0)) or 0.0),
                            "ask_amount": float(row.get("ask_amount_yuan", row.get("ask_amount", row.get("ar", 0.0))) or 0.0),
                            "ask_amount_yuan": float(row.get("ask_amount_yuan", row.get("ask_amount", row.get("ar", 0.0))) or 0.0),
                            "ask_amount_present": any(
                                key in row and row.get(key) is not None and str(row.get(key)).strip() != ""
                                for key in ("ask_amount_yuan", "ask_amount", "ar")
                            ),
                            "source": f"redis_preview_{latest_tag}",
                        }
                    )
                if rows:
                    return IntradayFetchResult(
                        dataset="auction_anchor",
                        trade_date=trade_date,
                        rows=rows,
                        source=f"redis_preview_{latest_tag}",
                        notes=(f"Recovered from market:auction:{tag}:{latest_tag} preview snapshot.",),
                    )

        if self._tdengine_auction_fetcher is not None:
            td_rows = list(self._tdengine_auction_fetcher(trade_date) or [])
            normalized_td_rows = []
            for row in td_rows:
                symbol = _normalize_symbol(row.get("code") or row.get("symbol"))
                if not symbol:
                    continue
                change_pct = normalize_auction_pct_ratio(row.get("change_pct", 0.0))
                normalized_td_rows.append(
                    {
                        "symbol": symbol,
                        "change_pct": change_pct,
                        "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                        "source": "tdengine",
                    }
                )
            if normalized_td_rows:
                archive_payload = self._encode_anchor_archive(normalized_td_rows, source="tdengine", tag="fallback")
                self.redis.set(anchor_key, json.dumps(archive_payload, ensure_ascii=False), ex=3 * 24 * 60 * 60)
                return IntradayFetchResult(
                    dataset="auction_anchor",
                    trade_date=trade_date,
                    rows=normalized_td_rows,
                    source="tdengine",
                    redis_keys_written=(anchor_key,),
                    notes=("Recovered from TDengine fallback and archived to anchor.",),
                )

        if self._wencai_auction_fetcher is not None:
            wc_rows = list(self._wencai_auction_fetcher(trade_date) or [])
            normalized_wc_rows = []
            for row in wc_rows:
                symbol = _normalize_symbol(row.get("code") or row.get("symbol"))
                if not symbol:
                    continue
                change_pct = normalize_auction_pct_ratio(row.get("change_pct", 0.0))
                normalized_wc_rows.append(
                    {
                        "symbol": symbol,
                        "change_pct": change_pct,
                        "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                        "source": "wencai",
                    }
                )
            if normalized_wc_rows:
                archive_payload = self._encode_anchor_archive(normalized_wc_rows, source="wencai", tag="fallback")
                self.redis.set(anchor_key, json.dumps(archive_payload, ensure_ascii=False), ex=3 * 24 * 60 * 60)
                return IntradayFetchResult(
                    dataset="auction_anchor",
                    trade_date=trade_date,
                    rows=normalized_wc_rows,
                    source="wencai",
                    redis_keys_written=(anchor_key,),
                    notes=("Recovered from Wencai fallback and archived to anchor.",),
                )

        return IntradayFetchResult(
            dataset="auction_anchor",
            trade_date=trade_date,
            rows=[],
            source="empty",
            notes=("No auction anchor source returned usable rows.",),
        )

    def load_auction_snapshots(
        self,
        trade_date: str,
        *,
        tags: Sequence[str] = ("0920", "0924", "0925"),
    ) -> IntradayFetchResult:
        """Read Redis auction snapshots and attach lightweight cross-time deltas.

        This is local Redis only: no network fallback, no TDengine query, and no
        strategy decision. EAX/auction strategy can consume these normalized rows
        without reparsing large JSON blobs repeatedly.
        """

        date_tag = trade_date.replace("-", "")
        normalized_tags = tuple(dict.fromkeys(str(tag).strip() for tag in tags if str(tag).strip()))
        rows: list[dict[str, Any]] = []
        by_tag_symbol: dict[str, dict[str, dict[str, Any]]] = {}
        keys_read: list[str] = []

        for snapshot_tag in normalized_tags:
            key = f"market:auction:{date_tag}:{snapshot_tag}"
            summary = self._read_auction_summary(key)
            raw_rows = self._read_auction_hash_rows(key)
            if not raw_rows:
                continue
            keys_read.append(key)
            tag_map: dict[str, dict[str, Any]] = {}
            for raw_row in raw_rows:
                row = self._standardize_auction_snapshot_row(raw_row, tag=snapshot_tag, summary=summary)
                symbol = str(row.get("symbol") or "")
                if not symbol:
                    continue
                tag_map[symbol] = row
                rows.append(row)
            by_tag_symbol[snapshot_tag] = tag_map

        previous_by_tag = {
            "0924": "0920",
            "0925": "0924",
            "latest": "0925",
        }
        for row in rows:
            previous_tag = previous_by_tag.get(str(row.get("tag") or ""))
            previous = by_tag_symbol.get(previous_tag or "", {}).get(str(row.get("symbol") or ""))
            if not previous:
                continue
            prev_amount = _safe_float(previous.get("amount", 0.0))
            amount = _safe_float(row.get("amount", 0.0))
            row["previous_tag"] = previous_tag
            row["price_delta"] = _safe_float(row.get("price", 0.0)) - _safe_float(previous.get("price", 0.0))
            row["change_pct_delta"] = (
                _safe_float(row.get("change_pct", 0.0)) - _safe_float(previous.get("change_pct", 0.0))
            ) * 100.0
            row["amount_delta"] = amount - prev_amount
            row["bid_amount_delta"] = _safe_float(row.get("bid_amount", 0.0)) - _safe_float(
                previous.get("bid_amount", 0.0)
            )
            ask_present = bool(row.get("ask_amount_present", False)) and bool(
                previous.get("ask_amount_present", False)
            )
            row["ask_amount_delta"] = (
                _safe_float(row.get("ask_amount", 0.0)) - _safe_float(previous.get("ask_amount", 0.0))
                if ask_present
                else None
            )
            row["amount_ratio"] = (amount / prev_amount) if prev_amount > 0 else 0.0

        return IntradayFetchResult(
            dataset="auction_snapshots",
            trade_date=trade_date,
            rows=rows,
            source="redis_snapshots" if rows else "empty",
            redis_keys_written=tuple(keys_read),
            notes=("Loaded Redis auction snapshots with 0920/0924/0925 marginal deltas.",),
        )

    def fetch_hot_plates(self, trade_date: str, phase: RunPhase, *, today_mode: bool) -> IntradayFetchResult:
        decision = allow_intraday_request(FetchIntent.HOT_PLATE_DISCOVERY, phase)
        if not decision.allowed:
            return IntradayFetchResult(
                dataset="hot_plates",
                trade_date=trade_date,
                rows=[],
                source="blocked",
                notes=(decision.notes,),
            )

        raw = self.kaipan.fetch_today_hot_plates() if today_mode else self.kaipan.fetch_hot_plates(trade_date)
        rows = self.kaipan.to_tdengine_rows("hot_plates", raw, trade_date)
        redis_key = f"cache:hot_plates:{trade_date}"
        meta_key = f"cache:hot_plates_meta:{trade_date}"
        now_dt = datetime.now()
        attempt_at = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        attempt_at_ts = int(now_dt.timestamp())
        previous_cache = self._read_hash_payload(redis_key)
        previous_meta = self._read_json_string_key(meta_key)
        previous_updated_at = ""
        previous_updated_at_ts = 0
        if isinstance(previous_meta, dict):
            previous_updated_at = str(previous_meta.get("updated_at") or "")
            try:
                previous_updated_at_ts = int(float(previous_meta.get("updated_at_ts", 0) or 0))
            except (TypeError, ValueError):
                previous_updated_at_ts = 0
        cache_preserved = bool(previous_cache) and not rows
        cache_row_count = len(rows) if rows else len(previous_cache)
        if rows:
            try:
                if hasattr(self.redis, "delete"):
                    self.redis.delete(redis_key)
            except Exception:
                pass
            for row in rows:
                plate_name = str(row.get("plate_name") or "")
                if not plate_name:
                    continue
                self._write_hash_json(redis_key, plate_name, row)
        updated_at = attempt_at
        updated_at_ts = attempt_at_ts
        if cache_preserved and previous_updated_at_ts > 0:
            updated_at = previous_updated_at or attempt_at
            updated_at_ts = previous_updated_at_ts
        meta_payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "source": "kaipan",
            "today_mode": bool(today_mode),
            "row_count": cache_row_count,
            "fetched_row_count": len(rows),
            "cache_row_count": cache_row_count,
            "success": bool(rows),
            "cache_preserved": cache_preserved,
            "updated_at": updated_at,
            "updated_at_ts": updated_at_ts,
            "last_attempt_at": attempt_at,
            "last_attempt_at_ts": attempt_at_ts,
        }
        self.redis.set(meta_key, json.dumps(meta_payload, ensure_ascii=False))
        notes = ["Today mode uses empty-date Kaipan semantics; history mode uses explicit trade_date."]
        if cache_preserved:
            notes.append("Empty Kaipan hot-plate response preserved the previous Redis cache.")
        return IntradayFetchResult(
            dataset="hot_plates",
            trade_date=trade_date,
            rows=rows,
            source="kaipan",
            redis_keys_written=(redis_key, meta_key),
            notes=tuple(notes),
        )

    def fetch_hot_rank(self, trade_date: str, phase: RunPhase, *, top_n: int = 100) -> IntradayFetchResult:
        decision = allow_intraday_request(FetchIntent.HOT_RANK_DISCOVERY, phase)
        if not decision.allowed:
            return IntradayFetchResult(
                dataset="hot_rank",
                trade_date=trade_date,
                rows=[],
                source="blocked",
                notes=(decision.notes,),
            )

        redis_key = f"cache:hot_rank:{trade_date}"
        meta_key = f"cache:hot_rank_meta:{trade_date}"
        now_dt = datetime.now()
        attempt_at = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        attempt_at_ts = int(now_dt.timestamp())
        previous_cache = self._read_hash_payload(redis_key)
        previous_meta = self._read_json_string_key(meta_key)
        previous_updated_at = ""
        previous_updated_at_ts = 0
        if isinstance(previous_meta, dict):
            previous_updated_at = str(previous_meta.get("updated_at") or "")
            try:
                previous_updated_at_ts = int(float(previous_meta.get("updated_at_ts", 0) or 0))
            except (TypeError, ValueError):
                previous_updated_at_ts = 0

        rows: list[dict[str, Any]] = []
        try:
            raw = asyncio.run(self.ths_hot.fetch_hot_rank(top_n=top_n))
            if self.ths_hot.validate_hot_rank(raw):
                rows = self.ths_hot.to_tdengine_rows(raw, trade_date)
        except Exception as exc:
            notes = (f"THS hot rank fetch failed: {type(exc).__name__}: {exc}",)
            return IntradayFetchResult(
                dataset="hot_rank",
                trade_date=trade_date,
                rows=[],
                source="ths_error",
                notes=notes,
            )

        cache_preserved = bool(previous_cache) and not rows
        cache_row_count = len(rows) if rows else len(previous_cache)
        if rows:
            try:
                if hasattr(self.redis, "delete"):
                    self.redis.delete(redis_key)
            except Exception:
                pass
            for row in rows:
                symbol = _normalize_symbol(row.get("symbol"))
                if not symbol:
                    continue
                payload = {
                    "symbol": symbol,
                    "rank": _safe_int(row.get("rank"), 0),
                    "heat": _safe_float(row.get("heat", 0.0), 0.0),
                    "name": str(row.get("name") or ""),
                    "source": str(row.get("source") or "ths_hot_rank"),
                    "trade_date": trade_date,
                }
                self._write_hash_json(redis_key, symbol, payload)

        updated_at = attempt_at
        updated_at_ts = attempt_at_ts
        if cache_preserved and previous_updated_at_ts > 0:
            updated_at = previous_updated_at or attempt_at
            updated_at_ts = previous_updated_at_ts
        meta_payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "source": "ths_hot_rank",
            "row_count": cache_row_count,
            "fetched_row_count": len(rows),
            "cache_row_count": cache_row_count,
            "success": bool(rows),
            "cache_preserved": cache_preserved,
            "updated_at": updated_at,
            "updated_at_ts": updated_at_ts,
            "last_attempt_at": attempt_at,
            "last_attempt_at_ts": attempt_at_ts,
        }
        self.redis.set(meta_key, json.dumps(meta_payload, ensure_ascii=False))
        notes = ["THS hot rank is low-frequency and cached by trade_date."]
        if cache_preserved:
            notes.append("Empty THS hot-rank response preserved the previous Redis cache.")
        return IntradayFetchResult(
            dataset="hot_rank",
            trade_date=trade_date,
            rows=rows,
            source="ths_hot_rank",
            redis_keys_written=(redis_key, meta_key),
            notes=tuple(notes),
        )

    def fetch_yest_limit_pool(self, trade_date: str, phase: RunPhase, *, max_ban: int = 5) -> IntradayFetchResult:
        decision = allow_intraday_request(FetchIntent.YEST_LIMIT_POOL_BUILD, phase)
        if not decision.allowed:
            return IntradayFetchResult(
                dataset="yest_limit_pool",
                trade_date=trade_date,
                rows=[],
                source="blocked",
                notes=(decision.notes,),
            )

        raw = self.kaipan.fetch_yesterday_bans_pool(trade_date, max_ban=max_ban)
        rows = self.kaipan.to_tdengine_rows("yest_limit_pool", raw, trade_date)
        redis_key = f"cache:yest_limit_pool:{trade_date}"
        meta_key = f"cache:yest_limit_pool_meta:{trade_date}"
        now_dt = datetime.now()
        attempt_at = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        attempt_at_ts = int(now_dt.timestamp())
        previous_cache = self._read_hash_payload(redis_key)
        previous_meta = self._read_json_string_key(meta_key)
        previous_updated_at = ""
        previous_updated_at_ts = 0
        if isinstance(previous_meta, dict):
            previous_updated_at = str(previous_meta.get("updated_at") or "")
            try:
                previous_updated_at_ts = int(float(previous_meta.get("updated_at_ts", 0) or 0))
            except (TypeError, ValueError):
                previous_updated_at_ts = 0
        cache_preserved = bool(previous_cache) and not rows
        cache_row_count = len(rows) if rows else len(previous_cache)
        if rows:
            try:
                if hasattr(self.redis, "delete"):
                    self.redis.delete(redis_key)
            except Exception:
                pass
        refine_symbols: list[str] = []
        pool_plate_map: dict[str, str] = {}
        for row in rows:
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            self._write_hash_json(redis_key, symbol, row)
            pool_plate = str(row.get("plate") or "").strip()
            if not pool_plate:
                refine_symbols.append(symbol)
                continue
            pool_plate_map[symbol] = pool_plate
            existing_themes = decode_theme_list(self.redis.hget(PLATE_MAPPING_S2P_KEY, symbol))
            merged_themes = prioritize_core_themes((pool_plate,), existing_themes, max_count=2)
            if merged_themes:
                self.redis.hset(PLATE_MAPPING_S2P_KEY, symbol, encode_theme_list(merged_themes))
            primary_plate = choose_runtime_primary_plate(
                merged_themes,
                fallback=str(self.redis.hget(RUNTIME_PRIMARY_PLATE_KEY, symbol) or "") or pool_plate,
                pool_plate=pool_plate,
                reason_candidates=merged_themes,
            )
            current_plate = str(self.redis.hget(RUNTIME_PRIMARY_PLATE_KEY, symbol) or "").strip()
            if primary_plate and (
                not current_plate
                or is_generic_plate(current_plate)
                or normalize_plate_name(current_plate) != normalize_plate_name(primary_plate)
            ):
                self._write_hash_plain(RUNTIME_PRIMARY_PLATE_KEY, symbol, primary_plate)
            if self._should_refine_yest_limit_symbol(symbol, pool_plate):
                refine_symbols.append(symbol)
        if pool_plate_map:
            refine_symbols.extend(pool_plate_map.keys())
        unique_refine_symbols = tuple(dict.fromkeys(refine_symbols))
        updated_at = attempt_at
        updated_at_ts = attempt_at_ts
        if cache_preserved and previous_updated_at_ts > 0:
            updated_at = previous_updated_at or attempt_at
            updated_at_ts = previous_updated_at_ts
        meta_payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "source": "kaipan",
            "row_count": cache_row_count,
            "fetched_row_count": len(rows),
            "cache_row_count": cache_row_count,
            "success": bool(rows),
            "cache_preserved": cache_preserved,
            "updated_at": updated_at,
            "updated_at_ts": updated_at_ts,
            "last_attempt_at": attempt_at,
            "last_attempt_at_ts": attempt_at_ts,
        }
        self.redis.set(meta_key, json.dumps(meta_payload, ensure_ascii=False))
        redis_keys_written: list[str] = [redis_key, meta_key, PLATE_MAPPING_S2P_KEY, RUNTIME_PRIMARY_PLATE_KEY]
        notes = ["Yesterday limit pool is lightweight enough for startup repair and ladder context rebuild."]
        if cache_preserved:
            notes.append("Empty yesterday-limit response preserved the previous Redis cache.")
        if unique_refine_symbols and phase in (RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.POSTMARKET):
            enrich_result = self.enrich_stock_plate(
                trade_date,
                phase,
                unique_refine_symbols,
                max_symbols=None,
                pool_plate_map=pool_plate_map,
            )
            if enrich_result.source == "kaipan" and enrich_result.rows:
                redis_keys_written.extend(enrich_result.redis_keys_written)
                notes.append(
                    f"Detailed Kaipan ban-reason enrichment refreshed {len(unique_refine_symbols)} yesterday-limit symbols."
                )
        return IntradayFetchResult(
            dataset="yest_limit_pool",
            trade_date=trade_date,
            rows=rows,
            source="kaipan",
            redis_keys_written=tuple(dict.fromkeys(redis_keys_written)),
            notes=tuple(notes),
        )

    def fetch_limit_truth(self, trade_date: str, phase: RunPhase, *, max_stocks: int = 500) -> IntradayFetchResult:
        decision = allow_intraday_request(FetchIntent.LIMIT_TRUTH_BUILD, phase)
        if not decision.allowed:
            return IntradayFetchResult(
                dataset="limit_truth",
                trade_date=trade_date,
                rows=[],
                source="blocked",
                notes=(decision.notes,),
            )

        dataframe = asyncio.run(self.wencai.fetch_limitup_with_lb_days(max_stocks=max_stocks))
        rows = self.wencai.to_tdengine_rows("limit_truth", dataframe, trade_date)
        redis_key = f"cache:limit_truth:{trade_date}"
        meta_key = f"cache:limit_truth_meta:{trade_date}"
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated_at_ts = int(datetime.now().timestamp())
        try:
            if hasattr(self.redis, "delete"):
                self.redis.delete(redis_key)
        except Exception:
            pass
        for row in rows:
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            payload = dict(row)
            payload["symbol"] = symbol
            self._write_hash_json(redis_key, symbol, payload)
        meta_payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "source": "wencai",
            "row_count": len(rows),
            "updated_at": updated_at,
            "updated_at_ts": updated_at_ts,
            "truth_scope": "final_limit_up_only",
        }
        self.redis.set(meta_key, json.dumps(meta_payload, ensure_ascii=False))
        return IntradayFetchResult(
            dataset="limit_truth",
            trade_date=trade_date,
            rows=rows,
            source="wencai",
            redis_keys_written=(redis_key, meta_key),
            notes=("Today limit truth keeps only final Wencai limit-up rows and should not be mixed with intraday touched-limit traces.",),
        )

    def enrich_stock_plate(
        self,
        trade_date: str,
        phase: RunPhase,
        symbols: Iterable[str],
        *,
        max_symbols: int | None = 30,
        pool_plate_map: dict[str, str] | None = None,
    ) -> IntradayFetchResult:
        decision = allow_intraday_request(FetchIntent.STOCK_PLATE_ENRICHMENT, phase)
        if not decision.allowed:
            return IntradayFetchResult(
                dataset="stock_plate_enrichment",
                trade_date=trade_date,
                rows=[],
                source="blocked",
                notes=(decision.notes,),
            )

        rows: list[dict[str, Any]] = []
        selected_symbols = list(symbols)
        if max_symbols is not None:
            selected_symbols = selected_symbols[:max_symbols]
        for raw_symbol in selected_symbols:
            symbol = _normalize_symbol(raw_symbol)
            if not symbol:
                continue
            reasons = self.kaipan.fetch_ban_reasons(symbol)
            if not reasons:
                continue
            writebacks = self.kaipan.build_runtime_writebacks(
                reasons,
                trade_date,
                existing_themes=decode_theme_list(self.redis.hget(PLATE_MAPPING_S2P_KEY, symbol)),
                fallback_plate=str(self.redis.hget(RUNTIME_PRIMARY_PLATE_KEY, symbol) or ""),
                pool_plate=str((pool_plate_map or {}).get(symbol) or ""),
            )
            theme_payload = writebacks.get(PLATE_MAPPING_S2P_KEY, {})
            plate_payload = writebacks.get(RUNTIME_PRIMARY_PLATE_KEY, {})
            reason_payload = writebacks.get(RUNTIME_REASON_KEY, {})
            for code, themes in theme_payload.items():
                if themes:
                    self._merge_theme_hash_field(PLATE_MAPPING_S2P_KEY, code, themes)
            for code, plate in plate_payload.items():
                if plate:
                    self._write_hash_plain(RUNTIME_PRIMARY_PLATE_KEY, code, str(plate))
            for code, reason in reason_payload.items():
                if reason:
                    self._write_hash_plain(RUNTIME_REASON_KEY, code, str(reason))
            for reason_row in self.kaipan.to_tdengine_rows("ban_reasons", reasons, trade_date):
                rows.append(reason_row)
        return IntradayFetchResult(
            dataset="stock_plate_enrichment",
            trade_date=trade_date,
            rows=rows,
            source="kaipan",
            redis_keys_written=(PLATE_MAPPING_S2P_KEY, RUNTIME_PRIMARY_PLATE_KEY, RUNTIME_REASON_KEY),
            notes=("Kaipan ban reasons refine plate mapping and reason writeback in small batches.",),
        )

    def fetch_redis_quotes(self, symbols: Iterable[str]) -> IntradayFetchResult:
        normalized_symbols = tuple(
            dict.fromkeys(_normalize_symbol(raw_symbol) for raw_symbol in symbols if _normalize_symbol(raw_symbol))
        )
        rows: list[dict[str, Any]] = []
        legacy_keys = [f"stock:quote:{symbol}" for symbol in normalized_symbols]
        legacy_quotes = self._batch_hgetall(legacy_keys)
        missing_symbols: list[str] = []
        for symbol, legacy_quote in zip(normalized_symbols, legacy_quotes):
            if legacy_quote:
                rows.append(self._standardize_legacy_quote(symbol, legacy_quote))
            else:
                missing_symbols.append(symbol)
        q2_keys = [f"{self._redis_q2_prefix}{symbol}" for symbol in missing_symbols]
        for symbol, q2_quote in zip(missing_symbols, self._batch_hgetall(q2_keys)):
            if q2_quote and _is_q2_equity_quote(symbol, q2_quote):
                rows.append(self._standardize_q2_quote(symbol, q2_quote))
        return IntradayFetchResult(
            dataset="redis_quotes",
            trade_date="",
            rows=rows,
            source="redis",
            notes=("Redis quote path is the main intraday low-latency market data source.",),
        )

    def load_runtime_cache_views(
        self,
        trade_date: str,
        symbols: Iterable[str],
        *,
        hot_rank_trade_date: str | None = None,
    ) -> IntradayFetchResult:
        normalized_symbols = tuple(
            dict.fromkeys(_normalize_symbol(raw_symbol) for raw_symbol in symbols if _normalize_symbol(raw_symbol))
        )
        hot_rank_date = str(hot_rank_trade_date or trade_date or "").strip()
        factor_values = self._batch_hmget(f"cache:stock_extra:{trade_date}", normalized_symbols)
        chip_values = self._batch_hmget(f"cache:chip_peaks:{trade_date}", normalized_symbols)
        dde_values = self._batch_hmget(f"cache:dde_ready:{trade_date}", normalized_symbols)
        hot_rank_values = self._batch_hmget(f"cache:hot_rank:{hot_rank_date}", normalized_symbols)
        rows: list[dict[str, Any]] = []
        for symbol, factor_raw, chip_raw, dde_raw, hot_rank_raw in zip(
            normalized_symbols,
            factor_values,
            chip_values,
            dde_values,
            hot_rank_values,
        ):
            merged = {"symbol": symbol}
            found = False
            for raw in (factor_raw, chip_raw, dde_raw, hot_rank_raw):
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    merged.update(payload)
                    found = True
            if found:
                rows.append(merged)
        return IntradayFetchResult(
            dataset="runtime_cache_views",
            trade_date=trade_date,
            rows=rows,
            source="redis",
            notes=("Intraday consumers should prefer trimmed Redis views over TDengine queries.",),
        )
