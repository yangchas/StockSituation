from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Sequence

from engine_next.connectors import KaipanConnector, WencaiConnector
from engine_next.domain.enums import FetchIntent, RunPhase
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    RUNTIME_REASON_KEY,
    choose_primary_plate,
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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _milli_to_price(value: Any) -> float:
    return _safe_float(value) / 1000.0


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
        wencai_connector: WencaiConnector | None = None,
        tdengine_auction_fetcher: Callable[[str], Sequence[dict[str, Any]]] | None = None,
        wencai_auction_fetcher: Callable[[str], Sequence[dict[str, Any]]] | None = None,
    ) -> None:
        self._redis = redis_client
        self._kaipan = kaipan_connector
        self._wencai = wencai_connector
        self._tdengine_auction_fetcher = tdengine_auction_fetcher
        self._wencai_auction_fetcher = wencai_auction_fetcher

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
        speed_1m = _safe_int(quote.get("spd1m", 0)) / 10000.0
        return {
            "symbol": symbol,
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
                tag = str(raw.get("tag") or "").strip()
                source = str(raw.get("source") or "redis_anchor").strip() or "redis_anchor"
                rows.append(
                    {
                        "symbol": normalized_symbol,
                        "name": str(raw.get("name", raw.get("stock_name", "")) or ""),
                        "change_pct": float(raw.get("change_pct", 0.0) or 0.0),
                        "amount": amount,
                        "bid_amount": bid_amount,
                        "tag": tag,
                        "source": source,
                    }
                )
                if amount > 0 or bid_amount > 0 or bool(tag):
                    has_extended_fields = True
                continue
            rows.append(
                {
                    "symbol": normalized_symbol,
                    "name": "",
                    "change_pct": float(raw or 0.0),
                    "amount": 0.0,
                    "bid_amount": 0.0,
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
                "change_pct": float(row.get("change_pct", 0.0) or 0.0),
                "amount": float(row.get("amount", 0.0) or 0.0),
                "bid_amount": float(row.get("bid_amount", 0.0) or 0.0),
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
        hash_rows = self._read_auction_hash_rows(f"market:auction:{tag}:0925")
        if hash_rows:
            rows = []
            for row in hash_rows:
                symbol = _normalize_symbol(row.get("symbol") or row.get("code"))
                if not symbol:
                    continue
                change_pct = float(row.get("change_pct", 0.0) or 0.0)
                rows.append(
                    {
                        "symbol": symbol,
                        "name": str(row.get("name", row.get("stock_name", "")) or ""),
                        "change_pct": change_pct,
                        "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                        "bid_amount": float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0)) or 0.0),
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

        anchor_payload = self._read_json_string_key(anchor_key)
        archived_rows, archived_has_extended_fields = self._decode_archived_anchor_rows(anchor_payload)
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
                            "change_pct": float(row.get("change_pct", 0.0) or 0.0),
                            "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                            "bid_amount": float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0)) or 0.0),
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
                change_pct = float(row.get("change_pct", 0.0) or 0.0)
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
                change_pct = float(row.get("change_pct", 0.0) or 0.0)
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
            primary_plate = choose_primary_plate(merged_themes, fallback=pool_plate)
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
        q2_keys = [f"q2:{symbol}" for symbol in missing_symbols]
        for symbol, q2_quote in zip(missing_symbols, self._batch_hgetall(q2_keys)):
            if q2_quote:
                rows.append(self._standardize_q2_quote(symbol, q2_quote))
        return IntradayFetchResult(
            dataset="redis_quotes",
            trade_date="",
            rows=rows,
            source="redis",
            notes=("Redis quote path is the main intraday low-latency market data source.",),
        )

    def load_runtime_cache_views(self, trade_date: str, symbols: Iterable[str]) -> IntradayFetchResult:
        normalized_symbols = tuple(
            dict.fromkeys(_normalize_symbol(raw_symbol) for raw_symbol in symbols if _normalize_symbol(raw_symbol))
        )
        factor_values = self._batch_hmget(f"cache:stock_extra:{trade_date}", normalized_symbols)
        chip_values = self._batch_hmget(f"cache:chip_peaks:{trade_date}", normalized_symbols)
        dde_values = self._batch_hmget(f"cache:dde_ready:{trade_date}", normalized_symbols)
        rows: list[dict[str, Any]] = []
        for symbol, factor_raw, chip_raw, dde_raw in zip(normalized_symbols, factor_values, chip_values, dde_values):
            merged = {"symbol": symbol}
            found = False
            for raw in (factor_raw, chip_raw, dde_raw):
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
