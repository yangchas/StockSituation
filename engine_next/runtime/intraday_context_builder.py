from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from heapq import nlargest
from typing import Any, Iterable

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import IntradayContext, IntradayMarketSummary, StockStateSnapshot
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    build_plate_candidates_from_reason,
    choose_primary_plate,
    decode_theme_list,
    is_generic_plate,
    merge_theme_lists,
    split_plate_tokens,
)
from engine_next.runtime.intraday_data_hub import IntradayDataHub, normalize_auction_pct_ratio
from engine_next.runtime.session_facts import (
    build_session_facts,
    session_facts_from_payload,
    session_facts_to_payload,
)
from engine_next.runtime.tick_window_tracker import TickWindowTracker
from engine_next.runtime.tick_window_tracker import TickWindowMetrics


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


@dataclass(frozen=True)
class IntradayContextRequest:
    phase: RunPhase
    trade_date: str
    previous_trade_date: str
    symbols: tuple[str, ...]
    now: datetime | None = None
    offline_context_date: str | None = None
    require_auction_recovery: bool = False
    minute_index: int | None = None


@dataclass(frozen=True)
class PrimedIntradayRuntimeState:
    phase: RunPhase
    trade_date: str
    previous_trade_date: str
    offline_context_date: str
    symbols: tuple[str, ...]
    quote_rows: tuple[dict[str, Any], ...]
    cache_rows: tuple[dict[str, Any], ...]
    auction_rows: tuple[dict[str, Any], ...]
    yest_limit_map: dict[str, dict]
    hot_plate_map: dict[str, dict]
    yesterday_hot_plate_map: dict[str, dict]
    effective_hot_plate_map: dict[str, dict]
    stock_plate_map: dict[str, str]
    stock_theme_map: dict[str, tuple[str, ...]]
    stock_reason_map: dict[str, str]
    market_runtime_state: dict[str, Any]
    native_ingested: int = 0
    rust_ingested: int = 0
    tick_metric_map: dict[str, TickWindowMetrics] = field(default_factory=dict)
    rust_snapshot_map: dict[str, Any] = field(default_factory=dict)
    rust_market_extremes: dict[str, Any] = field(default_factory=dict)
    quote_fresh_count: int = 0
    quote_stale_count: int = 0
    quote_missing_count: int = 0
    quote_timestamped_count: int = 0
    quote_stale_threshold_seconds: int = 0
    latest_quote_timestamp_ms: int = 0
    latest_quote_time: str = ""
    latest_quote_age_seconds: int | None = None
    quote_fresh_ratio: float = 0.0
    hot_plate_cache_trade_date: str = ""
    hot_plate_updated_at_ts: int = 0
    hot_plate_signature: str = ""
    yest_limit_signature: str = ""
    auction_signature: str = ""
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if int(getattr(self, "native_ingested", 0) or 0) <= 0 and int(getattr(self, "rust_ingested", 0) or 0) > 0:
            object.__setattr__(self, "native_ingested", int(self.rust_ingested))
        if int(getattr(self, "rust_ingested", 0) or 0) <= 0 and int(getattr(self, "native_ingested", 0) or 0) > 0:
            object.__setattr__(self, "rust_ingested", int(self.native_ingested))


class IntradayContextBuilder:
    """
    Redis-first intraday context assembler.

    This follows the original engine_v2 data flow philosophy:
    fetch low-latency runtime views once, merge them into compact stock
    snapshots, and let the strategy layer consume one stable structure.
    """

    def __init__(
        self,
        *,
        intraday_hub: IntradayDataHub | None = None,
        tick_tracker: TickWindowTracker | None = None,
    ) -> None:
        self._hub = intraday_hub or IntradayDataHub()
        self._tick_tracker = tick_tracker or TickWindowTracker()
        self._scoped_cache_token: tuple[str, int | None] | None = None
        self._json_hash_cache: dict[str, dict[str, dict]] = {}
        self._string_hash_cache: dict[str, dict[str, str]] = {}
        self._string_key_cache: dict[str, dict[str, Any]] = {}
        self._primed_runtime_state: PrimedIntradayRuntimeState | None = None
        self._rust_bridge = None
        self._rust_feed = None
        self._f10_service: Any | None = None
        self._f10_name_cache: dict[str, str] = {}

    @property
    def hub(self) -> IntradayDataHub:
        return self._hub

    @property
    def tick_tracker(self) -> TickWindowTracker:
        return self._tick_tracker

    @staticmethod
    def _resolve_snapshot_name(
        *,
        quote: dict[str, Any],
        cache: dict[str, Any],
        auction: dict[str, Any],
        yest: dict[str, Any],
    ) -> str:
        for raw in (
            quote.get("name"),
            cache.get("name"),
            cache.get("stock_name"),
            auction.get("name"),
            auction.get("stock_name"),
            yest.get("name"),
        ):
            text = str(raw or "").strip()
            if text:
                return text
        return ""

    def _load_fallback_stock_names(self, symbols: Iterable[str]) -> dict[str, str]:
        normalized_symbols = tuple(
            dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol))
        )
        if not normalized_symbols:
            return {}
        pending = tuple(symbol for symbol in normalized_symbols if symbol not in self._f10_name_cache)
        if pending:
            try:
                if self._f10_service is None:
                    from web.services.f10_service import F10DataService

                    self._f10_service = F10DataService()
                if hasattr(self._f10_service, "batch_get_stock_names"):
                    payload = self._f10_service.batch_get_stock_names(list(pending))
                    for symbol in pending:
                        self._f10_name_cache[symbol] = str((payload or {}).get(symbol) or "").strip()
                    payload = None
                else:
                    payload = self._f10_service.batch_get_f10(list(pending))
            except Exception:
                payload = {}
            if payload is not None:
                for symbol in pending:
                    item = payload.get(symbol) if isinstance(payload, dict) else None
                    basic = item.get("basic") if isinstance(item, dict) else None
                    name = str((basic or {}).get("stock_name") or "").strip()
                    self._f10_name_cache[symbol] = name
        return {
            symbol: self._f10_name_cache.get(symbol, "")
            for symbol in normalized_symbols
            if str(self._f10_name_cache.get(symbol, "") or "").strip()
        }

    def _resolve_snapshot_plate(
        self,
        *,
        runtime_plate: str,
        yest_plate: str,
        themes: Iterable[str] = (),
        reason: str = "",
        hot_plate_map: dict[str, Any] | None = None,
    ) -> str:
        def _usable_tokens(*values: str) -> tuple[str, ...]:
            tokens: list[str] = []
            for value in values:
                for token in split_plate_tokens(value):
                    cleaned = str(token or "").strip()
                    if not cleaned or len(cleaned) > 12 or cleaned in tokens:
                        continue
                    tokens.append(cleaned)
            return tuple(tokens)

        reason_tokens = tuple(build_plate_candidates_from_reason(reason=reason)) if reason else ()
        candidate_entries: list[tuple[str, int]] = []

        def _extend(values: tuple[str, ...], priority: int) -> None:
            for value in values:
                if any(existing == value for existing, _ in candidate_entries):
                    continue
                candidate_entries.append((value, priority))

        _extend(_usable_tokens(yest_plate), 4)
        _extend(reason_tokens, 3)
        for theme in themes:
            _extend(_usable_tokens(str(theme or "")), 2)
        _extend(_usable_tokens(runtime_plate), 1)

        theme_candidates = tuple(name for name, _ in candidate_entries)
        fallback = str(yest_plate or reason_tokens[0] if reason_tokens else runtime_plate or next(iter(theme_candidates), "")).strip()
        runtime_tokens = _usable_tokens(runtime_plate)
        yest_tokens = _usable_tokens(yest_plate)
        theme_front = tuple(dict.fromkeys(theme_candidates[:2]))
        runtime_primary = runtime_tokens[0] if runtime_tokens else ""
        yest_primary = yest_tokens[0] if yest_tokens else ""
        if runtime_primary and runtime_primary in theme_front:
            if yest_primary and yest_primary == runtime_primary:
                return runtime_primary
            if yest_primary and yest_primary not in theme_front and is_generic_plate(yest_primary):
                return runtime_primary
        preferred = [entry for entry in candidate_entries if not is_generic_plate(entry[0])]
        if hot_plate_map and preferred:
            ranked = sorted(
                preferred,
                key=lambda item: (
                    -self._match_hot_plate_signal((item[0],), hot_plate_map),
                    -item[1],
                    item[0],
                ),
            )
            best_name, _ = ranked[0]
            if self._match_hot_plate_signal((best_name,), hot_plate_map) > 0 and not (
                yest_primary and not is_generic_plate(yest_primary)
            ):
                return best_name
        if preferred:
            ranked = sorted(
                preferred,
                key=lambda item: (
                    -item[1],
                    item[0] != yest_primary,
                    item[0] != (reason_tokens[0] if reason_tokens else ""),
                    item[0] == runtime_primary,
                    item[0],
                ),
            )
            return ranked[0][0]
        return choose_primary_plate(theme_candidates, fallback=fallback)

    @staticmethod
    def _parse_timestamp_ms(value: Any) -> int:
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _quote_stale_threshold_seconds(phase: RunPhase) -> int:
        if phase == RunPhase.AUCTION:
            return 20
        if phase == RunPhase.INTRADAY:
            return 45
        if phase == RunPhase.POSTMARKET:
            return 180
        return 300

    @staticmethod
    def _format_quote_time(timestamp_ms: int) -> str:
        if timestamp_ms <= 0:
            return ""
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%H:%M:%S")

    def _summarize_quote_freshness(
        self,
        *,
        phase: RunPhase,
        symbols: tuple[str, ...],
        quote_rows: Iterable[dict[str, Any]],
        now: datetime | None,
    ) -> dict[str, Any]:
        threshold_seconds = self._quote_stale_threshold_seconds(phase)
        if now is None:
            return {
                "fresh_count": 0,
                "stale_count": 0,
                "missing_count": max(len(symbols) - len(tuple(quote_rows)), 0),
                "timestamped_count": 0,
                "threshold_seconds": threshold_seconds,
                "latest_timestamp_ms": 0,
                "latest_time": "",
                "latest_age_seconds": None,
                "fresh_ratio": 0.0,
            }

        now_ms = int(now.timestamp() * 1000)
        seen_count = 0
        fresh_count = 0
        timestamped_count = 0
        latest_timestamp_ms = 0
        for row in quote_rows:
            seen_count += 1
            quote_timestamp_ms = self._parse_timestamp_ms(row.get("timestamp"))
            if quote_timestamp_ms <= 0:
                continue
            timestamped_count += 1
            if quote_timestamp_ms > latest_timestamp_ms:
                latest_timestamp_ms = quote_timestamp_ms
            age_ms = max(now_ms - quote_timestamp_ms, 0)
            if age_ms <= threshold_seconds * 1000:
                fresh_count += 1

        stale_count = max(seen_count - fresh_count, 0)
        missing_count = max(len(symbols) - seen_count, 0)
        latest_age_seconds = None
        if latest_timestamp_ms > 0:
            latest_age_seconds = max((now_ms - latest_timestamp_ms) // 1000, 0)
        denominator = len(symbols)
        fresh_ratio = (fresh_count / denominator) if denominator else 0.0
        return {
            "fresh_count": fresh_count,
            "stale_count": stale_count,
            "missing_count": missing_count,
            "timestamped_count": timestamped_count,
            "threshold_seconds": threshold_seconds,
            "latest_timestamp_ms": latest_timestamp_ms,
            "latest_time": self._format_quote_time(latest_timestamp_ms),
            "latest_age_seconds": latest_age_seconds,
            "fresh_ratio": fresh_ratio,
        }

    def prime_runtime_state(self, request: IntradayContextRequest) -> PrimedIntradayRuntimeState:
        self._prepare_scope_cache(request.trade_date, request.minute_index)
        offline_context_date = request.offline_context_date or request.previous_trade_date
        symbols = tuple(dict.fromkeys(_normalize_symbol(symbol) for symbol in request.symbols if _normalize_symbol(symbol)))

        if request.now is not None and request.now.strftime("%Y-%m-%d") == request.trade_date:
            self._ensure_hot_rank_cache(request.trade_date, request.phase)
        quotes_result = self.hub.fetch_redis_quotes(symbols)
        cache_result = self.hub.load_runtime_cache_views(
            offline_context_date,
            symbols,
            hot_rank_trade_date=request.trade_date,
        )
        auction_rows = self._load_auction_rows(request, symbols)
        yest_limit_map = self._load_json_hash(f"cache:yest_limit_pool:{request.previous_trade_date}")
        hot_plate_map = self._load_json_hash(f"cache:hot_plates:{request.trade_date}")
        yesterday_hot_plate_map = self._load_json_hash(f"cache:hot_plates:{request.previous_trade_date}")
        effective_hot_plate_map = hot_plate_map
        effective_hot_plate_trade_date = request.trade_date
        if (
            request.phase in (RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.INTRADAY)
            and not effective_hot_plate_map
            and yesterday_hot_plate_map
        ):
            effective_hot_plate_map = yesterday_hot_plate_map
            effective_hot_plate_trade_date = request.previous_trade_date
        hot_plate_meta = self._load_hot_plate_meta(effective_hot_plate_trade_date)
        stock_plate_map = self._load_string_hash("market:stock_plate")
        stock_theme_map = self._load_list_hash(PLATE_MAPPING_S2P_KEY)
        stock_reason_map = self._load_string_hash("market:stock_reason")
        market_runtime_state = self._load_market_runtime_state(request.trade_date)
        quote_health = self._summarize_quote_freshness(
            phase=request.phase,
            symbols=symbols,
            quote_rows=quotes_result.rows,
            now=request.now,
        )

        quote_map = {row["symbol"]: row for row in quotes_result.rows if row.get("symbol")}
        cache_map = {row["symbol"]: row for row in cache_result.rows if row.get("symbol")}
        auction_map = {row["symbol"]: row for row in auction_rows if row.get("symbol")}
        tick_metric_map = self.tick_tracker.ingest_quotes(
            quotes_result.rows,
            minute_index=request.minute_index,
        )

        primed_state = PrimedIntradayRuntimeState(
            phase=request.phase,
            trade_date=request.trade_date,
            previous_trade_date=request.previous_trade_date,
            offline_context_date=offline_context_date,
            symbols=symbols,
            quote_rows=tuple(quotes_result.rows),
            cache_rows=tuple(cache_result.rows),
            auction_rows=tuple(auction_rows),
            yest_limit_map=yest_limit_map,
            hot_plate_map=hot_plate_map,
            yesterday_hot_plate_map=yesterday_hot_plate_map,
            effective_hot_plate_map=effective_hot_plate_map,
            stock_plate_map=stock_plate_map,
            stock_theme_map=stock_theme_map,
            stock_reason_map=stock_reason_map,
            market_runtime_state=market_runtime_state,
            native_ingested=len(quotes_result.rows),
            tick_metric_map=tick_metric_map,
            quote_fresh_count=int(quote_health["fresh_count"]),
            quote_stale_count=int(quote_health["stale_count"]),
            quote_missing_count=int(quote_health["missing_count"]),
            quote_timestamped_count=int(quote_health["timestamped_count"]),
            quote_stale_threshold_seconds=int(quote_health["threshold_seconds"]),
            latest_quote_timestamp_ms=int(quote_health["latest_timestamp_ms"]),
            latest_quote_time=str(quote_health["latest_time"]),
            latest_quote_age_seconds=quote_health["latest_age_seconds"],
            quote_fresh_ratio=float(quote_health["fresh_ratio"]),
            hot_plate_cache_trade_date=effective_hot_plate_trade_date,
            hot_plate_updated_at_ts=int(float(hot_plate_meta.get("updated_at_ts", 0) or 0)),
            hot_plate_signature=self._hot_plate_signature(
                effective_hot_plate_map,
                trade_date=effective_hot_plate_trade_date,
            ),
            yest_limit_signature=self._yest_limit_signature(
                primed_trade_date=request.previous_trade_date,
                yest_limit_map=yest_limit_map,
            ),
            auction_signature=self._auction_signature(auction_rows),
            notes=(
                f"quotes={len(quotes_result.rows)}",
                (
                    "quote_freshness="
                    f"{int(quote_health['fresh_count'])}/{len(symbols)}"
                    f" latest={quote_health['latest_time'] or '-'}"
                ),
                f"cache_views={len(cache_result.rows)}",
                f"native_ingested={len(quotes_result.rows)}",
                "native_runtime=t1_v2_q2",
                f"tick_metrics={len(tick_metric_map)}",
                f"auction={len(auction_rows)}",
                f"yest_limit={len(yest_limit_map)}",
                f"hot_plates_today={len(hot_plate_map)}",
                f"hot_plates_yesterday={len(yesterday_hot_plate_map)}",
                f"hot_plates_effective={len(effective_hot_plate_map)}",
                f"hot_plate_effective_trade_date={effective_hot_plate_trade_date}",
                f"hot_plate_meta_ts={int(float(hot_plate_meta.get('updated_at_ts', 0) or 0))}",
            ),
        )
        self._primed_runtime_state = primed_state
        return primed_state

    def build(self, request: IntradayContextRequest) -> IntradayContext:
        primed = self.prime_runtime_state(request)
        return self.build_from_primed(primed)

    def build_from_primed(self, primed: PrimedIntradayRuntimeState) -> IntradayContext:
        quote_map = {row["symbol"]: row for row in primed.quote_rows if row.get("symbol")}
        cache_map = {row["symbol"]: row for row in primed.cache_rows if row.get("symbol")}
        auction_map = {row["symbol"]: row for row in primed.auction_rows if row.get("symbol")}
        fallback_name_map = self._load_fallback_stock_names(
            symbol
            for symbol in primed.symbols
            if not self._resolve_snapshot_name(
                quote=quote_map.get(symbol, {}),
                cache=cache_map.get(symbol, {}),
                auction=auction_map.get(symbol, {}),
                yest=primed.yest_limit_map.get(symbol, {}),
            )
        )

        snapshots = []
        for symbol in primed.symbols:
            quote = quote_map.get(symbol, {})
            cache = cache_map.get(symbol, {})
            tick_metrics = primed.tick_metric_map.get(symbol)
            auction = auction_map.get(symbol, {})
            yest = primed.yest_limit_map.get(symbol, {})
            theme_names = primed.stock_theme_map.get(symbol, ())

            price = float(quote.get("price", 0.0) or 0.0)
            pre_close = float(quote.get("pre_close", 0.0) or 0.0)
            current_pct = ((price / pre_close) - 1.0) if price > 0 and pre_close > 0 else float(cache.get("pct_chg", 0.0) or 0.0)
            limit_state = int(quote.get("limit_state", 0) or 0)
            is_limit_up_now = limit_state == 1
            auction_pct = normalize_auction_pct_ratio(auction.get("change_pct", 0.0))
            auction_from_q2 = False
            q2_a25_milli = float(quote.get("q2_a25_milli", 0.0) or 0.0)
            if not auction and q2_a25_milli > 0 and pre_close > 0:
                auction_pct = normalize_auction_pct_ratio((q2_a25_milli / (pre_close * 1000.0)) - 1.0)
                auction_from_q2 = True
            if not auction_from_q2 and price > 0 and pre_close > 0 and abs(current_pct) <= 0.35:
                if abs(auction_pct) > 0.35 or abs(auction_pct - current_pct) > 0.15:
                    auction_pct = current_pct
            peak_price = float(cache.get("peak_price", 0.0) or 0.0)
            resistance_gap = (peak_price - price) / price if peak_price > price > 0 else 0.0
            plate = self._resolve_snapshot_plate(
                runtime_plate=str(primed.stock_plate_map.get(symbol) or ""),
                yest_plate=str(yest.get("plate") or ""),
                themes=theme_names,
                reason=str(primed.stock_reason_map.get(symbol) or ""),
                hot_plate_map=primed.effective_hot_plate_map,
            )
            market_cap_yi = self._to_yi(cache.get("real_market_cap"))
            amount_day_yi = self._to_yi(quote.get("amount"))
            speed_1m = float(quote.get("speed_1m", quote.get("change_rate_1min", 0.0)) or 0.0)
            if speed_1m == 0.0 and tick_metrics is not None:
                speed_1m = float(tick_metrics.speed_1m)
            if speed_1m == 0.0:
                speed_1m = float(cache.get("speed_1m", 0.0) or 0.0)
            amount_2m = float(quote.get("amount_2m", quote.get("amount_2min", 0.0)) or 0.0)
            if amount_2m <= 0.0 and tick_metrics is not None:
                amount_2m = float(tick_metrics.amount_2m)
            if amount_2m <= 0.0:
                amount_2m = float(cache.get("amount_2m", 0.0) or 0.0)
            amount_5m = float(quote.get("amount_5m", 0.0) or 0.0)
            vector_3m = float(quote.get("vector_3m", 0.0) or 0.0)
            vector_5m = float(quote.get("vector_5m", 0.0) or 0.0)
            bid_amount = float(quote.get("bid_amount", quote.get("book1_amount_yuan", 0.0)) or 0.0)
            volume_intensity = 1.0
            if bid_amount > 0 and pre_close > 0:
                volume_intensity = max(1.0, round(bid_amount / 10_000_000, 2))
            elif amount_day_yi > 0:
                volume_intensity = max(1.0, round(min(amount_day_yi / 10, 5.0), 2))

            snapshots.append(
                StockStateSnapshot(
                    symbol=symbol,
                    name=self._resolve_snapshot_name(
                        quote=quote,
                        cache=cache,
                        auction=auction,
                        yest=yest,
                    )
                    or str(fallback_name_map.get(symbol) or ""),
                    plate=plate,
                    lb_days=int(yest.get("lb_days", 0) or 0),
                    open_pct=auction_pct,
                    current_pct=current_pct,
                    change_pct=current_pct,
                    auction_amount=float(
                        auction.get("amount", quote.get("q2_auction_amount_yuan", 0.0)) or 0.0
                    ),
                    volume_intensity=volume_intensity,
                    vol_ratio=float(cache.get("vol_ratio", 0.0) or 0.0),
                    speed_1m=speed_1m,
                    amount_2m=amount_2m,
                    amount_5m=amount_5m,
                    vector_3m=vector_3m,
                    vector_5m=vector_5m,
                    resonance_factor=self._plate_resonance(plate, primed.effective_hot_plate_map),
                    resistance_gap=resistance_gap,
                    concentration=float(cache.get("concentration", 0.0) or 0.0),
                    profit_ratio=float(cache.get("profit_ratio", 0.0) or 0.0),
                    bias_20=float(cache.get("bias_20", 0.0) or 0.0),
                    rsi_6=float(cache.get("rsi_6", 0.0) or 0.0),
                    ddje=float(cache.get("ddje", 0.0) or 0.0),
                    ddx=float(cache.get("ddx", 0.0) or 0.0),
                    ddy=float(cache.get("ddy", 0.0) or 0.0),
                    ddz=float(cache.get("ddz", 0.0) or 0.0),
                    structure_score_base=float(cache.get("structure_score_base", 0.0) or 0.0),
                    shape_platform_ready=float(cache.get("shape_platform_ready", 0.0) or 0.0),
                    shape_breakout_ready=float(cache.get("shape_breakout_ready", 0.0) or 0.0),
                    shape_repair_ready=float(cache.get("shape_repair_ready", 0.0) or 0.0),
                    shape_overheat_risk=float(cache.get("shape_overheat_risk", 0.0) or 0.0),
                    shape_chip_cleanliness=float(cache.get("shape_chip_cleanliness", 0.0) or 0.0),
                    shape_trend_health=float(cache.get("shape_trend_health", 0.0) or 0.0),
                    shape_t2_repair_bias=float(cache.get("shape_t2_repair_bias", 0.0) or 0.0),
                    theme_core_base=float(cache.get("theme_core_base", 0.0) or 0.0),
                    market_cap_yi=market_cap_yi,
                    amount_day_yi=amount_day_yi,
                    plate_persistence_score=self._plate_persistence_score(plate, primed.effective_hot_plate_map, primed.yesterday_hot_plate_map),
                    hot_plate_days=self._hot_plate_days(plate, primed.effective_hot_plate_map, primed.yesterday_hot_plate_map),
                    ths_hot_rank=self._to_optional_int(cache.get("rank")),
                    ths_hot_heat=float(cache.get("heat", 0.0) or 0.0),
                    t2_lb_days=int(cache.get("t2_lb_days", 0) or 0),
                    t2_pct=float(cache.get("t2_pct", 0.0) or 0.0),
                    is_yest_limit=bool(yest),
                    touched_limit_today=is_limit_up_now,
                    is_locked=is_limit_up_now,
                    real_plate_names=self._merge_plate_names(
                        plate,
                        primed.stock_reason_map.get(symbol, ""),
                        (
                            str(primed.stock_plate_map.get(symbol) or ""),
                            str(yest.get("plate") or ""),
                            *theme_names,
                        ),
                    ),
                )
            )

        ranked_snapshots = self._attach_theme_ranks(snapshots)
        session_facts = self._load_cached_session_facts(
            trade_date=primed.trade_date,
            phase=primed.phase,
            latest_quote_timestamp_ms=primed.latest_quote_timestamp_ms,
            symbol_count=len(ranked_snapshots),
            hot_plate_cache_trade_date=primed.hot_plate_cache_trade_date,
            hot_plate_updated_at_ts=primed.hot_plate_updated_at_ts,
            hot_plate_signature=primed.hot_plate_signature,
            yest_limit_signature=primed.yest_limit_signature,
            auction_signature=primed.auction_signature,
        )
        if session_facts is None:
            session_facts = build_session_facts(
                trade_date=primed.trade_date,
                phase_name=primed.phase.value,
                snapshots=ranked_snapshots,
                hot_plate_map=primed.effective_hot_plate_map,
                yesterday_hot_plate_map=primed.yesterday_hot_plate_map,
            )
            self._write_cached_session_facts(
                trade_date=primed.trade_date,
                phase=primed.phase,
                latest_quote_timestamp_ms=primed.latest_quote_timestamp_ms,
                symbol_count=len(ranked_snapshots),
                hot_plate_cache_trade_date=primed.hot_plate_cache_trade_date,
                hot_plate_updated_at_ts=primed.hot_plate_updated_at_ts,
                hot_plate_signature=primed.hot_plate_signature,
                yest_limit_signature=primed.yest_limit_signature,
                auction_signature=primed.auction_signature,
                facts=session_facts,
            )
        market_summary = self._build_market_summary(
            snapshots=ranked_snapshots,
            auction_map=auction_map,
            yest_limit_map=primed.yest_limit_map,
            session_facts=session_facts,
            cache_rows=tuple(cache_map.values()),
            market_runtime_state=primed.market_runtime_state,
            previous_market_runtime_state=self._load_market_runtime_state(primed.previous_trade_date),
        )
        return IntradayContext(
            phase=primed.phase,
            trade_date=primed.trade_date,
            offline_context_date=primed.offline_context_date,
            stock_snapshots=tuple(ranked_snapshots),
            market_summary=market_summary,
            session_facts=session_facts,
            hot_plate_map=primed.hot_plate_map,
            yesterday_hot_plate_map=primed.yesterday_hot_plate_map,
            yest_limit_map=primed.yest_limit_map,
            auction_map=auction_map,
            notes=primed.notes,
        )

    def _load_auction_rows(self, request: IntradayContextRequest, symbols: tuple[str, ...]) -> list[dict[str, Any]]:
        if request.phase in (RunPhase.AUCTION, RunPhase.INTRADAY) or request.require_auction_recovery:
            result = self.hub.recover_auction_anchor(request.trade_date, request.phase)
            rows = [row for row in result.rows if _normalize_symbol(row.get("symbol")) in symbols]
            if rows:
                return rows

        tag = request.trade_date.replace("-", "")
        archive = self.hub.redis.get(f"market:auction:anchor:{tag}")
        if archive:
            try:
                payload = json.loads(archive)
                return [
                    (
                        {
                            "symbol": _normalize_symbol(symbol),
                            "change_pct": normalize_auction_pct_ratio(change_pct.get("change_pct", 0.0)),
                            "amount": float(change_pct.get("amount", 0.0) or 0.0),
                            "bid_amount": float(change_pct.get("bid_amount", 0.0) or 0.0),
                            "source": str(change_pct.get("source") or "redis_anchor"),
                        }
                        if isinstance(change_pct, dict)
                        else {
                            "symbol": _normalize_symbol(symbol),
                            "change_pct": normalize_auction_pct_ratio(change_pct),
                            "amount": 0.0,
                            "bid_amount": 0.0,
                            "source": "redis_anchor",
                        }
                    )
                    for symbol, change_pct in payload.items()
                    if _normalize_symbol(symbol) in symbols
                ]
            except Exception:
                return []
        return []

    def _load_json_hash(self, redis_key: str) -> dict[str, dict]:
        cached = self._json_hash_cache.get(redis_key)
        if cached is not None:
            return cached
        raw = self.hub.redis.hgetall(redis_key) or {}
        result: dict[str, dict] = {}
        for key, value in raw.items():
            try:
                parsed = json.loads(value) if isinstance(value, str) else value
            except Exception:
                continue
            if isinstance(parsed, dict):
                result[str(key)] = parsed
        self._json_hash_cache[redis_key] = result
        return result

    def _load_string_hash(self, redis_key: str) -> dict[str, str]:
        cached = self._string_hash_cache.get(redis_key)
        if cached is not None:
            return cached
        raw = self.hub.redis.hgetall(redis_key) or {}
        result = {str(key): str(value) for key, value in raw.items() if value not in (None, "")}
        self._string_hash_cache[redis_key] = result
        return result

    def _load_list_hash(self, redis_key: str) -> dict[str, tuple[str, ...]]:
        raw = self.hub.redis.hgetall(redis_key) or {}
        result: dict[str, tuple[str, ...]] = {}
        for key, value in raw.items():
            decoded = tuple(decode_theme_list(value))
            if decoded:
                result[str(key)] = decoded
        return result

    def _load_market_runtime_state(self, trade_date: str) -> dict[str, Any]:
        candidates = (
            f"market:runtime:summary:{trade_date}",
            "market:runtime:summary:latest",
        )
        for key in candidates:
            payload = self._load_json_string(key)
            if isinstance(payload, dict):
                return payload
        return {}

    def _load_json_string(self, redis_key: str) -> dict[str, Any] | None:
        if redis_key in self._string_key_cache:
            payload = self._string_key_cache[redis_key]
            return payload if isinstance(payload, dict) else None
        raw = self.hub.redis.get(redis_key)
        if not raw:
            self._string_key_cache[redis_key] = {}
            return None
        try:
            payload = json.loads(raw)
        except Exception:
            self._string_key_cache[redis_key] = {}
            return None
        if isinstance(payload, dict):
            self._string_key_cache[redis_key] = payload
            return payload
        self._string_key_cache[redis_key] = {}
        return None

    def _load_hot_plate_meta(self, trade_date: str) -> dict[str, Any]:
        payload = self._load_json_string(f"cache:hot_plates_meta:{trade_date}")
        return payload if isinstance(payload, dict) else {}

    def _load_hot_rank_meta(self, trade_date: str) -> dict[str, Any]:
        payload = self._load_json_string(f"cache:hot_rank_meta:{trade_date}")
        return payload if isinstance(payload, dict) else {}

    def _ensure_hot_rank_cache(self, trade_date: str, phase: RunPhase) -> None:
        if phase not in (RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.INTRADAY):
            return
        redis_key = f"cache:hot_rank:{trade_date}"
        meta = self._load_hot_rank_meta(trade_date)
        stale_limit_seconds = 30 * 60 if phase in (RunPhase.AUCTION, RunPhase.INTRADAY) else 4 * 60 * 60
        try:
            updated_at_ts = int(float(meta.get("updated_at_ts", 0) or 0))
        except (TypeError, ValueError):
            updated_at_ts = 0
        age_seconds = max(int(datetime.now().timestamp()) - updated_at_ts, 0) if updated_at_ts > 0 else None
        has_cache = bool(self.hub.redis.hlen(redis_key) or 0)
        is_fresh = bool(has_cache and age_seconds is not None and age_seconds <= stale_limit_seconds)
        if is_fresh:
            return
        try:
            self.hub.fetch_hot_rank(trade_date, phase, top_n=200)
        except Exception:
            return

    def _prepare_scope_cache(self, trade_date: str, minute_index: int | None) -> None:
        token = (trade_date, minute_index)
        if self._scoped_cache_token == token:
            return
        self._scoped_cache_token = token
        self._json_hash_cache = {}
        self._string_hash_cache = {}
        self._string_key_cache = {}

    def _session_facts_cache_key(self, trade_date: str, phase: RunPhase) -> str:
        return f"cache:session_facts:{trade_date}:{phase.value}"

    def _session_facts_meta_key(self, trade_date: str, phase: RunPhase) -> str:
        return f"cache:session_facts_meta:{trade_date}:{phase.value}"

    def _load_cached_session_facts(
        self,
        *,
        trade_date: str,
        phase: RunPhase,
        latest_quote_timestamp_ms: int,
        symbol_count: int,
        hot_plate_cache_trade_date: str,
        hot_plate_updated_at_ts: int,
        hot_plate_signature: str,
        yest_limit_signature: str,
        auction_signature: str,
    ):
        meta = self._load_json_string(self._session_facts_meta_key(trade_date, phase))
        if not isinstance(meta, dict):
            return None
        try:
            meta_quote_ts = int(float(meta.get("latest_quote_timestamp_ms", 0) or 0))
            meta_symbol_count = int(meta.get("symbol_count", 0) or 0)
            meta_hot_plate_ts = int(float(meta.get("hot_plate_updated_at_ts", 0) or 0))
        except (TypeError, ValueError):
            return None
        if (
            meta_quote_ts != latest_quote_timestamp_ms
            or meta_symbol_count != symbol_count
            or str(meta.get("hot_plate_cache_trade_date") or "") != hot_plate_cache_trade_date
            or meta_hot_plate_ts != hot_plate_updated_at_ts
            or str(meta.get("hot_plate_signature") or "") != hot_plate_signature
            or str(meta.get("yest_limit_signature") or "") != yest_limit_signature
            or str(meta.get("auction_signature") or "") != auction_signature
        ):
            return None
        payload = self._load_json_string(self._session_facts_cache_key(trade_date, phase))
        if not isinstance(payload, dict):
            return None
        try:
            return session_facts_from_payload(payload)
        except Exception:
            return None

    def _write_cached_session_facts(
        self,
        *,
        trade_date: str,
        phase: RunPhase,
        latest_quote_timestamp_ms: int,
        symbol_count: int,
        hot_plate_cache_trade_date: str,
        hot_plate_updated_at_ts: int,
        hot_plate_signature: str,
        yest_limit_signature: str,
        auction_signature: str,
        facts,
    ) -> None:
        payload = session_facts_to_payload(facts)
        meta = {
            "fact_set_id": facts.fact_set_id,
            "trade_date": trade_date,
            "phase": phase.value,
            "latest_quote_timestamp_ms": latest_quote_timestamp_ms,
            "symbol_count": symbol_count,
            "hot_plate_cache_trade_date": hot_plate_cache_trade_date,
            "hot_plate_updated_at_ts": hot_plate_updated_at_ts,
            "hot_plate_signature": hot_plate_signature,
            "yest_limit_signature": yest_limit_signature,
            "auction_signature": auction_signature,
        }
        self.hub.redis.set(
            self._session_facts_cache_key(trade_date, phase),
            json.dumps(payload, ensure_ascii=False),
        )
        self.hub.redis.set(
            self._session_facts_meta_key(trade_date, phase),
            json.dumps(meta, ensure_ascii=False),
        )
        self._string_key_cache[self._session_facts_cache_key(trade_date, phase)] = payload
        self._string_key_cache[self._session_facts_meta_key(trade_date, phase)] = meta

    def _plate_persistence_score(
        self,
        plate: str,
        hot_plate_map: dict[str, dict],
        yesterday_hot_plate_map: dict[str, dict],
    ) -> float:
        score = 0.0
        if plate in hot_plate_map:
            score += 1.0
            rank = int(hot_plate_map[plate].get("rank", 99) or 99)
            score += max(0.0, (20 - min(rank, 20)) / 20)
        if plate in yesterday_hot_plate_map:
            score += 0.8
        return round(score, 2)

    def _hot_plate_days(
        self,
        plate: str,
        hot_plate_map: dict[str, dict],
        yesterday_hot_plate_map: dict[str, dict],
    ) -> int:
        days = 0
        if plate in yesterday_hot_plate_map:
            days += 1
        if plate in hot_plate_map:
            days += 1
        return days

    def _merge_plate_names(self, plate: str, reason: str, themes: Iterable[str] = ()) -> tuple[str, ...]:
        reason_candidates = build_plate_candidates_from_reason(reason=reason) if reason else []
        names = merge_theme_lists((), (plate, *reason_candidates, *themes))
        if not names:
            return ()
        normalized_plate = str(plate).strip()
        if normalized_plate:
            names = [normalized_plate] + [name for name in names if name != normalized_plate]
        primary = names[0]
        secondary_candidates = [name for name in names[1:] if name != primary]
        prioritized_reason_candidates = [
            name for name in reason_candidates if name and name != primary and name in secondary_candidates
        ]
        for name in prioritized_reason_candidates:
            return (primary, name)
        nongeneric = [name for name in secondary_candidates if not is_generic_plate(name)]
        generic = [name for name in secondary_candidates if is_generic_plate(name)]
        ordered = [primary, *(nongeneric or generic)]
        return tuple(ordered[:2])

    def _attach_theme_ranks(self, snapshots: list[StockStateSnapshot]) -> list[StockStateSnapshot]:
        grouped: dict[str, list[StockStateSnapshot]] = {}
        for snapshot in snapshots:
            grouped.setdefault(self._resolve_theme_group(snapshot), []).append(snapshot)

        rank_map: dict[str, int] = {}
        for peers in grouped.values():
            sorted_peers = sorted(peers, key=lambda item: (item.lb_days, item.auction_amount, item.current_pct), reverse=True)
            for idx, peer in enumerate(sorted_peers, start=1):
                rank_map[peer.symbol] = idx

        ranked: list[StockStateSnapshot] = []
        for snapshot in snapshots:
            ranked.append(
                StockStateSnapshot(
                    **{
                        **snapshot.__dict__,
                        "leader_rank_in_theme": rank_map.get(snapshot.symbol, 999),
                    }
                )
            )
        return ranked

    def _resolve_theme_group(self, snapshot: StockStateSnapshot) -> str:
        candidates = []
        for raw in (snapshot.plate, *snapshot.real_plate_names):
            for token in split_plate_tokens(raw):
                if token not in candidates:
                    candidates.append(token)
        for name in candidates:
            if not is_generic_plate(name):
                return name
        return candidates[0] if candidates else ""

    def _build_market_summary(
        self,
        *,
        snapshots: list[StockStateSnapshot],
        auction_map: dict[str, dict],
        yest_limit_map: dict[str, dict],
        session_facts,
        cache_rows: tuple[dict[str, Any], ...],
        market_runtime_state: dict[str, Any],
        previous_market_runtime_state: dict[str, Any] | None = None,
    ) -> IntradayMarketSummary:
        top_plate_name = ""
        top_plate_strength = 0.0
        top_plate_migration_type = ""
        mainline_net_inflow_yi = 0.0
        top_sector_pct = 0.0
        resonance_score = 0.0
        yest_hot_plate_match_count = 0
        persistent_plate_count = 0
        emerging_plate_count = 0
        fading_plate_count = 0
        mainline_switch = False
        previous_top_plate_name = ""
        hot_plate_map = {fact.plate_name: fact for fact in session_facts.hot_plate_today}
        yesterday_hot_plate_map = {fact.plate_name: fact for fact in session_facts.hot_plate_yesterday}
        if yesterday_hot_plate_map:
            previous_top_plate_name = session_facts.hot_plate_yesterday[0].plate_name if session_facts.hot_plate_yesterday else ""
        runtime_mainline_sector = self._infer_runtime_mainline_sector(snapshots, hot_plate_map)
        if session_facts.hot_plate_today:
            top_fact = session_facts.hot_plate_today[0]
            top_plate_name = top_fact.plate_name
            top_plate_strength = top_fact.strength
            mainline_net_inflow_yi = top_fact.net_inflow_yi
            top_sector_pct = top_fact.change_pct
            top_strengths = [fact.strength for fact in session_facts.hot_plate_today[:5] if fact.strength > 0.0]
            if top_strengths:
                resonance_score = round(sum(top_strengths) / len(top_strengths), 2)
            for migration in session_facts.plate_migration:
                migration_type = self._classify_plate_migration(migration)
                if migration.present_today and migration.present_yesterday:
                    yest_hot_plate_match_count += 1
                    if migration_type == "PERSIST":
                        persistent_plate_count += 1
                    elif migration_type == "FADING":
                        fading_plate_count += 1
                    else:
                        emerging_plate_count += 1
                elif migration.present_today:
                    if migration_type == "EMERGING":
                        emerging_plate_count += 1
                    elif migration_type == "FADING":
                        fading_plate_count += 1
                    else:
                        persistent_plate_count += 1
                elif migration.present_yesterday and migration_type == "FADING":
                    fading_plate_count += 1
                if migration.plate_name == top_plate_name:
                    top_plate_migration_type = migration_type
            if not top_plate_migration_type and top_plate_name:
                top_plate_migration_type = "PERSIST" if top_plate_name in yesterday_hot_plate_map else "EMERGING"
            mainline_switch = bool(previous_top_plate_name and top_plate_name and previous_top_plate_name != top_plate_name)

        yest_limit_symbols = set(yest_limit_map.keys())
        total = len(yest_limit_symbols)
        red_open_cnt = 0
        promotion_cnt = 0
        headshot_cnt = 0
        total_bid_amt = 0.0
        snapshot_map = {snapshot.symbol: snapshot for snapshot in snapshots}
        for symbol in yest_limit_symbols:
            snapshot = snapshot_map.get(symbol)
            if not snapshot:
                continue
            if snapshot.open_pct > 0:
                red_open_cnt += 1
            if snapshot.is_locked or snapshot.touched_limit_today:
                promotion_cnt += 1
            if snapshot.open_pct > 0.05 and snapshot.current_pct < 0:
                headshot_cnt += 1
            auction_row = auction_map.get(symbol, {})
            total_bid_amt += float(auction_row.get("amount", 0.0) or 0.0)

        promotion_rate = (promotion_cnt / total) if total else 0.0
        red_open_rate = (red_open_cnt / total) if total else 0.0
        headshot_rate = (headshot_cnt / total) if total else 0.0
        sentiment_score = round((promotion_rate * 0.5 + red_open_rate * 0.3 + (1 - headshot_rate) * 0.2) * 10, 1) if total else 0.0
        red_green_ratio = red_open_cnt / max(1, total - red_open_cnt) if total else 0.0
        avg_bid_amt = total_bid_amt / total if total else 0.0
        context_auc_amt = sum(float(row.get("amount", 0.0) or 0.0) for row in auction_map.values())
        context_coverage_factor = 1.0
        sample_size = len(auction_map)
        if 800 <= sample_size <= 1500:
            context_coverage_factor = 0.65
        elif 50 <= sample_size < 800:
            context_coverage_factor = 0.35
        avg_5d_values = []
        for row in cache_rows:
            raw_avg = row.get("avg_turnover_5d")
            try:
                avg_value = float(raw_avg or 0.0)
            except (TypeError, ValueError):
                avg_value = 0.0
            if avg_value > 0:
                avg_5d_values.append(avg_value)
        context_avg_5d_vol = sum(avg_5d_values) if avg_5d_values else 0.0

        market_full_auc_amt = self._to_float(market_runtime_state.get("full_market_auc_amt"))
        market_avg_5d_vol = self._to_float(market_runtime_state.get("avg_5d_vol"))
        market_predicted_full_day_amount = self._to_float(market_runtime_state.get("predicted_full_day_amount"))
        auction_top10_amount = self._to_float(market_runtime_state.get("auction_top10_amount"))
        auction_top20_amount = self._to_float(market_runtime_state.get("auction_top20_amount"))
        open_2m_top10_amount = self._to_float(market_runtime_state.get("open_2m_top10_amount"))
        open_2m_top20_amount = self._to_float(market_runtime_state.get("open_2m_top20_amount"))
        previous_market_runtime_state = previous_market_runtime_state or {}
        previous_auction_top10_amount = self._to_float(previous_market_runtime_state.get("auction_top10_amount"))
        previous_auction_top20_amount = self._to_float(previous_market_runtime_state.get("auction_top20_amount"))
        previous_open_2m_top10_amount = self._to_float(previous_market_runtime_state.get("open_2m_top10_amount"))
        previous_open_2m_top20_amount = self._to_float(previous_market_runtime_state.get("open_2m_top20_amount"))
        auction_top10_vs_prev_ratio = (
            auction_top10_amount / previous_auction_top10_amount if previous_auction_top10_amount > 0.0 else 1.0
        )
        auction_top20_vs_prev_ratio = (
            auction_top20_amount / previous_auction_top20_amount if previous_auction_top20_amount > 0.0 else 1.0
        )
        open_2m_top10_vs_prev_ratio = (
            open_2m_top10_amount / previous_open_2m_top10_amount if previous_open_2m_top10_amount > 0.0 else 1.0
        )
        open_2m_top20_vs_prev_ratio = (
            open_2m_top20_amount / previous_open_2m_top20_amount if previous_open_2m_top20_amount > 0.0 else 1.0
        )
        if market_predicted_full_day_amount <= 0.0 and market_full_auc_amt > 0.0:
            market_predicted_full_day_amount = market_full_auc_amt / 0.045
        market_volume_level = str(market_runtime_state.get("volume_level") or "")
        if not market_volume_level and market_predicted_full_day_amount > 0 and market_avg_5d_vol > 0:
            market_volume_level = (
                "high"
                if market_predicted_full_day_amount > market_avg_5d_vol * 1.1
                else ("low" if market_predicted_full_day_amount < market_avg_5d_vol * 0.9 else "flat")
            )

        top_turnover_symbols = tuple(
            snapshot.symbol
            for snapshot in nlargest(
                20,
                snapshots,
                key=lambda item: (
                    float(item.amount_day_yi or 0.0),
                    float(item.auction_amount or 0.0),
                ),
            )
            if snapshot.symbol
        )

        return IntradayMarketSummary(
            top_turnover_symbols=top_turnover_symbols,
            top_plate_name=top_plate_name,
            top_plate_strength=top_plate_strength,
            top_plate_migration_type=top_plate_migration_type,
            mainline_sector=runtime_mainline_sector or top_plate_name,
            mainline_net_inflow_yi=mainline_net_inflow_yi,
            top_sector_pct=top_sector_pct,
            resonance_score=resonance_score,
            hot_plate_count=len(hot_plate_map),
            yest_hot_plate_count=len(yesterday_hot_plate_map),
            yest_hot_plate_match_count=yest_hot_plate_match_count,
            persistent_plate_count=persistent_plate_count,
            emerging_plate_count=emerging_plate_count,
            fading_plate_count=fading_plate_count,
            mainline_switch=mainline_switch,
            total_yest_limit_count=total,
            context_auc_amt=context_auc_amt,
            context_avg_5d_vol=context_avg_5d_vol,
            context_symbol_count=sample_size,
            context_coverage_factor=context_coverage_factor,
            market_full_auc_amt=market_full_auc_amt,
            market_predicted_full_day_amount=market_predicted_full_day_amount,
            market_avg_5d_vol=market_avg_5d_vol,
            market_volume_level=market_volume_level,
            promotion_rate=promotion_rate,
            red_open_rate=red_open_rate,
            headshot_rate=headshot_rate,
            sentiment_score=sentiment_score,
            red_green_ratio=red_green_ratio,
            avg_bid_amt=avg_bid_amt,
            auction_top10_amount=auction_top10_amount,
            auction_top20_amount=auction_top20_amount,
            auction_top10_vs_prev_ratio=round(auction_top10_vs_prev_ratio, 3),
            auction_top20_vs_prev_ratio=round(auction_top20_vs_prev_ratio, 3),
            open_2m_top10_amount=open_2m_top10_amount,
            open_2m_top20_amount=open_2m_top20_amount,
            open_2m_top10_vs_prev_ratio=round(open_2m_top10_vs_prev_ratio, 3),
            open_2m_top20_vs_prev_ratio=round(open_2m_top20_vs_prev_ratio, 3),
            battle_status=self._judge_battle_status(headshot_rate, promotion_rate, total),
            notes=(
                "mainline sector is inferred from runtime snapshots and falls back to the strongest Kaipan hot-plate signal",
                "top_plate_name remains the strongest Kaipan hot-plate signal for cross-checking runtime mainline",
                "top turnover comes from t1_v2/q2 amount fields and falls back to auction amount",
                "auction_top10/top20 compare front-row auction concentration against previous trade day",
                "open_2m_top10/top20 compare opening two-minute front-row strength against previous trade day",
                "open_2m market slice must come from explicit runtime summary cache and must not fall back to request-scope snapshots",
                "hot-plate strength/change_pct/net inflow follow Kaipan normalized contract when present",
                "net inflow is interpreted together with change_pct to distinguish buying pressure from distribution",
                "plate migration is classified only from strength/change_pct/net inflow deltas plus today/yesterday presence",
                "context_* fields are derived from the requested symbol set only",
                "market_* fields must come from explicit runtime summary cache, not from watchlist subset inference",
            ),
        )

    def _infer_runtime_mainline_sector(
        self,
        snapshots: list[StockStateSnapshot],
        hot_plate_map: dict[str, Any],
    ) -> str:
        plate_scores: dict[str, float] = {}
        plate_leader_counts: dict[str, int] = {}
        for snapshot in snapshots:
            plate_names = self._runtime_plate_names(snapshot)
            if not plate_names:
                continue
            hot_signal = self._match_hot_plate_signal(plate_names, hot_plate_map)
            leader_bonus = 0.0
            if snapshot.leader_rank_in_theme == 1:
                leader_bonus = 3.5
            elif 1 < snapshot.leader_rank_in_theme <= 3:
                leader_bonus = 2.0
            score = 0.0
            score += min(snapshot.auction_amount / 100_000_000, 8.0) * 1.8
            score += min(snapshot.amount_2m / 100_000_000, 10.0) * 1.2
            score += min(snapshot.amount_day_yi, 30.0) * 0.35
            score += max(snapshot.current_pct, 0.0) * 120.0
            score += max(snapshot.open_pct, 0.0) * 60.0
            score += min(snapshot.lb_days, 6) * 1.5
            score += 2.2 if snapshot.is_yest_limit else 0.0
            score += leader_bonus
            score += 0.8 if snapshot.touched_limit_today else 0.0
            score += hot_signal
            if score <= 0.0:
                continue
            for plate_name in plate_names:
                plate_scores[plate_name] = plate_scores.get(plate_name, 0.0) + score
                if snapshot.leader_rank_in_theme == 1:
                    plate_leader_counts[plate_name] = plate_leader_counts.get(plate_name, 0) + 1
        if not plate_scores:
            return ""
        return min(
            plate_scores.items(),
            key=lambda item: (
                -item[1],
                -plate_leader_counts.get(item[0], 0),
                -self._match_hot_plate_signal((item[0],), hot_plate_map),
                item[0],
            ),
        )[0]

    def _runtime_plate_names(self, snapshot: StockStateSnapshot) -> tuple[str, ...]:
        names: list[str] = []
        for raw_name in snapshot.real_plate_names or (snapshot.plate,):
            for token in split_plate_tokens(raw_name):
                name = str(token or "").strip()
                if name and not is_generic_plate(name) and name not in names:
                    names.append(name)
        if not names and snapshot.plate:
            plate = str(snapshot.plate or "").strip()
            if plate and not is_generic_plate(plate):
                names.append(plate)
        return tuple(names)

    def _match_hot_plate_signal(
        self,
        plate_names: Iterable[str],
        hot_plate_map: dict[str, Any],
    ) -> float:
        if not hot_plate_map:
            return 0.0
        matched_signal = 0.0
        for plate_name in plate_names:
            if not plate_name:
                continue
            payload = hot_plate_map.get(plate_name)
            if payload is None:
                for hot_name, hot_payload in hot_plate_map.items():
                    if hot_name and (hot_name in plate_name or plate_name in hot_name):
                        payload = hot_payload
                        break
            if not payload:
                continue
            signal = self._hot_plate_signal_score(payload)
            if signal > matched_signal:
                matched_signal = signal
        return matched_signal

    def _hot_plate_metric(self, payload: Any) -> tuple[float, float, float, float]:
        return (
            self._hot_plate_strength_value(payload),
            self._hot_plate_field(payload, "change_pct"),
            self._hot_plate_field(payload, "net_inflow_yi"),
            self._hot_plate_field(payload, "hot"),
        )

    def _hot_plate_sort_key(self, item: tuple[str, Any]) -> tuple[float, float, float, float, int, str]:
        plate_name, payload = item
        strength, change_pct, net_inflow_yi, hot = self._hot_plate_metric(payload)
        rank = int(self._hot_plate_field(payload, "rank", default=999) or 999)
        return (-strength, -change_pct, -net_inflow_yi, -hot, rank, plate_name)

    def _hot_plate_strength_value(self, payload: Any) -> float:
        strength = self._hot_plate_field(payload, "strength")
        if strength > 0.0:
            return strength
        return self._hot_plate_field(payload, "hot")

    def _hot_plate_signal_score(self, payload: Any) -> float:
        strength = self._hot_plate_strength_value(payload)
        strength_signal = min(strength / 5000.0, 2.5) if strength > 0.0 else 0.0
        return round(strength_signal + self._hot_plate_capital_behavior_score(payload), 4)

    def _hot_plate_capital_behavior_score(self, payload: Any) -> float:
        change_pct = self._hot_plate_field(payload, "change_pct")
        net_inflow_yi = self._hot_plate_field(payload, "net_inflow_yi")
        flow_signal = min(abs(net_inflow_yi), 20.0) / 20.0
        price_signal = min(abs(change_pct), 8.0) / 8.0
        if net_inflow_yi > 0 and change_pct > 0:
            score = 1.2 + flow_signal * 1.0 + price_signal * 0.6
        elif net_inflow_yi < 0 and change_pct > 0:
            score = -0.4 - flow_signal * 1.1 + price_signal * 0.2
        elif net_inflow_yi > 0 and change_pct <= 0:
            score = 0.35 + flow_signal * 0.85 - price_signal * 0.15
        elif net_inflow_yi < 0 and change_pct <= 0:
            score = -0.8 - flow_signal * 0.9 - price_signal * 0.3
        else:
            score = max(change_pct, 0.0) * 0.08
        return round(score, 4)

    def _plate_resonance(self, plate: str, hot_plate_map: dict[str, dict]) -> float:
        if not plate or not hot_plate_map:
            return 1.0
        matched = hot_plate_map.get(plate)
        if matched is None:
            for name, payload in hot_plate_map.items():
                if name and (name in plate or plate in name):
                    matched = payload
                    break
        if not matched:
            return 1.0
        rank = int(matched.get("rank", 999) or 999)
        hot = float(matched.get("hot", 0.0) or 0.0)
        rank_bonus = 1.2 if rank <= 3 else (1.1 if rank <= 10 else 1.0)
        hot_bonus = 0.2 if hot >= 95 else (0.1 if hot >= 80 else 0.0)
        return round(rank_bonus + hot_bonus, 2)

    def _judge_battle_status(self, headshot_rate: float, promotion_rate: float, total: int) -> str:
        if total <= 0:
            return "no_yest_limit_context"
        if headshot_rate > 0.15:
            return "danger"
        if promotion_rate > 0.4:
            return "bullish"
        if headshot_rate < 0.05 and promotion_rate < 0.15:
            return "frozen"
        return "neutral"

    def _to_yi(self, value: Any) -> float:
        try:
            number = float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if number <= 0:
            return 0.0
        return round(number / 100_000_000, 2)

    def _to_optional_int(self, value: Any) -> int | None:
        if value in (None, "", 0, "0"):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _to_float(self, value: Any) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _classify_plate_migration(self, migration: Any) -> str:
        if migration.present_today and not migration.present_yesterday:
            return "EMERGING"
        if migration.present_yesterday and not migration.present_today:
            return "FADING"

        up_votes = int(migration.strength_delta > 0) + int(migration.change_pct_delta > 0) + int(migration.net_inflow_yi_delta > 0)
        down_votes = int(migration.strength_delta < 0) + int(migration.change_pct_delta < 0) + int(migration.net_inflow_yi_delta < 0)

        if down_votes >= 2:
            return "FADING"
        if up_votes >= 2:
            return "PERSIST"
        if migration.strength_delta < 0 and (migration.change_pct_delta < 0 or migration.net_inflow_yi_delta < 0):
            return "FADING"
        if migration.strength_delta > 0 and (migration.change_pct_delta > 0 or migration.net_inflow_yi_delta > 0):
            return "PERSIST"
        return "PERSIST" if migration.today_strength >= migration.yesterday_strength else "FADING"

    def _hot_plate_field(self, payload: Any, field: str, *, default: float = 0.0) -> float:
        value = default
        if isinstance(payload, dict):
            value = payload.get(field, default)
        else:
            value = getattr(payload, field, default)
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return float(default or 0.0)

    def _hot_plate_signature(self, hot_plate_map: dict[str, dict], *, trade_date: str) -> str:
        compact_rows = [
            {
                "plate": str(plate_name or "").strip(),
                "rank": int(payload.get("rank", 999) or 999),
                "strength": round(float(payload.get("strength", payload.get("hot", 0.0)) or 0.0), 2),
                "change_pct": round(float(payload.get("change_pct", 0.0) or 0.0), 3),
                "net_inflow_yi": round(float(payload.get("net_inflow_yi", 0.0) or 0.0), 2),
            }
            for plate_name, payload in hot_plate_map.items()
            if isinstance(payload, dict)
        ]
        compact_rows.sort(key=lambda item: (-item["strength"], -item["change_pct"], -item["net_inflow_yi"], item["rank"], item["plate"]))
        return self._stable_signature(
            {
                "trade_date": trade_date,
                "row_count": len(compact_rows),
                "top_rows": compact_rows[:12],
            }
        )

    def _yest_limit_signature(self, *, primed_trade_date: str, yest_limit_map: dict[str, dict]) -> str:
        compact_rows = [
            {
                "symbol": symbol,
                "lb_days": int(payload.get("lb_days", 0) or 0),
                "plate": str(payload.get("plate") or "").strip(),
            }
            for symbol, payload in sorted(yest_limit_map.items())
            if isinstance(payload, dict)
        ]
        return self._stable_signature(
            {
                "trade_date": primed_trade_date,
                "row_count": len(compact_rows),
                "rows": compact_rows,
            }
        )

    def _auction_signature(self, auction_rows: Iterable[dict[str, Any]]) -> str:
        rows = [row for row in auction_rows if isinstance(row, dict)]
        top_rows = nlargest(
            12,
            rows,
            key=lambda item: float(item.get("amount", 0.0) or 0.0),
        )
        source_counts: dict[str, int] = {}
        compact_top_rows: list[dict[str, Any]] = []
        for row in rows:
            source = str(row.get("source") or "").strip() or "-"
            source_counts[source] = source_counts.get(source, 0) + 1
        for row in top_rows:
            compact_top_rows.append(
                {
                    "symbol": _normalize_symbol(row.get("symbol")),
                    "amount": round(float(row.get("amount", 0.0) or 0.0), 2),
                    "change_pct": round(float(row.get("change_pct", 0.0) or 0.0), 3),
                    "source": str(row.get("source") or "").strip() or "-",
                }
            )
        return self._stable_signature(
            {
                "row_count": len(rows),
                "source_counts": source_counts,
                "top_rows": compact_top_rows,
            }
        )

    def _stable_signature(self, payload: Any) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(encoded.encode("utf-8")).hexdigest()
