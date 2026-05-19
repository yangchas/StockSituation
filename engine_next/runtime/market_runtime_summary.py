from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class MarketRuntimeSummaryResult:
    trade_date: str
    summary: dict[str, Any]
    redis_keys_written: tuple[str, ...]
    notes: tuple[str, ...] = ()


class MarketRuntimeSummaryService:
    """
    Produces market-wide runtime summary caches for intraday consumers.

    Unlike watchlist context metrics, these fields are built from the full
    Redis cache buckets prepared before market consumption:
    - cache:stock_extra:{date}
    - market:auction:{date}:0925
    """

    def __init__(self, *, redis_client: Any) -> None:
        self._redis = redis_client
        self._redis_q2_prefix = os.getenv("REDIS_Q2_PREFIX", "q2:")
        self._allow_open_2m_full_scan_fallback = str(
            os.getenv("MARKET_OPEN2M_ALLOW_FULL_SCAN_FALLBACK", "")
        ).strip().lower() in {"1", "true", "yes", "on"}

    @property
    def redis(self) -> Any:
        return self._redis

    def _summary_key(self, trade_date: str) -> str:
        return f"market:runtime:summary:{trade_date}"

    def _latest_key(self) -> str:
        return "market:runtime:summary:latest"

    def _open_2m_summary_key(self, trade_date: str) -> str:
        return f"market:open2m:summary:{trade_date}"

    def load_cached(
        self,
        trade_date: str,
        *,
        offline_context_date: str | None = None,
        max_age_seconds: int | None = None,
    ) -> MarketRuntimeSummaryResult | None:
        try:
            raw = self.redis.get(self._summary_key(trade_date))
        except Exception:
            return None
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        if str(payload.get("trade_date") or "") != trade_date:
            return None
        expected_context_date = str(offline_context_date or trade_date)
        cached_context_date = str(payload.get("offline_context_date") or "")
        if cached_context_date and cached_context_date != expected_context_date:
            return None
        if max_age_seconds is not None:
            try:
                updated_at_ts = int(payload.get("updated_at_ts", 0) or 0)
            except (TypeError, ValueError):
                updated_at_ts = 0
            if updated_at_ts <= 0:
                return None
            age_seconds = max(int(datetime.now().timestamp()) - updated_at_ts, 0)
            if age_seconds > max_age_seconds:
                return None
        return MarketRuntimeSummaryResult(
            trade_date=trade_date,
            summary=payload,
            redis_keys_written=(),
            notes=("market runtime summary cache hit",),
        )

    def get_or_build(
        self,
        trade_date: str,
        *,
        offline_context_date: str | None = None,
        max_age_seconds: int | None = None,
        force_rebuild: bool = False,
    ) -> MarketRuntimeSummaryResult:
        if not force_rebuild:
            cached = self.load_cached(
                trade_date,
                offline_context_date=offline_context_date,
                max_age_seconds=max_age_seconds,
            )
            if cached is not None:
                return cached
        return self.build_and_write(trade_date, offline_context_date=offline_context_date)

    def build_and_write(self, trade_date: str, *, offline_context_date: str | None = None) -> MarketRuntimeSummaryResult:
        summary = self.build_summary(trade_date, offline_context_date=offline_context_date)
        now = datetime.now()
        summary.update(
            {
                "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at_ts": int(now.timestamp()),
            }
        )
        keys = (
            self._summary_key(trade_date),
            self._latest_key(),
        )
        payload = json.dumps(summary, ensure_ascii=False)
        self.redis.set(keys[0], payload)
        self.redis.set(keys[1], payload)
        return MarketRuntimeSummaryResult(
            trade_date=trade_date,
            summary=summary,
            redis_keys_written=keys,
            notes=(
                "Market runtime summary is built from full Redis buckets, not watchlist subset inference.",
                "Auction totals are projected from sampled 09:25 auction rows using coverage factor.",
            ),
        )

    def refresh_open_2m_runtime_summary(
        self,
        trade_date: str,
        *,
        offline_context_date: str | None = None,
        create_if_missing: bool = False,
    ) -> MarketRuntimeSummaryResult | None:
        stats = self._read_open_2m_top_amount_stats(trade_date)
        if int(stats.get("top10_count", 0) or 0) <= 0 and int(stats.get("top20_count", 0) or 0) <= 0:
            return None
        summary_payload = self._load_runtime_summary_payload(trade_date)
        if not summary_payload:
            if not create_if_missing:
                return None
            return self.build_and_write(trade_date, offline_context_date=offline_context_date)
        now = datetime.now()
        merged_summary = dict(summary_payload)
        merged_summary.update(self._open_2m_summary_fields(stats))
        merged_summary["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        merged_summary["updated_at_ts"] = int(now.timestamp())
        payload = json.dumps(merged_summary, ensure_ascii=False)
        summary_key = self._summary_key(trade_date)
        latest_key = self._latest_key()
        self.redis.set(summary_key, payload)
        written_keys = [summary_key]
        latest_payload = self._load_runtime_summary_payload(trade_date, prefer_latest=True)
        if latest_payload and str(latest_payload.get("trade_date") or "") == trade_date:
            self.redis.set(latest_key, payload)
            written_keys.append(latest_key)
        return MarketRuntimeSummaryResult(
            trade_date=trade_date,
            summary=merged_summary,
            redis_keys_written=tuple(written_keys),
            notes=(
                "opening two-minute market slice merged into runtime summary cache",
            ),
        )

    def build_summary(self, trade_date: str, *, offline_context_date: str | None = None) -> dict[str, Any]:
        factor_date = offline_context_date or trade_date
        avg_5d_vol = self._sum_avg_turnover_5d(f"cache:stock_extra:{factor_date}")
        auction_sample = self._read_auction_sample(trade_date)
        hot_plate_summary = self._read_hot_plate_summary(trade_date, factor_date)
        top_amount_stats = self._read_top_amount_stats(auction_sample)
        open_2m_top_amount_stats = self._read_open_2m_top_amount_stats(trade_date)
        sample_auc_amt = sum(float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0) for row in auction_sample)
        sample_size = len(auction_sample)
        coverage_factor = self._infer_coverage_factor(sample_size)
        full_market_auc_amt = sample_auc_amt / coverage_factor if coverage_factor > 0 else sample_auc_amt
        predicted_full_day_amount = full_market_auc_amt / 0.045 if full_market_auc_amt > 0 else 0.0
        volume_level = self._infer_volume_level(predicted_full_day_amount, avg_5d_vol)
        return {
            "trade_date": trade_date,
            "offline_context_date": factor_date,
            "sample_auc_amt": round(sample_auc_amt, 2),
            "auction_sample_size": sample_size,
            "auction_coverage_factor": round(coverage_factor, 4),
            "full_market_auc_amt": round(full_market_auc_amt, 2),
            "avg_5d_vol": round(avg_5d_vol, 2),
            "predicted_full_day_amount": round(predicted_full_day_amount, 2),
            "volume_level": volume_level,
            "auction_top10_amount": round(top_amount_stats["top10_amount"], 2),
            "auction_top20_amount": round(top_amount_stats["top20_amount"], 2),
            "auction_top10_count": top_amount_stats["top10_count"],
            "auction_top20_count": top_amount_stats["top20_count"],
            **self._open_2m_summary_fields(open_2m_top_amount_stats),
            "top_plate_name": hot_plate_summary.get("top_plate_name", ""),
            "hot_plate_count": hot_plate_summary.get("hot_plate_count", 0),
            "source": "redis_runtime_projection",
        }

    def _open_2m_summary_fields(self, stats: dict[str, Any]) -> dict[str, Any]:
        return {
            "open_2m_top10_amount": round(float(stats.get("top10_amount", 0.0) or 0.0), 2),
            "open_2m_top20_amount": round(float(stats.get("top20_amount", 0.0) or 0.0), 2),
            "open_2m_top10_count": int(stats.get("top10_count", 0) or 0),
            "open_2m_top20_count": int(stats.get("top20_count", 0) or 0),
            "open_2m_source": str(stats.get("source") or ""),
            "open_2m_updated_at_ts": int(stats.get("updated_at_ts", 0) or 0),
        }

    def _load_runtime_summary_payload(self, trade_date: str, *, prefer_latest: bool = False) -> dict[str, Any]:
        keys = [self._latest_key(), self._summary_key(trade_date)] if prefer_latest else [self._summary_key(trade_date), self._latest_key()]
        for key in keys:
            try:
                raw = self.redis.get(key)
            except Exception:
                raw = None
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if str(payload.get("trade_date") or "") != trade_date:
                continue
            return payload
        return {}

    def _sum_avg_turnover_5d(self, redis_key: str) -> float:
        bucket = self.redis.hgetall(redis_key) or {}
        total = 0.0
        for raw in bucket.values():
            try:
                payload = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            try:
                avg_value = float(payload.get("avg_turnover_5d", 0.0) or 0.0)
            except (TypeError, ValueError):
                avg_value = 0.0
            if avg_value > 0:
                total += avg_value
        return total

    def _read_auction_sample(self, trade_date: str) -> list[dict[str, Any]]:
        raw = self.redis.hget(f"market:auction:{trade_date.replace('-', '')}:0925", "top_amount")
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except Exception:
            return []
        return [row for row in payload if isinstance(row, dict)]

    def _read_hot_plate_summary(self, trade_date: str, fallback_trade_date: str) -> dict[str, Any]:
        for key in (f"cache:hot_plates:{trade_date}", f"cache:hot_plates:{fallback_trade_date}"):
            bucket = self.redis.hgetall(key) or {}
            rows: list[dict[str, Any]] = []
            for raw in bucket.values():
                try:
                    payload = json.loads(raw) if isinstance(raw, str) else raw
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
            if not rows:
                continue
            top = max(
                rows,
                key=lambda item: (
                    self._hot_plate_strength_value(item),
                    float(item.get("change_pct", 0.0) or 0.0),
                    float(item.get("net_inflow_yi", 0.0) or 0.0),
                    float(item.get("hot", 0.0) or 0.0),
                    -float(item.get("rank", 999) or 999),
                ),
            )
            return {
                "top_plate_name": str(top.get("plate_name") or ""),
                "hot_plate_count": len(rows),
            }
        return {
            "top_plate_name": "",
            "hot_plate_count": 0,
        }

    def _read_top_amount_stats(self, auction_rows: list[dict[str, Any]]) -> dict[str, Any]:
        ranked_amounts = sorted(
            (
                float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0)
                for row in auction_rows
                if isinstance(row, dict)
            ),
            reverse=True,
        )
        top10 = ranked_amounts[:10]
        top20 = ranked_amounts[:20]
        return {
            "top10_amount": sum(top10),
            "top20_amount": sum(top20),
            "top10_count": len(top10),
            "top20_count": len(top20),
        }

    def _read_open_2m_top_amount_stats(self, trade_date: str) -> dict[str, Any]:
        trade_date = str(trade_date or "").strip()
        if not trade_date:
            return {
                "top10_amount": 0.0,
                "top20_amount": 0.0,
                "top10_count": 0,
                "top20_count": 0,
            }
        cached = self._read_cached_open_2m_top_amount_stats(trade_date)
        if cached is not None:
            return cached
        rows = self._read_open_2m_q2_rows(trade_date)
        ranked_amounts: list[float] = []
        for values in rows:
            if not isinstance(values, list) or len(values) < 3:
                continue
            try:
                amount_2m = float(values[0] or 0.0)
            except (TypeError, ValueError):
                amount_2m = 0.0
            try:
                timestamp_ms = int(float(values[1] or 0))
            except (TypeError, ValueError):
                timestamp_ms = 0
            try:
                phase = int(float(values[2] or 0))
            except (TypeError, ValueError):
                phase = 0
            if amount_2m <= 0.0 or timestamp_ms <= 0 or phase == 1:
                continue
            try:
                quote_trade_date = datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")
            except Exception:
                continue
            if quote_trade_date != trade_date:
                continue
            ranked_amounts.append(amount_2m)
        ranked_amounts.sort(reverse=True)
        top10 = ranked_amounts[:10]
        top20 = ranked_amounts[:20]
        stats = {
            "top10_amount": sum(top10),
            "top20_amount": sum(top20),
            "top10_count": len(top10),
            "top20_count": len(top20),
            "source": "redis_q2_projection",
            "updated_at_ts": int(datetime.now().timestamp()),
        }
        self._write_cached_open_2m_top_amount_stats(trade_date, stats)
        return stats

    def _read_cached_open_2m_top_amount_stats(self, trade_date: str) -> dict[str, Any] | None:
        try:
            raw = self.redis.get(self._open_2m_summary_key(trade_date))
        except Exception:
            return None
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        if str(payload.get("trade_date") or "") != trade_date:
            return None
        try:
            top10_amount = float(payload.get("top10_amount", 0.0) or 0.0)
            top20_amount = float(payload.get("top20_amount", 0.0) or 0.0)
            top10_count = int(payload.get("top10_count", 0) or 0)
            top20_count = int(payload.get("top20_count", 0) or 0)
        except (TypeError, ValueError):
            return None
        if top10_count <= 0 and top20_count <= 0:
            return None
        return {
            "top10_amount": top10_amount,
            "top20_amount": top20_amount,
            "top10_count": top10_count,
            "top20_count": top20_count,
            "source": str(payload.get("source") or "redis_open2m_summary"),
            "updated_at_ts": int(payload.get("updated_at_ts", 0) or 0),
        }

    def _write_cached_open_2m_top_amount_stats(self, trade_date: str, stats: dict[str, Any]) -> None:
        top10_count = int(stats.get("top10_count", 0) or 0)
        top20_count = int(stats.get("top20_count", 0) or 0)
        if top10_count <= 0 and top20_count <= 0:
            return
        payload = {
            "trade_date": trade_date,
            "top10_amount": round(float(stats.get("top10_amount", 0.0) or 0.0), 2),
            "top20_amount": round(float(stats.get("top20_amount", 0.0) or 0.0), 2),
            "top10_count": top10_count,
            "top20_count": top20_count,
            "source": str(stats.get("source") or "python_runtime_refresh"),
            "updated_at_ts": int(stats.get("updated_at_ts", 0) or int(datetime.now().timestamp())),
        }
        try:
            self.redis.set(self._open_2m_summary_key(trade_date), json.dumps(payload, ensure_ascii=False))
        except Exception:
            return

    def _read_open_2m_q2_rows(self, trade_date: str) -> list[list[Any]]:
        date_tag = trade_date.replace("-", "")
        active_key = f"q2:active:{date_tag}"
        symbols = self._read_q2_active_symbols(active_key)
        if symbols:
            return self._read_q2_rows_for_symbols(symbols)
        if not self._allow_open_2m_full_scan_fallback:
            return []
        # Compatibility fallback for environments that have not yet populated q2:active.
        prefix = str(self._redis_q2_prefix or "q2:").strip() or "q2:"
        keys = self._read_q2_keys_by_prefix(prefix)
        if not keys:
            return []
        return self._read_q2_rows_for_keys(keys)

    def _read_q2_active_symbols(self, redis_key: str) -> tuple[str, ...]:
        try:
            if hasattr(self.redis, "smembers"):
                values = self.redis.smembers(redis_key) or []
                return tuple(str(value) for value in values if str(value))
        except Exception:
            return ()
        return ()

    def _read_q2_keys_by_prefix(self, prefix: str) -> tuple[str, ...]:
        match_pattern = f"{prefix}*"
        scan_iter = getattr(self.redis, "scan_iter", None)
        if callable(scan_iter):
            try:
                return tuple(str(key) for key in scan_iter(match=match_pattern) if str(key))
            except Exception:
                return ()
        key_reader = getattr(self.redis, "keys", None)
        if callable(key_reader):
            try:
                return tuple(str(key) for key in key_reader(match_pattern) if str(key))
            except Exception:
                return ()
        return ()

    def _read_q2_rows_for_symbols(self, symbols: tuple[str, ...]) -> list[list[Any]]:
        prefix = str(self._redis_q2_prefix or "q2:").strip() or "q2:"
        keys = tuple(f"{prefix}{symbol}" for symbol in symbols if str(symbol))
        return self._read_q2_rows_for_keys(keys)

    def _read_q2_rows_for_keys(self, keys: tuple[str, ...] | list[str]) -> list[list[Any]]:
        normalized_keys = tuple(str(key) for key in keys if str(key))
        if not normalized_keys:
            return []
        rows: list[list[Any]] = []
        pipeline_factory = getattr(self.redis, "pipeline", None)
        if callable(pipeline_factory):
            try:
                pipe = pipeline_factory()
                for key in normalized_keys:
                    pipe.hmget(key, ["amt2m", "ts", "ph"])
                executed = pipe.execute()
                if isinstance(executed, list):
                    rows = [item if isinstance(item, list) else [] for item in executed]
            except Exception:
                rows = []
        if rows:
            return rows
        hmget = getattr(self.redis, "hmget", None)
        for key in normalized_keys:
            values: list[Any] | None = None
            if callable(hmget):
                try:
                    raw_values = hmget(key, ["amt2m", "ts", "ph"])
                    if isinstance(raw_values, list):
                        values = raw_values
                except Exception:
                    values = None
            if values is None:
                values = [
                    self.redis.hget(key, "amt2m"),
                    self.redis.hget(key, "ts"),
                    self.redis.hget(key, "ph"),
                ]
            rows.append(values)
        return rows

    def _hot_plate_strength_value(self, payload: dict[str, Any]) -> float:
        strength = float(payload.get("strength", 0.0) or 0.0)
        if strength > 0.0:
            return strength
        return float(payload.get("hot", 0.0) or 0.0)

    def _hot_plate_capital_behavior_score(self, payload: dict[str, Any]) -> float:
        change_pct = float(payload.get("change_pct", 0.0) or 0.0)
        net_inflow_yi = float(payload.get("net_inflow_yi", 0.0) or 0.0)
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

    def _infer_coverage_factor(self, sample_size: int) -> float:
        if sample_size >= 1000:
            return 0.65
        if sample_size >= 800:
            return 0.6
        if sample_size >= 300:
            return 0.45
        if sample_size >= 50:
            return 0.35
        if sample_size > 0:
            return 0.2
        return 1.0

    def _infer_volume_level(self, predicted_full_day_amount: float, avg_5d_vol: float) -> str:
        if predicted_full_day_amount <= 0 or avg_5d_vol <= 0:
            return ""
        if predicted_full_day_amount > avg_5d_vol * 1.1:
            return "high"
        if predicted_full_day_amount < avg_5d_vol * 0.9:
            return "low"
        return "flat"
