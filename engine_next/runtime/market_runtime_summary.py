from __future__ import annotations

import json
from dataclasses import dataclass
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

    @property
    def redis(self) -> Any:
        return self._redis

    def build_and_write(self, trade_date: str, *, offline_context_date: str | None = None) -> MarketRuntimeSummaryResult:
        summary = self.build_summary(trade_date, offline_context_date=offline_context_date)
        keys = (
            f"market:runtime:summary:{trade_date}",
            "market:runtime:summary:latest",
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

    def build_summary(self, trade_date: str, *, offline_context_date: str | None = None) -> dict[str, Any]:
        factor_date = offline_context_date or trade_date
        avg_5d_vol = self._sum_avg_turnover_5d(f"cache:stock_extra:{factor_date}")
        auction_sample = self._read_auction_sample(trade_date)
        hot_plate_summary = self._read_hot_plate_summary(trade_date, factor_date)
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
            "top_plate_name": hot_plate_summary.get("top_plate_name", ""),
            "hot_plate_count": hot_plate_summary.get("hot_plate_count", 0),
            "source": "redis_runtime_projection",
        }

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
