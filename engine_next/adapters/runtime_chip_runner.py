from __future__ import annotations

import logging
from threading import Lock
from typing import Any, Optional

import numpy as np
import pandas as pd
try:
    import talib
except Exception:  # pragma: no cover - optional dependency
    talib = None


logger = logging.getLogger(__name__)


def _clamp(value: float, minimum: float = 0.0, maximum: float = 10.0) -> float:
    return max(minimum, min(maximum, value))


class RuntimeChipRunner:
    """Engine-next private chip/factor calculator without batch-runner side effects."""

    def __init__(self, f10_service: Optional[Any] = None) -> None:
        self._f10_service = f10_service
        self._f10_market_cap: Optional[dict[str, float]] = None
        self._f10_lock = Lock()

    @property
    def f10_service(self) -> Any:
        if self._f10_service is None:
            from web.services.f10_service import F10DataService

            self._f10_service = F10DataService()
        return self._f10_service

    @property
    def f10_market_cap(self) -> dict[str, float]:
        if self._f10_market_cap is None:
            with self._f10_lock:
                if self._f10_market_cap is None:
                    self._f10_market_cap = {}
        return self._f10_market_cap

    def market_cap_for(self, symbol: str) -> float:
        code6 = str(symbol or "").split(".")[-1][-6:]
        if not code6:
            return 0.0
        cache = self.f10_market_cap
        cached = cache.get(code6)
        if cached is not None:
            return cached
        try:
            f10 = self.f10_service.get_stock_f10(code6)
            cap = 0.0
            if f10 and "financial" in f10:
                raw_cap = f10["financial"].get("circulating_market_cap")
                if raw_cap:
                    cap = round(float(raw_cap) / 100000000, 2)
            cache[code6] = cap
            return cap
        except Exception as exc:
            logger.debug("runtime chip runner market cap load failed | symbol=%s | error=%s", code6, exc)
            cache[code6] = 0.0
            return 0.0

    def stock_name_for(self, symbol: str) -> str:
        code6 = str(symbol or "").split(".")[-1][-6:]
        if not code6:
            return ""
        try:
            return str(self.f10_service.get_stock_name(code6) or "").strip()
        except Exception:
            return ""

    def limit_up_threshold_pct(self, symbol: str) -> float:
        code6 = str(symbol or "").split(".")[-1][-6:]
        name = self.stock_name_for(code6).upper()
        if "ST" in name:
            return 4.8
        if code6.startswith(("300", "301", "688", "689")):
            return 19.8
        if code6.startswith(("8", "92")):
            return 29.8
        return 9.8

    def calculate_chip_peak(self, dataframe: Any) -> dict[str, Any]:
        try:
            df = pd.DataFrame(dataframe).copy()
            if df.empty or len(df) < 5:
                return {}

            if "turn" not in df.columns:
                df["turn"] = 0.0

            data = df.tail(120).copy()
            prices = data["close"].astype(float).values
            turns = data["turn"].fillna(0).astype(float).values / 100.0

            p_min = prices.min()
            p_max = prices.max()
            if p_max == p_min:
                return {"peak_price": float(p_min), "concentration": 0, "dense_area_count": 1}

            bins = 50
            bin_edges = np.linspace(p_min, p_max, bins + 1)
            bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
            current_chips = np.zeros(bins)
            for idx, price in enumerate(prices):
                turn = min(turns[idx], 1.0)
                current_chips *= 1.0 - turn
                price_idx = min(int((price - p_min) / (p_max - p_min) * bins), bins - 1)
                current_chips[price_idx] += turn

            peak_idx = int(np.argmax(current_chips))
            peak_price = round((bin_edges[peak_idx] + bin_edges[peak_idx + 1]) / 2, 2)
            current_price = float(prices[-1])
            total = float(current_chips.sum())
            if total > 0:
                sorted_chips = np.sort(current_chips)[::-1]
                cum_chips = np.cumsum(sorted_chips)
                cutoff_idx = int(np.searchsorted(cum_chips, total * 0.7))
                concentration = round((cutoff_idx + 1) / bins, 4)
                avg_cost = round(float(np.dot(bin_centers, current_chips) / total), 2)
                profit_ratio = round(float(current_chips[bin_centers <= current_price].sum() / total), 4)
                loss_ratio = round(max(0.0, 1.0 - profit_ratio), 4)
                chip_percent = round(float(current_chips[peak_idx] / total * 100.0), 2)
                cum_by_price = np.cumsum(current_chips)
                lower_idx = int(np.searchsorted(cum_by_price, total * 0.15))
                upper_idx = int(np.searchsorted(cum_by_price, total * 0.85))
                lower_cost = round(float(bin_centers[min(lower_idx, bins - 1)]), 2)
                upper_cost = round(float(bin_centers[min(upper_idx, bins - 1)]), 2)
            else:
                concentration = 1.0
                avg_cost = peak_price
                profit_ratio = 0.0
                loss_ratio = 0.0
                chip_percent = 0.0
                lower_cost = peak_price
                upper_cost = peak_price

            return {
                "peak_price": peak_price,
                "peak_weight": chip_percent,
                "chip_percent": chip_percent,
                "avg_cost": avg_cost,
                "profit_ratio": profit_ratio,
                "loss_ratio": loss_ratio,
                "upper_cost": upper_cost,
                "lower_cost": lower_cost,
                "concentration": concentration,
                "dense_area_count": int(np.sum(current_chips > total * 0.05)),
            }
        except Exception as exc:
            logger.error("runtime chip peak calculation failed | error=%s", exc)
            return {}

    def calculate_extra_factors(self, symbol: str, dataframe: Any) -> dict[str, Any]:
        try:
            df = pd.DataFrame(dataframe).copy()
            if df.empty or len(df) < 35:
                return {}

            if "turn" not in df.columns:
                df["turn"] = 0.0
            if "pct_chg" not in df.columns:
                df["pct_chg"] = 0.0

            last_idx = len(df) - 1
            curr_close = float(df.iloc[last_idx]["close"])
            idx_5d = max(0, last_idx - 5)
            close_5d = float(df.iloc[idx_5d]["close"])
            change_5d = round((curr_close - close_5d) / close_5d * 100, 2) if close_5d else 0.0
            avg_turn = round(float(df.tail(5)["turn"].astype(float).mean()), 2)
            limit_threshold = self.limit_up_threshold_pct(symbol)
            limit_ups = int((df.tail(5)["pct_chg"].astype(float) > limit_threshold).sum())

            rsi_6 = bias_20 = 0.0
            ma5 = ma10 = ma20 = 0.0
            macd_dif = macd_dea = macd_hist = 0.0
            kdj_k = kdj_d = kdj_j = 0.0
            boll_up = boll_mid = boll_low = 0.0

            t2_lb_days = 0
            t2_pct = 0.0
            if len(df) >= 3:
                t2_row = df.iloc[len(df) - 3]
                t2_pct = round(float(t2_row.get("pct_chg", 0) or 0), 2)
                t2_history = df.iloc[: len(df) - 2]
                for idx in range(len(t2_history) - 1, -1, -1):
                    if float(t2_history.iloc[idx].get("pct_chg", 0) or 0) > 9.8:
                        t2_lb_days += 1
                    else:
                        break

            closes = df["close"].astype(float).values
            highs = df["high"].astype(float).values
            lows = df["low"].astype(float).values

            if talib and len(df) >= 30:
                ma5_arr = talib.SMA(closes, timeperiod=5)
                ma10_arr = talib.SMA(closes, timeperiod=10)
                ma20_arr = talib.SMA(closes, timeperiod=20)
                ma5 = round(float(ma5_arr[-1]), 2) if not np.isnan(ma5_arr[-1]) else 0.0
                ma10 = round(float(ma10_arr[-1]), 2) if not np.isnan(ma10_arr[-1]) else 0.0
                ma20 = round(float(ma20_arr[-1]), 2) if not np.isnan(ma20_arr[-1]) else 0.0

                rsi_6_arr = talib.RSI(closes, timeperiod=6)
                rsi_6 = round(float(rsi_6_arr[-1]), 2) if not np.isnan(rsi_6_arr[-1]) else 0.0
                bias_20 = round((closes[-1] - ma20) / ma20 * 100, 2) if ma20 else 0.0

                dif, dea, hist = talib.MACD(closes, fastperiod=12, slowperiod=26, signalperiod=9)
                macd_dif = round(float(dif[-1]), 3) if not np.isnan(dif[-1]) else 0.0
                macd_dea = round(float(dea[-1]), 3) if not np.isnan(dea[-1]) else 0.0
                macd_hist = round(float(hist[-1]) * 2, 3) if not np.isnan(hist[-1]) else 0.0

                upper, middle, lower = talib.BBANDS(
                    closes,
                    timeperiod=20,
                    nbdevup=2,
                    nbdevdn=2,
                    matype=0,
                )
                boll_up = round(float(upper[-1]), 2) if not np.isnan(upper[-1]) else 0.0
                boll_mid = round(float(middle[-1]), 2) if not np.isnan(middle[-1]) else 0.0
                boll_low = round(float(lower[-1]), 2) if not np.isnan(lower[-1]) else 0.0

                low_9 = talib.MIN(lows, timeperiod=9)
                high_9 = talib.MAX(highs, timeperiod=9)
                rsv = (closes - low_9) / (high_9 - low_9 + 0.001) * 100
                k_arr = pd.Series(rsv).ewm(com=2, adjust=False).mean().values
                d_arr = pd.Series(k_arr).ewm(com=2, adjust=False).mean().values
                j_arr = 3 * k_arr - 2 * d_arr
                kdj_k = round(float(k_arr[-1]), 2)
                kdj_d = round(float(d_arr[-1]), 2)
                kdj_j = round(float(j_arr[-1]), 2)

            turnover_base = float(df["turn"].iloc[-20:-5].astype(float).mean()) if len(df) >= 20 else 0.0
            chip_payload = self.calculate_chip_peak(df)
            profit_ratio = float(chip_payload.get("profit_ratio", 0.0) or 0.0)
            concentration = float(chip_payload.get("concentration", 0.0) or 0.0)
            shape_chip_cleanliness = _clamp(
                5.0
                + (1.6 if 0.05 <= profit_ratio <= 0.35 else -1.0 if profit_ratio > 0.75 else 0.0)
                + (1.4 if 0.05 <= concentration <= 0.22 else -1.0 if concentration > 0.38 else 0.0)
            )
            shape_t2_repair_bias = _clamp(
                5.0
                + (2.2 if t2_lb_days >= 1 and -8.0 <= t2_pct <= -2.0 else 0.0)
                + (0.8 if bias_20 < 0 else 0.0)
            )
            shape_platform_ready = _clamp(
                4.5
                + (1.8 if -3.0 <= bias_20 <= 8.0 else -1.0 if bias_20 > 15.0 else 0.0)
                + (1.2 if 45.0 <= rsi_6 <= 68.0 else -0.8 if rsi_6 >= 82.0 else 0.0)
                + (0.8 if limit_ups <= 1 else -0.6)
            )
            shape_breakout_ready = _clamp(
                4.2
                + (1.4 if -1.5 <= change_5d <= 12.0 else -1.0 if change_5d > 20.0 else 0.0)
                + (1.0 if avg_turn >= 1.0 else -0.5)
                + (0.8 if 48.0 <= rsi_6 <= 72.0 else -0.8 if rsi_6 >= 85.0 else 0.0)
            )
            shape_overheat_risk = _clamp(
                2.5
                + (2.4 if bias_20 > 12.0 else 0.0)
                + (1.8 if profit_ratio > 0.75 else 0.0)
                + (1.6 if rsi_6 >= 82.0 else 0.0)
                + (1.0 if change_5d > 18.0 else 0.0)
            )
            shape_trend_health = _clamp(
                4.5
                + (1.5 if bias_20 > -4.0 else -1.0)
                + (1.2 if rsi_6 >= 45.0 else -1.0)
                + (1.0 if avg_turn >= 1.0 else 0.0)
                + (0.8 if limit_ups <= 2 else -0.6)
            )
            structure_score_base = _clamp(
                0.24 * shape_chip_cleanliness
                + 0.18 * shape_platform_ready
                + 0.18 * shape_breakout_ready
                + 0.18 * shape_trend_health
                + 0.14 * shape_t2_repair_bias
                + 0.08 * (10.0 - shape_overheat_risk)
            )
            theme_core_base = _clamp(
                3.5
                + (1.2 if limit_ups >= 1 else 0.0)
                + (1.0 if avg_turn >= 1.5 else 0.0)
                + (0.8 if change_5d > 0 else 0.0)
                + (0.8 if 8.0 <= self.market_cap_for(symbol) <= 800.0 else 0.0)
                + (0.7 if bias_20 > -2.0 else 0.0)
            )
            return {
                "change_pct_5d": change_5d,
                "avg_turnover_5d": avg_turn,
                "limit_up_days_5": limit_ups,
                "real_market_cap": self.market_cap_for(symbol),
                "avg_cost": float(chip_payload.get("avg_cost", 0.0) or 0.0),
                "rsi_6": rsi_6,
                "bias_20": bias_20,
                "profit_ratio": profit_ratio,
                "vol_ratio": round(avg_turn / (turnover_base + 0.1), 2),
                "concentration": concentration,
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "macd_dif": macd_dif,
                "macd_dea": macd_dea,
                "macd_hist": macd_hist,
                "kdj_k": kdj_k,
                "kdj_d": kdj_d,
                "kdj_j": kdj_j,
                "boll_up": boll_up,
                "boll_mid": boll_mid,
                "boll_low": boll_low,
                "t2_lb_days": t2_lb_days,
                "t2_pct": t2_pct,
                "structure_score_base": round(structure_score_base, 2),
                "shape_platform_ready": round(shape_platform_ready, 2),
                "shape_breakout_ready": round(shape_breakout_ready, 2),
                "shape_repair_ready": round(max(shape_t2_repair_bias, shape_trend_health * 0.8), 2),
                "shape_overheat_risk": round(shape_overheat_risk, 2),
                "shape_chip_cleanliness": round(shape_chip_cleanliness, 2),
                "shape_trend_health": round(shape_trend_health, 2),
                "shape_t2_repair_bias": round(shape_t2_repair_bias, 2),
                "theme_core_base": round(theme_core_base, 2),
            }
        except Exception as exc:
            logger.error("runtime factor calculation failed | symbol=%s | error=%s", symbol, exc)
            return {}


_shared_runtime_chip_runner: Optional[RuntimeChipRunner] = None
_shared_runtime_chip_runner_lock = Lock()


def get_shared_runtime_chip_runner() -> RuntimeChipRunner:
    global _shared_runtime_chip_runner
    if _shared_runtime_chip_runner is None:
        with _shared_runtime_chip_runner_lock:
            if _shared_runtime_chip_runner is None:
                _shared_runtime_chip_runner = RuntimeChipRunner()
    return _shared_runtime_chip_runner
