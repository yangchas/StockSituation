from __future__ import annotations

import logging
from threading import Lock
from typing import Any, Optional

import numpy as np
import pandas as pd
import talib


logger = logging.getLogger(__name__)


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
            current_chips = np.zeros(bins)
            for idx, price in enumerate(prices):
                turn = min(turns[idx], 1.0)
                current_chips *= 1.0 - turn
                price_idx = min(int((price - p_min) / (p_max - p_min) * bins), bins - 1)
                current_chips[price_idx] += turn

            peak_idx = int(np.argmax(current_chips))
            peak_price = round((bin_edges[peak_idx] + bin_edges[peak_idx + 1]) / 2, 2)
            total = float(current_chips.sum())
            if total > 0:
                sorted_chips = np.sort(current_chips)[::-1]
                cum_chips = np.cumsum(sorted_chips)
                cutoff_idx = int(np.searchsorted(cum_chips, total * 0.7))
                concentration = round((cutoff_idx + 1) / bins, 4)
            else:
                concentration = 1.0

            return {
                "peak_price": peak_price,
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
            limit_ups = int((df.tail(5)["pct_chg"].astype(float) > 9.8).sum())

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
            return {
                "change_pct_5d": change_5d,
                "avg_turnover_5d": avg_turn,
                "limit_up_days_5": limit_ups,
                "real_market_cap": self.market_cap_for(symbol),
                "rsi_6": rsi_6,
                "bias_20": bias_20,
                "vol_ratio": round(avg_turn / (turnover_base + 0.1), 2),
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
