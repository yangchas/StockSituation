import unittest

import pandas as pd

from engine_next.adapters.runtime_chip_runner import RuntimeChipRunner


class _FakeF10Service:
    def __init__(self, names=None):
        self._names = names or {}

    def get_stock_f10(self, stock_code: str):
        return {"financial": {"circulating_market_cap": 1_000_000_000}}

    def get_stock_name(self, stock_code: str) -> str:
        return self._names.get(stock_code, "")


def _build_df(pct_values):
    rows = []
    base_close = 10.0
    for idx, pct in enumerate(pct_values):
        close = round(base_close * (1 + pct / 100.0), 2)
        rows.append(
            {
                "date": f"2026-04-{idx + 1:02d}",
                "open": close,
                "high": round(close * 1.01, 2),
                "low": round(close * 0.99, 2),
                "close": close,
                "volume": 100000 + idx,
                "amount": 1000000 + idx * 1000,
                "turn": 2.0 + (idx % 5),
                "pct_chg": pct,
            }
        )
        base_close = close
    return pd.DataFrame(rows)


class RuntimeChipRunnerTests(unittest.TestCase):
    def test_calculate_chip_peak_returns_avg_cost_profit_ratio_and_concentration(self):
        runner = RuntimeChipRunner(_FakeF10Service())
        df = _build_df([0.5] * 40)

        payload = runner.calculate_chip_peak(df)

        self.assertIn("avg_cost", payload)
        self.assertIn("profit_ratio", payload)
        self.assertIn("concentration", payload)
        self.assertGreater(payload["avg_cost"], 0)
        self.assertGreaterEqual(payload["profit_ratio"], 0.0)
        self.assertLessEqual(payload["profit_ratio"], 1.0)

    def test_calculate_extra_factors_uses_real_chip_semantics(self):
        runner = RuntimeChipRunner(_FakeF10Service())
        df = _build_df(([0.8] * 30) + ([1.2] * 10))

        factors = runner.calculate_extra_factors("000001", df)

        self.assertIn("avg_cost", factors)
        self.assertIn("profit_ratio", factors)
        self.assertIn("concentration", factors)
        self.assertNotEqual(factors["profit_ratio"], factors["change_pct_5d"])
        self.assertGreaterEqual(factors["profit_ratio"], 0.0)
        self.assertLessEqual(factors["profit_ratio"], 1.0)

    def test_limit_up_days_5_respects_20cm_symbols(self):
        runner = RuntimeChipRunner(_FakeF10Service())
        pct_values = ([0.5] * 35) + [10.0, 10.5, 11.0, 19.9, 20.1]
        df = _build_df(pct_values)

        factors = runner.calculate_extra_factors("300001", df)

        self.assertEqual(factors["limit_up_days_5"], 2)

    def test_limit_up_days_5_respects_st_symbols(self):
        runner = RuntimeChipRunner(_FakeF10Service({"000001": "*ST样本"}))
        pct_values = ([0.5] * 35) + [4.7, 4.8, 4.9, 5.0, 3.0]
        df = _build_df(pct_values)

        factors = runner.calculate_extra_factors("000001", df)

        self.assertEqual(factors["limit_up_days_5"], 2)


if __name__ == "__main__":
    unittest.main()
