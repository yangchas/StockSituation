from __future__ import annotations

import unittest

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import PlateMigrationFact, StockStateSnapshot
from engine_next.runtime.intraday_context_builder import IntradayContextBuilder
from engine_next.runtime.session_facts import (
    build_session_facts,
    session_facts_from_payload,
    session_facts_to_payload,
)


class _FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def get(self, key: str) -> str | None:
        return self._store.get(key)


class _FakeHub:
    def __init__(self) -> None:
        self.redis = _FakeRedis()


class SessionFactsTests(unittest.TestCase):
    def _build_builder(self) -> IntradayContextBuilder:
        builder = IntradayContextBuilder.__new__(IntradayContextBuilder)
        builder._hub = _FakeHub()
        builder._scoped_cache_token = None
        builder._json_hash_cache = {}
        builder._string_hash_cache = {}
        builder._string_key_cache = {}
        builder._primed_runtime_state = None
        return builder

    def test_hot_plate_sort_prefers_strength_then_change_pct_then_inflow(self) -> None:
        facts = build_session_facts(
            trade_date="2026-04-25",
            phase_name="intraday",
            snapshots=(),
            hot_plate_map={
                "通信": {"strength": 3000, "change_pct": 1.2, "net_inflow_yi": 5.0, "rank": 2},
                "农业": {"strength": 3000, "change_pct": 2.1, "net_inflow_yi": 1.0, "rank": 1},
                "电力": {"strength": 2800, "change_pct": 5.0, "net_inflow_yi": 9.0, "rank": 3},
            },
            yesterday_hot_plate_map={},
        )
        self.assertEqual(tuple(item.plate_name for item in facts.hot_plate_today[:3]), ("农业", "通信", "电力"))

    def test_session_fact_round_trip_keeps_migration_and_ladder(self) -> None:
        snapshots = (
            StockStateSnapshot(symbol="000001", plate="通信", lb_days=2, open_pct=0.02, current_pct=0.1, auction_amount=90_000_000, is_yest_limit=True),
            StockStateSnapshot(symbol="000002", plate="通信", lb_days=1, open_pct=0.01, current_pct=0.05, auction_amount=30_000_000),
            StockStateSnapshot(symbol="000003", plate="电力", lb_days=3, open_pct=-0.01, current_pct=0.03, auction_amount=20_000_000, is_yest_limit=True),
        )
        facts = build_session_facts(
            trade_date="2026-04-25",
            phase_name="auction",
            snapshots=snapshots,
            hot_plate_map={"通信": {"strength": 3200, "change_pct": 1.5, "net_inflow_yi": 8.0}},
            yesterday_hot_plate_map={"通信": {"strength": 2800, "change_pct": 0.8, "net_inflow_yi": 3.0}},
        )
        restored = session_facts_from_payload(session_facts_to_payload(facts))
        self.assertEqual(restored.hot_plate_today[0].plate_name, "通信")
        self.assertAlmostEqual(restored.plate_migration_map["通信"].strength_delta, 400.0)
        self.assertIn("1B->2B", restored.ladder_fact_map)
        self.assertEqual(restored.theme_fact_map["通信"].leader_symbol, "000001")

    def test_session_fact_cache_invalidates_when_structure_signature_changes(self) -> None:
        builder = self._build_builder()
        facts = build_session_facts(
            trade_date="2026-04-25",
            phase_name="auction",
            snapshots=(),
            hot_plate_map={"通信": {"strength": 3200, "change_pct": 1.5, "net_inflow_yi": 8.0}},
            yesterday_hot_plate_map={},
        )
        builder._write_cached_session_facts(
            trade_date="2026-04-25",
            phase=RunPhase.AUCTION,
            latest_quote_timestamp_ms=1000,
            symbol_count=5000,
            hot_plate_cache_trade_date="2026-04-25",
            hot_plate_updated_at_ts=2000,
            hot_plate_signature="hot-a",
            yest_limit_signature="yl-a",
            auction_signature="auc-a",
            facts=facts,
        )
        hit = builder._load_cached_session_facts(
            trade_date="2026-04-25",
            phase=RunPhase.AUCTION,
            latest_quote_timestamp_ms=1000,
            symbol_count=5000,
            hot_plate_cache_trade_date="2026-04-25",
            hot_plate_updated_at_ts=2000,
            hot_plate_signature="hot-a",
            yest_limit_signature="yl-a",
            auction_signature="auc-a",
        )
        miss = builder._load_cached_session_facts(
            trade_date="2026-04-25",
            phase=RunPhase.AUCTION,
            latest_quote_timestamp_ms=1000,
            symbol_count=5000,
            hot_plate_cache_trade_date="2026-04-25",
            hot_plate_updated_at_ts=2000,
            hot_plate_signature="hot-b",
            yest_limit_signature="yl-a",
            auction_signature="auc-a",
        )
        self.assertIsNotNone(hit)
        self.assertIsNone(miss)

    def test_migration_classification_does_not_mislabel_weakening_as_emerging(self) -> None:
        builder = self._build_builder()
        weakening = PlateMigrationFact(
            plate_name="通信",
            today_strength=2600,
            yesterday_strength=3200,
            strength_delta=-600,
            today_change_pct=-1.5,
            yesterday_change_pct=2.0,
            change_pct_delta=-3.5,
            today_net_inflow_yi=-4.0,
            yesterday_net_inflow_yi=8.0,
            net_inflow_yi_delta=-12.0,
            present_today=True,
            present_yesterday=True,
        )
        emerging = PlateMigrationFact(
            plate_name="电力",
            today_strength=2100,
            yesterday_strength=0.0,
            strength_delta=2100,
            today_change_pct=1.0,
            yesterday_change_pct=0.0,
            change_pct_delta=1.0,
            today_net_inflow_yi=3.0,
            yesterday_net_inflow_yi=0.0,
            net_inflow_yi_delta=3.0,
            present_today=True,
            present_yesterday=False,
        )
        self.assertEqual(builder._classify_plate_migration(weakening), "FADING")
        self.assertEqual(builder._classify_plate_migration(emerging), "EMERGING")


if __name__ == "__main__":
    unittest.main()
