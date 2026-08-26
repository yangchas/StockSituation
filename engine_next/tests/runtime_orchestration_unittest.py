from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime

sys.modules.setdefault("talib", types.ModuleType("talib"))
holidays_stub = types.ModuleType("holidays")
holidays_stub.CN = lambda: set()
sys.modules.setdefault("holidays", holidays_stub)

from engine_next.app_main import EngineApp
from engine_next.contracts.offline_sync_contracts import IntegratedSyncResult, WatermarkSnapshot
from engine_next.domain.enums import ExecutionEnvironment, RunPhase
from engine_next.runtime.controllers.settlement_controller import SettlementController
from engine_next.runtime.offline_sync_executor import OfflineSyncDecision, OfflineSyncRequest, ServerOnlyOfflineSyncExecutor


class RuntimeOrchestrationTests(unittest.TestCase):
    def test_safe_keys_prefers_scan_iter_to_avoid_blocking_keys_scan(self) -> None:
        class StubRedis:
            def __init__(self) -> None:
                self.keys_called = 0

            def scan_iter(self, match=None, count=None):
                self.last_match = match
                self.last_count = count
                return iter(("stock:quote:000001", "stock:quote:000002"))

            def keys(self, pattern):
                self.keys_called += 1
                return ("stock:quote:999999",)

        app = EngineApp.__new__(EngineApp)
        app._redis_client = StubRedis()

        keys = app._safe_keys("stock:quote:*")

        self.assertEqual(keys, ("stock:quote:000001", "stock:quote:000002"))
        self.assertEqual(app.redis.keys_called, 0)

    def test_filter_active_runtime_symbols_keeps_quotes_and_focus_pool(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._safe_keys = lambda pattern: ("stock:quote:000001",) if pattern == "stock:quote:*" else ()
        app._safe_hkeys = lambda key: ("000002",) if key == "cache:yest_limit_pool:2026-04-23" else ()
        app._safe_get = lambda key: None
        app._safe_hget = lambda key, field: None
        app._extract_symbols_from_json_text = EngineApp._extract_symbols_from_json_text.__get__(app, EngineApp)

        filtered = app._filter_active_runtime_symbols(
            ("000001", "000002", "000003"),
            now=datetime(2026, 4, 24, 14, 30, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
        )
        self.assertEqual(filtered, ("000001", "000002"))

    def test_filter_active_runtime_symbols_does_not_trim_premarket_universe(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._safe_keys = lambda pattern: ()
        app._safe_hkeys = lambda key: ()
        app._safe_get = lambda key: None
        app._safe_hget = lambda key, field: None
        app._extract_symbols_from_json_text = EngineApp._extract_symbols_from_json_text.__get__(app, EngineApp)

        symbols = ("000001", "000002", "000003")
        filtered = app._filter_active_runtime_symbols(
            symbols,
            now=datetime(2026, 4, 24, 8, 45, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
        )
        self.assertEqual(filtered, symbols)

    def test_filter_active_runtime_symbols_does_not_trim_historical_target_without_live_quotes(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._safe_keys = lambda pattern: ()
        app._safe_hkeys = lambda key: ("000001",) if key == "cache:yest_limit_pool:2026-04-24" else ()
        app._safe_get = lambda key: None
        app._safe_hget = lambda key, field: None
        app._extract_symbols_from_json_text = EngineApp._extract_symbols_from_json_text.__get__(app, EngineApp)

        symbols = ("000001", "000002", "000003")
        filtered = app._filter_active_runtime_symbols(
            symbols,
            now=datetime(2026, 4, 26, 11, 2, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
        )
        self.assertEqual(filtered, symbols)

    def test_filter_active_runtime_symbols_skips_trim_for_historical_replay_even_when_target_date_matches_now(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._safe_keys = lambda pattern: ("stock:quote:000001",) if pattern == "stock:quote:*" else ()
        app._safe_hkeys = lambda key: ("000002",) if key == "cache:yest_limit_pool:2026-04-23" else ()
        app._safe_get = lambda key: None
        app._safe_hget = lambda key, field: None
        app._extract_symbols_from_json_text = EngineApp._extract_symbols_from_json_text.__get__(app, EngineApp)

        symbols = ("000001", "000002", "000003")
        filtered = app._filter_active_runtime_symbols(
            symbols,
            now=datetime(2026, 4, 24, 15, 5, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            historical_replay=True,
        )
        self.assertEqual(filtered, symbols)

    def test_load_runtime_readiness_counts_selected_cache_coverage(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app.MIN_STOCK_PLATE_MAPPING_COUNT = 1000
        app._safe_hmget_presence_map = lambda key, fields: {
            ("cache:stock_extra:2026-04-23", ("000001", "000002", "000003")): {
                "000001": True,
                "000002": False,
                "000003": False,
            },
            ("cache:stock_extra:2026-04-24", ("000001", "000002", "000003")): {
                "000001": True,
                "000002": True,
                "000003": False,
            },
            ("cache:chip_peaks:2026-04-23", ("000001", "000002", "000003")): {
                "000001": True,
                "000002": False,
                "000003": False,
            },
            ("cache:chip_peaks:2026-04-24", ("000001", "000002", "000003")): {
                "000001": True,
                "000002": False,
                "000003": False,
            },
            ("cache:dde_ready:2026-04-23", ("000001", "000002", "000003")): {
                "000001": True,
                "000002": True,
                "000003": False,
            },
        }.get((key, tuple(fields)), {field: False for field in fields})
        app._safe_hlen = lambda key: {
            "market:stock_plate": 1500,
            "config:plate_mapping:s2p": 1500,
            "cache:hot_plates:2026-04-24": 10,
            "cache:yest_limit_pool:2026-04-23": 3,
        }.get(key, 0)
        app._safe_get_json = lambda key: {
            "cache:hot_plates_meta:2026-04-24": {
                "row_count": 10,
                "updated_at_ts": int(datetime(2026, 4, 24, 14, 29, 30).timestamp()),
                "trade_date": "2026-04-24",
            }
        }.get(key, {})
        app._hot_plate_freshness_limit_seconds = lambda phase: 45 * 60
        app._safe_exists = lambda key: key == "market:auction:anchor:20260424"

        readiness = app._load_runtime_readiness(
            now=datetime(2026, 4, 24, 14, 30, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            offline_context_date="2026-04-23",
            symbols=("000001", "000002", "000003"),
        )

        self.assertEqual(readiness["redis_factor_ready_count"], 1)
        self.assertEqual(readiness["current_trade_factor_ready_count"], 2)
        self.assertEqual(readiness["redis_chip_ready_count"], 1)
        self.assertEqual(readiness["current_trade_chip_ready_count"], 1)
        self.assertEqual(readiness["redis_dde_ready_count"], 2)
        self.assertTrue(readiness["hot_plates_ready"])
        self.assertTrue(readiness["hot_plates_today_ready"])
        self.assertTrue(readiness["hot_plates_effective_ready"])
        self.assertEqual(readiness["hot_plates_effective_trade_date"], "2026-04-24")
        self.assertTrue(readiness["hot_plates_live_fresh"])

    def test_load_runtime_readiness_uses_offline_context_fact_cache_before_postmarket(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app.MIN_STOCK_PLATE_MAPPING_COUNT = 1000
        app._safe_hmget_presence_map = lambda key, fields: {field: False for field in fields}
        app._safe_hlen = lambda key: 0
        app._safe_get_json = lambda key: {}
        app._safe_exists = lambda key: False
        app._safe_hmget_json_map = lambda key, fields: {
            "cache:analytics_readiness:2026-04-23": {
                "000001": {"kline_rows": 4, "structural_factor_gap": True},
            },
            "cache:symbol_meta:2026-04-23": {
                "000001": {"listing_date": "2026-04-20"},
            },
        }.get(key, {})

        readiness = app._load_runtime_readiness(
            now=datetime(2026, 4, 24, 8, 50, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            offline_context_date="2026-04-23",
            symbols=("000001",),
        )

        self.assertEqual(readiness["analytics_cache_date"], "2026-04-23")
        self.assertEqual(readiness["cached_kline_row_counts"], {"000001": 4})
        self.assertEqual(readiness["cached_listing_dates"], {"000001": "2026-04-20"})
        self.assertEqual(readiness["cached_structural_factor_gap"], {"000001": True})

    def test_load_runtime_readiness_accepts_historical_hot_plate_cache_without_live_freshness(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app.MIN_STOCK_PLATE_MAPPING_COUNT = 1000
        app._safe_hmget_presence_map = lambda key, fields: {field: False for field in fields}
        app._safe_hlen = lambda key: {
            "market:stock_plate": 1500,
            "config:plate_mapping:s2p": 1500,
            "cache:hot_plates:2026-04-24": 50,
            "cache:yest_limit_pool:2026-04-23": 50,
        }.get(key, 0)
        app._safe_get_json = lambda key: {
            "cache:hot_plates_meta:2026-04-24": {
                "row_count": 50,
                "updated_at_ts": int(datetime(2026, 4, 24, 14, 55, 0).timestamp()),
                "trade_date": "2026-04-24",
            }
        }.get(key, {})
        app._safe_exists = lambda key: False
        app._safe_hmget_json_map = lambda key, fields: {}
        app._hot_plate_freshness_limit_seconds = lambda phase: 45 * 60

        readiness = app._load_runtime_readiness(
            now=datetime(2026, 4, 26, 11, 2, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            offline_context_date="2026-04-23",
            symbols=("000001",),
        )

        self.assertTrue(readiness["hot_plates_ready"])
        self.assertTrue(readiness["hot_plates_live_fresh"])
        self.assertFalse(readiness["live_target_session"])

    def test_load_runtime_readiness_splits_today_and_effective_hot_plate_flags_in_premarket(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app.MIN_STOCK_PLATE_MAPPING_COUNT = 1000
        app._safe_hmget_presence_map = lambda key, fields: {field: False for field in fields}
        app._safe_hlen = lambda key: {
            "market:stock_plate": 1500,
            "config:plate_mapping:s2p": 1500,
            "cache:hot_plates:2026-04-23": 18,
            "cache:yest_limit_pool:2026-04-23": 50,
        }.get(key, 0)
        app._safe_get_json = lambda key: {
            "cache:hot_plates_meta:2026-04-23": {
                "row_count": 18,
                "updated_at_ts": int(datetime(2026, 4, 23, 15, 1, 0).timestamp()),
                "trade_date": "2026-04-23",
            }
        }.get(key, {})
        app._safe_exists = lambda key: False
        app._safe_hmget_json_map = lambda key, fields: {}
        app._hot_plate_freshness_limit_seconds = lambda phase: 45 * 60

        readiness = app._load_runtime_readiness(
            now=datetime(2026, 4, 24, 8, 50, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            offline_context_date="2026-04-23",
            symbols=("000001",),
        )

        self.assertTrue(readiness["hot_plates_ready"])
        self.assertFalse(readiness["hot_plates_today_ready"])
        self.assertTrue(readiness["hot_plates_effective_ready"])
        self.assertEqual(readiness["hot_plates_effective_trade_date"], "2026-04-23")

    def test_load_runtime_readiness_uses_yesterday_effective_hot_plates_when_intraday_today_missing(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app.MIN_STOCK_PLATE_MAPPING_COUNT = 1000
        app._safe_hmget_presence_map = lambda key, fields: {field: False for field in fields}
        app._safe_hlen = lambda key: {
            "market:stock_plate": 1500,
            "config:plate_mapping:s2p": 1500,
            "cache:hot_plates:2026-04-23": 12,
            "cache:yest_limit_pool:2026-04-23": 50,
        }.get(key, 0)
        app._safe_get_json = lambda key: {
            "cache:hot_plates_meta:2026-04-23": {
                "row_count": 12,
                "updated_at_ts": int(datetime(2026, 4, 23, 15, 1, 0).timestamp()),
                "trade_date": "2026-04-23",
            }
        }.get(key, {})
        app._safe_exists = lambda key: False
        app._safe_hmget_json_map = lambda key, fields: {}
        app._hot_plate_freshness_limit_seconds = lambda phase: 45 * 60

        readiness = app._load_runtime_readiness(
            now=datetime(2026, 4, 24, 9, 26, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            offline_context_date="2026-04-23",
            symbols=("000001",),
        )

        self.assertFalse(readiness["hot_plates_ready"])
        self.assertFalse(readiness["hot_plates_today_ready"])
        self.assertTrue(readiness["hot_plates_effective_ready"])
        self.assertEqual(readiness["hot_plates_effective_trade_date"], "2026-04-23")

    def test_load_runtime_readiness_marks_anchored_history_as_non_live_target_session(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app.MIN_STOCK_PLATE_MAPPING_COUNT = 1000
        app._safe_hmget_presence_map = lambda key, fields: {field: False for field in fields}
        app._safe_hlen = lambda key: 1 if key == "cache:hot_plates:2026-04-24" else 0
        app._safe_get_json = lambda key: {
            "cache:hot_plates_meta:2026-04-24": {
                "row_count": 1,
                "updated_at_ts": int(datetime(2026, 4, 24, 9, 35, 8).timestamp()),
                "trade_date": "2026-04-24",
            }
        }.get(key, {})
        app._safe_exists = lambda key: False
        app._safe_hmget_json_map = lambda key, fields: {}
        app._hot_plate_freshness_limit_seconds = lambda phase: 45 * 60

        readiness = app._load_runtime_readiness(
            now=datetime(2026, 4, 24, 15, 5, 0),
            trade_date="2026-04-24",
            previous_trade_date="2026-04-23",
            offline_context_date="2026-04-23",
            symbols=("000001",),
            historical_replay=True,
        )

        self.assertFalse(readiness["live_target_session"])
        self.assertTrue(readiness["hot_plates_ready"])
        self.assertTrue(readiness["hot_plates_live_fresh"])

    def test_settlement_audit_treats_newer_watermark_as_ready(self) -> None:
        executor = ServerOnlyOfflineSyncExecutor.__new__(ServerOnlyOfflineSyncExecutor)
        executor._safe_redis_hmget_count = lambda key, fields: 0
        executor._safe_redis_ping = lambda: True
        request = OfflineSyncRequest(
            now=datetime(2026, 4, 25, 14, 0, 0),
            target_date="2026-04-24",
            previous_trade_date="2026-04-23",
            symbols=("000001",),
            kline_watermarks={},
            factor_watermarks={},
            redis_factor_cache_ready={},
            environment=ExecutionEnvironment.SERVER,
        )
        decision = OfflineSyncDecision(
            allowed=True,
            target_date="2026-04-24",
            formal_kline_date="2026-04-23",
            availability=types.SimpleNamespace(ready=True, fallback_trade_date=None),
            missing_kline_symbols=(),
            missing_factor_symbols=(),
            kline_gap_plan=None,
            factor_gap_plan=None,
            stage_names=(),
            notes="",
        )
        snapshot = WatermarkSnapshot(
            target_date="2026-04-23",
            kline_latest_dates={"000001": "2026-04-24"},
            dde_latest_dates={"000001": "2026-04-24"},
            factor_latest_dates={"000001": "2026-04-24"},
        )

        lines = executor._build_settlement_audit_lines(
            request=request,
            decision=decision,
            snapshot=snapshot,
            pipeline_symbols=(),
            network_symbols=(),
            analytics_symbols=(),
        )

        self.assertTrue(any(line.startswith("[settlement]") and "0" in line for line in lines))

    def test_settlement_persists_startup_fact_cache_for_postmarket(self) -> None:
        class StubRedis:
            def __init__(self) -> None:
                self.hashes: dict[str, dict[str, str]] = {}
                self.expiries: dict[str, int] = {}
                self.strings: dict[str, str] = {}

            def hset(self, key: str, mapping=None, field=None, value=None):
                bucket = self.hashes.setdefault(key, {})
                if mapping is not None:
                    bucket.update(mapping)
                    return len(mapping)
                if field is not None:
                    bucket[str(field)] = str(value)
                    return 1
                return 0

            def hget(self, key: str, field: str):
                return (self.hashes.get(key) or {}).get(field)

            def hexists(self, key: str, field: str) -> bool:
                return field in (self.hashes.get(key) or {})

            def expire(self, key: str, ttl: int):
                self.expiries[key] = ttl
                return True

            def hlen(self, key: str) -> int:
                return len(self.hashes.get(key) or {})

            def set(self, key: str, value: str, ex=None):
                self.strings[key] = value
                return True

            def get(self, key: str):
                return self.strings.get(key)

        redis_client = StubRedis()
        redis_client.hset("cache:chip_peaks:2026-04-24", mapping={"000001": "{}"})
        controller = SettlementController(redis_client=redis_client)
        controller._load_listing_dates = lambda symbols: {"000002": "2026-04-20"}
        controller._load_kline_row_counts = lambda symbols, trade_date: {"000002": 4}

        stats = controller._persist_startup_fact_cache(
            request=OfflineSyncRequest(
                now=datetime(2026, 4, 24, 17, 45, 0),
                target_date="2026-04-24",
                previous_trade_date="2026-04-23",
                symbols=("000001", "000002"),
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready={},
                environment=ExecutionEnvironment.SERVER,
            ),
            watermark_snapshot=WatermarkSnapshot(
                target_date="2026-04-24",
                kline_latest_dates={"000001": "2026-04-24", "000002": "2026-04-24"},
                dde_latest_dates={"000001": "2026-04-24"},
                factor_latest_dates={"000001": "2026-04-24"},
            ),
            integrated_sync_results=(),
        )

        analytics_payload = controller._load_cached_settlement("2026-04-24")
        self.assertIsNone(analytics_payload)
        self.assertEqual(stats["startup_fact_analytics_count"], 2)
        self.assertEqual(stats["startup_fact_symbol_meta_count"], 1)
        self.assertEqual(stats["startup_fact_structural_count"], 1)
        self.assertIn("cache:analytics_readiness:2026-04-24", redis_client.hashes)
        self.assertIn("cache:symbol_meta:2026-04-24", redis_client.hashes)
        self.assertIn('"structural_factor_gap": true', redis_client.hashes["cache:analytics_readiness:2026-04-24"]["000002"])
        self.assertIn('"listing_date": "2026-04-20"', redis_client.hashes["cache:symbol_meta:2026-04-24"]["000002"])

    def test_cached_settlement_rebuilds_missing_startup_fact_cache(self) -> None:
        class StubRedis:
            def __init__(self) -> None:
                self.hashes: dict[str, dict[str, str]] = {}
                self.strings: dict[str, str] = {
                    "market:settlement:20260424:done": '{"trade_date":"2026-04-24","effective_targets":2,"result_count":2}'
                }

            def hset(self, key: str, mapping=None, field=None, value=None):
                bucket = self.hashes.setdefault(key, {})
                if mapping is not None:
                    bucket.update(mapping)
                    return len(mapping)
                if field is not None:
                    bucket[str(field)] = str(value)
                    return 1
                return 0

            def hget(self, key: str, field: str):
                return (self.hashes.get(key) or {}).get(field)

            def hlen(self, key: str) -> int:
                return len(self.hashes.get(key) or {})

            def hexists(self, key: str, field: str) -> bool:
                return field in (self.hashes.get(key) or {})

            def set(self, key: str, value: str, ex=None):
                self.strings[key] = value
                return True

            def get(self, key: str):
                return self.strings.get(key)

            def expire(self, key: str, ttl: int):
                return True

        redis_client = StubRedis()
        controller = SettlementController(redis_client=redis_client)
        controller._load_listing_dates = lambda symbols: {"000001": "2026-04-20"}
        controller._load_kline_row_counts = lambda symbols, trade_date: {"000001": 4}

        result = controller.execute(
            request=OfflineSyncRequest(
                now=datetime(2026, 4, 24, 18, 0, 0),
                target_date="2026-04-24",
                previous_trade_date="2026-04-23",
                symbols=("000001", "000002"),
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready={},
                environment=ExecutionEnvironment.SERVER,
            ),
            phase=RunPhase.POSTMARKET,
            should_audit_integrated_sync=True,
            integrated_sync_requested=True,
            requested_symbols=(),
            offline_decision=OfflineSyncDecision(
                allowed=True,
                target_date="2026-04-24",
                formal_kline_date="2026-04-24",
                availability=types.SimpleNamespace(ready=True, fallback_trade_date=None),
                missing_kline_symbols=(),
                missing_factor_symbols=(),
                kline_gap_plan=None,
                factor_gap_plan=None,
                stage_names=(),
                notes="",
            ),
            watermark_snapshot=WatermarkSnapshot(
                target_date="2026-04-24",
                kline_latest_dates={"000001": "2026-04-24", "000002": "2026-04-24"},
                dde_latest_dates={"000001": "2026-04-24", "000002": "2026-04-24"},
                factor_latest_dates={},
            ),
        )

        self.assertTrue(result.settlement_cached)
        self.assertIn("cache:analytics_readiness:2026-04-24", redis_client.hashes)
        self.assertTrue(result.settlement_payload.get("startup_fact_cache_rebuilt"))
        self.assertEqual(result.settlement_payload.get("startup_fact_analytics_count"), 2)

    def test_settlement_does_not_run_heavy_integrated_sync_during_intraday(self) -> None:
        class StubOfflineExecutor:
            def __init__(self) -> None:
                self.execute_calls = 0

            def resolve_effective_target_symbols(self, request, offline_decision, watermark_snapshot):
                return ("000001",)

            def execute_integrated_sync(self, request, watermark_snapshot=None):
                self.execute_calls += 1
                return (
                    IntegratedSyncResult(
                        symbol="000001",
                        target_date=request.target_date,
                        kline_ready=True,
                        dde_ready=True,
                        factor_ready=True,
                        chip_ready=True,
                        redis_cache_ready=True,
                        wrote_tdengine=(),
                        wrote_redis=(),
                    ),
                )

        controller = SettlementController(
            offline_executor=StubOfflineExecutor(),
            redis_client=types.SimpleNamespace(),
        )

        result = controller.execute(
            request=OfflineSyncRequest(
                now=datetime(2026, 4, 24, 10, 5, 0),
                target_date="2026-04-24",
                previous_trade_date="2026-04-23",
                symbols=("000001",),
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready={},
                environment=ExecutionEnvironment.SERVER,
            ),
            phase=RunPhase.INTRADAY,
            should_audit_integrated_sync=True,
            integrated_sync_requested=True,
            requested_symbols=(),
            offline_decision=OfflineSyncDecision(
                allowed=True,
                target_date="2026-04-24",
                formal_kline_date="2026-04-24",
                availability=types.SimpleNamespace(ready=True, fallback_trade_date=None),
                missing_kline_symbols=("000001",),
                missing_factor_symbols=("000001",),
                kline_gap_plan=None,
                factor_gap_plan=None,
                stage_names=(),
                notes="",
            ),
            watermark_snapshot=WatermarkSnapshot(
                target_date="2026-04-24",
                kline_latest_dates={},
                dde_latest_dates={},
                factor_latest_dates={},
            ),
        )

        self.assertEqual(result.effective_sync_symbols, ("000001",))
        self.assertEqual(result.integrated_sync_results, ())
        self.assertEqual(controller._offline_executor.execute_calls, 0)
        self.assertEqual(result.sync_pipeline_targets, 1)
        self.assertEqual(result.sync_network_targets, 1)
        self.assertEqual(result.sync_analytics_targets, 1)

    def test_settlement_allows_premarket_cache_heal_with_large_universe_when_network_gap_is_zero(self) -> None:
        class StubOfflineExecutor:
            def __init__(self) -> None:
                self.execute_calls = 0

            def resolve_effective_target_symbols(self, request, offline_decision, watermark_snapshot):
                return tuple(f"{i:06d}" for i in range(1, 121))

            def resolve_pipeline_target_symbols(self, request, watermark_snapshot, offline_decision):
                return tuple(f"{i:06d}" for i in range(1, 121))

            def resolve_network_target_symbols(self, request, watermark_snapshot, offline_decision):
                return ()

            def resolve_analytics_target_symbols(self, request, network_symbols, offline_decision):
                return tuple(f"{i:06d}" for i in range(1, 121))

            def execute_integrated_sync(self, request, watermark_snapshot=None):
                self.execute_calls += 1
                return ()

        controller = SettlementController(
            offline_executor=StubOfflineExecutor(),
            auto_discovered_sync_limit=50,
            redis_client=types.SimpleNamespace(),
        )

        result = controller.execute(
            request=OfflineSyncRequest(
                now=datetime(2026, 4, 24, 9, 5, 0),
                target_date="2026-04-24",
                previous_trade_date="2026-04-23",
                symbols=tuple(f"{i:06d}" for i in range(1, 401)),
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready={},
                environment=ExecutionEnvironment.SERVER,
            ),
            phase=RunPhase.PREMARKET,
            should_audit_integrated_sync=True,
            integrated_sync_requested=True,
            requested_symbols=(),
            offline_decision=OfflineSyncDecision(
                allowed=True,
                target_date="2026-04-24",
                formal_kline_date="2026-04-24",
                availability=types.SimpleNamespace(ready=True, fallback_trade_date=None),
                missing_kline_symbols=(),
                missing_factor_symbols=tuple(f"{i:06d}" for i in range(1, 121)),
                kline_gap_plan=None,
                factor_gap_plan=None,
                stage_names=(),
                notes="",
            ),
            watermark_snapshot=WatermarkSnapshot(
                target_date="2026-04-24",
                kline_latest_dates={},
                dde_latest_dates={},
                factor_latest_dates={},
            ),
        )

        self.assertTrue(result.integrated_sync_allowed)
        self.assertEqual(result.sync_pipeline_targets, 120)
        self.assertEqual(result.sync_network_targets, 0)
        self.assertEqual(result.sync_analytics_targets, 120)
        self.assertEqual(result.sync_load_units, 120)
        self.assertEqual(controller._offline_executor.execute_calls, 1)

    def test_settlement_blocks_premarket_large_network_gap_after_0900(self) -> None:
        class StubOfflineExecutor:
            def __init__(self) -> None:
                self.execute_calls = 0

            def resolve_effective_target_symbols(self, request, offline_decision, watermark_snapshot):
                return tuple(f"{i:06d}" for i in range(1, 121))

            def resolve_pipeline_target_symbols(self, request, watermark_snapshot, offline_decision):
                return tuple(f"{i:06d}" for i in range(1, 121))

            def resolve_network_target_symbols(self, request, watermark_snapshot, offline_decision):
                return tuple(f"{i:06d}" for i in range(1, 61))

            def resolve_analytics_target_symbols(self, request, network_symbols, offline_decision):
                return tuple(f"{i:06d}" for i in range(1, 121))

            def execute_integrated_sync(self, request, watermark_snapshot=None):
                self.execute_calls += 1
                return ()

        controller = SettlementController(
            offline_executor=StubOfflineExecutor(),
            auto_discovered_sync_limit=50,
            redis_client=types.SimpleNamespace(),
        )

        result = controller.execute(
            request=OfflineSyncRequest(
                now=datetime(2026, 4, 24, 9, 5, 0),
                target_date="2026-04-24",
                previous_trade_date="2026-04-23",
                symbols=tuple(f"{i:06d}" for i in range(1, 401)),
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready={},
                environment=ExecutionEnvironment.SERVER,
            ),
            phase=RunPhase.PREMARKET,
            should_audit_integrated_sync=True,
            integrated_sync_requested=True,
            requested_symbols=(),
            offline_decision=OfflineSyncDecision(
                allowed=True,
                target_date="2026-04-24",
                formal_kline_date="2026-04-24",
                availability=types.SimpleNamespace(ready=True, fallback_trade_date=None),
                missing_kline_symbols=tuple(f"{i:06d}" for i in range(1, 61)),
                missing_factor_symbols=tuple(f"{i:06d}" for i in range(1, 121)),
                kline_gap_plan=None,
                factor_gap_plan=None,
                stage_names=(),
                notes="",
            ),
            watermark_snapshot=WatermarkSnapshot(
                target_date="2026-04-24",
                kline_latest_dates={},
                dde_latest_dates={},
                factor_latest_dates={},
            ),
        )

        self.assertFalse(result.integrated_sync_allowed)
        self.assertEqual(result.sync_pipeline_targets, 120)
        self.assertEqual(result.sync_network_targets, 60)
        self.assertEqual(result.sync_analytics_targets, 120)
        self.assertEqual(result.sync_load_units, 360)
        self.assertEqual(controller._offline_executor.execute_calls, 0)

    def test_settlement_reuses_precomputed_sync_scope_for_execution(self) -> None:
        class StubOfflineExecutor:
            def __init__(self) -> None:
                self.received_scope = None

            def build_sync_scope(self, request, watermark_snapshot, offline_decision):
                return types.SimpleNamespace(
                    target_symbols=("000001", "000002"),
                    network_symbols=("000001",),
                    analytics_symbols=("000001", "000002"),
                    factor_cache_gap_count=1,
                    load_units=6,
                    pipeline_count=2,
                    network_count=1,
                    analytics_count=2,
                )

            def execute_integrated_sync(self, request, watermark_snapshot=None, sync_scope=None):
                self.received_scope = sync_scope
                return (
                    IntegratedSyncResult(
                        symbol="000001",
                        target_date=request.target_date,
                        kline_ready=True,
                        dde_ready=True,
                        factor_ready=True,
                        chip_ready=True,
                        redis_cache_ready=True,
                        wrote_tdengine=(),
                        wrote_redis=(),
                    ),
                )

        controller = SettlementController(
            offline_executor=StubOfflineExecutor(),
            redis_client=types.SimpleNamespace(),
        )

        result = controller.execute(
            request=OfflineSyncRequest(
                now=datetime(2026, 4, 24, 8, 55, 0),
                target_date="2026-04-24",
                previous_trade_date="2026-04-23",
                symbols=("000001", "000002"),
                kline_watermarks={},
                factor_watermarks={},
                redis_factor_cache_ready={},
                environment=ExecutionEnvironment.SERVER,
            ),
            phase=RunPhase.PREMARKET,
            should_audit_integrated_sync=True,
            integrated_sync_requested=True,
            requested_symbols=("000001", "000002"),
            offline_decision=OfflineSyncDecision(
                allowed=True,
                target_date="2026-04-24",
                formal_kline_date="2026-04-24",
                availability=types.SimpleNamespace(ready=True, fallback_trade_date=None),
                missing_kline_symbols=("000001",),
                missing_factor_symbols=("000001", "000002"),
                kline_gap_plan=None,
                factor_gap_plan=None,
                stage_names=(),
                notes="",
            ),
            watermark_snapshot=WatermarkSnapshot(
                target_date="2026-04-24",
                kline_latest_dates={},
                dde_latest_dates={},
                factor_latest_dates={},
            ),
        )

        self.assertEqual(result.effective_sync_symbols, ("000001", "000002"))
        self.assertEqual(result.sync_pipeline_targets, 2)
        self.assertEqual(result.sync_network_targets, 1)
        self.assertEqual(result.sync_analytics_targets, 2)
        self.assertEqual(result.sync_factor_cache_gaps, 1)
        self.assertEqual(result.sync_load_units, 6)
        self.assertIsNotNone(controller._offline_executor.received_scope)
        self.assertEqual(controller._offline_executor.received_scope.target_symbols, ("000001", "000002"))

    def test_cleanup_auction_temp_state_runs_once_before_0915(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._last_auction_cleanup_trade_date = None
        deleted_keys = []
        app._safe_keys = lambda pattern: ("market:auction:20260425:0921",)
        app._safe_delete = lambda keys: deleted_keys.append(tuple(sorted(keys))) or 3

        notes = app._cleanup_auction_temp_state_if_needed(
            now=datetime(2026, 4, 25, 8, 59, 0),
            trade_date="2026-04-25",
        )
        again = app._cleanup_auction_temp_state_if_needed(
            now=datetime(2026, 4, 25, 9, 0, 0),
            trade_date="2026-04-25",
        )

        self.assertEqual(notes, ("auction_temp_cleanup | trade_date=2026-04-25 | deleted=3",))
        self.assertEqual(len(deleted_keys), 1)
        self.assertIn("market:auction:20260425:0921", deleted_keys[0])
        self.assertEqual(again, ())

    def test_intraday_startup_auction_recap_emits_once_per_trade_date(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._last_intraday_auction_recap_trade_date = None

        first = app._should_emit_intraday_startup_auction_recap(
            phase=RunPhase.INTRADAY,
            trade_date="2026-04-25",
            now=datetime(2026, 4, 25, 9, 31, 0),
            lifecycle_audit_ran=True,
        )
        second = app._should_emit_intraday_startup_auction_recap(
            phase=RunPhase.INTRADAY,
            trade_date="2026-04-25",
            now=datetime(2026, 4, 25, 9, 31, 30),
            lifecycle_audit_ran=True,
        )
        third = app._should_emit_intraday_startup_auction_recap(
            phase=RunPhase.INTRADAY,
            trade_date="2026-04-26",
            now=datetime(2026, 4, 26, 9, 31, 0),
            lifecycle_audit_ran=True,
        )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertTrue(third)

    def test_resolve_loop_sleep_seconds_quiets_postmarket_until_1740(self) -> None:
        app = EngineApp.__new__(EngineApp)

        sleep_seconds = app._resolve_loop_sleep_seconds(
            now=datetime(2026, 4, 25, 16, 25, 0),
            phase=RunPhase.POSTMARKET,
            default_interval_seconds=30,
        )

        self.assertEqual(sleep_seconds, 4500)

    def test_resolve_loop_sleep_seconds_quiets_night_until_next_trade_day(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._next_trade_day_checkpoint = lambda now, clock_time: datetime(2026, 4, 27, 8, 30, 0)

        sleep_seconds = app._resolve_loop_sleep_seconds(
            now=datetime(2026, 4, 26, 23, 0, 0),
            phase=RunPhase.NIGHT,
            default_interval_seconds=30,
        )

        self.assertEqual(sleep_seconds, 34200)

    def test_should_render_cycle_suppresses_premarket_non_event_output(self) -> None:
        app = EngineApp.__new__(EngineApp)
        app._last_render_token = None

        should_render = app._should_render_cycle(
            request=types.SimpleNamespace(
                trade_date="2026-04-25",
                now=datetime(2026, 4, 25, 8, 45, 0),
            ),
            phase=RunPhase.PREMARKET,
            lifecycle_audit_ran=False,
            scheduled_event_result=types.SimpleNamespace(executed=False),
        )

        self.assertFalse(should_render)


if __name__ == "__main__":
    unittest.main()
