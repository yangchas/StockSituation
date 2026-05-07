from __future__ import annotations

import json
import unittest
from datetime import datetime
from pathlib import Path
import sys
import types

sys.modules.setdefault("talib", types.ModuleType("talib"))
holidays_stub = types.ModuleType("holidays")
holidays_stub.CN = lambda: set()
sys.modules.setdefault("holidays", holidays_stub)

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import IntradayContext, IntradayMarketSummary, PlateMigrationFact, StockStateSnapshot
from engine_next.contracts.offline_sync_contracts import IntegratedSyncResult
from engine_next.app_main import EngineApp
from engine_next.runtime.controllers.auction_runtime_controller import AuctionRuntimeController, StrategyConsoleState
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.runtime.intraday_context_builder import IntradayContextBuilder
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    build_plate_candidates_from_reason,
    build_runtime_writebacks_from_reasons,
    merge_theme_lists,
    split_plate_tokens,
)
from engine_next.runtime.startup_self_check import StartupSelfCheckRequest, StartupSelfCheckService
from engine_next.runtime.startup_runtime_coordinator import RuntimeStartupCoordinator, StartupCoordinationPlan
from engine_next.runtime.session_facts import (
    build_session_facts,
    session_facts_from_payload,
    session_facts_to_payload,
)
from engine_next.strategy_skill_layer.auction_plate_buckets import AuctionPlateBucketStat
from engine_next.contracts.offline_sync_contracts import WatermarkSnapshot


class _FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._set_options: dict[str, dict[str, object]] = {}

    def set(self, key: str, value: str, *args, **kwargs) -> None:
        self._store[key] = value
        self._set_options[key] = dict(kwargs)

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._hashes.pop(key, None)

    def hset(self, key: str, field: str, value: str) -> None:
        self._hashes.setdefault(key, {})[field] = value

    def hget(self, key: str, field: str) -> str | None:
        return self._hashes.get(key, {}).get(field)

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))


class _FakeHub:
    def __init__(self) -> None:
        self.redis = _FakeRedis()


class SessionFactsTests(unittest.TestCase):
    @staticmethod
    def _load_fixture(name: str) -> dict:
        fixture_path = Path(__file__).resolve().parent / "fixtures" / name
        return json.loads(fixture_path.read_text(encoding="utf-8"))

    def _build_builder(self) -> IntradayContextBuilder:
        builder = IntradayContextBuilder.__new__(IntradayContextBuilder)
        builder._hub = _FakeHub()
        builder._scoped_cache_token = None
        builder._json_hash_cache = {}
        builder._string_hash_cache = {}
        builder._string_key_cache = {}
        builder._primed_runtime_state = None
        builder._f10_service = None
        builder._f10_name_cache = {}
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

    def test_snapshot_name_falls_back_to_cache_then_auction_then_yest_limit(self) -> None:
        self.assertEqual(
            IntradayContextBuilder._resolve_snapshot_name(
                quote={},
                cache={"stock_name": "缓存名称"},
                auction={"name": "竞价名称"},
                yest={"name": "昨日名称"},
            ),
            "缓存名称",
        )
        self.assertEqual(
            IntradayContextBuilder._resolve_snapshot_name(
                quote={},
                cache={},
                auction={"name": "竞价名称"},
                yest={"name": "昨日名称"},
            ),
            "竞价名称",
        )
        self.assertEqual(
            IntradayContextBuilder._resolve_snapshot_name(
                quote={},
                cache={},
                auction={},
                yest={"name": "昨日名称"},
            ),
            "昨日名称",
        )

    def test_fallback_stock_names_are_loaded_once_and_cached(self) -> None:
        builder = self._build_builder()

        class _FakeF10Service:
            def __init__(self) -> None:
                self.calls = 0

            def batch_get_stock_names(self, codes):
                self.calls += 1
                return {"000001": "平安银行", "600000": "浦发银行"}

        builder._f10_service = _FakeF10Service()

        first = builder._load_fallback_stock_names(("000001", "600000"))
        second = builder._load_fallback_stock_names(("000001",))

        self.assertEqual(first["000001"], "平安银行")
        self.assertEqual(first["600000"], "浦发银行")
        self.assertEqual(second["000001"], "平安银行")
        self.assertEqual(builder._f10_service.calls, 1)

    def test_snapshot_plate_prefers_non_generic_theme_over_generic_runtime_plate(self) -> None:
        builder = self._build_builder()
        resolved = builder._resolve_snapshot_plate(
            runtime_plate="\u91d1\u878d\u6982\u5ff5",
            yest_plate="",
            themes=("\u7535\u529b", "\u7eff\u8272\u7535\u529b"),
            reason="",
        )
        self.assertEqual(resolved, "\u7535\u529b")

    def test_snapshot_plate_splits_compound_plate_candidates(self) -> None:
        builder = self._build_builder()
        resolved = builder._resolve_snapshot_plate(
            runtime_plate="\u7eff\u8272\u7535\u529b,\u7b97\u529b",
            yest_plate="",
            themes=("\u7535\u529b",),
            reason="\u901a\u8fc7\u5bf9\u5168\u7f51\u8282\u70b9\u8d44\u6e90\u5229\u7528\u8d1f\u8377\u7684\u5b9e\u65f6\u611f\u77e5\u4e0eAI\u667a\u80fd\u8c03\u5ea6",
        )
        self.assertIn(resolved, ("\u7eff\u8272\u7535\u529b", "\u7b97\u529b", "\u7535\u529b"))
        self.assertNotIn(",", resolved)
        self.assertNotIn("\u3001", resolved)

    def test_snapshot_plate_prefers_yest_limit_plate_over_runtime_plate(self) -> None:
        builder = self._build_builder()
        resolved = builder._resolve_snapshot_plate(
            runtime_plate="\u65e0\u4eba\u9a7e\u9a76",
            yest_plate="\u4e00\u5b63\u62a5\u589e\u957f",
            themes=("\u65e0\u4eba\u9a7e\u9a76", "\u4e00\u5b63\u62a5\u589e\u957f"),
            reason="",
        )
        self.assertEqual(resolved, "\u4e00\u5b63\u62a5\u589e\u957f")

    def test_snapshot_plate_ignores_long_reason_like_plate_text(self) -> None:
        builder = self._build_builder()
        resolved = builder._resolve_snapshot_plate(
            runtime_plate="",
            yest_plate="\u901a\u8fc7\u5bf9\u5168\u7f51\u8282\u70b9\u8d44\u6e90\u5229\u7528\u8d1f\u8377\u7684\u5b9e\u65f6\u611f\u77e5\u4e0eAI\u667a\u80fd\u8c03\u5ea6",
            themes=("\u5316\u5de5", "\u5e76\u8d2d\u91cd\u7ec4"),
            reason="",
        )
        self.assertIn(resolved, ("\u5316\u5de5", "\u5e76\u8d2d\u91cd\u7ec4"))

    def test_snapshot_plate_prefers_hot_plate_matched_theme_over_stale_runtime_plate(self) -> None:
        builder = self._build_builder()
        resolved = builder._resolve_snapshot_plate(
            runtime_plate="\u65e0\u4eba\u9a7e\u9a76",
            yest_plate="",
            themes=("\u65e0\u4eba\u9a7e\u9a76", "\u4e00\u5b63\u62a5\u589e\u957f"),
            reason="",
            hot_plate_map={
                "\u4e00\u5b63\u62a5\u589e\u957f": {"strength": 3200, "change_pct": 0.4, "net_inflow_yi": 72.04},
                "\u65e0\u4eba\u9a7e\u9a76": {"strength": 0, "change_pct": 0.0, "net_inflow_yi": 0.0},
            },
        )
        self.assertEqual(resolved, "\u4e00\u5b63\u62a5\u589e\u957f")

    def test_merge_plate_names_keeps_resolved_primary_plate_first(self) -> None:
        builder = self._build_builder()
        merged = builder._merge_plate_names(
            "\u7535\u529b",
            "\u673a\u5668\u4eba\u6982\u5ff5+\u667a\u80fd\u7535\u7f51",
            ("\u91d1\u878d\u6982\u5ff5", "\u7535\u529b", "\u667a\u80fd\u7535\u7f51"),
        )
        self.assertEqual(merged[0], "\u7535\u529b")
        self.assertIn("\u673a\u5668\u4eba", merged)
        self.assertEqual(len(merged), 2)

    def test_runtime_writebacks_prefer_reason_candidates_over_existing_themes(self) -> None:
        writebacks = build_runtime_writebacks_from_reasons(
            symbol="603390",
            reason_rows=(
                {
                    "reason": "",
                    "group_str": "\u4e00\u5b63\u62a5\u589e\u957f",
                    "gnsm": "\u65e0\u4eba\u9a7e\u9a76",
                },
            ),
            existing_themes=("\u65e0\u4eba\u9a7e\u9a76",),
            fallback_plate="\u65e0\u4eba\u9a7e\u9a76",
        )
        self.assertEqual(writebacks[RUNTIME_PRIMARY_PLATE_KEY]["603390"], "\u4e00\u5b63\u62a5\u589e\u957f")
        self.assertEqual(
            writebacks[PLATE_MAPPING_S2P_KEY]["603390"][:2],
            ["\u4e00\u5b63\u62a5\u589e\u957f", "\u65e0\u4eba\u9a7e\u9a76"],
        )

    def test_runtime_writebacks_keep_existing_themes_after_reason_candidates(self) -> None:
        writebacks = build_runtime_writebacks_from_reasons(
            symbol="603390",
            reason_rows=(
                {
                    "reason": "",
                    "group_str": "\u4e00\u5b63\u62a5\u589e\u957f",
                    "gnsm": "",
                },
            ),
            existing_themes=("\u65e0\u4eba\u9a7e\u9a76", "\u4e13\u7528\u8bbe\u5907"),
            fallback_plate="\u65e0\u4eba\u9a7e\u9a76",
        )
        self.assertEqual(
            writebacks[PLATE_MAPPING_S2P_KEY]["603390"],
            ["\u4e00\u5b63\u62a5\u589e\u957f", "\u65e0\u4eba\u9a7e\u9a76"],
        )

    def test_runtime_writebacks_ignore_free_text_reason_and_region_only_candidates(self) -> None:
        writebacks = build_runtime_writebacks_from_reasons(
            symbol="001234",
            reason_rows=(
                {
                    "reason": "\u670d\u88c5\u5bb6\u7eba+\u6c5f\u82cf\u7701\uff1b\u516c\u53f8\u4f4d\u4e8e\u6c5f\u82cf\u7701\u3002\u516c\u53f8\u4e3b\u8425\u4e1a\u52a1\u4e3a\u9488\u7ec7\u9762\u6599\u4e0e\u9488\u7ec7\u670d\u88c5\u7684\u7814\u53d1\u3001\u751f\u4ea7\u548c\u9500\u552e\u3002",
                    "group_str": "\u670d\u88c5\u5bb6\u7eba+\u6c5f\u82cf\u7701",
                    "gnsm": "",
                },
            ),
            existing_themes=(),
            fallback_plate="",
        )
        self.assertEqual(writebacks[RUNTIME_PRIMARY_PLATE_KEY]["001234"], "\u670d\u88c5\u5bb6\u7eba")
        self.assertEqual(writebacks[PLATE_MAPPING_S2P_KEY]["001234"], ["\u670d\u88c5\u5bb6\u7eba"])

    def test_split_plate_tokens_filters_region_only_and_business_phrases(self) -> None:
        tokens = split_plate_tokens(
            "\u6c5f\u82cf\u7701+\u5316\u5de5+\u516c\u53f8\u4f4d\u4e8e\u6c5f\u82cf\u7701+\u751f\u4ea7\u548c\u9500\u552e"
        )
        self.assertEqual(tokens, ["\u5316\u5de5"])

    def test_merge_theme_lists_splits_compound_candidates_and_drops_region(self) -> None:
        merged = merge_theme_lists((), ["\u670d\u88c5\u5bb6\u7eba\u3001\u6c5f\u82cf\u7701", "\u516c\u53f8\u4f4d\u4e8e\u6c5f\u82cf\u7701"])
        self.assertEqual(merged, ["\u670d\u88c5\u5bb6\u7eba"])

    def test_merge_theme_lists_drops_ascii_brand_like_noise(self) -> None:
        merged = merge_theme_lists((), ["Quiksilver", "Kappa", "AI", "\u7b97\u529b"])
        self.assertEqual(merged, ["AI", "\u7b97\u529b"])

    def test_build_plate_candidates_from_reason_prefers_reason_head_over_verbose_gnsm(self) -> None:
        candidates = build_plate_candidates_from_reason(
            reason="\u4e00\u5b63\u62a5\u589e\u957f\uff1b4\u670824\u65e5\u665a\u516c\u544a\uff0c2026\u5e741-3\u6708\u5f52\u5c5e\u4e0a\u5e02\u516c\u53f8\u80a1\u4e1c\u7684\u51c0\u5229\u6da6\u540c\u6bd4\u4e0a\u5e74\u589e\u957f\uff1a39.03%",
            group_str="",
            gnsm="4\u670824\u65e5\u665a\u516c\u544a\uff0c2026\u5e741-3\u6708\u5f52\u5c5e\u4e0a\u5e02\u516c\u53f8\u80a1\u4e1c\u7684\u51c0\u5229\u6da6\u540c\u6bd4\u4e0a\u5e74\u589e\u957f\uff1a39.03%",
        )
        self.assertEqual(candidates, ["\u4e00\u5b63\u62a5\u589e\u957f"])

    def test_build_plate_candidates_from_reason_strips_bracket_detail(self) -> None:
        candidates = build_plate_candidates_from_reason(
            reason="\u82af\u7247(\u6a21\u62df\u82af\u7247)\uff1b\u516c\u53f8\u4e13\u6ce8\u4e8e\u9ad8\u6027\u80fd\u6a21\u62df\u82af\u7247\u7814\u53d1",
            group_str="",
            gnsm="\u516c\u53f8\u4e13\u6ce8\u4e8e\u9ad8\u6027\u80fd\u6a21\u62df\u82af\u7247\u7814\u53d1",
        )
        self.assertEqual(candidates, ["\u82af\u7247"])

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

    def test_build_default_request_uses_latest_trading_day_on_weekend(self) -> None:
        sys.modules.setdefault("talib", types.ModuleType("talib"))
        holidays_stub = types.ModuleType("holidays")
        holidays_stub.CN = lambda: set()
        sys.modules.setdefault("holidays", holidays_stub)
        from engine_next.app_main import build_default_request

        request = build_default_request(now=datetime(2026, 4, 25, 12, 0, 0), run_integrated_sync=False)
        self.assertEqual(request.trade_date, "2026-04-24")
        self.assertEqual(request.previous_trade_date, "2026-04-23")

    def test_capital_text_and_net_inflow_format_are_human_readable(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        self.assertEqual(controller._fmt_net_inflow_yi(0.0), "0.00亿")
        self.assertEqual(controller._fmt_net_inflow_yi(12.345), "+12.35亿")
        self.assertEqual(controller._capital_behavior_text(1.3), "主力流入")
        self.assertEqual(controller._capital_behavior_text(0.5), "偏强流入")
        self.assertEqual(controller._capital_behavior_text(-0.4), "主力流出")
        self.assertEqual(controller._capital_behavior_text(0.0), "分歧震荡")

    def test_theme_facts_bind_each_snapshot_to_single_primary_theme(self) -> None:
        snapshots = (
            StockStateSnapshot(
                symbol="000001",
                plate="\u7535\u529b",
                real_plate_names=(
                    "\u91d1\u878d\u6982\u5ff5",
                    "\u673a\u5668\u4eba\u6982\u5ff5",
                    "\u7535\u529b",
                ),
                lb_days=2,
                auction_amount=80_000_000,
            ),
            StockStateSnapshot(
                symbol="000002",
                plate="\u6570\u5b57\u7ecf\u6d4e",
                real_plate_names=(
                    "\u6570\u5b57\u7ecf\u6d4e",
                    "\u4e00\u5b63\u62a5\u589e\u957f",
                ),
                lb_days=1,
                auction_amount=30_000_000,
            ),
        )
        facts = build_session_facts(
            trade_date="2026-04-25",
            phase_name="intraday",
            snapshots=snapshots,
            hot_plate_map={},
            yesterday_hot_plate_map={},
        )
        self.assertIn("\u7535\u529b", facts.theme_fact_map)
        self.assertIn("\u4e00\u5b63\u62a5\u589e\u957f", facts.theme_fact_map)
        self.assertNotIn("\u91d1\u878d\u6982\u5ff5", facts.theme_fact_map)
        self.assertNotIn("\u673a\u5668\u4eba\u6982\u5ff5", facts.theme_fact_map)
        self.assertNotIn("\u6570\u5b57\u7ecf\u6d4e", facts.theme_fact_map)
        self.assertEqual(facts.theme_fact_map["\u7535\u529b"].top3_symbols, ("000001",))
        self.assertEqual(facts.theme_fact_map["\u4e00\u5b63\u62a5\u589e\u957f"].top3_symbols, ("000002",))

    def test_display_plate_name_prefers_primary_plate_for_high_board(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(
            symbol="000001",
            plate="\u7535\u529b",
            real_plate_names=(
                "\u91d1\u878d\u6982\u5ff5",
                "\u673a\u5668\u4eba\u6982\u5ff5",
                "\u7535\u529b",
            ),
            lb_days=3,
            is_yest_limit=True,
        )
        self.assertEqual(controller._display_plate_name(snapshot, prefer_high_board=True), "\u7535\u529b")


    def test_startup_summary_shows_trade_date_and_offline_formal_separately(self) -> None:
        coordinator = RuntimeStartupCoordinator.__new__(RuntimeStartupCoordinator)
        service = StartupSelfCheckService()
        fixture = self._load_fixture("startup_factor_gap_real_2026-04-24.json")
        report = service.build_report(
            StartupSelfCheckRequest(
                now=datetime(2026, 4, 25, 14, 0, 0),
                trade_date=fixture["trade_date"],
                previous_trade_date=fixture["previous_trade_date"],
                symbols=tuple(fixture["focus_symbols"]),
                symbol_count=len(fixture["focus_symbols"]),
                watermark_snapshot=WatermarkSnapshot(
                    target_date=fixture["watermark_snapshot"]["target_date"],
                    kline_latest_dates=fixture["watermark_snapshot"]["kline_latest_dates"],
                    dde_latest_dates=fixture["watermark_snapshot"]["dde_latest_dates"],
                    factor_latest_dates=fixture["watermark_snapshot"]["factor_latest_dates"],
                ),
                redis_factor_cache_ready=fixture["redis_factor_cache_ready"],
                current_trade_factor_cache_ready=fixture["current_trade_factor_cache_ready"],
                current_trade_chip_cache_ready=fixture["current_trade_chip_cache_ready"],
                listing_dates=fixture["listing_dates"],
                kline_row_counts=fixture["kline_row_counts"],
                yest_limit_pool_ready=fixture["runtime_flags"]["yest_limit_pool_ready"],
                hot_plates_ready=fixture["runtime_flags"]["hot_plates_ready"],
                stock_plate_mapping_ready=fixture["runtime_flags"]["stock_plate_mapping_ready"],
                auction_anchor_ready=fixture["runtime_flags"]["auction_anchor_ready"],
                redis_factor_ready_count=sum(1 for ok in fixture["redis_factor_cache_ready"].values() if ok),
                redis_chip_ready_count=fixture["runtime_counts"]["redis_chip_ready_count"],
                redis_dde_ready_count=fixture["runtime_counts"]["redis_dde_ready_count"],
            )
        )
        plan = StartupCoordinationPlan(
            phase=RunPhase.INTRADAY,
            report=report,
            offline_decision=None,
            should_attempt_auction_recovery=False,
            should_refresh_hot_plates=False,
            should_refresh_yest_limit_pool=False,
            should_refresh_market_runtime_summary=True,
            should_run_postmarket_recap=False,
            sync_pipeline_targets=12,
            sync_network_targets=3,
            sync_analytics_targets=9,
            sync_factor_cache_gaps=4,
            previous_settlement_payload=self._load_fixture("settlement_done_2026-04-23_legacy.json"),
        )

        summary = coordinator.render_console_summary(plan)
        self.assertIn("trade_date=2026-04-24", summary)
        self.assertIn("offline_formal=2026-04-23", summary)
        self.assertNotIn("| formal=", summary)
        self.assertIn("sync_scope | pipe=12 | net=3 | calc=9 | factor_cache_gap=4", summary)
        self.assertIn("prev_settlement | trade_date=2026-04-23", summary)
        self.assertIn("| quality=legacy_payload", summary)

    def test_startup_summary_marks_legacy_previous_settlement_payload(self) -> None:
        coordinator = RuntimeStartupCoordinator.__new__(RuntimeStartupCoordinator)
        payload = self._load_fixture("settlement_done_2026-04-23_legacy.json")
        line = coordinator._render_previous_settlement_line(payload)
        self.assertEqual(
            line,
            "prev_settlement | trade_date=2026-04-23 | targets=5210 | results=5210 | quality=legacy_payload",
        )

    def test_render_execution_summary_adds_missing_digest(self) -> None:
        coordinator = RuntimeStartupCoordinator.__new__(RuntimeStartupCoordinator)
        service = StartupSelfCheckService()
        fixture = self._load_fixture("startup_factor_gap_real_2026-04-24.json")
        report = service.build_report(
            StartupSelfCheckRequest(
                now=datetime(2026, 4, 27, 0, 17, 0),
                trade_date="2026-04-27",
                previous_trade_date="2026-04-24",
                symbols=tuple(fixture["focus_symbols"]),
                symbol_count=len(fixture["focus_symbols"]),
                watermark_snapshot=WatermarkSnapshot(
                    target_date=fixture["watermark_snapshot"]["target_date"],
                    kline_latest_dates=fixture["watermark_snapshot"]["kline_latest_dates"],
                    dde_latest_dates=fixture["watermark_snapshot"]["dde_latest_dates"],
                    factor_latest_dates=fixture["watermark_snapshot"]["factor_latest_dates"],
                ),
                redis_factor_cache_ready=fixture["redis_factor_cache_ready"],
                current_trade_factor_cache_ready=fixture["current_trade_factor_cache_ready"],
                current_trade_chip_cache_ready=fixture["current_trade_chip_cache_ready"],
                listing_dates=fixture["listing_dates"],
                kline_row_counts=fixture["kline_row_counts"],
                yest_limit_pool_ready=True,
                hot_plates_ready=True,
                stock_plate_mapping_ready=True,
                auction_anchor_ready=False,
                redis_factor_ready_count=sum(1 for ok in fixture["redis_factor_cache_ready"].values() if ok),
                redis_chip_ready_count=fixture["runtime_counts"]["redis_chip_ready_count"],
                redis_dde_ready_count=fixture["runtime_counts"]["redis_dde_ready_count"],
            )
        )
        bundle = StartupCoordinationPlan(
            phase=RunPhase.PREMARKET,
            report=report,
            offline_decision=None,
            should_attempt_auction_recovery=False,
            should_refresh_hot_plates=False,
            should_refresh_yest_limit_pool=False,
            should_refresh_market_runtime_summary=False,
            should_run_postmarket_recap=False,
        )
        execution_summary = coordinator.render_execution_summary(
            types.SimpleNamespace(
                plan=bundle,
                stock_plate_result=None,
                auction_result=None,
                hot_plate_result=None,
                yest_limit_result=None,
                market_runtime_summary_result=None,
            )
        )
        joined = "\n".join(execution_summary)
        self.assertIn("状态摘要 |", joined)
        self.assertIn("数据缺口 |", joined)
        self.assertIn("影响判断 |", joined)
        self.assertIn("竞价锚点待竞价生成", joined)
        self.assertNotIn("因子说明 |", joined)

    def test_integrated_sync_summary_adds_prose_digest(self) -> None:
        app = EngineApp.__new__(EngineApp)
        lines = app._summarize_integrated_sync(
            (
                IntegratedSyncResult(
                    symbol="600200",
                    target_date="2026-04-24",
                    kline_ready=False,
                    dde_ready=False,
                    factor_ready=False,
                    chip_ready=False,
                    redis_cache_ready=False,
                    wrote_tdengine=(),
                    wrote_redis=(),
                    notes=("kline:not_ready",),
                ),
                IntegratedSyncResult(
                    symbol="600201",
                    target_date="2026-04-24",
                    kline_ready=True,
                    dde_ready=False,
                    factor_ready=True,
                    chip_ready=True,
                    redis_cache_ready=True,
                    wrote_tdengine=("kline", "factor", "chip"),
                    wrote_redis=("redis",),
                    notes=("dde:stale",),
                ),
                IntegratedSyncResult(
                    symbol="600202",
                    target_date="2026-04-24",
                    kline_ready=True,
                    dde_ready=True,
                    factor_ready=True,
                    chip_ready=True,
                    redis_cache_ready=True,
                    wrote_tdengine=("kline", "dde", "factor", "chip"),
                    wrote_redis=("redis",),
                ),
            )
        )
        joined = "\n".join(lines)
        self.assertIn("同步结果 |", joined)
        self.assertIn("主失败根因为 kline=1", joined)

    def test_startup_self_check_treats_newer_watermark_as_ready_for_older_formal_date(self) -> None:
        service = StartupSelfCheckService()
        fixture = self._load_fixture("startup_factor_gap_real_2026-04-24.json")
        report = service.build_report(
            StartupSelfCheckRequest(
                now=datetime(2026, 4, 25, 14, 0, 0),
                trade_date=fixture["trade_date"],
                previous_trade_date=fixture["previous_trade_date"],
                symbols=tuple(fixture["focus_symbols"]),
                symbol_count=len(fixture["focus_symbols"]),
                watermark_snapshot=WatermarkSnapshot(
                    target_date=fixture["watermark_snapshot"]["target_date"],
                    kline_latest_dates=fixture["watermark_snapshot"]["kline_latest_dates"],
                    dde_latest_dates=fixture["watermark_snapshot"]["dde_latest_dates"],
                    factor_latest_dates=fixture["watermark_snapshot"]["factor_latest_dates"],
                ),
                redis_factor_cache_ready=fixture["redis_factor_cache_ready"],
                current_trade_factor_cache_ready=fixture["current_trade_factor_cache_ready"],
                current_trade_chip_cache_ready=fixture["current_trade_chip_cache_ready"],
                listing_dates=fixture["listing_dates"],
                kline_row_counts=fixture["kline_row_counts"],
                yest_limit_pool_ready=fixture["runtime_flags"]["yest_limit_pool_ready"],
                hot_plates_ready=fixture["runtime_flags"]["hot_plates_ready"],
                stock_plate_mapping_ready=fixture["runtime_flags"]["stock_plate_mapping_ready"],
                auction_anchor_ready=fixture["runtime_flags"]["auction_anchor_ready"],
                redis_factor_ready_count=sum(1 for ok in fixture["redis_factor_cache_ready"].values() if ok),
                redis_chip_ready_count=fixture["runtime_counts"]["redis_chip_ready_count"],
                redis_dde_ready_count=fixture["runtime_counts"]["redis_dde_ready_count"],
            )
        )

        self.assertEqual(report.by_dataset()["daily_kline"].missing_count, 0)

    def test_startup_self_check_classifies_factor_gap_breakdown(self) -> None:
        service = StartupSelfCheckService()
        fixture = self._load_fixture("startup_factor_gap_real_2026-04-24.json")
        report = service.build_report(
            StartupSelfCheckRequest(
                now=datetime(2026, 4, 25, 14, 0, 0),
                trade_date=fixture["trade_date"],
                previous_trade_date=fixture["previous_trade_date"],
                symbols=tuple(fixture["focus_symbols"]),
                symbol_count=len(fixture["focus_symbols"]),
                watermark_snapshot=WatermarkSnapshot(
                    target_date=fixture["watermark_snapshot"]["target_date"],
                    kline_latest_dates=fixture["watermark_snapshot"]["kline_latest_dates"],
                    dde_latest_dates=fixture["watermark_snapshot"]["dde_latest_dates"],
                    factor_latest_dates=fixture["watermark_snapshot"]["factor_latest_dates"],
                ),
                redis_factor_cache_ready=fixture["redis_factor_cache_ready"],
                current_trade_factor_cache_ready=fixture["current_trade_factor_cache_ready"],
                current_trade_chip_cache_ready=fixture["current_trade_chip_cache_ready"],
                listing_dates=fixture["listing_dates"],
                kline_row_counts=fixture["kline_row_counts"],
                yest_limit_pool_ready=fixture["runtime_flags"]["yest_limit_pool_ready"],
                hot_plates_ready=fixture["runtime_flags"]["hot_plates_ready"],
                stock_plate_mapping_ready=fixture["runtime_flags"]["stock_plate_mapping_ready"],
                auction_anchor_ready=fixture["runtime_flags"]["auction_anchor_ready"],
                redis_factor_ready_count=sum(1 for ok in fixture["redis_factor_cache_ready"].values() if ok),
                redis_chip_ready_count=fixture["runtime_counts"]["redis_chip_ready_count"],
                redis_dde_ready_count=fixture["runtime_counts"]["redis_dde_ready_count"],
            )
        )

        factor_status = report.by_dataset()["daily_factors"]
        self.assertEqual(factor_status.missing_count, 8)
        self.assertEqual(factor_status.actionable_missing_count, 0)
        self.assertEqual(factor_status.cache_gap_count, 0)
        self.assertEqual(factor_status.structural_gap_count, 8)
        self.assertEqual(factor_status.dead_symbol_count, 0)
        self.assertEqual(factor_status.current_trade_ready_count, 0)

    def test_limitup_plate_board_prefers_limit_truth_cache_over_snapshot_guess(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._postmarket_limit_truth_cache = {}

        redis_key = "cache:limit_truth:2026-04-27"
        controller._intraday_hub.redis.hset(
            redis_key,
            "000001",
            json.dumps({"trade_date": "2026-04-27", "symbol": "000001", "lb_days": 2, "source": "wencai"}, ensure_ascii=False),
        )
        controller._intraday_hub.redis.hset(
            redis_key,
            "000002",
            json.dumps({"trade_date": "2026-04-27", "symbol": "000002", "lb_days": 1, "source": "wencai"}, ensure_ascii=False),
        )

        snapshots = {
            "000001": StockStateSnapshot(symbol="000001", name="通达电气", plate="通信", lb_days=2, current_pct=0.03, auction_amount=50_000_000),
            "000002": StockStateSnapshot(symbol="000002", name="卓郎智能", plate="通信", lb_days=1, current_pct=0.01, auction_amount=20_000_000),
            "000003": StockStateSnapshot(symbol="000003", name="其他个股", plate="电力", lb_days=1, current_pct=0.10, auction_amount=10_000_000),
        }
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-27",
                offline_context_date="2026-04-24",
                stock_snapshots=tuple(snapshots.values()),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000001", "000002", "000003"),
            candidate_scope_set=frozenset({"000001", "000002", "000003"}),
            actual_source="stale_intraday_snapshot",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map=snapshots,
            stock_name_map={symbol: snapshot.name for symbol, snapshot in snapshots.items()},
            plate_symbol_map={"通信": ("000001", "000002"), "电力": ("000003",)},
            decision_map={},
            frozen_postmarket_snapshot=True,
        )

        lines = controller._render_limitup_plate_board(state)
        joined = "\n".join(lines)

        self.assertIn("【涨停板块】题材 | 涨停数 | 最高板 | 代表 | 定性", joined)
        self.assertIn("通信 | 2 | 2板 | 通达电气", joined)
        self.assertNotIn("电力 | 1 | 1板 | 其他个股", joined)

    def test_limitup_plate_board_treats_missing_truth_lb_days_as_first_board(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._postmarket_limit_truth_cache = {}

        redis_key = "cache:limit_truth:2026-04-27"
        controller._intraday_hub.redis.hset(
            redis_key,
            "000001",
            json.dumps({"trade_date": "2026-04-27", "symbol": "000001", "lb_days": None, "source": "wencai"}, ensure_ascii=False),
        )

        snapshots = {
            "000001": StockStateSnapshot(symbol="000001", name="东方材料", plate="通信", lb_days=0, current_pct=0.10, auction_amount=30_000_000),
        }
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-27",
                offline_context_date="2026-04-24",
                stock_snapshots=tuple(snapshots.values()),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000001",),
            candidate_scope_set=frozenset({"000001"}),
            actual_source="stale_intraday_snapshot",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map=snapshots,
            stock_name_map={"000001": "东方材料"},
            plate_symbol_map={"通信": ("000001",)},
            decision_map={},
            frozen_postmarket_snapshot=True,
        )

        joined = "\n".join(controller._render_limitup_plate_board(state))

        self.assertIn("通信 | 1 | 首板 | 东方材料", joined)
        self.assertNotIn("0板", joined)

    def test_limitup_plate_board_prefers_kaipan_primary_plate_over_snapshot_guess(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._postmarket_limit_truth_cache = {}
        controller._postmarket_limit_truth_enriched_dates = {"2026-04-27"}

        redis_key = "cache:limit_truth:2026-04-27"
        controller._intraday_hub.redis.hset(
            redis_key,
            "000001",
            json.dumps(
                {
                    "trade_date": "2026-04-27",
                    "symbol": "000001",
                    "lb_days": 2,
                    "source": "wencai",
                },
                ensure_ascii=False,
            ),
        )
        controller._intraday_hub.redis.hset(
            redis_key,
            "000002",
            json.dumps(
                {
                    "trade_date": "2026-04-27",
                    "symbol": "000002",
                    "lb_days": 1,
                    "source": "wencai",
                },
                ensure_ascii=False,
            ),
        )
        controller._intraday_hub.redis.hset(RUNTIME_PRIMARY_PLATE_KEY, "000001", "一季报增长")
        controller._intraday_hub.redis.hset(RUNTIME_PRIMARY_PLATE_KEY, "000002", "一季报增长")
        controller._intraday_hub.redis.hset(PLATE_MAPPING_S2P_KEY, "000001", json.dumps(["一季报增长", "通信"], ensure_ascii=False))
        controller._intraday_hub.redis.hset(PLATE_MAPPING_S2P_KEY, "000002", json.dumps(["一季报增长", "PCB化学品"], ensure_ascii=False))

        snapshots = {
            "000001": StockStateSnapshot(symbol="000001", name="通达电气", plate="通信", lb_days=2, current_pct=0.03, auction_amount=50_000_000),
            "000002": StockStateSnapshot(symbol="000002", name="光华科技", plate="芯片", lb_days=1, current_pct=0.10, auction_amount=20_000_000),
        }
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-27",
                offline_context_date="2026-04-24",
                stock_snapshots=tuple(snapshots.values()),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000001", "000002"),
            candidate_scope_set=frozenset({"000001", "000002"}),
            actual_source="stale_intraday_snapshot",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map=snapshots,
            stock_name_map={symbol: snapshot.name for symbol, snapshot in snapshots.items()},
            plate_symbol_map={"通信": ("000001",), "芯片": ("000002",)},
            decision_map={},
            frozen_postmarket_snapshot=True,
        )

        joined = "\n".join(controller._render_limitup_plate_board(state))

        self.assertIn("一季报增长 | 2 | 2板 | 通达电气", joined)
        self.assertNotIn("通信 | 1 | 2板 | 通达电气", joined)


    def test_build_console_state_replays_previous_auction_snapshot_for_premarket_recap(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._intraday_hub.fetch_limit_truth = lambda *args, **kwargs: types.SimpleNamespace(rows=[])
        controller._market_runtime_summary_service = None
        controller._postmarket_limit_truth_cache = {}
        controller._postmarket_limit_truth_enriched_dates = set()
        controller._intraday_hub.redis.hset(
            "market:auction:20260428:0925",
            "000001",
            json.dumps(
                {
                    "symbol": "000001",
                    "name": "绀轰緥鑲",
                    "change_pct": 0.075,
                    "amount": 62_000_000,
                    "bid_amount": 31_000_000,
                },
                ensure_ascii=False,
            ),
        )
        controller._intraday_hub.redis.hset(
            "cache:hot_plates:2026-04-28",
            "绠楀姏",
            json.dumps({"plate_name": "绠楀姏", "rank": 1, "strength": 1000, "change_pct": 1.2, "net_inflow_yi": 5.0}, ensure_ascii=False),
        )
        controller._intraday_hub.redis.hset(
            "cache:hot_plates:2026-04-27",
            "閫氫俊",
            json.dumps({"plate_name": "閫氫俊", "rank": 1, "strength": 900, "change_pct": 0.8, "net_inflow_yi": 3.0}, ensure_ascii=False),
        )
        context = IntradayContext(
            phase=RunPhase.PREMARKET,
            trade_date="2026-04-29",
            offline_context_date="2026-04-28",
            stock_snapshots=(
                StockStateSnapshot(symbol="000001", name="绀轰緥鑲", plate="绠楀姏", current_pct=0.10),
            ),
            market_summary=IntradayMarketSummary(top_turnover_symbols=()),
            hot_plate_map={},
            yesterday_hot_plate_map={},
            yest_limit_map={},
            auction_map={},
        )

        state = controller._build_console_state(
            context,
            min_confidence=60,
            phase_label="premarket",
            historical_only=True,
        )

        self.assertAlmostEqual(state.snapshot_map["000001"].open_pct, 0.075)
        self.assertAlmostEqual(state.snapshot_map["000001"].auction_amount, 62_000_000)
        self.assertEqual(state.context.session_facts.hot_plate_today[0].plate_name, "绠楀姏")
        self.assertEqual(state.context.session_facts.hot_plate_yesterday[0].plate_name, "閫氫俊")

    def test_recap_feedback_metrics_use_t_minus_2_yesterday_limit_pool(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._intraday_hub.fetch_limit_truth = lambda *args, **kwargs: types.SimpleNamespace(rows=[])
        controller._market_runtime_summary_service = None
        controller._postmarket_limit_truth_cache = {}
        controller._postmarket_limit_truth_enriched_dates = set()
        controller._intraday_hub.redis.hset(
            "cache:yest_limit_pool:2026-04-27",
            "000001",
            json.dumps({"symbol": "000001", "lb_days": 1, "plate": "绠楀姏"}, ensure_ascii=False),
        )
        controller._intraday_hub.redis.hset(
            "market:auction:20260428:0925",
            "000001",
            json.dumps({"symbol": "000001", "change_pct": 0.03, "amount": 55_000_000}, ensure_ascii=False),
        )
        controller._intraday_hub.redis.hset(
            "cache:hot_plates:2026-04-28",
            "绠楀姏",
            json.dumps({"plate_name": "绠楀姏", "rank": 1, "strength": 1000, "change_pct": 1.2, "net_inflow_yi": 5.0}, ensure_ascii=False),
        )
        controller._intraday_hub.redis.hset(
            "cache:hot_plates:2026-04-27",
            "閫氫俊",
            json.dumps({"plate_name": "閫氫俊", "rank": 1, "strength": 900, "change_pct": 0.8, "net_inflow_yi": 3.0}, ensure_ascii=False),
        )
        context = IntradayContext(
            phase=RunPhase.PREMARKET,
            trade_date="2026-04-29",
            offline_context_date="2026-04-28",
            stock_snapshots=(
                StockStateSnapshot(symbol="000001", name="绀轰緥鑲", plate="绠楀姏", current_pct=0.10, is_locked=True),
            ),
            market_summary=IntradayMarketSummary(top_turnover_symbols=()),
            hot_plate_map={},
            yesterday_hot_plate_map={},
            yest_limit_map={},
            auction_map={},
        )
        state = controller._build_console_state(
            context,
            min_confidence=60,
            phase_label="premarket",
            historical_only=True,
        )

        metrics = controller._compute_recap_feedback_metrics(state, phase_label="premarket")

        self.assertEqual(metrics["sample_total"], 1)
        self.assertEqual(metrics["sample_matched"], 1)
        self.assertAlmostEqual(float(metrics["promotion_rate"]), 1.0)
        self.assertAlmostEqual(float(metrics["red_open_rate"]), 1.0)
        self.assertAlmostEqual(float(metrics["headshot_rate"]), 0.0)

    def test_premarket_recap_renders_dashes_without_replayed_auction_sample(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._intraday_hub.fetch_limit_truth = lambda *args, **kwargs: types.SimpleNamespace(rows=[])
        controller._market_runtime_summary_service = None
        controller._postmarket_limit_truth_cache = {}
        controller._postmarket_limit_truth_enriched_dates = set()
        controller._intraday_hub.redis.hset(
            "cache:yest_limit_pool:2026-04-27",
            "000001",
            json.dumps({"symbol": "000001", "lb_days": 1, "plate": "通信"}, ensure_ascii=False),
        )
        context = IntradayContext(
            phase=RunPhase.PREMARKET,
            trade_date="2026-04-29",
            offline_context_date="2026-04-28",
            stock_snapshots=(
                StockStateSnapshot(symbol="000001", name="测试一", plate="通信", current_pct=0.10, is_locked=True, is_yest_limit=True, lb_days=2),
            ),
            market_summary=IntradayMarketSummary(top_turnover_symbols=()),
            hot_plate_map={},
            yesterday_hot_plate_map={},
            yest_limit_map={},
            auction_map={},
        )

        state = controller._build_console_state(
            context,
            min_confidence=60,
            phase_label="premarket",
            historical_only=True,
        )

        close_joined = "\n".join(controller._render_recap_close_recap(state, phase_label="premarket"))
        ladder_joined = "\n".join(controller._render_ladder_map(state))
        ladder_recap_joined = "\n".join(controller._render_recap_ladder_recap(state, phase_label="premarket"))

        self.assertIn("红开率 | --", close_joined)
        self.assertIn("核按钮率 | --", close_joined)
        self.assertIn("0B->1B", ladder_joined)
        self.assertIn("| -- | 100% |", ladder_joined)
        self.assertIn("红开率 | --", ladder_recap_joined)
        self.assertIn("核按钮率 | --", ladder_recap_joined)

    def test_premarket_recap_risk_guard_filters_today_auction_pending(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.PREMARKET,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_formal",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=("auction_anchor_pending", "hot_plates"),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
            historical_only=True,
        )

        joined = "\n".join(controller._render_risk_guard(state, phase_label="postmarket"))

        self.assertNotIn("竞价锚点待生成", joined)
        self.assertIn("热点题材", joined)

    def test_premarket_plan_text_becomes_time_aware_near_auction(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)

        text = controller._premarket_plan_text("09:13")

        self.assertIn("12 分钟", text)
        self.assertNotIn("还远", text)

    def test_ladder_labels_do_not_use_open_judgement_without_auction_sample(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)

        extreme = controller._ladder_extreme_label("2B->3B", red_count=-1, promoted_count=0, total=2)
        mid = controller._mid_ladder_label("2B->3B", red_count=-1, promoted_count=0, total=2)

        self.assertEqual(extreme, "晋级偏弱")
        self.assertEqual(mid, "以晋级反馈为主")


    def test_fetch_hot_plates_preserves_previous_cache_on_empty_response(self) -> None:
        class _EmptyKaipan:
            def fetch_today_hot_plates(self):
                return []

            def fetch_hot_plates(self, trade_date: str):
                return []

            def to_tdengine_rows(self, dataset: str, payload, trade_date: str):
                return []

        redis_client = _FakeRedis()
        redis_client.hset(
            "cache:hot_plates:2026-04-29",
            "绠楀姏",
            json.dumps({"plate_name": "绠楀姏", "rank": 1}, ensure_ascii=False),
        )
        redis_client.set(
            "cache:hot_plates_meta:2026-04-29",
            json.dumps(
                {
                    "trade_date": "2026-04-29",
                    "row_count": 1,
                    "updated_at": "2026-04-29 09:25:00",
                    "updated_at_ts": 123456,
                },
                ensure_ascii=False,
            ),
        )
        hub = IntradayDataHub(redis_client=redis_client, kaipan_connector=_EmptyKaipan())

        result = hub.fetch_hot_plates("2026-04-29", RunPhase.AUCTION, today_mode=True)

        self.assertEqual(result.rows, [])
        self.assertIn("绠楀姏", redis_client.hgetall("cache:hot_plates:2026-04-29"))
        meta = json.loads(redis_client.get("cache:hot_plates_meta:2026-04-29") or "{}")
        self.assertEqual(meta.get("row_count"), 1)
        self.assertFalse(meta.get("success"))
        self.assertTrue(meta.get("cache_preserved"))
        self.assertEqual(meta.get("updated_at_ts"), 123456)

    def test_fetch_yest_limit_pool_preserves_previous_cache_on_empty_response(self) -> None:
        class _EmptyKaipan:
            def fetch_yesterday_bans_pool(self, trade_date: str, max_ban: int = 5):
                return []

            def to_tdengine_rows(self, dataset: str, payload, trade_date: str):
                return []

        redis_client = _FakeRedis()
        redis_client.hset(
            "cache:yest_limit_pool:2026-04-28",
            "000001",
            json.dumps({"symbol": "000001", "plate": "绠楀姏"}, ensure_ascii=False),
        )
        redis_client.set(
            "cache:yest_limit_pool_meta:2026-04-28",
            json.dumps(
                {
                    "trade_date": "2026-04-28",
                    "row_count": 1,
                    "updated_at": "2026-04-29 09:25:00",
                    "updated_at_ts": 654321,
                },
                ensure_ascii=False,
            ),
        )
        hub = IntradayDataHub(redis_client=redis_client, kaipan_connector=_EmptyKaipan())

        result = hub.fetch_yest_limit_pool("2026-04-28", RunPhase.AUCTION)

        self.assertEqual(result.rows, [])
        self.assertIn("000001", redis_client.hgetall("cache:yest_limit_pool:2026-04-28"))
        meta = json.loads(redis_client.get("cache:yest_limit_pool_meta:2026-04-28") or "{}")
        self.assertEqual(meta.get("row_count"), 1)
        self.assertFalse(meta.get("success"))
        self.assertTrue(meta.get("cache_preserved"))
        self.assertEqual(meta.get("updated_at_ts"), 654321)

    def test_collect_missing_inputs_marks_today_hot_plates_missing_when_yesterday_exists(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        context = IntradayContext(
            phase=RunPhase.AUCTION,
            trade_date="2026-04-29",
            offline_context_date="2026-04-28",
            stock_snapshots=(),
            market_summary=IntradayMarketSummary(top_turnover_symbols=()),
            hot_plate_map={},
            yesterday_hot_plate_map={"绠楀姏": {"plate_name": "绠楀姏"}},
            yest_limit_map={"000001": {"symbol": "000001"}},
            auction_map={"000001": {"symbol": "000001"}},
        )

        missing = controller._collect_missing_inputs(context, phase_label="auction")

        self.assertEqual(missing, ("hot_plates_today_missing",))
        self.assertEqual(controller._missing_text("hot_plates_today_missing"), "当日热板缺失(沿用昨日热板)")

    def test_auction_rendering_degrades_when_only_yesterday_hot_plates_exist(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        summary = IntradayMarketSummary(
            top_turnover_symbols=("000001",),
            top_plate_name="芯片",
            top_plate_migration_type="PERSIST",
            mainline_sector="算力",
            mainline_net_inflow_yi=12.3,
            top_sector_pct=2.6,
            hot_plate_count=0,
            persistent_plate_count=3,
            emerging_plate_count=2,
            fading_plate_count=1,
            mainline_switch=True,
            total_yest_limit_count=15,
            market_predicted_full_day_amount=8_500_000_000,
            market_volume_level="high",
            promotion_rate=0.2,
            red_open_rate=0.5,
            headshot_rate=0.0,
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(),
                market_summary=summary,
                hot_plate_map={},
                yesterday_hot_plate_map={
                    "一季报增长": {"plate_name": "一季报增长"},
                    "芯片": {"plate_name": "芯片"},
                },
                yest_limit_map={"000001": {"symbol": "000001"}},
                auction_map={"000001": {"symbol": "000001"}},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=("hot_plates_today_missing",),
            snapshot_map={},
            stock_name_map={"000001": "中国长城"},
            plate_symbol_map={},
            decision_map={},
        )

        mainline_joined = "\n".join(controller._render_mainline_board(state))
        structure_joined = "\n".join(controller._render_auction_structure(state))
        feedback_joined = "\n".join(controller._render_yest_limit_feedback(state))
        plan_joined = "\n".join(controller._render_auction_plan(state))

        self.assertIn("题材主攻/次强 | -- / -- (沿用昨日热板)", mainline_joined)
        self.assertIn("是否切换/迁移 | -- / --", mainline_joined)
        self.assertIn("共振分 | --", "\n".join(controller._render_auction_thermo(state)))
        self.assertIn("热门题材数 | 2(沿用昨日)", structure_joined)
        self.assertIn("延续/新发酵/兑现 | --/--/--", structure_joined)
        self.assertIn("主线/副线 | -- / --", "\n".join(controller._render_mainline_recap(state)))
        self.assertIn("当日热板缺失，先看昨日涨停反馈，不判主攻切换", feedback_joined)
        self.assertIn("盘面归类 | 观察盘", plan_joined)
        self.assertIn("当日热板缺失，先看昨日热板延续与高标承接，不判主攻切换", plan_joined)
        self.assertIn("沿用昨日热板，不输出今日强度/涨幅/净流入真值", "\n".join(controller._render_today_hot_plates(state)))

    def test_auction_plan_degrades_when_hot_plates_are_fully_missing(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(
                    top_turnover_symbols=(),
                    promotion_rate=0.1,
                    red_open_rate=0.3,
                    headshot_rate=0.0,
                ),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={"000001": {"symbol": "000001"}},
                auction_map={"000001": {"symbol": "000001"}},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=("hot_plates",),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
        )

        plan_joined = "\n".join(controller._render_auction_plan(state))

        self.assertIn("盘面归类 | 观察盘", plan_joined)
        self.assertIn("热点题材缺失，先看昨日涨停反馈与高标承接，不判主攻切换", plan_joined)

    def test_auction_rendering_uses_dashes_when_feedback_inputs_are_missing(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(
                    top_turnover_symbols=(),
                    promotion_rate=0.0,
                    red_open_rate=0.0,
                    headshot_rate=0.0,
                    sentiment_score=0.0,
                    market_full_auc_amt=0.0,
                    context_auc_amt=0.0,
                    avg_bid_amt=0.0,
                    total_yest_limit_count=0,
                ),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=("auction_anchor", "yest_limit_pool", "hot_plates"),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
        )

        thermo_joined = "\n".join(controller._render_auction_thermo(state))
        structure_joined = "\n".join(controller._render_auction_structure(state))
        feedback_joined = "\n".join(controller._render_yest_limit_feedback(state))

        self.assertIn("情绪分 | --", thermo_joined)
        self.assertIn("晋级率 | --", thermo_joined)
        self.assertIn("红开率 | --", thermo_joined)
        self.assertIn("核按钮率 | --", thermo_joined)
        self.assertIn("全市场竞价额 | --", structure_joined)
        self.assertIn("核心样本竞价额 | --", structure_joined)
        self.assertIn("平均承接 | --", structure_joined)
        self.assertIn("昨涨停样本 | --", structure_joined)
        self.assertIn("晋级率 -- | 样本不足", feedback_joined)
        self.assertIn("样本 --，竞价锚点和昨日涨停池未就绪", feedback_joined)

    def test_opening_validation_renders_feedback_loop(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        strong = StockStateSnapshot(
            symbol="000001",
            name="强势股",
            plate="算力",
            open_pct=0.04,
            current_pct=0.10,
            auction_amount=80_000_000,
            amount_2m=60_000_000,
            speed_1m=0.02,
            is_yest_limit=True,
            leader_rank_in_theme=1,
        )
        weak = StockStateSnapshot(
            symbol="000002",
            name="走弱股",
            plate="芯片",
            open_pct=0.06,
            current_pct=-0.01,
            auction_amount=50_000_000,
            leader_rank_in_theme=1,
        )
        rebound = StockStateSnapshot(
            symbol="000003",
            name="反包股",
            plate="智元机器人",
            open_pct=-0.03,
            current_pct=0.06,
            auction_amount=30_000_000,
            amount_2m=25_000_000,
            leader_rank_in_theme=1,
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(strong, weak, rebound),
                market_summary=IntradayMarketSummary(top_turnover_symbols=("000001", "000002")),
                hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 120.0}},
                yesterday_hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 90.0}},
                yest_limit_map={"000001": {"symbol": "000001"}},
                auction_map={"000001": {"symbol": "000001"}},
            ),
            candidate_scope=("000001", "000002", "000003"),
            candidate_scope_set=frozenset({"000001", "000002", "000003"}),
            actual_source="redis_0925",
            plate_stats=(
                AuctionPlateBucketStat(
                    plate_name="算力",
                    weighted_score=88.0,
                    symbol_count=3,
                    auction_symbol_count=2,
                    auction_amount=90_000_000,
                    yest_limit_count=1,
                    leader_count=1,
                    hot_rank=1,
                    hot_change_pct=1.5,
                    hot_strength=120.0,
                    hot_net_inflow_yi=6.0,
                    hot_capital_behavior=1.2,
                    expectation="attack",
                    sample_symbols=("000001",),
                ),
            ),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map={"000001": strong, "000002": weak, "000003": rebound},
            stock_name_map={"000001": "强势股", "000002": "走弱股", "000003": "反包股"},
            plate_symbol_map={},
            decision_map={},
        )

        rendered = "\n".join(controller._render_opening_validation(state))

        self.assertIn("【开盘验证】维度 | 结果", rendered)
        self.assertIn("强开兑现 | 强势股(+4.0%/+10.0%/算力)", rendered)
        self.assertIn("高开转虚 | 走弱股(+6.0%/-1.0%/芯片)", rendered)
        self.assertIn("低开转强 | 反包股(-3.0%/+6.0%/智元机器人)", rendered)
        self.assertIn("题材验证 | 算力(额0.90亿/竞价样本2/昨板1/代表强势股(+4.0%/+10.0%/算力))", rendered)

    def test_recap_plan_review_includes_opening_feedback(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        strong = StockStateSnapshot(symbol="000001", name="强势股", plate="算力", open_pct=0.04, current_pct=0.10)
        weak = StockStateSnapshot(symbol="000002", name="走弱股", plate="芯片", open_pct=0.06, current_pct=-0.01)
        rebound = StockStateSnapshot(symbol="000003", name="反包股", plate="智元机器人", open_pct=-0.03, current_pct=0.06)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(strong, weak, rebound),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
                session_facts=build_session_facts(
                    trade_date="2026-04-29",
                    phase_name="postmarket",
                    snapshots=(strong, weak, rebound),
                    hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                    yesterday_hot_plate_map={},
                ),
            ),
            candidate_scope=("000001", "000002", "000003"),
            candidate_scope_set=frozenset({"000001", "000002", "000003"}),
            actual_source="redis_formal",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map={"000001": strong, "000002": weak, "000003": rebound},
            stock_name_map={"000001": "强势股", "000002": "走弱股", "000003": "反包股"},
            plate_symbol_map={},
            decision_map={},
        )
        controller._load_recap_reference = lambda _state, phase_label: {
            "auction_map": {
                "000001": {"symbol": "000001", "amount": 50_000_000},
                "000002": {"symbol": "000002", "amount": 40_000_000},
            },
            "truth_rows": (
                {"symbol": "000001", "lb_days": 2, "name": "强势股", "plate": "算力"},
                {"symbol": "000002", "lb_days": 1, "name": "走弱股", "plate": "芯片"},
            ),
            "trade_date": "2026-04-29",
            "previous_trade_date": "2026-04-28",
            "yest_limit_map": {},
        }
        controller._summarize_limitup_mainline_by_rows = lambda _state, rows: ("算力", "芯片")

        rendered = "\n".join(controller._render_recap_plan_review(state, phase_label="postmarket"))

        self.assertIn("开盘兑现 | 强开兑现=强势股(+4.0%/+10.0%/算力) ; 高开转虚=走弱股(+6.0%/-1.0%/芯片) ; 低开转强=反包股(-3.0%/+6.0%/智元机器人)", rendered)
        self.assertIn("题材验证 | -", rendered)

    def test_opening_validation_checkpoint_persists_and_recap_prefers_frozen_payload(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        strong_live = StockStateSnapshot(symbol="000001", name="强势股", plate="算力", open_pct=0.04, current_pct=0.10)
        weak_live = StockStateSnapshot(symbol="000002", name="走弱股", plate="芯片", open_pct=0.06, current_pct=-0.01)
        rebound_live = StockStateSnapshot(symbol="000003", name="反包股", plate="智元机器人", open_pct=-0.03, current_pct=0.06)
        live_context = IntradayContext(
            phase=RunPhase.INTRADAY,
            trade_date="2026-04-29",
            offline_context_date="2026-04-28",
            stock_snapshots=(strong_live, weak_live, rebound_live),
            market_summary=IntradayMarketSummary(top_turnover_symbols=()),
            hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
            yesterday_hot_plate_map={},
            yest_limit_map={},
            auction_map={},
            session_facts=build_session_facts(
                trade_date="2026-04-29",
                phase_name="opening",
                snapshots=(strong_live, weak_live, rebound_live),
                hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                yesterday_hot_plate_map={},
            ),
        )
        persist_notes = controller.persist_opening_validation_checkpoint(
            trade_date="2026-04-29",
            intraday_context=live_context,
            now=datetime(2026, 4, 29, 9, 31, 5),
        )
        self.assertIn("opening_validation_checkpoint persisted", persist_notes)
        self.assertEqual(
            controller._intraday_hub.redis._set_options["market:opening:validation:20260429"].get("ex"),
            controller.OPENING_VALIDATION_TTL_SECONDS,
        )

        # Change postmarket snapshots so fallback recompute would differ if frozen payload were not used.
        strong_close = StockStateSnapshot(symbol="000001", name="强势股", plate="算力", open_pct=0.04, current_pct=0.05)
        weak_close = StockStateSnapshot(symbol="000002", name="走弱股", plate="芯片", open_pct=0.06, current_pct=0.04)
        rebound_close = StockStateSnapshot(symbol="000003", name="反包股", plate="智元机器人", open_pct=-0.03, current_pct=-0.02)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(strong_close, weak_close, rebound_close),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
                session_facts=build_session_facts(
                    trade_date="2026-04-29",
                    phase_name="postmarket",
                    snapshots=(strong_close, weak_close, rebound_close),
                    hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                    yesterday_hot_plate_map={},
                ),
            ),
            candidate_scope=("000001", "000002", "000003"),
            candidate_scope_set=frozenset({"000001", "000002", "000003"}),
            actual_source="redis_formal",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map={"000001": strong_close, "000002": weak_close, "000003": rebound_close},
            stock_name_map={"000001": "强势股", "000002": "走弱股", "000003": "反包股"},
            plate_symbol_map={},
            decision_map={},
        )
        controller._load_recap_reference = lambda _state, phase_label: {
            "auction_map": {
                "000001": {"symbol": "000001", "amount": 50_000_000},
                "000002": {"symbol": "000002", "amount": 40_000_000},
            },
            "truth_rows": (
                {"symbol": "000001", "lb_days": 2, "name": "强势股", "plate": "算力"},
                {"symbol": "000002", "lb_days": 1, "name": "走弱股", "plate": "芯片"},
            ),
            "trade_date": "2026-04-29",
            "previous_trade_date": "2026-04-28",
            "yest_limit_map": {},
        }
        controller._summarize_limitup_mainline_by_rows = lambda _state, rows: ("算力", "芯片")

        rendered = "\n".join(controller._render_recap_plan_review(state, phase_label="postmarket"))

        self.assertIn("开盘兑现 | 强开兑现=强势股(+4.0%/+10.0%/算力) ; 高开转虚=走弱股(+6.0%/-1.0%/芯片) ; 低开转强=反包股(-3.0%/+6.0%/智元机器人)", rendered)

    def test_recap_plan_review_downgrades_overlap_when_opening_validation_is_negative(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._intraday_hub = _FakeHub()
        controller._intraday_hub.redis.set(
            "market:opening:validation:20260429",
            json.dumps(
                {
                    "trade_date": "2026-04-29",
                    "updated_at_ts": 1,
                    "validated": ["竞价龙头A=高开转虚", "竞价龙头B=承接偏弱"],
                    "plate_checks": [],
                },
                ensure_ascii=False,
            ),
        )
        strong_close = StockStateSnapshot(symbol="000001", name="强势股", plate="算力", open_pct=0.04, current_pct=0.10)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(strong_close,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
                session_facts=build_session_facts(
                    trade_date="2026-04-29",
                    phase_name="postmarket",
                    snapshots=(strong_close,),
                    hot_plate_map={"算力": {"plate_name": "算力", "rank": 1, "strength": 100.0}},
                    yesterday_hot_plate_map={},
                ),
            ),
            candidate_scope=("000001",),
            candidate_scope_set=frozenset({"000001"}),
            actual_source="redis_formal",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map={"000001": strong_close},
            stock_name_map={"000001": "强势股"},
            plate_symbol_map={},
            decision_map={},
        )
        controller._load_recap_reference = lambda _state, phase_label: {
            "auction_map": {
                "000001": {"symbol": "000001", "amount": 50_000_000},
            },
            "truth_rows": (
                {"symbol": "000001", "lb_days": 2, "name": "强势股", "plate": "算力"},
            ),
            "trade_date": "2026-04-29",
            "previous_trade_date": "2026-04-28",
            "yest_limit_map": {},
        }
        controller._summarize_limitup_mainline_by_rows = lambda _state, rows: ("算力", "芯片")

        rendered = "\n".join(controller._render_recap_plan_review(state, phase_label="postmarket"))

        self.assertIn("预案判断 | 半对半错", rendered)
        self.assertIn("调整建议 | 竞价强桶与收盘主线仍重合在 算力，但开盘验证偏弱", rendered)

    def test_postmarket_summary_uses_dashes_when_feedback_inputs_are_missing(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.POSTMARKET,
                trade_date="2026-04-29",
                offline_context_date="2026-04-28",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(
                    top_turnover_symbols=(),
                    promotion_rate=0.0,
                    red_open_rate=0.0,
                    headshot_rate=0.0,
                    sentiment_score=0.0,
                ),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_formal",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=("auction_anchor", "yest_limit_pool", "hot_plates"),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
        )

        close_joined = "\n".join(controller._render_close_recap(state))
        ladder_joined = "\n".join(controller._render_ladder_recap(state))
        story_joined = "\n".join(controller._render_day_recap_story(state))

        self.assertIn("结论 | --", close_joined)
        self.assertIn("情绪分 | --", close_joined)
        self.assertIn("晋级率 | --", close_joined)
        self.assertIn("核按钮率 | --", close_joined)
        self.assertIn("红开率 | --", close_joined)
        self.assertIn("晋级率 | --", ladder_joined)
        self.assertIn("核按钮率 | --", ladder_joined)
        self.assertIn("竞价开局 | 红开率 --，竞价反馈样本不足", story_joined)
        self.assertIn("收盘结果 | --，晋级率 --，核按钮率 --", story_joined)

    def test_opening_validation_score_uses_shared_label_constants(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)

        score = controller._score_opening_validations(
            (
                f"A={controller.OPENING_VALIDATION_TRUE_STRONG}",
                f"B={controller.OPENING_VALIDATION_LOW_OPEN_STRONG}",
                f"C={controller.OPENING_VALIDATION_PULLBACK_REBOUND}",
                f"D={controller.OPENING_VALIDATION_GAP_WEAK}",
                f"E={controller.OPENING_VALIDATION_UNDERTAKE_WEAK}",
                f"F={controller.OPENING_VALIDATION_PENDING}",
            )
        )

        self.assertEqual(score["positive"], 3)
        self.assertEqual(score["negative"], 2)

if __name__ == "__main__":
    unittest.main()
