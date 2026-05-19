from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime

sys.modules.setdefault("talib", types.ModuleType("talib"))
holidays_stub = types.ModuleType("holidays")
holidays_stub.CN = lambda: set()
sys.modules.setdefault("holidays", holidays_stub)

from engine_next.domain.enums import (
    ExposureState,
    FailedPromotionType,
    FeedbackState,
    LeaderTier,
    OperatorStyleHint,
    RunPhase,
    StockArchetype,
    StockStage,
    TradeWindowState,
)
from engine_next.domain.models import (
    AuctionLadderDecision,
    IntradayContext,
    IntradayMarketSummary,
    StockProfileAssessment,
    StockSelectionContext,
    StockStateSnapshot,
    ThemeSelectionContext,
)
from engine_next.runtime.controllers.auction_runtime_controller import AuctionRuntimeController, StrategyConsoleState
from engine_next.runtime.controllers.auction_runtime_controller import AuctionThemeCollisionStat
from engine_next.strategy_skill_layer.auction_and_ladder import build_auction_and_ladder_decision
from engine_next.strategy_skill_layer.context_pipeline import ContextStrategyBundle, filter_watch_candidates


class AuctionRuntimeChainTests(unittest.TestCase):
    def test_auction_runtime_loop_switches_to_formal_phase_once_anchor_is_ready(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        controller._runtime_text = lambda label: label
        controller._render_strategy_view_from_state = (
            lambda state, phase_label, minute_tag=None, historical_only=False: (f"phase={phase_label}",)
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(top_turnover_symbols=(), total_yest_limit_count=81),
                hot_plate_map={},
                yesterday_hot_plate_map={"芯片": {"plate_name": "芯片", "rank": 1}},
                yest_limit_map={"000001": {"symbol": "000001"}},
                auction_map={"000001": {"symbol": "000001", "source": "redis_0925"}},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
        )
        controller._build_console_state = lambda *args, **kwargs: state

        rendered = controller.render_auction_runtime_loop(
            intraday_context=state.context,
            runtime_readiness_label="ready",
            symbols=5201,
            quotes=5201,
            native=5201,
            now=datetime(2026, 5, 19, 9, 26, 0),
            quote_freshness_line=None,
        )

        self.assertIn("phase=auction", rendered)
        self.assertNotIn("phase=auction_preview", rendered)

    def test_auction_structure_hides_partial_amount_bundle_until_atomic_snapshot_is_complete(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(
                    top_turnover_symbols=(),
                    market_full_auc_amt=0.0,
                    context_auc_amt=191.78 * 100000000,
                    auction_top10_amount=31.89 * 100000000,
                    auction_top20_amount=45.47 * 100000000,
                    avg_bid_amt=4033.2 * 10000,
                    total_yest_limit_count=81,
                    hot_plate_count=50,
                ),
                hot_plate_map={"芯片": {"plate_name": "芯片", "rank": 1}},
                yesterday_hot_plate_map={"芯片": {"plate_name": "芯片", "rank": 1}},
                yest_limit_map={"000001": {"symbol": "000001"}},
                auction_map={"000001": {"symbol": "000001", "source": "redis_0925"}},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            missing_inputs=(),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
        )

        rendered = "\n".join(controller._render_auction_structure(state))

        self.assertIn("全市场竞价额 | --", rendered)
        self.assertIn("核心样本竞价额 | --", rendered)
        self.assertIn("Top10竞价额/昨比 | -- / --", rendered)
        self.assertIn("Top20竞价额/昨比 | -- / --", rendered)

    def test_watch_candidates_keep_strong_front_row_when_theme_is_not_tradable(self) -> None:
        snapshot = StockStateSnapshot(
            symbol="000001",
            name="前排强票",
            plate="机器人",
            leader_rank_in_theme=2,
            lb_days=1,
            auction_amount=45_000_000,
            amount_2m=68_000_000,
        )
        profile = StockProfileAssessment(
            symbol="000001",
            archetype=StockArchetype.CORE_TREND,
            leader_tier=LeaderTier.CORE,
            stage=StockStage.CONFIRMATION,
            failed_promotion_type=FailedPromotionType.NONE,
            operator_style_hint=OperatorStyleHint.INSTITUTION,
            feedback_state=FeedbackState.NEUTRAL,
            exposure_state=ExposureState.BALANCED,
            trade_window=TradeWindowState.EARLY_BOARDING,
            darkness_exposure_score=30,
            continuation_score=70,
            retail_attention_proxy=60,
        )
        selection = StockSelectionContext(
            symbol="000001",
            plate_name="机器人",
            is_active_pool=True,
            is_front_row=True,
            leader_bucket="front_row",
            theme_core_score=7.4,
            execution_quality_score=6.0,
            open_undertake_score=5.9,
            shape_quality_score=5.9,
            turnover_quality_score=5.8,
            activity_score=6.6,
            total_score=6.4,
            theme_tradable=False,
            hot_rank=135,
            kline_pattern="platform_breakout",
        )
        theme = ThemeSelectionContext(
            plate_name="机器人",
            tradable=False,
            trade_conclusion="unknown",
            fakeout_level="low",
            x_score=2.2,
        )
        decision = build_auction_and_ladder_decision(
            snapshot,
            profile=profile,
            stock_selection=selection,
            theme_selection=theme,
        )
        bundle = ContextStrategyBundle(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            profiles=(profile,),
            theme_context_map={"机器人": theme},
            stock_selection_contexts=(selection,),
            decisions=(decision,),
            focus_symbols=("000001",),
        )

        watch_candidates = filter_watch_candidates(bundle, min_confidence=60)

        self.assertEqual(decision.action, "observe_only")
        self.assertGreaterEqual(decision.confidence, 60)
        self.assertEqual(tuple(item.symbol for item in watch_candidates), ("000001",))

    def test_display_action_upgrades_strong_watch_only_front_row(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(
            symbol="000001",
            name="前排强票",
            plate="机器人",
            leader_rank_in_theme=2,
            lb_days=1,
            auction_amount=45_000_000,
            amount_2m=68_000_000,
        )
        profile = StockProfileAssessment(
            symbol="000001",
            archetype=StockArchetype.CORE_TREND,
            leader_tier=LeaderTier.CORE,
            stage=StockStage.CONFIRMATION,
            failed_promotion_type=FailedPromotionType.NONE,
            operator_style_hint=OperatorStyleHint.INSTITUTION,
            feedback_state=FeedbackState.NEUTRAL,
            exposure_state=ExposureState.BALANCED,
            trade_window=TradeWindowState.EARLY_BOARDING,
            darkness_exposure_score=30,
            continuation_score=70,
            retail_attention_proxy=60,
        )
        selection = StockSelectionContext(
            symbol="000001",
            plate_name="机器人",
            is_active_pool=True,
            is_front_row=True,
            leader_bucket="front_row",
            theme_core_score=7.4,
            execution_quality_score=6.0,
            open_undertake_score=5.9,
            shape_quality_score=5.9,
            turnover_quality_score=5.8,
            activity_score=6.6,
            total_score=6.4,
            theme_tradable=False,
            hot_rank=135,
            kline_pattern="platform_breakout",
        )
        theme = ThemeSelectionContext(
            plate_name="机器人",
            tradable=False,
            trade_conclusion="unknown",
            fakeout_level="low",
            x_score=2.2,
        )
        decision = build_auction_and_ladder_decision(
            snapshot,
            profile=profile,
            stock_selection=selection,
            theme_selection=theme,
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000001",),
            candidate_scope_set=frozenset({"000001"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={"000001": snapshot},
            stock_name_map={"000001": "前排强票"},
            plate_symbol_map={},
            decision_map={"000001": decision},
            selection_context_map={"000001": selection},
        )

        self.assertEqual(decision.action, "observe_only")
        self.assertEqual(
            controller._display_action_code(decision, state, phase_label="auction"),
            "front_row_watch",
        )
        self.assertEqual(
            controller._display_action_label(decision, state, phase_label="auction"),
            "前排跟踪",
        )

    def test_auction_execution_map_surfaces_watch_track_bucket(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(
            symbol="000001",
            name="前排强票",
            plate="机器人",
            leader_rank_in_theme=2,
            lb_days=1,
            auction_amount=45_000_000,
            amount_2m=68_000_000,
        )
        profile = StockProfileAssessment(
            symbol="000001",
            archetype=StockArchetype.CORE_TREND,
            leader_tier=LeaderTier.CORE,
            stage=StockStage.CONFIRMATION,
            failed_promotion_type=FailedPromotionType.NONE,
            operator_style_hint=OperatorStyleHint.INSTITUTION,
            feedback_state=FeedbackState.NEUTRAL,
            exposure_state=ExposureState.BALANCED,
            trade_window=TradeWindowState.EARLY_BOARDING,
            darkness_exposure_score=30,
            continuation_score=70,
            retail_attention_proxy=60,
        )
        selection = StockSelectionContext(
            symbol="000001",
            plate_name="机器人",
            is_active_pool=True,
            is_front_row=True,
            leader_bucket="front_row",
            theme_core_score=7.4,
            execution_quality_score=6.0,
            open_undertake_score=5.9,
            shape_quality_score=5.9,
            turnover_quality_score=5.8,
            activity_score=6.6,
            total_score=6.4,
            theme_tradable=False,
            hot_rank=135,
            kline_pattern="platform_breakout",
        )
        theme = ThemeSelectionContext(
            plate_name="机器人",
            tradable=False,
            trade_conclusion="unknown",
            fakeout_level="low",
            x_score=2.2,
        )
        decision = build_auction_and_ladder_decision(
            snapshot,
            profile=profile,
            stock_selection=selection,
            theme_selection=theme,
        )
        bundle = ContextStrategyBundle(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            profiles=(profile,),
            theme_context_map={"机器人": theme},
            stock_selection_contexts=(selection,),
            decisions=(decision,),
            focus_symbols=("000001",),
        )
        state = StrategyConsoleState(
            context=bundle.context,
            candidate_scope=("000001",),
            candidate_scope_set=frozenset({"000001"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=bundle,
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={"000001": snapshot},
            stock_name_map={"000001": "前排强票"},
            plate_symbol_map={},
            decision_map={"000001": decision},
            selection_context_map={"000001": selection},
        )

        rendered = "\n".join(controller._render_auction_execution_map(state))

        self.assertIn("跟踪 |", rendered)
        self.assertIn("前排跟踪@", rendered)

    def test_auction_execution_map_allows_repair_bucket_for_observe_only_candidate(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(
            symbol="000002",
            name="修复候选",
            plate="机器人",
            leader_rank_in_theme=4,
            open_pct=0.01,
            current_pct=0.02,
            auction_amount=20_000_000,
            amount_2m=36_000_000,
        )
        decision = AuctionLadderDecision(
            symbol="000002",
            setup_id="",
            action="observe_only",
            confidence=66,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.2,
            profile=StockProfileAssessment(
                symbol="000002",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
            reasons=("repair watch",),
        )
        selection = StockSelectionContext(
            symbol="000002",
            plate_name="机器人",
            is_front_row=False,
            open_undertake_score=6.0,
            execution_quality_score=6.0,
            theme_core_score=6.2,
            total_score=6.0,
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000002",),
            candidate_scope_set=frozenset({"000002"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=ContextStrategyBundle(
                context=IntradayContext(
                    phase=RunPhase.AUCTION,
                    trade_date="2026-05-19",
                    offline_context_date="2026-05-16",
                    stock_snapshots=(snapshot,),
                    market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                    hot_plate_map={},
                    yesterday_hot_plate_map={},
                    yest_limit_map={},
                    auction_map={},
                ),
                profiles=(decision.profile,),
                theme_context_map={},
                stock_selection_contexts=(selection,),
                decisions=(decision,),
                focus_symbols=("000002",),
            ),
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={"000002": snapshot},
            stock_name_map={"000002": "修复候选"},
            plate_symbol_map={},
            decision_map={"000002": decision},
            selection_context_map={"000002": selection},
        )

        controller._focus_ordered_decisions = lambda *_args, **_kwargs: (decision,)
        controller._focus_candidates_for_phase = lambda *_args, **_kwargs: ()
        controller._display_action_code = lambda *_args, **_kwargs: "observe_only"
        controller._display_action_label = lambda *_args, **_kwargs: "只观察"
        controller._selection_is_repair_watch_candidate = lambda **_kwargs: True
        controller._decision_name = lambda _state, _decision: "修复候选"
        controller._is_stock_auction_fakeout = lambda *_args, **_kwargs: False

        rendered = "\n".join(controller._render_auction_execution_map(state))

        self.assertIn("修复预备 | 修复候选:修复预备@66", rendered)

    def test_auction_repair_watch_list_keeps_upgraded_observe_only(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(symbol="000003", name="修复观察", plate="机器人", open_pct=0.01)
        selection = StockSelectionContext(symbol="000003", plate_name="机器人")
        decision = AuctionLadderDecision(
            symbol="000003",
            setup_id="",
            action="observe_only",
            confidence=62,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000003",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000003",),
            candidate_scope_set=frozenset({"000003"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=ContextStrategyBundle(
                context=IntradayContext(
                    phase=RunPhase.AUCTION,
                    trade_date="2026-05-19",
                    offline_context_date="2026-05-16",
                    stock_snapshots=(snapshot,),
                    market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                    hot_plate_map={},
                    yesterday_hot_plate_map={},
                    yest_limit_map={},
                    auction_map={},
                ),
                profiles=(decision.profile,),
                theme_context_map={},
                stock_selection_contexts=(selection,),
                decisions=(decision,),
                focus_symbols=("000003",),
            ),
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={"000003": snapshot},
            stock_name_map={"000003": "修复观察"},
            plate_symbol_map={},
            decision_map={"000003": decision},
            selection_context_map={"000003": selection},
        )
        controller._focus_ordered_decisions = lambda *_args, **_kwargs: (decision,)
        controller._display_action_code = lambda *_args, **_kwargs: "front_row_watch"
        controller._is_stock_auction_fakeout = lambda *_args, **_kwargs: False
        controller._is_low_open_rebound_snapshot = lambda *_args, **_kwargs: True

        rendered = controller._auction_repair_watch_list(state)

        self.assertEqual(len(rendered), 1)
        self.assertIn("修复观察", rendered[0])

    def test_risk_guard_uses_display_downgrade_for_hold_only(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(symbol="000004", name="风险票", plate="芯片")
        decision = AuctionLadderDecision(
            symbol="000004",
            setup_id="",
            action="hold_only",
            confidence=80,
            kelly_position_pct=0.2,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000004",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000004",),
            candidate_scope_set=frozenset({"000004"}),
            actual_source="t1_v2_q2_live",
            plate_stats=(),
            bundle=ContextStrategyBundle(
                context=IntradayContext(
                    phase=RunPhase.INTRADAY,
                    trade_date="2026-05-19",
                    offline_context_date="2026-05-16",
                    stock_snapshots=(snapshot,),
                    market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                    hot_plate_map={},
                    yesterday_hot_plate_map={},
                    yest_limit_map={},
                    auction_map={},
                ),
                profiles=(decision.profile,),
                theme_context_map={},
                stock_selection_contexts=(),
                decisions=(decision,),
                focus_symbols=("000004",),
            ),
            candidates=(),
            watch_candidates=(),
            missing_inputs=(),
            snapshot_map={"000004": snapshot},
            stock_name_map={"000004": "风险票"},
            plate_symbol_map={},
            decision_map={"000004": decision},
        )
        controller._display_action_code = lambda *_args, **_kwargs: "failed_promo_guard"
        controller._decision_name = lambda _state, _decision: "风险票"
        controller._mode_risk_prompt = lambda *_args, **_kwargs: "测试模式"

        rendered = "\n".join(controller._render_risk_guard(state, phase_label="intraday"))

        self.assertIn("风险票:失败回避", rendered)

    def test_opening_validation_uses_watch_candidates_when_trade_candidates_empty(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(
            symbol="000005",
            name="确认观察",
            plate="机器人",
            open_pct=0.01,
            current_pct=0.03,
        )
        decision = AuctionLadderDecision(
            symbol="000005",
            setup_id="",
            action="observe_only",
            confidence=67,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000005",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
            reasons=("watch confirm",),
        )
        selection = StockSelectionContext(symbol="000005", plate_name="机器人")
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000005",),
            candidate_scope_set=frozenset({"000005"}),
            actual_source="t1_v2_q2_live",
            plate_stats=(),
            bundle=ContextStrategyBundle(
                context=IntradayContext(
                    phase=RunPhase.INTRADAY,
                    trade_date="2026-05-19",
                    offline_context_date="2026-05-16",
                    stock_snapshots=(snapshot,),
                    market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                    hot_plate_map={},
                    yesterday_hot_plate_map={},
                    yest_limit_map={},
                    auction_map={},
                ),
                profiles=(decision.profile,),
                theme_context_map={},
                stock_selection_contexts=(selection,),
                decisions=(decision,),
                focus_symbols=("000005",),
            ),
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={"000005": snapshot},
            stock_name_map={"000005": "确认观察"},
            plate_symbol_map={},
            decision_map={"000005": decision},
            selection_context_map={"000005": selection},
        )
        controller._focus_candidates_for_phase = lambda *_args, **_kwargs: ()
        controller._decision_allowed_in_focus_output = lambda *_args, **_kwargs: True
        controller._display_action_label = lambda *_args, **_kwargs: "确认后再做"
        controller._leader_truth_label = lambda *_args, **_kwargs: "真强给机"
        controller._is_low_open_rebound_snapshot = lambda *_args, **_kwargs: False
        controller._pick_auction_outcome_names = lambda *_args, **_kwargs: ()
        controller._expectation_ready = lambda *_args, **_kwargs: False
        controller._effective_money_mode_code = lambda *_args, **_kwargs: "no_clear_edge"
        controller._money_mode_label = lambda *_args, **_kwargs: "无明确模式"
        controller._money_mode_confidence = lambda *_args, **_kwargs: 0.0
        controller._opening_mode_hard_override = lambda **_kwargs: (_kwargs["opening_mode_code"], "")
        controller._validate_auction_mode_with_opening_2m = lambda **_kwargs: ("pending", "-")

        payload = controller._build_opening_validation_payload(state)

        self.assertEqual(len(payload["validated"]), 1)
        self.assertIn("确认后再做/真强给机", payload["validated"][0])

    def test_intraday_focus_candidates_fallback_to_watch_candidates(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        decision = AuctionLadderDecision(
            symbol="000006",
            setup_id="",
            action="observe_only",
            confidence=68,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000006",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="t1_v2_q2_live",
            plate_stats=(),
            bundle=ContextStrategyBundle(
                context=IntradayContext(
                    phase=RunPhase.INTRADAY,
                    trade_date="2026-05-19",
                    offline_context_date="2026-05-16",
                    stock_snapshots=(),
                    market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                    hot_plate_map={},
                    yesterday_hot_plate_map={},
                    yest_limit_map={},
                    auction_map={},
                ),
                profiles=(decision.profile,),
                theme_context_map={},
                stock_selection_contexts=(),
                decisions=(decision,),
                focus_symbols=("000006",),
            ),
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={"000006": decision},
        )
        controller._focus_ordered_decisions = lambda *_args, **_kwargs: (decision,)
        controller._filter_trade_candidates_for_state = lambda *_args, **_kwargs: ()
        controller._decision_allowed_in_focus_output = lambda *_args, **_kwargs: True

        rendered = controller._focus_candidates_for_phase(state, phase_label="intraday")

        self.assertEqual(rendered, (decision,))

    def test_intraday_stale_focus_pool_uses_watch_candidates_when_candidates_empty(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(symbol="000007", name="盘中观察", plate="机器人")
        decision = AuctionLadderDecision(
            symbol="000007",
            setup_id="",
            action="observe_only",
            confidence=66,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000007",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000007",),
            candidate_scope_set=frozenset({"000007"}),
            actual_source="stale_intraday_snapshot",
            plate_stats=(),
            bundle=ContextStrategyBundle(
                context=IntradayContext(
                    phase=RunPhase.INTRADAY,
                    trade_date="2026-05-19",
                    offline_context_date="2026-05-16",
                    stock_snapshots=(snapshot,),
                    market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                    hot_plate_map={},
                    yesterday_hot_plate_map={},
                    yest_limit_map={},
                    auction_map={},
                ),
                profiles=(decision.profile,),
                theme_context_map={},
                stock_selection_contexts=(),
                decisions=(decision,),
                focus_symbols=("000007",),
            ),
            candidates=(),
            watch_candidates=(decision,),
            missing_inputs=(),
            snapshot_map={"000007": snapshot},
            stock_name_map={"000007": "盘中观察"},
            plate_symbol_map={},
            decision_map={"000007": decision},
            stale_snapshot_only=True,
        )
        controller._decision_allowed_in_focus_output = lambda *_args, **_kwargs: True
        controller._format_watch_item = lambda _decision, _snapshot_map, state=None: "盘中观察 | 66 | +0.0% | +0.0% | 机器人"

        rendered = "\n".join(controller._render_focus_pool(state, phase_label="intraday"))

        self.assertIn("盘中观察 | 66 | +0.0% | +0.0% | 机器人", rendered)

    def test_candidate_reason_summary_contains_action_and_score_drivers(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(symbol="000008", name="理由票", plate="机器人")
        decision = AuctionLadderDecision(
            symbol="000008",
            setup_id="",
            action="observe_only",
            confidence=70,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000008",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
            reasons=("within first echelon of the theme",),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000008",),
            candidate_scope_set=frozenset({"000008"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            watch_candidates=(),
            missing_inputs=(),
            snapshot_map={"000008": snapshot},
            stock_name_map={"000008": "理由票"},
            plate_symbol_map={},
            decision_map={"000008": decision},
        )
        controller._display_action_label = lambda *_args, **_kwargs: "前排跟踪"
        controller._focus_candidate_score_breakdown = lambda *_args, **_kwargs: {
            "base": 70.0,
            "selection": 12.0,
            "judge": 6.0,
            "opening": 0.0,
            "action": 0.0,
            "total": 88.0,
        }

        rendered = controller._candidate_reason_summary(state, decision, phase_label="auction")

        self.assertIn("前排跟踪", rendered)
        self.assertIn("位于题材第一梯队，值得继续盯。", rendered)
        self.assertIn("驱动=", rendered)
        self.assertIn("个股承接强", rendered)

    def test_candidate_reason_summary_can_render_collision_style_driver_text(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(symbol="000010", name="对撞票", plate="通信")
        decision = AuctionLadderDecision(
            symbol="000010",
            setup_id="",
            action="observe_only",
            confidence=72,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000010",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
            reasons=("within first echelon of the theme",),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000010",),
            candidate_scope_set=frozenset({"000010"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            watch_candidates=(),
            missing_inputs=(),
            snapshot_map={"000010": snapshot},
            stock_name_map={"000010": "对撞票"},
            plate_symbol_map={},
            decision_map={"000010": decision},
        )
        controller._display_action_label = lambda *_args, **_kwargs: "前排跟踪"
        controller._focus_candidate_score_breakdown = lambda *_args, **_kwargs: {
            "base": 72.0,
            "selection": 6.0,
            "judge": 9.0,
            "opening": 0.0,
            "action": 0.0,
            "total": 87.0,
        }
        controller._snapshot_theme_collision = lambda *_args, **_kwargs: AuctionThemeCollisionStat(
            plate_name="通信",
            row=None,  # type: ignore[arg-type]
            capital_rank=1,
            limitup_rank=5,
            turn_rank=5,
            hot_rank=2,
            yesterday_hot_rank=3,
            continuation_rank=4,
            collision_score=0.0,
            expectation_score=0.0,
            expectation_delta=0.0,
            expectation_label="局部超预期",
            signal="有量无板",
        )

        rendered = controller._candidate_reason_summary(state, decision, phase_label="auction")

        self.assertIn("有量无板仅局部验证", rendered)

    def test_reject_reason_summary_contains_action_and_primary_secondary_reasons(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        snapshot = StockStateSnapshot(symbol="000009", name="淘汰票", plate="芯片")
        decision = AuctionLadderDecision(
            symbol="000009",
            setup_id="",
            action="observe_only",
            confidence=55,
            kelly_position_pct=0.1,
            risk_reward_ratio=1.0,
            profile=StockProfileAssessment(
                symbol="000009",
                archetype=StockArchetype.CORE_TREND,
                leader_tier=LeaderTier.CORE,
                stage=StockStage.CONFIRMATION,
                failed_promotion_type=FailedPromotionType.NONE,
                operator_style_hint=OperatorStyleHint.INSTITUTION,
                feedback_state=FeedbackState.NEUTRAL,
                exposure_state=ExposureState.BALANCED,
                trade_window=TradeWindowState.EARLY_BOARDING,
                darkness_exposure_score=20,
                continuation_score=60,
                retail_attention_proxy=40,
            ),
        )
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.AUCTION,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(snapshot,),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=("000009",),
            candidate_scope_set=frozenset({"000009"}),
            actual_source="redis_0925",
            plate_stats=(),
            bundle=None,
            candidates=(),
            watch_candidates=(),
            missing_inputs=(),
            snapshot_map={"000009": snapshot},
            stock_name_map={"000009": "淘汰票"},
            plate_symbol_map={},
            decision_map={"000009": decision},
        )
        controller._display_action_label = lambda *_args, **_kwargs: "只观察"

        rendered = controller._reject_reason_summary(
            state,
            decision=decision,
            snapshot=snapshot,
            plate="芯片",
            reasons=("开盘承接偏弱", "热榜位次靠后"),
            phase_label="auction",
        )

        self.assertIn("只观察|开盘承接偏弱/热榜位次靠后", rendered)

    def test_mode_risk_prompt_adds_intraday_structure_counts(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="t1_v2_q2_live",
            plate_stats=(),
            bundle=None,
            candidates=(),
            watch_candidates=(),
            missing_inputs=(),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
            selection_context_map={
                "a": StockSelectionContext(symbol="a", open_follow_state="repair_strength"),
                "b": StockSelectionContext(symbol="b", open_follow_state="faded"),
                "c": StockSelectionContext(symbol="c", open_follow_state="faded"),
            },
        )
        controller._effective_money_mode_code = lambda *_args, **_kwargs: "no_clear_edge"
        controller._money_mode_label = lambda *_args, **_kwargs: "无明确模式"

        rendered = controller._mode_risk_prompt(state, phase_label="intraday")

        self.assertIn("盘中结构=修复1/掉队2", rendered)

    def test_render_market_narrative_uses_fixed_story_skeleton(self) -> None:
        controller = AuctionRuntimeController.__new__(AuctionRuntimeController)
        state = StrategyConsoleState(
            context=IntradayContext(
                phase=RunPhase.INTRADAY,
                trade_date="2026-05-19",
                offline_context_date="2026-05-16",
                stock_snapshots=(),
                market_summary=IntradayMarketSummary(top_turnover_symbols=()),
                hot_plate_map={},
                yesterday_hot_plate_map={},
                yest_limit_map={},
                auction_map={},
            ),
            candidate_scope=(),
            candidate_scope_set=frozenset(),
            actual_source="t1_v2_q2_live",
            plate_stats=(),
            bundle=None,
            candidates=(),
            watch_candidates=(),
            missing_inputs=(),
            snapshot_map={},
            stock_name_map={},
            plate_symbol_map={},
            decision_map={},
        )
        controller._narrative_current_trade_text = lambda *_args, **_kwargs: "电力，主攻强化"
        controller._narrative_previous_hypothesis_text = lambda *_args, **_kwargs: "通信=有量无板/局部超预期/前排确认"
        controller._narrative_validation_text = lambda *_args, **_kwargs: "部分成立 | 开盘后切到电力"
        controller._narrative_switch_text = lambda *_args, **_kwargs: "资金从通信分流到电力，只保留前排有效承接"
        controller._narrative_focus_themes_text = lambda *_args, **_kwargs: "电力 / 算力"
        controller._narrative_focus_stocks_text = lambda *_args, **_kwargs: "晋控电力=题材首板 ; 大唐发电=龙头持有"
        controller._narrative_avoid_text = lambda *_args, **_kwargs: "通信杂毛=禁止追高"

        rendered = controller._render_market_narrative(state, phase_label="intraday")

        self.assertEqual(rendered[0], "【主叙事】维度 | 内容")
        self.assertIn("市场在交易什么 | 电力，主攻强化", rendered[1])
        self.assertIn("上一阶段判断 | 通信=有量无板/局部超预期/前排确认", rendered[2])
        self.assertIn("切换说明 | 资金从通信分流到电力，只保留前排有效承接", rendered[4])
        self.assertIn("当前回避方向 | 通信杂毛=禁止追高", rendered[7])
