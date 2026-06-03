from __future__ import annotations

import json
import logging
import math
import re
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from heapq import nlargest
from typing import Iterable

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import (
    AuctionLadderDecision,
    IntradayContext,
    StartupSelfCheckReport,
    StockSelectionContext,
    StockStateSnapshot,
    ThemeSelectionContext,
)
from engine_next.runtime.intraday_data_hub import IntradayDataHub, IntradayFetchResult, normalize_auction_pct_ratio
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    collapse_runtime_primary_plate,
    choose_primary_plate,
    decode_theme_list,
    is_generic_plate,
    normalize_plate_name,
)
from engine_next.runtime.market_runtime_summary import MarketRuntimeSummaryResult, MarketRuntimeSummaryService
from engine_next.runtime.session_facts import build_session_facts
from engine_next.strategy_skill_layer.auction_plate_buckets import (
    AuctionPlateBucketStat,
    AuctionSnapshotDeltaStat,
    build_auction_snapshot_delta_stats,
    build_auction_plate_bucket_stats,
)
from engine_next.strategy_skill_layer.context_pipeline import (
    ContextStrategyBundle,
    build_context_strategy_bundle_for_symbols,
)
from engine_next.strategy_skill_layer.opening_validation_hub import (
    build_opening_validation_bundle,
    match_opening_validation,
)
from engine_next.strategy_skill_layer.playbook_runtime_rank import (
    classify_playbook_theme_tier,
)
from engine_next.strategy_skill_layer.playbook_runtime_gate import (
    has_playbook_non_hot_strength,
    is_playbook_stock_auction_fakeout,
)
from engine_next.strategy_skill_layer.shape_engine import filter_shape_eval_scope
from engine_next.strategy_skill_layer.slice_comparison import (
    build_market_topn_slice_comparison,
    build_opening_2m_slice_comparison,
)
from engine_next.strategy_skill_layer.trap_guards import is_high_dayk_weak_trap
from engine_next.strategy_skill_layer.theme_selection_context_factory import (
    build_theme_selection_context,
    normalize_theme_fakeout_level,
    resolve_theme_trade_conclusion,
)
from engine_next.strategy_skill_layer.theme_trade_labels import classify_theme_trade_label_from_collision
from web.services.trading_calendar_service import TradingCalendarService


logger = logging.getLogger(__name__)

_TRAILING_NAME_NOISE_RE = re.compile(r"[A-Za-z0-9]+$")


@dataclass(frozen=True)
class AuctionReplayResult:
    executed: bool
    notes: tuple[str, ...] = ()
    yest_limit_result: IntradayFetchResult | None = None
    hot_plate_result: IntradayFetchResult | None = None
    auction_result: IntradayFetchResult | None = None
    market_runtime_summary_result: MarketRuntimeSummaryResult | None = None


@dataclass(frozen=True)
class StrategyConsoleState:
    context: IntradayContext
    coverage_scope: tuple[str, ...]
    coverage_scope_set: frozenset[str]
    actual_source: str
    plate_stats: tuple[AuctionPlateBucketStat, ...]
    bundle: ContextStrategyBundle | None
    playbook_decisions: tuple[AuctionLadderDecision, ...]
    missing_inputs: tuple[str, ...]
    snapshot_map: dict[str, StockStateSnapshot]
    stock_name_map: dict[str, str]
    plate_symbol_map: dict[str, tuple[str, ...]]
    decision_map: dict[str, AuctionLadderDecision]
    full_plate_stats: tuple[AuctionPlateBucketStat, ...] = ()
    historical_only: bool = False
    stale_snapshot_only: bool = False
    frozen_postmarket_snapshot: bool = False
    collision_rows: tuple["AuctionThemeCollisionStat", ...] = ()
    auction_delta_stats: tuple[AuctionSnapshotDeltaStat, ...] = ()
    theme_judge_map: dict[str, "ThemeJudgeResult"] | None = None
    selection_context_map: dict[str, StockSelectionContext] | None = None
    theme_collision_map: dict[str, "AuctionThemeCollisionStat"] | None = None
    normalized_plate_names_map: dict[str, tuple[str, ...]] | None = None
    matched_theme_judge_map: dict[str, tuple["ThemeJudgeResult | None", str]] | None = None


@dataclass(frozen=True)
class AuctionThemeCollisionStat:
    plate_name: str
    row: AuctionPlateBucketStat
    capital_rank: int
    limitup_rank: int
    turn_rank: int
    hot_rank: int
    yesterday_hot_rank: int
    continuation_rank: int
    collision_score: float
    expectation_score: float
    expectation_delta: float
    expectation_label: str
    signal: str
    e_score: float = 0.0
    a_score: float = 0.0
    x_score: float = 0.0
    eax_label: str = ""
    eax_action: str = ""
    fakeout_level: str = "none"


@dataclass(frozen=True)
class ThemeJudgeResult:
    plate_name: str
    opportunity_score: float
    trap_score: float
    validation_state: str
    action_class: str
    signal: str
    expectation_label: str
    eax_label: str
    eax_action: str
    notes: tuple[str, ...] = ()


class AuctionRuntimeController:
    """Owns auction/opening/intraday strategy-console rendering."""

    AUCTION_TOP_AMOUNT_LIMIT = 1000
    SHAPE_EVAL_SCOPE_BASE_LIMIT = 160
    SHAPE_EVAL_SCOPE_MAX_LIMIT = 320
    FOCUS_FALLBACK_LIMIT = 2
    AUCTION_MIN_OUTPUT_COUNT = 2
    AUCTION_SNAPSHOT_CACHE_TTL_SECONDS = 1.0
    OPENING_VALIDATION_TTL_SECONDS = 3 * 24 * 60 * 60
    OPENING_VALIDATION_TRUE_STRONG = "真强给机"
    OPENING_VALIDATION_GAP_WEAK = "高开转虚"
    OPENING_VALIDATION_HARD_TO_CHASE = "顶强难接"
    OPENING_VALIDATION_LOW_OPEN_STRONG = "低开真强"
    OPENING_VALIDATION_PULLBACK_REBOUND = "分歧回拉"
    OPENING_VALIDATION_UNDERTAKE_WEAK = "承接偏弱"
    OPENING_VALIDATION_PENDING = "强弱待判"
    MONEY_MODE_LABELS = {
        "high_board_huddle": "高位抱团",
        "mid_rank_promotion": "中位晋级",
        "first_board_expansion": "首板扩散",
        "large_cap_trend": "大票趋势",
        "repair_reversal": "修复反包",
        "no_clear_edge": "无明确模式",
    }
    MONEY_MODE_CONSTRAINTS = {
        "high_board_huddle": "只看龙头活口，不做后排扩散",
        "mid_rank_promotion": "只做前排换手晋级，不追一致后排",
        "first_board_expansion": "只看板块前排首板，不做孤立票",
        "large_cap_trend": "只看容量核心，不做小票接力幻想",
        "repair_reversal": "只做低开转强修复，不做高开兑现",
        "no_clear_edge": "等待确认，宁可少做也不乱做",
    }
    OPENING_VALIDATION_POSITIVE_LABELS = frozenset(
        {
            OPENING_VALIDATION_TRUE_STRONG,
            OPENING_VALIDATION_LOW_OPEN_STRONG,
            OPENING_VALIDATION_PULLBACK_REBOUND,
        }
    )
    OPENING_VALIDATION_NEGATIVE_LABELS = frozenset(
        {
            OPENING_VALIDATION_GAP_WEAK,
            OPENING_VALIDATION_UNDERTAKE_WEAK,
        }
    )
    AUCTION_MIN_AMOUNT = 5_000_000.0
    OPENING_CANDIDATE_MIN_CONFIDENCE = 58
    INTRADAY_CANDIDATE_MIN_CONFIDENCE = 60

    ACTION_LABELS = {
        "dragon_early_board": "dragon_board",
        "early_boarding_candidate": "theme_first_board",
        "hold_only": "leader_hold",
        "small_probe_only": "ice_probe",
        "observe_only": "observe_only",
        "avoid_after_failed_promotion": "failed_promo_guard",
        "do_not_chase": "do_not_chase",
        "leader_watch": "leader_watch",
        "front_row_watch": "front_row_watch",
        "confirm_then_go": "confirm_then_go",
    }

    EXPECTATION_LABELS = {
        "mainline_attack": "attack",
        "hot_follow": "follow",
        "distribution": "distribution",
        "ladder_extension": "ladder",
        "cluster_move": "cluster",
        "observe": "observe",
        "noise": "noise",
    }

    def __init__(
        self,
        *,
        intraday_hub: IntradayDataHub | None = None,
        market_runtime_summary_service: MarketRuntimeSummaryService | None = None,
    ) -> None:
        self._intraday_hub = intraday_hub or IntradayDataHub()
        self._market_runtime_summary_service = market_runtime_summary_service or MarketRuntimeSummaryService(
            redis_client=self._intraday_hub.redis
        )
        self._postmarket_limit_truth_cache: dict[str, tuple[dict[str, object], ...]] = {}
        self._postmarket_limit_truth_enriched_dates: set[str] = set()
        self._auction_snapshot_cache: dict[str, tuple[float, IntradayFetchResult]] = {}

    @staticmethod
    def _opening_validation_redis_key(trade_date: str) -> str:
        return f"market:opening:validation:{str(trade_date or '').replace('-', '')}"

    @staticmethod
    def _redis_set_with_optional_ttl(redis_client, key: str, value: str, *, ttl_seconds: int | None = None) -> None:
        if ttl_seconds is None:
            redis_client.set(key, value)
            return
        try:
            redis_client.set(key, value, ex=ttl_seconds)
        except TypeError:
            redis_client.set(key, value)

    def _load_opening_validation_payload(self, trade_date: str) -> dict[str, object]:
        redis_client = getattr(getattr(self, "_intraday_hub", None), "redis", None)
        if redis_client is None:
            return {}
        key = self._opening_validation_redis_key(trade_date)
        try:
            raw = redis_client.get(key)
        except Exception:
            return {}
        if not raw:
            return {}
        if isinstance(raw, dict):
            return dict(raw)
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def execute_auction_finalize_0925(
        self,
        *,
        trade_date: str,
        previous_trade_date: str,
        offline_context_date: str,
    ) -> AuctionReplayResult:
        logger.info("scheduled event execute | name=auction_finalize_0925")
        yest_limit_result = self._intraday_hub.fetch_yest_limit_pool(previous_trade_date, RunPhase.AUCTION)
        hot_plate_result = self._intraday_hub.fetch_hot_plates(trade_date, RunPhase.AUCTION, today_mode=True)
        auction_result = self._intraday_hub.recover_auction_anchor(trade_date, RunPhase.AUCTION)
        market_runtime_summary_result = self._market_runtime_summary_service.get_or_build(
            trade_date,
            offline_context_date=offline_context_date,
            force_rebuild=True,
        )
        return AuctionReplayResult(
            executed=True,
            notes=("09:25 finalize refreshed yesterday limit pool, today hot plates, auction anchor, and market runtime summary.",),
            yest_limit_result=yest_limit_result,
            hot_plate_result=hot_plate_result,
            auction_result=auction_result,
            market_runtime_summary_result=market_runtime_summary_result,
        )

    def execute_auction_followup_0926(
        self,
        *,
        trade_date: str,
        previous_trade_date: str,
        offline_context_date: str,
    ) -> AuctionReplayResult:
        logger.info("scheduled event execute | name=auction_followup_0926")
        yest_limit_result = self._intraday_hub.fetch_yest_limit_pool(previous_trade_date, RunPhase.AUCTION)
        hot_plate_result = self._intraday_hub.fetch_hot_plates(trade_date, RunPhase.AUCTION, today_mode=True)
        auction_result = None
        anchor_key = f"market:auction:anchor:{trade_date.replace('-', '')}"
        if not self._intraday_hub.redis.get(anchor_key):
            auction_result = self._intraday_hub.recover_auction_anchor(trade_date, RunPhase.AUCTION)
        market_runtime_summary_result = self._market_runtime_summary_service.get_or_build(
            trade_date,
            offline_context_date=offline_context_date,
            force_rebuild=True,
        )
        notes = ["09:26 follow-up refreshed yesterday limit pool, today hot plates, and market runtime summary."]
        if auction_result is not None and auction_result.rows:
            notes.append("09:26 follow-up also recovered missing auction anchor.")
        return AuctionReplayResult(
            executed=True,
            notes=tuple(notes),
            yest_limit_result=yest_limit_result,
            hot_plate_result=hot_plate_result,
            auction_result=auction_result,
            market_runtime_summary_result=market_runtime_summary_result,
        )

    def render_auction_view(self, intraday_context: IntradayContext | None) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        return self._render_strategy_view(
            intraday_context,
            phase_label="auction",
            min_confidence=60,
        )

    def render_auction_preview_view(
        self,
        intraday_context: IntradayContext | None,
        *,
        now: datetime,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        return self._render_strategy_view(
            intraday_context,
            phase_label="auction_preview",
            minute_tag=now.strftime("%H:%M"),
            min_confidence=60,
        )

    def render_opening_view(
        self,
        intraday_context: IntradayContext | None,
        *,
        now: datetime,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        return self._render_strategy_view(
            intraday_context,
            phase_label="open_confirm",
            minute_tag=now.strftime("%H:%M"),
            min_confidence=self.OPENING_CANDIDATE_MIN_CONFIDENCE,
        )

    def persist_opening_validation_checkpoint(
        self,
        *,
        trade_date: str,
        intraday_context: IntradayContext | None,
        now: datetime,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return ("opening_validation_checkpoint skipped: intraday_context missing.",)
        state = self._build_console_state(
            intraday_context,
            min_confidence=self.OPENING_CANDIDATE_MIN_CONFIDENCE,
            phase_label="open_confirm",
        )
        payload = self._build_opening_validation_payload(state, now=now)
        key = self._opening_validation_redis_key(trade_date)
        latest_key = "market:opening:validation:latest"
        raw = json.dumps(payload, ensure_ascii=False)
        self._redis_set_with_optional_ttl(
            self._intraday_hub.redis,
            key,
            raw,
            ttl_seconds=self.OPENING_VALIDATION_TTL_SECONDS,
        )
        self._redis_set_with_optional_ttl(
            self._intraday_hub.redis,
            latest_key,
            raw,
            ttl_seconds=self.OPENING_VALIDATION_TTL_SECONDS,
        )
        return (
            "opening_validation_checkpoint persisted",
            f"opening_validation_key={key}",
        )

    def has_opening_validation_checkpoint(self, trade_date: str) -> bool:
        payload = self._load_opening_validation_payload(trade_date)
        return bool(payload.get("updated_at_ts"))

    def render_intraday_view(
        self,
        intraday_context: IntradayContext | None,
        *,
        now: datetime,
        stale_snapshot_only: bool = False,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        return self._render_strategy_view(
            intraday_context,
            phase_label="intraday",
            minute_tag=now.strftime("%H:%M"),
            min_confidence=self.INTRADAY_CANDIDATE_MIN_CONFIDENCE,
            stale_snapshot_only=stale_snapshot_only,
        )

    def render_premarket_view(
        self,
        intraday_context: IntradayContext | None,
        *,
        now: datetime,
        startup_report: StartupSelfCheckReport | None = None,
        historical_only: bool = False,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        return self._render_strategy_view(
            intraday_context,
            phase_label="premarket",
            minute_tag=now.strftime("%H:%M"),
            min_confidence=self.INTRADAY_CANDIDATE_MIN_CONFIDENCE,
            startup_report=startup_report,
            historical_only=historical_only,
        )

    def render_postmarket_view(
        self,
        intraday_context: IntradayContext | None,
        *,
        now: datetime,
    ) -> tuple[str, ...]:
        if intraday_context is None:
            return ()
        return self._render_strategy_view(
            intraday_context,
            phase_label="postmarket",
            minute_tag=now.strftime("%H:%M"),
            min_confidence=self.INTRADAY_CANDIDATE_MIN_CONFIDENCE,
        )

    def render_auction_takeover(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        native: int,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        lines = [
            "运行事件=竞价接管",
            (
                f"运行状态={self._runtime_text(runtime_readiness_label)} "
                f"| 行情={quotes}/{symbols} "
                f"| Native={native} "
                f"| 竞价时窗=09:25-09:30"
            ),
        ]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        lines.extend(self.render_auction_view(intraday_context))
        return tuple(lines)

    def render_auction_runtime_loop(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        native: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        settling_mode = now.strftime("%H:%M:%S") < "09:25:10"
        preview_mode = now.strftime("%H:%M") < "09:25"
        if settling_mode:
            summary_lines = [f"runtime_readiness={self._runtime_text(runtime_readiness_label)} | quotes={quotes}/{symbols} | Native={native}"]
            if quote_freshness_line:
                summary_lines.append(quote_freshness_line)
            summary_lines.append("auction_anchor | status=waiting_finalization | earliest=09:25:10 | action=hold_formal_analysis")
            return tuple(summary_lines)
        if settling_mode:
            lines = [f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Native={native}"]
            if quote_freshness_line:
                lines.append(quote_freshness_line)
            lines.append("auction_anchor | status=waiting_finalization | earliest=09:25:10 | action=hold_formal_analysis")
            return tuple(lines)
        if intraday_context is None:
            lines = [
                f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Native={native}",
                "竞价预热上下文=加载中" if preview_mode else "竞价上下文=加载中",
            ]
            if quote_freshness_line:
                lines.insert(1, quote_freshness_line)
            return tuple(lines)
        lines = [f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Native={native}"]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        state = self._build_console_state(
            intraday_context,
            min_confidence=60,
            phase_label="auction",
        )
        if preview_mode:
            lines.extend(self._render_strategy_view_from_state(state, phase_label="auction_preview", minute_tag=None))
            return tuple(lines)
        if not self._auction_anchor_ready(state):
            lines.append("auction_anchor | status=awaiting_anchor | action=hold_preview_until_anchor_ready")
            lines.extend(self._render_strategy_view_from_state(state, phase_label="auction_preview", minute_tag=None))
            return tuple(lines)
        if not self._expectation_ready(state):
            lines.append("auction_context | status=partial_ready | action=render_formal_auction_with_pending_expectation")
        elif not self._auction_metrics_atomic_ready(state):
            lines.append("auction_metrics | status=pending_atomic_snapshot | action=hide_partial_amount_bundle")
        lines.extend(self._render_strategy_view_from_state(state, phase_label="auction", minute_tag=None))
        return tuple(lines)

    def render_opening_runtime_loop(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        native: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        header = f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Native={native}"
        if intraday_context is None:
            lines = [header, "开盘上下文=加载中"]
            if quote_freshness_line:
                lines.insert(1, quote_freshness_line)
            return tuple(lines)
        lines = [header]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        lines.extend(self.render_opening_view(intraday_context, now=now))
        return tuple(lines)

    def render_intraday_runtime_loop(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        native: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        header = f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Native={native}"
        if intraday_context is None:
            lines = [header, "盘中上下文=加载中"]
            if quote_freshness_line:
                lines.insert(1, quote_freshness_line)
            return tuple(lines)
        lines = [header]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        lines.extend(
            self.render_intraday_view(
                intraday_context,
                now=now,
                stale_snapshot_only=(runtime_readiness_label in {"observe_runtime", "degraded_runtime"}),
            )
        )
        return tuple(lines)

    def render_postmarket_runtime_loop(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        native: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        frozen_snapshot = False
        if intraday_context is not None:
            frozen_snapshot = self._is_frozen_postmarket_context(intraday_context)
        runtime_text = "冻结复盘中" if frozen_snapshot else self._runtime_text(runtime_readiness_label)
        header = (
            f"运行状态={runtime_text} "
            f"| 行情={quotes}/{symbols} "
            f"| Native={native} "
            f"| 结算时窗=17:40+"
        )
        if intraday_context is None:
            lines = [header, "盘后上下文=加载中"]
            if quote_freshness_line:
                lines.insert(1, quote_freshness_line)
            return tuple(lines)
        lines = [header]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        lines.extend(self.render_postmarket_view(intraday_context, now=now))
        return tuple(lines)

    def render_premarket_runtime_loop(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        native: int,
        now: datetime,
        quote_freshness_line: str | None = None,
        startup_report: StartupSelfCheckReport | None = None,
        historical_only: bool = False,
    ) -> tuple[str, ...]:
        header = f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Native={native}"
        if intraday_context is None:
            lines = [header, "盘前上下文=加载中"]
            if quote_freshness_line:
                lines.insert(1, quote_freshness_line)
            return tuple(lines)
        lines = [header]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        lines.extend(
            self.render_premarket_view(
                intraday_context,
                now=now,
                startup_report=startup_report,
                historical_only=historical_only,
            )
        )
        return tuple(lines)

    def _is_premarket_plan_mode(
        self,
        *,
        phase_label: str,
        minute_tag: str | None,
        historical_only: bool,
    ) -> bool:
        return (
            phase_label == "premarket"
            and bool(minute_tag)
            and str(minute_tag) < "09:15"
        )

    def _render_strategy_view(
        self,
        intraday_context: IntradayContext,
        *,
        phase_label: str,
        minute_tag: str | None = None,
        min_confidence: int,
        startup_report: StartupSelfCheckReport | None = None,
        historical_only: bool = False,
        stale_snapshot_only: bool = False,
    ) -> tuple[str, ...]:
        state = self._build_console_state(
            intraday_context,
            min_confidence=min_confidence,
            phase_label=phase_label,
            minute_tag=minute_tag,
            startup_report=startup_report,
            historical_only=historical_only,
            stale_snapshot_only=stale_snapshot_only,
        )
        return self._render_strategy_view_from_state(
            state,
            phase_label=phase_label,
            minute_tag=minute_tag,
            historical_only=historical_only,
        )

    def _render_strategy_view_from_state(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
        minute_tag: str | None = None,
        historical_only: bool = False,
    ) -> tuple[str, ...]:
        self._current_eval_context = state.context
        detail_phase_label = "intraday" if phase_label == "postmarket" and state.frozen_postmarket_snapshot else phase_label
        premarket_plan_mode = self._is_premarket_plan_mode(
            phase_label=phase_label,
            minute_tag=minute_tag,
            historical_only=historical_only,
        )
        regime_phase_label = phase_label if state.frozen_postmarket_snapshot else detail_phase_label
        window = "00:00-09:14" if premarket_plan_mode else self._phase_window_label(phase_label)
        lines = [
            (
                f"策略看板 | 阶段={self._phase_text(phase_label)} "
                f"| 时窗={window} "
                f"| 时间={minute_tag or '-'} "
                f"| 样本={len(state.coverage_scope)}"
            ),
            self._render_recap_market_regime(state, phase_label=phase_label)
            if premarket_plan_mode
            else self._render_market_regime(state, phase_label=regime_phase_label),
        ]
        if premarket_plan_mode:
            lines.append(self._premarket_plan_text(minute_tag))
            lines.extend(self._render_recap_close_recap(state, phase_label=phase_label))
            lines.extend(self._render_recap_mainline_recap(state, phase_label=phase_label))
            lines.extend(self._render_recap_limitup_plate_board(state, phase_label=phase_label))
            lines.extend(self._render_auction_outcome(state))
            lines.extend(self._render_recap_chance_board(state, phase_label=phase_label))
            lines.extend(self._render_recap_plan_review(state, phase_label=phase_label))
            lines.extend(self._render_recap_ladder_recap(state, phase_label=phase_label))
            lines.extend(self._render_ladder_map(state))
            lines.extend(self._render_tomorrow_plan(state))
            lines.append("【核心观察池】09:15前仅做复盘预案，不跑盘前个股筛选")
            lines.extend(self._render_risk_guard(state, phase_label="premarket"))
            return tuple(lines)
        if phase_label == "postmarket" and state.frozen_postmarket_snapshot:
            lines.append("冻结说明 | 当前仅有盘中冻结快照，先做过渡复盘；正式结算完成后，再切正式收盘结论。")
            lines.extend(self._render_close_recap(state))
            lines.extend(self._render_day_recap_story(state))
            lines.extend(self._render_mainline_recap(state))
            lines.extend(self._render_today_hot_plates(state))
            lines.extend(self._render_limitup_plate_board(state))
            lines.extend(self._render_auction_outcome(state))
            lines.extend(self._render_recap_chance_board(state, phase_label=phase_label))
            lines.extend(self._render_recap_plan_review(state, phase_label=phase_label))
            lines.extend(self._render_recap_ladder_recap(state, phase_label=phase_label))
            lines.extend(self._render_ladder_map(state))
            lines.extend(self._render_tomorrow_plan(state))
            lines.extend(self._render_playbook_pool(state, phase_label="postmarket"))
            lines.extend(self._render_risk_guard(state, phase_label="postmarket"))
            return tuple(lines)
        lines.extend(self._render_market_narrative(state, phase_label=detail_phase_label))
        lines.extend(self._render_temporal_migration_chain(state))
        if phase_label in {"auction", "auction_preview"}:
            lines.extend(self._render_yest_limit_breakdown(state))
        if phase_label == "open_confirm":
            lines.extend(self._render_opening_validation_hub(state))
        if phase_label == "postmarket" and not state.frozen_postmarket_snapshot:
            lines.extend(self._render_close_recap(state))
            lines.extend(self._render_day_recap_story(state))
            lines.extend(self._render_mainline_recap(state))
            lines.extend(self._render_today_hot_plates(state))
            lines.extend(self._render_limitup_plate_board(state))
            lines.extend(self._render_auction_outcome(state))
            lines.extend(self._render_ladder_recap(state))
            lines.extend(self._render_yest_limit_breakdown(state))
            lines.extend(self._render_tomorrow_plan(state))
        lines.extend(self._render_ladder_map(state))
        if phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}:
            lines.extend(self._render_auction_execution_map(state, phase_label=detail_phase_label))
        lines.extend(self._render_playbook_pool(state, phase_label=detail_phase_label))
        lines.extend(self._render_risk_guard(state, phase_label=detail_phase_label))
        return tuple(lines)

    def _render_market_narrative(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        if phase_label in {"opening", "open_confirm", "intraday"}:
            rows = [
                "【主叙事】维度 | 内容",
                f"  主结论 | {self._narrative_current_trade_text(state, phase_label=phase_label)}",
                f"  当前环境 | {self._narrative_validation_text(state, phase_label=phase_label)}",
                f"  当前机会 | {self._narrative_opportunity_focus_text(state, phase_label=phase_label)}",
                f"  当前风险 | {self._narrative_avoid_text(state, phase_label=phase_label)}",
            ]
            global_decision = self._global_market_decision_for_state(state)
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or "")) if global_decision is not None else ""
            watch_text = self._narrative_watch_focus_text(state)
            if not main_attack and watch_text and watch_text != "-":
                rows.append(f"  补充观察 | {watch_text}")
            return tuple(rows)
        return (
            "【主叙事】维度 | 内容",
            f"  市场在交易什么 | {self._narrative_current_trade_text(state, phase_label=phase_label)}",
            f"  此前预判什么 | {self._narrative_previous_hypothesis_text(state, phase_label=phase_label)}",
            f"  当前验证结果 | {self._narrative_validation_text(state, phase_label=phase_label)}",
            f"  切换说明 | {self._narrative_switch_text(state, phase_label=phase_label)}",
            f"  当前聚焦题材 | {self._narrative_playbook_themes_text(state, phase_label=phase_label)}",
            f"  当前机会锚点 | {self._narrative_opportunity_anchor_text(state)}",
            f"  当前回避方向 | {self._narrative_avoid_text(state, phase_label=phase_label)}",
        )

    def _render_temporal_migration_chain(self, state: StrategyConsoleState) -> tuple[str, ...]:
        decision_bundle = state.bundle.decision_bundle if state.bundle is not None else None
        temporal = decision_bundle.temporal_migration_decision if decision_bundle is not None else None
        if temporal is None:
            return ("【时间迁移】暂无多粒度迁移证据",)
        invalidation_text = " / ".join(
            text
            for text in (
                self._invalidation_point_text(point)
                for point in tuple(getattr(temporal.trace, "invalidation_points", ()) or ())[:3]
            )
            if text
        ) or "-"
        memory_lines = [
            self._temporal_memory_line_text(line)
            for line in tuple(getattr(temporal, "memory_lines", ()) or ())
            if self._temporal_memory_line_text(line)
        ]
        rows = [
            "【时间迁移】维度 | 结论",
            f"  过程状态 | {self._temporal_exchange_state_text(str(temporal.exchange_state or ''))}",
            f"  资金迁出 | {', '.join(tuple(temporal.source_themes or ())[:3]) or '-'}",
            f"  资金迁入 | {', '.join(tuple(temporal.target_themes or ())[:3]) or '-'}",
            f"  衰竭方向 | {', '.join(tuple(temporal.fading_themes or ())[:3]) or '-'}",
            f"  主观察锚 | {temporal.hot_plate_anchor or '-'}",
            f"  失效条件 | {invalidation_text}",
        ]
        for item in memory_lines[:2]:
            rows.append(f"  过程证据 | {item}")
        if len(memory_lines) < 2:
            for item in tuple(temporal.chain_summary or ())[:2]:
                rows.append(f"  补充线索 | {item}")
        return tuple(rows)

    def _narrative_opportunity_focus_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        themes_text = self._narrative_playbook_themes_text(state, phase_label=phase_label)
        anchor_text = self._narrative_opportunity_anchor_text(state)
        if themes_text and themes_text != "-" and anchor_text and anchor_text != "-" and anchor_text != themes_text:
            return f"{themes_text} | 锚点={anchor_text}"
        return anchor_text or themes_text or "-"

    def _narrative_watch_focus_text(self, state: StrategyConsoleState) -> str:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is None:
            return "-"
        watch_themes = [
            normalize_plate_name(str(item or ""))
            for item in tuple(getattr(global_decision, "watch_themes", ()) or ())
        ]
        watch_themes = [item for item in watch_themes if item]
        return " / ".join(watch_themes[:2]) or "-"

    @staticmethod
    def _temporal_exchange_state_text(state: str) -> str:
        mapping = {
            "rotation_exchange": "旧主线分流，新方向切换验证",
            "rotation_attack": "切换方向开始进攻",
            "mainline_extend": "主线延续，仍以原方向为主",
            "rolling_rotation_exchange": "滚动轮动，资金在多方向换手",
            "rolling_acceleration": "原方向继续加速",
            "rolling_withdrawal": "原方向持续撤退",
            "rolling_rebound_repair": "修复回流，但仍偏观察",
            "timeframe_aligned": "多周期共振，等待进一步确认",
            "risk_rotation": "风险驱动的被动轮动",
            "micro_noise_watch": "短噪音为主，暂不当成切换",
            "fake_breakout": "疑似假突破，先不追",
            "observe": "暂未形成清晰迁移",
            "unknown": "暂未形成清晰迁移",
        }
        return mapping.get(str(state or "").strip(), str(state or "").strip() or "-")

    @staticmethod
    def _temporal_memory_line_text(line) -> str:
        plate_name = normalize_plate_name(str(getattr(line, "plate_name", "") or ""))
        if not plate_name:
            return ""
        transition = str(getattr(line, "transition_state", "") or "")
        process_state = str(getattr(line, "process_state", "") or "")
        prev_state = str(getattr(line, "previous_process_state", "") or "") or "-"
        hot_rank = int(getattr(line, "hot_rank", 999) or 999)
        prev_hot_rank = int(getattr(line, "previous_hot_rank", 999) or 999)
        flow_now = float(getattr(line, "net_inflow_yi_delta", 0.0) or 0.0)
        flow_prev = float(getattr(line, "previous_net_inflow_yi_delta", 0.0) or 0.0)
        amount_now = float(getattr(line, "amount_2m", 0.0) or 0.0)
        amount_prev = float(getattr(line, "previous_amount_2m", 0.0) or 0.0)
        if transition == "steady" and not amount_now and not flow_now:
            return ""
        return (
            f"{plate_name}: {prev_state}->{process_state}"
            f"/切换={transition or '-'}"
            f"/2m={amount_prev:.0f}->{amount_now:.0f}"
            f"/热度={prev_hot_rank}->{hot_rank}"
            f"/流向={flow_prev:.2f}->{flow_now:.2f}"
        )

    def _narrative_current_trade_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            script_label = self._market_script_prompt_text(str(getattr(global_decision, "market_script", "") or ""))
            if main_attack:
                return f"{main_attack}，{script_label or '等待确认'}"
            watch_themes = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "watch_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            if watch_themes:
                return f"{watch_themes[0]}，等待扩散确认"
        summary = state.context.market_summary
        theme = normalize_plate_name(getattr(summary, "top_plate_name", "") or getattr(summary, "mainline_sector", ""))
        return theme or "-"

    def _narrative_previous_hypothesis_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            summary_text = self._primary_prediction_summary(state)
            if summary_text and summary_text != "-":
                return summary_text
        if phase_label in {"open_confirm", "intraday", "postmarket"}:
            payload = self._load_opening_validation_payload(str(getattr(state.context, "trade_date", "") or ""))
            text = str(payload.get("primary_prediction") or "").strip()
            if text:
                return text
        return self._primary_prediction_summary(state)

    def _narrative_opportunity_anchor_text(self, state: StrategyConsoleState) -> str:
        summary_text = self._primary_prediction_summary(state)
        if summary_text and summary_text != "-":
            return summary_text
        return self._narrative_current_trade_text(state, phase_label="intraday")

    def _narrative_validation_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            watch_themes = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "watch_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            script = str(getattr(global_decision, "market_script", "") or "")
            if script == "attack_confirmed" and main_attack:
                return f"{main_attack}=主攻确认/只看前排承接与扩散"
            if script == "hot_risk_validation" and main_attack:
                return f"{main_attack}=热板过热/只做确认不做追高"
            if script in {"risk_validation", "pressure_validation"} and main_attack:
                return f"{main_attack}=验证承压/先降级观察"
            if script == "watch_validation" and watch_themes:
                return f"{watch_themes[0]}=待验证/先看2分钟承接与扩散"
            if script == "risk_off":
                return "风险扩散=只观察/等待新的低风险确认"
        if phase_label in {"open_confirm", "intraday", "postmarket"}:
            payload = self._load_opening_validation_payload(str(getattr(state.context, "trade_date", "") or ""))
            validation = dict(payload.get("mode_validation") or {})
            label = str(validation.get("label") or "").strip()
            reason = str(validation.get("reason") or "").strip()
            if label:
                return label if not reason or reason == "-" else f"{label} | {reason}"
        return "待验证"

    def _narrative_switch_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        global_decision = self._global_market_decision_for_state(state)
        output_summary = self._playbook_output_summary_for_state(state)
        if global_decision is not None:
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            secondary = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "secondary_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            watch_themes = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "watch_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            avoid_themes = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "avoid_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            script = str(getattr(global_decision, "market_script", "") or "")
            if script == "risk_off":
                return "高位与中军风险扩散，先防守观察，不追扩散"
            if script in {"risk_validation", "pressure_validation"}:
                if main_attack:
                    return f"{main_attack} 承压，先降级观察，只保留局部前排"
                return "主线承压，先看修复，不做一致扩散"
            if script == "attack_confirmed" and main_attack:
                if secondary:
                    return f"资金聚焦 {main_attack}，副攻看 {'/'.join(secondary[:2])} 是否继续共振"
                if watch_themes:
                    return f"{main_attack} 延续，观察 {'/'.join(watch_themes[:2])} 是否跟随扩散"
                return f"{main_attack} 延续为主，继续只看前排承接与扩散"
            if script == "hot_risk_validation" and main_attack:
                return f"{main_attack} 虽强但过热，宁可等分歧确认，不追一致高开"
            if script == "watch_validation" and watch_themes:
                return f"切换尚未坐实，先看 {'/'.join(watch_themes[:2])} 能否从观察走到确认"
            if avoid_themes and main_attack:
                return f"资金从 {'/'.join(avoid_themes[:2])} 退潮，转向观察 {main_attack}"
        if output_summary is not None:
            migration_lines = tuple(getattr(output_summary, "migration_lines", ()) or ())
            for line in migration_lines:
                text = str(line or "").strip()
                if text and all(not text.startswith(prefix) for prefix in ("global:", "time:", "hot:")):
                    return text
        if phase_label in {"open_confirm", "intraday", "postmarket"}:
            payload = self._load_opening_validation_payload(str(getattr(state.context, "trade_date", "") or ""))
            correction = str(payload.get("correction_conclusion") or "").strip()
            if correction:
                return correction
            theme_validation = tuple(item for item in payload.get("theme_validation", ()) if isinstance(item, dict))
            strengthened = [
                str(item.get("plate_name") or "").strip()
                for item in theme_validation
                if str(item.get("validation_state") or "") == "strengthened"
            ]
            falsified = [
                str(item.get("plate_name") or "").strip()
                for item in theme_validation
                if str(item.get("validation_state") or "") == "falsified"
            ]
            if strengthened and falsified:
                return f"资金从 {'/'.join(falsified[:2])} 分流到 {'/'.join(strengthened[:2])}，只保留前排有效承接"
            if strengthened:
                return f"{'/'.join(strengthened[:2])} 获得开盘验证，延续主观察"
            if falsified:
                leader_only = [
                    name
                    for name in falsified
                    if self._is_theme_falsified_but_leader_alive(state, plate_name=name)
                ]
                if leader_only:
                    return f"{'/'.join(leader_only[:2])} 板块证伪，只剩龙头独活，不做扩散"
                return f"{'/'.join(falsified[:2])} 开盘验证偏弱，先降级到观察"
        return "暂未形成明确切换线索"

    def _narrative_playbook_themes_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            ordered: list[str] = []
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            if main_attack:
                ordered.append(main_attack)
            for plate_name in tuple(getattr(global_decision, "secondary_themes", ()) or ()):
                normalized = normalize_plate_name(str(plate_name or ""))
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
            for plate_name in tuple(getattr(global_decision, "watch_themes", ()) or ()):
                normalized = normalize_plate_name(str(plate_name or ""))
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
            if ordered:
                return " / ".join(ordered[:2])
        names = list(self._narrative_priority_plates(state, phase_label=phase_label)[:2])
        return " / ".join(names) or "-"

    def _narrative_priority_plates(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> tuple[str, ...]:
        ordered: list[str] = []
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            if main_attack:
                ordered.append(main_attack)
            for plate_name in tuple(getattr(global_decision, "secondary_themes", ()) or ()):
                normalized = normalize_plate_name(str(plate_name or ""))
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
            for plate_name in tuple(getattr(global_decision, "watch_themes", ()) or ()):
                normalized = normalize_plate_name(str(plate_name or ""))
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
            if ordered:
                return tuple(ordered[:4])
        if phase_label in {"open_confirm", "intraday", "postmarket"}:
            payload = self._load_opening_validation_payload(str(getattr(state.context, "trade_date", "") or ""))
            for item in tuple(payload.get("theme_validation", ()) or ()):
                if not isinstance(item, dict):
                    continue
                plate_name = normalize_plate_name(str(item.get("plate_name") or ""))
                if not plate_name:
                    continue
                validation_state = str(item.get("validation_state") or "")
                if validation_state == "strengthened" and plate_name not in ordered:
                    ordered.append(plate_name)
            for item in tuple(payload.get("theme_validation", ()) or ()):
                if not isinstance(item, dict):
                    continue
                plate_name = normalize_plate_name(str(item.get("plate_name") or ""))
                if plate_name and plate_name not in ordered:
                    ordered.append(plate_name)
        summary = state.context.market_summary
        for raw_name in (
            getattr(summary, "top_plate_name", ""),
            getattr(summary, "mainline_sector", ""),
        ):
            normalized = normalize_plate_name(raw_name)
            if normalized and normalized not in ordered:
                ordered.append(normalized)
        return tuple(ordered[:4])

    def _narrative_avoid_text(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        output_summary = self._playbook_output_summary_for_state(state)
        if output_summary is not None:
            avoid_actions = tuple(getattr(output_summary, "avoid_actions", ()) or ())
            reject_reasons = tuple(getattr(output_summary, "reject_reasons", ()) or ())
            if avoid_actions:
                return " ; ".join(str(item) for item in avoid_actions[:3] if str(item).strip()) or "-"
            if reject_reasons:
                return "只观察"
            return "-"
        parts: list[str] = []
        if state.bundle is not None:
            for decision in state.bundle.decisions:
                display_code = self._display_action_code(decision, state, phase_label=phase_label)
                if display_code not in {"failed_promo_guard", "do_not_chase"}:
                    continue
                parts.append(f"{self._decision_name(state, decision)}={self._action_text(display_code)}")
                if len(parts) >= 3:
                    break
        return " ; ".join(parts) or "-"

    def _build_console_state(
        self,
        intraday_context: IntradayContext,
        *,
        min_confidence: int,
        phase_label: str,
        minute_tag: str | None = None,
        startup_report: StartupSelfCheckReport | None = None,
        historical_only: bool = False,
        stale_snapshot_only: bool = False,
    ) -> StrategyConsoleState:
        premarket_plan_mode = (
            phase_label == "premarket"
            and bool(minute_tag)
            and str(minute_tag) < "09:15"
        )
        if phase_label == "premarket" and historical_only:
            recap_trade_date, recap_previous_trade_date = self._resolve_recap_trade_dates(
                trade_date=intraday_context.trade_date,
                phase_label=phase_label,
                historical_only=historical_only,
            )
            recap_auction_map = self._load_recap_auction_map(recap_trade_date)
            recap_yest_limit_map = self._load_json_hash(f"cache:yest_limit_pool:{recap_previous_trade_date}")
            overlay_snapshots = tuple(
                self._overlay_snapshot_with_auction(
                    snapshot,
                    recap_auction_map.get(snapshot.symbol),
                    yest_limit_row=recap_yest_limit_map.get(snapshot.symbol),
                )
                for snapshot in intraday_context.stock_snapshots
            )
            recap_hot_plate_map = self._load_json_hash(f"cache:hot_plates:{recap_trade_date}")
            recap_previous_hot_plate_map = self._load_json_hash(f"cache:hot_plates:{recap_previous_trade_date}")
            recap_session_facts = build_session_facts(
                trade_date=recap_trade_date,
                phase_name="premarket_recap",
                snapshots=overlay_snapshots,
                hot_plate_map=recap_hot_plate_map,
                yesterday_hot_plate_map=recap_previous_hot_plate_map,
            )
            intraday_context = replace(
                intraday_context,
                stock_snapshots=overlay_snapshots,
                auction_map=recap_auction_map or intraday_context.auction_map,
                yest_limit_map=recap_yest_limit_map,
                session_facts=recap_session_facts,
            )

        snapshot_map = {snapshot.symbol: snapshot for snapshot in intraday_context.stock_snapshots}
        stock_name_map = {
            symbol: self._short_stock_name(snapshot, symbol=symbol)
            for symbol, snapshot in snapshot_map.items()
        }
        plate_symbol_index: dict[str, list[str]] = defaultdict(list)
        for snapshot in snapshot_map.values():
            names: list[str] = []
            for raw_name in (snapshot.plate, *snapshot.real_plate_names):
                text = str(raw_name or "").strip()
                if text and text not in names:
                    names.append(text)
            for plate_name in names:
                plate_symbol_index[plate_name].append(snapshot.symbol)
        coverage_scope = self._build_coverage_scope(intraday_context, snapshot_map=snapshot_map)
        coverage_scope_set = frozenset(coverage_scope)
        coverage_scope_snapshots = tuple(
            snapshot_map[symbol] for symbol in coverage_scope if symbol in snapshot_map
        )
        shape_eval_limit = self._shape_eval_scope_limit(
            coverage_scope_count=len(coverage_scope_snapshots),
            phase_label=phase_label,
        )
        shape_eval_scope = filter_shape_eval_scope(
            coverage_scope_snapshots,
            max_count=min(len(coverage_scope_snapshots), shape_eval_limit),
        )
        actual_source = self._infer_actual_source(
            intraday_context,
            coverage_scope,
            phase_label=phase_label,
            startup_report=startup_report,
        )
        frozen_postmarket_snapshot = (
            phase_label == "postmarket"
            and actual_source in {
                "redis_anchor",
                "redis_0925",
                "redis_preview_0920",
                "redis_preview_0924",
                "stale_intraday_snapshot",
            }
        )
        missing_inputs = self._collect_missing_inputs(
            intraday_context,
            phase_label=phase_label,
            startup_report=startup_report,
        )
        plate_stats = build_auction_plate_bucket_stats(
            intraday_context,
            symbols=coverage_scope,
            top_n=5,
        )
        full_plate_stats = build_auction_plate_bucket_stats(
            intraday_context,
            top_n=max(len(snapshot_map), 5),
        )
        auction_delta_stats: tuple[AuctionSnapshotDeltaStat, ...] = ()
        if phase_label in {"auction", "auction_preview", "open_confirm"}:
            try:
                auction_snapshot_result = self._load_auction_snapshots_cached(intraday_context.trade_date)
                auction_delta_stats = build_auction_snapshot_delta_stats(
                    auction_snapshot_result.rows,
                    snapshot_map.values(),
                    top_n=5,
                )
            except Exception:
                logger.exception("auction snapshot delta build failed | trade_date=%s", intraday_context.trade_date)
        collision_rows = self._build_theme_collision_rows(
            full_plate_stats,
            context=intraday_context,
            auction_delta_stats=auction_delta_stats,
        )
        theme_judge_map = self._build_theme_judge_map(collision_rows)
        theme_collision_map = {
            normalized: item
            for item in collision_rows
            if (normalized := normalize_plate_name(item.plate_name))
        }
        normalized_plate_names_map = {
            symbol: self._normalized_plate_names(snapshot)
            for symbol, snapshot in snapshot_map.items()
        }
        matched_theme_judge_map = {
            symbol: next(
                (
                    (theme_judge_map.get(plate_name), plate_name)
                    for plate_name in normalized_plate_names_map.get(symbol, ())
                    if theme_judge_map.get(plate_name) is not None
                ),
                (None, ""),
            )
            for symbol in snapshot_map
        }
        pre_bundle_state = StrategyConsoleState(
            context=intraday_context,
            coverage_scope=coverage_scope,
            coverage_scope_set=coverage_scope_set,
            actual_source=actual_source,
            plate_stats=plate_stats,
            bundle=None,
            playbook_decisions=(),
            missing_inputs=missing_inputs,
            snapshot_map=snapshot_map,
            stock_name_map=stock_name_map,
            plate_symbol_map={plate_name: tuple(symbols) for plate_name, symbols in plate_symbol_index.items()},
            decision_map={},
            full_plate_stats=full_plate_stats,
            historical_only=historical_only,
            stale_snapshot_only=(stale_snapshot_only or frozen_postmarket_snapshot),
            frozen_postmarket_snapshot=frozen_postmarket_snapshot,
            collision_rows=collision_rows,
            auction_delta_stats=auction_delta_stats,
            theme_judge_map=theme_judge_map,
            theme_collision_map=theme_collision_map,
            normalized_plate_names_map=normalized_plate_names_map,
            matched_theme_judge_map=matched_theme_judge_map,
        )
        if phase_label == "open_confirm":
            theme_judge_map = self._build_open_confirm_theme_judge_map(
                pre_bundle_state,
                collision_rows,
                base_map=theme_judge_map,
            )
        bundle = None
        playbook_decisions: tuple[AuctionLadderDecision, ...] = ()
        decision_map: dict[str, AuctionLadderDecision] = {}
        selection_context_map: dict[str, StockSelectionContext] = {}
        if coverage_scope and not premarket_plan_mode:
            formal_theme_context_map = self._build_formal_theme_context_map(
                collision_rows,
                theme_judge_map=theme_judge_map,
                phase_label=phase_label,
            )
            if (
                phase_label in {"opening", "open_confirm", "intraday", "postmarket"}
                and any(float(getattr(snapshot, "amount_2m", 0.0) or 0.0) > 0.0 for snapshot in intraday_context.stock_snapshots)
            ):
                opening_validation_bundle = build_opening_validation_bundle(
                    intraday_context,
                    formal_theme_context_map,
                    phase_label=phase_label,
                )
                intraday_context = replace(
                    intraday_context,
                    opening_validation_bundle=opening_validation_bundle,
                )
            temporal_memory_write = (
                phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}
                and not historical_only
                and not stale_snapshot_only
                and not frozen_postmarket_snapshot
            )
            intraday_context = replace(
                intraday_context,
                notes=(
                    *tuple(intraday_context.notes or ()),
                    f"temporal_memory_write={1 if temporal_memory_write else 0}",
                    f"temporal_sample_phase={phase_label}",
                    f"temporal_sample_minute={minute_tag or '-'}",
                ),
            )
            bundle = build_context_strategy_bundle_for_symbols(
                intraday_context,
                symbols=shape_eval_scope or coverage_scope,
                theme_context_map=formal_theme_context_map,
            )
            playbook_decisions = tuple(
                decision for decision in bundle.decisions if int(getattr(decision, "confidence", 0)) >= min_confidence
            )
            decision_map = {decision.symbol: decision for decision in bundle.decisions}
            selection_context_map = {item.symbol: item for item in bundle.stock_selection_contexts}
            try:
                bundle_note_map = {
                    str(note).split("=", 1)[0]: str(note).split("=", 1)[1]
                    for note in bundle.notes
                    if isinstance(note, str) and "=" in note
                }
                logger.info(
                    "shape eval scope | phase=%s | mode=%s | selected=%s | total=%s | compression=%s | decisions=%s | playbook_decisions=%s | stock_ctx_recomputed=%s | stock_ctx_reused=%s",
                    phase_label,
                    bundle_note_map.get("shape_scope_mode", "-"),
                    bundle_note_map.get("selected_snapshot_count", "-"),
                    bundle_note_map.get("total_snapshot_count", "-"),
                    bundle_note_map.get("shape_prefilter_compression_ratio", "-"),
                    bundle_note_map.get("decision_count", "-"),
                    len(playbook_decisions),
                    bundle_note_map.get("stock_ctx_recomputed", "-"),
                    bundle_note_map.get("stock_ctx_reused", "-"),
                )
                logger.info(
                    "shape eval narrowed | phase=%s | coverage_scope=%s | shape_eval_scope=%s | controller_compression=%.4f",
                    phase_label,
                    len(coverage_scope),
                    len(shape_eval_scope),
                    (1.0 - (len(shape_eval_scope) / len(coverage_scope))) if coverage_scope else 0.0,
                )
                plate_conflicts = bundle_note_map.get("plate_conflicts", "")
                plate_conflict_samples = bundle_note_map.get("plate_conflict_samples", "")
                if plate_conflicts:
                    logger.info(
                        "plate mapping audit | phase=%s | conflicts=%s | samples=%s",
                        phase_label,
                        plate_conflicts,
                        plate_conflict_samples or "-",
                    )
            except Exception:
                logger.exception("shape eval scope logging failed | phase=%s", phase_label)
        return StrategyConsoleState(
            context=intraday_context,
            coverage_scope=coverage_scope,
            coverage_scope_set=coverage_scope_set,
            actual_source=actual_source,
            plate_stats=plate_stats,
            bundle=bundle,
            playbook_decisions=playbook_decisions,
            missing_inputs=missing_inputs,
            snapshot_map=snapshot_map,
            stock_name_map=stock_name_map,
            plate_symbol_map={plate_name: tuple(symbols) for plate_name, symbols in plate_symbol_index.items()},
            decision_map=decision_map,
            full_plate_stats=full_plate_stats,
            historical_only=historical_only,
            stale_snapshot_only=(stale_snapshot_only or frozen_postmarket_snapshot),
            frozen_postmarket_snapshot=frozen_postmarket_snapshot,
            collision_rows=collision_rows,
            auction_delta_stats=auction_delta_stats,
            theme_judge_map=theme_judge_map,
            selection_context_map=selection_context_map if bundle is not None else {},
            theme_collision_map=theme_collision_map,
            normalized_plate_names_map=normalized_plate_names_map,
            matched_theme_judge_map=matched_theme_judge_map,
        )

    def _build_open_confirm_theme_judge_map(
        self,
        state: StrategyConsoleState,
        collision_rows: Iterable[AuctionThemeCollisionStat],
        *,
        base_map: dict[str, ThemeJudgeResult],
    ) -> dict[str, ThemeJudgeResult]:
        updated = dict(base_map)
        for item in collision_rows:
            validation_state, metrics = self._theme_opening_validation_state(state, item)
            previous = updated.get(item.plate_name)
            notes = list(previous.notes if previous is not None else ())
            notes.extend(
                (
                    "source=open_confirm",
                    f"open_confirm_validation={validation_state}",
                    f"undertake_2m={int(metrics.get('undertake_count', 0.0))}/{int(metrics.get('front_row_count', 0.0))}",
                    f"undertake_5m={int(metrics.get('undertake_count_5m', 0.0))}/{int(metrics.get('front_row_count', 0.0))}",
                    f"weak_count={int(metrics.get('weak_count', 0.0))}",
                    f"high_open_fail={int(metrics.get('high_open_fail_count', 0.0))}",
                    f"low_open_repair={int(metrics.get('low_open_repair_count', 0.0))}",
                    f"expansion={int(metrics.get('expansion_count', 0.0))}",
                )
            )
            if previous is not None:
                updated[item.plate_name] = replace(
                    previous,
                    notes=tuple(notes),
                )
            else:
                updated[item.plate_name] = self._build_theme_judge_result(
                    item,
                    validation_state=self._theme_open_confirm_state(item),
                    action_class=self._theme_action_class(
                        item,
                        validation_state=self._theme_open_confirm_state(item),
                    ),
                    notes=tuple(notes),
                )
        return updated

    def _shape_eval_scope_limit(
        self,
        *,
        coverage_scope_count: int,
        phase_label: str,
    ) -> int:
        if coverage_scope_count <= 0:
            return self.SHAPE_EVAL_SCOPE_BASE_LIMIT
        limit = self.SHAPE_EVAL_SCOPE_BASE_LIMIT
        if coverage_scope_count >= 240:
            limit = max(limit, int(coverage_scope_count * 0.28))
        elif coverage_scope_count >= 160:
            limit = max(limit, int(coverage_scope_count * 0.32))
        if phase_label == "intraday" and coverage_scope_count >= 180:
            limit += 40
        elif phase_label in {"auction", "opening", "open_confirm"} and coverage_scope_count >= 240:
            limit += 20
        return max(self.SHAPE_EVAL_SCOPE_BASE_LIMIT, min(limit, self.SHAPE_EVAL_SCOPE_MAX_LIMIT))

    @staticmethod
    def _theme_open_confirm_state(item: AuctionThemeCollisionStat) -> str:
        row = item.row
        if item.fakeout_level == "strong" or item.x_score >= 6.0:
            return "falsified"
        if (item.a_score >= 6.0 and item.x_score < 5.0) or (row.rebound_count >= 1 and item.x_score < 5.5):
            return "strengthened"
        return "maintained"

    @staticmethod
    def _external_validation_state(validation_state: str) -> str:
        if validation_state == "strengthened":
            return "confirmed"
        if validation_state == "falsified":
            return "falsified"
        return "partial"

    def _build_theme_judge_map(
        self,
        collision_rows: Iterable[AuctionThemeCollisionStat],
    ) -> dict[str, ThemeJudgeResult]:
        judge_map: dict[str, ThemeJudgeResult] = {}
        for item in collision_rows:
            validation_state = self._theme_open_confirm_state(item)
            action_class = self._theme_action_class(item, validation_state=validation_state)
            judge_map[item.plate_name] = self._build_theme_judge_result(
                item,
                validation_state=validation_state,
                action_class=action_class,
                notes=(
                    f"fakeout={item.fakeout_level}",
                    f"signal={item.signal}",
                    f"eax={item.e_score:.1f}/{item.a_score:.1f}/{item.x_score:.1f}",
                ),
            )
        return judge_map

    @staticmethod
    def _theme_action_class(
        item: AuctionThemeCollisionStat,
        *,
        validation_state: str,
    ) -> str:
        row = item.row
        if validation_state == "falsified" or item.fakeout_level == "strong" or item.x_score >= 6.0:
            return "trap_avoid"
        if row.limit_up_count >= 1 and row.highest_lb_days >= 2 and item.a_score >= 5.0:
            return "main_attack"
        if item.a_score >= 5.0 and item.x_score < 5.0:
            return "anchor_only"
        if item.signal in {"资金试错"} or item.fakeout_level == "warn":
            return "observe"
        if item.signal == "有量无板":
            if (
                row.leader_count >= 1
                and row.auction_amount >= 80_000_000
                and item.a_score >= 4.8
                and item.x_score < 5.2
            ):
                return "anchor_only"
            return "observe"
        if validation_state == "strengthened" and item.a_score >= 5.0 and item.x_score < 5.5:
            return "front_row_confirm"
        if item.a_score >= 5.0 and item.x_score < 5.0:
            return "observe"
        return "observe"

    @staticmethod
    def _theme_judge_opportunity_score(item: AuctionThemeCollisionStat) -> float:
        return round(min(max(((item.e_score * 0.55) + (item.a_score * 0.45) - (item.x_score * 0.25)), 0.0), 10.0), 1)

    def _build_theme_judge_result(
        self,
        item: AuctionThemeCollisionStat,
        *,
        validation_state: str,
        action_class: str,
        notes: Iterable[str] = (),
    ) -> ThemeJudgeResult:
        return ThemeJudgeResult(
            plate_name=item.plate_name,
            opportunity_score=self._theme_judge_opportunity_score(item),
            trap_score=round(item.x_score, 1),
            validation_state=validation_state,
            action_class=action_class,
            signal=item.signal,
            expectation_label=item.expectation_label,
            eax_label=item.eax_label,
            eax_action=item.eax_action,
            notes=tuple(notes),
        )

    @staticmethod
    def _theme_judge_note_metric_int(judge_notes: tuple[str, ...], prefix: str) -> int:
        for note in judge_notes:
            if isinstance(note, str) and note.startswith(prefix):
                try:
                    return int(float(note.split("=", 1)[1]))
                except (TypeError, ValueError):
                    return 0
        return 0

    @staticmethod
    def _theme_context_bias_action(
        item: AuctionThemeCollisionStat,
        *,
        judge: ThemeJudgeResult | None,
        trade_label: str,
        trade_conclusion: str,
        tradable: bool,
        open_confirm_state: str,
        external_confirm_state: str,
    ) -> str:
        bias_action = "observe_only"
        judge_action = judge.action_class if judge is not None else ""
        if open_confirm_state == "falsified" or judge_action == "trap_avoid":
            bias_action = "avoid_after_open_confirm"
        elif judge_action in {"main_attack", "front_row_confirm"} and external_confirm_state == "confirmed":
            bias_action = "front_row_confirm"
        elif tradable and item.e_score <= 4.0 and item.a_score >= 6.0 and item.x_score < 5.0:
            bias_action = "small_probe_only"
        elif tradable and external_confirm_state == "confirmed":
            bias_action = "front_row_watch"
        if trade_label == "high_event":
            return "observe_only"
        if trade_conclusion == "leader_only_alive" and trade_label != "high_event":
            return "front_row_watch"
        if external_confirm_state == "partial" and trade_conclusion in {"old_mainline_weak_continue", "switch_wait_confirm"}:
            return "observe_only" if item.row.leader_count <= 1 else "front_row_watch"
        return bias_action

    def _build_formal_theme_context_map(
        self,
        collision_rows: Iterable[AuctionThemeCollisionStat],
        *,
        theme_judge_map: dict[str, ThemeJudgeResult] | None = None,
        phase_label: str = "auction",
    ) -> dict[str, ThemeSelectionContext]:
        context_map: dict[str, ThemeSelectionContext] = {}
        for item in collision_rows:
            judge = (theme_judge_map or {}).get(item.plate_name)
            trade_label = classify_theme_trade_label_from_collision(
                item.plate_name,
                item.row,
                yesterday_hot_rank=item.yesterday_hot_rank,
            )
            fakeout_level = normalize_theme_fakeout_level(item.fakeout_level)

            open_confirm_state = judge.validation_state if judge is not None else self._theme_open_confirm_state(item)
            external_confirm_state = self._external_validation_state(open_confirm_state)
            high_open_fail = 0
            low_open_repair = 0
            expansion_count = 0
            if judge is not None:
                high_open_fail = self._theme_judge_note_metric_int(judge.notes, "high_open_fail=")
                low_open_repair = self._theme_judge_note_metric_int(judge.notes, "low_open_repair=")
                expansion_count = self._theme_judge_note_metric_int(judge.notes, "expansion=")
                if open_confirm_state == "falsified" or high_open_fail >= 1:
                    fakeout_level = "high"
                elif (low_open_repair >= 1 or expansion_count >= 2) and fakeout_level == "medium":
                    fakeout_level = "low"
            trade_conclusion = resolve_theme_trade_conclusion(
                theme_trade_label=trade_label,
                open_confirm_state=open_confirm_state,
                fakeout_level=fakeout_level,
                high_open_fail_count=high_open_fail,
                low_open_repair_count=low_open_repair,
                expansion_count=expansion_count,
                leader_count=item.row.leader_count,
                yest_limit_count=item.row.yest_limit_count,
            )
            tradable = (
                fakeout_level != "high"
                and external_confirm_state == "partial"
                and (
                    (item.e_score >= 6.0 and item.a_score >= 6.0 and item.x_score < 5.0)
                    or (item.e_score <= 4.0 and item.a_score >= 6.0 and item.x_score < 5.0)
                    or (item.a_score >= 5.0 and item.x_score < 4.0)
                )
            )
            if trade_label == "high_event" and item.row.leader_count <= 1:
                tradable = False
            if trade_conclusion in {"old_mainline_distribution", "high_event_self_excited", "switch_failed"}:
                tradable = False
            if (
                external_confirm_state == "partial"
                and trade_conclusion in {"old_mainline_weak_continue", "switch_wait_confirm"}
                and item.row.turn_strong_count <= 1
                and item.row.limit_up_count <= 1
            ):
                tradable = False
            bias_action = self._theme_context_bias_action(
                item,
                judge=judge,
                trade_label=trade_label,
                trade_conclusion=trade_conclusion,
                tradable=tradable,
                open_confirm_state=open_confirm_state,
                external_confirm_state=external_confirm_state,
            )
            phase_priority_bias = 0.0
            if judge is not None:
                if judge.action_class == "main_attack":
                    phase_priority_bias += 1.0
                elif judge.action_class == "front_row_confirm":
                    phase_priority_bias += 0.8
                elif judge.action_class == "anchor_only":
                    phase_priority_bias += 0.35
                if judge.validation_state == "strengthened":
                    phase_priority_bias += 0.8
                elif judge.validation_state == "falsified":
                    phase_priority_bias -= 0.8
            if item.expectation_label in {"符合/强化", "局部转强"}:
                phase_priority_bias += 0.3
            if item.fakeout_level == "strong":
                phase_priority_bias -= 0.8
            elif item.fakeout_level == "warn":
                phase_priority_bias -= 0.35
            if phase_label in {"open_confirm", "intraday"}:
                if open_confirm_state == "falsified":
                    phase_priority_bias -= 0.6
                elif open_confirm_state == "strengthened":
                    phase_priority_bias += 0.4
            phase_priority_bias = round(max(-1.0, min(2.0, phase_priority_bias)), 2)

            context_map[item.plate_name] = build_theme_selection_context(
                plate_name=item.plate_name,
                e_score=item.e_score,
                a_score=item.a_score,
                x_score=item.x_score,
                theme_trade_label=trade_label,
                trade_conclusion=trade_conclusion,
                fakeout_level=fakeout_level,
                cohesion_level=self._theme_cohesion_level(item.row),
                tradable=tradable,
                bias_action=bias_action,
                open_confirm_state=open_confirm_state,
                phase_priority_bias=phase_priority_bias,
                high_open_fail_count=high_open_fail,
                low_open_repair_count=low_open_repair,
                expansion_count=expansion_count,
                leader_count=item.row.leader_count,
                yest_limit_count=item.row.yest_limit_count,
                notes=(
                    f"trade_label={trade_label}",
                    f"trade_conclusion={trade_conclusion}",
                    f"signal={item.signal}",
                    f"expectation={item.expectation_label}",
                    f"eax={item.eax_label}",
                    f"action={item.eax_action}",
                    f"phase_priority_bias={phase_priority_bias:.2f}",
                ),
            )
        return context_map

    def _load_auction_snapshots_cached(self, trade_date: str) -> IntradayFetchResult:
        load_auction_snapshots = getattr(self._intraday_hub, "load_auction_snapshots", None)
        if not callable(load_auction_snapshots):
            return IntradayFetchResult(
                dataset="auction_snapshots",
                trade_date=trade_date,
                rows=[],
                source="missing_hub_method",
            )

        now = time.monotonic()
        cache = getattr(self, "_auction_snapshot_cache", None)
        if cache is None:
            cache = {}
            self._auction_snapshot_cache = cache
        cached = cache.get(trade_date)
        if cached and now - cached[0] <= self.AUCTION_SNAPSHOT_CACHE_TTL_SECONDS:
            return cached[1]

        result = load_auction_snapshots(trade_date)
        cache[trade_date] = (now, result)
        return result

    def _resolve_recap_trade_dates(
        self,
        *,
        trade_date: str,
        phase_label: str,
        historical_only: bool,
    ) -> tuple[str, str]:
        recap_trade_date = str(trade_date or "").strip()
        if phase_label == "premarket" and historical_only:
            recap_trade_date = self._previous_trade_day(recap_trade_date)
        recap_previous_trade_date = self._previous_trade_day(recap_trade_date) if recap_trade_date else ""
        return recap_trade_date, recap_previous_trade_date

    def _previous_trade_day(self, trade_date: str) -> str:
        date_text = str(trade_date or "").strip()
        if not date_text:
            return ""
        try:
            return TradingCalendarService().get_previous_trading_day(date_text)
        except Exception:
            return date_text

    def _load_json_hash(self, key: str) -> dict[str, dict[str, object]]:
        try:
            raw_map = self._intraday_hub.redis.hgetall(key) or {}
        except Exception:
            return {}
        payload: dict[str, dict[str, object]] = {}
        for field, raw in raw_map.items():
            symbol = str(field or "").strip()[-6:]
            if not symbol:
                continue
            row: dict[str, object] | None = None
            if isinstance(raw, dict):
                row = dict(raw)
            else:
                try:
                    parsed = json.loads(raw)
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    row = parsed
            if row is None:
                continue
            row.setdefault("symbol", symbol)
            payload[symbol] = row
        return payload

    def _normalize_pct_value(self, raw: object) -> float:
        value = normalize_auction_pct_ratio(raw)
        return value if not math.isnan(value) else value

    def _load_recap_auction_map(self, trade_date: str) -> dict[str, dict[str, object]]:
        tag = str(trade_date or "").replace("-", "")
        if not tag:
            return {}
        auction_map = self._load_json_hash(f"market:auction:{tag}:0925")
        if not auction_map:
            auction_map = self._load_json_hash(f"market:auction:{tag}:0924")
        if auction_map:
            normalized: dict[str, dict[str, object]] = {}
            for symbol, row in auction_map.items():
                normalized[symbol] = {
                    **row,
                    "symbol": symbol,
                    "change_pct": self._normalize_pct_value(row.get("change_pct", row.get("open_pct", 0.0))),
                    "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
                    "bid_amount": float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0)) or 0.0),
                }
            return normalized
        try:
            raw = self._intraday_hub.redis.get(f"market:auction:anchor:{tag}")
        except Exception:
            raw = None
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        rows = payload.get("rows") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return {}
        normalized = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip()[-6:]
            if not symbol:
                continue
            normalized[symbol] = {
                **item,
                "symbol": symbol,
                "change_pct": self._normalize_pct_value(item.get("change_pct", 0.0)),
                "amount": float(item.get("auction_amount_yuan", item.get("amount", 0.0)) or 0.0),
                "bid_amount": float(item.get("bid_amount_yuan", item.get("bid_amount", 0.0)) or 0.0),
            }
        return normalized

    def _overlay_snapshot_with_auction(
        self,
        snapshot: StockStateSnapshot,
        auction_row: dict[str, object] | None,
        *,
        yest_limit_row: dict[str, object] | None = None,
    ) -> StockStateSnapshot:
        open_pct = float("nan")
        auction_amount = snapshot.auction_amount
        bid_amount = 0.0
        volume_intensity = snapshot.volume_intensity
        name = snapshot.name
        if auction_row:
            open_pct = self._normalize_pct_value(auction_row.get("change_pct", auction_row.get("open_pct", snapshot.open_pct)))
            auction_amount = float(auction_row.get("amount", snapshot.auction_amount) or 0.0)
            bid_amount = float(auction_row.get("bid_amount", 0.0) or 0.0)
            if bid_amount > 0:
                volume_intensity = max(1.0, round(bid_amount / 10_000_000, 2))
            elif auction_amount > 0 and volume_intensity <= 1.0:
                volume_intensity = max(1.0, round(auction_amount / 100_000_000, 2))
            name = str(auction_row.get("name", snapshot.name) or snapshot.name)
        lb_days = snapshot.lb_days
        is_yest_limit = snapshot.is_yest_limit
        if yest_limit_row is not None:
            try:
                lb_days = int(yest_limit_row.get("lb_days", snapshot.lb_days) or snapshot.lb_days)
            except (TypeError, ValueError):
                lb_days = snapshot.lb_days
            is_yest_limit = True
        return replace(
            snapshot,
            name=name,
            open_pct=open_pct,
            auction_amount=auction_amount,
            volume_intensity=volume_intensity,
            lb_days=lb_days,
            is_yest_limit=is_yest_limit,
        )

    def _render_market_regime(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        summary = state.context.market_summary
        feedback_ready = self._feedback_metrics_ready(state)
        score = summary.sentiment_score if feedback_ready else 0.0
        historical_mode = self._is_historical_mode(state, phase_label=phase_label)
        battle = "历史快照" if historical_mode else (
            self._battle_text(summary.battle_status or "-") if feedback_ready else "等待锚点"
        )
        regime = self._infer_regime_stage(summary, state, phase_label=phase_label)
        pos_cap = self._infer_position_cap(summary, state, phase_label=phase_label)
        allow_setups = self._collect_allowed_setups(state, phase_label=phase_label)
        banned_actions = self._collect_banned_actions(state, phase_label=phase_label)
        source = self._display_source_label(state, phase_label=phase_label)
        return (
            f"情绪总览 | 情绪分={f'{score:.1f}/10' if feedback_ready else '--'} "
            f"| 阶段={self._regime_text(regime)} "
            f"| 数据={source} "
            f"| 对局={battle} "
            f"| 仓位上限={pos_cap}% "
            f"| 可做={','.join(self._allow_text(item) for item in allow_setups)} "
            f"| 禁做={','.join(self._ban_text(item) for item in banned_actions)} "
            f"| 场景={self._phase_text(phase_label)}"
        )

    def _render_recap_market_regime(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        metrics = self._compute_recap_feedback_metrics(state, phase_label=phase_label)
        score = float(metrics["sentiment_score"])
        battle = "历史快照" if phase_label == "premarket" else self._battle_text(str(metrics["battle"]))
        regime = "watch" if score < 4.0 else ("review" if score < 6.0 else "attack")
        pos_cap = self._infer_position_cap(
            type(
                "RecapSummary",
                (),
                {
                    "sentiment_score": score,
                    "headshot_rate": float(metrics["headshot_rate"]),
                    "mainline_switch": False,
                    "battle_status": str(metrics["battle"]),
                },
            )(),
            state,
            phase_label=phase_label,
        )
        allow_setups = self._collect_allowed_setups(state, phase_label=phase_label)
        banned_actions = self._collect_banned_actions(state, phase_label=phase_label)
        return (
            f"情绪总览 | 情绪分={score:.1f}/10 "
            f"| 阶段={self._regime_text(regime)} "
            f"| 数据=昨日复盘 "
            f"| 对局={battle} "
            f"| 仓位上限={pos_cap}% "
            f"| 可做={','.join(self._allow_text(item) for item in allow_setups)} "
            f"| 禁做={','.join(self._ban_text(item) for item in banned_actions)} "
            f"| 场景={self._phase_text(phase_label)}"
        )

    def _plate_rows_for_decision(self, state: StrategyConsoleState) -> tuple[AuctionPlateBucketStat, ...]:
        rows = tuple(row for row in state.plate_stats if not row.generic)
        return rows or state.plate_stats

    def _plate_rows_for_market(self, state: StrategyConsoleState) -> tuple[AuctionPlateBucketStat, ...]:
        rows = tuple(row for row in state.full_plate_stats if not row.generic)
        if rows:
            return rows
        if state.full_plate_stats:
            return state.full_plate_stats
        return self._plate_rows_for_decision(state)

    def _theme_collision_rows(self, state: StrategyConsoleState) -> tuple[AuctionThemeCollisionStat, ...]:
        rows = tuple(item for item in state.collision_rows if not item.row.generic)
        if rows:
            return rows
        return self._build_theme_collision_rows(self._plate_rows_for_market(state), context=state.context)

    @staticmethod
    def _theme_judge_for_plate(state: StrategyConsoleState, plate_name: str) -> ThemeJudgeResult | None:
        if not plate_name or not state.theme_judge_map:
            return None
        return state.theme_judge_map.get(plate_name)

    def _auction_formal_ready(self, state: StrategyConsoleState) -> bool:
        return self._auction_anchor_ready(state)

    def _auction_metrics_atomic_ready(self, state: StrategyConsoleState) -> bool:
        if not self._auction_anchor_ready(state):
            return False
        summary = state.context.market_summary
        metric_values = (
            getattr(summary, "market_full_auc_amt", 0.0),
            getattr(summary, "context_auc_amt", 0.0),
            getattr(summary, "auction_top10_amount", 0.0),
            getattr(summary, "auction_top20_amount", 0.0),
            getattr(summary, "avg_bid_amt", 0.0),
        )
        return all(float(value or 0.0) > 0.0 for value in metric_values)

    def _auction_amount_readiness(
        self,
        state: StrategyConsoleState,
        *,
        amount: float,
        ratio: float | None = None,
    ) -> str:
        if not self._auction_anchor_ready(state):
            return "unavailable"
        amount_value = float(amount or 0.0)
        if amount_value > 0.0:
            if ratio is None:
                return "ready"
            return "ready" if float(ratio or 0.0) > 0.0 else "partial"
        return "warming_up"

    def _format_auction_amount_readiness(
        self,
        state: StrategyConsoleState,
        *,
        amount: float,
        ratio: float | None = None,
    ) -> str:
        readiness = self._auction_amount_readiness(state, amount=amount, ratio=ratio)
        if readiness == "ready":
            amount_text = self._fmt_amount_yi_precise(amount)
            if ratio is None:
                return amount_text
            return f"{amount_text} / {float(ratio):.2f}x"
        if readiness == "partial":
            return f"{self._fmt_amount_yi_precise(amount)} / 昨比就绪中"
        if readiness == "warming_up":
            return "汇总就绪中"
        return "--"

    def _expectation_ready(self, state: StrategyConsoleState) -> bool:
        return (
            self._hot_plate_render_mode(state) == "today"
            and self._auction_anchor_ready(state)
            and self._yest_limit_ready(state)
        )

    def _build_theme_collision_rows(
        self,
        rows: Iterable[AuctionPlateBucketStat],
        *,
        context: IntradayContext | None = None,
        auction_delta_stats: Iterable[AuctionSnapshotDeltaStat] = (),
    ) -> tuple[AuctionThemeCollisionStat, ...]:
        rows = tuple(row for row in rows if row.plate_name)
        if not rows:
            return ()
        yesterday_hot_rank_map = self._yesterday_hot_rank_map(context)
        plate_migration_map = getattr(getattr(context, "session_facts", None), "plate_migration_map", {}) if context is not None else {}
        delta_map = {item.plate_name: item for item in auction_delta_stats}
        capital_ranks = self._rank_bucket_rows(
            rows,
            key=lambda row: (
                row.hot_net_inflow_yi,
                row.hot_strength,
                row.auction_amount,
                row.hot_change_pct,
                row.primary_reason_hits,
                row.weighted_score,
            ),
        )
        limitup_ranks = self._rank_bucket_rows(
            rows,
            key=lambda row: (
                row.limit_up_count,
                row.highest_lb_days,
                row.yest_limit_count,
                row.leader_count,
                row.primary_reason_hits,
                row.weighted_score,
                row.auction_amount,
            ),
        )
        turn_ranks = self._rank_bucket_rows(
            rows,
            key=lambda row: (
                row.turn_strong_count,
                row.strong_lock_count,
                row.rebound_count,
                row.avg_current_pct,
                row.highest_lb_days,
                row.weighted_score,
            ),
        )
        hot_ranks = self._rank_bucket_rows_by_hot(rows)
        continuation_ranks = self._rank_bucket_rows(
            rows,
            key=lambda row: (
                row.yest_limit_count,
                row.highest_lb_days,
                row.leader_count,
                row.primary_reason_hits,
                row.secondary_reason_hits,
                row.weighted_score,
            ),
        )
        total = len(rows)
        built: list[AuctionThemeCollisionStat] = []
        for row in rows:
            capital_rank = capital_ranks[row.plate_name]
            limitup_rank = limitup_ranks[row.plate_name]
            turn_rank = turn_ranks[row.plate_name]
            hot_rank = hot_ranks[row.plate_name]
            yesterday_hot_rank = int(yesterday_hot_rank_map.get(row.plate_name, 999) or 999)
            continuation_rank = continuation_ranks[row.plate_name]
            hot_points = 0.0
            if row.hot_rank < 999 or row.hot_strength > 0 or row.hot_net_inflow_yi != 0:
                hot_points = self._collision_rank_points(hot_rank, total)
            collision_score = round(
                self._collision_rank_points(capital_rank, total) * 1.20
                + self._collision_rank_points(limitup_rank, total) * 1.35
                + self._collision_rank_points(turn_rank, total) * 1.15
                + hot_points * 0.95
                + self._collision_rank_points(continuation_rank, total) * 1.05
                + min(row.primary_reason_hits, 3) * 0.30
                + min(row.secondary_reason_hits, 3) * 0.12,
                4,
            )
            expectation_score = self._expected_theme_score(
                row,
                yesterday_hot_rank=yesterday_hot_rank,
                migration=plate_migration_map.get(row.plate_name),
            )
            expectation_label, expectation_delta = self._classify_theme_expectation_gap(
                row,
                capital_rank=capital_rank,
                limitup_rank=limitup_rank,
                turn_rank=turn_rank,
                hot_rank=hot_rank,
                yesterday_hot_rank=yesterday_hot_rank,
                expectation_score=expectation_score,
                context=context,
            )
            delta_stat = delta_map.get(row.plate_name)
            e_score = self._theme_eax_e_score(
                row,
                expectation_score=expectation_score,
                yesterday_hot_rank=yesterday_hot_rank,
                migration=plate_migration_map.get(row.plate_name),
            )
            a_score = self._theme_eax_a_score(
                row,
                capital_rank=capital_rank,
                limitup_rank=limitup_rank,
                turn_rank=turn_rank,
                hot_rank=hot_rank,
                total=total,
                delta_stat=delta_stat,
                context=context,
            )
            x_score = self._theme_eax_x_score(
                row,
                capital_rank=capital_rank,
                limitup_rank=limitup_rank,
                turn_rank=turn_rank,
                expectation_score=expectation_score,
                delta_stat=delta_stat,
                context=context,
            )
            fakeout_level = self._classify_theme_fakeout(
                row,
                capital_rank=capital_rank,
                limitup_rank=limitup_rank,
                turn_rank=turn_rank,
                context=context,
            )
            eax_label, eax_action = self._classify_eax_action(
                e_score,
                a_score,
                x_score,
                fakeout_level=fakeout_level,
            )
            built.append(
                AuctionThemeCollisionStat(
                    plate_name=row.plate_name,
                    row=row,
                    capital_rank=capital_rank,
                    limitup_rank=limitup_rank,
                    turn_rank=turn_rank,
                    hot_rank=hot_rank,
                    yesterday_hot_rank=yesterday_hot_rank,
                    continuation_rank=continuation_rank,
                    collision_score=collision_score,
                    expectation_score=expectation_score,
                    expectation_delta=expectation_delta,
                    expectation_label=expectation_label,
                    signal=self._classify_theme_collision_signal(
                        row,
                        capital_rank=capital_rank,
                        limitup_rank=limitup_rank,
                        turn_rank=turn_rank,
                        hot_rank=hot_rank,
                    ),
                    e_score=e_score,
                    a_score=a_score,
                    x_score=x_score,
                    eax_label=eax_label,
                    eax_action=eax_action,
                    fakeout_level=fakeout_level,
                )
            )
        built.sort(
            key=lambda item: (
                item.row.generic,
                -item.expectation_delta,
                -item.collision_score,
                item.limitup_rank,
                item.turn_rank,
                item.capital_rank,
                item.hot_rank,
                item.plate_name,
            )
        )
        return tuple(built)

    @staticmethod
    def _yesterday_hot_rank_map(context: IntradayContext | None) -> dict[str, int]:
        if context is None:
            return {}
        rank_map: dict[str, int] = {}
        facts = getattr(context, "session_facts", None)
        if facts is not None:
            for fact in getattr(facts, "hot_plate_yesterday", ()):
                plate_name = normalize_plate_name(getattr(fact, "plate_name", ""))
                if not plate_name:
                    continue
                try:
                    rank_map[plate_name] = int(getattr(fact, "rank", 999) or 999)
                except (TypeError, ValueError):
                    rank_map[plate_name] = 999
        if rank_map:
            return rank_map
        for plate_name, payload in getattr(context, "yesterday_hot_plate_map", {}).items():
            normalized = normalize_plate_name(plate_name)
            if not normalized or not isinstance(payload, dict):
                continue
            try:
                rank_map[normalized] = int(payload.get("rank", 999) or 999)
            except (TypeError, ValueError):
                rank_map[normalized] = 999
        return rank_map

    @staticmethod
    def _expected_theme_score(
        row: AuctionPlateBucketStat,
        *,
        yesterday_hot_rank: int,
        migration: object | None,
    ) -> float:
        score = 0.0
        if yesterday_hot_rank <= 3:
            score += max(4 - yesterday_hot_rank, 0) * 1.1
        elif yesterday_hot_rank <= 6:
            score += 0.8
        score += min(row.yest_limit_count, 4) * 0.55
        if row.highest_lb_days >= 2:
            score += min(row.highest_lb_days - 1, 3) * 0.45
        if migration is not None:
            present_yesterday = bool(getattr(migration, "present_yesterday", False))
            present_today = bool(getattr(migration, "present_today", False))
            strength_delta = float(getattr(migration, "strength_delta", 0.0) or 0.0)
            net_delta = float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0)
            if present_yesterday and present_today and (strength_delta > 0 or net_delta > 0):
                score += 0.4
            elif present_yesterday and present_today and (strength_delta < 0 or net_delta < 0):
                score -= 0.3
        return round(score, 4)

    @staticmethod
    def _classify_theme_expectation_gap(
        row: AuctionPlateBucketStat,
        *,
        capital_rank: int,
        limitup_rank: int,
        turn_rank: int,
        hot_rank: int,
        yesterday_hot_rank: int,
        expectation_score: float,
        context: IntradayContext | None = None,
    ) -> tuple[str, float]:
        front_comparison = build_market_topn_slice_comparison(getattr(context, "market_summary", None))
        weak_front = front_comparison.is_weak
        strong_front = front_comparison.is_strong
        has_hot_truth = row.hot_rank < 999 or row.hot_strength > 0 or row.hot_net_inflow_yi != 0
        hot_top = has_hot_truth and hot_rank <= 2
        actual_strong = (
            limitup_rank <= 2
            and turn_rank <= 2
            and (capital_rank <= 2 or hot_top)
            and row.limit_up_count >= 2
        )
        actual_core_strong = (
            row.leader_count >= 1
            and (
                (capital_rank <= 2 and row.auction_amount >= 60_000_000)
                or (hot_top and row.auction_amount >= 50_000_000)
                or (turn_rank <= 2 and (row.turn_strong_count >= 1 or row.highest_lb_days >= 2))
            )
        )
        actual_good = (
            capital_rank <= 2
            or limitup_rank <= 2
            or turn_rank <= 2
            or (hot_top and row.auction_amount >= 50_000_000)
        )
        has_expectation = expectation_score >= 1.5 or yesterday_hot_rank <= 6 or row.yest_limit_count >= 1
        strong_expectation = expectation_score >= 2.6 or yesterday_hot_rank <= 3 or row.yest_limit_count >= 2 or row.highest_lb_days >= 3
        if strong_expectation:
            if actual_strong:
                label, delta = ("强更强", 2.4)
            elif actual_core_strong:
                label, delta = ("局部超预期", 1.6)
            elif actual_good and (row.turn_strong_count >= 1 or row.leader_count >= 1):
                label, delta = ("符合预期", 1.2)
            else:
                label, delta = ("低于预期", -2.2)
        elif has_expectation:
            if actual_strong:
                label, delta = ("超预期", 2.0)
            elif actual_core_strong:
                label, delta = ("局部超预期", 1.4)
            elif actual_good:
                label, delta = ("有预期差", 0.8)
            else:
                label, delta = ("不及预期", -1.4)
        else:
            if actual_strong:
                label, delta = ("超预期", 1.8)
            elif actual_core_strong:
                label, delta = ("新强试错", 1.0)
            elif actual_good and (row.turn_strong_count >= 1 or row.limit_up_count >= 1):
                label, delta = ("新强试错", 0.9)
            else:
                label, delta = ("无明显预期差", 0.0)

        # Weak front-row backdrop means relative strength is more valuable.
        if weak_front:
            if label in {"符合预期", "有预期差", "新强试错"} and actual_core_strong:
                label, delta = ("局部超预期", max(delta, 1.4))
            elif label in {"低于预期", "不及预期"} and actual_good and (row.turn_strong_count >= 1 or row.leader_count >= 1):
                label, delta = ("符合预期", max(delta, 0.8))
        elif strong_front:
            if label == "局部超预期" and not actual_strong and row.limit_up_count <= 1 and row.turn_strong_count <= 1:
                label, delta = ("符合预期", min(delta, 1.2))
            elif label == "超预期" and not actual_strong:
                label, delta = ("局部超预期", min(delta, 1.6))
        return (label, delta)

    @staticmethod
    def _theme_eax_e_score(
        row: AuctionPlateBucketStat,
        *,
        expectation_score: float,
        yesterday_hot_rank: int,
        migration: object | None,
    ) -> float:
        score = max(expectation_score, 0.0) * 1.55
        cohesion = AuctionRuntimeController._theme_cohesion_level(row)
        if yesterday_hot_rank <= 3:
            score += 1.2
        elif yesterday_hot_rank <= 6:
            score += 0.55
        score += min(row.yest_limit_count, 4) * 0.35
        score += min(max(row.highest_lb_days - 1, 0), 4) * 0.25
        if cohesion == "strong":
            score += 0.8
        elif cohesion == "medium":
            score += 0.35
        elif cohesion == "weak":
            score -= 0.55
        if row.hot_strength >= 3000:
            score += 0.75
        elif row.hot_strength >= 1800:
            score += 0.35
        if migration is not None:
            present_yesterday = bool(getattr(migration, "present_yesterday", False))
            present_today = bool(getattr(migration, "present_today", False))
            strength_delta = float(getattr(migration, "strength_delta", 0.0) or 0.0)
            net_delta = float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0)
            if present_yesterday and not present_today:
                score -= 0.7
            elif present_yesterday and present_today and (strength_delta > 0 or net_delta > 0):
                score += 0.45
            elif present_yesterday and present_today and (strength_delta < 0 or net_delta < 0):
                score -= 0.35
        return round(min(max(score, 0.0), 10.0), 1)

    @staticmethod
    def _theme_eax_a_score(
        row: AuctionPlateBucketStat,
        *,
        capital_rank: int,
        limitup_rank: int,
        turn_rank: int,
        hot_rank: int,
        total: int,
        delta_stat: AuctionSnapshotDeltaStat | None,
        context: IntradayContext | None = None,
    ) -> float:
        def rank_points(rank: int, weight: float) -> float:
            if total <= 0 or rank <= 0:
                return 0.0
            if rank == 1:
                return weight
            if rank == 2:
                return weight * 0.72
            if rank == 3:
                return weight * 0.45
            return max(weight * 0.18 * (total - rank + 1) / max(total, 1), 0.0)

        score = (
            rank_points(capital_rank, 2.0)
            + rank_points(limitup_rank, 2.2)
            + rank_points(turn_rank, 1.8)
            + (rank_points(hot_rank, 1.4) if row.hot_rank < 999 or row.hot_strength > 0 else 0.0)
        )
        score += min(row.auction_amount / 100_000_000, 2.0)
        total_breadth = row.red_count + row.green_count
        if total_breadth > 0:
            red_ratio = row.red_count / total_breadth
            if red_ratio >= 0.7:
                score += 0.7
            elif red_ratio <= 0.35:
                score -= 0.45
        if row.avg_current_pct >= 0.04:
            score += 0.55
        elif row.avg_current_pct < 0:
            score -= 0.55
        if delta_stat is not None:
            if delta_stat.amount_delta_24_25 > 0:
                score += min(delta_stat.amount_delta_24_25 / 80_000_000, 1.2)
            if delta_stat.bid_amount_delta_24_25 > 0:
                score += min(delta_stat.bid_amount_delta_24_25 / 30_000_000, 0.7)
            if delta_stat.change_pct_delta_avg > 0:
                score += min(delta_stat.change_pct_delta_avg / 2.0, 0.6)
            elif delta_stat.change_pct_delta_avg < -1.0:
                score -= min(abs(delta_stat.change_pct_delta_avg) / 3.0, 1.0)
        front_comparison = build_market_topn_slice_comparison(getattr(context, "market_summary", None))
        weak_front = front_comparison.is_weak
        strong_front = front_comparison.is_strong
        if weak_front and (capital_rank <= 3 or turn_rank <= 3) and (row.turn_strong_count >= 1 or row.limit_up_count >= 1):
            score += 0.45
        elif strong_front and capital_rank <= 2 and row.limit_up_count <= 1 and row.turn_strong_count <= 1:
            score -= 0.35
        return round(min(max(score, 0.0), 10.0), 1)

    @staticmethod
    def _theme_eax_x_score(
        row: AuctionPlateBucketStat,
        *,
        capital_rank: int,
        limitup_rank: int,
        turn_rank: int,
        expectation_score: float,
        delta_stat: AuctionSnapshotDeltaStat | None,
        context: IntradayContext | None,
    ) -> float:
        score = 0.0
        cohesion = AuctionRuntimeController._theme_cohesion_level(row)
        summary = getattr(context, "market_summary", None)
        front_comparison = build_market_topn_slice_comparison(summary)
        headshot_rate = float(getattr(summary, "headshot_rate", 0.0) or 0.0) if summary is not None else 0.0
        promotion_rate = float(getattr(summary, "promotion_rate", 0.0) or 0.0) if summary is not None else 0.0
        weak_front = front_comparison.is_weak
        strong_front = front_comparison.is_strong
        if headshot_rate >= 0.12:
            score += 2.0
        elif headshot_rate >= 0.08:
            score += 1.0
        if 0 < promotion_rate <= 0.15:
            score += 1.2
        if row.avg_open_pct >= 0.07 and row.limit_up_count <= 0:
            score += 1.1
        if row.hot_capital_behavior <= -0.3 and row.hot_change_pct > 0:
            score += 1.3
        if expectation_score >= 2.6 and (limitup_rank > 3 or turn_rank > 3):
            score += 1.2
        if capital_rank <= 2 and limitup_rank > 3:
            score += 0.8
        if delta_stat is not None:
            if delta_stat.amount_delta_24_25 >= 50_000_000 and delta_stat.change_pct_delta_avg <= -1.0:
                score += 2.4
            if delta_stat.bid_amount_delta_24_25 < 0 and delta_stat.amount_delta_24_25 > 0:
                score += 1.0
            if delta_stat.amount_ratio_avg >= 1.8 and delta_stat.change_pct_delta_avg <= 0:
                score += 0.8
        total_breadth = row.red_count + row.green_count
        red_ratio = (row.red_count / total_breadth) if total_breadth > 0 else 0.0
        if capital_rank <= 3 and limitup_rank > 10:
            score += 1.3
        if capital_rank <= 3 and turn_rank > 10:
            score += 1.0
        if row.auction_amount >= 80_000_000 and row.limit_up_count <= 1:
            score += 1.0
        if row.avg_current_pct < 0:
            score += 1.2
        if total_breadth > 0 and red_ratio < 0.35:
            score += 1.1
        if cohesion == "weak":
            score += 1.4
        elif cohesion == "medium":
            score += 0.4
        if weak_front:
            if row.avg_open_pct >= 0.05 and row.limit_up_count <= 1 and row.turn_strong_count <= 1:
                score += 0.9
            elif capital_rank <= 3 and (row.turn_strong_count >= 1 or row.leader_count >= 1):
                score -= 0.35
        elif strong_front and capital_rank <= 3 and row.limit_up_count <= 1 and row.turn_strong_count <= 1:
            score += 0.45
        return round(min(max(score, 0.0), 10.0), 1)

    @staticmethod
    def _theme_cohesion_level(row: AuctionPlateBucketStat) -> str:
        linked_front = 0
        if row.leader_count >= 2:
            linked_front += 1
        if row.limit_up_count >= 2:
            linked_front += 1
        if row.turn_strong_count >= 2 or row.strong_lock_count >= 1:
            linked_front += 1
        if row.auction_symbol_count >= 3 and row.symbol_count >= 4:
            linked_front += 1
        if linked_front >= 3:
            return "strong"
        if linked_front >= 2:
            return "medium"
        return "weak"

    @staticmethod
    def _classify_theme_fakeout(
        row: AuctionPlateBucketStat,
        *,
        capital_rank: int,
        limitup_rank: int,
        turn_rank: int,
        context: IntradayContext | None = None,
    ) -> str:
        flags = 0
        total_breadth = row.red_count + row.green_count
        red_ratio = (row.red_count / total_breadth) if total_breadth > 0 else 0.5
        front_comparison = build_market_topn_slice_comparison(getattr(context, "market_summary", None))
        if capital_rank <= 3:
            if row.avg_current_pct <= 0:
                flags += 1
            if red_ratio < 0.40:
                flags += 1
            if limitup_rank > 10 or row.limit_up_count <= 1:
                flags += 1
            if turn_rank > 10 or row.turn_strong_count <= 1:
                flags += 1
            if front_comparison.is_weak and row.avg_open_pct >= 0.04:
                flags += 1
        if flags >= 3:
            return "strong"
        if flags >= 2:
            return "warn"
        return "none"

    @staticmethod
    def _classify_eax_action(
        e_score: float,
        a_score: float,
        x_score: float,
        *,
        fakeout_level: str = "none",
    ) -> tuple[str, str]:
        if fakeout_level == "strong":
            return ("疑似骗炮", "不开盘确认不做")
        if fakeout_level == "warn" and x_score >= 4.0:
            return ("疑似假强", "仅看龙头，不做扩散")
        if x_score >= 6.0:
            return ("强但过热", "只看换手，不追加速")
        if e_score >= 6.0 and a_score <= 4.0:
            return ("低于预期", "回避/等修复")
        if e_score <= 4.0 and a_score >= 6.0 and x_score < 5.0:
            return ("超预期", "小仓试错")
        if e_score >= 6.0 and a_score >= 6.0 and x_score < 5.0:
            return ("符合/强化", "前排换手确认")
        if a_score >= 5.0 and x_score < 4.0:
            return ("局部转强", "仅观察前排")
        return ("无明显差", "只观察")

    @staticmethod
    def _rank_bucket_rows(
        rows: Iterable[AuctionPlateBucketStat],
        *,
        key,
    ) -> dict[str, int]:
        ordered = sorted(rows, key=key, reverse=True)
        return {row.plate_name: idx + 1 for idx, row in enumerate(ordered)}

    @staticmethod
    def _rank_bucket_rows_by_hot(rows: Iterable[AuctionPlateBucketStat]) -> dict[str, int]:
        ordered = sorted(
            rows,
            key=lambda row: (
                row.hot_rank if row.hot_rank < 999 else 9999,
                -row.hot_strength,
                -row.hot_net_inflow_yi,
                -row.hot_change_pct,
                -row.primary_reason_hits,
                -row.weighted_score,
            ),
        )
        return {row.plate_name: idx + 1 for idx, row in enumerate(ordered)}

    @staticmethod
    def _collision_rank_points(rank: int, total: int) -> float:
        if rank <= 0 or total <= 0:
            return 0.0
        return float(max(total - rank + 1, 0))

    @staticmethod
    def _classify_theme_collision_signal(
        row: AuctionPlateBucketStat,
        *,
        capital_rank: int,
        limitup_rank: int,
        turn_rank: int,
        hot_rank: int,
    ) -> str:
        capital_top = capital_rank <= 2
        limit_top = limitup_rank <= 2
        turn_top = turn_rank <= 2
        hot_top = hot_rank <= 2 and (row.hot_rank < 999 or row.hot_strength > 0 or row.hot_net_inflow_yi != 0)
        if limit_top and turn_top and (capital_top or hot_top) and row.limit_up_count >= 2:
            return "共振主攻"
        if row.yest_limit_count >= 2 and limit_top and (turn_top or row.highest_lb_days >= 2):
            return "连板延续"
        if capital_top and hot_top and row.limit_up_count <= 1:
            return "资金试错"
        if hot_top and limit_top and row.limit_up_count >= 2:
            return "热板补强"
        if capital_top and not limit_top:
            return "有量无板"
        if limit_top and not capital_top:
            return "有板待放量"
        return "观察跟踪"

    def _top_theme_by_capital(self, state: StrategyConsoleState, *, market_scope: bool = False) -> AuctionPlateBucketStat | None:
        rows = self._plate_rows_for_market(state) if market_scope else self._plate_rows_for_decision(state)
        if not rows:
            return None
        return max(
            rows,
            key=lambda row: (
                row.hot_net_inflow_yi,
                row.hot_strength,
                row.auction_amount,
                row.hot_change_pct,
                row.primary_reason_hits,
                row.weighted_score,
            ),
        )

    def _top_theme_by_limitups(self, state: StrategyConsoleState, *, market_scope: bool = False) -> AuctionPlateBucketStat | None:
        rows = self._plate_rows_for_market(state) if market_scope else self._plate_rows_for_decision(state)
        if not rows:
            return None
        return max(
            rows,
            key=lambda row: (
                row.limit_up_count,
                row.highest_lb_days,
                row.leader_count,
                row.primary_reason_hits,
                row.weighted_score,
                row.auction_amount,
            ),
        )

    def _top_theme_by_turn_strong(self, state: StrategyConsoleState, *, market_scope: bool = False) -> AuctionPlateBucketStat | None:
        rows = self._plate_rows_for_market(state) if market_scope else self._plate_rows_for_decision(state)
        if not rows:
            return None
        return max(
            rows,
            key=lambda row: (
                row.turn_strong_count,
                row.strong_lock_count,
                row.rebound_count,
                row.avg_current_pct,
                row.highest_lb_days,
                row.weighted_score,
            ),
        )

    def _theme_red_green_ratio_text(self, row: AuctionPlateBucketStat) -> str:
        total = row.red_count + row.green_count
        if total <= 0:
            return "--"
        return f"{row.red_count}:{row.green_count}"

    def _theme_trade_posture_text(self, row: AuctionPlateBucketStat) -> str:
        if row.hot_capital_behavior <= -0.3 and row.hot_change_pct > 0:
            return "兑现回避"
        if row.limit_up_count >= 2 and row.highest_lb_days >= 2 and row.turn_strong_count >= 1:
            return "strong"
        if row.limit_up_count >= 2 and (row.strong_lock_count >= 1 or row.highest_lb_days >= 2):
            return "strong"
        if row.limit_up_count >= 2 and row.turn_strong_count <= 0:
            return "strong"
        if row.hot_net_inflow_yi > 0 and row.limit_up_count <= 1:
            return "资金试错"
        if row.limit_up_count >= 1 and row.symbol_count >= 3:
            return "首板扩散"
        return "观察题材"

    @staticmethod
    def _expectation_gap_display_text(label: str) -> str:
        mapping = {
            "强更强": "强预期兑现",
            "局部超预期": "局部超预期",
            "符合预期": "预期内偏强",
            "低于预期": "强预期落空",
            "超预期": "弱预期转强",
            "有预期差": "有承接待确认",
            "不及预期": "有预期偏弱",
            "新强试错": "新方向试强",
            "无明显预期差": "无明显预期差",
        }
        return mapping.get(label, label)

    def _theme_zone_observation_text(self, row: AuctionPlateBucketStat) -> str:
        posture = self._theme_trade_posture_text(row)
        _theme_state, trade_state, cue = self._theme_trade_profile(row)
        if posture in {"主线攻击", "主线延续"}:
            return f"{posture},{cue}"
        if posture == "兑现分歧":
            return f"{posture},不接后排"
        if posture == "兑现回避":
            return f"{posture},先看高标反馈"
        if posture == "资金试错":
            return f"先手试错,{cue}"
        if posture == "首板扩散":
            return f"{posture},{cue}"
        return f"{trade_state},{cue}"

    @staticmethod
    def _trade_conclusion_text(conclusion: str) -> str:
        mapping = {
            "old_mainline_strong_continue": "旧主线延续: 强者恒强",
            "old_mainline_weak_continue": "旧主线延续: 分歧延续",
            "old_mainline_distribution": "旧主线兑现: 高位派发",
            "switch_expansion_confirmed": "切换确认: 新主线扩散",
            "switch_partially_confirmed": "切换确认: 但仍分歧",
            "switch_wait_confirm": "切换观察: 等开盘确认",
            "switch_failed": "切换失败: 回流旧线",
            "leader_only_alive": "龙头独活: 板块掉队",
            "high_event_self_excited": "事件自嗨: 群体跟随弱",
            "independent_hug_failed": "抱团失败: 承接不足",
            "rotation_noise": "轮动噪音: 持续性弱",
        }
        return mapping.get(str(conclusion or "").strip(), "-")

    def _theme_conclusion_for_plate(self, state: StrategyConsoleState, plate_name: str) -> str:
        bundle = state.bundle
        if bundle is None:
            return "unknown"
        theme_context_map = getattr(bundle, "theme_context_map", None)
        if not isinstance(theme_context_map, dict):
            return "unknown"
        theme_context = theme_context_map.get(plate_name)
        if theme_context is None:
            return "unknown"
        return str(theme_context.trade_conclusion or "unknown")

    @staticmethod
    def _theme_action_class_text(action_class: str) -> str:
        mapping = {
            "main_attack": "主攻确认",
            "front_row_confirm": "前排确认",
            "observe": "只观察",
            "trap_avoid": "兑现回避",
            "anchor_only": "仅龙头可看",
        }
        return mapping.get(action_class, action_class or "只观察")

    def _is_theme_falsified_but_leader_alive(
        self,
        state: StrategyConsoleState,
        *,
        plate_name: str,
    ) -> bool:
        judge = self._theme_judge_for_plate(state, plate_name)
        if judge is None:
            return False
        conclusion = self._theme_conclusion_for_plate(state, plate_name)
        execution_state = self._external_validation_state(judge.validation_state)
        return execution_state == "falsified" and (
            conclusion == "leader_only_alive" or judge.action_class == "anchor_only"
        )

    def _snapshot_is_falsified_but_leader_alive(
        self,
        state: StrategyConsoleState,
        snapshot: StockStateSnapshot | None,
    ) -> bool:
        judge, matched_plate = self._matched_theme_judge(state, snapshot)
        if judge is None:
            return False
        plate_name = normalize_plate_name(matched_plate)
        if not plate_name:
            return False
        return self._is_theme_falsified_but_leader_alive(state, plate_name=plate_name)

    @staticmethod
    def _theme_action_priority(action_class: str) -> int:
        mapping = {
            "main_attack": 4,
            "front_row_confirm": 3,
            "anchor_only": 2,
            "observe": 1,
            "trap_avoid": 0,
        }
        return mapping.get(action_class, -1)

    def _top_yesterday_hot_plates(self, state: StrategyConsoleState, *, limit: int = 3) -> tuple[str, ...]:
        ordered: list[str] = []
        facts = getattr(state.context, "session_facts", None)
        if facts is not None:
            for fact in getattr(facts, "hot_plate_yesterday", ()):
                name = normalize_plate_name(getattr(fact, "plate_name", ""))
                if name and name != "-" and name not in ordered:
                    ordered.append(name)
                if len(ordered) >= limit:
                    return tuple(ordered)
        payloads: list[tuple[int, str]] = []
        for plate_name, payload in getattr(state.context, "yesterday_hot_plate_map", {}).items():
            if not isinstance(payload, dict):
                continue
            name = normalize_plate_name(plate_name)
            if not name or name == "-" or name in ordered:
                continue
            try:
                rank = int(payload.get("rank", 999) or 999)
            except (TypeError, ValueError):
                rank = 999
            payloads.append((rank, name))
        for _rank, name in sorted(payloads, key=lambda item: (item[0], item[1])):
            ordered.append(name)
            if len(ordered) >= limit:
                break
        return tuple(ordered)

    def _background_mainline_pair(self, state: StrategyConsoleState) -> tuple[str, str]:
        summary = state.context.market_summary
        ordered: list[str] = []
        for raw_name in (
            *self._top_yesterday_hot_plates(state, limit=3),
            summary.mainline_sector,
            summary.top_plate_name,
            *(row.plate_name for row in self._plate_rows_for_market(state)[:3]),
        ):
            name = normalize_plate_name(raw_name)
            if not name or name == "-" or name in ordered:
                ordered.append(name)
        if not ordered:
            return "-", "-"
        lead = ordered[0]
        secondary = next((name for name in ordered[1:] if name != lead), "-")
        return lead, secondary

    def _render_auction_execution_map(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        output_summary = self._playbook_output_summary_for_state(state)
        title = self._execution_map_title(phase_label)
        if output_summary is None:
            return (
                title,
                "  进攻 | 无",
                "  跟踪 | 无",
                "  修复 | 无",
                "  回避 | 无",
            )

        primary_actions = tuple(getattr(output_summary, "primary_actions", ()) or ())
        watch_actions = tuple(getattr(output_summary, "watch_actions", ()) or ())
        repair_actions = tuple(getattr(output_summary, "repair_actions", ()) or ())
        avoid_actions = tuple(getattr(output_summary, "avoid_actions", ()) or ())
        return (
            title,
            f"  进攻 | {' ; '.join(primary_actions[:4]) if primary_actions else '无'}",
            f"  跟踪 | {' ; '.join(watch_actions[:6]) if watch_actions else '无'}",
            f"  修复 | {' ; '.join(repair_actions[:4]) if repair_actions else '无'}",
            f"  回避 | {' ; '.join(avoid_actions[:5]) if avoid_actions else '无'}",
        )

    def _render_playbook_pool(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        output_summary = self._playbook_output_summary_for_state(state)
        if state.bundle is None or output_summary is None:
            title = "【明日观察池】个股 | 动作 | 题材 | Playbook | 风险 | 证据 | 证伪" if phase_label == "postmarket" else "【核心观察池】个股 | 动作 | 题材 | Playbook | 风险 | 证据 | 证伪"
            return (title, "  -", "模式提示 | 无策略层输出，保持观察")

        primary_actions = list(getattr(output_summary, "primary_actions", ()) or ())
        watch_actions = list(getattr(output_summary, "watch_actions", ()) or ())
        repair_actions = list(getattr(output_summary, "repair_actions", ()) or ())
        avoid_actions = list(getattr(output_summary, "avoid_actions", ()) or ())
        aligned_watch_actions, off_mainline_watch_actions = self._partition_watch_actions(
            state,
            output_summary,
            watch_actions,
        )
        reasons = list(self._summary_reason_lines(output_summary))
        reject_text = self._summary_reject_text(output_summary)
        if not reject_text or reject_text == "-":
            reject_text = " ; ".join(tuple(output_summary.reject_reasons or avoid_actions[:5] or ("无",)))
        mode_note = output_summary.mode_note or self._aligned_mode_risk_prompt(state, phase_label=phase_label)
        realtime_primary_mode = phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"} and bool(primary_actions)
        if phase_label == "postmarket":
            display_parts = (primary_actions[:3] + aligned_watch_actions[:1])[:4] if primary_actions else (aligned_watch_actions[:4] or watch_actions[:4] or ["-"])
        elif phase_label in {"opening", "open_confirm", "intraday"}:
            display_parts = (primary_actions[:2] + aligned_watch_actions[:1])[:3] if realtime_primary_mode else (aligned_watch_actions[:2] or watch_actions[:2] or primary_actions[:2] or ["-"])
        else:
            display_parts = primary_actions[:4] if realtime_primary_mode else (watch_actions[:4] or primary_actions[:4] or ["-"])
        if repair_actions:
            supplement_title = "修复补充"
            supplement_parts = repair_actions[:3]
        elif realtime_primary_mode and watch_actions:
            supplement_title = "观察补充"
            supplement_parts = watch_actions[:1] if phase_label in {"opening", "open_confirm", "intraday"} else watch_actions[:3]
        elif phase_label == "postmarket" and off_mainline_watch_actions:
            supplement_title = "非主线观察"
            supplement_parts = off_mainline_watch_actions[:3]
        elif avoid_actions:
            supplement_title = "回避补充"
            supplement_parts = avoid_actions[:3]
        else:
            supplement_title = "观察补充"
            supplement_parts = ["无"]
        if not reasons:
            reasons = list(
                self._default_playbook_reason_lines(
                    primary_actions=primary_actions,
                    watch_actions=watch_actions,
                    repair_actions=repair_actions,
                    avoid_actions=avoid_actions,
                )
            )
        title = (
            "【主买点池】个股 | 动作 | 题材 | Playbook | 风险 | 证据 | 证伪"
            if realtime_primary_mode
            else "【核心观察池】个股 | 动作 | 题材 | Playbook | 风险 | 证据 | 证伪"
        )
        if phase_label == "postmarket":
            title = "【明日观察池】个股 | 动作 | 题材 | Playbook | 风险 | 证据 | 证伪"
        reason_title = "主买理由" if realtime_primary_mode else ("明日理由" if phase_label == "postmarket" else "观察理由")
        narrative_text = self._summary_narrative_text(output_summary)
        migration_text = self._summary_migration_text(output_summary)
        quant_text = self._summary_quant_text(output_summary)
        invalidation_text = self._summary_invalidation_text(output_summary)
        return (
            title,
            *[f"  {item}" for item in display_parts],
            f"主叙事 | {narrative_text}",
            f"时间迁移 | {migration_text}",
            f"{reason_title} | {' ; '.join(reasons)}",
            f"证伪条件 | {invalidation_text}",
            f"【{supplement_title}】{' ; '.join(supplement_parts)}",
            f"淘汰理由 | {reject_text}",
            f"数量摘要 | {quant_text}",
            f"模式提示 | {mode_note}",
        )

    def _aligned_mode_risk_prompt(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        base = self._mode_risk_prompt(state, phase_label=phase_label)
        if self._summary_mode_risk_prompt(state, phase_label=phase_label):
            return base
        preferred_plates = (
            self._phase_priority_plates(state, phase_label=phase_label)
            if phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday", "postmarket"}
            else ()
        )
        if not preferred_plates:
            return base
        playbook_text = f"聚焦{'/'.join(preferred_plates[:2])}"
        if playbook_text in base:
            return base
        return f"{base} | {playbook_text}"

    def _playbook_output_summary_for_state(self, state: StrategyConsoleState):
        decision_bundle = state.bundle.decision_bundle if state.bundle is not None else None
        return getattr(decision_bundle, "playbook_output_summary", None) if decision_bundle is not None else None

    def _global_market_decision_for_state(self, state: StrategyConsoleState):
        decision_bundle = state.bundle.decision_bundle if state.bundle is not None else None
        return getattr(decision_bundle, "global_decision", None) if decision_bundle is not None else None

    @staticmethod
    def _summary_reason_text_map(output_summary) -> dict[str, str]:
        reason_map: dict[str, str] = {}

        def _collect(lines: Iterable[str]) -> None:
            for raw in lines:
                text = str(raw or "").strip()
                if not text or "=" not in text:
                    continue
                symbol, payload = text.split("=", 1)
                symbol = str(symbol or "").strip()
                if not symbol or symbol in reason_map:
                    continue
                parts: dict[str, str] = {}
                for item in payload.split(";"):
                    item = str(item or "").strip()
                    if not item:
                        continue
                    if ":" in item:
                        key, value = item.split(":", 1)
                        parts[str(key or "").strip()] = str(value or "").strip()
                    elif "status" not in parts:
                        parts["status"] = item
                reason = parts.get("reason", "")
                if reason and reason != "-":
                    reason_map[symbol] = reason
                    continue
                risk = parts.get("risk", "")
                if risk and risk != "-":
                    reason_map[symbol] = risk
                    continue
                playbook = parts.get("playbook", "")
                path_type = parts.get("path", "")
                bucket = parts.get("bucket", "")
                detail = next((item for item in (playbook, path_type, bucket) if item and item != "-"), "")
                status = parts.get("status", "")
                if detail and status:
                    reason_map[symbol] = f"{status}/{detail}"
                elif detail:
                    reason_map[symbol] = detail
                elif status:
                    reason_map[symbol] = status

        _collect(tuple(getattr(output_summary, "primary_reasons", ()) or ()))
        _collect(tuple(getattr(output_summary, "watch_reasons", ()) or ()))
        _collect(tuple(getattr(output_summary, "reject_reasons", ()) or ()))
        return reason_map

    @staticmethod
    def _summary_compact_value_map(line: str) -> dict[str, str]:
        values: dict[str, str] = {}
        for item in str(line or "").split(";"):
            item = str(item or "").strip()
            if not item or "=" not in item:
                continue
            key, value = item.split("=", 1)
            key = str(key or "").strip()
            value = str(value or "").strip()
            if key:
                values[key] = value
        return values

    def _summary_narrative_text(self, output_summary) -> str:
        lines = tuple(getattr(output_summary, "narrative_lines", ()) or ())
        if not lines:
            return "-"
        primary = self._summary_compact_value_map(lines[0])
        parts: list[str] = []
        main_theme = normalize_plate_name(primary.get("main", ""))
        script_text = self._market_script_prompt_text(primary.get("script", ""))
        secondary = primary.get("secondary", "-")
        cap = primary.get("cap", "")
        if main_theme and script_text:
            parts.append(f"{main_theme}={script_text}")
        elif main_theme:
            parts.append(main_theme)
        if secondary and secondary != "-":
            parts.append(f"副线={secondary}")
        if cap:
            parts.append(f"仓位={cap}")
        for line in lines[1:]:
            values = self._summary_compact_value_map(line)
            if "focus_stress" in values:
                parts.append(
                    f"焦点压力={values.get('focus_stress', '-')}/扩散={values.get('spread', '-')}/题材={values.get('themes', '0')}/独活={values.get('dragon_alone', '0')}"
                )
                continue
            if "hot_anchor" in values:
                parts.append(
                    f"热板={values.get('hot_anchor', '-')}/主锚={values.get('primary', '-')}/轮动={values.get('rotate', '-')}/风险={values.get('risk', '-')}"
                )
                continue
            if "playbooks" in values or "blocked" in values:
                active_text = str(values.get("playbooks", "")).removeprefix("active:") or "-"
                blocked_text = values.get("blocked", "-")
                parts.append(f"剧本=活跃{active_text}/阻塞{blocked_text}")
                continue
            text = str(line or "").strip()
            if text:
                parts.append(text)
        return " ; ".join(parts[:4]) or "-"

    def _summary_migration_text(self, output_summary) -> str:
        lines = tuple(getattr(output_summary, "migration_lines", ()) or ())
        if not lines:
            return "-"
        formatted: list[str] = []
        for line in lines[:4]:
            text = str(line or "").strip()
            if not text:
                continue
            if text.startswith("hot:") or text.startswith("global:") or text.startswith("time:") or ":theme=" in text:
                continue
            formatted.append(text)
        return " ; ".join(formatted) or "暂无清晰迁移链"

    def _summary_quant_text(self, output_summary) -> str:
        lines = tuple(getattr(output_summary, "quant_lines", ()) or ())
        if not lines:
            return "-"
        parts: list[str] = []
        for line in lines[:4]:
            text = str(line or "").strip()
            if not text:
                continue
            if text.startswith("global:"):
                values = self._summary_compact_value_map(text.removeprefix("global:"))
                parts.append(
                    "全局:"
                    f"确认={values.get('confirmed', '-')}"
                    f"/观察={values.get('watch', '-')}"
                    f"/风险={values.get('risk', '-')}"
                    f"/热板主锚={values.get('hot_primary', '-')}"
                    f"/焦点压力={values.get('focus_stress', '-')}"
                    f"/仓位={values.get('cap', '-')}"
                )
                continue
            if text.startswith("hot:"):
                values = self._summary_compact_value_map(text.removeprefix("hot:"))
                parts.append(
                    "热板:"
                    f"状态={values.get('state', '-')}"
                    f"/主攻={values.get('primary', '-')}"
                    f"/延续={values.get('continue', '-')}"
                    f"/轮动={values.get('rotate', '-')}"
                    f"/风险={values.get('risk', '-')}"
                    f"/龙头={values.get('top', '-')}"
                )
                continue
            if text.startswith("time:"):
                values = self._summary_compact_value_map(text.removeprefix("time:"))
                parts.append(
                    "时序:"
                    f"状态={values.get('state', '-')}"
                    f"/目标题材={values.get('targets', '-')}"
                    f"/衰竭={values.get('fading', '-')}"
                    f"/增强={values.get('rolling_acc', '-')}"
                    f"/撤退={values.get('rolling_out', '-')}"
                    f"/修复={values.get('rolling_repair', '-')}"
                )
                continue
            if "=" in text:
                symbol, payload = text.split("=", 1)
                values = self._summary_compact_value_map(payload)
                theme_name = normalize_plate_name(values.get("theme", ""))
                metric_parts = [
                    f"题材={theme_name or '-'}",
                    f"2m={values.get('2m', '-')}",
                    f"1m={values.get('1m', '-')}",
                    f"竞价={values.get('auc', '-')}",
                    f"开盘={values.get('open', '-')}",
                    f"现涨={values.get('now', '-')}",
                ]
                parts.append(f"{symbol}:{'/'.join(metric_parts)}")
                continue
            parts.append(text)
        return " ; ".join(parts[:3]) or "-"

    def _summary_invalidation_text(self, output_summary) -> str:
        points = tuple(getattr(output_summary, "invalidation_points", ()) or ())
        texts = [self._invalidation_point_text(point) for point in points[:4] if self._invalidation_point_text(point)]
        return " ; ".join(texts) if texts else "-"

    def _summary_reject_text(self, output_summary) -> str:
        reasons = tuple(getattr(output_summary, "reject_reasons", ()) or ())
        if not reasons:
            return "-"
        reason_map = self._summary_reason_text_map(output_summary)
        parts: list[str] = []
        for line in reasons[:4]:
            text = str(line or "").strip()
            if not text or "=" not in text:
                continue
            symbol, _payload = text.split("=", 1)
            symbol = str(symbol or "").strip()
            reason = reason_map.get(symbol, "")
            parts.append(f"{symbol}:{reason}" if reason else text)
        return " ; ".join(parts) if parts else "-"

    def _summary_reason_lines(self, output_summary) -> tuple[str, ...]:
        raw_lines = tuple(
            getattr(output_summary, "primary_reasons", ()) or getattr(output_summary, "watch_reasons", ()) or ()
        )
        if not raw_lines:
            return ()
        reason_map = self._summary_reason_text_map(output_summary)
        parts: list[str] = []
        for line in raw_lines[:5]:
            text = str(line or "").strip()
            if not text or "=" not in text:
                continue
            symbol, _payload = text.split("=", 1)
            symbol = str(symbol or "").strip()
            reason = reason_map.get(symbol, "")
            parts.append(f"{symbol}:{reason}" if reason else text)
        return tuple(parts)

    def _default_playbook_reason_lines(
        self,
        *,
        primary_actions: Iterable[str],
        watch_actions: Iterable[str],
        repair_actions: Iterable[str],
        avoid_actions: Iterable[str],
    ) -> tuple[str, ...]:
        if tuple(repair_actions):
            return (
                "\u4ee5\u4fee\u590d\u8bd5\u9519\u4e3a\u4e3b\uff0c\u5148\u770b\u4f4e\u5f00\u8f6c\u5f3a\u30012\u5206\u949f\u627f\u63a5\u548c\u98ce\u9669\u6536\u655b",
            )
        if tuple(watch_actions):
            return (
                "\u5f53\u524d\u4ee5\u89c2\u5bdf\u4e3a\u4e3b\uff0c\u7b49\u5f85\u9898\u6750\u786e\u8ba4\u30012\u5206\u949f\u627f\u63a5\u548c\u98ce\u9669\u8bc1\u4f2a",
            )
        if tuple(avoid_actions) and not tuple(primary_actions):
            return (
                "\u5f53\u524d\u4e3b\u53d9\u4e8b\u504f\u9632\u5b88\uff0c\u6682\u4e0d\u7ed9\u4f4e\u98ce\u9669\u4e70\u70b9\uff0c\u7b49\u5f85\u98ce\u9669\u91ca\u653e",
            )
        return (
            "\u5f53\u524d\u4e3b\u53d9\u4e8b\u6682\u65e0\u4f4e\u98ce\u9669\u4e70\u70b9\uff0c\u7b49\u5f85\u9898\u6750\u786e\u8ba4\u30012\u5206\u949f\u627f\u63a5\u548c\u98ce\u9669\u8bc1\u4f2a",
        )

    @staticmethod
    def _market_script_prompt_text(script: str) -> str:
        mapping = {
            "attack_confirmed": "主攻确认",
            "hot_risk_validation": "过热验证",
            "pressure_validation": "承压验证",
            "risk_validation": "风险验证",
            "watch_validation": "观察验证",
            "risk_off": "风险防守",
            "observe": "继续观察",
        }
        return mapping.get(str(script or "").strip(), "")

    @staticmethod
    def _invalidation_point_text(point: str) -> str:
        mapping = {
            "confirmed_theme_fades": "主攻题材确认转弱",
            "risk_spread_expands": "高位与中军风险继续扩散",
            "theme_global_fades": "题材整体反馈转弱",
            "stock_2m_fades": "前2分钟承接转弱",
            "front_row_fades": "前排承接转弱",
            "mid_follow_missing": "中位跟随扩散不足",
            "rotation_volume_fades": "切换量能衰减",
            "old_mainline_reclaims": "旧主线重新夺回资金",
            "leader_repair": "高位龙头重新修复",
            "risk_spread_recedes": "风险扩散开始收敛",
            "mid_core_reclaims": "中军重新走强",
            "front_row_reclaims": "前排重新回流",
            "spread_expands": "扩散强度进一步放大",
            "T1_2m_fades": "2分钟启动承接转弱",
            "T2_spread_breaks": "5分钟扩散结构破坏",
            "T3_withdraws": "15分钟资金迁移转为撤退",
        }
        return mapping.get(str(point or "").strip(), "")

    def _summary_mode_note_text(self, mode_note: str) -> str:
        text = str(mode_note or "").strip()
        if not text:
            return ""
        values = self._summary_compact_value_map(text)
        if not values:
            return text.replace(";", " | ")
        parts: list[str] = []
        script_text = self._market_script_prompt_text(values.get("global", ""))
        if script_text:
            parts.append(f"剧本={script_text}")
        cap = values.get("cap", "")
        if cap:
            parts.append(f"仓位={cap}")
        active = values.get("active", "")
        if active and active != "-":
            parts.append(f"活跃剧本={active}")
        blocked = values.get("blocked", "")
        if blocked and blocked != "-":
            parts.append(f"阻塞剧本={blocked}")
        temporal = values.get("temporal", "")
        if temporal and temporal != "-":
            parts.append(f"时序={temporal}")
        targets = values.get("targets", "")
        if targets and targets != "-":
            parts.append(f"目标题材={targets}")
        fading = values.get("fading", "")
        if fading and fading != "-":
            parts.append(f"衰竭题材={fading}")
        return " | ".join(parts) if parts else text.replace(";", " | ")

    @staticmethod
    def _execution_map_title(phase_label: str) -> str:
        mapping = {
            "auction": "【竞价执行图】方向 | 清单",
            "auction_preview": "【竞价执行图】方向 | 清单",
            "opening": "【开盘执行图】方向 | 清单",
            "open_confirm": "【开盘执行图】方向 | 清单",
            "intraday": "【盘中执行图】方向 | 清单",
        }
        return mapping.get(phase_label, "【执行图】方向 | 清单")

    @staticmethod
    def _risk_tag_prompt_text(risk_tag: str) -> str:
        mapping = {
            "focus_asset_market_risk": "高位与中军退潮扩散，先防守观察",
            "dragon_alone_risk": "板块只剩龙头独活，不做跟风扩散",
            "theme_relative_risk": "题材相对强弱不占优，避免盲打弱线",
            "hot_plate_hard_risk": "热板风险过高，避免一致追涨",
            "high_focus_risk": "高标压力偏大，只看确认后的低风险承接",
            "risk_capped_pressure_repair": "修复只可轻仓试错，不做重仓进攻",
            "focus_asset_stress": "焦点资产压力明显，优先等待风险释放",
        }
        return mapping.get(str(risk_tag or "").strip(), "")

    def _summary_mode_risk_prompt(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        output_summary = self._playbook_output_summary_for_state(state)
        if output_summary is None:
            return ""
        parts: list[str] = []
        note_text = self._summary_mode_note_text(getattr(output_summary, "mode_note", ""))
        if note_text:
            parts.append(note_text)
        risk_tags = tuple(getattr(output_summary, "risk_tags", ()) or ())
        risk_parts = [
            text
            for text in (self._risk_tag_prompt_text(tag) for tag in risk_tags[:3])
            if text
        ]
        if risk_parts:
            parts.append(" / ".join(dict.fromkeys(risk_parts)))
        return " | ".join(parts)

    def _phase_priority_plates(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> tuple[str, ...]:
        if phase_label in {"open_confirm", "intraday", "postmarket"}:
            preferred = self._narrative_priority_plates(state, phase_label=phase_label)
            if preferred:
                return preferred
        return self._playbook_priority_plates(state)

    def _playbook_priority_plates(self, state: StrategyConsoleState) -> tuple[str, ...]:
        ordered: list[str] = []
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            if main_attack:
                ordered.append(main_attack)
            for plate_name in tuple(getattr(global_decision, "secondary_themes", ()) or ()):
                normalized = normalize_plate_name(str(plate_name or ""))
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
            for plate_name in tuple(getattr(global_decision, "watch_themes", ()) or ()):
                normalized = normalize_plate_name(str(plate_name or ""))
                if normalized and normalized not in ordered:
                    ordered.append(normalized)
            if ordered:
                return tuple(ordered)
        if state.theme_judge_map:
            actionable: list[str] = []
            anchor_only: list[str] = []
            for judge in sorted(
                state.theme_judge_map.values(),
                key=lambda item: (
                    self._theme_action_priority(item.action_class),
                    item.opportunity_score,
                    -item.trap_score,
                ),
                reverse=True,
            ):
                if judge.action_class not in {"main_attack", "front_row_confirm", "anchor_only"}:
                    continue
                if judge.validation_state == "falsified" or judge.trap_score >= 7.0:
                    continue
                execution_state = self._external_validation_state(judge.validation_state)
                if execution_state == "partial" and judge.action_class != "anchor_only":
                    continue
                name = normalize_plate_name(judge.plate_name)
                if not name or name == "-":
                    continue
                if judge.action_class in {"main_attack", "front_row_confirm"}:
                    if name not in actionable:
                        actionable.append(name)
                elif name not in anchor_only:
                    anchor_only.append(name)
            ordered.extend(actionable or anchor_only)
        elif self._expectation_ready(state):
            for item in self._theme_collision_rows(state):
                if item.fakeout_level == "strong" or item.x_score >= 7.0:
                    continue
                name = normalize_plate_name(item.plate_name)
                if name and name != "-" and name not in ordered:
                    ordered.append(name)
        summary = state.context.market_summary
        if not ordered:
            for raw_name in (
                summary.mainline_sector,
                summary.top_plate_name,
                *(row.plate_name for row in state.plate_stats[:3] if not row.generic),
            ):
                name = normalize_plate_name(raw_name)
                if name and name != "-" and name not in ordered:
                    ordered.append(name)
        return tuple(ordered)

    def _matched_theme_judge(
        self,
        state: StrategyConsoleState,
        snapshot: StockStateSnapshot | None,
    ) -> tuple[ThemeJudgeResult | None, str]:
        if snapshot is None:
            return None, ""
        if state.matched_theme_judge_map is not None:
            return state.matched_theme_judge_map.get(snapshot.symbol, (None, ""))
        for plate_name in self._normalized_plate_names(snapshot):
            judge = self._theme_judge_for_plate(state, plate_name)
            if judge is not None:
                return judge, plate_name
        return None, ""

    def _theme_collision_by_plate(self, state: StrategyConsoleState) -> dict[str, AuctionThemeCollisionStat]:
        if state.theme_collision_map is not None:
            return state.theme_collision_map
        return {
            normalize_plate_name(item.plate_name): item
            for item in self._theme_collision_rows(state)
            if normalize_plate_name(item.plate_name)
        }

    def _snapshot_theme_collision(
        self,
        state: StrategyConsoleState,
        snapshot: StockStateSnapshot | None,
    ) -> AuctionThemeCollisionStat | None:
        if snapshot is None:
            return None
        collision_map = self._theme_collision_by_plate(state)
        for plate_name in self._normalized_plate_names(snapshot):
            matched = collision_map.get(plate_name)
            if matched is not None:
                return matched
        return None

    def _is_decision_blocked_by_theme_risk(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> bool:
        if phase_label not in {"auction", "opening", "open_confirm", "intraday"}:
            return False
        snapshot = state.snapshot_map.get(decision.symbol)
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        repair_probe_exception = (
            decision.setup_id == "theme_not_tradable_repair_probe"
            and selection is not None
            and selection.open_follow_state in {"confirmed", "repair_strength"}
        )
        if (
            snapshot is not None
            and selection is not None
            and self._snapshot_is_falsified_but_leader_alive(state, snapshot)
            and not selection.is_true_leader
            and decision.action != "hold_only"
        ):
            return True
        judge, _matched_plate = self._matched_theme_judge(state, snapshot)
        if judge is not None:
            execution_state = self._external_validation_state(judge.validation_state)
            if execution_state == "falsified":
                return decision.action != "hold_only"
            if execution_state == "partial" and judge.action_class in {"observe", "anchor_only"} and decision.action != "hold_only":
                if not repair_probe_exception and (selection is None or not selection.is_true_leader):
                    return True
        collision = self._snapshot_theme_collision(state, snapshot)
        if collision is None:
            return False
        if collision.fakeout_level == "strong":
            return True
        if collision.x_score >= 7.0 and decision.action in ("dragon_early_board", "early_boarding_candidate"):
            return True
        return False

    def _stock_selection_context_map(self, state: StrategyConsoleState) -> dict[str, StockSelectionContext]:
        if state.selection_context_map is not None:
            return state.selection_context_map
        if state.bundle is None:
            return {}
        return {item.symbol: item for item in state.bundle.stock_selection_contexts}

    def _selection_theme_tier(
        self,
        selection: StockSelectionContext | None,
        snapshot: StockStateSnapshot | None,
    ) -> str:
        return classify_playbook_theme_tier(selection, snapshot)

    @staticmethod
    def _theme_tier_priority(tier: str) -> int:
        mapping = {
            "dragon": 4,
            "front_core": 3,
            "front_follow": 2,
            "back_noise": 1,
        }
        return mapping.get(tier, 0)

    def _is_stock_auction_fakeout(
        self,
        snapshot: StockStateSnapshot | None,
        selection: StockSelectionContext | None,
        *,
        phase_label: str,
    ) -> bool:
        front_comparison = self._current_market_slice_comparison_for_phase(phase_label)
        return is_playbook_stock_auction_fakeout(
            snapshot,
            selection,
            phase_label=phase_label,
            front_weak=front_comparison.is_weak,
            front_strong=front_comparison.is_strong,
        )

    def _is_high_dayk_weak_leader_trap(
        self,
        snapshot: StockStateSnapshot | None,
        selection: StockSelectionContext | None,
        *,
        phase_label: str,
    ) -> bool:
        if not is_high_dayk_weak_trap(snapshot, selection, phase_label=phase_label):
            return False
        if phase_label == "auction":
            return True
        confirm_label = self._leader_truth_label(snapshot)
        if confirm_label in {self.OPENING_VALIDATION_GAP_WEAK, self.OPENING_VALIDATION_UNDERTAKE_WEAK}:
            return True
        return True

    def _selection_has_non_hot_strength(
        self,
        selection: StockSelectionContext,
        snapshot: StockStateSnapshot | None,
    ) -> bool:
        return has_playbook_non_hot_strength(
            selection,
            snapshot,
            low_open_rebound=self._is_low_open_rebound_snapshot(snapshot),
        )

    def _normalized_plate_names(self, snapshot: StockStateSnapshot) -> tuple[str, ...]:
        ordered: list[str] = []
        for raw_name in self._ordered_plate_candidates(snapshot, prefer_high_board=True):
            name = normalize_plate_name(raw_name)
            if name and name != "-" and name not in ordered:
                ordered.append(name)
        return tuple(ordered)

    def _render_yest_limit_breakdown(self, state: StrategyConsoleState) -> tuple[str, ...]:
        snapshots = [snapshot for snapshot in state.snapshot_map.values() if snapshot.is_yest_limit]
        if not snapshots:
            return ("【昨日涨停拆层】暂无昨板样本",)
        high = [snapshot for snapshot in snapshots if snapshot.lb_days >= 3]
        mid = [snapshot for snapshot in snapshots if 1 <= snapshot.lb_days <= 2]
        first = [snapshot for snapshot in snapshots if snapshot.lb_days == 0]
        return (
            "【昨日涨停拆层】层级 | 样本 | 强弱 | 交易结论",
            f"  高位股 | {len(high)} | {self._yest_limit_bucket_strength(high)} | {self._yest_limit_bucket_action('high', high)}",
            f"  中位股 | {len(mid)} | {self._yest_limit_bucket_strength(mid)} | {self._yest_limit_bucket_action('mid', mid)}",
            f"  首板股 | {len(first)} | {self._yest_limit_bucket_strength(first)} | {self._yest_limit_bucket_action('first', first)}",
        )

    def _render_ladder_map(self, state: StrategyConsoleState) -> tuple[str, ...]:
        grouped_snapshots: dict[str, list[StockStateSnapshot]] = defaultdict(list)
        for snapshot in state.snapshot_map.values():
            if snapshot.is_yest_limit and snapshot.lb_days >= 1:
                grouped_snapshots[f"{max(snapshot.lb_days - 1, 0)}B->{snapshot.lb_days}B"].append(snapshot)

        def red_open_stats(snapshots: list[StockStateSnapshot]) -> tuple[str, int]:
            if not state.historical_only:
                red_count_local = sum(1 for snapshot in snapshots if snapshot.open_pct > 0)
                total_local = max(len(snapshots), 1)
                return (f"{red_count_local / total_local:.0%}", red_count_local)
            matched_snapshots = [snapshot for snapshot in snapshots if snapshot.symbol in state.context.auction_map]
            if not matched_snapshots:
                return ("--", -1)
            red_count_local = sum(
                1
                for snapshot in matched_snapshots
                if self._normalize_pct_value(
                    state.context.auction_map.get(snapshot.symbol, {}).get("change_pct", snapshot.open_pct)
                )
                > 0
            )
            total_local = max(len(matched_snapshots), 1)
            return (f"{red_count_local / total_local:.0%}", red_count_local)

        if state.context.phase != RunPhase.POSTMARKET and state.context.session_facts.ladder_facts:
            rows = ["【梯队映射】梯队 | 数量 | 红开率 | 晋级率 | 极值特征 | 层级定性 | 代表"]
            for fact in state.context.session_facts.ladder_facts[:4]:
                total = max(fact.total_count, 1)
                rep_snapshot = state.snapshot_map.get(fact.representative_symbol)
                fact_snapshots = grouped_snapshots.get(fact.key, [])
                red_open_text, red_count = red_open_stats(fact_snapshots)
                rows.append(
                    f"  {fact.key} | {fact.total_count} | {red_open_text} | {fact.promoted_count / total:.0%} | "
                    f"{self._ladder_extreme_label(fact.key, red_count=red_count, promoted_count=fact.promoted_count, total=fact.total_count)} | "
                    f"{self._mid_ladder_label(fact.key, red_count=red_count, promoted_count=fact.promoted_count, total=fact.total_count)} | "
                    f"{self._compact_stock_ref(rep_snapshot, symbol=fact.representative_symbol)}"
                )
            return tuple(rows)

        if state.context.phase == RunPhase.POSTMARKET:
            recap_facts = self._build_recap_ladder_truth_facts(state, phase_label="postmarket")
            if recap_facts:
                rows = ["【梯队映射】梯队 | 数量 | 红开率 | 晋级率 | 极值特征 | 层级定性 | 代表"]
                for fact in recap_facts[:4]:
                    total = max(fact.total_count, 1)
                    rep_snapshot = state.snapshot_map.get(fact.representative_symbol)
                    red_open_text = f"{fact.red_open_count / total:.0%}" if fact.total_count > 0 else "--"
                    rows.append(
                        f"  {fact.key} | {fact.total_count} | {red_open_text} | {fact.promoted_count / total:.0%} | "
                        f"{self._ladder_extreme_label(fact.key, red_count=fact.red_open_count, promoted_count=fact.promoted_count, total=fact.total_count)} | "
                        f"{self._mid_ladder_label(fact.key, red_count=fact.red_open_count, promoted_count=fact.promoted_count, total=fact.total_count)} | "
                        f"{self._compact_stock_ref(rep_snapshot, symbol=fact.representative_symbol)}"
                    )
                return tuple(rows)

        transitions: dict[str, list[StockStateSnapshot]] = defaultdict(list)
        fallback_groups: dict[str, list[StockStateSnapshot]] = defaultdict(list)
        for snapshot in state.snapshot_map.values():
            if snapshot.is_yest_limit and snapshot.lb_days >= 1:
                key = f"{max(snapshot.lb_days - 1, 0)}B->{snapshot.lb_days}B"
                transitions[key].append(snapshot)
            elif snapshot.lb_days >= 2:
                fallback_groups[f"{snapshot.lb_days}B"].append(snapshot)

        groups = transitions or fallback_groups
        if not groups:
            return ("【梯队映射】暂无梯队样本",)

        ordered = sorted(
            groups.items(),
            key=lambda item: (
                -self._ladder_sort_value(item[0]),
                -len(item[1]),
            ),
        )
        rows = ["【梯队映射】梯队 | 数量 | 红开率 | 晋级率 | 极值特征 | 层级定性 | 代表"]
        for key, snapshots in ordered[:4]:
            red_open_text, red_count = red_open_stats(snapshots)
            promoted_count = sum(1 for snapshot in snapshots if self._is_limit_up_snapshot(snapshot))
            rep = min(
                snapshots,
                key=lambda snapshot: (
                    snapshot.leader_rank_in_theme,
                    -snapshot.current_pct,
                    -snapshot.auction_amount,
                ),
            )
            rows.append(
                f"  {key} | {len(snapshots)} | {red_open_text} | {promoted_count / max(len(snapshots), 1):.0%} | "
                f"{self._ladder_extreme_label(key, red_count=red_count, promoted_count=promoted_count, total=len(snapshots))} | "
                f"{self._mid_ladder_label(key, red_count=red_count, promoted_count=promoted_count, total=len(snapshots))} | "
                f"{self._compact_stock_ref(rep)}"
            )
        return tuple(rows)

    def _leader_open_strength(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.open_pct >= 0.095:
            return "买一承接强"
        if snapshot.open_pct >= 0.06:
            return "强开锚定"
        if snapshot.open_pct >= 0.02:
            return "买一一般"
        if snapshot.open_pct >= 0.0:
            return "平开待判"
        return "低开承压"

    def _leader_seal_quality(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.is_locked or snapshot.open_pct >= 0.095:
            if snapshot.volume_intensity >= 3.0:
                return "买一强但热"
            return "顶强但过热"
        if (
            snapshot.open_pct >= 0.05
            and snapshot.auction_amount >= 50_000_000
            and snapshot.leader_rank_in_theme <= 2
        ):
            if snapshot.volume_intensity >= 2.5:
                return "买一承接强"
            return "强势锚定"
        if snapshot.volume_intensity >= 2.5:
            return "买一代理强"
        return "抛压偏大"

    def _leader_turnover_quality(self, snapshot: StockStateSnapshot) -> str:
        if (
            snapshot.open_pct >= 0.02
            and snapshot.auction_amount >= 20_000_000
            and (snapshot.speed_1m > 0 or snapshot.amount_2m >= 30_000_000)
        ):
            if snapshot.amount_2m >= 50_000_000 or snapshot.speed_1m > 0.01:
                return "放量确认"
            return "温和确认"
        if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
            return "低开转强"
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return "回落修复"
        return "强弱待判"
    def _leader_pressure_label(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.resistance_gap > 0.12:
            return "抛压偏大"
        if snapshot.resistance_gap > 0.06:
            return "仍有抛压"
        if snapshot.market_cap_yi >= 300 or snapshot.amount_day_yi >= 40:
            return "大票承压"
        if snapshot.current_pct < snapshot.open_pct - 0.03:
            return "冲高回落"
        return "承接正常"

    def _leader_heat_label(self, snapshot: StockStateSnapshot) -> str:
        return f"{self._leader_seal_quality(snapshot)}/{self._leader_turnover_quality(snapshot)}"

    def _leader_truth_label(self, snapshot: StockStateSnapshot) -> str:
        amount_ratio_2m = (snapshot.amount_2m / snapshot.auction_amount) if snapshot.auction_amount > 0 else 0.0
        if (
            0.02 <= snapshot.open_pct <= 0.07
            and snapshot.auction_amount >= 30_000_000
            and (snapshot.amount_2m >= 40_000_000 or snapshot.speed_1m > 0.01)
            and amount_ratio_2m >= 0.95
            and snapshot.current_pct >= max(snapshot.open_pct - 0.01, 0.0)
        ):
            return self.OPENING_VALIDATION_TRUE_STRONG
        if snapshot.open_pct >= 0.095 and snapshot.current_pct < snapshot.open_pct - 0.03:
            return self.OPENING_VALIDATION_GAP_WEAK
        if snapshot.open_pct >= 0.095:
            return self.OPENING_VALIDATION_HARD_TO_CHASE
        if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
            return self.OPENING_VALIDATION_LOW_OPEN_STRONG
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return self.OPENING_VALIDATION_PULLBACK_REBOUND
        if (
            snapshot.current_pct < 0.0
            or snapshot.current_pct < snapshot.open_pct - 0.04
            or (snapshot.auction_amount > 0 and amount_ratio_2m < 0.75 and snapshot.speed_1m <= 0)
        ):
            return self.OPENING_VALIDATION_UNDERTAKE_WEAK
        return self.OPENING_VALIDATION_PENDING

    def _entry_window_label(self, snapshot: StockStateSnapshot, *, phase_label: str) -> str:
        if phase_label == "postmarket":
            if snapshot.open_pct >= 0.095:
                return "一字不追"
            if (
                0.02 <= snapshot.open_pct <= 0.07
                and snapshot.leader_rank_in_theme <= 2
                and snapshot.auction_amount >= 20_000_000
            ):
                if snapshot.amount_2m >= 40_000_000 or snapshot.speed_1m > 0.01:
                    return "换手确认"
                return "放量观察"
            if snapshot.open_pct <= 0.01 and snapshot.current_pct > 0.03 and snapshot.amount_2m >= 20_000_000:
                return "低吸回流"
            if snapshot.open_pct < 0.0 and snapshot.current_pct <= 0.0:
                return "承接不足"
            return "等待确认"
        if snapshot.open_pct >= 0.095:
            return "一字不追"
        if (
            0.02 <= snapshot.open_pct <= 0.07
            and snapshot.leader_rank_in_theme <= 2
            and snapshot.auction_amount >= 20_000_000
        ):
            if snapshot.amount_2m >= 40_000_000 or snapshot.speed_1m > 0.01:
                return "换手确认"
            return "确认后看"
        if snapshot.open_pct <= 0.01 and snapshot.auction_amount > 0 and snapshot.amount_2m >= 20_000_000:
            return "低位试错"
        if snapshot.open_pct < 0.0 and snapshot.current_pct <= 0.0:
            return "先等承接"
        return "等待确认"

    def _theme_trade_profile(self, row: AuctionPlateBucketStat) -> tuple[str, str, str]:
        expectation = self.EXPECTATION_LABELS.get(row.expectation, row.expectation)
        cohesion = self._theme_cohesion_level(row)
        if expectation == "distribution":
            return ("兑现", "防守", "不追高")
        if row.generic:
            return ("泛题材", "观察", "只看辨识度")
        if expectation == "attack":
            if cohesion == "weak":
                return ("进攻", "分歧进攻", "只做前排确认")
            if row.auction_amount >= 150_000_000 and row.leader_count >= 2:
                return ("进攻", "主攻强化", "优先前排换手")
            return ("进攻", "局部进攻", "先盯核心龙头")
        if expectation == "follow":
            if row.hot_change_pct > 0 and row.auction_symbol_count >= 3:
                return ("跟随", "扩散跟随", "看板块联动")
            return ("跟随", "局部跟随", "只看前排")
        if expectation == "ladder":
            if row.hot_change_pct <= 0 and row.yest_limit_count >= 2:
                return ("晋级", "高位分歧", "看活口承接")
            if row.leader_count >= 2 and row.auction_amount >= 80_000_000:
                return ("晋级", "梯队成型", "先看中位卡位")
            return ("晋级", "梯队博弈", "只看前排")
        if expectation == "cluster":
            if cohesion == "weak":
                return ("联动", "弱联动", "防止假强骗炮")
            if row.symbol_count >= 4 and row.auction_symbol_count >= 3:
                return ("簇动", "共振加强", "看低位扩散")
            return ("联动", "局部共振", "看龙头带动")
        return ("观察", "等待确认", "先不下结论")

    def _theme_internal_names(self, state: StrategyConsoleState, plate_name: str) -> tuple[str, str, str]:
        theme_fact = state.context.session_facts.theme_fact_map.get(plate_name)
        if theme_fact is not None:
            names = [self._snapshot_name_by_symbol_compact(state, symbol) for symbol in theme_fact.top3_symbols[:3]]
            while len(names) < 3:
                names.append("-")
            return names[0], names[1], names[2]
        matched = nlargest(
            3,
            (
                state.snapshot_map[symbol]
                for symbol in state.plate_symbol_map.get(plate_name, ())
                if symbol in state.snapshot_map
            ),
            key=lambda snapshot: (
                snapshot.lb_days,
                -snapshot.leader_rank_in_theme,
                snapshot.auction_amount,
                snapshot.current_pct,
            ),
        )
        names = [self._compact_stock_ref(snapshot) for snapshot in matched]
        while len(names) < 3:
            names.append("-")
        return names[0], names[1], names[2]

    def _theme_layer_comment(self, state: StrategyConsoleState, row: AuctionPlateBucketStat) -> str:
        leader, assist, follower = self._theme_internal_names(state, row.plate_name)
        cohesion = self._theme_cohesion_level(row)
        if row.generic:
            return "泛题材方向较散，更适合只看辨识度和承接。"
        if cohesion == "weak":
            return f"{leader}相对更强，但{assist}、{follower}跟随不足，容易出现龙头独强。"
        if row.leader_count >= 2 and row.auction_amount >= 80_000_000:
            return f"{leader}与{assist}能形成双前排，说明板块有一定扩散性。"
        if row.auction_symbol_count >= 3 and row.symbol_count >= 4:
            return f"{leader}、{assist}到{follower}都出现跟随，板块梯队更完整。"
        if row.yest_limit_count >= 2 and row.hot_change_pct <= 0:
            return f"{leader}仍在维持强度，但{follower}没有明显扩散，偏向高位博弈。"
        return f"{leader}是当前核心，{assist}与{follower}还需要继续观察是否跟上。"

    def _premarket_plan_text(self, minute_tag: str | None) -> str:
        minute_text = str(minute_tag or "").strip()
        if re.fullmatch(r"\d{2}:\d{2}", minute_text):
            try:
                hour, minute = minute_text.split(":", 1)
                remaining = (9 * 60 + 25) - (int(hour) * 60 + int(minute))
            except ValueError:
                remaining = None
            else:
                if remaining is not None and remaining > 0:
                    if remaining <= 5:
                        prefix = f"盘前预案 | 距竞价约 {remaining} 分钟，进入最后校对窗。"
                    elif remaining <= 15:
                        prefix = f"盘前预案 | 距竞价约 {remaining} 分钟，进入临近竞价准备。"
                    else:
                        prefix = f"盘前预案 | 距竞价约 {remaining} 分钟，先按前一交易日复盘结论做今日预案。"
                    return prefix + " 不提前给竞价结论。"
        return "盘前预案 | 当前距竞价还远，先按前一交易日复盘结论做今日预案，不提前给竞价结论。"

    def _mid_ladder_label(self, key: str, *, red_count: int, promoted_count: int, total: int) -> str:
        if total <= 0:
            return "无有效样本"
        auction_unknown = red_count < 0
        if "1B->2B" in key and red_count >= max(1, total // 2) and promoted_count >= max(1, total // 3):
            return "最像上车层"
        if "2B->3B" in key and promoted_count >= max(1, total // 2):
            return "卡位确认层"
        if "3B->4B" in key and promoted_count >= 1:
            return "高位活口层"
        if promoted_count >= max(1, total // 2):
            return "卡位有机会"
        if auction_unknown:
            return "以晋级反馈为主"
        if red_count == total and promoted_count == 0:
            return "红开但偏弱"
        return "以分歧观察为主"

    def _ladder_extreme_label(self, key: str, *, red_count: int, promoted_count: int, total: int) -> str:
        if total <= 0:
            return "无样本"
        auction_unknown = red_count < 0
        red_rate = red_count / total
        promoted_rate = promoted_count / total
        if "3B->4B" in key and promoted_count >= 1:
            return "高位留活口"
        if not auction_unknown and "1B->2B" in key and red_rate >= 0.6 and promoted_rate >= 0.3:
            return "中位最强"
        if promoted_rate >= 0.5:
            return "晋级偏强"
        if auction_unknown:
            return "晋级偏弱" if promoted_rate < 0.2 else "分歧博弈"
        if red_rate >= 0.7 and promoted_rate < 0.2:
            return "红开虚强"
        if red_rate <= 0.3:
            return "开盘偏弱"
        return "分歧博弈"
    def _extreme_score(self, snapshot: StockStateSnapshot) -> float:
        score = 0.0
        score += max(snapshot.auction_amount / 100_000_000, 0.0) * 20
        score += max(snapshot.amount_2m / 100_000_000, 0.0) * 16
        score += max(snapshot.speed_1m, 0.0) * 500
        score += max(snapshot.current_pct, 0.0) * 120
        if snapshot.leader_rank_in_theme <= 2:
            score += 12
        if 0.02 <= snapshot.open_pct <= 0.07:
            score += 10
        elif snapshot.open_pct >= 0.095:
            score -= 8
        if snapshot.volume_intensity >= 2.5:
            score += 8
        return score

    def _extreme_type_label(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.auction_amount >= 100_000_000 and 0.02 <= snapshot.open_pct <= 0.07:
            return "竞价强承接"
        if snapshot.amount_2m >= 80_000_000 and snapshot.speed_1m > 0.01:
            return "开盘放量"
        if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
            return "低开转强"
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return "回落修复"
        if snapshot.open_pct >= 0.095:
            return "一字顶强"
        return "普通承接"

    def _rebound_score(self, snapshot: StockStateSnapshot) -> float:
        score = 0.0
        if snapshot.open_pct <= 0.01:
            score += 25
        if snapshot.open_pct < 0.0:
            score += 18
        score += max(snapshot.current_pct, 0.0) * 160
        score += max(snapshot.amount_2m / 100_000_000, 0.0) * 18
        score += max(snapshot.speed_1m, 0.0) * 600
        if snapshot.amount_2m >= snapshot.auction_amount > 0:
            score += 10
        if snapshot.leader_rank_in_theme <= 2:
            score += 8
        return score

    def _rebound_type_label(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
            return "低开走强"
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return "回落翻红"
        if snapshot.amount_2m >= 50_000_000 and snapshot.speed_1m > 0.01:
            return "放量拉升"
        if snapshot.amount_2m >= snapshot.auction_amount > 0:
            return "量能接力"
        return "普通修复"

    def _is_low_open_rebound_snapshot(self, snapshot: StockStateSnapshot) -> bool:
        if snapshot is None:
            return False
        if snapshot.open_pct < 0.0 and snapshot.current_pct >= 0.05:
            return True
        if snapshot.open_pct > 0.01:
            return False
        if snapshot.current_pct < 0.03:
            return False
        if snapshot.amount_2m < 20_000_000:
            return False
        if snapshot.amount_2m < snapshot.auction_amount and snapshot.speed_1m <= 0.008:
            return False
        return True

    def _display_plate_name(self, snapshot: StockStateSnapshot | None, *, prefer_high_board: bool = False) -> str:
        if snapshot is None:
            return "-"
        normalized_names: list[str] = []
        ordered_sources = self._ordered_plate_candidates(snapshot, prefer_high_board=prefer_high_board)
        for raw in ordered_sources:
            cleaned = normalize_plate_name(raw)
            if not cleaned or cleaned in normalized_names:
                continue
            normalized_names.append(cleaned)
        for name in normalized_names:
            if not is_generic_plate(name):
                return name
        return "-"

    def _ordered_plate_candidates(self, snapshot: StockStateSnapshot, *, prefer_high_board: bool) -> tuple[str, ...]:
        return (
            snapshot.plate,
            *snapshot.real_plate_names,
        )

    def _yest_limit_trade_env(self, summary) -> str:
        if summary.promotion_rate >= 0.35 and summary.headshot_rate <= 0.08:
            return "可博弈"
        if summary.headshot_rate >= 0.12 or summary.promotion_rate <= 0.15:
            return "风险大"
        return "分歧市"

    def _yest_limit_bucket_strength(self, snapshots: list[StockStateSnapshot]) -> str:
        if not snapshots:
            return "无样本"
        strong = sum(1 for snapshot in snapshots if self._is_limit_up_snapshot(snapshot))
        red = sum(1 for snapshot in snapshots if snapshot.open_pct > 0)
        weak = sum(1 for snapshot in snapshots if snapshot.current_pct < 0)
        if strong >= max(1, len(snapshots) // 2):
            return "强反馈"
        if weak >= max(1, len(snapshots) // 2):
            return "弱反馈"
        if red >= max(1, len(snapshots) // 2):
            return "有溢价但分歧"
        return "中性反馈"

    def _yest_limit_bucket_action(self, bucket: str, snapshots: list[StockStateSnapshot]) -> str:
        if not snapshots:
            return "无交易结论"
        strong = sum(1 for snapshot in snapshots if self._is_limit_up_snapshot(snapshot))
        weak = sum(1 for snapshot in snapshots if snapshot.current_pct < 0)
        if bucket == "high":
            if weak >= max(1, len(snapshots) // 2):
                return "高位兑现为主，不接一致后排"
            if strong >= 1:
                return "只看活口回封，不做一致顶"
            return "高位分歧观察为主"
        if bucket == "mid":
            if strong >= max(1, len(snapshots) // 3):
                return "中位分歧博弈，只做前排"
            if weak >= max(1, len(snapshots) // 2):
                return "中位负反馈偏多，先防兑现"
            return "中位以承接筛选为主"
        if strong >= 1:
            return "低位首板可试错，看换手确认"
        if weak >= max(1, len(snapshots) // 2):
            return "低位也偏弱，减少盲打"
        return "低位观察扩散和回流"

    def _yest_limit_opportunity_profile(self, summary) -> tuple[str, str]:
        if summary.promotion_rate >= 0.35:
            return ("机会较多", "可以积极找前排")
        if summary.promotion_rate >= 0.2:
            return ("机会一般", "只做确认后的机会")
        return ("机会偏少", "不做盲目接力")

    def _yest_limit_premium_profile(self, summary) -> tuple[str, str]:
        if summary.red_open_rate >= 0.75:
            return ("溢价较强", "红开溢价足，但要防高开兑现")
        if summary.red_open_rate >= 0.45:
            return ("溢价一般", "看分歧后谁回流")
        return ("溢价偏弱", "红开不多，承接更关键")

    def _yest_limit_risk_profile(self, summary) -> tuple[str, str]:
        if summary.headshot_rate >= 0.12:
            return ("风险较高", "核按钮偏多，先防高位负反馈")
        if summary.headshot_rate >= 0.06:
            return ("风险可控", "有分歧但还没到全面退潮")
        return ("负反馈轻", "允许前排试错")

    def _render_risk_guard(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        generic_plates = [row.plate_name for row in state.plate_stats if row.generic][:2]
        output_summary = self._playbook_output_summary_for_state(state)
        avoid_parts = list(tuple(getattr(output_summary, "avoid_actions", ()) or ()))[:4] if output_summary is not None else []
        if output_summary is not None:
            reject_reasons = tuple(getattr(output_summary, "reject_reasons", ()) or ())
            if not avoid_parts and reject_reasons:
                avoid_parts = ["只观察，等待下一轮确认"]
        elif state.bundle is not None:
            for decision in state.bundle.decisions:
                display_code = self._display_action_code(decision, state, phase_label=phase_label)
                if display_code in {"failed_promo_guard", "do_not_chase"}:
                    avoid_parts.append(
                        f"{self._decision_name(state, decision)}:{self._action_text(display_code)}"
                    )
                if len(avoid_parts) >= 4:
                    break
        field_name = "复盘风控" if phase_label == "postmarket" else "风险提示"
        missing_items = list(state.missing_inputs)
        if state.historical_only and phase_label == "postmarket":
            missing_items = [
                item for item in missing_items if item not in {"auction_anchor", "auction_anchor_pending"}
            ]
        mode_risk = self._mode_risk_prompt(state, phase_label=phase_label)
        return (
            f"【{field_name}】维度 | 内容",
            f"  - 回避 | {','.join(avoid_parts) or '-'}",
            f"  - 泛题材 | {','.join(generic_plates) or '-'}",
            f"  - 模式 | {mode_risk}",
            f"  - 缺失 | {','.join(self._missing_text(item) for item in missing_items) or '无'}",
            f"  - 数据 | {self._display_source_label(state, phase_label=phase_label)}",
        )

    def _score_marker(self, score: float) -> str:
        if score >= 6.0:
            return "★"
        if score >= 4.5:
            return "△"
        if score >= 3.0:
            return "→"
        return "×"

    def _promotion_marker(self, value: float) -> str:
        if value >= 0.35:
            return "↑"
        if value >= 0.18:
            return "→"
        return "↓"

    def _red_open_marker(self, value: float) -> str:
        if value >= 0.75:
            return "↑"
        if value >= 0.45:
            return "→"
        return "↓"

    def _headshot_marker(self, value: float) -> str:
        if value >= 0.12:
            return "!"
        if value >= 0.05:
            return "△"
        return "○"

    def _resonance_marker(self, value: float) -> str:
        if value >= 800:
            return "◎"
        if value >= 300:
            return "○"
        return "·"

    def _battle_marker(self, battle: str) -> str:
        mapping = {
            "bullish": "↑",
            "neutral": "→",
            "danger": "!",
            "frozen": "冻",
            "historical": "史",
        }
        return mapping.get(battle, "·")

    def _close_marker(self, verdict: str) -> str:
        mapping = {
            "strong_close": "↑",
            "mixed_close": "→",
            "risk_close": "△",
            "weak_close": "↓",
        }
        return mapping.get(verdict, "·")

    def _collect_allowed_setups(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        if phase_label == "postmarket":
            return ("tomorrow_watch", "leader_review", "plate_recap")
        if phase_label == "premarket" and state.historical_only:
            return ("carry_review", "watch_only", "auction_wait")
        if phase_label in {"auction", "opening", "open_confirm"} and not self._feedback_metrics_ready(state):
            return ("watch_only", "auction_wait", "risk_scan")
        if phase_label == "intraday" and state.stale_snapshot_only:
            return ("watch_only", "leader_review", "risk_scan")
        output_summary = self._playbook_output_summary_for_state(state)
        if output_summary is not None:
            labels: list[str] = []
            primary_actions = tuple(getattr(output_summary, "primary_actions", ()) or ())
            repair_actions = tuple(getattr(output_summary, "repair_actions", ()) or ())
            watch_actions = tuple(getattr(output_summary, "watch_actions", ()) or ())
            avoid_actions = tuple(getattr(output_summary, "avoid_actions", ()) or ())
            reject_reasons = tuple(getattr(output_summary, "reject_reasons", ()) or ())
            if primary_actions:
                labels.append("confirm_then_go")
            if repair_actions:
                labels.append("ice_probe")
            if watch_actions:
                labels.append("leader_watch")
            if labels:
                return tuple(labels[:3])
            if avoid_actions or reject_reasons:
                return ("watch_only",)
            return ("watch_only",)
        labels: list[str] = []
        for decision in state.playbook_decisions:
            label = self._display_action_code(decision, state, phase_label=phase_label)
            if label not in labels:
                labels.append(label)
            if len(labels) >= 3:
                break
        if not labels:
            labels.append("observe_only")
        return tuple(labels)

    def _collect_banned_actions(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        banned: list[str] = []
        summary = state.context.market_summary
        if phase_label == "postmarket":
            banned.extend(["live_trade", "blind_chase"])
        if phase_label == "premarket" and state.historical_only:
            banned.extend(["live_trade", "blind_trade", "blind_chase"])
        if phase_label in {"auction", "opening", "open_confirm"} and not self._feedback_metrics_ready(state):
            banned.extend(["live_trade", "blind_trade", "blind_chase", "full_position"])
        if phase_label == "intraday" and state.stale_snapshot_only:
            banned.extend(["live_trade", "blind_trade", "blind_chase"])
        output_summary = self._playbook_output_summary_for_state(state)
        if output_summary is not None:
            risk_tags = tuple(getattr(output_summary, "risk_tags", ()) or ())
            risk_mapping = {
                "focus_asset_market_risk": ("high_chase", "blind_trade", "blind_chase"),
                "dragon_alone_risk": ("follow_trade",),
                "theme_relative_risk": ("blind_trade", "generic_theme_only"),
                "hot_plate_hard_risk": ("high_chase", "blind_chase"),
                "high_focus_risk": ("blind_chase",),
                "risk_capped_pressure_repair": ("full_position",),
                "focus_asset_stress": ("blind_trade",),
            }
            for tag in risk_tags:
                banned.extend(risk_mapping.get(str(tag or "").strip(), ()))
        if summary.battle_status == "frozen":
            banned.append("high_chase")
        if state.missing_inputs:
            banned.append("blind_trade")
        if any(row.generic for row in state.plate_stats[:2]):
            banned.append("generic_theme_only")
        if summary.sentiment_score < 4.0:
            banned.append("full_position")
        if not banned:
            banned.append("none")
        return tuple(dict.fromkeys(banned))

    def _infer_regime_stage(self, summary, state: StrategyConsoleState, *, phase_label: str) -> str:
        if phase_label == "premarket" and state.historical_only:
            return "carry_review"
        if phase_label in {"auction", "opening", "open_confirm"} and not self._feedback_metrics_ready(state):
            return "await_anchor"
        if phase_label == "intraday" and state.stale_snapshot_only:
            return "stale_review"
        score = summary.sentiment_score
        battle = summary.battle_status or ""
        if battle == "bullish" and score >= 6.0:
            return "attack"
        if score >= 4.5:
            return "probe"
        if score >= 3.0:
            return "defense"
        return "ice"

    def _infer_position_cap(self, summary, state: StrategyConsoleState, *, phase_label: str) -> int:
        if phase_label == "postmarket":
            return 0
        if phase_label == "premarket" and state.historical_only:
            return 0
        if phase_label in {"auction", "opening", "open_confirm"} and not self._feedback_metrics_ready(state):
            return 0
        if phase_label == "intraday" and state.stale_snapshot_only:
            return 0
        score = summary.sentiment_score
        if score >= 6.0:
            cap = 80
        elif score >= 5.0:
            cap = 65
        elif score >= 4.0:
            cap = 50
        elif score >= 3.0:
            cap = 35
        else:
            cap = 20
        if summary.battle_status == "frozen":
            cap -= 10
        if state.missing_inputs:
            cap -= 10
        if summary.mainline_switch:
            cap -= 5
        return max(10, min(85, cap))

    def _collect_missing_inputs(
        self,
        intraday_context: IntradayContext,
        *,
        phase_label: str,
        startup_report: StartupSelfCheckReport | None = None,
    ) -> tuple[str, ...]:
        missing = []
        status_map = startup_report.by_dataset() if startup_report is not None else {}

        auction_status = status_map.get("auction_anchor")
        if auction_status is not None:
            if not auction_status.ready:
                missing.append("auction_anchor_pending" if phase_label == "premarket" else "auction_anchor")
        elif not intraday_context.auction_map:
            missing.append("auction_anchor_pending" if phase_label == "premarket" else "auction_anchor")

        yest_limit_status = status_map.get("yest_limit_pool")
        if yest_limit_status is not None:
            if not yest_limit_status.ready:
                missing.append("yest_limit_pool")
        elif not intraday_context.yest_limit_map:
            missing.append("yest_limit_pool")

        today_hot_plate_missing = not intraday_context.hot_plate_map
        yesterday_hot_plate_ready = bool(intraday_context.yesterday_hot_plate_map)
        if today_hot_plate_missing:
            if phase_label == "premarket" and yesterday_hot_plate_ready:
                pass
            elif yesterday_hot_plate_ready:
                missing.append("hot_plates_today_missing")
            else:
                missing.append("hot_plates")
        return tuple(missing)

    def _hot_plate_render_mode(self, state: StrategyConsoleState) -> str:
        if state.context.hot_plate_map:
            return "today"
        if state.context.yesterday_hot_plate_map:
            return "fallback"
        return "missing"

    def _hot_plate_note(self, state: StrategyConsoleState) -> str:
        mode = self._hot_plate_render_mode(state)
        if mode == "fallback":
            return "today"
        if mode == "missing":
            return "today"
        return ""

    def _has_missing_inputs(self, state: StrategyConsoleState, *keys: str) -> bool:
        missing = set(state.missing_inputs)
        return any(key in missing for key in keys)

    def _auction_anchor_ready(self, state: StrategyConsoleState) -> bool:
        return not self._has_missing_inputs(state, "auction_anchor", "auction_anchor_pending")

    def _yest_limit_ready(self, state: StrategyConsoleState) -> bool:
        return not self._has_missing_inputs(state, "yest_limit_pool")

    def _feedback_metrics_ready(self, state: StrategyConsoleState) -> bool:
        return self._auction_anchor_ready(state) and self._yest_limit_ready(state) and state.context.market_summary.total_yest_limit_count > 0

    def _build_coverage_scope(
        self,
        intraday_context: IntradayContext,
        *,
        snapshot_map: dict[str, StockStateSnapshot] | None = None,
    ) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()
        snapshot_map = snapshot_map or {snapshot.symbol: snapshot for snapshot in intraday_context.stock_snapshots}

        def _add(symbols: Iterable[str]) -> None:
            for symbol in symbols:
                text = str(symbol or "").strip()
                if not text or text in seen:
                    continue
                snapshot = snapshot_map.get(text)
                if snapshot is not None and self._display_plate_name(snapshot) == "-":
                    continue
                seen.add(text)
                ordered.append(text)

        auction_rows = tuple(intraday_context.auction_map.values())
        auction_ranked = nlargest(
            self.AUCTION_TOP_AMOUNT_LIMIT,
            auction_rows,
            key=lambda row: float(row.get("amount", 0.0) or 0.0),
        )
        top_amount_symbols = [
            str(row.get("symbol") or "")
            for row in auction_ranked[: self.AUCTION_TOP_AMOUNT_LIMIT]
            if str(row.get("symbol") or "").strip()
        ]
        amount_gate_symbols = [
            str(row.get("symbol") or "")
            for row in auction_rows
            if float(row.get("amount", 0.0) or 0.0) >= self.AUCTION_MIN_AMOUNT
            and str(row.get("symbol") or "").strip()
        ]
        yest_limit_symbols = list(intraday_context.yest_limit_map.keys())
        turnover_symbols = list(intraday_context.market_summary.top_turnover_symbols[:20])

        _add(top_amount_symbols)
        _add(amount_gate_symbols)
        _add(yest_limit_symbols)
        _add(turnover_symbols)
        return tuple(ordered)

    def _infer_actual_source(
        self,
        intraday_context: IntradayContext,
        coverage_scope: Iterable[str],
        *,
        phase_label: str,
        startup_report: StartupSelfCheckReport | None = None,
    ) -> str:
        if phase_label == "intraday":
            quote_fresh_ratio = self._quote_fresh_ratio_from_context_notes(intraday_context)
            if quote_fresh_ratio >= 0.95:
                return "t1_v2_q2_live"
            if quote_fresh_ratio >= 0.20:
                return "stale_intraday_snapshot"
        source_counts: dict[str, int] = {}
        scope = {str(symbol) for symbol in coverage_scope if str(symbol)}
        rows = (
            row
            for symbol, row in intraday_context.auction_map.items()
            if not scope or str(symbol) in scope
        )
        for row in rows:
            source = str(row.get("source") or "").strip()
            if not source:
                continue
            source_counts[source] = source_counts.get(source, 0) + 1
        if source_counts:
            dominant = max(source_counts.items(), key=lambda item: item[1])[0]
            if phase_label == "intraday" and dominant in {"redis_anchor", "redis_0925", "redis_preview_0920", "redis_preview_0924"}:
                return "stale_intraday_snapshot"
            return dominant

        status_map = startup_report.by_dataset() if startup_report is not None else {}
        if phase_label == "premarket":
            hot_plate_status = status_map.get("hot_plates")
            yest_limit_status = status_map.get("yest_limit_pool")
            if hot_plate_status and hot_plate_status.ready and yest_limit_status and yest_limit_status.ready:
                return "startup_repair"
            if hot_plate_status and hot_plate_status.ready:
                return hot_plate_status.source or "hot_plate_cache"
            if yest_limit_status and yest_limit_status.ready:
                return yest_limit_status.source or "yest_limit_cache"

        if intraday_context.hot_plate_map and intraday_context.yest_limit_map:
            return "stale_intraday_snapshot" if phase_label == "intraday" else "startup_repair"
        if intraday_context.hot_plate_map:
            return "hot_plate_cache"
        if intraday_context.yest_limit_map:
            return "yest_limit_cache"
        if getattr(intraday_context.market_summary, "top_plate_name", ""):
            return "stale_intraday_snapshot" if phase_label == "intraday" else "redis_runtime_projection"
        return "unknown"

    @staticmethod
    def _quote_fresh_ratio_from_context_notes(intraday_context: IntradayContext) -> float:
        for note in getattr(intraday_context, "notes", ()) or ():
            text = str(note or "")
            matched = re.search(r"quote_freshness=(\d+)/(\d+)", text)
            if not matched:
                continue
            fresh, total = matched.groups()
            try:
                denominator = int(total)
                return (int(fresh) / denominator) if denominator > 0 else 0.0
            except (TypeError, ValueError, ZeroDivisionError):
                return 0.0
        return 0.0

    def _is_frozen_postmarket_context(self, intraday_context: IntradayContext) -> bool:
        coverage_scope = self._build_coverage_scope(intraday_context)
        actual_source = self._infer_actual_source(
            intraday_context,
            coverage_scope,
            phase_label="postmarket",
        )
        return actual_source in {
            "redis_anchor",
            "redis_0925",
            "redis_preview_0920",
            "redis_preview_0924",
            "stale_intraday_snapshot",
        }

    def _is_historical_mode(self, state: StrategyConsoleState, *, phase_label: str) -> bool:
        return (phase_label == "premarket" and state.historical_only) or (
            phase_label == "intraday" and state.stale_snapshot_only
        )

    def _display_action_label(
        self,
        decision: AuctionLadderDecision | None,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> str:
        action_code = self._display_action_code(decision, state, phase_label=phase_label)
        label = self._action_text(action_code)
        if decision is None:
            return label
        if action_code == "observe_only" and phase_label in {"opening", "open_confirm"}:
            return "开盘先观察"
        return label

    def _display_action_code(
        self,
        decision: AuctionLadderDecision | None,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> str:
        if decision is None:
            return "observe_only"
        action = self.ACTION_LABELS.get(decision.action, decision.action)
        if phase_label == "intraday" and state.stale_snapshot_only:
            if action in {"failed_promo_guard", "do_not_chase", "observe_only"}:
                return action
            return "observe_only"
        return action

    def _display_action_reason_text(
        self,
        decision: AuctionLadderDecision | None,
        state: StrategyConsoleState | None,
        *,
        phase_label: str,
    ) -> str:
        if decision is None or state is None:
            return ""
        output_summary = self._playbook_output_summary_for_state(state)
        if output_summary is not None:
            summary_reason = self._summary_reason_text_map(output_summary).get(decision.symbol, "")
            if summary_reason:
                return summary_reason
            risk_tags = tuple(getattr(output_summary, "risk_tags", ()) or ())
            risk_parts = [
                text
                for text in (self._risk_tag_prompt_text(tag) for tag in risk_tags[:2])
                if text
            ]
            if risk_parts:
                return " / ".join(dict.fromkeys(risk_parts))
        if decision.reasons:
            primary_reason = str(next((item for item in decision.reasons if str(item).strip()), "") or "").strip()
            if primary_reason:
                return primary_reason
        snapshot = state.snapshot_map.get(decision.symbol)
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        display_code = self._display_action_code(decision, state, phase_label=phase_label)
        if state.stale_snapshot_only and phase_label == "intraday":
            return "数据滞后，仅供参考"
        if snapshot is not None and self._snapshot_is_falsified_but_leader_alive(state, snapshot):
            return "板块被证伪，但龙头仍有独活迹象"
        if selection is not None and decision.setup_id == "theme_not_tradable_repair_probe":
            if selection.open_follow_state in {"confirmed", "repair_strength"}:
                return "题材偏弱，但修复信号已经出现"
            return "题材偏弱，先当修复观察"
        if snapshot is not None and selection is not None and self._is_high_dayk_weak_leader_trap(snapshot, selection, phase_label=phase_label):
            return "日K位置偏高，容易形成高位假强"
        if snapshot is not None and selection is not None and self._is_stock_auction_fakeout(snapshot, selection, phase_label=phase_label):
            return "竞价形态像假强骗炮"
        if phase_label in {"opening", "open_confirm"} and snapshot is not None:
            confirm_label = self._leader_truth_label(snapshot)
            if confirm_label == self.OPENING_VALIDATION_TRUE_STRONG:
                return "开盘验证为真强，允许继续盯前排"
            if confirm_label in {self.OPENING_VALIDATION_LOW_OPEN_STRONG, self.OPENING_VALIDATION_PULLBACK_REBOUND}:
                return "开盘出现低开转强/回落修复"
            if confirm_label in {self.OPENING_VALIDATION_GAP_WEAK, self.OPENING_VALIDATION_UNDERTAKE_WEAK}:
                return "开盘承接不足，先不追"
            if confirm_label == self.OPENING_VALIDATION_HARD_TO_CHASE:
                return "高开过热，性价比偏低"
        if display_code == "confirm_then_go":
            return "等确认后再出手"
        if display_code == "leader_watch":
            return "先看龙头怎么走"
        if display_code == "front_row_watch":
            return "只看前排是否继续加强"
        if display_code == "ice_probe":
            return "冰点试错，仓位从轻"
        if display_code == "failed_promo_guard":
            return "晋级失败风险较高"
        if display_code == "do_not_chase":
            return "位置过热，禁止追高"
        if display_code == "observe_only":
            return "当前只适合观察"
        if display_code == "leader_hold":
            return "已有先手可考虑持有"
        return ""

    def _compact_stock_ref(
        self,
        snapshot: StockStateSnapshot | None,
        *,
        symbol: str = "",
        plate: str | None = None,
    ) -> str:
        if snapshot is None:
            return symbol or "-"
        resolved_plate = plate if plate is not None else self._display_plate_name(snapshot, prefer_high_board=True)
        return (
            f"{self._short_stock_name(snapshot, symbol=symbol)}"
            f"({self._fmt_pct(snapshot.open_pct)}/{self._fmt_pct(snapshot.current_pct)}/{resolved_plate or '-'})"
        )

    @staticmethod
    def _action_line_symbol(text: str) -> str:
        payload = str(text or "").strip()
        if "=" not in payload:
            return ""
        return str(payload.split("=", 1)[0] or "").strip()

    def _playbook_theme_family(self, state: StrategyConsoleState) -> tuple[str, frozenset[str]]:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is None:
            return "", frozenset()
        main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
        family: list[str] = []
        for raw in (
            main_attack,
            *tuple(getattr(global_decision, "secondary_themes", ()) or ()),
        ):
            normalized = collapse_runtime_primary_plate(str(raw or ""))
            if normalized and normalized not in family:
                family.append(normalized)
        return collapse_runtime_primary_plate(main_attack), frozenset(family)

    def _symbol_theme_name_for_partition(self, state: StrategyConsoleState, symbol: str) -> str:
        decision = state.decision_map.get(symbol)
        if decision is not None:
            normalized = collapse_runtime_primary_plate(str(getattr(decision, "theme_name", "") or ""))
            if normalized:
                return normalized
        snapshot = state.snapshot_map.get(symbol)
        if snapshot is not None:
            normalized = collapse_runtime_primary_plate(self._display_plate_name(snapshot, prefer_high_board=True))
            if normalized:
                return normalized
        return ""

    def _partition_watch_actions(
        self,
        state: StrategyConsoleState,
        output_summary,
        watch_actions: Iterable[str],
    ) -> tuple[list[str], list[str]]:
        main_attack, mainline_family = self._playbook_theme_family(state)
        off_mainline_symbols: set[str] = set()
        for raw in tuple(getattr(output_summary, "watch_reasons", ()) or ()):
            text = str(raw or "").strip()
            symbol = self._action_line_symbol(text)
            if not symbol:
                continue
            symbol_theme = self._symbol_theme_name_for_partition(state, symbol)
            if symbol_theme and mainline_family and symbol_theme not in mainline_family:
                off_mainline_symbols.add(symbol)
                continue
            if "_off_mainline" in text:
                off_mainline_symbols.add(symbol)
        aligned: list[str] = []
        off_mainline: list[str] = []
        for raw in watch_actions:
            text = str(raw or "").strip()
            if not text:
                continue
            symbol = self._action_line_symbol(text)
            symbol_theme = self._symbol_theme_name_for_partition(state, symbol) if symbol else ""
            is_structured_off_mainline = bool(symbol_theme and mainline_family and symbol_theme not in mainline_family)
            if "_off_mainline" in text or (symbol and symbol in off_mainline_symbols) or is_structured_off_mainline:
                off_mainline.append(text)
            else:
                aligned.append(text)
        return aligned, off_mainline

    def _snapshot_name_by_symbol_compact(self, state: StrategyConsoleState | IntradayContext, symbol: str) -> str:
        if isinstance(state, StrategyConsoleState):
            matched = state.snapshot_map.get(symbol)
            return self._compact_stock_ref(matched, symbol=symbol)
        matched = next((snapshot for snapshot in state.stock_snapshots if snapshot.symbol == symbol), None)
        return self._compact_stock_ref(matched, symbol=symbol)

    def _select_plate_representative_symbol(
        self,
        state: StrategyConsoleState,
        *,
        plate_name: str,
        symbols: Iterable[str],
    ) -> str:
        normalized_plate = normalize_plate_name(plate_name)
        unique_symbols = tuple(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
        if not unique_symbols:
            return ""

        def _matches_plate(snapshot: StockStateSnapshot | None) -> bool:
            if snapshot is None:
                return False
            names = tuple(normalize_plate_name(name) for name in tuple(snapshot.real_plate_names or ()) if normalize_plate_name(name))
            if normalized_plate and normalized_plate in names:
                return True
            display_plate = normalize_plate_name(self._display_plate_name(snapshot, prefer_high_board=True))
            return bool(normalized_plate and display_plate == normalized_plate)

        return max(
            unique_symbols,
            key=lambda symbol: (
                1 if _matches_plate(state.snapshot_map.get(symbol)) else 0,
                1 if state.snapshot_map.get(symbol) is not None and self._is_limit_up_snapshot(state.snapshot_map.get(symbol)) else 0,
                int(state.snapshot_map.get(symbol).lb_days if state.snapshot_map.get(symbol) is not None else 0),
                -int(state.snapshot_map.get(symbol).leader_rank_in_theme if state.snapshot_map.get(symbol) is not None else 999),
                float(state.snapshot_map.get(symbol).amount_2m if state.snapshot_map.get(symbol) is not None else 0.0),
                float(state.snapshot_map.get(symbol).current_pct if state.snapshot_map.get(symbol) is not None else 0.0),
                float(state.snapshot_map.get(symbol).auction_amount if state.snapshot_map.get(symbol) is not None else 0.0),
                symbol,
            ),
        )

    def _decision_name_compact(self, state: StrategyConsoleState, decision: AuctionLadderDecision) -> str:
        matched = state.snapshot_map.get(decision.symbol)
        return self._compact_stock_ref(matched, symbol=decision.symbol)

    def _format_plan_item(self, state: StrategyConsoleState, decision: AuctionLadderDecision) -> str:
        action = self._action_text(self.ACTION_LABELS.get(decision.action, decision.action))
        return f"{self._decision_name_compact(state, decision)}:{action}@{decision.confidence}"

    def _selection_context_for_symbol(
        self,
        state: StrategyConsoleState,
        symbol: str,
    ) -> StockSelectionContext | None:
        bundle = state.bundle
        if bundle is None:
            return None
        for item in bundle.stock_selection_contexts:
            if item.symbol == symbol:
                return item
        return None

    def _theme_selection_for_symbol(
        self,
        state: StrategyConsoleState | None,
        symbol: str,
    ) -> ThemeSelectionContext | None:
        if state is None or state.bundle is None:
            return None
        selection = self._selection_context_for_symbol(state, symbol)
        if selection is None:
            return None
        return state.bundle.theme_context_map.get(selection.plate_name)

    def _snapshot_2m_follow_tag(self, snapshot: StockStateSnapshot | None, *, concise: bool = False) -> str:
        if snapshot is None or snapshot.auction_amount <= 0:
            return ""
        ratio = float(snapshot.amount_2m or 0.0) / float(snapshot.auction_amount or 1.0)
        if ratio >= 1.2:
            return "2mFollow=strong" if concise else "前2分钟强承接"
        if ratio < 0.75:
            return "2mFollow=weak" if concise else "前2分钟承接弱"
        if snapshot.amount_2m >= snapshot.auction_amount:
            return "" if concise else "换手承接跟上"
        return ""

    @staticmethod
    def _dedupe_text_items(items: list[str], *, limit: int, sep: str = "、") -> str:
        deduped: list[str] = []
        for item in items:
            if item and item not in deduped:
                deduped.append(item)
        return sep.join(deduped[:limit]) if deduped else ""

    def _matrix_label_text(self, label: str) -> str:
        mapping = {
            "high_open_distribution_trap": "trap",
            "switch_front_row_attack": "switch",
            "mainline_continuation_attack": "continue",
            "weak_market_non_core_filter": "weak_filter",
            "defensive_absorb_repair": "repair",
            "weak_market_true_core_only": "weak_core",
            "selloff_repair_reversal": "reversal",
        }
        return mapping.get(str(label or ""), str(label or ""))

    def _rank_pct_bucket_text(self, value: float) -> str:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return "-"
        if number <= 0.20:
            return "top20%"
        if number <= 0.35:
            return "top35%"
        if number <= 0.50:
            return "top50%"
        return "back50%"

    def _decision_meta_tags(
        self,
        decision: AuctionLadderDecision,
        snapshot_map: dict[str, StockStateSnapshot],
        state: StrategyConsoleState | None = None,
    ) -> str:
        tags: list[str] = []
        snapshot = snapshot_map.get(decision.symbol)
        follow_tag = self._snapshot_2m_follow_tag(snapshot, concise=True)
        if follow_tag:
            tags.append(follow_tag)
        if decision.setup_id not in {"", "observe_only"}:
            tags.append(f"setup={self._matrix_label_text(decision.setup_id)}")
        if state is not None:
            selection = self._selection_context_for_symbol(state, decision.symbol)
            if selection is not None:
                tags.append(f"dayK={selection.daily_height_bucket}")
                tags.append(f"2m={self._rank_pct_bucket_text(selection.stock_amount_2m_rank_in_theme_pct)}")
                theme_selection = self._theme_selection_for_symbol(state, decision.symbol)
                if theme_selection is not None:
                    tags.append(f"role={theme_selection.plate_role}")
        return " / ".join(tags[:4])

    def _format_stock_hot_text(self, snapshot: StockStateSnapshot | None) -> str:
        if snapshot is None:
            return "-"
        rank = int(snapshot.ths_hot_rank or 999)
        heat = float(snapshot.ths_hot_heat or 0.0)
        rank_text = self._fmt_hot_rank(rank)
        if heat <= 0:
            return rank_text
        if heat >= 10_000:
            heat_text = f"{heat / 10_000:.1f}万"
        else:
            heat_text = f"{heat:.0f}"
        return f"{rank_text}/{heat_text}" if rank_text != "-" else heat_text

    def _money_mode_metrics(self, state: StrategyConsoleState) -> dict[str, int]:
        snapshots = tuple(state.snapshot_map.values())
        high_board_huddle_count = 0
        mid_promotion_count = 0
        first_board_expansion_count = 0
        large_cap_trend_count = 0
        repair_reversal_count = 0
        weak_open_count = 0
        for snapshot in snapshots:
            amount_2m = float(snapshot.amount_2m or 0.0)
            if (
                snapshot.lb_days >= 3
                and snapshot.leader_rank_in_theme <= 2
                and snapshot.current_pct >= snapshot.open_pct - 0.02
                and (amount_2m >= 30_000_000 or snapshot.speed_1m > 0.006)
            ):
                high_board_huddle_count += 1
            if (
                1 <= snapshot.lb_days <= 2
                and snapshot.leader_rank_in_theme <= 3
                and snapshot.current_pct >= snapshot.open_pct - 0.02
                and amount_2m >= 30_000_000
            ):
                mid_promotion_count += 1
            if (
                snapshot.lb_days == 0
                and snapshot.current_pct >= 0.05
                and amount_2m >= 20_000_000
                and snapshot.leader_rank_in_theme <= 3
            ):
                first_board_expansion_count += 1
            if (
                float(snapshot.market_cap_yi or 0.0) >= 200.0
                and snapshot.current_pct >= 0.02
                and amount_2m >= 100_000_000
                and snapshot.speed_1m > -0.002
            ):
                large_cap_trend_count += 1
            if self._is_low_open_rebound_snapshot(snapshot):
                repair_reversal_count += 1
            if snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.04:
                weak_open_count += 1
        return {
            "high_board_huddle_count": high_board_huddle_count,
            "mid_promotion_count": mid_promotion_count,
            "first_board_expansion_count": first_board_expansion_count,
            "large_cap_trend_count": large_cap_trend_count,
            "repair_reversal_count": repair_reversal_count,
            "weak_open_count": weak_open_count,
        }

    def _market_slice_comparison_for_phase(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str | None = None,
    ):
        resolved_phase = str(phase_label or self._phase_label_for_context(state.context.phase) or "")
        summary = getattr(state.context, "market_summary", None)
        if resolved_phase in {"open_confirm", "intraday", "postmarket"}:
            return build_opening_2m_slice_comparison(summary)
        return build_market_topn_slice_comparison(summary)

    def _front_row_strength_state(self, state: StrategyConsoleState, *, phase_label: str | None = None) -> str:
        comparison = self._market_slice_comparison_for_phase(state, phase_label=phase_label)
        return comparison.strength_state

    def _current_market_slice_comparison_for_phase(self, phase_label: str):
        context = getattr(self, "_current_eval_context", None)
        if context is None:
            return build_market_topn_slice_comparison(None)
        summary = getattr(context, "market_summary", None)
        if phase_label in {"open_confirm", "intraday", "postmarket"}:
            return build_opening_2m_slice_comparison(summary)
        return build_market_topn_slice_comparison(summary)

    @staticmethod
    def _money_mode_metrics_support_repair(metrics: dict[str, int]) -> bool:
        return metrics["repair_reversal_count"] >= 2 and metrics["weak_open_count"] <= max(1, metrics["repair_reversal_count"])

    @staticmethod
    def _money_mode_metrics_show_huddle_bias(metrics: dict[str, int]) -> bool:
        return (
            metrics["high_board_huddle_count"] >= 1
            and metrics["mid_promotion_count"] <= 1
            and metrics["first_board_expansion_count"] <= 1
        )

    @staticmethod
    def _money_mode_metrics_show_large_cap(metrics: dict[str, int]) -> bool:
        return metrics["large_cap_trend_count"] >= 2

    @staticmethod
    def _money_mode_metrics_show_mid_promotion(metrics: dict[str, int]) -> bool:
        return metrics["mid_promotion_count"] >= 2

    @staticmethod
    def _money_mode_metrics_show_first_board(metrics: dict[str, int]) -> bool:
        return metrics["first_board_expansion_count"] >= 3

    def _effective_money_mode_code(self, state: StrategyConsoleState) -> str:
        summary = state.context.market_summary
        phase_label = self._phase_label_for_context(state.context.phase)
        regime = self._infer_regime_stage(summary, state, phase_label=phase_label)
        collision_rows = self._theme_collision_rows(state) if self._expectation_ready(state) else ()
        collision_row = collision_rows[0] if collision_rows else None
        judge = self._theme_judge_for_plate(state, collision_row.plate_name) if collision_row is not None else None
        metrics = self._money_mode_metrics(state)
        front_state = self._front_row_strength_state(state, phase_label=phase_label)
        if phase_label in {"intraday", "open_confirm"}:
            if self._money_mode_metrics_support_repair(metrics):
                return "repair_reversal"
            if front_state in {"very_weak", "weak"} and metrics["first_board_expansion_count"] < 3:
                if metrics["repair_reversal_count"] >= 1:
                    return "repair_reversal"
                if metrics["high_board_huddle_count"] >= 1:
                    return "high_board_huddle"
            if self._money_mode_metrics_show_huddle_bias(metrics):
                if judge is not None and judge.action_class == "anchor_only":
                    return "high_board_huddle"
            if self._money_mode_metrics_show_mid_promotion(metrics):
                return "mid_rank_promotion"
            if self._money_mode_metrics_show_first_board(metrics):
                return "first_board_expansion"
            if self._money_mode_metrics_show_large_cap(metrics):
                return "large_cap_trend"
            return "no_clear_edge"
        if front_state in {"very_weak", "weak"}:
            if judge is not None and judge.action_class == "anchor_only":
                return "high_board_huddle"
            if collision_row is not None and collision_row.row.turn_strong_count >= 1 and collision_row.row.leader_count >= 1:
                return "repair_reversal"
        if judge is not None and judge.action_class == "anchor_only":
            return "high_board_huddle"
        if collision_row is not None:
            row = collision_row.row
            if row.limit_up_count >= 2 and row.highest_lb_days >= 2 and row.turn_strong_count >= 1:
                return "mid_rank_promotion"
            if row.limit_up_count >= 2 and row.highest_lb_days <= 1 and row.symbol_count >= 3:
                return "first_board_expansion"
        capital_row = self._top_theme_by_capital(state, market_scope=True)
        if capital_row is not None and capital_row.hot_net_inflow_yi > 0 and capital_row.limit_up_count <= 1 and capital_row.auction_amount >= 1_500_000_000:
            return "large_cap_trend"
        if regime == "defense":
            return "high_board_huddle"
        if regime == "probe":
            return "mid_rank_promotion"
        return "no_clear_edge"

    def _money_mode_label(self, mode_code: str) -> str:
        return self.MONEY_MODE_LABELS.get(mode_code, self.MONEY_MODE_LABELS["no_clear_edge"])

    def _mode_risk_prompt(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        summary_prompt = self._summary_mode_risk_prompt(state, phase_label=phase_label)
        if summary_prompt:
            return summary_prompt
        mode_code = self._effective_money_mode_code(state)
        mode_label = self._money_mode_label(mode_code)
        mode_constraint = self.MONEY_MODE_CONSTRAINTS.get(mode_code, "等待确认")
        selection_map = self._stock_selection_context_map(state)
        faded_count = sum(1 for item in selection_map.values() if item.open_follow_state == "faded")
        repair_count = sum(1 for item in selection_map.values() if item.open_follow_state == "repair_strength")
        opening_payload: dict[str, object] = {}
        if phase_label in {"opening", "open_confirm", "intraday"}:
            trade_date = str(getattr(state.context, "trade_date", "") or "")
            if trade_date:
                opening_payload = self._load_opening_validation_payload(trade_date)
        validation_state = str(opening_payload.get("mode_validation") or "")
        if validation_state == "falsified":
            return f"{mode_label}被开盘2分钟证伪，先收缩到观察；{mode_constraint}"
        if validation_state == "partial":
            return f"{mode_label}只部分成立，只留前排活口；{mode_constraint}"
        if mode_code == "repair_reversal" and repair_count >= 2:
            return f"{mode_label}，低开转强样本增加({repair_count})；{mode_constraint}"
        if mode_code == "high_board_huddle" and faded_count >= 3:
            return f"{mode_label}，后排开盘掉队较多({faded_count})，只看高位活口；{mode_constraint}"
        if mode_code == "high_board_huddle":
            return f"{mode_label}，板块扩散不明，只看高位活口"
        if phase_label in {"opening", "open_confirm", "intraday"} and (faded_count > 0 or repair_count > 0):
            return f"{mode_label}，盘中结构=修复{repair_count}/掉队{faded_count}；{mode_constraint}"
        return f"{mode_label}，{mode_constraint}"

    def _primary_prediction_summary(self, state: StrategyConsoleState) -> str:
        global_decision = self._global_market_decision_for_state(state)
        if global_decision is not None:
            main_attack = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
            secondary = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "secondary_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            watch_themes = tuple(
                normalize_plate_name(str(item or ""))
                for item in tuple(getattr(global_decision, "watch_themes", ()) or ())
                if normalize_plate_name(str(item or ""))
            )
            script_label = self._market_script_prompt_text(str(getattr(global_decision, "market_script", "") or ""))
            if main_attack:
                bias = "/".join(secondary[:2]) if secondary else ("/".join(watch_themes[:2]) if watch_themes else "-")
                return f"{main_attack}={script_label or '-'};跟踪={bias}"
            if watch_themes:
                return f"{watch_themes[0]}={script_label or '观察验证'};等待扩散确认"
        return "-"

    @staticmethod
    def _collision_rank_text(row: AuctionPlateBucketStat, rank: int, *, hot: bool = False) -> str:
        if hot and row.hot_rank >= 999 and row.hot_strength <= 0 and row.hot_net_inflow_yi == 0:
            return "-"
        return str(rank)

    def _execution_mainline_pair(self, state: StrategyConsoleState) -> tuple[str, str]:
        ordered = list(self._execution_theme_candidates(state))
        if not ordered:
            return "-", "-"
        lead = ordered[0]
        secondary = next((name for name in ordered[1:] if name != lead), "-")
        return lead, secondary

    def _money_mode_confidence(self, state: StrategyConsoleState, mode_code: str) -> float:
        metrics = self._money_mode_metrics(state)
        front_state = self._front_row_strength_state(
            state,
            phase_label=self._phase_label_for_context(state.context.phase),
        )
        score = 0.42
        if mode_code == "high_board_huddle":
            score += min(metrics["high_board_huddle_count"], 2) * 0.16
            score += 0.10 if metrics["mid_promotion_count"] <= 1 else 0.0
        elif mode_code == "mid_rank_promotion":
            score += min(metrics["mid_promotion_count"], 3) * 0.14
        elif mode_code == "first_board_expansion":
            score += min(metrics["first_board_expansion_count"], 4) * 0.10
        elif mode_code == "large_cap_trend":
            score += min(metrics["large_cap_trend_count"], 3) * 0.15
        elif mode_code == "repair_reversal":
            score += min(metrics["repair_reversal_count"], 3) * 0.14
            score -= min(metrics["weak_open_count"], 2) * 0.06
        else:
            score -= min(metrics["weak_open_count"], 2) * 0.04
        if front_state == "very_weak":
            score += 0.08 if mode_code in {"repair_reversal", "high_board_huddle"} else -0.06
        elif front_state == "weak":
            score += 0.04 if mode_code in {"repair_reversal", "high_board_huddle", "mid_rank_promotion"} else -0.03
        elif front_state == "strong":
            score += 0.05 if mode_code in {"first_board_expansion", "mid_rank_promotion", "large_cap_trend"} else -0.02
        return round(max(0.25, min(score, 0.95)), 2)

    def _validate_auction_mode_with_opening_2m(
        self,
        *,
        auction_mode_code: str,
        opening_mode_code: str,
        theme_validation: Iterable[dict[str, object]],
        state: StrategyConsoleState | None = None,
    ) -> tuple[str, str]:
        confirmed_count, falsified_count = self._money_mode_opening_alignment_counts(theme_validation)
        opening_front_weak = False
        opening_front_strong = False
        if state is not None:
            opening_front = self._market_slice_comparison_for_phase(state, phase_label="open_confirm")
            opening_front_weak = opening_front.is_weak
            opening_front_strong = opening_front.is_strong
        if (
            auction_mode_code == opening_mode_code
            and auction_mode_code != "no_clear_edge"
            and opening_front_weak
            and confirmed_count == 0
        ):
            return ("partial", "模式一致但前排2m走弱，先降级观察")
        if self._opening_mode_is_no_clear(auction_mode_code):
            return ("partial", "竞价无清晰模式，开盘继续看前排承接")
        if auction_mode_code == opening_mode_code:
            return ("confirmed", "竞价模式与开盘2分钟结构一致")
        if auction_mode_code == "high_board_huddle" and self._opening_mode_is_no_clear(opening_mode_code):
            if confirmed_count >= 1 and falsified_count == 0:
                return ("partial", "高位活口仍在，但扩散不足")
            return ("falsified", "高位活口未能稳住前排承接")
        if auction_mode_code in {"mid_rank_promotion", "first_board_expansion"} and opening_mode_code == "high_board_huddle":
            return ("falsified", "板块扩散未成立，只剩高位独活")
        if self._opening_mode_is_no_clear(opening_mode_code) and opening_front_strong and confirmed_count >= 1:
            return ("partial", "模式不清但前排2m仍有跟随，继续盯前排")
        if self._opening_mode_is_no_clear(opening_mode_code):
            return ("falsified", "竞价预判未获得开盘2分钟确认")
        return ("partial", f"开盘结构切到 {self._money_mode_label(opening_mode_code)}，原预判需降级")

    def _money_mode_validation_label(self, validation_state: str) -> str:
        mapping = {
            "confirmed": "确认",
            "partial": "待确认",
            "falsified": "证伪",
        }
        return mapping.get(validation_state, validation_state or "-")

    def _opening_mode_hard_override(
        self,
        *,
        auction_mode_code: str,
        opening_mode_code: str,
        theme_validation: Iterable[dict[str, object]],
        state: StrategyConsoleState,
    ) -> tuple[str, str]:
        if auction_mode_code not in {"mid_rank_promotion", "first_board_expansion"}:
            return opening_mode_code, ""
        validations = tuple(item for item in theme_validation if isinstance(item, dict))
        if not validations:
            return opening_mode_code, ""
        top = validations[0]
        front_row_count = int(top.get("front_row_count", 0) or 0)
        undertake_count = int(top.get("undertake_count", 0) or 0)
        undertake_count_5m = int(top.get("undertake_count_5m", 0) or 0)
        metrics = self._money_mode_metrics(state)
        high_board_huddle_count = int(metrics.get("high_board_huddle_count", 0) or 0)
        mid_promotion_count = int(metrics.get("mid_promotion_count", 0) or 0)
        first_board_expansion_count = int(metrics.get("first_board_expansion_count", 0) or 0)
        expansion_failed = self._opening_theme_expansion_failed(
            front_row_count=front_row_count,
            undertake_count=undertake_count,
            undertake_count_5m=undertake_count_5m,
            mid_promotion_count=mid_promotion_count,
            first_board_expansion_count=first_board_expansion_count,
        )
        if expansion_failed and high_board_huddle_count >= 1:
            plate_name = str(top.get("plate_name") or "-")
            return "high_board_huddle", f"{plate_name} 高位抱团，扩散不足，先看龙头活口"
        return opening_mode_code, ""

    def _load_recap_reference(self, state: StrategyConsoleState, *, phase_label: str) -> dict[str, object]:
        recap_trade_date, recap_previous_trade_date = self._resolve_recap_trade_dates(
            trade_date=state.context.trade_date,
            phase_label=phase_label,
            historical_only=state.historical_only,
        )
        recap_hot_plate_map = (
            state.context.hot_plate_map
            if phase_label == "postmarket" and recap_trade_date == state.context.trade_date and state.context.hot_plate_map
            else self._load_json_hash(f"cache:hot_plates:{recap_trade_date}")
        )
        recap_previous_hot_plate_map = (
            state.context.yesterday_hot_plate_map
            if phase_label == "postmarket" and state.context.yesterday_hot_plate_map
            else self._load_json_hash(f"cache:hot_plates:{recap_previous_trade_date}")
        )
        recap_yest_limit_map = self._load_json_hash(f"cache:yest_limit_pool:{recap_previous_trade_date}")
        recap_auction_map = (
            state.context.auction_map
            if phase_label == "postmarket" and recap_trade_date == state.context.trade_date and state.context.auction_map
            else self._load_recap_auction_map(recap_trade_date)
        )
        return {
            "trade_date": recap_trade_date,
            "previous_trade_date": recap_previous_trade_date,
            "hot_plate_map": recap_hot_plate_map,
            "previous_hot_plate_map": recap_previous_hot_plate_map,
            "yest_limit_map": recap_yest_limit_map,
            "auction_map": recap_auction_map,
            "truth_rows": self._load_postmarket_limit_truth_rows(recap_trade_date),
        }

    def _classify_recap_migration(self, migration: object) -> str:
        present_today = bool(getattr(migration, "present_today", False))
        present_yesterday = bool(getattr(migration, "present_yesterday", False))
        if present_today and not present_yesterday:
            return "EMERGING"
        if present_yesterday and not present_today:
            return "FADING"
        strength_delta = float(getattr(migration, "strength_delta", 0.0) or 0.0)
        change_pct_delta = float(getattr(migration, "change_pct_delta", 0.0) or 0.0)
        net_inflow_yi_delta = float(getattr(migration, "net_inflow_yi_delta", 0.0) or 0.0)
        up_votes = int(strength_delta > 0) + int(change_pct_delta > 0) + int(net_inflow_yi_delta > 0)
        down_votes = int(strength_delta < 0) + int(change_pct_delta < 0) + int(net_inflow_yi_delta < 0)
        if down_votes >= 2:
            return "FADING"
        if up_votes >= 2:
            return "PERSIST"
        if strength_delta < 0 and (change_pct_delta < 0 or net_inflow_yi_delta < 0):
            return "FADING"
        if strength_delta > 0 and (change_pct_delta > 0 or net_inflow_yi_delta > 0):
            return "PERSIST"
        today_strength = float(getattr(migration, "today_strength", 0.0) or 0.0)
        yesterday_strength = float(getattr(migration, "yesterday_strength", 0.0) or 0.0)
        return "PERSIST" if today_strength >= yesterday_strength else "FADING"

    def _score_opening_validations(self, validated: Iterable[str]) -> dict[str, int]:
        score = {"positive": 0, "negative": 0}
        for item in validated:
            label = self._opening_validation_label(item)
            if label in self.OPENING_VALIDATION_POSITIVE_LABELS:
                score["positive"] += 1
            elif label in self.OPENING_VALIDATION_NEGATIVE_LABELS:
                score["negative"] += 1
        return score

    def _extract_plate_check_names(self, plate_checks: Iterable[str]) -> list[str]:
        names: list[str] = []
        for item in plate_checks:
            name = self._plate_check_name(item)
            if name and name != "-":
                names.append(name)
        return names

    def _auction_outcome_summary(self, state: StrategyConsoleState) -> str:
        summary = state.context.market_summary
        if not self._feedback_metrics_ready(state):
            return "竞价反馈样本不足"
        premium_label, premium_action = self._yest_limit_premium_profile(summary)
        opportunity_label, _opportunity_action = self._yest_limit_opportunity_profile(summary)
        risk_label, _risk_action = self._yest_limit_risk_profile(summary)
        if premium_label == "红开溢价足" and risk_label == "负反馈轻":
            return "竞价溢价与风险都健康"
        if premium_label == "溢价不足":
            return "溢价不足，先手错了就要快撤"
        if opportunity_label == "机会偏少" and risk_label in {"负反馈重", "负反馈可见"}:
            return "机会少且风险高，接力环境差"
        if opportunity_label in {"机会偏少", "有少量机会"}:
            return "机会一般，尽量只看前排"
        return f"{premium_action}，{risk_label}"

    def _load_postmarket_limit_truth_rows(self, trade_date: str) -> tuple[dict[str, object], ...]:
        cache = getattr(self, "_postmarket_limit_truth_cache", None)
        if cache is None:
            cache = {}
            self._postmarket_limit_truth_cache = cache
        cached = cache.get(trade_date)
        if cached is not None:
            return cached
        redis_key = f"cache:limit_truth:{trade_date}"
        rows = self._read_limit_truth_cache(redis_key)
        if rows:
            payload = tuple(rows)
            cache[trade_date] = payload
            return payload
        rows = self._fetch_limit_truth_rows(trade_date)
        payload = tuple(rows)
        cache[trade_date] = payload
        return payload

    def _build_recap_ladder_truth_facts(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> tuple[LadderFact, ...]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        yest_limit_map = ref["yest_limit_map"]
        assert isinstance(yest_limit_map, dict)
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        auction_map = ref["auction_map"]
        assert isinstance(auction_map, dict)

        truth_lb_map: dict[str, int] = {
            str(row.get("symbol") or "").strip(): self._normalize_limitup_truth_lb_days(row.get("lb_days"))
            for row in truth_rows
            if row.get("symbol")
        }
        bucket_symbols: dict[str, list[str]] = defaultdict(list)
        bucket_red_open_count: dict[str, int] = defaultdict(int)
        bucket_promoted_symbols: dict[str, list[str]] = defaultdict(list)

        for symbol, row in yest_limit_map.items():
            symbol_text = str(symbol or "").strip()
            if not symbol_text:
                continue
            try:
                prev_lb_days = max(int((row or {}).get("lb_days", 0) or 0), 0)
            except (TypeError, ValueError, AttributeError):
                prev_lb_days = 0
            if prev_lb_days <= 0:
                continue
            key = f"{prev_lb_days}B->{prev_lb_days + 1}B"
            bucket_symbols[key].append(symbol_text)
            auction_row = auction_map.get(symbol_text, {})
            snapshot = state.snapshot_map.get(symbol_text)
            open_pct = self._normalize_pct_value(
                auction_row.get("change_pct", snapshot.open_pct if snapshot is not None else 0.0)
            )
            if open_pct > 0:
                bucket_red_open_count[key] += 1
            if truth_lb_map.get(symbol_text) == prev_lb_days + 1:
                bucket_promoted_symbols[key].append(symbol_text)

        first_board_symbols = [
            str(row.get("symbol") or "").strip()
            for row in truth_rows
            if self._normalize_limitup_truth_lb_days(row.get("lb_days")) <= 1 and row.get("symbol")
        ]
        if first_board_symbols:
            bucket_symbols["0B->1B"] = first_board_symbols
            bucket_red_open_count["0B->1B"] = sum(
                1
                for symbol in first_board_symbols
                if self._normalize_pct_value(
                    auction_map.get(symbol, {}).get(
                        "change_pct",
                        state.snapshot_map.get(symbol).open_pct if state.snapshot_map.get(symbol) is not None else 0.0,
                    )
                )
                > 0
            )
            bucket_promoted_symbols["0B->1B"] = first_board_symbols

        facts: list[LadderFact] = []
        for key, symbols in bucket_symbols.items():
            promoted_symbols = bucket_promoted_symbols.get(key, [])
            candidate_symbols = promoted_symbols or symbols
            representative_symbol = ""
            if candidate_symbols:
                representative_symbol = max(
                    candidate_symbols,
                    key=lambda symbol: (
                        self._normalize_limitup_truth_lb_days(truth_lb_map.get(symbol, 0)),
                        float(state.snapshot_map.get(symbol).current_pct if state.snapshot_map.get(symbol) is not None else 0.0),
                        float(state.snapshot_map.get(symbol).auction_amount if state.snapshot_map.get(symbol) is not None else 0.0),
                    ),
                )
            facts.append(
                LadderFact(
                    key=key,
                    total_count=len(symbols),
                    red_open_count=int(bucket_red_open_count.get(key, 0)),
                    promoted_count=len(promoted_symbols),
                    representative_symbol=representative_symbol,
                )
            )
        facts.sort(key=lambda item: (-self._ladder_sort_value(item.key), -item.total_count, item.key))
        return tuple(facts)

    def _rank_limitup_plates_from_truth(
        self,
        state: StrategyConsoleState,
        truth_rows: tuple[dict[str, object], ...],
    ) -> list[tuple[str, list[dict[str, object]]]]:
        primary_plate_map = self._load_string_hash(RUNTIME_PRIMARY_PLATE_KEY)
        theme_map = self._load_list_hash(PLATE_MAPPING_S2P_KEY)
        plate_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in truth_rows:
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            snapshot = state.snapshot_map.get(symbol)
            plate_candidates = self._truth_plate_candidates(
                row,
                snapshot,
                primary_plate_map=primary_plate_map,
                theme_map=theme_map,
            )
            if not plate_candidates:
                continue
            enriched = {
                "symbol": symbol,
                "lb_days": self._normalize_limitup_truth_lb_days(row.get("lb_days")),
                "name": str(row.get("name") or self._short_stock_name(snapshot, symbol=symbol)),
                "auction_amount": float(snapshot.auction_amount if snapshot is not None else 0.0),
                "current_pct": float(snapshot.current_pct if snapshot is not None else 0.0),
            }
            for plate in plate_candidates:
                plate_rows[plate].append(enriched)
        return sorted(
            plate_rows.items(),
            key=lambda item: (
                len(item[1]),
                max((self._normalize_limitup_truth_lb_days(row.get("lb_days")) for row in item[1]), default=1),
                max((float(row.get("auction_amount", 0.0) or 0.0) for row in item[1]), default=0.0),
            ),
            reverse=True,
        )

    def _limitup_plate_comment_from_truth(self, rows: list[dict[str, object]]) -> str:
        count = len(rows)
        high_board = max((self._normalize_limitup_truth_lb_days(row.get("lb_days")) for row in rows), default=1)
        if count >= 3 and high_board >= 2:
            return "成队最明显"
        if high_board >= 2:
            return "有高标带队"
        if count >= 3:
            return "首板扩散明显"
        if count >= 2:
            return "前排联动"
        return "零散轮动"

    def _normalize_limitup_truth_lb_days(self, raw: object) -> int:
        try:
            value = int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            value = 1
        return max(value, 1)

    def _format_limitup_board_height(self, lb_days: int) -> str:
        return "首板" if lb_days <= 1 else f"{lb_days}板"

    def _summarize_limitup_mainline(self, state: StrategyConsoleState) -> tuple[str, str]:
        truth_rows = self._load_postmarket_limit_truth_rows(state.context.trade_date)
        return self._summarize_limitup_mainline_by_rows(state, truth_rows)

    def _summarize_limitup_mainline_by_rows(
        self,
        state: StrategyConsoleState,
        truth_rows: tuple[dict[str, object], ...],
    ) -> tuple[str, str]:
        if truth_rows:
            ranked = self._rank_limitup_plates_from_truth(state, truth_rows)
            lead = ranked[0][0] if ranked else "-"
            secondary = ranked[1][0] if len(ranked) > 1 else "-"
            return lead, secondary
        plate_counter: dict[str, int] = defaultdict(int)
        for row in state.context.yest_limit_map.values():
            plate = normalize_plate_name(str((row or {}).get("plate") or ""))
            if plate and not is_generic_plate(plate):
                plate_counter[plate] += 1
        if not plate_counter:
            return "-", "-"
        ranked = sorted(plate_counter.items(), key=lambda item: (-item[1], item[0]))
        return ranked[0][0], (ranked[1][0] if len(ranked) > 1 else "-")

    def _ensure_postmarket_limit_truth_plate_enrichment(
        self,
        trade_date: str,
        truth_rows: tuple[dict[str, object], ...],
    ) -> None:
        if not truth_rows:
            return
        enriched_dates = getattr(self, "_postmarket_limit_truth_enriched_dates", None)
        if enriched_dates is None:
            enriched_dates = set()
            self._postmarket_limit_truth_enriched_dates = enriched_dates
        if trade_date in enriched_dates:
            return
        symbols = tuple(
            dict.fromkeys(
                str(row.get("symbol") or "").strip()
                for row in truth_rows
                if str(row.get("symbol") or "").strip()
            )
        )
        if not symbols:
            enriched_dates.add(trade_date)
            return
        try:
            self._intraday_hub.enrich_stock_plate(
                trade_date,
                RunPhase.POSTMARKET,
                symbols,
                max_symbols=len(symbols),
            )
        except Exception:
            logger.exception("postmarket limit truth plate enrichment failed | trade_date=%s", trade_date)
        enriched_dates.add(trade_date)

    def _render_validated_candidates(self, state: StrategyConsoleState) -> tuple[str, ...]:
        bundle = getattr(state.context, "opening_validation_bundle", None)
        if bundle is None:
            return ()
        selection_map = self._stock_selection_context_map(state)
        confirmed_map = getattr(bundle, "confirmed_themes", {}) or {}
        watch_map = getattr(bundle, "watch_themes", {}) or {}
        falsified_map = getattr(bundle, "falsified_themes", {}) or {}
        attack_items: list[str] = []
        probe_items: list[str] = []
        watch_items: list[str] = []
        avoid_items: list[str] = []
        primary_candidates = tuple(
            decision
            for decision in tuple(state.playbook_decisions or ())
            if decision.action not in {"observe_only", "avoid_after_failed_promotion", "do_not_chase"}
        )
        watch_candidates = tuple(
            decision
            for decision in tuple(state.playbook_decisions or ())
            if decision.action == "observe_only"
        )
        for decision in primary_candidates:
            selection = selection_map.get(decision.symbol)
            if selection is None:
                continue
            snapshot = state.snapshot_map.get(decision.symbol)
            validation = self._opening_validation_for_display(
                state,
                snapshot=snapshot,
                selection=selection,
            )
            if validation is None:
                continue
            plate_name = normalize_plate_name(str(getattr(validation, "plate_name", "") or selection.plate_name or "-"))
            item_text = f"{decision.symbol}:{plate_name}/{decision.action}@{decision.confidence}"
            level = str(getattr(validation, "tradable_level", "") or "")
            status = str(getattr(validation, "validation_state", "") or "")
            if status == "confirmed" and level == "attack":
                attack_items.append(item_text)
            elif status == "confirmed" and level == "probe":
                probe_items.append(item_text)
            elif status == "watch":
                watch_items.append(item_text)
            else:
                avoid_items.append(item_text)
        for decision in watch_candidates:
            selection = selection_map.get(decision.symbol)
            if selection is None:
                continue
            snapshot = state.snapshot_map.get(decision.symbol)
            validation = self._opening_validation_for_display(
                state,
                snapshot=snapshot,
                selection=selection,
            )
            if validation is None:
                continue
            plate_name = normalize_plate_name(str(getattr(validation, "plate_name", "") or selection.plate_name or "-"))
            item_text = f"{decision.symbol}:{plate_name}/{decision.action}@{decision.confidence}"
            status = str(getattr(validation, "validation_state", "") or "")
            if status == "watch" and item_text not in watch_items:
                watch_items.append(item_text)
            elif status == "falsified" and item_text not in avoid_items:
                avoid_items.append(item_text)
        return (
            "【验证后候选】方向 | 清单",
            f"  主攻 | {' ; '.join(attack_items[:3]) or '-'}",
            f"  试错 | {' ; '.join(probe_items[:3]) or '-'}",
            f"  观察 | {' ; '.join(watch_items[:4]) or '-'}",
            f"  回避 | {' ; '.join(avoid_items[:4]) or '-'}",
        )

    def _render_opening_front_slice_line(self, comparison) -> str:
        return (
            f"  前排2m | Top10 {self._fmt_amount_yi_precise(comparison.top10_amount)} / 昨比 {comparison.top10_vs_prev_ratio:.2f}x"
            f" ; Top20 {self._fmt_amount_yi_precise(comparison.top20_amount)} / 昨比 {comparison.top20_vs_prev_ratio:.2f}x"
        )
        return (
            f"{self._close_verdict_text(verdict)}，晋级率 {summary.promotion_rate:.1%}，核按钮率 {summary.headshot_rate:.1%}"
            f"{self._close_verdict_text(verdict)}，晋级率 {summary.promotion_rate:.1%}，核按钮率 {summary.headshot_rate:.1%}"
        )

    def _opening_validation_focus_decisions(
        self,
        state: StrategyConsoleState,
    ) -> tuple[AuctionLadderDecision, ...]:
        picked: list[AuctionLadderDecision] = []
        seen_symbols: set[str] = set()
        for decision in self._order_decisions_by_narrative(
            state,
            self._focus_candidates_for_phase(state, phase_label="open_confirm"),
            phase_label="open_confirm",
        ):
            if decision.symbol in seen_symbols:
                continue
            picked.append(decision)
            seen_symbols.add(decision.symbol)
            if len(picked) >= 5:
                return tuple(picked)
        ordered_playbook = self._order_decisions_by_narrative(
            state,
            tuple(
                decision
                for decision in tuple(state.playbook_decisions or ())
                if self._decision_allowed_in_focus_output(state, decision, phase_label="open_confirm")
            ),
            phase_label="open_confirm",
        )
        for decision in ordered_playbook:
            if decision.symbol in seen_symbols:
                continue
            picked.append(decision)
            seen_symbols.add(decision.symbol)
            if len(picked) >= 5:
                break
        return tuple(picked)

    def _pick_auction_outcome_names(
        self,
        state: StrategyConsoleState,
        *,
        predicate,
        limit: int = 3,
    ) -> list[str]:
        matched = nlargest(
            limit,
            (
                snapshot
                for snapshot in state.snapshot_map.values()
                if snapshot.symbol in state.coverage_scope_set and predicate(snapshot)
            ),
            key=lambda snapshot: (
                snapshot.lb_days,
                snapshot.auction_amount,
                snapshot.amount_2m,
                snapshot.current_pct,
            ),
        )
        return [self._compact_stock_ref(snapshot) for snapshot in matched]

    def _limitup_plate_comment(self, snapshots: list[StockStateSnapshot]) -> str:
        if not snapshots:
            return "-"
        count = len(snapshots)
        high_board = max((snapshot.lb_days for snapshot in snapshots), default=0)
        if count >= 3 and high_board >= 2:
            return "成队最明显"
        if high_board >= 2:
            return "有高标带队"
        if count >= 3:
            return "首板扩散明显"
        if count >= 2:
            return "前排联动"
        return "局部活跃"

    def _order_decisions_by_narrative(
        self,
        state: StrategyConsoleState,
        decisions: tuple[AuctionLadderDecision, ...],
        *,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        if len(decisions) <= 1:
            return decisions
        preferred = self._narrative_priority_plates(state, phase_label=phase_label)
        if not preferred:
            return decisions
        return tuple(
            sorted(
                decisions,
                key=lambda decision: (
                    self._decision_narrative_plate_index(state, decision, phase_label=phase_label),
                    -self._focus_candidate_priority_score(state, decision, phase_label=phase_label),
                    -decision.confidence,
                ),
            )
        )

    def _execution_theme_candidates(self, state: StrategyConsoleState) -> tuple[str, ...]:
        ordered: list[str] = []
        actionable: list[str] = []
        anchor_only: list[str] = []
        if state.theme_judge_map:
            for judge in sorted(
                state.theme_judge_map.values(),
                key=lambda item: (
                    self._theme_action_priority(item.action_class),
                    item.opportunity_score,
                    -item.trap_score,
                ),
                reverse=True,
            ):
                name = normalize_plate_name(judge.plate_name)
                execution_state = self._external_validation_state(judge.validation_state)
                if not name or name == "-" or name in ordered or execution_state == "falsified":
                    continue
                if execution_state == "partial" and judge.action_class == "anchor_only":
                    continue
                if judge.action_class in {"main_attack", "front_row_confirm"}:
                    if name not in actionable:
                        actionable.append(name)
                elif judge.action_class == "anchor_only" and judge.trap_score < 6.0:
                    if name not in anchor_only:
                        anchor_only.append(name)
            ordered.extend(actionable or anchor_only)
        if ordered:
            return tuple(ordered)
        if self._expectation_ready(state):
            for item in self._theme_collision_rows(state):
                name = normalize_plate_name(item.plate_name)
                if (
                    not name
                    or name == "-"
                    or name in ordered
                    or item.fakeout_level == "strong"
                    or item.x_score >= 6.2
                ):
                    continue
                ordered.append(name)
                if len(ordered) >= 3:
                    break
        return tuple(ordered)

    @staticmethod
    def _money_mode_opening_alignment_counts(theme_validation: Iterable[dict[str, object]]) -> tuple[int, int]:
        validations = tuple(theme_validation)
        confirmed_count = sum(1 for item in validations if str(item.get("execution_state") or "") == "confirmed")
        falsified_count = sum(1 for item in validations if str(item.get("execution_state") or "") == "falsified")
        return confirmed_count, falsified_count

    @staticmethod
    def _opening_mode_is_no_clear(mode_code: str) -> bool:
        return not mode_code or mode_code == "no_clear_edge"

    @staticmethod
    def _opening_theme_expansion_failed(
        *,
        front_row_count: int,
        undertake_count: int,
        undertake_count_5m: int,
        mid_promotion_count: int,
        first_board_expansion_count: int,
    ) -> bool:
        return (
            front_row_count >= 2
            and undertake_count < 2
            and undertake_count_5m < 2
            and mid_promotion_count < 2
            and first_board_expansion_count < 2
        )

    @staticmethod
    def _opening_validation_label(item: str) -> str:
        text = str(item or "").strip()
        if not text:
            return ""
        _, _, tail = text.rpartition("=")
        return tail.strip() if tail else text

    @staticmethod
    def _plate_check_name(item: str) -> str:
        text = str(item or "").strip()
        if not text:
            return ""
        head, _, _ = text.partition("(")
        return head.strip()

    def _read_limit_truth_cache(self, redis_key: str) -> list[dict[str, object]]:
        try:
            raw_map = self._intraday_hub.redis.hgetall(redis_key) or {}
        except Exception:
            return []
        rows: list[dict[str, object]] = []
        for symbol, raw in raw_map.items():
            payload: dict[str, object] | None = None
            if isinstance(raw, dict):
                payload = raw
            else:
                try:
                    parsed = json.loads(raw)
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    payload = parsed
            if payload is None:
                continue
            normalized_symbol = str(payload.get("symbol") or symbol or "").strip()[-6:]
            if not normalized_symbol:
                continue
            rows.append(
                {
                    "trade_date": str(payload.get("trade_date") or ""),
                    "symbol": normalized_symbol,
                    "lb_days": self._normalize_limitup_truth_lb_days(payload.get("lb_days")),
                    "source": str(payload.get("source") or "cache"),
                    "name": str(payload.get("name") or ""),
                }
            )
        return rows

    def _fetch_limit_truth_rows(self, trade_date: str) -> list[dict[str, object]]:
        try:
            result = self._intraday_hub.fetch_limit_truth(trade_date, RunPhase.POSTMARKET, max_stocks=500)
            rows = result.rows
        except Exception:
            logger.exception("postmarket limit truth fetch failed | trade_date=%s", trade_date)
            return []
        return [dict(row) for row in rows]

    def _truth_plate_candidates(
        self,
        row: dict[str, object],
        snapshot: StockStateSnapshot | None,
        *,
        primary_plate_map: dict[str, str],
        theme_map: dict[str, list[str]],
    ) -> tuple[str, ...]:
        symbol = str(row.get("symbol") or "").strip()
        primary_plate = normalize_plate_name(primary_plate_map.get(symbol, ""))
        if primary_plate and not is_generic_plate(primary_plate):
            return (primary_plate,)
        themes = theme_map.get(symbol, ())
        chosen = choose_primary_plate(themes)
        if chosen and not is_generic_plate(chosen):
            return (chosen,)
        if snapshot is not None:
            fallback = self._display_plate_name(snapshot, prefer_high_board=True)
            if fallback and fallback != "-":
                return (fallback,)
        return ()

    def _load_string_hash(self, key: str) -> dict[str, str]:
        try:
            raw = self._intraday_hub.redis.hgetall(key) or {}
        except Exception:
            return {}
        return {
            str(field or "").strip(): str(value or "").strip()
            for field, value in raw.items()
            if str(field or "").strip()
        }

    def _load_list_hash(self, key: str) -> dict[str, list[str]]:
        try:
            raw = self._intraday_hub.redis.hgetall(key) or {}
        except Exception:
            return {}
        payload: dict[str, list[str]] = {}
        for field, value in raw.items():
            symbol = str(field or "").strip()
            if not symbol:
                continue
            payload[symbol] = decode_theme_list(value)
        return payload

    def _opening_validation_for_display(
        self,
        state: StrategyConsoleState,
        *,
        snapshot: StockStateSnapshot | None,
        selection: StockSelectionContext | None,
    ):
        extra_plate_names: list[str] = []
        if snapshot is not None:
            judge, matched_plate = self._matched_theme_judge(state, snapshot)
            if judge is not None:
                matched_name = normalize_plate_name(matched_plate or judge.plate_name)
                if matched_name and matched_name != "-":
                    extra_plate_names.append(matched_name)
            for plate_name in self._normalized_plate_names(snapshot):
                normalized_name = normalize_plate_name(plate_name)
                if normalized_name and normalized_name != "-" and normalized_name not in extra_plate_names:
                    extra_plate_names.append(normalized_name)
        return match_opening_validation(
            getattr(state.context, "opening_validation_bundle", None),
            snapshot=snapshot,
            selection=selection,
            extra_plate_names=tuple(extra_plate_names),
        )

    def _focus_candidates_for_phase(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        if state.bundle is None:
            return ()
        ordered = self._focus_ordered_decisions(state, phase_label=phase_label)
        min_confidence = self._focus_min_confidence_for_phase(phase_label)
        filtered = self._filter_trade_candidates_for_state(
            state,
            min_confidence=min_confidence,
            phase_label=phase_label,
        )
        if not filtered:
            fallback_filtered = tuple(
                decision
                for decision in tuple(state.playbook_decisions or ())
                if self._decision_allowed_in_focus_output(state, decision, phase_label=phase_label)
            )
            if fallback_filtered:
                return fallback_filtered
            return self._focus_fallback_candidates(state, ordered, phase_label=phase_label)
        _mode_name, allowed_actions, _mode_tiers, _mode_theme_cap = self._money_mode_profile(state)
        selection_map = self._stock_selection_context_map(state)
        filtered = tuple(
            decision
            for decision in filtered
            if (
                decision.action in allowed_actions
                or decision.action == "hold_only"
                or self._is_soft_focus_exception(
                    state,
                    decision,
                    selection=selection_map.get(decision.symbol),
                    snapshot=state.snapshot_map.get(decision.symbol),
                    phase_label=phase_label,
                )
            )
        )
        if not filtered:
            confirmed_backfill = self._try_confirmed_backfill_for_phase(
                state,
                phase_label=phase_label,
            )
            if confirmed_backfill:
                return confirmed_backfill
            return self._focus_fallback_candidates(state, ordered, phase_label=phase_label)
        filtered = tuple(
            decision
            for decision in filtered
            if not self._is_decision_blocked_by_theme_risk(state, decision, phase_label=phase_label)
        )
        if not filtered:
            confirmed_backfill = self._try_confirmed_backfill_for_phase(
                state,
                phase_label=phase_label,
            )
            if confirmed_backfill:
                return confirmed_backfill
            return self._focus_fallback_candidates(state, ordered, phase_label=phase_label)
        if phase_label in {"auction", "opening", "open_confirm", "intraday"}:
            mode_matched = tuple(
                decision
                for decision in filtered
                if self._decision_matches_money_mode(state, decision, phase_label=phase_label)
            )
            if mode_matched:
                filtered = mode_matched
            else:
                confirmed_backfill = self._try_confirmed_backfill_for_phase(
                    state,
                    phase_label=phase_label,
                )
                if confirmed_backfill:
                    return confirmed_backfill
        filtered_symbols = {decision.symbol for decision in filtered}
        prioritized = tuple(decision for decision in ordered if decision.symbol in filtered_symbols)
        ranked_source = prioritized or filtered
        ranked = tuple(
            sorted(
                ranked_source,
                key=lambda decision: (
                    self._focus_candidate_priority_score(state, decision, phase_label=phase_label),
                    decision.confidence,
                ),
                reverse=True,
            )
        )
        self._log_focus_candidate_breakdown(
            state,
            ranked,
            phase_label=phase_label,
            stage="ranked",
        )
        gated = tuple(
            decision
            for decision in ranked
            if self._focus_candidate_passes_gate(state, decision, phase_label=phase_label)
        )
        if phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}:
            accepted = self._apply_theme_execution_quota(state, gated, phase_label=phase_label)
            self._log_focus_candidate_breakdown(
                state,
                accepted,
                phase_label=phase_label,
                stage="accepted",
            )
            if accepted:
                self._remember_effective_focus_candidates(
                    trade_date=str(getattr(state.context, "trade_date", "") or ""),
                    phase_label=phase_label,
                    decisions=accepted,
                )
                return accepted
            confirmed_backfill = self._try_confirmed_backfill_for_phase(
                state,
                phase_label=phase_label,
            )
            if confirmed_backfill:
                return confirmed_backfill
            return self._focus_fallback_candidates(state, ranked, phase_label=phase_label)
        if gated:
            self._remember_effective_focus_candidates(
                trade_date=str(getattr(state.context, "trade_date", "") or ""),
                phase_label=phase_label,
                decisions=gated,
            )
        return gated

    def _decision_allowed_in_focus_output(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> bool:
        if decision.action in {"avoid_after_failed_promotion", "do_not_chase"}:
            return False
        snapshot = state.snapshot_map.get(decision.symbol)
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        if snapshot is None or selection is None:
            return False
        if decision.action == "observe_only" and not self._can_surface_watch_only_decision(
            state,
            decision=decision,
            snapshot=snapshot,
            selection=selection,
            phase_label=phase_label,
        ):
            return False
        if self._is_decision_blocked_by_theme_risk(state, decision, phase_label=phase_label):
            return False
        judge, _matched_plate = self._matched_theme_judge(state, snapshot)
        opening_validation = self._opening_validation_for_display(
            state,
            snapshot=snapshot,
            selection=selection,
        )
        opening_confirmed = bool(
            opening_validation is not None
            and str(getattr(opening_validation, "validation_state", "") or "") == "confirmed"
            and str(getattr(opening_validation, "tradable_level", "") or "") in {"attack", "probe"}
        )
        repair_probe_exception = (
            decision.setup_id == "theme_not_tradable_repair_probe"
            and selection.open_follow_state in {"confirmed", "repair_strength"}
        )
        if judge is not None:
            execution_state = self._external_validation_state(judge.validation_state)
            tier = self._selection_theme_tier(selection, snapshot)
            if execution_state == "falsified" and decision.action != "hold_only":
                return False
            if judge.action_class == "anchor_only" and not selection.is_true_leader and decision.action != "hold_only":
                if not repair_probe_exception and not opening_confirmed:
                    return False
            if execution_state == "partial" and decision.action != "hold_only" and tier != "dragon":
                if not repair_probe_exception and not opening_confirmed:
                    return False
            if judge.action_class in {"observe", "anchor_only"} and not selection.is_true_leader and decision.action != "hold_only":
                if not repair_probe_exception and not opening_confirmed:
                    return False
        if (
            phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}
            and self._is_stock_auction_fakeout(snapshot, selection, phase_label=phase_label)
            and decision.action != "hold_only"
        ):
            return False
        if not selection.theme_tradable and not selection.is_true_leader and decision.action != "hold_only":
            if not repair_probe_exception and not opening_confirmed:
                return False
        if decision.action == "observe_only" and not self._can_surface_watch_only_decision(
            state,
            decision=decision,
            snapshot=snapshot,
            selection=selection,
            phase_label=phase_label,
        ):
            return False
        return True

    def _decision_narrative_plate_index(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> int:
        preferred = self._narrative_priority_plates(state, phase_label=phase_label)
        if not preferred:
            return 999
        snapshot = state.snapshot_map.get(decision.symbol)
        if snapshot is None:
            return 999
        normalized_names = self._normalized_plate_names(snapshot)
        for idx, plate_name in enumerate(preferred):
            if plate_name in normalized_names:
                return idx
        return 999

    @staticmethod
    def _money_mode_profile_for_code(mode: str) -> tuple[str, frozenset[str], frozenset[str], int]:
        profile_map = {
            "high_board_huddle": (
                "leader_only",
                frozenset({"hold_only"}),
                frozenset({"dragon"}),
                1,
            ),
            "repair_reversal": (
                "repair",
                frozenset({"hold_only", "small_probe_only", "early_boarding_candidate"}),
                frozenset({"dragon", "front_core"}),
                2,
            ),
            "mid_rank_promotion": (
                "front_rotation",
                frozenset({"hold_only", "dragon_early_board", "early_boarding_candidate"}),
                frozenset({"dragon", "front_core", "front_follow"}),
                3,
            ),
            "first_board_expansion": (
                "front_confirm",
                frozenset({"hold_only", "early_boarding_candidate"}),
                frozenset({"dragon", "front_core", "front_follow"}),
                2,
            ),
            "large_cap_trend": (
                "front_confirm",
                frozenset({"hold_only", "early_boarding_candidate"}),
                frozenset({"dragon", "front_core", "front_follow"}),
                2,
            ),
        }
        return profile_map.get(
            mode,
            (
                "watch_only",
                frozenset({"hold_only"}),
                frozenset({"dragon"}),
                1,
            ),
        )

    def _money_mode_profile(self, state: StrategyConsoleState) -> tuple[str, frozenset[str], frozenset[str], int]:
        mode = self._effective_money_mode_code(state)
        return self._money_mode_profile_for_code(mode)

    def _decision_matches_money_mode(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> bool:
        if decision.action == "hold_only":
            return True
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        if selection is None:
            return True
        snapshot = state.snapshot_map.get(decision.symbol)
        judge, _matched_plate = self._matched_theme_judge(state, snapshot)
        tier = self._selection_theme_tier(selection, snapshot)
        mode_name, _allowed_actions, _mode_allowed_tiers, _mode_theme_cap = self._money_mode_profile(state)
        strong_non_hot_signal = self._selection_has_non_hot_strength(selection, snapshot)
        if mode_name == "leader_only":
            return selection.is_true_leader or tier == "dragon"
        if mode_name == "repair":
            if selection.open_follow_state in {"repair_strength", "confirmed"}:
                return True
            return selection.kline_pattern in {"low_open_strength", "pullback_repair", "n_rebound"}
        if mode_name == "front_rotation":
            if tier not in {"dragon", "front_core", "front_follow"}:
                return False
            if judge is not None and judge.action_class in {"main_attack", "front_row_confirm", "anchor_only"}:
                return True
            return selection.is_front_row and selection.open_follow_state != "faded"
        if mode_name == "front_confirm":
            if judge is not None and judge.action_class in {"main_attack", "front_row_confirm"}:
                return True
            return (
                selection.is_front_row
                and selection.open_follow_state in {"confirmed", "repair_strength"}
                and (selection.hot_rank <= 80 or strong_non_hot_signal)
            )
        if phase_label in {"auction", "opening", "open_confirm", "intraday"}:
            return selection.is_true_leader
        return True

    def _remember_effective_focus_candidates(
        self,
        *,
        trade_date: str,
        phase_label: str,
        decisions: tuple[AuctionLadderDecision, ...],
    ) -> None:
        if not trade_date or not decisions:
            return
        cache = getattr(self, "_last_effective_focus_cache", None)
        if cache is None:
            cache = {}
            self._last_effective_focus_cache = cache
        cache[(trade_date, phase_label)] = tuple(decisions[:6])

    def _try_confirmed_backfill_for_phase(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
        existing_symbols: set[str] | None = None,
    ) -> tuple[AuctionLadderDecision, ...]:
        if phase_label not in {"opening", "open_confirm", "intraday"}:
            return ()
        confirmed_backfill = self._backfill_candidates_from_confirmed_themes(
            state,
            phase_label=phase_label,
            existing_symbols=existing_symbols or set(),
        )
        if not confirmed_backfill:
            return ()
        self._remember_effective_focus_candidates(
            trade_date=str(getattr(state.context, "trade_date", "") or ""),
            phase_label=phase_label,
            decisions=confirmed_backfill,
        )
        return confirmed_backfill

    def _focus_min_confidence_for_phase(self, phase_label: str) -> int:
        if phase_label in {"auction", "auction_preview", "opening", "open_confirm"}:
            return self.OPENING_CANDIDATE_MIN_CONFIDENCE
        return self.INTRADAY_CANDIDATE_MIN_CONFIDENCE

    def _filter_trade_candidates_for_state(
        self,
        state: StrategyConsoleState,
        *,
        min_confidence: int,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        bundle = state.bundle
        if bundle is None:
            return ()
        ordered = self._focus_ordered_decisions(state, phase_label=phase_label)
        selection_map = self._stock_selection_context_map(state)
        playbook_symbols = {decision.symbol for decision in tuple(state.playbook_decisions or ())}
        if playbook_symbols:
            filtered: list[AuctionLadderDecision] = []
            seen_symbols: set[str] = set()
            for decision in ordered:
                if decision.symbol not in playbook_symbols or decision.symbol in seen_symbols:
                    continue
                if decision.confidence < max(55, min_confidence - 8):
                    continue
                selection = selection_map.get(decision.symbol)
                snapshot = state.snapshot_map.get(decision.symbol)
                if not self._decision_allowed_in_focus_output(
                    state,
                    decision,
                    phase_label=phase_label,
                ):
                    continue
                filtered.append(decision)
                seen_symbols.add(decision.symbol)
            return tuple(filtered)
        fallback_filtered: list[AuctionLadderDecision] = []
        for decision in ordered:
            if decision.confidence < min_confidence:
                continue
            selection = selection_map.get(decision.symbol)
            snapshot = state.snapshot_map.get(decision.symbol)
            if (
                selection is not None
                and not selection.theme_tradable
                and not self._is_soft_focus_exception(
                    state,
                    decision,
                    selection=selection,
                    snapshot=snapshot,
                    phase_label=phase_label,
                )
            ):
                continue
            fallback_filtered.append(decision)
        return tuple(fallback_filtered)

    def _is_soft_focus_exception(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        selection: StockSelectionContext | None,
        snapshot: StockStateSnapshot | None,
        phase_label: str,
    ) -> bool:
        if selection is None or snapshot is None:
            return False
        if phase_label not in {"auction", "auction_preview", "opening", "open_confirm", "intraday", "postmarket"}:
            return False
        if self._is_stock_auction_fakeout(snapshot, selection, phase_label=phase_label):
            return False
        if self._is_high_dayk_weak_leader_trap(snapshot, selection, phase_label=phase_label):
            return False
        if selection.open_follow_state == "faded":
            return False
        strong_non_hot_signal = self._selection_has_non_hot_strength(selection, snapshot)
        if decision.action == "hold_only":
            if selection.is_true_leader:
                return True
            return bool(
                selection.is_front_row
                and (
                    strong_non_hot_signal
                    or selection.total_score >= 7.4
                    or (
                        selection.execution_quality_score >= 6.2
                        and selection.open_undertake_score >= 5.8
                    )
                )
            )
        if decision.action != "observe_only" and decision.setup_id not in {"theme_not_tradable_watch", "theme_not_tradable_guard"}:
            return False
        if selection.theme_tradable:
            return False
        if selection.is_true_leader:
            return True
        return bool(
            selection.is_front_row
            and (
                strong_non_hot_signal
                or selection.theme_core_score >= 7.0
                or selection.execution_quality_score >= 6.0
                or selection.open_undertake_score >= 5.8
                or selection.total_score >= 7.4
                or selection.activity_score >= 6.8
            )
        )

    def _focus_fallback_candidates(
        self,
        state: StrategyConsoleState,
        ranked: tuple[AuctionLadderDecision, ...],
        *,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        fallback: list[AuctionLadderDecision] = []
        seen_symbols: set[str] = set()
        selection_map = self._stock_selection_context_map(state)
        preferred_plates = self._phase_priority_plates(state, phase_label=phase_label)

        def collect(*, require_priority_plate: bool) -> None:
            for decision in ranked:
                if decision.symbol in seen_symbols:
                    continue
                if require_priority_plate and preferred_plates:
                    if not self._decision_hits_priority_plate(state, decision, preferred_plates=preferred_plates):
                        continue
                if not self._decision_allowed_in_focus_output(state, decision, phase_label=phase_label):
                    continue
                snapshot = state.snapshot_map.get(decision.symbol)
                selection = selection_map.get(decision.symbol)
                if snapshot is None or selection is None:
                    continue
                judge = None
                for plate_name in self._normalized_plate_names(snapshot):
                    judge = self._theme_judge_for_plate(state, plate_name)
                    if judge is not None:
                        break
                if not self._selection_is_focus_fallback_candidate(
                    state,
                    snapshot=snapshot,
                    selection=selection,
                    judge=judge,
                ):
                    continue
                fallback.append(decision)
                seen_symbols.add(decision.symbol)
                if len(fallback) >= self.FOCUS_FALLBACK_LIMIT:
                    break

        collect(require_priority_plate=True)
        if len(fallback) < self.FOCUS_FALLBACK_LIMIT:
            collect(require_priority_plate=False)
        return tuple(fallback)

    def _can_surface_watch_only_decision(
        self,
        state: StrategyConsoleState,
        *,
        decision: AuctionLadderDecision,
        snapshot: StockStateSnapshot,
        selection: StockSelectionContext,
        phase_label: str,
    ) -> bool:
        if phase_label not in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}:
            return False
        if self._is_stock_auction_fakeout(snapshot, selection, phase_label=phase_label):
            return False
        if self._is_high_dayk_weak_leader_trap(snapshot, selection, phase_label=phase_label):
            return False
        judge, _matched_plate = self._matched_theme_judge(state, snapshot)
        opening_validation = self._opening_validation_for_display(
            state,
            snapshot=snapshot,
            selection=selection,
        )
        opening_confirmed = bool(
            opening_validation is not None
            and str(getattr(opening_validation, "validation_state", "") or "") == "confirmed"
            and str(getattr(opening_validation, "tradable_level", "") or "") in {"attack", "probe"}
        )
        if judge is not None and judge.action_class == "trap_avoid" and not opening_confirmed:
            return False
        if selection.is_true_leader:
            return True
        if not selection.is_front_row:
            return False
        return bool(
            self._selection_has_non_hot_strength(selection, snapshot)
            or selection.theme_core_score >= 7.0
            or selection.execution_quality_score >= 6.0
            or selection.open_undertake_score >= 5.8
        )

    def _focus_ordered_decisions(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        if state.bundle is None:
            return ()
        decisions = tuple(state.bundle.decisions)
        if phase_label not in {"auction", "auction_preview", "opening", "open_confirm", "intraday", "postmarket"}:
            return decisions
        preferred_plates = self._phase_priority_plates(state, phase_label=phase_label)
        if not preferred_plates:
            return decisions
        matched: list[AuctionLadderDecision] = []
        remainder: list[AuctionLadderDecision] = []
        for decision in decisions:
            if self._decision_hits_priority_plate(state, decision, preferred_plates=preferred_plates):
                matched.append(decision)
            else:
                remainder.append(decision)
        if not matched:
            return decisions
        if self._expectation_ready(state) and phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}:
            preserved = [
                decision
                for decision in remainder
                if decision.action in {"avoid_after_failed_promotion", "do_not_chase", "hold_only"}
            ]
            return tuple(matched + preserved)
        return tuple(matched + remainder)

    def _apply_theme_execution_quota(
        self,
        state: StrategyConsoleState,
        decisions: tuple[AuctionLadderDecision, ...],
        *,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        if not decisions:
            return ()
        selection_map = self._stock_selection_context_map(state)
        _mode_name, _allowed_actions, _mode_allowed_tiers, mode_theme_cap = self._money_mode_profile(state)
        accepted: list[AuctionLadderDecision] = []
        plate_counts: dict[str, int] = defaultdict(int)
        for decision in decisions:
            snapshot = state.snapshot_map.get(decision.symbol)
            selection = selection_map.get(decision.symbol)
            matched_judge = None
            matched_plate = ""
            if snapshot is not None:
                for plate_name in self._normalized_plate_names(snapshot):
                    matched_judge = self._theme_judge_for_plate(state, plate_name)
                    if matched_judge is not None:
                        matched_plate = normalize_plate_name(plate_name)
                        break
            if matched_judge is None:
                accepted.append(decision)
                continue
            allowed_count, allowed_tiers = self._theme_quota_for_action_class(matched_judge.action_class)
            if mode_theme_cap > 0:
                allowed_count = min(allowed_count, mode_theme_cap) if allowed_count > 0 else 0
            if allowed_count <= 0 and decision.action != "hold_only":
                continue
            execution_state = self._external_validation_state(matched_judge.validation_state)
            if (
                phase_label in {"auction", "auction_preview", "opening", "open_confirm", "intraday"}
                and execution_state == "falsified"
                and decision.action != "hold_only"
            ):
                continue
            if decision.action != "hold_only" and matched_plate:
                if plate_counts[matched_plate] >= allowed_count:
                    continue
                plate_counts[matched_plate] += 1
            accepted.append(decision)
        return tuple(accepted)

    def _log_focus_candidate_breakdown(
        self,
        state: StrategyConsoleState,
        decisions: tuple[AuctionLadderDecision, ...],
        *,
        phase_label: str,
        stage: str,
        limit: int = 5,
    ) -> None:
        if not decisions:
            return
        parts: list[str] = []
        for decision in decisions[:limit]:
            breakdown = self._focus_candidate_story_breakdown(state, decision, phase_label=phase_label)
            parts.append(
                f"{decision.symbol}:{breakdown['total']:.1f}[{self._story_score_driver_text(breakdown)}]"
            )
        logger.info(
            "focus score audit | phase=%s | stage=%s | picks=%s",
            phase_label,
            stage,
            " ; ".join(parts),
        )

    def _focus_candidate_priority_score(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> float:
        return self._focus_candidate_score_breakdown(
            state,
            decision,
            phase_label=phase_label,
        )["total"]

    def _focus_candidate_passes_gate(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> bool:
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        if selection is None:
            return True
        snapshot = state.snapshot_map.get(decision.symbol)
        judge, matched_plate = self._matched_theme_judge(state, snapshot)
        if phase_label in {"auction", "opening", "open_confirm", "intraday"}:
            strong_non_hot_signal = self._selection_has_non_hot_strength(selection, snapshot)
            preferred_plates = self._phase_priority_plates(state, phase_label=phase_label)
            repair_probe_exception = (
                decision.setup_id == "theme_not_tradable_repair_probe"
                and selection.open_follow_state in {"confirmed", "repair_strength"}
            )
            priority_front_row_exception = (
                selection.is_front_row
                and strong_non_hot_signal
                and phase_label in {"auction", "opening", "open_confirm"}
            )
            if self._is_high_dayk_weak_leader_trap(snapshot, selection, phase_label=phase_label):
                return False
            if self._is_stock_auction_fakeout(snapshot, selection, phase_label=phase_label):
                return False
            if (
                phase_label in {"opening", "open_confirm"}
                and selection.open_follow_state == "weak_follow"
            ):
                return False
            if (
                phase_label in {"open_confirm", "intraday"}
                and preferred_plates
                and not self._decision_hits_priority_plate(state, decision, preferred_plates=preferred_plates)
                and not selection.is_true_leader
                and not strong_non_hot_signal
            ):
                if not repair_probe_exception and not priority_front_row_exception:
                    return False
            if judge is not None:
                tier = self._selection_theme_tier(selection, snapshot)
                mode_name, _mode_actions, _mode_allowed_tiers, _mode_theme_cap = self._money_mode_profile(state)
                allowed_count, allowed_tiers = self._theme_quota_for_action_class(judge.action_class)
                if decision.action != "hold_only" and allowed_count <= 0:
                    return False
                if decision.action != "hold_only" and allowed_tiers and tier not in allowed_tiers:
                    if not repair_probe_exception:
                        return False
                if judge.action_class in {"observe", "trap_avoid"} and not selection.is_true_leader:
                    if not repair_probe_exception:
                        return False
                if judge.action_class == "anchor_only" and not selection.is_true_leader:
                    if not repair_probe_exception:
                        return False
                if (
                    judge.validation_state == "falsified"
                    and (decision.action != "hold_only" or not selection.is_true_leader)
                ):
                    return False
                if (
                    judge.action_class in {"observe", "trap_avoid"}
                    and decision.action == "hold_only"
                    and (
                        not selection.is_true_leader
                        or selection.open_follow_state in {"weak_follow", "faded"}
                        or selection.theme_x_score >= 5.6
                    )
                ):
                    return False
                if (
                    phase_label in {"auction", "opening", "open_confirm"}
                    and matched_plate
                    and matched_plate in self._execution_theme_candidates(state)
                    and judge.action_class in {"main_attack", "front_row_confirm"}
                    and selection.is_front_row
                    and strong_non_hot_signal
                ):
                    return True
                if (
                    mode_name == "leader_only"
                    and not selection.is_true_leader
                    and selection.open_follow_state != "confirmed"
                    and not repair_probe_exception
                ):
                    return False
            if (
                not selection.is_true_leader
                and not selection.is_active_pool
                and selection.theme_core_score < 7.2
                and not strong_non_hot_signal
            ):
                if not repair_probe_exception and not priority_front_row_exception:
                    return False
            if (
                not selection.is_true_leader
                and selection.kline_score < 5.2
                and selection.structure_score < 5.4
                and not strong_non_hot_signal
            ):
                if not repair_probe_exception and not priority_front_row_exception:
                    return False
            if (
                not selection.is_true_leader
                and selection.shape_quality_score < 5.8
                and selection.execution_quality_score < 5.6
                and not strong_non_hot_signal
            ):
                if not repair_probe_exception and not priority_front_row_exception:
                    return False
            if (
                not selection.is_true_leader
                and selection.open_undertake_score < 4.8
                and selection.execution_quality_score < 5.8
                and not strong_non_hot_signal
            ):
                if not repair_probe_exception and not priority_front_row_exception:
                    return False
            if (
                        decision.action == "hold_only"
                and selection.auction_open_bucket == "near_limit_open"
                and selection.open_follow_state != "confirmed"
                and not selection.is_true_leader
            ):
                return False
            if (
                        decision.action == "hold_only"
                and selection.auction_open_bucket == "overheat_high_open"
                and selection.open_follow_state == "weak_follow"
                and selection.open_undertake_score < 5.8
                and not selection.is_true_leader
            ):
                return False
            if (
                not selection.is_true_leader
                and selection.theme_x_score >= 5.6
                and selection.activity_score < 7.0
            ):
                return False
            if (
                not selection.is_true_leader
                and selection.hot_rank > 120
                and selection.turnover_quality_score < 5.0
                and selection.shape_quality_score < 6.2
                and not strong_non_hot_signal
            ):
                return False
            if (
                snapshot is not None
                and snapshot.lb_days >= 1
                and not selection.is_true_leader
                and selection.hot_rank > 100
                and selection.heat_flow_score < 5.0
                and selection.open_undertake_score < 5.6
                and not strong_non_hot_signal
            ):
                return False
            if (
                snapshot is not None
                and snapshot.lb_days >= 1
                and not selection.is_true_leader
                and snapshot.leader_rank_in_theme > 3
                and snapshot.auction_amount < 20_000_000
                and snapshot.amount_2m < 25_000_000
                and selection.execution_quality_score < 6.0
            ):
                return False
            if phase_label in {"opening", "open_confirm"} and snapshot is not None:
                confirm_label = self._leader_truth_label(snapshot)
                if (
                    confirm_label in {self.OPENING_VALIDATION_GAP_WEAK, self.OPENING_VALIDATION_UNDERTAKE_WEAK}
                    and (
                        decision.action == "hold_only"
                        or not selection.is_true_leader
                        or not strong_non_hot_signal
                    )
                ):
                    return False
            if decision.action in {"dragon_early_board", "early_boarding_candidate"} and selection.timing_score < 4.6:
                return False
        return True

    def _backfill_candidates_from_confirmed_themes(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
        existing_symbols: set[str],
    ) -> tuple[AuctionLadderDecision, ...]:
        if phase_label not in {"opening", "open_confirm", "intraday", "postmarket"}:
            return ()
        confirmed_plates = self._confirmed_theme_names_for_focus(state)
        if not confirmed_plates:
            return ()
        preferred_plates = self._phase_priority_plates(state, phase_label=phase_label)
        effective_confirmed_plates = confirmed_plates
        if preferred_plates:
            overlapped_plates = tuple(plate for plate in confirmed_plates if plate in preferred_plates)
            if overlapped_plates:
                effective_confirmed_plates = overlapped_plates

        selection_map = self._stock_selection_context_map(state)
        ranked_candidates: list[tuple[float, AuctionLadderDecision]] = []
        seen_symbols = set(existing_symbols)
        source: list[AuctionLadderDecision] = []
        playbook_watch_candidates = tuple(
            item
            for item in tuple(state.playbook_decisions or ())
            if item.action == "observe_only"
        )
        ordered_watch_candidates = self._order_decisions_by_narrative(
            state,
            tuple(
                item
                for item in playbook_watch_candidates
                if item.symbol not in seen_symbols
            ),
            phase_label=phase_label,
        )
        for decision in ordered_watch_candidates:
            source.append(decision)
        for decision in self._focus_ordered_decisions(state, phase_label=phase_label):
            if decision.symbol in seen_symbols:
                continue
            if any(item.symbol == decision.symbol for item in source):
                continue
            source.append(decision)

        for decision in source:
            snapshot = state.snapshot_map.get(decision.symbol)
            selection = selection_map.get(decision.symbol)
            if snapshot is None or selection is None:
                continue
            judge, matched_plate = self._matched_theme_judge(state, snapshot)
            if judge is None:
                continue
            plate_name = normalize_plate_name(matched_plate or judge.plate_name)
            if plate_name not in effective_confirmed_plates:
                continue
            execution_state = self._external_validation_state(judge.validation_state)
            if execution_state == "falsified":
                continue
            if self._is_stock_auction_fakeout(snapshot, selection, phase_label=phase_label):
                continue
            if self._is_high_dayk_weak_leader_trap(snapshot, selection, phase_label=phase_label):
                continue
            if judge.action_class == "anchor_only" and not selection.is_true_leader:
                continue
            strong_non_hot_signal = self._selection_has_non_hot_strength(selection, snapshot)
            if (
                not selection.is_true_leader
                and not selection.is_front_row
                and not strong_non_hot_signal
            ):
                continue
            if (
                selection.open_follow_state in {"weak_follow", "faded"}
                and not selection.is_true_leader
                and not strong_non_hot_signal
            ):
                continue
            display_action = self._display_action_code(decision, state, phase_label=phase_label)
            if display_action in {"failed_promo_guard", "do_not_chase"}:
                continue
            if (
                display_action == "observe_only"
                and not selection.is_true_leader
                and not (
                    selection.is_front_row
                    and selection.open_follow_state in {"confirmed", "repair_strength"}
                )
                and not strong_non_hot_signal
            ):
                continue
            score = self._focus_candidate_priority_score(
                state,
                decision,
                phase_label=phase_label,
            )
            if selection.open_follow_state == "confirmed":
                score += 1.2
            elif selection.open_follow_state == "repair_strength":
                score += 0.8
            if self._decision_hits_priority_plate(
                state,
                decision,
                preferred_plates=effective_confirmed_plates,
            ):
                score += 0.6
            if selection.is_true_leader:
                score += 0.5
            elif selection.is_front_row:
                score += 0.3
            ranked_candidates.append((score, decision))
            seen_symbols.add(decision.symbol)

        ranked_candidates.sort(key=lambda item: item[0], reverse=True)
        return tuple(decision for _score, decision in ranked_candidates[:3])

    def _selection_is_focus_fallback_candidate(
        self,
        state: StrategyConsoleState,
        *,
        snapshot: StockStateSnapshot,
        selection: StockSelectionContext,
        judge: ThemeJudgeResult | None,
    ) -> bool:
        strong_non_hot_signal = self._selection_has_non_hot_strength(selection, snapshot)
        if judge is not None and judge.action_class == "trap_avoid":
            return False
        if (
            judge is not None
            and judge.validation_state == "falsified"
            and not selection.is_true_leader
            and not strong_non_hot_signal
        ):
            return False
        if (
            not selection.is_true_leader
            and not selection.is_front_row
            and not strong_non_hot_signal
        ):
            return False
        if (
            selection.execution_quality_score < 5.0
            and selection.open_undertake_score < 5.0
            and not strong_non_hot_signal
        ):
            return False
        return True

    def _decision_hits_priority_plate(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        preferred_plates: tuple[str, ...],
    ) -> bool:
        snapshot = state.snapshot_map.get(decision.symbol)
        if snapshot is None:
            return False
        names = (
            state.normalized_plate_names_map.get(snapshot.symbol, ())
            if state.normalized_plate_names_map is not None
            else self._normalized_plate_names(snapshot)
        )
        return any(name in preferred_plates for name in names)

    def _theme_quota_for_action_class(self, action_class: str) -> tuple[int, frozenset[str]]:
        mapping = {
            "main_attack": (3, frozenset({"dragon", "front_core", "front_follow"})),
            "front_row_confirm": (2, frozenset({"dragon", "front_core", "front_follow"})),
            "anchor_only": (1, frozenset({"dragon"})),
            "observe": (1, frozenset({"dragon"})),
            "trap_avoid": (0, frozenset()),
        }
        return mapping.get(action_class, (1, frozenset({"dragon", "front_core"})))

    def _focus_candidate_score_breakdown(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> dict[str, float]:
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        snapshot = state.snapshot_map.get(decision.symbol)
        judge, matched_plate = self._matched_theme_judge(state, snapshot)
        collision = self._snapshot_theme_collision(state, snapshot)
        base = float(decision.confidence)
        selection_score = 0.0
        if selection is not None:
            selection_score = self._focus_score_from_selection(
                state,
                selection=selection,
                snapshot=snapshot,
                collision=collision,
                matched_plate=matched_plate,
                phase_label=phase_label,
            )
        judge_score = self._focus_score_from_judge(judge)
        opening_score = self._focus_score_from_opening_validation(snapshot, phase_label=phase_label)
        action_score = 0.0
        if decision.action == "hold_only":
            action_score = 2.0 if phase_label in {"auction", "opening", "open_confirm"} else -2.0
        elif decision.action == "small_probe_only":
            action_score = -3.0
        total = round(base + selection_score + judge_score + opening_score + action_score, 3)
        return {
            "base": round(base, 3),
            "selection": round(selection_score, 3),
            "judge": round(judge_score, 3),
            "opening": round(opening_score, 3),
            "action": round(action_score, 3),
            "total": total,
        }

    def _focus_candidate_story_breakdown(
        self,
        state: StrategyConsoleState,
        decision: AuctionLadderDecision,
        *,
        phase_label: str,
    ) -> dict[str, float]:
        selection = self._stock_selection_context_map(state).get(decision.symbol)
        snapshot = state.snapshot_map.get(decision.symbol)
        judge, matched_plate = self._matched_theme_judge(state, snapshot)
        collision = self._snapshot_theme_collision(state, snapshot)
        theme_context = self._theme_selection_for_symbol(state, decision.symbol)

        theme_score = 0.0
        role_score = 0.0
        undertake_score = 0.0
        flow_score = 0.0
        risk_score = 0.0
        action_score = 0.0

        if judge is not None:
            theme_score += float(judge.opportunity_score) * 1.8
            theme_score -= float(judge.trap_score) * 1.2
            if judge.action_class == "main_attack":
                theme_score += 6.0
            elif judge.action_class == "front_row_confirm":
                theme_score += 4.0
            elif judge.action_class == "anchor_only":
                theme_score += 1.0
            elif judge.action_class == "observe":
                theme_score -= 3.0
            elif judge.action_class == "trap_avoid":
                risk_score -= 10.0
            if judge.validation_state == "strengthened":
                theme_score += 4.0
            elif judge.validation_state == "falsified":
                risk_score -= 9.0

        if collision is not None:
            if collision.expectation_label in {"绗﹀悎/寮哄寲", "局部转强"}:
                theme_score += 2.0
            if collision.fakeout_level == "strong":
                risk_score -= 8.0
            elif collision.fakeout_level == "warn":
                risk_score -= 4.0

        if theme_context is not None:
            theme_score += float(getattr(theme_context, "phase_priority_bias", 0.0) or 0.0) * 3.0
            if not bool(getattr(theme_context, "tradable", True)):
                risk_score -= 4.0

        if matched_plate:
            preferred_plates = self._phase_priority_plates(state, phase_label=phase_label)
            if matched_plate in preferred_plates:
                theme_score += 3.0

        if selection is not None:
            if selection.is_true_leader:
                role_score += 9.0
            elif selection.is_front_row:
                role_score += 5.0
            else:
                role_score -= 4.0
            role_score += max(-3.0, min(5.0, (float(selection.theme_core_score) - 5.0) * 1.2))

            undertake_score += max(-4.0, min(6.0, (float(selection.open_undertake_score) - 5.0) * 1.8))
            undertake_score += self._focus_score_from_open_follow(selection, phase_label=phase_label) * 0.6
            if snapshot is not None and snapshot.auction_amount > 0:
                amount_ratio = float(snapshot.amount_2m or 0.0) / max(float(snapshot.auction_amount or 0.0), 1.0)
                if amount_ratio >= 1.6:
                    undertake_score += 4.0
                elif amount_ratio >= 1.2:
                    undertake_score += 2.5
                elif amount_ratio <= 0.7:
                    undertake_score -= 2.0

            flow_score += max(-4.0, min(5.0, (float(selection.activity_score) - 5.0) * 1.4))
            flow_score += max(-3.0, min(4.0, (float(selection.turnover_quality_score) - 5.0) * 1.2))
            flow_score += max(-2.0, min(3.0, (float(selection.heat_flow_score) - 5.0) * 1.0))
            if selection.hot_rank <= 20:
                flow_score += 2.0
            elif selection.hot_rank > 100:
                flow_score -= 2.0

            if not selection.theme_tradable:
                if selection.is_true_leader:
                    risk_score -= 1.0
                elif selection.is_front_row:
                    risk_score -= 3.0
                else:
                    risk_score -= 6.0
            if selection.theme_x_score >= 6.0:
                risk_score -= 5.0
            elif selection.theme_x_score >= 4.5:
                risk_score -= 2.5
            if selection.kline_pattern in {"high_open_then_weak", "volume_up_price_flat", "explosive_failed_board"}:
                risk_score -= 6.0
            elif selection.kline_pattern == "high_divergence":
                risk_score -= 3.0
            elif selection.kline_pattern in {"platform_breakout", "breakout", "pullback_repair", "low_open_strength", "n_rebound"}:
                role_score += 2.0
            if selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"} and not selection.is_true_leader:
                risk_score -= 3.0

        if snapshot is not None:
            opening_score = self._focus_score_from_opening_validation(snapshot, phase_label=phase_label)
            undertake_score += opening_score * 0.5
            if snapshot.amount_2m >= 50_000_000:
                flow_score += 2.0

        if decision.action == "hold_only":
            action_score += 1.5
        elif decision.action == "small_probe_only":
            action_score -= 2.0
        elif decision.action in {"avoid_after_failed_promotion", "do_not_chase"}:
            action_score -= 6.0

        total = round(
            float(decision.confidence)
            + theme_score
            + role_score
            + undertake_score
            + flow_score
            + risk_score
            + action_score,
            3,
        )
        return {
            "theme": round(theme_score, 3),
            "role": round(role_score, 3),
            "undertake": round(undertake_score, 3),
            "flow": round(flow_score, 3),
            "risk": round(risk_score, 3),
            "action": round(action_score, 3),
            "total": total,
        }

    @staticmethod
    def _story_score_driver_text(breakdown: dict[str, float], *, top_n: int = 3) -> str:
        items = [
            (name, float(value or 0.0))
            for name, value in breakdown.items()
            if name != "total" and abs(float(value or 0.0)) > 0.0
        ]
        if not items:
            return "flat"
        ranked = sorted(items, key=lambda item: (abs(item[1]), item[1]), reverse=True)
        return ",".join(f"{name}={value:+.1f}" for name, value in ranked[:top_n])

    def _confirmed_theme_names_for_focus(
        self,
        state: StrategyConsoleState,
    ) -> tuple[str, ...]:
        opening_bundle = getattr(state.context, "opening_validation_bundle", None)
        if opening_bundle is not None:
            ordered_bundle_items = sorted(
                tuple((getattr(opening_bundle, "confirmed_themes", {}) or {}).values()),
                key=lambda item: (
                    str(getattr(item, "tradable_level", "") or "") == "attack",
                    -float(getattr(item, "amount_2m_rank_pct", 1.0) or 1.0),
                    bool(getattr(item, "front_row_confirmed", False)),
                    bool(getattr(item, "mid_follow_confirmed", False)),
                ),
                reverse=True,
            )
            ordered_names: list[str] = []
            for item in ordered_bundle_items:
                name = normalize_plate_name(str(getattr(item, "plate_name", "") or ""))
                if not name or name == "-" or name in ordered_names:
                    continue
                ordered_names.append(name)
            if ordered_names:
                return tuple(ordered_names)
        if not state.theme_judge_map:
            return ()
        ordered: list[str] = []
        for judge in sorted(
            state.theme_judge_map.values(),
            key=lambda item: (
                self._theme_action_priority(item.action_class),
                item.opportunity_score,
                -item.trap_score,
            ),
            reverse=True,
        ):
            if self._external_validation_state(judge.validation_state) != "confirmed":
                continue
            if judge.action_class not in {"main_attack", "front_row_confirm", "anchor_only"}:
                continue
            if judge.trap_score >= 7.0:
                continue
            name = normalize_plate_name(judge.plate_name)
            if not name or name == "-" or name in ordered:
                continue
            ordered.append(name)
        return tuple(ordered)

    def _focus_score_from_selection(
        self,
        state: StrategyConsoleState,
        *,
        selection: StockSelectionContext,
        snapshot: StockStateSnapshot | None,
        collision: AuctionThemeCollisionStat | None,
        matched_plate: str,
        phase_label: str,
    ) -> float:
        score = 0.0
        tier = self._selection_theme_tier(selection, snapshot)
        front_state = self._front_row_strength_state(state, phase_label=phase_label)
        mode_name, _mode_actions, mode_allowed_tiers, _mode_theme_cap = self._money_mode_profile(state)
        strong_non_hot_signal = self._selection_has_non_hot_strength(selection, snapshot)

        score += self._theme_tier_priority(tier) * 3.5
        score += float(selection.total_score) * 3.2
        score += float(selection.shape_quality_score) * 2.0
        score += float(selection.execution_quality_score) * 1.8
        score += float(selection.open_undertake_score) * 1.8
        score += float(selection.turnover_quality_score) * 1.2
        score += min(float(selection.heat_flow_score), 8.0) * 0.8
        score += float(selection.theme_core_score) * 2.4
        score += float(selection.activity_score) * 1.8
        score += float(selection.kline_score) * 1.6
        score += float(selection.structure_score) * 1.4
        score += float(selection.auction_score) * (1.2 if phase_label in {"auction", "opening", "open_confirm"} else 0.4)
        score += float(selection.timing_score) * (1.0 if phase_label in {"intraday", "opening", "open_confirm"} else 0.5)
        score += self._focus_score_from_heat_profile(selection, strong_non_hot_signal=strong_non_hot_signal)
        score += self._focus_score_from_leader_tier(selection, tier=tier)

        if mode_allowed_tiers and tier not in mode_allowed_tiers:
            score -= 16.0
        elif mode_name == "front_rotation" and tier in {"dragon", "front_core"}:
            score += 4.0
        elif mode_name == "repair" and selection.kline_pattern in {"low_open_strength", "pullback_repair", "n_rebound"}:
            score += 5.0

        if selection.is_active_pool:
            score += 3.5
        elif strong_non_hot_signal:
            score += 3.0
        else:
            score -= 6.0
        if not selection.theme_tradable:
            if selection.is_true_leader:
                score -= 2.0
            elif selection.is_front_row and strong_non_hot_signal:
                score -= 4.0
            elif selection.is_front_row and (
                selection.execution_quality_score >= 6.0
                or selection.open_undertake_score >= 5.8
                or selection.total_score >= 7.4
            ):
                score -= 6.0
            else:
                score -= 14.0
        score += self._focus_score_from_market_state(
            selection,
            front_state=front_state,
            strong_non_hot_signal=strong_non_hot_signal,
        )

        if selection.kline_pattern in {"high_open_then_weak", "volume_up_price_flat", "explosive_failed_board"}:
            score -= 18.0
        elif selection.kline_pattern == "high_divergence":
            score -= 8.0
        elif selection.kline_pattern in {"platform_breakout", "low_open_strength", "n_rebound", "breakout", "pullback_repair"}:
            score += 6.0
        score += self._focus_score_from_open_follow(selection, phase_label=phase_label)

        if selection.kline_pattern in {"platform_breakout", "breakout"}:
            if selection.auction_open_bucket == "flat_open":
                score += 2.5
            elif selection.auction_open_bucket == "healthy_high_open":
                score += 1.0
            elif selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
                score -= 8.0 if not selection.is_true_leader else 3.0

        if selection.kline_pattern in {"pullback_repair", "low_open_strength", "n_rebound"}:
            if selection.auction_open_bucket in {"deep_low_open", "low_open", "flat_open"}:
                score += 3.0
            elif selection.auction_open_bucket == "near_limit_open":
                score -= 6.0

        if selection.execution_quality_score < 5.0:
            score -= 8.0
        if selection.open_undertake_score < 5.0:
            score -= 10.0
        if selection.shape_quality_score < 5.4:
            score -= 8.0

        if (
            snapshot is not None
            and snapshot.lb_days >= 1
            and not selection.is_true_leader
            and selection.hot_rank > 60
            and selection.heat_flow_score < 5.0
            and selection.open_undertake_score < 5.6
            and not strong_non_hot_signal
        ):
            score -= 18.0

        if strong_non_hot_signal and selection.is_front_row:
            score += 4.0

        if collision is not None:
            if collision.fakeout_level == "strong":
                score -= 18.0
            elif collision.fakeout_level == "warn":
                score -= 8.0
            if front_state in {"very_weak", "weak"} and collision.expectation_label in {"局部超预期", "超预期"}:
                score += 3.0
            elif front_state == "strong" and collision.expectation_label in {"符合预期", "有预期差"} and collision.row.limit_up_count <= 1:
                score -= 3.0

        if (
            snapshot is not None
            and snapshot.lb_days >= 1
            and not selection.is_true_leader
            and snapshot.leader_rank_in_theme > 3
            and snapshot.auction_amount < 20_000_000
            and snapshot.amount_2m < 25_000_000
            and selection.execution_quality_score < 6.0
        ):
            score -= 14.0
        score += self._focus_score_from_theme_risk(selection)

        if phase_label in {"auction", "auction_preview", "opening", "open_confirm"}:
            execution_themes = self._execution_theme_candidates(state)
            if matched_plate and matched_plate in execution_themes:
                score += 6.0
                if selection.is_true_leader:
                    score += 4.0
                elif selection.is_front_row or tier in {"front_core", "front_follow"}:
                    score += 2.5
            elif execution_themes:
                score -= 3.0
        return score

    @staticmethod
    def _focus_score_from_open_follow(
        selection: StockSelectionContext,
        *,
        phase_label: str,
    ) -> float:
        if selection.open_follow_state == "confirmed":
            return 6.0 if phase_label in {"opening", "open_confirm", "intraday"} else 2.0
        if selection.open_follow_state == "repair_strength":
            return 8.0 if phase_label in {"opening", "open_confirm", "intraday"} else 3.0
        if selection.open_follow_state == "weak_follow":
            return -4.0
        if selection.open_follow_state == "faded":
            return -16.0
        return 0.0

    @staticmethod
    def _focus_score_from_judge(judge: ThemeJudgeResult | None) -> float:
        if judge is None:
            return 0.0
        score = float(judge.opportunity_score) * 3.0
        score -= float(judge.trap_score) * 2.6
        if judge.action_class == "main_attack":
            score += 10.0
        elif judge.action_class == "front_row_confirm":
            score += 6.0
        elif judge.action_class == "anchor_only":
            score += 1.0
        elif judge.action_class == "observe":
            score -= 5.0
        elif judge.action_class == "trap_avoid":
            score -= 18.0
        if judge.validation_state == "strengthened":
            score += 5.0
        elif judge.validation_state == "falsified":
            score -= 12.0
        return score

    def _focus_score_from_opening_validation(
        self,
        snapshot: StockStateSnapshot | None,
        *,
        phase_label: str,
    ) -> float:
        if phase_label not in {"opening", "open_confirm"} or snapshot is None:
            return 0.0
        confirm_label = self._leader_truth_label(snapshot)
        if confirm_label == self.OPENING_VALIDATION_TRUE_STRONG:
            return 14.0
        if confirm_label in {self.OPENING_VALIDATION_LOW_OPEN_STRONG, self.OPENING_VALIDATION_PULLBACK_REBOUND}:
            return 10.0
        if confirm_label in {self.OPENING_VALIDATION_GAP_WEAK, self.OPENING_VALIDATION_UNDERTAKE_WEAK}:
            return -18.0
        if confirm_label == self.OPENING_VALIDATION_HARD_TO_CHASE:
            return -8.0
        return 0.0

    @staticmethod
    def _focus_score_from_heat_profile(
        selection: StockSelectionContext,
        *,
        strong_non_hot_signal: bool,
    ) -> float:
        score = 0.0
        if selection.hot_rank <= 20:
            score += 4.0
        elif selection.hot_rank <= 50:
            score += 2.0
        elif selection.hot_rank > 80 and strong_non_hot_signal:
            score += 3.0
        elif selection.hot_rank > 100 and not strong_non_hot_signal:
            score -= 4.0

        if selection.heat_flow_score >= 5.8:
            score += 2.5
        elif selection.heat_flow_score < 4.5:
            score -= 3.5
        return score

    @staticmethod
    def _focus_score_from_leader_tier(
        selection: StockSelectionContext,
        *,
        tier: str,
    ) -> float:
        score = 0.0
        if selection.is_true_leader:
            score += 12.0
        elif selection.is_front_row:
            score += 5.0
        else:
            score -= 6.0

        if tier == "back_noise":
            score -= 12.0
        elif tier == "front_follow":
            score -= 2.0
        return score

    @staticmethod
    def _focus_score_from_market_state(
        selection: StockSelectionContext,
        *,
        front_state: str,
        strong_non_hot_signal: bool,
    ) -> float:
        score = 0.0
        if front_state in {"very_weak", "weak"}:
            if strong_non_hot_signal:
                score += 4.5
            if selection.is_front_row and selection.auction_open_bucket in {"flat_open", "low_open", "deep_low_open"}:
                score += 2.5
            if selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"} and not selection.is_true_leader:
                score -= 6.0
        elif front_state == "strong":
            if selection.is_true_leader:
                score += 2.0
            if selection.auction_open_bucket == "healthy_high_open" and selection.open_follow_state == "confirmed":
                score += 1.5
        return score

    @staticmethod
    def _focus_score_from_theme_risk(selection: StockSelectionContext) -> float:
        if selection.theme_x_score >= 6.0:
            return -6.0
        if selection.theme_x_score >= 4.5:
            return -3.0
        return 0.0

    def _theme_opening_validation_state(
        self,
        state: StrategyConsoleState,
        item: AuctionThemeCollisionStat,
    ) -> tuple[str, dict[str, float]]:
        front_comparison = self._market_slice_comparison_for_phase(state, phase_label="open_confirm")
        two_min_ratio_floor = 0.65 if front_comparison.is_weak else (0.78 if front_comparison.is_strong else 0.70)
        five_min_ratio_floor = 0.85 if front_comparison.is_weak else (0.98 if front_comparison.is_strong else 0.90)
        weak_ratio_cut = 0.70 if front_comparison.is_weak else (0.82 if front_comparison.is_strong else 0.75)
        front_row: list[StockStateSnapshot] = []
        for snapshot in state.snapshot_map.values():
            if item.plate_name not in self._normalized_plate_names(snapshot):
                continue
            if snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1:
                front_row.append(snapshot)
        if not front_row:
            return self._theme_open_confirm_state(item), {"front_row_count": 0.0, "undertake_count": 0.0, "undertake_ratio": 0.0}
        undertake_count = 0
        undertake_count_5m = 0
        undertake_count_10m_proxy = 0
        weak_count = 0
        high_open_fail_count = 0
        low_open_repair_count = 0
        expansion_count = 0
        for snapshot in front_row:
            auction_amount = float(snapshot.auction_amount or 0.0)
            amount_2m = float(snapshot.amount_2m or 0.0)
            amount_5m = float(snapshot.amount_5m or 0.0)
            ratio = (amount_2m / auction_amount) if auction_amount > 0 else 0.0
            ratio_5m = (amount_5m / auction_amount) if auction_amount > 0 else 0.0
            if (
                (
                    amount_2m >= max(auction_amount, 20_000_000)
                    or (amount_2m >= 40_000_000 and snapshot.speed_1m > 0)
                    or (snapshot.current_pct >= 0.095 and amount_2m >= 30_000_000)
                )
                and ratio >= two_min_ratio_floor
                and snapshot.current_pct >= snapshot.open_pct - 0.015
                and snapshot.speed_1m > -0.002
            ):
                undertake_count += 1
            if (
                (
                    amount_5m >= max(auction_amount * 1.2, 30_000_000)
                    or (amount_5m >= 50_000_000 and snapshot.vector_5m > 0)
                )
                and ratio_5m >= five_min_ratio_floor
                and snapshot.current_pct >= snapshot.open_pct - 0.02
                and snapshot.vector_5m > -0.01
            ):
                undertake_count_5m += 1
            if (
                amount_5m >= max(auction_amount * 1.5, 40_000_000)
                and snapshot.current_pct >= snapshot.open_pct - 0.015
                and snapshot.vector_5m >= 0
            ):
                undertake_count_10m_proxy += 1
            if (
                (snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.03)
                or (auction_amount > 0 and amount_2m < auction_amount * weak_ratio_cut and snapshot.speed_1m <= 0)
            ):
                weak_count += 1
            if (
                amount_5m > 0
                and (
                    snapshot.current_pct <= snapshot.open_pct - 0.04
                    or snapshot.vector_5m <= -0.012
                    or (auction_amount > 0 and amount_5m < auction_amount * 0.95 and snapshot.current_pct <= snapshot.open_pct)
                )
            ):
                weak_count += 1
            if snapshot.open_pct >= 0.05 and snapshot.current_pct <= snapshot.open_pct - 0.03:
                high_open_fail_count += 1
            if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and amount_2m >= 20_000_000:
                low_open_repair_count += 1
        all_plate_snapshots = [
            snapshot
            for snapshot in state.snapshot_map.values()
            if item.plate_name in self._normalized_plate_names(snapshot)
        ]
        for snapshot in all_plate_snapshots:
            if snapshot in front_row:
                continue
            if (
                snapshot.current_pct >= 0.03
                and (float(snapshot.amount_2m or 0.0) >= 20_000_000 or float(snapshot.speed_1m or 0.0) > 0.008)
            ):
                expansion_count += 1
        undertake_ratio = undertake_count / max(len(front_row), 1)
        if (
            undertake_count >= max(1, len(front_row) // 2)
            and weak_count == 0
            and high_open_fail_count == 0
        ) or low_open_repair_count >= 1 or expansion_count >= 2:
            validation_state = "strengthened"
        elif weak_count >= max(1, len(front_row) // 2) or high_open_fail_count >= max(1, len(front_row) // 2):
            validation_state = "falsified"
        else:
            validation_state = "maintained"
        return (
            validation_state,
            {
                "front_row_count": float(len(front_row)),
                "undertake_count": float(undertake_count),
                "undertake_count_5m": float(undertake_count_5m),
                "undertake_count_10m_proxy": float(undertake_count_10m_proxy),
                "undertake_ratio": round(undertake_ratio, 3),
                "weak_count": float(weak_count),
                "high_open_fail_count": float(high_open_fail_count),
                "low_open_repair_count": float(low_open_repair_count),
                "expansion_count": float(expansion_count),
            },
        )

    def _compute_recap_feedback_metrics(self, state: StrategyConsoleState, *, phase_label: str) -> dict[str, object]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        yest_limit_map = ref["yest_limit_map"]
        assert isinstance(yest_limit_map, dict)
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        auction_map = ref["auction_map"]
        assert isinstance(auction_map, dict)
        total = len(yest_limit_map)
        matched = 0
        auction_sample_matched = 0
        truth_symbols = {
            str(row.get("symbol") or "").strip()
            for row in truth_rows
            if row.get("symbol")
        }
        promoted_count = 0
        red_open_count = 0
        headshot_count = 0
        for symbol in yest_limit_map.keys():
            snapshot = state.snapshot_map.get(symbol)
            if snapshot is None:
                continue
            matched += 1
            if symbol in truth_symbols:
                promoted_count += 1
            auction_row = auction_map.get(symbol)
            if auction_row is None:
                continue
            auction_sample_matched += 1
            open_pct = self._normalize_pct_value(auction_row.get("change_pct", snapshot.open_pct))
            if open_pct > 0:
                red_open_count += 1
            if open_pct > 0.05 and snapshot.current_pct < 0:
                headshot_count += 1
        denominator = matched or total
        promotion_rate = (promoted_count / denominator) if denominator else 0.0
        red_open_rate = (red_open_count / auction_sample_matched) if auction_sample_matched else 0.0
        headshot_rate = (headshot_count / auction_sample_matched) if auction_sample_matched else 0.0
        auction_ready = auction_sample_matched > 0
        sentiment_score = round((promotion_rate * 0.5 + red_open_rate * 0.3 + (1 - headshot_rate) * 0.2) * 10, 1) if denominator else 0.0
        battle = "bullish" if promotion_rate >= 0.35 and headshot_rate <= 0.08 else ("danger" if headshot_rate >= 0.12 or promotion_rate <= 0.15 else "neutral")
        return {
            "trade_date": ref["trade_date"],
            "previous_trade_date": ref["previous_trade_date"],
            "sample_total": total,
            "sample_matched": matched,
            "auction_ready": auction_ready,
            "auction_sample_matched": auction_sample_matched,
            "promoted_count": promoted_count,
            "promotion_rate": promotion_rate,
            "red_open_rate": red_open_rate,
            "headshot_rate": headshot_rate,
            "sentiment_score": sentiment_score,
            "battle": battle,
        }

    def _render_recap_close_recap(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        metrics = self._compute_recap_feedback_metrics(state, phase_label=phase_label)
        recap_summary = type(
            "RecapSummary",
            (),
            {
                "sentiment_score": metrics["sentiment_score"],
                "headshot_rate": metrics["headshot_rate"],
            },
        )()
        verdict = self._infer_close_verdict(recap_summary)
        auction_ready = int(metrics.get("auction_sample_matched", 0) or 0) > 0
        red_open_value = float(metrics["red_open_rate"])
        headshot_value = float(metrics["headshot_rate"])
        red_open_text = f"{red_open_value:.1%}" if auction_ready else "--"
        headshot_text = f"{headshot_value:.1%}" if auction_ready else "--"
        red_open_marker = self._red_open_marker(red_open_value) if auction_ready else "?"
        headshot_marker = self._headshot_marker(headshot_value) if auction_ready else "?"
        return (
            "【收盘定性】指标 | 数值",
            f"  {self._close_marker(verdict)} 结论 | {self._close_verdict_text(verdict)}",
            f"  {self._score_marker(float(metrics['sentiment_score']))} 情绪分 | {float(metrics['sentiment_score']):.1f}/10",
            f"  {self._promotion_marker(float(metrics['promotion_rate']))} 晋级率 | {float(metrics['promotion_rate']):.1%}",
            f"  {headshot_marker} 核按钮率 | {headshot_text}",
            f"  {red_open_marker} 红开率 | {red_open_text}",
            f"  {self._battle_marker(str(metrics['battle']))} 对局 | {self._battle_text(str(metrics['battle']))}",
            f"  ◎ 样本 | 前日涨停 {int(metrics['sample_total'])} | 覆盖 {int(metrics['sample_matched'])}/{int(metrics['sample_total'])}",
        )

    def _render_recap_mainline_recap(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        facts = state.context.session_facts
        hot_today = tuple(facts.hot_plate_today)
        lead = hot_today[0].plate_name if hot_today else "-"
        secondary = hot_today[1].plate_name if len(hot_today) > 1 else "-"
        previous_hot = tuple(facts.hot_plate_yesterday)
        previous_lead = previous_hot[0].plate_name if previous_hot else "-"
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        limit_lead, limit_secondary = self._summarize_limitup_mainline_by_rows(state, truth_rows)
        mainline_switch = bool(previous_lead and lead and previous_lead != lead)
        persistent = 0
        emerging = 0
        fading = 0
        for migration in facts.plate_migration:
            migration_type = self._classify_recap_migration(migration)
            if migration_type == "PERSIST":
                persistent += 1
            elif migration_type == "EMERGING":
                emerging += 1
            else:
                fading += 1
        return (
            "【主线复盘】维度 | 内容",
            f"  主线/副线 | {lead} / {secondary}",
            f"  涨停主线/次主线 | {limit_lead} / {limit_secondary}",
            f"  前日热板龙头 | {previous_lead or '-'}",
            f"  是否切换/迁移 | {'是' if mainline_switch else '否'} / {self._migration_text('EMERGING' if mainline_switch else 'PERSIST')}",
            f"  延续/新发酵/兑现 | {persistent}/{emerging}/{fading}",
        )

    def _render_recap_limitup_plate_board(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        self._ensure_postmarket_limit_truth_plate_enrichment(str(ref["trade_date"]), truth_rows)
        truth_ranked = self._rank_limitup_plates_from_truth(state, truth_rows)
        if not truth_ranked:
            return ("【涨停板块】暂无昨日涨停板块样本",)
        rows = ["【涨停板块】题材 | 涨停数 | 最高板 | 代表 | 定性"]
        for plate, items in truth_ranked[:6]:
            leader = max(
                items,
                key=lambda item: (
                    self._normalize_limitup_truth_lb_days(item.get("lb_days")),
                    float(item.get("auction_amount", 0.0) or 0.0),
                    float(item.get("current_pct", 0.0) or 0.0),
                ),
            )
            rows.append(
                "  "
                f"{plate}"
                f" | {len(items)}"
                f" | {self._format_limitup_board_height(max((self._normalize_limitup_truth_lb_days(item.get('lb_days')) for item in items), default=1))}"
                f" | {str(leader.get('name') or '-')}"
                f" | {self._limitup_plate_comment_from_truth(items)}"
            )
        return tuple(rows)

    def _render_recap_chance_board(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        yest_limit_map = ref["yest_limit_map"]
        assert isinstance(yest_limit_map, dict)
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        auction_map = ref["auction_map"]
        assert isinstance(auction_map, dict)
        truth_lb_map = {
            str(row.get("symbol") or "").strip(): self._normalize_limitup_truth_lb_days(row.get("lb_days"))
            for row in truth_rows
            if row.get("symbol")
        }
        promoted = [
            snapshot
            for symbol in yest_limit_map.keys()
            for snapshot in (state.snapshot_map.get(symbol),)
            if snapshot is not None and symbol in truth_lb_map
        ]
        promoted.sort(
            key=lambda item: (
                -truth_lb_map.get(item.symbol, max(item.lb_days, 1)),
                -item.current_pct,
                -item.auction_amount,
            )
        )
        first_board = [
            state.snapshot_map.get(str(row.get("symbol") or "").strip())
            for row in truth_rows
            if self._normalize_limitup_truth_lb_days(row.get("lb_days")) <= 1
        ]
        first_board = [snapshot for snapshot in first_board if snapshot is not None]
        first_board.sort(key=lambda item: (-item.current_pct, -item.auction_amount, item.leader_rank_in_theme))
        rebound = []
        for symbol, row in auction_map.items():
            snapshot = state.snapshot_map.get(symbol)
            if snapshot is None:
                continue
            open_pct = self._normalize_pct_value(row.get("change_pct", snapshot.open_pct))
            if open_pct < 0 and snapshot.current_pct >= 0.05:
                rebound.append(snapshot)
        rebound.sort(key=lambda item: (-item.current_pct, -item.auction_amount, item.leader_rank_in_theme))
        return (
            "【昨日机会】方向 | 样本",
            f"  连板承接 | {', '.join(self._compact_stock_ref(item) for item in promoted[:3]) or '-'}",
            f"  首板扩散 | {', '.join(self._compact_stock_ref(item) for item in first_board[:3]) or '-'}",
            f"  低开转强 | {', '.join(self._compact_stock_ref(item) for item in rebound[:3]) or '-'}",
        )

    def _render_recap_plan_review(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        auction_map = ref["auction_map"]
        assert isinstance(auction_map, dict)
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        persisted_opening = self._load_opening_validation_payload(state.context.trade_date)
        opening_payload = (
            persisted_opening
            or (
                {}
                if phase_label == "premarket"
                else self._build_opening_validation_payload(state)
            )
        )
        if persisted_opening:
            strong = tuple(str(item) for item in persisted_opening.get("strong", ()) if str(item))
            weak = tuple(str(item) for item in persisted_opening.get("weak", ()) if str(item))
            rebound = tuple(str(item) for item in persisted_opening.get("rebound", ()) if str(item))
        else:
            strong = self._pick_auction_outcome_names(
                state,
                predicate=lambda snapshot: snapshot.open_pct >= 0.02 and self._is_limit_up_snapshot(snapshot),
                limit=2,
            )
            weak = self._pick_auction_outcome_names(
                state,
                predicate=lambda snapshot: snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.05,
                limit=2,
            )
            rebound = self._pick_auction_outcome_names(
                state,
                predicate=lambda snapshot: snapshot.open_pct < 0.0 and snapshot.current_pct >= 0.05,
                limit=2,
            )
        opening_feedback_parts: list[str] = []
        if strong:
            opening_feedback_parts.append("强开兑现=" + "、".join(strong))
        if weak:
            opening_feedback_parts.append("高开转虚=" + "、".join(weak))
        if rebound:
            opening_feedback_parts.append("低开转强=" + "、".join(rebound))
        validated = tuple(str(item) for item in opening_payload.get("validated", ()) if str(item))
        plate_checks = tuple(str(item) for item in opening_payload.get("plate_checks", ()) if str(item))
        prediction_checks = tuple(str(item) for item in opening_payload.get("prediction_checks", ()) if str(item))
        auction_plate_amounts: dict[str, float] = defaultdict(float)
        auction_plate_counts: dict[str, int] = defaultdict(int)
        for symbol, row in sorted(
            auction_map.items(),
            key=lambda item: float(item[1].get("amount", 0.0) or 0.0),
            reverse=True,
        )[:30]:
            snapshot = state.snapshot_map.get(symbol)
            if snapshot is None:
                continue
            plate = self._display_plate_name(snapshot, prefer_high_board=True)
            if not plate or plate == "-" or is_generic_plate(plate):
                continue
            auction_plate_amounts[plate] += float(row.get("amount", 0.0) or 0.0)
            auction_plate_counts[plate] += 1
        auction_leads = [
            plate
            for plate, _ in sorted(
                auction_plate_amounts.items(),
                key=lambda item: (item[1], auction_plate_counts[item[0]]),
                reverse=True,
            )[:3]
        ]
        hot_leads = [fact.plate_name for fact in state.context.session_facts.hot_plate_today[:3]]
        limit_lead, limit_secondary = self._summarize_limitup_mainline_by_rows(state, truth_rows)
        final_leads = [plate for plate in (limit_lead, limit_secondary, *hot_leads[:2]) if plate and plate != "-"]
        overlap = [plate for plate in auction_leads if plate in final_leads]
        validation_score = self._score_opening_validations(validated)
        plate_check_names = self._extract_plate_check_names(plate_checks)
        plate_support = [plate for plate in plate_check_names if plate in final_leads]
        hot_plate_support = [plate for plate in hot_leads[:2] if plate in final_leads]
        if overlap and validation_score["negative"] > validation_score["positive"]:
            verdict = "预判偏错"
            adjust = (
                f"竞价主看方向是 {','.join(overlap)}，"
                "但开盘后的承接和回流没有兑现，说明预判需要降级处理。"
            )
        elif overlap:
            verdict = "预判半对"
            if validation_score["positive"] > 0:
                adjust = f"竞价主看方向仍有 {','.join(overlap)}，但只有局部兑现，后续更适合只盯前排和回流确认。"
            else:
                adjust = f"竞价主看方向仍是 {','.join(overlap)}，但强度没有明显扩散，说明更多是存量博弈。"
        elif validation_score["positive"] > 0 or plate_support or hot_plate_support:
            verdict = "预判修正"
            if plate_support:
                adjust = f"开盘后资金进一步收敛到 {','.join(dict.fromkeys(plate_support[:2]))}，说明盘面真实主攻已完成切换，需按新主线处理。"
            elif validation_score["positive"] > 0:
                adjust = "开盘验证里出现了更强的承接和回流信号，说明真实机会不完全在竞价结论里，需用开盘结果修正预案。"
            else:
                adjust = "竞价本身不够清楚，但开盘后的板块联动更完整，说明需要以后验主线为准。"
        else:
            verdict = "继续观察"
            adjust = "竞价和开盘都没有形成清晰主攻，先以防守和等待确认为主，不急着给强结论。"
        return (
            "【竞价收盘对照】维度 | 结果",
            f"  竞价主看 | {', '.join(auction_leads) or '-'}",
            f"  收盘主线 | {', '.join(dict.fromkeys(final_leads[:3])) or '-'}",
            f"  开盘反馈 | {' ; '.join(opening_feedback_parts) or '-'}",
            f"  预判校验 | {' ; '.join(prediction_checks[:2]) or '-'}",
            f"  开盘验证 | {' ; '.join(validated) or '-'}",
            f"  板块验证 | {' ; '.join(plate_checks[:2]) or '-'}",
            f"  结论判断 | {verdict}",
            f"  调整建议 | {adjust}",
        )

    def _render_recap_ladder_recap(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        metrics = self._compute_recap_feedback_metrics(state, phase_label=phase_label)
        ref = self._load_recap_reference(state, phase_label=phase_label)
        truth_rows = ref["truth_rows"]
        assert isinstance(truth_rows, tuple)
        high_board_count = sum(
            1
            for row in truth_rows
            if self._normalize_limitup_truth_lb_days(row.get("lb_days")) >= 3
        )
        yest_limit_count = int(metrics["sample_total"])
        locked_count = int(metrics.get("promoted_count", 0) or 0)
        auction_ready = int(metrics.get("auction_sample_matched", 0) or 0) > 0
        red_open_rate = metrics["red_open_rate"]
        headshot_rate = float(metrics["headshot_rate"])
        if auction_ready and isinstance(red_open_rate, float):
            red_open_text = f"{red_open_rate:.1%}"
            red_marker = self._red_open_marker(red_open_rate)
        else:
            red_open_text = "--"
            red_marker = "?"
        headshot_text = f"{headshot_rate:.1%}" if auction_ready else "--"
        headshot_marker = self._headshot_marker(headshot_rate) if auction_ready else "?"
        return (
            "【高位梯队复盘】指标 | 数值",
            f"  ▲ 三板及以上 | {high_board_count}",
            f"  ◇ 前日涨停反馈样本 | {yest_limit_count}",
            f"  ⛔ 封死数量 | {locked_count}",
            f"  {self._promotion_marker(float(metrics['promotion_rate']))} 晋级率 | {float(metrics['promotion_rate']):.1%}",
            f"  {headshot_marker} 核按钮率 | {headshot_text}",
            f"  {red_marker} 红开率 | {red_open_text}",
        )

    def _render_close_recap(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        verdict = self._infer_close_verdict(summary)
        feedback_ready = self._feedback_metrics_ready(state)
        close_marker = self._close_marker(verdict) if feedback_ready else "?"
        close_text = self._close_verdict_text(verdict) if feedback_ready else "--"
        score_marker = self._score_marker(summary.sentiment_score) if feedback_ready else "?"
        score_text = f"{summary.sentiment_score:.1f}/10" if feedback_ready else "--"
        promotion_marker = self._promotion_marker(summary.promotion_rate) if feedback_ready else "?"
        promotion_text = f"{summary.promotion_rate:.1%}" if feedback_ready else "--"
        headshot_marker = self._headshot_marker(summary.headshot_rate) if feedback_ready else "?"
        headshot_text = f"{summary.headshot_rate:.1%}" if feedback_ready else "--"
        red_open_marker = self._red_open_marker(summary.red_open_rate) if feedback_ready else "?"
        red_open_text = f"{summary.red_open_rate:.1%}" if feedback_ready else "--"
        battle_marker = self._battle_marker(summary.battle_status or "-") if feedback_ready else "?"
        battle_text = self._battle_text(summary.battle_status or "-") if feedback_ready else "--"
        return (
            "【收盘定性】指标 | 数值",
            f"  {close_marker} 结论 | {close_text}",
            f"  {score_marker} 情绪分 | {score_text}",
            f"  {promotion_marker} 晋级率 | {promotion_text}",
            f"  {headshot_marker} 核按钮率 | {headshot_text}",
            f"  {red_open_marker} 红开率 | {red_open_text}",
            f"  {battle_marker} 对局 | {battle_text}",
        )

    def _render_mainline_recap(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        hot_plate_mode = self._hot_plate_render_mode(state)
        if hot_plate_mode != "today":
            hot_plate_note = self._hot_plate_note(state)
            limitup_lead, limitup_secondary = self._summarize_limitup_mainline(state)
            return (
                "【主线复盘】维度 | 内容",
                "  主线/副线 | -- / --",
                f"  涨停主线/次主线 | {limitup_lead} / {limitup_secondary}",
                f"  前日热板龙头 | -- ({hot_plate_note})",
                "  是否切换/迁移 | -- / --",
                "  延续/新发酵/兑现 | --/--/--",
            )
        lead, secondary = self._background_mainline_pair(state)
        if lead == "-":
            lead = summary.mainline_sector or summary.top_plate_name or (state.plate_stats[0].plate_name if state.plate_stats else "-")
        scope_lead, _scope_secondary = self._execution_mainline_pair(state)
        if scope_lead == "-":
            scope_lead = state.plate_stats[0].plate_name if state.plate_stats else "-"
        limitup_lead, limitup_secondary = self._summarize_limitup_mainline(state)
        return (
            "【主线复盘】维度 | 内容",
            f"  主线/副线 | {lead} / {secondary}",
            f"  涨停主线/次主线 | {limitup_lead} / {limitup_secondary}",
            f"  前日热板龙头 | {scope_lead}",
            f"  是否切换/迁移 | {'是' if summary.mainline_switch else '否'} / {self._migration_text(summary.top_plate_migration_type or '-')}",
            f"  延续/新发酵/兑现 | {summary.persistent_plate_count}/{summary.emerging_plate_count}/{summary.fading_plate_count}",
        )

    def _render_ladder_recap(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        high_board_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.lb_days >= 3)
        yest_limit_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.is_yest_limit)
        locked_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.is_yest_limit and snapshot.is_locked)
        hot_plate_mode = self._hot_plate_render_mode(state)
        feedback_ready = self._feedback_metrics_ready(state)
        resonance_marker = self._resonance_marker(summary.resonance_score) if hot_plate_mode == "today" else "?"
        resonance_text = f"{summary.resonance_score:.2f}" if hot_plate_mode == "today" else "--"
        promotion_marker = self._promotion_marker(summary.promotion_rate) if feedback_ready else "?"
        promotion_text = f"{summary.promotion_rate:.1%}" if feedback_ready else "--"
        headshot_marker = self._headshot_marker(summary.headshot_rate) if feedback_ready else "?"
        headshot_text = f"{summary.headshot_rate:.1%}" if feedback_ready else "--"
        return (
            "【高位梯队复盘】指标 | 数值",
            f"  ▲ 三板及以上 | {high_board_count}",
            f"  ◇ 前日涨停反馈样本 | {yest_limit_count}",
            f"  ⛔ 封死数量 | {locked_count}",
            f"  {promotion_marker} 晋级率 | {promotion_text}",
            f"  {headshot_marker} 核按钮率 | {headshot_text}",
            f"  {resonance_marker} 共振分 | {resonance_text}",
        )

    def _render_tomorrow_plan(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        if summary.sentiment_score >= 6.0 and summary.headshot_rate <= 0.05:
            primary = "若主线继续强化，优先看核心龙头低风险延续和前排题材跟随。"
        elif summary.sentiment_score >= 4.0:
            primary = "若主线延续，优先看前排分歧转强和中位卡位，不追一致后排。"
        else:
            primary = "若负反馈继续扩散，缩到观察名单，等新的低风险信号。"
        if summary.mainline_switch:
            secondary = "若切换被确认，只做新主线前排，不在老主线后排里纠缠。"
        else:
            secondary = "若主线延续，明天先看核心龙头是否获得资金再承接。"
        return (
            "【明日预案】脚本 | 内容",
            f"  A 主预案 | {primary}",
            f"  B 次预案 | {secondary}",
        )

    def _render_day_recap_story(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        verdict = self._infer_close_verdict(summary)
        feedback_ready = self._feedback_metrics_ready(state)
        lead, secondary = self._background_mainline_pair(state)
        if lead == "-":
            lead = summary.mainline_sector or summary.top_plate_name or (state.plate_stats[0].plate_name if state.plate_stats else "-")
        scope_lead, _scope_secondary = self._execution_mainline_pair(state)
        if scope_lead == "-":
            scope_lead = state.plate_stats[0].plate_name if state.plate_stats else lead
        open_text = f"红开率 {summary.red_open_rate:.1%}，{self._auction_outcome_summary(state)}" if feedback_ready else "红开率 --，竞价反馈样本不足"
        close_text = (
            f"{self._close_verdict_text(verdict)}，晋级率 {summary.promotion_rate:.1%}，核按钮率 {summary.headshot_rate:.1%}"
            if feedback_ready
            else "--，晋级率 --，核按钮率 --"
        )
        return (
            "【竞价收盘对照】维度 | 结果",
            f"  竞价观察 | {open_text}",
            f"  主线演绎 | {scope_lead} 对比收盘主线 {lead} / {secondary}，{'发生切换' if summary.mainline_switch else '未发生切换'}",
            f"  收盘结论 | {close_text}",
        )

    def _render_today_hot_plates(self, state: StrategyConsoleState) -> tuple[str, ...]:
        hot_plate_mode = self._hot_plate_render_mode(state)
        if hot_plate_mode != "today":
            return (f"【今日热点】{self._hot_plate_note(state)}，暂不展示当日热板排名/热度/强度。",)
        if not state.plate_stats:
            return ("【今日热点】暂无题材样本",)
        rows = ["【今日热点】题材 | 热度 | 热度名次 | 涨跌/净额 | 结论"]
        for row in state.plate_stats[:4]:
            representative_symbol = self._select_plate_representative_symbol(
                state,
                plate_name=row.plate_name,
                symbols=row.sample_symbols,
            )
            representative = self._snapshot_name_by_symbol_compact(state, representative_symbol) if representative_symbol else "-"
            rows.append(
                "  "
                f"{row.plate_name}"
                f" | {row.weighted_score:.1f}"
                f" | {row.hot_change_pct:+.1f}%"
                f" | {self._fmt_net_inflow_yi(row.hot_net_inflow_yi)}"
                f" | {self._capital_behavior_text(row.hot_capital_behavior)}"
                f" | {representative}"
            )
        return tuple(rows)

    def _render_limitup_plate_board(self, state: StrategyConsoleState) -> tuple[str, ...]:
        truth_rows = self._load_postmarket_limit_truth_rows(state.context.trade_date)
        self._ensure_postmarket_limit_truth_plate_enrichment(state.context.trade_date, truth_rows)
        truth_ranked = self._rank_limitup_plates_from_truth(state, truth_rows)
        if truth_ranked:
            rows = ["【涨停板块】题材 | 涨停数 | 最高板 | 代表 | 定性"]
            for plate, items in truth_ranked[:6]:
                leader = max(
                    items,
                    key=lambda item: (
                        self._normalize_limitup_truth_lb_days(item.get("lb_days")),
                        float(item.get("auction_amount", 0.0) or 0.0),
                        float(item.get("current_pct", 0.0) or 0.0),
                    ),
                )
                rows.append(
                    "  "
                    f"{plate}"
                    f" | {len(items)}"
                    f" | {self._format_limitup_board_height(max((self._normalize_limitup_truth_lb_days(item.get('lb_days')) for item in items), default=1))}"
                    f" | {str(leader.get('name') or '-')}"
                    f" | {self._limitup_plate_comment_from_truth(items)}"
                )
            return tuple(rows)

        plate_rows: dict[str, list[StockStateSnapshot]] = defaultdict(list)
        for snapshot in state.snapshot_map.values():
            if not self._is_limit_up_snapshot(snapshot):
                continue
            plate = self._display_plate_name(snapshot, prefer_high_board=True)
            if not plate or plate == "-":
                continue
            plate_rows[plate].append(snapshot)
        if not plate_rows:
            return ("暂无涨停板块归因",)
        ranked = sorted(
            plate_rows.items(),
            key=lambda item: (
                len(item[1]),
                max((snapshot.lb_days for snapshot in item[1]), default=0),
                max((snapshot.auction_amount for snapshot in item[1]), default=0.0),
            ),
            reverse=True,
        )
        rows = ["【涨停板块】题材 | 涨停数 | 最高板 | 代表 | 定性"]
        for plate, snapshots in ranked[:4]:
            leader = max(
                snapshots,
                key=lambda snapshot: (max(snapshot.lb_days, 1), snapshot.auction_amount, snapshot.current_pct),
            )
            rows.append(
                "  "
                f"{plate}"
                f" | {len(snapshots)}"
                f" | {self._format_limitup_board_height(max((max(snapshot.lb_days, 1) for snapshot in snapshots), default=1))}"
                f" | {self._compact_stock_ref(leader)}"
                f" | {self._limitup_plate_comment(snapshots)}"
            )
        return tuple(rows)

    def _render_auction_outcome(self, state: StrategyConsoleState) -> tuple[str, ...]:
        strong = self._pick_auction_outcome_names(
            state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.02 and self._is_limit_up_snapshot(snapshot),
        )
        weak = self._pick_auction_outcome_names(
            state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.05,
        )
        rebound = self._pick_auction_outcome_names(
            state,
            predicate=lambda snapshot: snapshot.open_pct < 0.0 and snapshot.current_pct >= 0.05,
        )
        return (
            "【竞价结局】方向 | 结果",
            f"  强开兑现 | {', '.join(strong) or '-'}",
            f"  高开转虚 | {', '.join(weak) or '-'}",
            f"  低开转强 | {', '.join(rebound) or '-'}",
        )

    def _render_opening_validation_hub(self, state: StrategyConsoleState) -> tuple[str, ...]:
        bundle = getattr(state.context, "opening_validation_bundle", None)
        if bundle is None:
            return ()
        script_label = {
            "extension": "延续",
            "rotation": "切换",
            "distribution": "兑现",
            "unknown": "待判",
        }
        state_label = {
            "confirmed": "确认",
            "watch": "观察",
            "falsified": "证伪",
        }
        tradable_label = {
            "attack": "主攻",
            "probe": "试错",
            "watch": "观察",
            "avoid": "回避",
        }
        confirmed = tuple((getattr(bundle, "confirmed_themes", {}) or {}).values())
        falsified = tuple((getattr(bundle, "falsified_themes", {}) or {}).values())
        watch = tuple((getattr(bundle, "watch_themes", {}) or {}).values())
        lines = ["【剧本裁决】方向 | 结果"]
        lines.append(
            self._render_opening_front_slice_line(
                self._market_slice_comparison_for_phase(state, phase_label="open_confirm")
            )
        )
        lines.append(
            f"  主验证题材 | {str(getattr(bundle, 'main_validated_theme', '') or '-')}"
            f" / 次验证题材 {str(getattr(bundle, 'backup_validated_theme', '') or '-')}"
        )
        lines.append(f"  已确认/证伪/观察 | {len(confirmed)} / {len(falsified)} / {len(watch)}")
        lines.append(f"  延续 | {', '.join(item.plate_name for item in confirmed if item.predicted_script == 'extension') or '-'}")
        lines.append(f"  切换 | {', '.join(item.plate_name for item in confirmed if item.predicted_script == 'rotation') or '-'}")
        lines.append(f"  兑现 | {', '.join(item.plate_name for item in falsified if item.predicted_script in {'distribution', 'extension'}) or '-'}")
        lines.append("【验证后题材】题材 | 预判 | 验证 | 可做 | 证据")
        top_rows = sorted(
            list(confirmed) + list(watch) + list(falsified),
            key=lambda item: (
                str(getattr(item, "validation_state", "") or "") == "confirmed",
                str(getattr(item, "tradable_level", "") or "") == "attack",
                -float(getattr(item, "amount_2m_rank_pct", 1.0) or 1.0),
            ),
            reverse=True,
        )[:6]
        for item in top_rows:
            evidence = " / ".join(tuple(getattr(item, "evidence", ()) or ())[:2]) or str(getattr(item, "invalid_reason", "") or "-")
            lines.append(
                f"  {item.plate_name} | {script_label.get(item.predicted_script, item.predicted_script)}"
                f" | {state_label.get(item.validation_state, item.validation_state)}"
                f" | {tradable_label.get(item.tradable_level, item.tradable_level)}"
                f" | {evidence}"
            )
        lines.extend(self._render_validated_candidates(state))
        return tuple(lines)

    def _build_opening_validation_payload(
        self,
        state: StrategyConsoleState,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]:
        eval_state = state
        if not state.coverage_scope_set and state.snapshot_map:
            all_symbols = tuple(state.snapshot_map.keys())
            eval_state = replace(
                state,
                coverage_scope=all_symbols,
                coverage_scope_set=frozenset(all_symbols),
            )
        strong = self._pick_auction_outcome_names(
            eval_state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.02 and self._is_limit_up_snapshot(snapshot),
        )
        weak = self._pick_auction_outcome_names(
            eval_state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.05,
        )
        rebound = self._pick_auction_outcome_names(
            eval_state,
            predicate=lambda snapshot: self._is_low_open_rebound_snapshot(snapshot),
        )
        confirmations: list[str] = []
        validated: list[str] = []
        for decision in self._opening_validation_focus_decisions(eval_state):
            snapshot = eval_state.snapshot_map.get(decision.symbol)
            if snapshot is None:
                continue
            truth_label = self._leader_truth_label(snapshot)
            if self._is_low_open_rebound_snapshot(snapshot):
                truth_label = "低开转强"
            action_label = self._display_action_label(decision, eval_state, phase_label="open_confirm")
            validated.append(f"{self._decision_name_compact(eval_state, decision)}={action_label}/{truth_label}")
            if len(validated) >= 3:
                break
        plate_checks: list[str] = []
        prediction_checks: list[str] = []
        invalidation_reasons: list[str] = []
        theme_validation: list[dict[str, object]] = []
        collision_rows = self._theme_collision_rows(eval_state)[:3] if self._expectation_ready(eval_state) else ()
        for item in collision_rows:
            row = item.row
            judge = self._theme_judge_for_plate(eval_state, row.plate_name)
            validation_state, validation_metrics = self._theme_opening_validation_state(eval_state, item)
            action_class = (
                judge.action_class
                if judge is not None
                else self._theme_action_class(item, validation_state=validation_state)
            )
            trap_score = judge.trap_score if judge is not None else round(item.x_score, 1)
            opportunity_score = (
                judge.opportunity_score
                if judge is not None
                else round(min(max(((item.e_score * 0.55) + (item.a_score * 0.45) - (item.x_score * 0.25)), 0.0), 10.0), 1)
            )
            representative = self._snapshot_name_by_symbol_compact(eval_state, row.sample_symbols[0]) if row.sample_symbols else "-"
            hot_rank = self._collision_rank_text(row, item.hot_rank, hot=True)
            yest_hot_rank = self._collision_rank_text(row, item.yesterday_hot_rank, hot=True)
            confirm_label = "缁存寔"
            if validation_state == "falsified":
                confirm_label = "璇佷吉"
            elif validation_state == "strengthened":
                confirm_label = "鍔犲己"
            execution_state = self._external_validation_state(validation_state)
            confirmations.append(f"{row.plate_name}={confirm_label}")
            expected_bias = (
                str(getattr(judge, "action_class", "") or "")
                if judge is not None
                else str(getattr(item, "eax_action", "") or "")
            )
            prediction_checks.append(
                f"{row.plate_name}=预判{self._theme_action_class_text(expected_bias) if expected_bias in {'main_attack','front_row_confirm','observe','trap_avoid','anchor_only'} else expected_bias or '-'}"
                f"→验证{execution_state}"
                f"(前排承接2m {int(validation_metrics.get('undertake_count', 0.0))}/{int(validation_metrics.get('front_row_count', 0.0))}"
                f", 5m {int(validation_metrics.get('undertake_count_5m', 0.0))}/{int(validation_metrics.get('front_row_count', 0.0))}"
                f", 10m代理 {int(validation_metrics.get('undertake_count_10m_proxy', 0.0))}/{int(validation_metrics.get('front_row_count', 0.0))})"
            )
            if validation_state == "falsified":
                invalidation_reasons.append(
                    f"{row.plate_name}=前排承接偏弱({int(validation_metrics.get('undertake_count', 0.0))}/{int(validation_metrics.get('front_row_count', 0.0))})"
                )
            theme_validation.append(
                {
                    "plate_name": row.plate_name,
                    "validation_state": validation_state,
                    "execution_state": execution_state,
                    "action_class": action_class,
                    "trap_score": trap_score,
                    "opportunity_score": opportunity_score,
                    "signal": judge.signal if judge is not None else item.signal,
                    "expectation_label": judge.expectation_label if judge is not None else item.expectation_label,
                    "undertake_ratio": float(validation_metrics.get("undertake_ratio", 0.0)),
                    "undertake_count": int(validation_metrics.get("undertake_count", 0.0)),
                    "undertake_count_5m": int(validation_metrics.get("undertake_count_5m", 0.0)),
                    "undertake_count_10m_proxy": int(validation_metrics.get("undertake_count_10m_proxy", 0.0)),
                    "front_row_count": int(validation_metrics.get("front_row_count", 0.0)),
                    "leader_only_alive": 1
                    if (
                        execution_state == "falsified"
                        and (
                            self._theme_conclusion_for_plate(eval_state, row.plate_name) == "leader_only_alive"
                            or action_class == "anchor_only"
                        )
                    )
                    else 0,
                }
            )
            plate_checks.append(
                f"{row.plate_name}"
                f"{row.plate_name}"
                f" | {row.weighted_score:.1f}"
            )
        if not invalidation_reasons and weak:
            invalidation_reasons.extend(f"{name}=高开后承接转弱" for name in weak[:2])
        correction_conclusion = self._opening_correction_conclusion(
            confirmations=confirmations,
            theme_validation=theme_validation,
            weak=weak,
            rebound=rebound,
        )
        auction_mode_code = self._effective_money_mode_code(replace(eval_state, context=replace(eval_state.context, phase=RunPhase.AUCTION)))
        opening_mode_code = self._effective_money_mode_code(eval_state)
        opening_mode_code, opening_mode_override_reason = self._opening_mode_hard_override(
            auction_mode_code=auction_mode_code,
            opening_mode_code=opening_mode_code,
            theme_validation=theme_validation,
            state=eval_state,
        )
        mode_validation_state, mode_validation_reason = self._validate_auction_mode_with_opening_2m(
            auction_mode_code=auction_mode_code,
            opening_mode_code=opening_mode_code,
            theme_validation=theme_validation,
            state=eval_state,
        )
        if opening_mode_override_reason:
            mode_validation_reason = f"{mode_validation_reason}；{opening_mode_override_reason}"
        selection_contexts = tuple(getattr(eval_state.bundle, "stock_selection_contexts", ()) or ())
        open_follow_summary = {
            "confirmed": sum(1 for item in selection_contexts if item.open_follow_state == "confirmed"),
            "repair_strength": sum(1 for item in selection_contexts if item.open_follow_state == "repair_strength"),
            "weak_follow": sum(1 for item in selection_contexts if item.open_follow_state == "weak_follow"),
            "faded": sum(1 for item in selection_contexts if item.open_follow_state == "faded"),
        }
        return {
            "trade_date": eval_state.context.trade_date,
            "phase": eval_state.context.phase.value,
            "updated_at": (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at_ts": int((now or datetime.now()).timestamp()),
            "primary_prediction": self._primary_prediction_summary(eval_state),
            "auction_mode": {
                "code": auction_mode_code,
                "label": self._money_mode_label(auction_mode_code),
                "confidence": self._money_mode_confidence(replace(eval_state, context=replace(eval_state.context, phase=RunPhase.AUCTION)), auction_mode_code),
            },
            "opening_mode": {
                "code": opening_mode_code,
                "label": self._money_mode_label(opening_mode_code),
                "confidence": self._money_mode_confidence(eval_state, opening_mode_code),
            },
            "mode_validation": {
                "state": mode_validation_state,
                "label": self._money_mode_validation_label(mode_validation_state),
                "reason": mode_validation_reason,
            },
            "strong": list(strong),
            "weak": list(weak),
            "rebound": list(rebound),
            "confirmations": confirmations,
            "prediction_checks": prediction_checks,
            "invalidation_reasons": invalidation_reasons,
            "validated": validated,
            "correction_conclusion": correction_conclusion,
            "plate_checks": plate_checks,
            "theme_validation": theme_validation,
            "open_follow_summary": open_follow_summary,
        }

    def _auction_invalidation_text(self, state: StrategyConsoleState) -> str:
        output_summary = self._playbook_output_summary_for_state(state)
        global_decision = self._global_market_decision_for_state(state)
        if output_summary is not None:
            invalidation_points = tuple(getattr(output_summary, "invalidation_points", ()) or ())
            invalidation_texts = [
                text
                for text in (self._invalidation_point_text(point) for point in invalidation_points[:3])
                if text
            ]
            if invalidation_texts:
                theme_name = ""
                if global_decision is not None:
                    theme_name = normalize_plate_name(str(getattr(global_decision, "main_attack_theme", "") or ""))
                prefix = f"{theme_name} 若" if theme_name else "若"
                return f"{prefix}{'，'.join(invalidation_texts)}，则主预判失效。"
        return "若前排2分钟承接不足、高开后快速回落，或修复信号迟迟不出现，则主预判失效。"

    def _opening_correction_conclusion(
        self,
        *,
        confirmations: Iterable[str],
        theme_validation: Iterable[dict[str, object]],
        weak: Iterable[str],
        rebound: Iterable[str],
    ) -> str:
        validations = tuple(theme_validation)
        strengthened = [item for item in validations if str(item.get("validation_state") or "") == "strengthened"]
        falsified = [item for item in validations if str(item.get("validation_state") or "") == "falsified"]
        if strengthened and not falsified:
            return "维持主预判，继续只看前排承接和换手确认。"
        if falsified and strengthened:
            return "主预判部分失效，降级到分歧观察，只保留局部前排。"
        if falsified:
            return "主预判失效，切到防守观察，回避后排扩散。"
        if tuple(rebound):
            return "盘面有低开转强修复，优先看修复型前排。"
        if tuple(weak):
            return "高开兑现明显，先降速，等待回封和承接再判断。"
        if tuple(confirmations):
            return "预判暂未证伪，继续等待更明确的板块确认。"
        return "暂无统一修正结论，保持观察。"

    def _format_playbook_item(
        self,
        decision: AuctionLadderDecision,
        snapshot: StockStateSnapshot | None,
        *,
        action: str,
        plate: str,
        evidence: str,
        state: StrategyConsoleState | None = None,
        phase_label: str = "auction",
    ) -> str:
        snapshot_map = {decision.symbol: snapshot} if snapshot is not None else {}
        tags = self._decision_meta_tags(decision, snapshot_map, state=state)
        action_reason = self._display_action_reason_text(decision, state, phase_label=phase_label) if state is not None else ""
        merged_evidence = evidence if not tags else f"{tags} / {evidence}"
        if action_reason:
            merged_evidence = f"{merged_evidence} / {action_reason}" if merged_evidence else action_reason
        return (
            f"{self._short_stock_name(snapshot, symbol=decision.symbol)}"
            f" | {action}"
            f" | {decision.confidence}"
            f" | {self._fmt_pct(snapshot.open_pct) if snapshot is not None else '-'}"
            f" | {self._fmt_pct(snapshot.current_pct) if snapshot is not None else '-'}"
            f" | {self._format_stock_hot_text(snapshot)}"
            f" | {plate}"
            f" | {merged_evidence}"
        )

    def _infer_market_mainline_label(self, summary, main_name: str) -> str:
        if not main_name or main_name == "-":
            return "-"
        if summary.mainline_switch:
            return "switch"
        migration = str(summary.top_plate_migration_type or "").upper()
        if migration == "PERSIST":
            return "market"
        if migration == "EMERGING":
            return "emerging"
        if migration == "FADING":
            return "fading"
        return "market"

    def _short_stock_name(self, snapshot: StockStateSnapshot | None, *, symbol: str = "") -> str:
        raw_name = ""
        if snapshot is not None:
            raw_name = str(snapshot.name or "").strip()
        text = raw_name.replace(" ", "").replace("(", "").replace(")", "").replace("?", "").replace("?", "").replace("*", "")
        text = _TRAILING_NAME_NOISE_RE.sub("", text)
        text = text.rstrip("-_")
        if text:
            return text[:4]
        return symbol or "-"

    def _plate_role_text(self, row: AuctionPlateBucketStat) -> str:
        expectation = self.EXPECTATION_LABELS.get(row.expectation, row.expectation)
        if row.generic:
            return "泛题材"
        if expectation == "attack":
            return "进攻"
        if expectation == "follow":
            return "跟随"
        if expectation == "ladder":
            if row.hot_change_pct <= 0 and row.yest_limit_count >= 2:
                return "晋级"
            return "梯队"
        if expectation == "cluster":
            return "联动"
        return "观察"

    def _fmt_volume_intensity(self, value: float) -> str:
        if value <= 1.0:
            return "-"
        return f"{value:.1f}倍"
    def _snapshot_name_by_symbol(self, state_or_context: StrategyConsoleState | IntradayContext, symbol: str) -> str:
        if isinstance(state_or_context, StrategyConsoleState):
            matched = state_or_context.snapshot_map.get(symbol)
            return self._compact_stock_ref(matched, symbol=symbol)
        matched = next((snapshot for snapshot in state_or_context.stock_snapshots if snapshot.symbol == symbol), None)
        return self._compact_stock_ref(matched, symbol=symbol)

    def _decision_name(self, state: StrategyConsoleState, decision: AuctionLadderDecision) -> str:
        matched = state.snapshot_map.get(decision.symbol)
        return self._compact_stock_ref(matched, symbol=decision.symbol)

    def _display_source_label(self, state: StrategyConsoleState, *, phase_label: str) -> str:
        raw_source = state.actual_source or "-"
        if self._is_historical_mode(state, phase_label=phase_label):
            if raw_source in ("startup_repair", "hot_plate_cache", "yest_limit_cache", "redis_runtime_projection"):
                return "昨收快照" if phase_label == "premarket" else "盘中静态快照"
        if phase_label in {"opening", "open_confirm"}:
            if raw_source in {"redis_anchor", "redis_0925", "redis_preview_0920", "redis_preview_0924"}:
                return "09:25竞价预期+开盘验证"
            if raw_source == "t1_v2_q2_live":
                return "竞价预期+开盘实时验证"
            if raw_source == "stale_intraday_snapshot":
                return "竞价预期+开盘滞后验证"
        if state.frozen_postmarket_snapshot:
            return "盘中冻结快照"
        if phase_label == "intraday" and raw_source in {
            "redis_anchor",
            "redis_0925",
            "redis_preview_0920",
            "redis_preview_0924",
            "stale_intraday_snapshot",
        }:
            return "盘中滞后快照"
        if phase_label == "postmarket" and raw_source in {
            "redis_anchor",
            "redis_0925",
            "redis_preview_0920",
            "redis_preview_0924",
            "stale_intraday_snapshot",
        }:
            return "收盘快照"
        return self._source_text(raw_source)

    def _phase_text(self, phase_label: str) -> str:
        mapping = {
            "premarket": "盘前",
            "auction_preview": "竞价预热",
            "auction": "竞价",
            "opening": "开盘",
            "open_confirm": "开盘确认",
            "intraday": "盘中",
            "postmarket": "盘后",
        }
        return mapping.get(phase_label, phase_label)

    @staticmethod
    def _phase_label_for_context(phase: RunPhase) -> str:
        mapping = {
            RunPhase.PREMARKET: "premarket",
            RunPhase.AUCTION: "auction",
            RunPhase.INTRADAY: "intraday",
            RunPhase.POSTMARKET: "postmarket",
        }
        return mapping.get(phase, "intraday")

    def _phase_window_label(self, phase_label: str) -> str:
        if phase_label == "premarket":
            return "00:00-09:25"
        if phase_label == "auction_preview":
            return "09:15-09:24"
        if phase_label == "auction":
            return "09:25-09:30"
        if phase_label == "opening":
            return "09:30-09:40"
        if phase_label == "open_confirm":
            return "09:31-09:35"
        if phase_label == "postmarket":
            return "15:00-17:40"
        return "09:40-15:00"

    def _runtime_text(self, label: str) -> str:
        mapping = {
            "trade_ready_runtime": "可交易",
            "degraded_runtime": "降级运行",
            "observe_runtime": "仅观察",
            "anchor_ready_runtime": "锚点就绪",
            "historical_context_only": "仅历史快照",
            "postmarket_recap_ready": "复盘就绪",
            "postmarket_partial": "部分复盘",
            "postmarket_warming": "盘后加载中",
        }
        return mapping.get(label, label)

    def _ladder_sort_value(self, key: str) -> int:
        digits = "".join(ch for ch in key if ch.isdigit())
        return int(digits or 0)

    def _action_text(self, action: str) -> str:
        mapping = {
            "dragon_board": "龙头打板",
            "theme_first_board": "题材首板",
            "leader_hold": "龙头持有",
            "ice_probe": "冰点试错",
            "observe_only": "只观察",
            "leader_watch": "龙头观察",
            "front_row_watch": "前排跟踪",
            "confirm_then_go": "确认后再做",
            "failed_promo_guard": "失败回避",
            "do_not_chase": "禁止追高",
        }
        return mapping.get(action, action)

    def _allow_text(self, item: str) -> str:
        mapping = {
            "tomorrow_watch": "明日观察",
            "leader_review": "龙头复盘",
            "plate_recap": "题材复盘",
            "carry_review": "承接复核",
            "watch_only": "只观察",
            "auction_wait": "等待竞价",
            "risk_scan": "风险扫描",
        }
        return mapping.get(item, self._action_text(item))

    def _ban_text(self, item: str) -> str:
        mapping = {
            "live_trade": "禁止实盘",
            "blind_trade": "禁止乱打",
            "blind_chase": "禁止乱追",
            "high_chase": "禁止追高",
            "follow_trade": "禁止做跟风",
            "generic_theme_only": "泛题材勿上",
            "full_position": "禁止满仓",
            "none": "无",
        }
        return mapping.get(item, item)

    def _expectation_text(self, item: str) -> str:
        mapping = {
            "attack": "主攻",
            "follow": "跟随",
            "distribution": "兑现",
            "ladder": "梯队",
            "cluster": "抱团",
            "observe": "观察",
            "noise": "杂波",
        }
        return mapping.get(item, item)

    def _mainline_label_text(self, item: str) -> str:
        mapping = {
            "switch": "切换",
            "market": "主线",
            "emerging": "新发酵",
            "fading": "走弱",
            "-": "-",
        }
        return mapping.get(item, item)

    def _battle_text(self, battle: str) -> str:
        mapping = {
            "bullish": "进攻",
            "neutral": "均衡",
            "danger": "危险",
            "frozen": "冻结",
            "historical": "历史快照",
            "no_yest_limit_context": "无昨板样本",
            "-": "-",
            "": "-",
        }
        return mapping.get(battle, battle)

    def _regime_text(self, regime: str) -> str:
        mapping = {
            "attack": "进攻",
            "probe": "试错",
            "defense": "防守",
            "ice": "冰点",
            "await_anchor": "等待锚点",
            "carry_review": "承接复核",
            "stale_review": "滞后观察",
        }
        return mapping.get(regime, regime)

    def _source_text(self, source: str) -> str:
        mapping = {
            "stale_intraday_snapshot": "盘中滞后快照",
            "prev_close_snapshot": "昨收快照",
            "redis_runtime_projection": "全市场投影",
            "t1_v2_q2_live": "盘中实时快照",
            "startup_repair": "启动修复",
            "hot_plate_cache": "热板缓存",
            "yest_limit_cache": "昨日涨停缓存",
            "redis_anchor": "竞价锚点",
            "redis_0925": "09:25竞价锚点",
            "redis_preview_0920": "09:20预览锚点",
            "redis_preview_0924": "09:24预览锚点",
            "unknown": "未知",
            "-": "-",
        }
        return mapping.get(source, source)

    def _migration_text(self, migration: str) -> str:
        mapping = {
            "PERSIST": "延续",
            "EMERGING": "新发酵",
            "FADING": "兑现",
            "-": "-",
            "": "-",
        }
        return mapping.get(str(migration).upper(), migration)

    def _volume_text(self, volume: str) -> str:
        mapping = {
            "high": "放量",
            "flat": "平量",
            "low": "缩量",
            "": "-",
            "-": "-",
        }
        return mapping.get(volume, volume)

    def _close_verdict_text(self, verdict: str) -> str:
        mapping = {
            "strong_close": "强收盘",
            "mixed_close": "分歧收盘",
            "risk_close": "风险收盘",
            "weak_close": "弱收盘",
        }
        return mapping.get(verdict, verdict)

    def _bucket_text(self, row: AuctionPlateBucketStat) -> str:
        if row.expectation == "distribution":
            return "兑现"
        mapping = {
            "attack": "主攻题材",
            "follow": "跟随题材",
            "ladder": "高位梯队",
            "cluster": "抱团观察",
            "observe": "观察题材",
            "noise": "杂波题材",
        }
        return mapping.get(self.EXPECTATION_LABELS.get(row.expectation, row.expectation), "观察题材")

    def _reason_text(self, reason: str) -> str:
        mapping = {
            "highest board count inside the theme defines the strongest leader": "题材内板位最高，属于最强核心。",
            "theme participant but not leader": "属于题材内前排，但不是绝对龙头。",
            "within first echelon of the theme": "位于题材第一梯队，值得继续盯。",
            "historical snapshot only; wait for live auction flow before treating any candidate as actionable": "当前只有历史快照，等竞价实时流确认。",
            "wait for confirmation": "等待更强确认。",
        }
        mapping.setdefault("secondary participant inside theme", "属于题材跟风前排，更多看板块联动，不是核心龙头。")
        mapping.setdefault("not absolute leader but still inside front rank of theme", "属于题材前排，但辨识度弱于核心龙头。")
        mapping.setdefault("front rank candidate inside theme", "属于题材前排候选，强于后排，弱于核心。")
        return mapping.get(reason, reason)

    def _missing_text(self, item: str) -> str:
        if item == "hot_plates_today_missing":
            return "当日热板缺失(沿用昨日热板)"
        mapping = {
            "auction_anchor": "竞价锚点",
            "auction_anchor_pending": "竞价锚点待生成",
            "yest_limit_pool": "昨日涨停池",
            "hot_plates": "热点题材",
        }
        return mapping.get(item, item)

    @staticmethod
    def _is_limit_up_snapshot(snapshot: StockStateSnapshot) -> bool:
        return bool(snapshot.is_locked or snapshot.touched_limit_today)

    def _fmt_pct(self, value: float) -> str:
        if isinstance(value, float) and math.isnan(value):
            return "--"
        return f"{value * 100:+.1f}%"

    def _fmt_amount_yi(self, value: float) -> str:
        if value <= 0:
            return "-"
        return f"{value / 1e8:.0f}亿"

    def _fmt_amount_yi_precise(self, value: float) -> str:
        if value <= 0:
            return "-"
        return f"{value / 1e8:.2f}亿"

    def _fmt_amount_wan(self, value: float) -> str:
        if value <= 0:
            return "-"
        return f"{value / 1e4:.1f}万"

    def _fmt_amount_wan_precise(self, value: float) -> str:
        if value <= 0:
            return "-"
        return f"{value / 1e4:.2f}万"

    def _fmt_hot_rank(self, rank: int) -> str:
        return "-" if rank >= 999 else str(rank)

    def _fmt_net_inflow_yi(self, value: float) -> str:
        if abs(value) < 0.005:
            return "0.00亿"
        return f"{value:+.2f}亿"

    def _capital_behavior_text(self, value: float) -> str:
        if value >= 1.2:
            return "主动进攻"
        if value >= 0.35:
            return "偏强承接"
        if value <= -0.3:
            return "兑现流出"
        return "中性震荡"
    def _infer_close_verdict(self, summary) -> str:
        if summary.sentiment_score >= 6.0 and summary.headshot_rate <= 0.05:
            return "strong_close"
        if summary.sentiment_score >= 4.5:
            return "mixed_close"
        if summary.headshot_rate >= 0.12:
            return "risk_close"
        return "weak_close"
