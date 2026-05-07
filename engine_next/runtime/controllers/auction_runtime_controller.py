from __future__ import annotations

import json
import logging
import math
import re
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
    StockStateSnapshot,
)
from engine_next.runtime.intraday_data_hub import IntradayDataHub, IntradayFetchResult
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    choose_primary_plate,
    decode_theme_list,
    is_generic_plate,
    normalize_plate_name,
)
from engine_next.runtime.market_runtime_summary import MarketRuntimeSummaryResult, MarketRuntimeSummaryService
from engine_next.runtime.session_facts import build_session_facts
from engine_next.strategy_skill_layer.auction_plate_buckets import (
    AuctionPlateBucketStat,
    build_auction_plate_bucket_stats,
)
from engine_next.strategy_skill_layer.context_pipeline import (
    ContextStrategyBundle,
    build_context_strategy_bundle_for_symbols,
    filter_trade_candidates,
)
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
    candidate_scope: tuple[str, ...]
    candidate_scope_set: frozenset[str]
    actual_source: str
    plate_stats: tuple[AuctionPlateBucketStat, ...]
    bundle: ContextStrategyBundle | None
    candidates: tuple[AuctionLadderDecision, ...]
    missing_inputs: tuple[str, ...]
    snapshot_map: dict[str, StockStateSnapshot]
    stock_name_map: dict[str, str]
    plate_symbol_map: dict[str, tuple[str, ...]]
    decision_map: dict[str, AuctionLadderDecision]
    historical_only: bool = False
    stale_snapshot_only: bool = False
    frozen_postmarket_snapshot: bool = False


class AuctionRuntimeController:
    """Owns auction/opening/intraday strategy-console rendering."""

    AUCTION_TOP_AMOUNT_LIMIT = 1000
    OPENING_VALIDATION_TTL_SECONDS = 3 * 24 * 60 * 60
    OPENING_VALIDATION_TRUE_STRONG = "真强给机"
    OPENING_VALIDATION_GAP_WEAK = "高开转虚"
    OPENING_VALIDATION_HARD_TO_CHASE = "顶强难接"
    OPENING_VALIDATION_LOW_OPEN_STRONG = "低开真强"
    OPENING_VALIDATION_PULLBACK_REBOUND = "分歧回拉"
    OPENING_VALIDATION_UNDERTAKE_WEAK = "承接偏弱"
    OPENING_VALIDATION_PENDING = "强弱待判"
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
            phase_label="opening",
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
            phase_label="opening",
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
        rust: int,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        lines = [
            "运行事件=竞价接管",
            (
                f"运行状态={self._runtime_text(runtime_readiness_label)} "
                f"| 行情={quotes}/{symbols} "
                f"| Rust={rust} "
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
        rust: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        settling_mode = now.strftime("%H:%M:%S") < "09:25:10"
        preview_mode = now.strftime("%H:%M") < "09:25"
        if settling_mode:
            summary_lines = [f"runtime_readiness={self._runtime_text(runtime_readiness_label)} | quotes={quotes}/{symbols} | Rust={rust}"]
            if quote_freshness_line:
                summary_lines.append(quote_freshness_line)
            summary_lines.append("auction_anchor | status=waiting_finalization | earliest=09:25:10 | action=hold_formal_analysis")
            return tuple(summary_lines)
        if settling_mode:
            lines = [f"æ©æ„¯î”‘é˜èˆµâ‚¬?{self._runtime_text(runtime_readiness_label)} | ç›å±¾å„={quotes}/{symbols} | Rust={rust}"]
            if quote_freshness_line:
                lines.append(quote_freshness_line)
            lines.append("auction_anchor | status=waiting_finalization | earliest=09:25:10 | action=hold_formal_analysis")
            return tuple(lines)
        if intraday_context is None:
            lines = [
                f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Rust={rust}",
                "竞价预热上下文=加载中" if preview_mode else "竞价上下文=加载中",
            ]
            if quote_freshness_line:
                lines.insert(1, quote_freshness_line)
            return tuple(lines)
        lines = [f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Rust={rust}"]
        if quote_freshness_line:
            lines.append(quote_freshness_line)
        lines.extend(
            self.render_auction_preview_view(intraday_context, now=now)
            if preview_mode
            else self.render_auction_view(intraday_context)
        )
        return tuple(lines)

    def render_opening_runtime_loop(
        self,
        *,
        intraday_context: IntradayContext | None,
        runtime_readiness_label: str,
        symbols: int,
        quotes: int,
        rust: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        header = f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Rust={rust}"
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
        rust: int,
        now: datetime,
        quote_freshness_line: str | None = None,
    ) -> tuple[str, ...]:
        header = f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Rust={rust}"
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
                stale_snapshot_only=(runtime_readiness_label == "observe_runtime"),
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
        rust: int,
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
            f"| Rust={rust} "
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
        rust: int,
        now: datetime,
        quote_freshness_line: str | None = None,
        startup_report: StartupSelfCheckReport | None = None,
        historical_only: bool = False,
    ) -> tuple[str, ...]:
        header = f"运行状态={self._runtime_text(runtime_readiness_label)} | 行情={quotes}/{symbols} | Rust={rust}"
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
            startup_report=startup_report,
            historical_only=historical_only,
            stale_snapshot_only=stale_snapshot_only,
        )
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
                f"| 样本={len(state.candidate_scope)}"
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
            lines.extend(self._render_focus_pool(state, phase_label="postmarket"))
            lines.extend(self._render_risk_guard(state, phase_label="postmarket"))
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
            lines.extend(self._render_high_board_book(state, phase_label="postmarket"))
            lines.extend(self._render_ladder_map(state))
            lines.extend(self._render_tomorrow_plan(state))
            lines.extend(self._render_focus_pool(state, phase_label="postmarket"))
            lines.extend(self._render_risk_guard(state, phase_label="postmarket"))
            return tuple(lines)
        lines.extend(self._render_mainline_board(state))
        if phase_label in {"auction", "auction_preview"}:
            lines.extend(self._render_auction_thermo(state))
            lines.extend(self._render_auction_structure(state))
            lines.extend(self._render_yest_limit_feedback(state))
            lines.extend(self._render_yest_limit_breakdown(state))
            lines.extend(self._render_auction_plan(state))
        if phase_label == "opening":
            lines.extend(self._render_opening_validation(state))
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
        lines.extend(self._render_high_board_book(state, phase_label=detail_phase_label))
        if detail_phase_label in {"auction", "auction_preview"}:
            lines.extend(self._render_theme_zone(state))
        if detail_phase_label in {"auction", "auction_preview", "intraday", "postmarket"}:
            lines.extend(self._render_extreme_board(state, phase_label=detail_phase_label))
            lines.extend(self._render_rebound_board(state, phase_label=detail_phase_label))
        if detail_phase_label not in {"auction", "auction_preview"}:
            lines.extend(self._render_plate_heat(state))
            lines.extend(self._render_theme_internal_layers(state))
        lines.extend(self._render_ladder_map(state))
        if phase_label in {"auction", "auction_preview"}:
            lines.extend(self._render_auction_leader_watch(state))
            lines.extend(self._render_auction_execution_map(state))
        lines.extend(self._render_focus_pool(state, phase_label=detail_phase_label))
        lines.extend(self._render_risk_guard(state, phase_label=detail_phase_label))
        return tuple(lines)

    def _build_console_state(
        self,
        intraday_context: IntradayContext,
        *,
        min_confidence: int,
        phase_label: str,
        startup_report: StartupSelfCheckReport | None = None,
        historical_only: bool = False,
        stale_snapshot_only: bool = False,
    ) -> StrategyConsoleState:
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
        candidate_scope = self._build_candidate_scope(intraday_context, snapshot_map=snapshot_map)
        candidate_scope_set = frozenset(candidate_scope)
        actual_source = self._infer_actual_source(
            intraday_context,
            candidate_scope,
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
            symbols=candidate_scope,
            top_n=5,
        )
        bundle = None
        candidates: tuple[AuctionLadderDecision, ...] = ()
        decision_map: dict[str, AuctionLadderDecision] = {}
        if candidate_scope:
            bundle = build_context_strategy_bundle_for_symbols(intraday_context, symbols=candidate_scope)
            candidates = filter_trade_candidates(bundle, min_confidence=min_confidence)
            decision_map = {decision.symbol: decision for decision in bundle.decisions}
        return StrategyConsoleState(
            context=intraday_context,
            candidate_scope=candidate_scope,
            candidate_scope_set=candidate_scope_set,
            actual_source=actual_source,
            plate_stats=plate_stats,
            bundle=bundle,
            candidates=candidates,
            missing_inputs=missing_inputs,
            snapshot_map=snapshot_map,
            stock_name_map=stock_name_map,
            plate_symbol_map={plate_name: tuple(symbols) for plate_name, symbols in plate_symbol_index.items()},
            decision_map=decision_map,
            historical_only=historical_only,
            stale_snapshot_only=(stale_snapshot_only or frozen_postmarket_snapshot),
            frozen_postmarket_snapshot=frozen_postmarket_snapshot,
        )

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
        try:
            value = float(raw or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if math.isnan(value):
            return value
        if abs(value) > 1.5:
            value /= 100.0
        return value

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
        score = summary.sentiment_score
        historical_mode = self._is_historical_mode(state, phase_label=phase_label)
        battle = "历史快照" if historical_mode else self._battle_text(summary.battle_status or "-")
        regime = self._infer_regime_stage(summary, state, phase_label=phase_label)
        pos_cap = self._infer_position_cap(summary, state, phase_label=phase_label)
        allow_setups = self._collect_allowed_setups(state, phase_label=phase_label)
        banned_actions = self._collect_banned_actions(state, phase_label=phase_label)
        source = self._display_source_label(state, phase_label=phase_label)
        return (
            f"情绪总览 | 情绪分={score:.1f}/10 "
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

    def _render_mainline_board(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        top = state.plate_stats[0] if state.plate_stats else None
        second = next((row for row in state.plate_stats[1:] if not row.generic), None)
        main_name = summary.mainline_sector or summary.top_plate_name or (top.plate_name if top else "-")
        main_expect = self._infer_market_mainline_label(summary, main_name)
        secondary = summary.top_plate_name if summary.top_plate_name and summary.top_plate_name != main_name else "-"
        scope_lead = top.plate_name if top else "-"
        scope_expect = self._expectation_text(self.EXPECTATION_LABELS.get(top.expectation, top.expectation)) if top else "-"
        scope_secondary = second.plate_name if second else "-"
        top_turnover = ", ".join(self._snapshot_name_by_symbol_compact(state, symbol) for symbol in summary.top_turnover_symbols[:3]) or "-"
        volume_pred = self._fmt_amount_yi(summary.market_predicted_full_day_amount)
        switch_badge = "⇄" if summary.mainline_switch else "→"
        hot_plate_mode = self._hot_plate_render_mode(state)
        if hot_plate_mode != "today":
            hot_plate_note = self._hot_plate_note(state)
            return (
                "【主线脉络】摘要 | 内容",
                f"  {switch_badge} 主线/副线 | {main_name}:{self._mainline_label_text(main_expect)} / {secondary}",
                f"  ★ 题材主攻/次强 | -- / -- ({hot_plate_note})",
                "  ◇ 是否切换/迁移 | -- / --",
                "  ￥ 板块涨幅/净流入 | -- / --",
                f"  ◎ 量能/成交核心 | {self._volume_text(summary.market_volume_level)}@{volume_pred} / {top_turnover}",
            )
        return (
            "【主线脉络】摘要 | 内容",
            f"  {switch_badge} 主线/副线 | {main_name}:{self._mainline_label_text(main_expect)} / {secondary}",
            f"  ★ 题材主攻/次强 | {scope_lead}:{scope_expect} / {scope_secondary}",
            f"  ◇ 是否切换/迁移 | {'是' if summary.mainline_switch else '否'} / {self._migration_text(summary.top_plate_migration_type or '-')}",
            f"  ￥ 板块涨幅/净流入 | {summary.top_sector_pct:.2f}% / {summary.mainline_net_inflow_yi:.2f}亿",
            f"  ◎ 量能/成交核心 | {self._volume_text(summary.market_volume_level)}@{volume_pred} / {top_turnover}",
        )

    def _render_auction_thermo(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        hot_plate_mode = self._hot_plate_render_mode(state)
        feedback_ready = self._feedback_metrics_ready(state)
        resonance_marker = self._resonance_marker(summary.resonance_score) if hot_plate_mode == "today" else "?"
        resonance_text = f"{summary.resonance_score:.2f}" if hot_plate_mode == "today" else "--"
        score_marker = self._score_marker(summary.sentiment_score) if feedback_ready else "?"
        score_text = f"{summary.sentiment_score:.1f}/10" if feedback_ready else "--"
        battle_marker = self._battle_marker(summary.battle_status or "-") if feedback_ready else "?"
        battle_text = self._battle_text(summary.battle_status or "-") if feedback_ready else "--"
        promotion_marker = self._promotion_marker(summary.promotion_rate) if feedback_ready else "?"
        promotion_text = f"{summary.promotion_rate:.1%}" if feedback_ready else "--"
        red_open_marker = self._red_open_marker(summary.red_open_rate) if feedback_ready else "?"
        red_open_text = f"{summary.red_open_rate:.1%}" if feedback_ready else "--"
        headshot_marker = self._headshot_marker(summary.headshot_rate) if feedback_ready else "?"
        headshot_text = f"{summary.headshot_rate:.1%}" if feedback_ready else "--"
        return (
            "【竞价总览】指标 | 数值",
            f"  {score_marker} 情绪分 | {score_text}",
            f"  {battle_marker} 对局 | {battle_text}",
            f"  {promotion_marker} 晋级率 | {promotion_text}",
            f"  {red_open_marker} 红开率 | {red_open_text}",
            f"  {headshot_marker} 核按钮率 | {headshot_text}",
            f"  {resonance_marker} 共振分 | {resonance_text}",
        )

    def _render_auction_structure(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        hot_plate_mode = self._hot_plate_render_mode(state)
        auction_ready = self._auction_anchor_ready(state)
        yest_limit_ready = self._yest_limit_ready(state)
        if hot_plate_mode == "today":
            hot_plate_text = str(summary.hot_plate_count)
            migration_text = (
                f"{summary.persistent_plate_count}/{summary.emerging_plate_count}/{summary.fading_plate_count}"
            )
        elif hot_plate_mode == "fallback":
            hot_plate_text = f"{len(state.context.yesterday_hot_plate_map)}(沿用昨日)"
            migration_text = "--/--/--"
        else:
            hot_plate_text = "--"
            migration_text = "--/--/--"
        return (
            "【竞价结构】指标 | 数值",
            f"  ￥ 全市场竞价额 | {self._fmt_amount_yi_precise(summary.market_full_auc_amt) if auction_ready else '--'}",
            f"  ￥ 核心样本竞价额 | {self._fmt_amount_yi_precise(summary.context_auc_amt) if auction_ready else '--'}",
            f"  ◎ 平均承接 | {self._fmt_amount_wan(summary.avg_bid_amt) if auction_ready else '--'}",
            f"  ◇ 昨涨停样本 | {summary.total_yest_limit_count if yest_limit_ready else '--'}",
            f"  ◇ 热门题材数 | {hot_plate_text}",
            f"  ⇄ 延续/新发酵/兑现 | {migration_text}",
        )

    def _render_auction_attack_map(self, state: StrategyConsoleState) -> tuple[str, ...]:
        if not state.plate_stats:
            return ("【竞价攻击图】暂无题材样本",)
        rows = ["【竞价攻击图】定位 | 题材 | 强度 | 竞价额 | 龙头数 | 昨板 | 主力净额 | 资金定性 | 代表"]
        for row in state.plate_stats[:3]:
            representative = self._snapshot_name_by_symbol(state, row.sample_symbols[0]) if row.sample_symbols else "-"
            rows.append(
                "  "
                f"{self._bucket_text(row)}"
                f" | {row.plate_name}"
                f" | {row.weighted_score:.1f}"
                f" | {self._fmt_amount_yi_precise(row.auction_amount)}"
                f" | {row.leader_count}"
                f" | {row.yest_limit_count}"
                f" | {self._fmt_net_inflow_yi(row.hot_net_inflow_yi)}"
                f" | {self._capital_behavior_text(row.hot_capital_behavior)}"
                f" | {representative}"
            )
        return tuple(rows)

    def _render_theme_zone(self, state: StrategyConsoleState) -> tuple[str, ...]:
        ranked_rows = [row for row in state.plate_stats if not row.generic][:4]
        if not ranked_rows:
            ranked_rows = list(state.plate_stats[:4])
        if not ranked_rows:
            return ("【题材区】暂无题材样本",)
        rows = ["【题材区】定位 | 题材 | 竞价额 | 昨板 | 龙头数 | 前排 | 观察"]
        for row in ranked_rows:
            leader, assist, follower = self._theme_internal_names(state, row.plate_name)
            front = " ; ".join(name for name in (leader, assist, follower) if name and name != "-") or "-"
            rows.append(
                "  "
                f"{self._plate_role_text(row)}"
                f" | {row.plate_name}"
                f" | {self._fmt_amount_yi_precise(row.auction_amount)}"
                f" | {row.yest_limit_count}"
                f" | {row.leader_count}"
                f" | {front}"
                f" | {self._theme_layer_comment(state, row)}"
            )
        return tuple(rows)

    def _render_yest_limit_feedback(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        feedback_ready = self._feedback_metrics_ready(state)
        verdict = "今日机会" if summary.promotion_rate >= 0.35 and summary.headshot_rate <= 0.08 else (
            "昨日兑现" if summary.headshot_rate >= 0.12 or summary.promotion_rate <= 0.15 else "分歧观察"
        )
        trade_env = self._yest_limit_trade_env(summary)
        opportunity_label, opportunity_action = self._yest_limit_opportunity_profile(summary)
        premium_label, premium_action = self._yest_limit_premium_profile(summary)
        risk_label, risk_action = self._yest_limit_risk_profile(summary)
        hot_plate_mode = self._hot_plate_render_mode(state)
        if hot_plate_mode == "fallback":
            verdict_note = "当日热板缺失，先看昨日涨停反馈，不判主攻切换"
        elif hot_plate_mode == "missing":
            verdict_note = "热点题材缺失，先看昨日涨停反馈，不判主攻切换"
        else:
            verdict_note = "先看中位还是先防兑现一眼能看懂"
        if not feedback_ready:
            if not self._auction_anchor_ready(state) and not self._yest_limit_ready(state):
                verdict_note = "竞价锚点和昨日涨停池未就绪，先不判断接力环境"
            elif not self._auction_anchor_ready(state):
                verdict_note = "竞价锚点未就绪，先不判断红开溢价和核按钮风险"
            else:
                verdict_note = "昨日涨停池未就绪，先不判断接力环境"
            return (
                "【昨日涨停反馈】维度 | 数值 | 交易解读",
                "  机会面 | 晋级率 -- | 样本不足，先不判断接力机会",
                "  溢价面 | 红开率 -- | 样本不足，先不判断高开溢价",
                "  风险面 | 核按钮率 -- | 样本不足，先不判断负反馈强弱",
                f"  环境结论 | -- / -- | 样本 --，{verdict_note}",
            )
        return (
            "【昨日涨停反馈】维度 | 数值 | 交易解读",
            f"  机会面 | 晋级率 {summary.promotion_rate:.1%} | {opportunity_label}，{opportunity_action}",
            f"  溢价面 | 红开率 {summary.red_open_rate:.1%} | {premium_label}，{premium_action}",
            f"  风险面 | 核按钮率 {summary.headshot_rate:.1%} | {risk_label}，{risk_action}",
            f"  环境结论 | {trade_env} / {verdict} | 样本 {summary.total_yest_limit_count}，{verdict_note}",
        )

    def _render_auction_plan(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        hot_plate_mode = self._hot_plate_render_mode(state)
        if hot_plate_mode == "fallback":
            plan = "当日热板缺失，先看昨日热板延续与高标承接，不判主攻切换。"
            style = "观察盘"
        elif hot_plate_mode == "missing":
            plan = "热点题材缺失，先看昨日涨停反馈与高标承接，不判主攻切换。"
            style = "观察盘"
        elif summary.headshot_rate >= 0.12:
            plan = "更像昨日兑现盘，只盯核心龙头是否超预期，不接后排扩散。"
            style = "兑现盘"
        elif summary.mainline_switch and summary.emerging_plate_count >= summary.persistent_plate_count:
            plan = "更像今日新机会盘，先盯新题材前排与一进二承接，等开盘确认再动手。"
            style = "新机会"
        elif summary.persistent_plate_count > summary.emerging_plate_count:
            plan = "更像老主线延续盘，只做核心回流，不做杂毛补涨。"
            style = "延续盘"
        else:
            plan = "更像题材切换试错盘，先看竞价最强桶能否带动高位承接。"
            style = "试错盘"
        return (
            "【竞价预案】维度 | 内容",
            f"  盘面归类 | {style}",
            f"  操作预案 | {plan}",
        )

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

    def _compute_recap_feedback_metrics(self, state: StrategyConsoleState, *, phase_label: str) -> dict[str, object]:
        ref = self._load_recap_reference(state, phase_label=phase_label)
        yest_limit_map = ref["yest_limit_map"]
        assert isinstance(yest_limit_map, dict)
        auction_map = ref["auction_map"]
        assert isinstance(auction_map, dict)
        total = len(yest_limit_map)
        matched = 0
        auction_sample_matched = 0
        promoted_count = 0
        red_open_count = 0
        headshot_count = 0
        for symbol in yest_limit_map.keys():
            snapshot = state.snapshot_map.get(symbol)
            if snapshot is None:
                continue
            matched += 1
            if snapshot.is_locked or snapshot.current_pct >= 0.098:
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
        promoted = [
            snapshot
            for symbol in yest_limit_map.keys()
            for snapshot in (state.snapshot_map.get(symbol),)
            if snapshot is not None and (snapshot.is_locked or snapshot.current_pct >= 0.098)
        ]
        promoted.sort(key=lambda item: (-item.lb_days, -item.current_pct, -item.auction_amount))
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
        opening_payload = persisted_opening or self._build_opening_validation_payload(state)
        if persisted_opening:
            strong = tuple(str(item) for item in persisted_opening.get("strong", ()) if str(item))
            weak = tuple(str(item) for item in persisted_opening.get("weak", ()) if str(item))
            rebound = tuple(str(item) for item in persisted_opening.get("rebound", ()) if str(item))
        else:
            strong = self._pick_auction_outcome_names(
                state,
                predicate=lambda snapshot: snapshot.open_pct >= 0.02 and snapshot.current_pct >= 0.098,
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
            verdict = "半对半错"
            adjust = (
                f"竞价强桶与收盘主线仍重合在 {','.join(overlap)}，"
                "但开盘验证偏弱，明天要把高开转虚和承接偏弱放在更前面过滤。"
            )
        elif overlap:
            verdict = "基本正确"
            if validation_score["positive"] > 0:
                adjust = f"竞价强桶与收盘主线重合在 {','.join(overlap)}，且开盘验证有正反馈，明天继续盯核心龙头与前排承接。"
            else:
                adjust = f"竞价强桶与收盘主线重合在 {','.join(overlap)}，明天继续盯核心龙头与前排承接。"
        elif validation_score["positive"] > 0 or plate_support or hot_plate_support:
            verdict = "半对半错"
            if plate_support:
                adjust = f"竞价强桶本身不稳，但题材验证仍指向 {','.join(dict.fromkeys(plate_support[:2]))}，明天应提高热板延续和涨停成队权重。"
            elif validation_score["positive"] > 0:
                adjust = "竞价强桶未直接命中收盘主线，但开盘验证有真强样本，明天应提高开盘兑现和题材成队的联合权重。"
            else:
                adjust = "前日热板延续比竞价瞬时量更有效，明天应提高热板延续和涨停成队权重。"
        else:
            verdict = "明显偏离"
            adjust = "竞价不能只看大额单，明天要把前日热板、前日涨停晋级链和题材成队一起校验。"
        return (
            "【预案复核】维度 | 结论",
            f"  竞价强桶 | {', '.join(auction_leads) or '-'}",
            f"  收盘主线 | {', '.join(dict.fromkeys(final_leads[:3])) or '-'}",
            f"  开盘兑现 | {' ; '.join(opening_feedback_parts) or '-'}",
            f"  开盘预案 | {' ; '.join(validated) or '-'}",
            f"  题材验证 | {' ; '.join(plate_checks[:2]) or '-'}",
            f"  预案判断 | {verdict}",
            f"  调整建议 | {adjust}",
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

    def _render_recap_ladder_recap(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        metrics = self._compute_recap_feedback_metrics(state, phase_label=phase_label)
        high_board_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.lb_days >= 3)
        yest_limit_count = int(metrics["sample_total"])
        locked_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.is_locked)
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
                f"  盘中最强 | -- ({hot_plate_note})",
                "  是否切换/迁移 | -- / --",
                "  延续/新发酵/兑现 | --/--/--",
            )
        lead = summary.mainline_sector or summary.top_plate_name or (state.plate_stats[0].plate_name if state.plate_stats else "-")
        secondary = summary.top_plate_name if summary.top_plate_name and summary.top_plate_name != lead else "-"
        scope_lead = state.plate_stats[0].plate_name if state.plate_stats else "-"
        limitup_lead, limitup_secondary = self._summarize_limitup_mainline(state)
        return (
            "【主线复盘】维度 | 内容",
            f"  主线/副线 | {lead} / {secondary}",
            f"  涨停主线/次主线 | {limitup_lead} / {limitup_secondary}",
            f"  盘中最强 | {scope_lead}",
            f"  是否切换/迁移 | {'是' if summary.mainline_switch else '否'} / {self._migration_text(summary.top_plate_migration_type or '-')}",
            f"  延续/新发酵/兑现 | {summary.persistent_plate_count}/{summary.emerging_plate_count}/{summary.fading_plate_count}",
        )

    def _render_ladder_recap(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        high_board_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.lb_days >= 3)
        yest_limit_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.is_yest_limit)
        locked_count = sum(1 for snapshot in state.snapshot_map.values() if snapshot.is_locked)
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
            f"  ◇ 昨日涨停 | {yest_limit_count}",
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
            primary = "若龙头分歧但主线未死，只看回封与承接，不盲追后排。"
        else:
            primary = "若负反馈继续扩散，缩到观察名单，等新的低风险信号。"
        if summary.mainline_switch:
            secondary = "若题材切换继续，老主线以冲高兑现为主，不再新增进攻。"
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
        lead = summary.mainline_sector or summary.top_plate_name or (state.plate_stats[0].plate_name if state.plate_stats else "-")
        secondary = summary.top_plate_name if summary.top_plate_name and summary.top_plate_name != lead else "-"
        scope_lead = state.plate_stats[0].plate_name if state.plate_stats else lead
        open_text = f"红开率 {summary.red_open_rate:.1%}，{self._auction_outcome_summary(state)}" if feedback_ready else "红开率 --，竞价反馈样本不足"
        close_text = (
            f"{self._close_verdict_text(verdict)}，晋级率 {summary.promotion_rate:.1%}，核按钮率 {summary.headshot_rate:.1%}"
            if feedback_ready
            else "--，晋级率 --，核按钮率 --"
        )
        return (
            "【全天回放】阶段 | 结论",
            f"  竞价开局 | {open_text}",
            f"  盘中主线 | {scope_lead}最强，主线/副线围绕 {lead} / {secondary} 演绎，{'发生切换' if summary.mainline_switch else '整体延续'}",
            f"  收盘结果 | {close_text}",
        )

    def _render_today_hot_plates(self, state: StrategyConsoleState) -> tuple[str, ...]:
        hot_plate_mode = self._hot_plate_render_mode(state)
        if hot_plate_mode != "today":
            return (f"【今日热点】{self._hot_plate_note(state)}，不输出今日强度/涨幅/净流入真值",)
        if not state.plate_stats:
            return ("【今日热点】暂无题材样本",)
        rows = ["【今日热点】题材 | 强度 | 涨幅 | 净流入 | 资金定性 | 代表"]
        for row in state.plate_stats[:4]:
            representative = self._snapshot_name_by_symbol_compact(state, row.sample_symbols[0]) if row.sample_symbols else "-"
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
            if not (snapshot.touched_limit_today or snapshot.is_locked or snapshot.current_pct >= 0.098):
                continue
            plate = self._display_plate_name(snapshot, prefer_high_board=True)
            if not plate or plate == "-":
                continue
            plate_rows[plate].append(snapshot)
        if not plate_rows:
            return ("【涨停板块】暂无涨停板块样本",)
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
        return "单点表现"

    def _normalize_limitup_truth_lb_days(self, raw: object) -> int:
        try:
            value = int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            value = 1
        return max(value, 1)

    def _format_limitup_board_height(self, lb_days: int) -> str:
        return "首板" if lb_days <= 1 else f"{lb_days}板"

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

    def _render_auction_outcome(self, state: StrategyConsoleState) -> tuple[str, ...]:
        strong = self._pick_auction_outcome_names(
            state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.02 and snapshot.current_pct >= 0.098,
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

    def _render_opening_validation(self, state: StrategyConsoleState) -> tuple[str, ...]:
        payload = self._build_opening_validation_payload(state)
        strong = tuple(str(item) for item in payload.get("strong", ()) if str(item))
        weak = tuple(str(item) for item in payload.get("weak", ()) if str(item))
        rebound = tuple(str(item) for item in payload.get("rebound", ()) if str(item))
        validated = tuple(str(item) for item in payload.get("validated", ()) if str(item))
        plate_checks = tuple(str(item) for item in payload.get("plate_checks", ()) if str(item))
        return (
            "【开盘验证】维度 | 结果",
            f"  强开兑现 | {', '.join(strong) or '-'}",
            f"  高开转虚 | {', '.join(weak) or '-'}",
            f"  低开转强 | {', '.join(rebound) or '-'}",
            f"  预案兑现 | {' ; '.join(validated) or '-'}",
            f"  题材验证 | {' ; '.join(plate_checks) or '-'}",
        )

    def _build_opening_validation_payload(
        self,
        state: StrategyConsoleState,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]:
        eval_state = state
        if not state.candidate_scope_set and state.snapshot_map:
            all_symbols = tuple(state.snapshot_map.keys())
            eval_state = replace(
                state,
                candidate_scope=all_symbols,
                candidate_scope_set=frozenset(all_symbols),
            )
        strong = self._pick_auction_outcome_names(
            eval_state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.02 and snapshot.current_pct >= 0.098,
        )
        weak = self._pick_auction_outcome_names(
            eval_state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.03 and snapshot.current_pct <= snapshot.open_pct - 0.05,
        )
        rebound = self._pick_auction_outcome_names(
            eval_state,
            predicate=lambda snapshot: snapshot.open_pct < 0.0 and snapshot.current_pct >= 0.05,
        )
        validated: list[str] = []
        for decision in eval_state.candidates[:5]:
            snapshot = eval_state.snapshot_map.get(decision.symbol)
            if snapshot is None:
                continue
            validated.append(f"{self._decision_name_compact(eval_state, decision)}={self._leader_truth_label(snapshot)}")
            if len(validated) >= 3:
                break
        plate_checks: list[str] = []
        for row in eval_state.plate_stats[:3]:
            representative = self._snapshot_name_by_symbol_compact(eval_state, row.sample_symbols[0]) if row.sample_symbols else "-"
            plate_checks.append(
                f"{row.plate_name}(额{self._fmt_amount_yi_precise(row.auction_amount)}/竞价样本{row.auction_symbol_count}/昨板{row.yest_limit_count}/代表{representative})"
            )
        return {
            "trade_date": eval_state.context.trade_date,
            "phase": eval_state.context.phase.value,
            "updated_at": (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at_ts": int((now or datetime.now()).timestamp()),
            "strong": list(strong),
            "weak": list(weak),
            "rebound": list(rebound),
            "validated": validated,
            "plate_checks": plate_checks,
        }

    def _load_opening_validation_payload(self, trade_date: str) -> dict[str, object]:
        if not hasattr(self, "_intraday_hub") or getattr(self, "_intraday_hub", None) is None:
            return {}
        key = self._opening_validation_redis_key(trade_date)
        try:
            raw = self._intraday_hub.redis.get(key)
        except Exception:
            return {}
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _render_high_board_book(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        selected: list[StockStateSnapshot] = []
        top_board = 0
        buy1_king_symbol = ""
        buy1_king_score = float("-inf")
        historical_mode = phase_label == "premarket" and state.historical_only
        for snapshot in state.snapshot_map.values():
            if snapshot.symbol not in state.candidate_scope_set:
                continue
            if snapshot.lb_days < 2 and not snapshot.is_yest_limit:
                continue
            selected.append(snapshot)
            if snapshot.lb_days > top_board:
                top_board = snapshot.lb_days
            if not historical_mode and snapshot.volume_intensity > buy1_king_score:
                buy1_king_score = snapshot.volume_intensity
                buy1_king_symbol = snapshot.symbol
        if not selected:
            return ("【高位梯队】暂无高位样本",)
        ranked = nlargest(
            4,
            selected,
            key=lambda snapshot: (
                snapshot.lb_days,
                -snapshot.leader_rank_in_theme,
                snapshot.current_pct,
                snapshot.auction_amount,
            ),
        )
        rows = ["【高标生死簿】标的(题材) | 梯队 | 溢价(竞) | 现价(实) | 状态 | 买一承接 | 特征 | 动作"]
        for snapshot in ranked:
            decision = state.decision_map.get(snapshot.symbol)
            action = self._display_action_label(decision, state, phase_label=phase_label) if decision else "只观察"
            plate = self._display_plate_name(snapshot, prefer_high_board=True)
            rows.append(
                "  "
                f"{self._short_stock_name(snapshot)}({plate})"
                f" | {self._high_board_ladder_text(snapshot)}"
                f" | {self._high_board_open_text(snapshot, phase_label=phase_label, historical_only=state.historical_only)}"
                f" | {self._fmt_pct(snapshot.current_pct)}"
                f" | {self._high_board_state_label(snapshot, phase_label=phase_label, historical_only=state.historical_only)}"
                f" | {self._high_board_buy1_text(snapshot, phase_label=phase_label, historical_only=state.historical_only)}"
                f" | {self._high_board_feature_tags(snapshot, top_board=top_board, buy1_king_symbol=buy1_king_symbol, historical_only=state.historical_only)}"
                f" | {action}"
            )
        return tuple(rows)

    def _auction_outcome_summary(self, state: StrategyConsoleState) -> str:
        strong = self._pick_auction_outcome_names(
            state,
            predicate=lambda snapshot: snapshot.open_pct >= 0.02 and snapshot.current_pct >= 0.098,
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
        parts: list[str] = []
        if strong:
            parts.append("强开兑现看 " + "、".join(strong))
        if weak:
            parts.append("高开转虚看 " + "、".join(weak))
        if rebound:
            parts.append("低开转强看 " + "、".join(rebound))
        return "；".join(parts) if parts else "竞价更多是分歧筛选，没有形成一致强开胜局"

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
                if snapshot.symbol in state.candidate_scope_set and predicate(snapshot)
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
        return "单点表现"

    def _high_board_ladder_text(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.is_yest_limit and snapshot.lb_days >= 1:
            return f"{max(snapshot.lb_days - 1, 0)}->{snapshot.lb_days}B"
        return f"{snapshot.lb_days}B"

    def _high_board_open_text(self, snapshot: StockStateSnapshot, *, phase_label: str, historical_only: bool) -> str:
        if phase_label == "premarket" and historical_only:
            return "待竞价"
        return self._fmt_pct(snapshot.open_pct)

    def _high_board_state_label(self, snapshot: StockStateSnapshot, *, phase_label: str, historical_only: bool) -> str:
        if phase_label == "premarket" and historical_only:
            if snapshot.current_pct >= 0.098:
                return "昨收封板"
            if snapshot.current_pct >= 0.05:
                return "昨收强势"
            if snapshot.current_pct > 0:
                return "昨收分歧"
            return "昨收回落"
        if snapshot.is_locked or snapshot.current_pct >= 0.098:
            return "封板"
        if snapshot.open_pct >= 0.08 and snapshot.current_pct < snapshot.open_pct - 0.02:
            return "炸板"
        if snapshot.current_pct < 0.0:
            return "回落"
        if snapshot.current_pct < snapshot.open_pct - 0.02:
            return "分歧"
        return "承接"

    def _high_board_buy1_text(self, snapshot: StockStateSnapshot, *, phase_label: str, historical_only: bool) -> str:
        if phase_label == "premarket" and historical_only:
            return "待盘口"
        return self._leader_seal_quality(snapshot)

    def _high_board_feature_tags(self, snapshot: StockStateSnapshot, *, top_board: int, buy1_king_symbol: str, historical_only: bool) -> str:
        tags: list[str] = []
        if snapshot.lb_days == top_board and top_board > 0:
            tags.append("[最高标]")
        if buy1_king_symbol and snapshot.symbol == buy1_king_symbol and snapshot.volume_intensity >= 2.5:
            tags.append("[买一最强]")
        if snapshot.leader_rank_in_theme <= 1:
            tags.append("[题材先锋]")
        if snapshot.current_pct >= 0.098:
            tags.append("[昨收封板]" if historical_only else "[封板]")
        elif snapshot.current_pct < snapshot.open_pct - 0.03:
            tags.append("[分歧回落]")
        elif snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03:
            tags.append("[低开转强]")
        if snapshot.market_cap_yi >= 300 or snapshot.amount_day_yi >= 40:
            tags.append("[容量票]")
        return "".join(tags[:3]) or "[观察]"

    def _render_plate_heat(self, state: StrategyConsoleState) -> tuple[str, ...]:
        if not state.plate_stats:
            return ("【题材分桶】暂无题材样本",)
        rows = ["【题材分桶】定位 | 题材 | 题材强度 | 涨幅 | 主力净额 | 资金定性 | 竞价额 | 昨板 | 机会类型 | 下手层 | 情绪先锋"]
        for row in state.plate_stats[:4]:
            leader = self._snapshot_name_by_symbol_compact(state, row.sample_symbols[0]) if row.sample_symbols else "-"
            theme_state, trade_state, _ = self._theme_trade_profile(row)
            rows.append(
                "  "
                f"{self._plate_role_text(row)}"
                f" | {row.plate_name}"
                f" | {row.weighted_score:.1f}"
                f" | {row.hot_change_pct:+.1f}%"
                f" | {self._fmt_net_inflow_yi(row.hot_net_inflow_yi)}"
                f" | {self._capital_behavior_text(row.hot_capital_behavior)}"
                f" | {self._fmt_amount_yi_precise(row.auction_amount)}"
                f" | {row.yest_limit_count}"
                f" | {theme_state}"
                f" | {trade_state}"
                f" | {leader}"
            )
        return tuple(rows)

    def _render_theme_internal_layers(self, state: StrategyConsoleState) -> tuple[str, ...]:
        if not state.plate_stats:
            return ("【题材层级】暂无题材样本",)
        rows = ["【题材层级】题材 | 情绪先锋 | 前排助攻 | 跟风前排 | 层级判断"]
        for row in state.plate_stats[:4]:
            leader, assist, follower = self._theme_internal_names(state, row.plate_name)
            rows.append(
                "  "
                f"{row.plate_name}"
                f" | {leader}"
                f" | {assist}"
                f" | {follower}"
                f" | {self._theme_layer_comment(state, row)}"
            )
        return tuple(rows)

    def _render_extreme_board(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        snapshots = nlargest(
            4,
            (
                snapshot
                for snapshot in state.snapshot_map.values()
                if snapshot.symbol in state.candidate_scope_set and (snapshot.auction_amount > 0 or snapshot.amount_2m > 0 or snapshot.lb_days >= 1)
            ),
            key=lambda snapshot: (
                self._extreme_score(snapshot),
                snapshot.auction_amount,
                snapshot.amount_2m,
                -snapshot.leader_rank_in_theme,
            ),
        )
        if not snapshots:
            return ("\u3010\u7ade\u4ef7\u6781\u503c\u699c\u3011\u6682\u65e0\u6781\u503c\u6837\u672c",)
        rows: list[str] = []
        if phase_label == "intraday" and state.stale_snapshot_only:
            rows.append("\u3010\u7ade\u4ef7\u6781\u503c\u699c\u3011\u57fa\u4e8e\u76d8\u4e2d\u6ede\u540e\u5feb\u7167\uff0c\u4ec5\u4f9b\u590d\u76d8\u53c2\u8003")
        rows.append("\u3010\u7ade\u4ef7\u6781\u503c\u699c\u3011\u4e2a\u80a1 | \u6781\u503c\u7c7b\u578b | \u7ade\u4ef7\u6da8\u8dcc | \u73b0\u6da8\u8dcc | \u7ade\u4ef7\u989d | \u524d2\u5206\u91d1\u989d | \u9898\u6750 | \u4e0a\u8f66\u7ed3\u8bba")
        for snapshot in snapshots:
            rows.append(
                "  "
                f"{self._short_stock_name(snapshot)}"
                f" | {self._extreme_type_label(snapshot)}"
                f" | {self._fmt_pct(snapshot.open_pct)}"
                f" | {self._fmt_pct(snapshot.current_pct)}"
                f" | {self._fmt_amount_yi_precise(snapshot.auction_amount)}"
                f" | {self._fmt_amount_yi_precise(snapshot.amount_2m)}"
                f" | {self._display_plate_name(snapshot, prefer_high_board=True)}"
                f" | {self._entry_window_label(snapshot, phase_label=phase_label)}"
            )
        return tuple(rows)

    def _render_rebound_board(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        snapshots = nlargest(
            4,
            (
                snapshot
                for snapshot in state.snapshot_map.values()
                if snapshot.symbol in state.candidate_scope_set
                and (
                    snapshot.amount_2m >= 20_000_000
                    or (snapshot.open_pct <= 0.01 and snapshot.current_pct > 0.0)
                    or (snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0)
                )
            ),
            key=lambda snapshot: (
                self._rebound_score(snapshot),
                -snapshot.leader_rank_in_theme,
                snapshot.amount_2m,
                snapshot.current_pct,
            ),
        )
        if not snapshots:
            return ("\u3010\u627f\u63a5\u8f6c\u5f3a\u699c\u3011\u6682\u65e0\u627f\u63a5\u6837\u672c",)
        rows: list[str] = []
        if phase_label == "intraday" and state.stale_snapshot_only:
            rows.append("\u3010\u627f\u63a5\u8f6c\u5f3a\u699c\u3011\u57fa\u4e8e\u76d8\u4e2d\u6ede\u540e\u5feb\u7167\uff0c\u4ec5\u4f9b\u590d\u76d8\u53c2\u8003")
        rows.append("\u3010\u627f\u63a5\u8f6c\u5f3a\u699c\u3011\u4e2a\u80a1 | \u673a\u4f1a\u6807\u7b7e | \u7ade\u4ef7\u6da8\u8dcc | \u73b0\u6da8\u8dcc | \u524d2\u5206\u91d1\u989d | \u9898\u6750 | \u8bc1\u636e")
        for snapshot in snapshots:
            rows.append(
                "  "
                f"{self._short_stock_name(snapshot)}"
                f" | {self._rebound_type_label(snapshot)}"
                f" | {self._fmt_pct(snapshot.open_pct)}"
                f" | {self._fmt_pct(snapshot.current_pct)}"
                f" | {self._fmt_amount_yi_precise(snapshot.amount_2m)}"
                f" | {self._display_plate_name(snapshot, prefer_high_board=True)}"
                f" | {self._focus_evidence(snapshot, phase_label=phase_label)}"
            )
        return tuple(rows)

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

        if state.context.session_facts.ladder_facts:
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
            promoted_count = sum(1 for snapshot in snapshots if snapshot.is_locked or snapshot.current_pct >= 0.098)
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

    def _render_auction_leader_watch(self, state: StrategyConsoleState) -> tuple[str, ...]:
        leaders = nlargest(
            5,
            (
                snapshot
                for snapshot in state.snapshot_map.values()
                if snapshot.symbol in state.candidate_scope_set
                and (snapshot.auction_amount > 0 or snapshot.lb_days >= 2 or snapshot.is_yest_limit)
            ),
            key=lambda snapshot: (
                snapshot.lb_days,
                -snapshot.leader_rank_in_theme,
                snapshot.auction_amount,
                snapshot.current_pct,
            ),
        )
        if not leaders:
            return ("【竞价龙头】暂无竞价观察",)
        rows = ["【竞价龙头】板位 | 个股 | 高开 | 现涨 | 竞价额 | 量比 | 强弱定性 | 机会上车 | 动作"]
        for snapshot in leaders:
            decision = state.decision_map.get(snapshot.symbol)
            action = self._display_action_label(decision, state, phase_label="auction") if decision else "只观察"
            leader_heat = self._leader_truth_label(snapshot)
            entry_tag = self._entry_window_label(snapshot, phase_label="auction")
            rows.append(
                "  "
                f"{snapshot.lb_days}板"
                f" | {self._short_stock_name(snapshot)}"
                f" | {self._fmt_pct(snapshot.open_pct)}"
                f" | {self._fmt_pct(snapshot.current_pct)}"
                f" | {self._fmt_amount_yi_precise(snapshot.auction_amount)}"
                f" | {self._fmt_volume_intensity(snapshot.volume_intensity)}"
                f" | {self._display_plate_name(snapshot, prefer_high_board=True)}"
                f" | {leader_heat}"
                f" | {entry_tag}"
                f" | {action}"
            )
        return tuple(rows)

    def _render_auction_execution_map(self, state: StrategyConsoleState) -> tuple[str, ...]:
        if state.bundle is None:
            return ("【竞价预案拆解】候选样本尚未就绪",)
        attack = [
            decision
            for decision in state.bundle.decisions
            if decision.action in ("dragon_early_board", "early_boarding_candidate")
        ][:3]
        hold = [decision for decision in state.bundle.decisions if decision.action == "hold_only"][:3]
        avoid = [
            decision
            for decision in state.bundle.decisions
            if decision.action in ("avoid_after_failed_promotion", "do_not_chase")
        ][:3]
        attack_text = " ; ".join(
            f"{self._decision_name(state, row)}:{self._action_text(self.ACTION_LABELS.get(row.action, row.action))}@{row.confidence}" for row in attack
        ) or "无"
        hold_text = " ; ".join(
            f"{self._decision_name(state, row)}:{self._action_text(self.ACTION_LABELS.get(row.action, row.action))}@{row.confidence}" for row in hold
        ) or "无"
        avoid_text = " ; ".join(
            f"{self._decision_name(state, row)}:{self._action_text(self.ACTION_LABELS.get(row.action, row.action))}@{row.confidence}" for row in avoid
        ) or "无"
        return (
            "【竞价预案拆解】方向 | 清单",
            f"  进攻 | {attack_text}",
            f"  持有 | {hold_text}",
            f"  回避 | {avoid_text}",
        )

    def _render_focus_pool(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        if state.bundle is None:
            return (("明日观察池" if phase_label == "postmarket" else "核心观察池") + " | 候选样本尚未就绪",)
        focus_candidates = self._focus_candidates_for_phase(state, phase_label=phase_label)
        selected_symbols = {row.symbol for row in focus_candidates[:4]}
        buy_parts: list[str] = []
        for decision in focus_candidates[:4]:
            snapshot = state.snapshot_map.get(decision.symbol)
            plate = self._display_plate_name(snapshot, prefer_high_board=True)
            action = self._display_action_label(decision, state, phase_label=phase_label)
            evidence = self._focus_evidence(snapshot, phase_label=phase_label)
            buy_parts.append(self._format_focus_item(decision, snapshot, action=action, plate=plate, evidence=evidence))

        ordered_decisions = self._focus_ordered_decisions(state, phase_label=phase_label)
        preferred_plates = self._focus_priority_plates(state) if phase_label == "postmarket" else ()
        alt_parts: list[str] = []
        for decision in ordered_decisions:
            if decision.symbol in selected_symbols:
                continue
            if decision.action in ("avoid_after_failed_promotion", "do_not_chase", "observe_only"):
                continue
            snapshot = state.snapshot_map.get(decision.symbol)
            if preferred_plates and not self._decision_hits_priority_plate(state, decision, preferred_plates=preferred_plates):
                continue
            plate = self._display_plate_name(snapshot, prefer_high_board=True)
            action = self._display_action_label(decision, state, phase_label=phase_label)
            evidence = self._focus_evidence(snapshot, phase_label=phase_label)
            alt_parts.append(self._format_focus_item(decision, snapshot, action=action, plate=plate, evidence=evidence))
            if len(alt_parts) >= 3:
                break

        if not buy_parts:
            buy_parts.append("无")
        if not alt_parts:
            alt_parts.append("无")

        reasons: list[str] = []
        for decision in focus_candidates[:2]:
            note = next((reason for reason in decision.reasons if reason), "wait for confirmation")
            reasons.append(f"{self._decision_name(state, decision)}={self._reason_text(note)}")
        if not reasons:
            reasons.append("暂无=保持观察，等待确认")

        if phase_label == "postmarket":
            return (
                "【明日观察池】个股 | 动作 | 评分 | 竞价涨跌 | 现涨跌 | 题材 | 证据",
                *[f"  {item}" for item in buy_parts],
                f"【留意补充】{' ; '.join(alt_parts)}",
                f"明日理由 | {' ; '.join(reasons)}",
            )

        if phase_label == "premarket" and state.historical_only:
            return (
                "【核心观察池】个股 | 动作 | 评分 | 竞价涨跌 | 现涨跌 | 题材 | 证据",
                *[f"  {item}" for item in buy_parts],
                f"【留意补充】{' ; '.join(alt_parts)}",
                "观察理由 | 当前仅有历史快照，等真实竞价流确认后再转成可执行机会。",
            )

        if phase_label == "intraday" and state.stale_snapshot_only:
            watch_parts = [self._format_watch_item(decision, state.snapshot_map) for decision in state.candidates[:4]] or ["无"]
            carry_parts = []
            for decision in state.bundle.decisions:
                if decision.symbol in selected_symbols:
                    continue
                if decision.action in ("avoid_after_failed_promotion", "do_not_chase", "observe_only"):
                    continue
                carry_parts.append(self._format_watch_item(decision, state.snapshot_map))
                if len(carry_parts) >= 3:
                    break
            if not carry_parts:
                carry_parts.append("无")
            return (
                "【核心观察池】观察 | 评分 | 竞价涨跌 | 现涨跌 | 题材",
                *[f"  {item}" for item in watch_parts],
                f"【留意补充】{' ; '.join(carry_parts)}",
                "观察理由 | 当前仅有滞后盘中快照，先保留观察，不把它当实时机会。",
            )

        return (
            "【核心观察池】个股 | 动作 | 评分 | 竞价涨跌 | 现涨跌 | 题材 | 证据",
            *[f"  {item}" for item in buy_parts],
            f"【备选补充】{' ; '.join(alt_parts)}",
            f"候选理由 | {' ; '.join(reasons)}",
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
        filtered = filter_trade_candidates(state.bundle, min_confidence=self.INTRADAY_CANDIDATE_MIN_CONFIDENCE)
        if not filtered:
            return ()
        filtered_symbols = {decision.symbol for decision in filtered}
        prioritized = tuple(decision for decision in ordered if decision.symbol in filtered_symbols)
        return prioritized or filtered

    def _focus_ordered_decisions(
        self,
        state: StrategyConsoleState,
        *,
        phase_label: str,
    ) -> tuple[AuctionLadderDecision, ...]:
        if state.bundle is None:
            return ()
        decisions = tuple(state.bundle.decisions)
        if phase_label != "postmarket":
            return decisions
        preferred_plates = self._focus_priority_plates(state)
        if not preferred_plates:
            return decisions
        matched: list[AuctionLadderDecision] = []
        remainder: list[AuctionLadderDecision] = []
        for decision in decisions:
            if self._decision_hits_priority_plate(state, decision, preferred_plates=preferred_plates):
                matched.append(decision)
            else:
                remainder.append(decision)
        return tuple(matched + remainder) if matched else decisions

    def _focus_priority_plates(self, state: StrategyConsoleState) -> tuple[str, ...]:
        summary = state.context.market_summary
        ordered: list[str] = []
        for raw_name in (
            summary.mainline_sector,
            summary.top_plate_name,
            *(row.plate_name for row in state.plate_stats[:3] if not row.generic),
        ):
            name = normalize_plate_name(raw_name)
            if name and name != "-" and name not in ordered:
                ordered.append(name)
        return tuple(ordered)

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
        return any(name in preferred_plates for name in self._normalized_plate_names(snapshot))

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

    def _leader_open_strength(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.open_pct >= 0.095:
            return "一字过热"
        if snapshot.open_pct >= 0.06:
            return "强开锚定"
        if snapshot.open_pct >= 0.02:
            return "偏强可看"
        if snapshot.open_pct >= 0.0:
            return "平开待判"
        return "低开承压"

    def _leader_seal_quality(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.is_locked or snapshot.open_pct >= 0.095:
            if snapshot.volume_intensity >= 3.0:
                return "买一强但热"
            return "顶高偏虚"
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
        return "买一一般"

    def _leader_turnover_quality(self, snapshot: StockStateSnapshot) -> str:
        if (
            snapshot.open_pct >= 0.02
            and snapshot.auction_amount >= 20_000_000
            and (snapshot.speed_1m > 0 or snapshot.amount_2m >= 30_000_000)
        ):
            if snapshot.amount_2m >= 50_000_000 or snapshot.speed_1m > 0.01:
                return "换手质量高"
            return "有承接"
        if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
            return "低开转强"
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return "分歧回拉"
        return "换手一般"

    def _leader_pressure_label(self, snapshot: StockStateSnapshot) -> str:
        if snapshot.resistance_gap > 0.12:
            return "抛压偏大"
        if snapshot.resistance_gap > 0.06:
            return "仍有抛压"
        if snapshot.market_cap_yi >= 300 or snapshot.amount_day_yi >= 40:
            return "大票承压"
        if snapshot.current_pct < snapshot.open_pct - 0.03:
            return "承接转弱"
        return "压力可控"

    def _leader_heat_label(self, snapshot: StockStateSnapshot) -> str:
        return f"{self._leader_seal_quality(snapshot)}/{self._leader_turnover_quality(snapshot)}"

    def _leader_truth_label(self, snapshot: StockStateSnapshot) -> str:
        if (
            0.02 <= snapshot.open_pct <= 0.07
            and snapshot.auction_amount >= 30_000_000
            and (snapshot.amount_2m >= 40_000_000 or snapshot.speed_1m > 0.01)
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
        if snapshot.current_pct < 0.0 or snapshot.current_pct < snapshot.open_pct - 0.04:
            return self.OPENING_VALIDATION_UNDERTAKE_WEAK
        return self.OPENING_VALIDATION_PENDING

    def _entry_window_label(self, snapshot: StockStateSnapshot, *, phase_label: str) -> str:
        if phase_label == "postmarket":
            if snapshot.open_pct >= 0.095:
                return "只看回封"
            if (
                0.02 <= snapshot.open_pct <= 0.07
                and snapshot.leader_rank_in_theme <= 2
                and snapshot.auction_amount >= 20_000_000
            ):
                if snapshot.amount_2m >= 40_000_000 or snapshot.speed_1m > 0.01:
                    return "换手后可上"
                return "给过换手"
            if snapshot.open_pct <= 0.01 and snapshot.current_pct > 0.03 and snapshot.amount_2m >= 20_000_000:
                return "低开承接"
            if snapshot.open_pct < 0.0 and snapshot.current_pct <= 0.0:
                return "承接不足"
            return "以观察为主"
        if snapshot.open_pct >= 0.095:
            return "不追一字"
        if (
            0.02 <= snapshot.open_pct <= 0.07
            and snapshot.leader_rank_in_theme <= 2
            and snapshot.auction_amount >= 20_000_000
        ):
            if snapshot.amount_2m >= 40_000_000 or snapshot.speed_1m > 0.01:
                return "换手后可上"
            return "可换手上车"
        if snapshot.open_pct <= 0.01 and snapshot.auction_amount > 0 and snapshot.amount_2m >= 20_000_000:
            return "看低开承接"
        if snapshot.open_pct < 0.0 and snapshot.current_pct <= 0.0:
            return "承接不足"
        return "先等确认"

    def _theme_trade_profile(self, row: AuctionPlateBucketStat) -> tuple[str, str, str]:
        if row.expectation == "distribution":
            return ("兑现观察", "先不出手", "冲高兑现为主")
        if row.generic:
            return ("大题材泛化", "只看辨识度", "量大但太散")
        if row.expectation == "attack":
            if row.auction_amount >= 150_000_000 and row.leader_count >= 2:
                return ("今日主攻", "只做最前排", "竞价额极强")
            return ("今日机会", "只做前排", "强度先手")
        if row.expectation == "follow":
            if row.hot_change_pct > 0 and row.auction_symbol_count >= 3:
                return ("回流修复", "可看分歧转强", "回流确认")
            return ("回流修复", "看前排分歧", "弱转强观察")
        if row.expectation == "ladder":
            if row.hot_change_pct <= 0 and row.yest_limit_count >= 2:
                return ("昨日兑现", "不追后排", "高位先兑现")
            if row.leader_count >= 2 and row.auction_amount >= 80_000_000:
                return ("主线延续", "可做中位晋级", "中位卡位")
            return ("主线延续", "偏向中高位", "看高位活口")
        if row.expectation == "cluster":
            if row.symbol_count >= 4 and row.auction_symbol_count >= 3:
                return ("新发酵", "可找首板前排", "首板扩散")
            return ("首板发酵", "可找前排", "前排试错")
        return ("观察跟踪", "先不出手", "无突出极值")

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
        if row.generic:
            return "大题材太散，只看辨识度核心"
        if row.leader_count >= 2 and row.auction_amount >= 80_000_000:
            return f"{leader}带队，{assist}跟随，能看中位卡位"
        if row.auction_symbol_count >= 3 and row.symbol_count >= 4:
            return f"{leader}点火，{assist}助攻，{follower}属跟风观察"
        if row.yest_limit_count >= 2 and row.hot_change_pct <= 0:
            return f"{leader}仍在但后排易兑现，{follower}别乱接"
        return f"{leader}是核心锚，{assist}和{follower}先看承接"

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
            return "竞价额极强"
        if snapshot.amount_2m >= 80_000_000 and snapshot.speed_1m > 0.01:
            return "开盘抢筹"
        if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= 20_000_000:
            return "低开转强"
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return "水下回拉"
        if snapshot.open_pct >= 0.095:
            return "高开过热"
        return "普通强样本"

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
            return "低开转强"
        if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
            return "水下回拉"
        if snapshot.amount_2m >= 50_000_000 and snapshot.speed_1m > 0.01:
            return "换手抢筹"
        if snapshot.amount_2m >= snapshot.auction_amount > 0:
            return "承接跟上"
        return "观察承接"

    def _focus_evidence(self, snapshot: StockStateSnapshot | None, *, phase_label: str) -> str:
        if snapshot is None:
            return "证据不足"
        evidence: list[str] = []
        if snapshot.leader_rank_in_theme <= 2:
            evidence.append("前排辨识度")
        if snapshot.auction_amount >= 50_000_000:
            evidence.append("竞价额达标")
        if snapshot.volume_intensity >= 2.5:
            evidence.append("买一承接偏强")
        if snapshot.speed_1m > 0:
            evidence.append("开盘有加速")
        if 0.02 <= snapshot.open_pct <= 0.07:
            evidence.append("开幅不算高")
        elif snapshot.open_pct >= 0.095:
            evidence.append("高开偏热")
        if snapshot.amount_2m >= 30_000_000:
            evidence.append("前2分钟放量")
        if snapshot.amount_2m >= snapshot.auction_amount > 0:
            evidence.append("换手承接跟上")
        if snapshot.market_cap_yi >= 80:
            evidence.append("容量票特征")
        if snapshot.resistance_gap > 0.08:
            evidence.append("上方压力大")
        if snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank <= 20:
            evidence.append("热榜位次靠前")
        if phase_label == "postmarket" and snapshot.current_pct != 0:
            evidence.append("收盘强弱已定型")
        return "、".join(evidence[:3]) if evidence else "仅有基础观察信号"

    def _display_plate_name(self, snapshot: StockStateSnapshot | None, *, prefer_high_board: bool = False) -> str:
        if snapshot is None:
            return "-"
        candidates: list[str] = []
        ordered_sources = self._ordered_plate_candidates(snapshot, prefer_high_board=prefer_high_board)
        for raw in ordered_sources:
            cleaned = normalize_plate_name(raw)
            if not cleaned or cleaned in candidates:
                continue
            candidates.append(cleaned)
        for name in candidates:
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
            return "接力友好"
        if summary.headshot_rate >= 0.12 or summary.promotion_rate <= 0.15:
            return "接力恶劣"
        return "接力一般"

    def _yest_limit_bucket_strength(self, snapshots: list[StockStateSnapshot]) -> str:
        if not snapshots:
            return "无样本"
        strong = sum(1 for snapshot in snapshots if snapshot.is_locked or snapshot.current_pct >= 0.098)
        red = sum(1 for snapshot in snapshots if snapshot.open_pct > 0)
        weak = sum(1 for snapshot in snapshots if snapshot.current_pct < 0)
        if strong >= max(1, len(snapshots) // 2):
            return "晋级偏强"
        if weak >= max(1, len(snapshots) // 2):
            return "负反馈重"
        if red >= max(1, len(snapshots) // 2):
            return "有溢价但分歧"
        return "强弱一般"

    def _yest_limit_bucket_action(self, bucket: str, snapshots: list[StockStateSnapshot]) -> str:
        if not snapshots:
            return "无交易结论"
        strong = sum(1 for snapshot in snapshots if snapshot.is_locked or snapshot.current_pct >= 0.098)
        weak = sum(1 for snapshot in snapshots if snapshot.current_pct < 0)
        if bucket == "high":
            if weak >= max(1, len(snapshots) // 2):
                return "高位兑现为主，不接加速后排"
            if strong >= 1:
                return "只看活口回封，不做一致顶"
            return "高位只观察龙头承接"
        if bucket == "mid":
            if strong >= max(1, len(snapshots) // 3):
                return "中位可做卡位，是主要上车层"
            if weak >= max(1, len(snapshots) // 2):
                return "中位承接弱，谨防晋级失败"
            return "中位分歧博弈，只做前排"
        if strong >= 1:
            return "首板有溢价，可看新题材前排"
        if weak >= max(1, len(snapshots) // 2):
            return "首板溢价不足，追板要谨慎"
        return "首板偏试错，等题材共振"

    def _yest_limit_opportunity_profile(self, summary) -> tuple[str, str]:
        if summary.promotion_rate >= 0.35:
            return ("中位有肉", "可多看晋级和卡位")
        if summary.promotion_rate >= 0.2:
            return ("有少量机会", "只看最前排")
        return ("机会偏少", "不做盲目接力")

    def _yest_limit_premium_profile(self, summary) -> tuple[str, str]:
        if summary.red_open_rate >= 0.75:
            return ("红开溢价足", "但要防高开兑现")
        if summary.red_open_rate >= 0.45:
            return ("溢价一般", "看分歧后谁回流")
        return ("溢价不足", "偏向先手错了就砍")

    def _yest_limit_risk_profile(self, summary) -> tuple[str, str]:
        if summary.headshot_rate >= 0.12:
            return ("负反馈重", "高位和后排都要缩")
        if summary.headshot_rate >= 0.06:
            return ("负反馈可见", "只做辨识度")
        return ("负反馈轻", "允许前排试错")

    def _render_risk_guard(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        generic_plates = [row.plate_name for row in state.plate_stats if row.generic][:2]
        avoid_parts: list[str] = []
        if state.bundle is not None:
            for decision in state.bundle.decisions:
                if decision.action in ("avoid_after_failed_promotion", "do_not_chase"):
                    avoid_parts.append(
                        f"{self._decision_name(state, decision)}:{self._action_text(self.ACTION_LABELS.get(decision.action, decision.action))}"
                    )
                if len(avoid_parts) >= 4:
                    break
        field_name = "复盘风控" if phase_label == "postmarket" else "风险提示"
        missing_items = list(state.missing_inputs)
        if state.historical_only and phase_label == "postmarket":
            missing_items = [
                item for item in missing_items if item not in {"auction_anchor", "auction_anchor_pending"}
            ]
        return (
            f"【{field_name}】维度 | 内容",
            f"  - 回避 | {','.join(avoid_parts) or '-'}",
            f"  - 泛题材 | {','.join(generic_plates) or '-'}",
            f"  - 缺失 | {','.join(self._missing_text(item) for item in missing_items) or '无'}",
            f"  - 数据 | {self._display_source_label(state, phase_label=phase_label)}",
        )

    def _score_marker(self, score: float) -> str:
        if score >= 6.0:
            return "★★"
        if score >= 4.5:
            return "★"
        if score >= 3.0:
            return "△"
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
        return "✓"

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
            "historical": "□",
        }
        return mapping.get(battle, "·")

    def _close_marker(self, verdict: str) -> str:
        mapping = {
            "strong_close": "↑",
            "mixed_close": "→",
            "risk_close": "⚠",
            "weak_close": "↓",
        }
        return mapping.get(verdict, "·")

    def _collect_allowed_setups(self, state: StrategyConsoleState, *, phase_label: str) -> tuple[str, ...]:
        if phase_label == "postmarket":
            return ("tomorrow_watch", "leader_review", "plate_recap")
        if phase_label == "premarket" and state.historical_only:
            return ("carry_review", "watch_only", "auction_wait")
        if phase_label == "intraday" and state.stale_snapshot_only:
            return ("watch_only", "leader_review", "risk_scan")
        labels: list[str] = []
        for decision in state.candidates:
            label = self.ACTION_LABELS.get(decision.action, decision.action)
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
        if phase_label == "intraday" and state.stale_snapshot_only:
            banned.extend(["live_trade", "blind_trade", "blind_chase"])
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
            return "沿用昨日热板"
        if mode == "missing":
            return "热点题材缺失"
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

    def _build_candidate_scope(
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
        candidate_scope: Iterable[str],
        *,
        phase_label: str,
        startup_report: StartupSelfCheckReport | None = None,
    ) -> str:
        source_counts: dict[str, int] = {}
        scope = {str(symbol) for symbol in candidate_scope if str(symbol)}
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

    def _is_frozen_postmarket_context(self, intraday_context: IntradayContext) -> bool:
        candidate_scope = self._build_candidate_scope(intraday_context)
        actual_source = self._infer_actual_source(
            intraday_context,
            candidate_scope,
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
        if decision is None:
            return self._action_text("observe_only")
        action = self.ACTION_LABELS.get(decision.action, decision.action)
        if phase_label == "intraday" and state.stale_snapshot_only:
            if action in {"failed_promo_guard", "do_not_chase", "observe_only"}:
                return self._action_text(action)
            return self._action_text("observe_only")
        return self._action_text(action)

    def _format_watch_item(
        self,
        decision: AuctionLadderDecision,
        snapshot_map: dict[str, StockStateSnapshot],
    ) -> str:
        snapshot = snapshot_map.get(decision.symbol)
        plate = snapshot.plate if snapshot and snapshot.plate else "-"
        return (
            f"{self._short_stock_name(snapshot, symbol=decision.symbol)}"
            f" | {decision.confidence}"
            f" | {self._fmt_pct(snapshot.open_pct) if snapshot is not None else '-'}"
            f" | {self._fmt_pct(snapshot.current_pct) if snapshot is not None else '-'}"
            f" | {plate}"
        )

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

    def _snapshot_name_by_symbol_compact(self, state: StrategyConsoleState | IntradayContext, symbol: str) -> str:
        if isinstance(state, StrategyConsoleState):
            matched = state.snapshot_map.get(symbol)
            return self._compact_stock_ref(matched, symbol=symbol)
        matched = next((snapshot for snapshot in state.stock_snapshots if snapshot.symbol == symbol), None)
        return self._compact_stock_ref(matched, symbol=symbol)

    def _decision_name_compact(self, state: StrategyConsoleState, decision: AuctionLadderDecision) -> str:
        matched = state.snapshot_map.get(decision.symbol)
        return self._compact_stock_ref(matched, symbol=decision.symbol)

    def _format_plan_item(self, state: StrategyConsoleState, decision: AuctionLadderDecision) -> str:
        action = self._action_text(self.ACTION_LABELS.get(decision.action, decision.action))
        return f"{self._decision_name_compact(state, decision)}:{action}@{decision.confidence}"

    def _format_focus_item(
        self,
        decision: AuctionLadderDecision,
        snapshot: StockStateSnapshot | None,
        *,
        action: str,
        plate: str,
        evidence: str,
    ) -> str:
        return (
            f"{self._short_stock_name(snapshot, symbol=decision.symbol)}"
            f" | {action}"
            f" | {decision.confidence}"
            f" | {self._fmt_pct(snapshot.open_pct) if snapshot is not None else '-'}"
            f" | {self._fmt_pct(snapshot.current_pct) if snapshot is not None else '-'}"
            f" | {plate}"
            f" | {evidence}"
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
        text = raw_name.replace(" ", "").replace("(", "").replace(")", "").replace("（", "").replace("）", "").replace("*", "")
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
            return "主攻"
        if expectation == "follow":
            return "跟随"
        if expectation == "ladder":
            if row.hot_change_pct <= 0 and row.yest_limit_count >= 2:
                return "兑现"
            return "晋级"
        if expectation == "cluster":
            return "发酵"
        return "观察"

    def _fmt_volume_intensity(self, value: float) -> str:
        if value <= 0:
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
        if state.frozen_postmarket_snapshot:
            return "盘中冻结快照"
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
            "intraday": "盘中",
            "postmarket": "盘后",
        }
        return mapping.get(phase_label, phase_label)

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

    def _action_text(self, action: str) -> str:
        mapping = {
            "dragon_board": "龙头打板",
            "theme_first_board": "题材首板",
            "leader_hold": "龙头持有",
            "ice_probe": "冰点试错",
            "observe_only": "只观察",
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
            "generic_theme_only": "泛题材勿上",
            "full_position": "禁止满仓",
            "none": "无",
        }
        return mapping.get(item, item)

    def _expectation_text(self, item: str) -> str:
        mapping = {
            "attack": "主攻",
            "follow": "跟随",
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
            "carry_review": "承接复核",
            "stale_review": "滞后观察",
        }
        return mapping.get(regime, regime)

    def _source_text(self, source: str) -> str:
        mapping = {
            "stale_intraday_snapshot": "盘中滞后快照",
            "prev_close_snapshot": "昨收快照",
            "redis_runtime_projection": "全市场投影",
            "startup_repair": "启动修复",
            "hot_plate_cache": "热点题材缓存",
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
            "strong_close": "强修复",
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

    def _phase_window_label(self, phase_label: str) -> str:
        if phase_label == "premarket":
            return "00:00-09:25"
        if phase_label == "auction_preview":
            return "09:15-09:24"
        if phase_label == "auction":
            return "09:25-09:30"
        if phase_label == "opening":
            return "09:30-09:40"
        if phase_label == "postmarket":
            return "15:00-17:40"
        return "09:40-15:00"

    def _ladder_sort_value(self, key: str) -> int:
        digits = "".join(ch for ch in key if ch.isdigit())
        return int(digits or 0)

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

    def _fmt_hot_rank(self, rank: int) -> str:
        return "-" if rank >= 999 else str(rank)

    def _fmt_net_inflow_yi(self, value: float) -> str:
        if abs(value) < 0.005:
            return "0.00亿"
        return f"{value:+.2f}亿"

    def _capital_behavior_text(self, value: float) -> str:
        if value >= 1.2:
            return "主力流入"
        if value >= 0.35:
            return "偏强流入"
        if value <= -0.3:
            return "主力流出"
        return "分歧震荡"

    def _infer_close_verdict(self, summary) -> str:
        if summary.sentiment_score >= 6.0 and summary.headshot_rate <= 0.05:
            return "strong_close"
        if summary.sentiment_score >= 4.5:
            return "mixed_close"
        if summary.headshot_rate >= 0.12:
            return "risk_close"
        return "weak_close"
