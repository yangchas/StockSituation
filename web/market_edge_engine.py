from __future__ import annotations
import asyncio
import json
import time
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import bisect
import traceback
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ThreadPoolExecutor

import sys

# Resolve paths dynamically to ensure 'ai' and 'services' are findable
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

logger = logging.getLogger(__name__)

MARKET_EDGE_ENGINE_VERSION = "snapshot_v1"
SNAPSHOT_SCHEMA_VERSION = "1.0"
SNAPSHOT_BASE_DIR = os.path.join(_current_dir, "snapshots")
SNAPSHOT_ENABLED = True
SNAPSHOT_TYPES_ENABLED = {
    "MarketSnapshot": True,
    "PlateSnapshot": True,
    "ABPairSnapshot": True,
    "SignalSnapshot": True,
    "ExecutionSnapshot": True,
    "StockDecisionSnapshot": False,
}

try:
    # Use a more robust import approach for the internal API package
    import ai.API.api as api_mod
    UnifiedMarketDataFetcher = getattr(api_mod, 'UnifiedMarketDataFetcher', None)
    # Check if the module-level StockAnalyzer is False, which indicates partial init
    if getattr(api_mod, "StockAnalyzer", None) is None:
        if hasattr(api_mod, "reinitialize_specialized_apis"):
            if api_mod.reinitialize_specialized_apis():
                 logger.info("Successfully re-initialized specialized APIs on second attempt.")
    
    if UnifiedMarketDataFetcher is None:
        logger.warning("UnifiedMarketDataFetcher not found in ai.API.api")
except (ImportError, ModuleNotFoundError) as e:
    logger.warning(f"Failed to import ai.API.api: {e}")
    UnifiedMarketDataFetcher = None
except Exception as e:
    logger.warning(f"Global Error importing UnifiedMarketDataFetcher: {e}")
    UnifiedMarketDataFetcher = None

try:
    from ai.StockAnalyzer import StockAnalyzer
except ImportError:
    try:
        from ai.API.StockAnalyzer import StockAnalyzer
    except ImportError:
        StockAnalyzer = None

try:
    from services.kaipan_plate_service import fetch_kaipan_plate_rank
except Exception:
    fetch_kaipan_plate_rank = None

@dataclass
class EmotionPhaseResult:
    ts: int
    date: str

    emotion_phase: str           # ICE_POINT / IGNITION / MAIN_RISE / DIVERGE / RETREAT
    phase_confidence: float      # 当前阶段判定置信度
    transition_reason_code: str  # 跃迁归因 (CORE_SEALED, RISING_CONFIRM, DIVERGE_REPAIR, BROAD_CRASH)

    position_cap: float          # 阶段仓位上限
    allowed_setups: List[str]    # 放行的交易模式 (low_level_relay, core_dip_buying 等)
    blocked_setups: List[str]    # 强制禁止模式 (blind_relay, late_rotation 等)

    global_fakeout_penalty: float# 全局退潮/骗炮惩罚基数
    phase_age_days: int          # 当前情绪阶段持续天数
    phase_age_intraday_bars: int # 当前情绪阶段内持续的分时/秒级条数
    leader_candidates: List[Dict[str, Any]] # 前排池 (序列化 StockRoleProfile)
    plate_phase_map: Dict[str, str]

@dataclass
class StockRoleProfile:
    code: str
    name: str
    role_type: str               # leader / core_anchor / relay_candidate / follower
    leadership_score: float
    follow_score: float
    chip_safety_score: float = 0.0
    chip_zone_status: str = ""
    board_position_rank: int = 0     # 题材料身位：1=龙头，2=中军核心，3=补涨，4及以后=跟风
    theme_overlap_count: int = 0
    real_market_cap: float = 0.0
    limit_up_type: str = ""
    primary_plate: str = ""
    primary_plate_id: str = ""
    plate_phase: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class ScenarioCard:
    scenario_id: str
    scenario_type: str
    title: str
    priority: int
    confidence: float
    trigger_conditions: Dict[str, Any]
    cancel_condition: Dict[str, Any]
    invalid_after_ts: int
    risk_cut_trigger: Dict[str, Any]
    candidate_codes: List[str]
    candidate_roles: List[Dict[str, Any]]
    position_hint: Dict[str, Any]
    phase_binding: str
    notes: str

@dataclass
class SignalCard:
    signal_id: str
    scenario_type: str
    code: str
    name: str

    theme: str
    role_type: str
    board_position_rank: int

    signal_score: float
    confidence: float
    suggested_position: float

    entry_hint: Dict[str, Any]
    exit_plan: Dict[str, Any]
    invalid_after_ts: int
    reason: str
    risk_flags: List[str]
    chip_context: Dict[str, Any] = field(default_factory=dict)
    market_phase: str = ""
    plate_phase: str = ""
    plate_phase_confidence: float = 0.0
    setup_type: str = ""
    setup_matrix_weight: float = 1.0
    setup_confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StockRankItem:
    code: str
    score: float
    primary_theme: str
    top_theme_weight: float
    top_theme_conflict: bool
    auction_rank: int
    auction_bid_amount_yuan: float
    auction_change_pct: float
    amount_2min: float
    change_pct: float
    plate_ids: List[str]
    plate_best: str
    plate_best_score: float
    theme_score: float
    plate_score: float
    dde_score: float
    co_move_score: float
    follow_5m_ratio: float
    follow_30m_ratio: float
    co_move_active_peers: int
    resonance_role: str
    lead_follow_count: int
    lead_follow_ratio: float
    ts: int
    analysis_reason: str = ""



@dataclass
class ExpectationSignal:
    code: str
    type: str  # 'price_gap', 'plate_divergence', 'volume_gap', 'weak_to_strong', 'strong_to_weak'
    change: float  # Current/Auction Change (%)
    score: float    # Signal Strength
    details: Dict[str, Any]
    reason: str
    
    def to_dict(self):
        return {
            "code": self.code,
            "type": self.type,
            "change": self.change,
            "score": self.score,
            "details": self.details,
            "reason": self.reason,
            "timestamp": int(time.time() * 1000)
        }


@dataclass
class ExecutionPolicy:
    ts: int
    position_max: float
    mode_allow: List[str]
    candidate_pool_key: str
    ban_conditions: List[str]
    risk_budget: Dict[str, Any]
    explain: Dict[str, Any]


@dataclass
class YesterdayStateProfile:
    code: str
    state_type: str  # ZT_STRONG, ZT_WEAK, BOMB_STRONG, BOMB_WEAK, FLOOR_RESCUED, FLOOR_LOCKED, NORMAL_WEAK, NORMAL_STRONG, NORMAL_NEUTRAL
    change_pct: float
    close_strength: float
    limit_up_type: str
    vol_ratio: float
    upper_shadow: float
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExpectationTransitionProfile:
    code: str
    label: str  # weak_to_strong_confirmed, weak_to_strong_watch, fake_strength_trap, strong_to_weak_confirmed, strong_to_weak_watch, neutral
    total_score: float
    y_profile: YesterdayStateProfile
    
    # 细分评分项
    breakout_score: float = 0.0
    amount_score: float = 0.0
    hold_score: float = 0.0
    chip_score: float = 0.0
    plate_score: float = 0.0
    
    metrics: Dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['y_profile'] = self.y_profile.to_dict()
        return d


@dataclass
class ExpectationStateSummary:
    date: str
    ts: int
    
    # 指标汇总
    weak_to_strong_window_score: float
    strong_to_weak_pressure_score: float
    fake_strength_trap_score: float
    
    # 详细明细
    details_by_code: Dict[str, ExpectationTransitionProfile]
    
    # 榜单 (Top 10)
    weak_to_strong_top: List[str]
    strong_to_weak_top: List[str]
    fake_strength_top: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "ts": self.ts,
            "weak_to_strong_window_score": round(self.weak_to_strong_window_score, 4),
            "strong_to_weak_pressure_score": round(self.strong_to_weak_pressure_score, 4),
            "fake_strength_trap_score": round(self.fake_strength_trap_score, 4),
            "details": {c: p.to_dict() for c, p in self.details_by_code.items()},
            "weak_to_strong_top": self.weak_to_strong_top,
            "strong_to_weak_top": self.strong_to_weak_top,
            "fake_strength_top": self.fake_strength_top
        }


@dataclass
class StandardSnapshot:
    snapshot_id: str
    snapshot_type: str
    date: str
    ts_ms: int
    time_str: str
    run_id: str
    module: str
    engine_version: str
    snapshot_schema_version: str
    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
    reasons: Dict[str, Any]
    meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MarketEdgeEngine:
    """流程二：赚钱效应识别/挖掘系统（最小可用闭环）。

    目标：不影响 integrated_server 现有逻辑，独立后台任务写入 market/rank key。

    依赖注入：
    - redis: aioredis client (decode_responses=True)
    - redis_storage: RedisStorageManager (用于 get_data/store_data 等同步封装)
    - plate_updater: OptimizedEnhancedPlateUpdater
    - calendar: TradeCalendar
    - advanced_indicators: OptimizedAdvancedTechnicalIndicators
    - theme_ranker: ThemeRanker

    说明：
    - 本模块实现流程二最小闭环：CandidatePool/PlateSpread/ThemeRank/ComfortExit/Sentiment/OpenScenario/ExecutionPolicy。
    - 若某些事实层 key 不存在，ExecutionPolicy 会降级并标记 explain.stale=true。
    """

    # --- 垃圾/背景属性板块拦截清单 (Phase E) ---
    GENERIC_PLATE_KEYWORDS = {
        "国有企业", "国企改革", "年报增长", "中字头", "破净股", 
        "转融券", "融资融券", "昨日涨停", "昨日触板", "证金持股", 
        "汇金持股", "标普大盘", "机构重仓", "预盈预增", "深股通", 
        "沪股通", "茅指数", "宁指数", "成分股", "MSCI中国", "基本面100"
    }

    # P1: Explicit Phase-Pattern Binding
    PHASE_MODE_MAPPING = {
        "ice_point": ["WEAK_TO_STRONG", "DEEP_V_REBOUND"],
        "start": ["A_DIRECT", "A_TO_B_ARBITRAGE"],
        "climax": ["LEADER_RELAY", "CORE_TREND", "A_DIRECT", "LOW_LEVEL_RELAY"],
        "divergence": ["A_TO_B_ARBITRAGE", "CORE_DIP_BUYING"],
        "retreat": ["RARE_REVERSAL_ONLY", "WAIT"],
    }

    def __init__(
        self,
        *,
        redis,
        redis_storage,
        plate_updater,
        calendar,
        advanced_indicators,
        theme_ranker,
        stock_analyzer: Optional[StockAnalyzer] = None,
        candidate_pool_size: int = 500,
    ):
        self.redis = redis
        self.redis_storage = redis_storage
        self.plate_updater = plate_updater
        self.calendar = calendar
        self.advanced_indicators = advanced_indicators
        self.theme_ranker = theme_ranker
        self.stock_analyzer = stock_analyzer
        self.candidate_pool_size = candidate_pool_size

        self.candidate_pool_cache: Set[str] = set()
        self.last_candidate_pool_update: float = 0.0
        self.opening_verification_done: bool = False

        self.last_stock_rank_update: float = 0.0
        self.last_plate_spread_update: float = 0.0
        self.last_theme_rank_update: float = 0.0
        self.last_comfort_exit_update: float = 0.0
        self.last_sentiment_update: float = 0.0
        self.last_execution_policy_update: float = 0.0
        self.last_intraday_state_update: float = 0.0
        self.last_plate_attitude_update: float = 0.0
        self.last_static_precompute_update: float = 0.0
        self.last_fear_greed_update: float = 0.0
        self.last_herding_update: float = 0.0
        self.current_run_id: str = ""
        self.last_resonance_update: float = 0.0
        self.last_analysis_universe_update: float = 0.0
        self.last_market_overview_update: float = 0.0
        self.last_stock_profile_update: float = 0.0
        self.last_plate_snapshot_update: float = 0.0
        self.last_plate_profile_update: float = 0.0
        self.last_market_process_profile_update: float = 0.0
        self.last_expectation_eval_update: float = 0.0
        self.last_strategy_tags_update: float = 0.0
        self.last_ab_arbitrage_update: float = 0.0
        self.last_preopen_plan_update: float = 0.0
        self.last_open_verify_plan_update: float = 0.0
        self.last_data_contract_check_update: float = 0.0

        logger.info(f"馃殌 Market Edge Engine Started: FETCH_API={UnifiedMarketDataFetcher is not None}, CUR_DIR={os.getcwd()}")
        self.last_trading_action: str = "WAIT"
        self.action_stable_count: int = 0
        self.last_position_max: float = 0.0
        self.plate_recommendation_history: Dict[str, int] = {}
        self.active_long_plates: List[str] = []

        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
        self.sys_health_status = {"data_integrity": True, "reason": "ok"}

        self.manual_date: Optional[str] = None
        self.auction_profile_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        self.stock_state_cache: Dict[str, Dict[str, Any]] = {}
        self.plate_weight_cache: Dict[str, List[Tuple[str, float]]] = {}
        self.static_stock_to_plates: Dict[str, List[str]] = {}
        self.static_plate_info: Dict[str, Dict[str, Any]] = {}
        self.last_static_plate_sync: float = 0.0
        self.chip_peaks: Dict[str, Dict[str, Any]] = {}
        self.last_chip_peaks_sync: float = 0.0
        self.stock_extra: Dict[str, Dict[str, Any]] = {}
        self.last_stock_extra_sync: float = 0.0
        self.precomputed_static_date: Optional[str] = None
        self.intraday_transition_seen: Dict[str, int] = {}
        self.return_history: Dict[str, List[float]] = {}
        self.leading_plate_history: List[Tuple[int, str]] = []
        self.log_last_payload: Dict[str, str] = {}
        self.log_last_ts: Dict[str, float] = {}
        self.analysis_universe_cache: Set[str] = set()
        self.profile_transition_seen: Dict[str, int] = {}
        self.code_change_history: Dict[str, List[Tuple[int, float]]] = {}
        self.pending_eod_calc: bool = False
        self.daily_profiles_done_for: str = ""
        self._auction_cache: Dict[str, Dict[str, Any]] = {}

        # P6: Wencai Redesign State
        self._wencai_fetch_lock = asyncio.Lock()
        self._wencai_fail_count = 0
        self._wencai_executor = ThreadPoolExecutor(max_workers=3)
        
        self._first_limit_cache: Dict[str, Any] = {"ts": 0.0, "codes": set()}

        self._quote_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._kaipan_plate_cache: Dict[str, Any] = {"ts": 0.0, "by_id": {}, "count": 0}
        self.kaipan_plate_cache_ttl_sec: int = int(os.getenv("KAIPAN_PLATE_CACHE_TTL_SEC", "60"))
        self.enable_kaipan_plate_blend: bool = os.getenv("ENABLE_KAIPAN_PLATE_BLEND", "1") == "1"
        self.kaipan_plate_blend_weight: float = float(os.getenv("KAIPAN_PLATE_BLEND_WEIGHT", "0.35"))

        self.task_intervals: Dict[str, int] = {
            "candidate_pool": 300,
            "indicators": 60,
            "plate_spread": 300,
            "theme_rank": 300,
            "comfort_exit": 300,
            "sentiment": 300,
            "stock_rank": 300,
            "intraday_state": 60,
            "plate_attitude": 60,
            "execution_policy": 60,
            "static_precompute": 1800,
            "fear_greed": 300,
            "herding": 300,
            "resonance": 300,
            "analysis_universe": 300,
            "market_overview": 300,
            "stock_profile": 300,
            "plate_snapshot": 300,
            "plate_profile": 300,
            "market_process_profile": 300,
            "expectation_eval": 1800,
            "strategy_tags": 300,
            "ab_arbitrage": 180,
            "preopen_plan": 120,
            "open_verify_plan": 120,
            "data_contract_check": 300,
        }

    def _new_run_id(self, today_str: str) -> str:
        compact_day = str(today_str or "").replace("-", "")
        now = datetime.now()
        return f"run_{compact_day}_{now.strftime('%H%M%S')}_{int(now.microsecond / 100):04d}"

    def _ensure_run_id(self, today_str: str, run_id: Optional[str] = None, refresh: bool = False) -> str:
        if run_id:
            self.current_run_id = run_id
            return run_id
        current_run_id = getattr(self, "current_run_id", "")
        if refresh or not current_run_id:
            self.current_run_id = self._new_run_id(today_str)
        return self.current_run_id

    def _format_time_str(self, ts_ms: int) -> str:
        try:
            return datetime.fromtimestamp(ts_ms / 1000.0).strftime("%H:%M:%S.%f")[:-3]
        except Exception:
            return ""

    def _build_snapshot_id(self, date: str, run_id: str, snapshot_type: str, object_id: str) -> str:
        safe_object_id = re.sub(r"[^0-9A-Za-z_\\-]+", "_", str(object_id or "obj")).strip("_") or "obj"
        return f"{date}_{run_id}_{snapshot_type}_{safe_object_id}"

    def _snapshot_file_for_type(self, date: str, snapshot_type: str) -> str:
        file_map = {
            "MarketSnapshot": "market.jsonl",
            "PlateSnapshot": "plate.jsonl",
            "ABPairSnapshot": "ab_pair.jsonl",
            "SignalSnapshot": "signal.jsonl",
            "ExecutionSnapshot": "execution.jsonl",
        }
        day_dir = os.path.join(SNAPSHOT_BASE_DIR, str(date or "unknown"))
        return os.path.join(day_dir, file_map.get(snapshot_type, "misc.jsonl"))

    async def _store_snapshot_redis(self, snapshot: Dict[str, Any]) -> None:
        snapshot_id = str(snapshot.get("snapshot_id", ""))
        if not snapshot_id:
            return
        key = f"market:snapshot:{snapshot_id}"
        payload = {
            "snapshot_id": snapshot_id,
            "snapshot_type": str(snapshot.get("snapshot_type", "")),
            "date": str(snapshot.get("date", "")),
            "ts_ms": int(snapshot.get("ts_ms", 0) or 0),
            "time_str": str(snapshot.get("time_str", "")),
            "run_id": str(snapshot.get("run_id", "")),
            "module": str(snapshot.get("module", "")),
            "engine_version": str(snapshot.get("engine_version", "")),
            "snapshot_schema_version": str(snapshot.get("snapshot_schema_version", "")),
            "payload": json.dumps(snapshot, ensure_ascii=False),
        }
        await self.redis.hset(key, mapping=payload)
        await self.redis.expire(key, 7 * 24 * 3600)
        if hasattr(self.redis, "zadd"):
            await self.redis.zadd(f"market:snapshots:index:{snapshot.get('date', '')}", {snapshot_id: payload["ts_ms"]})
            await self.redis.expire(f"market:snapshots:index:{snapshot.get('date', '')}", 7 * 24 * 3600)

    def _write_snapshot_file(self, snapshot: Dict[str, Any]) -> None:
        file_path = self._snapshot_file_for_type(str(snapshot.get("date", "")), str(snapshot.get("snapshot_type", "")))
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(snapshot, ensure_ascii=False) + "\n")

    async def _emit_snapshot(self, snapshot: StandardSnapshot) -> None:
        snapshot_type = snapshot.snapshot_type
        if not SNAPSHOT_ENABLED or not SNAPSHOT_TYPES_ENABLED.get(snapshot_type, False):
            return
        raw = snapshot.to_dict()
        await self._store_snapshot_redis(raw)
        self._write_snapshot_file(raw)

    def _snapshot_meta(
        self,
        *,
        code: str = "",
        plate_id: str = "",
        signal_id: str = "",
        a_code: str = "",
        b_code: str = "",
        emotion_phase_key: str = "",
        plate_phase_detail_key: str = "",
        ab_key: str = "",
        plan_key: str = "",
        execution_key: str = "",
        storage: Optional[Dict[str, Any]] = None,
        flags: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "object_ref": {
                "code": code,
                "plate_id": plate_id,
                "signal_id": signal_id,
                "a_code": a_code,
                "b_code": b_code,
            },
            "upstream_refs": {
                "emotion_phase_key": emotion_phase_key,
                "plate_phase_detail_key": plate_phase_detail_key,
                "ab_key": ab_key,
                "plan_key": plan_key,
                "execution_key": execution_key,
            },
            "storage": storage or {},
            "flags": flags or {},
        }

    async def _mark_plate_snapshot_seen(self, today_str: str, plate_id: str) -> None:
        if not plate_id:
            return
        await self.redis.hset(
            f"market:snapshots:plate_seen:{today_str}",
            mapping={plate_id: str(int(time.time() * 1000))},
        )
        await self.redis.expire(f"market:snapshots:plate_seen:{today_str}", 7 * 24 * 3600)

    async def _is_plate_snapshot_seen(self, today_str: str, plate_id: str) -> bool:
        if not plate_id:
            return False
        seen = await self.redis.hgetall(f"market:snapshots:plate_seen:{today_str}") or {}
        return plate_id in seen

    def _build_plate_snapshot(
        self,
        *,
        today_str: str,
        run_id: str,
        module: str,
        detail: Dict[str, Any],
        is_consumed_plate: bool = False,
    ) -> StandardSnapshot:
        ts_ms = int(time.time() * 1000)
        plate_id = str(detail.get("plate_id", ""))
        plate_name = str(detail.get("plate_name", plate_id))
        return StandardSnapshot(
            snapshot_id=self._build_snapshot_id(today_str, run_id, "PlateSnapshot", plate_id or plate_name),
            snapshot_type="PlateSnapshot",
            date=today_str,
            ts_ms=ts_ms,
            time_str=self._format_time_str(ts_ms),
            run_id=run_id,
            module=module,
            engine_version=MARKET_EDGE_ENGINE_VERSION,
            snapshot_schema_version=SNAPSHOT_SCHEMA_VERSION,
            inputs={
                "spread_ratio": self._safe_float(detail.get("spread_ratio", 0.0), 0.0),
                "attitude_score": self._safe_float(detail.get("attitude_score", 0.0), 0.0),
                "profile_score": self._safe_float(detail.get("profile_score", 0.0), 0.0),
                "headshot_rate": self._safe_float(detail.get("headshot_rate", 0.0), 0.0),
                "strong_to_weak_rate": self._safe_float(detail.get("strong_to_weak_rate", 0.0), 0.0),
                "weak_to_strong_rate": self._safe_float(detail.get("weak_to_strong_rate", 0.0), 0.0),
                "single_down_rate": self._safe_float(detail.get("single_down_rate", 0.0), 0.0),
                "single_up_rate": self._safe_float(detail.get("single_up_rate", 0.0), 0.0),
                "main_net": self._safe_float(detail.get("main_net", 0.0), 0.0),
                "strength": self._safe_float(detail.get("strength", 0.0), 0.0),
                "first_board_count": self._safe_int(detail.get("first_board_count", 0), 0),
                "second_board_count": self._safe_int(detail.get("second_board_count", 0), 0),
                "high_board_count": self._safe_int(detail.get("high_board_count", 0), 0),
            },
            outputs={
                "plate_id": plate_id,
                "plate_name": plate_name,
                "phase": str(detail.get("phase", "UNKNOWN")),
                "confidence": self._safe_float(detail.get("confidence", 0.0), 0.0),
                "leader_code": str(detail.get("leader_code", "")),
                "max_lb": self._safe_int(detail.get("max_lb", 0), 0),
                "active_stock_n": self._safe_int(detail.get("active_stock_n", 0), 0),
            },
            reasons={
                "reason_text": str(detail.get("reason", "")),
                "spread_ratio": self._safe_float(detail.get("spread_ratio", 0.0), 0.0),
                "attitude_score": self._safe_float(detail.get("attitude_score", 0.0), 0.0),
                "profile_score": self._safe_float(detail.get("profile_score", 0.0), 0.0),
                "headshot_rate": self._safe_float(detail.get("headshot_rate", 0.0), 0.0),
                "strong_to_weak_rate": self._safe_float(detail.get("strong_to_weak_rate", 0.0), 0.0),
                "weak_to_strong_rate": self._safe_float(detail.get("weak_to_strong_rate", 0.0), 0.0),
                "single_down_rate": self._safe_float(detail.get("single_down_rate", 0.0), 0.0),
                "single_up_rate": self._safe_float(detail.get("single_up_rate", 0.0), 0.0),
                "main_net": self._safe_float(detail.get("main_net", 0.0), 0.0),
                "strength": self._safe_float(detail.get("strength", 0.0), 0.0),
            },
            meta=self._snapshot_meta(
                plate_id=plate_id,
                plate_phase_detail_key=f"market:plate_phase_detail:{today_str}",
                storage={
                    "phase_map_key": f"market:plate_phase_map:{today_str}",
                    "phase_detail_key": f"market:plate_phase_detail:{today_str}",
                },
                flags={
                    "plate_phase_engine_version": str(detail.get("plate_phase_engine_version", "v2_unified")),
                    "is_consumed_plate": bool(is_consumed_plate),
                },
            ),
        )

    def _build_ab_pair_snapshot(
        self,
        *,
        today_str: str,
        run_id: str,
        module: str,
        pair: Dict[str, Any],
        pair_kind: str,
        thresholds: Dict[str, Any],
    ) -> StandardSnapshot:
        ts_ms = int(time.time() * 1000)
        a_code = str(pair.get("a_code") or pair.get("code") or "")
        b_code = str(pair.get("b_code", ""))
        plate_id = str(pair.get("plate_id", ""))
        object_id = f"{pair_kind}_{a_code or 'na'}_{b_code or 'none'}"
        pair_score = self._safe_float(pair.get("pair_score", pair.get("score", 0.0)), 0.0)
        return StandardSnapshot(
            snapshot_id=self._build_snapshot_id(today_str, run_id, "ABPairSnapshot", object_id),
            snapshot_type="ABPairSnapshot",
            date=today_str,
            ts_ms=ts_ms,
            time_str=self._format_time_str(ts_ms),
            run_id=run_id,
            module=module,
            engine_version=MARKET_EDGE_ENGINE_VERSION,
            snapshot_schema_version=SNAPSHOT_SCHEMA_VERSION,
            inputs={
                "a_score": self._safe_float(pair.get("a_score", pair.get("score", 0.0)), 0.0),
                "b_score": self._safe_float(pair.get("b_score", 0.0), 0.0),
                "a_auction_change_pct": self._safe_float(pair.get("a_auction_change_pct", pair.get("auction_change_pct", 0.0)), 0.0),
                "a_auction_seal_ratio": self._safe_float(pair.get("a_auction_seal_ratio", pair.get("auction_seal_ratio", 0.0)), 0.0),
                "a_current_change_pct": self._safe_float(pair.get("a_current_change_pct", pair.get("current_change_pct", 0.0)), 0.0),
                "b_current_change_pct": self._safe_float(pair.get("b_current_change_pct", 0.0), 0.0),
                "b_change_rate_1min": self._safe_float(pair.get("b_change_rate_1min", 0.0), 0.0),
                "b_amount_2min": self._safe_float(pair.get("b_amount_2min", 0.0), 0.0),
                "thresholds": dict(thresholds or {}),
            },
            outputs={
                "a_code": a_code,
                "b_code": b_code,
                "plate_id": plate_id,
                "plate_name": str(pair.get("plate_name", self._get_plate_name(plate_id))),
                "a_role_type": "leader",
                "b_role_type": "" if pair_kind == "AA" else "relay_candidate",
                "pair_score": pair_score,
                "confidence": pair.get("confidence", ""),
            },
            reasons={
                "pair_reason": str(pair.get("reason", "aa_direct_candidate" if pair_kind == "AA" else "")),
                "reject_reason": str(pair.get("reject_reason", "")),
                "a_score": self._safe_float(pair.get("a_score", pair.get("score", 0.0)), 0.0),
                "b_score": self._safe_float(pair.get("b_score", 0.0), 0.0),
                "thresholds": dict(thresholds or {}),
                "role_hints": {
                    "a_role_type": "leader",
                    "b_role_type": "" if pair_kind == "AA" else "relay_candidate",
                },
            },
            meta=self._snapshot_meta(
                plate_id=plate_id,
                a_code=a_code,
                b_code=b_code,
                plate_phase_detail_key=f"market:plate_phase_detail:{today_str}",
                ab_key=f"market:ab_arbitrage:{today_str}",
                storage={"ab_key": f"market:ab_arbitrage:{today_str}"},
                flags={"pair_kind": pair_kind},
            ),
        )

    def _build_signal_snapshot(
        self,
        *,
        today_str: str,
        run_id: str,
        signal_card: Dict[str, Any],
        primary_plate_id: str,
        primary_plate_name: str,
        input_bundle: Dict[str, Any],
    ) -> StandardSnapshot:
        ts_ms = int(time.time() * 1000)
        signal_id = str(signal_card.get("signal_id", ""))
        code = str(signal_card.get("code", ""))
        return StandardSnapshot(
            snapshot_id=self._build_snapshot_id(today_str, run_id, "SignalSnapshot", signal_id or code),
            snapshot_type="SignalSnapshot",
            date=today_str,
            ts_ms=ts_ms,
            time_str=self._format_time_str(ts_ms),
            run_id=run_id,
            module="build_open_verify_plan",
            engine_version=MARKET_EDGE_ENGINE_VERSION,
            snapshot_schema_version=SNAPSHOT_SCHEMA_VERSION,
            inputs={"input_bundle": dict(input_bundle or {})},
            outputs={
                "signal_id": signal_id,
                "code": code,
                "name": str(signal_card.get("name", "")),
                "signal_score_raw": self._safe_float(signal_card.get("signal_score", 0.0), 0.0),
                "signal_score_final": self._safe_float(signal_card.get("signal_score", 0.0), 0.0),
                "confidence": self._safe_float(signal_card.get("confidence", 0.0), 0.0),
                "suggested_position": self._safe_float(signal_card.get("suggested_position", 0.0), 0.0),
                "market_phase": str(signal_card.get("market_phase", "")),
                "plate_phase": str(signal_card.get("plate_phase", "")),
                "primary_plate_id": primary_plate_id,
                "primary_plate_name": primary_plate_name,
                "role_type": str(signal_card.get("role_type", "")),
                "setup_type": str(signal_card.get("setup_type", "")),
                "reason": str(signal_card.get("reason", "")),
                "risk_flags": list(signal_card.get("risk_flags", []) or []),
            },
            reasons={
                "reason_text": str(signal_card.get("reason", "")),
                "risk_flags": list(signal_card.get("risk_flags", []) or []),
                "entry_hint": dict(signal_card.get("entry_hint", {}) or {}),
                "exit_plan": dict(signal_card.get("exit_plan", {}) or {}),
                "setup_matrix_weight": self._safe_float(signal_card.get("setup_matrix_weight", 0.0), 0.0),
                "plate_phase_confidence": self._safe_float(signal_card.get("plate_phase_confidence", 0.0), 0.0),
            },
            meta=self._snapshot_meta(
                code=code,
                plate_id=primary_plate_id,
                signal_id=signal_id,
                emotion_phase_key=f"market:emotion_phase:{today_str}",
                plate_phase_detail_key=f"market:plate_phase_detail:{today_str}",
                ab_key=f"market:ab_arbitrage:{today_str}",
                plan_key=f"market:plan:open_verify:{today_str}",
                storage={"plan_key": f"market:plan:open_verify:{today_str}"},
                flags={"setup_type": str(signal_card.get("setup_type", ""))},
            ),
        )

    def _build_market_snapshot(
        self,
        *,
        today_str: str,
        run_id: str,
        payload: Dict[str, Any],
        emotion_result: EmotionPhaseResult,
        last_phase: str,
        st_score: float,
        max_lb_today: int,
        plate_consensus: float,
        auction_red_green: float,
        effectiveness: float,
        fade_count: int,
        rise_count: int,
        one_word_break_rate: float,
        seal_ratio_front20: float,
    ) -> StandardSnapshot:
        ts_ms = int(time.time() * 1000)
        leader_candidates = emotion_result.leader_candidates[:3]
        plate_phase_map_top = dict(list((emotion_result.plate_phase_map or {}).items())[:5])
        return StandardSnapshot(
            snapshot_id=self._build_snapshot_id(today_str, run_id, "MarketSnapshot", "market"),
            snapshot_type="MarketSnapshot",
            date=today_str,
            ts_ms=ts_ms,
            time_str=self._format_time_str(ts_ms),
            run_id=run_id,
            module="calculate_expectation_eval",
            engine_version=MARKET_EDGE_ENGINE_VERSION,
            snapshot_schema_version=SNAPSHOT_SCHEMA_VERSION,
            inputs={
                "auction_sample_size": self._safe_int(payload.get("sample_size", 0), 0),
                "effectiveness": round(effectiveness, 4),
                "fade_count": fade_count,
                "rise_count": rise_count,
                "one_word_break_rate": round(one_word_break_rate, 4),
                "seal_ratio_front20": round(seal_ratio_front20, 4),
                "max_lb_today": self._safe_int(max_lb_today, 1),
                "auction_red_green": round(auction_red_green, 4),
                "plate_consensus": round(plate_consensus, 4),
                "st_score": round(st_score, 4),
                "last_phase": last_phase,
            },
            outputs={
                "emotion_phase": emotion_result.emotion_phase,
                "phase_confidence": emotion_result.phase_confidence,
                "transition_reason_code": emotion_result.transition_reason_code,
                "position_cap": emotion_result.position_cap,
                "allowed_setups": list(emotion_result.allowed_setups),
                "blocked_setups": list(emotion_result.blocked_setups),
                "global_fakeout_penalty": emotion_result.global_fakeout_penalty,
                "leader_candidates_top3": leader_candidates,
                "plate_phase_map_top": plate_phase_map_top,
            },
            reasons={
                "predict_market_phase_inputs": {
                    "st_score": round(st_score, 4),
                    "red_green_ratio": round(auction_red_green, 4),
                    "max_lb": self._safe_int(max_lb_today, 1),
                    "consensus_score": round(plate_consensus, 4),
                    "effectiveness": round(effectiveness, 4),
                    "fade_count": fade_count,
                    "one_word_break_rate": round(one_word_break_rate, 4),
                    "seal_ratio_front20": round(seal_ratio_front20, 4),
                    "last_phase": last_phase,
                },
                "phase_age_days": emotion_result.phase_age_days,
                "phase_age_intraday_bars": emotion_result.phase_age_intraday_bars,
                "transition_reason_code": emotion_result.transition_reason_code,
            },
            meta=self._snapshot_meta(
                emotion_phase_key=f"market:emotion_phase:{today_str}",
                storage={"diag_key": f"diag:expectation_eval:{today_str}"},
                flags={},
            ),
        )

    def _build_execution_snapshot(
        self,
        *,
        today_str: str,
        run_id: str,
        phase: str,
        position_cap: float,
        allowed_setups: List[str],
        blocked_setups: List[str],
        fear_greed_score: float,
        resonance_score: float,
        process_state: str,
        process_risk_strength: float,
        danger_count: int,
        active_setup_weights: Dict[str, float],
        avg_setup_weight: float,
        policy: ExecutionPolicy,
        action: str,
        risk_level: str,
        reason_list: List[str],
    ) -> StandardSnapshot:
        ts_ms = int(time.time() * 1000)
        return StandardSnapshot(
            snapshot_id=self._build_snapshot_id(today_str, run_id, "ExecutionSnapshot", "execution"),
            snapshot_type="ExecutionSnapshot",
            date=today_str,
            ts_ms=ts_ms,
            time_str=self._format_time_str(ts_ms),
            run_id=run_id,
            module="calculate_execution_policy",
            engine_version=MARKET_EDGE_ENGINE_VERSION,
            snapshot_schema_version=SNAPSHOT_SCHEMA_VERSION,
            inputs={
                "emotion_phase": phase,
                "position_cap": round(position_cap, 4),
                "allowed_setups": list(allowed_setups or []),
                "blocked_setups": list(blocked_setups or []),
                "fear_greed_score": round(fear_greed_score, 4),
                "resonance_score": round(resonance_score, 4),
                "process_state": process_state,
                "process_risk_strength": round(process_risk_strength, 4),
                "danger_count": self._safe_int(danger_count, 0),
                "pattern_setup_weights": dict(active_setup_weights or {}),
                "pattern_avg_setup_weight": round(avg_setup_weight, 4),
            },
            outputs={
                "position_max": self._safe_float(policy.position_max, 0.0),
                "mode_allow": list(policy.mode_allow or []),
                "ban_conditions": list(policy.ban_conditions or []),
                "risk_budget": dict(policy.risk_budget or {}),
                "action": action,
                "risk_level": risk_level,
            },
            reasons={
                "policy_explain": dict(policy.explain or {}),
                "action": action,
                "risk_level": risk_level,
                "reason_list": list(reason_list or []),
            },
            meta=self._snapshot_meta(
                emotion_phase_key=f"market:emotion_phase:{today_str}",
                execution_key=f"market:execution_policy:{today_str}",
                storage={"execution_key": f"market:execution_policy:{today_str}"},
                flags={"pattern_matrix_observation_only": True},
            ),
        )

    async def _load_plate_phase_detail(self, today_str: str, plate_id: str) -> Dict[str, Any]:
        if not plate_id:
            return {}
        detail_raw = await self.redis.hgetall(f"market:plate_phase_detail:{today_str}") or {}
        if plate_id in detail_raw:
            return self._safe_json_dict(detail_raw.get(plate_id, {}))

        spread_details_raw = await self.redis.hgetall(f"rank:plate_spread:details:{today_str}") or {}
        attitude_details_raw = await self.redis.hgetall(f"rank:plate_attitude:details:{today_str}") or {}
        profile_details_raw = await self.redis.hgetall(f"rank:plate_profile:details:{today_str}") or {}
        kaipan_by_id = await self._get_kaipan_plate_by_id_cached()
        return self._calculate_plate_emotion_state(
            plate_id,
            today_str,
            spread_detail=self._safe_json_dict(spread_details_raw.get(plate_id, {})),
            attitude_detail=self._safe_json_dict(attitude_details_raw.get(plate_id, {})),
            profile_detail=self._safe_json_dict(profile_details_raw.get(plate_id, {})),
            kaipan_info=(kaipan_by_id or {}).get(plate_id),
        )

    async def _ensure_plate_snapshot(
        self,
        today_str: str,
        plate_id: str,
        run_id: str,
        *,
        module: str,
        is_consumed_plate: bool = False,
    ) -> None:
        if not plate_id or await self._is_plate_snapshot_seen(today_str, plate_id):
            return
        detail = await self._load_plate_phase_detail(today_str, plate_id)
        if not detail or not detail.get("plate_id"):
            return
        await self._emit_snapshot(
            self._build_plate_snapshot(
                today_str=today_str,
                run_id=run_id,
                module=module,
                detail=detail,
                is_consumed_plate=is_consumed_plate,
            )
        )
        await self._mark_plate_snapshot_seen(today_str, plate_id)
        return
        self.last_resonance_update: float = 0.0
        self.last_analysis_universe_update: float = 0.0
        self.last_market_overview_update: float = 0.0
        self.last_stock_profile_update: float = 0.0
        self.last_plate_snapshot_update: float = 0.0
        self.last_plate_profile_update: float = 0.0
        self.last_market_process_profile_update: float = 0.0
        self.last_expectation_eval_update: float = 0.0
        self.last_strategy_tags_update: float = 0.0
        self.last_ab_arbitrage_update: float = 0.0
        self.last_preopen_plan_update: float = 0.0
        self.last_open_verify_plan_update: float = 0.0
        self.last_data_contract_check_update: float = 0.0
        # Startup Diagnostic
        logger.info(f"🚀 Market Edge Engine Started: FETCH_API={UnifiedMarketDataFetcher is not None}, CUR_DIR={os.getcwd()}")
        # 平滑操作建议状态
        self.last_trading_action: str = "WAIT"
        self.action_stable_count: int = 0
        self.last_position_max: float = 0.0

        # 板块建议稳定性追踪 (Mainline Hysteresis)
        self.plate_recommendation_history: Dict[str, int] = {} # plate_id -> continuous_cycles
        self.active_long_plates: List[str] = []
        self.action_stable_count: int = 0
        
        # 调度器 (APScheduler 分层架构)
        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
        self.sys_health_status = {"data_integrity": True, "reason": "ok"}


        # 调试/回放模式：手动指定日期
        self.manual_date: Optional[str] = None
        self.auction_profile_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        self.stock_state_cache: Dict[str, Dict[str, Any]] = {}
        self.plate_weight_cache: Dict[str, List[Tuple[str, float]]] = {}
        self.static_stock_to_plates: Dict[str, List[str]] = {}
        self.static_plate_info: Dict[str, Dict[str, Any]] = {}
        self.last_static_plate_sync: float = 0.0
        self.chip_peaks: Dict[str, Dict[str, Any]] = {}
        self.last_chip_peaks_sync: float = 0.0
        self.stock_extra: Dict[str, Dict[str, Any]] = {}   # 盘后多因子: change_pct_5d, avg_turnover_5d, limit_up_days_5...
        self.last_stock_extra_sync: float = 0.0
        self.precomputed_static_date: Optional[str] = None
        self.intraday_transition_seen: Dict[str, int] = {}
        self.return_history: Dict[str, List[float]] = {}
        self.leading_plate_history: List[Tuple[int, str]] = []
        self.log_last_payload: Dict[str, str] = {}
        self.log_last_ts: Dict[str, float] = {}
        self.analysis_universe_cache: Set[str] = set()
        self.profile_transition_seen: Dict[str, int] = {}
        self.code_change_history: Dict[str, List[Tuple[int, float]]] = {}
        # 日级批处理控制：避免盘中重负载
        self.pending_eod_calc: bool = False
        self.daily_profiles_done_for: str = ""
        # Runtime caches (reduce repeated Redis fetch in one loop window)
        self._auction_cache: Dict[str, Dict[str, Any]] = {}
        self._first_limit_cache: Dict[str, Any] = {"ts": 0.0, "codes": set()}
        self._quote_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._kaipan_plate_cache: Dict[str, Any] = {"ts": 0.0, "by_id": {}, "count": 0}
        self.kaipan_plate_cache_ttl_sec: int = int(os.getenv("KAIPAN_PLATE_CACHE_TTL_SEC", "60"))
        self.enable_kaipan_plate_blend: bool = os.getenv("ENABLE_KAIPAN_PLATE_BLEND", "1") == "1"
        self.kaipan_plate_blend_weight: float = float(os.getenv("KAIPAN_PLATE_BLEND_WEIGHT", "0.35"))

        # 分层频率（秒）：验证模式（低频）
        # 目标：先验证逻辑与流程，不追求高频实时性
        self.task_intervals: Dict[str, int] = {
            "candidate_pool": 300,
            "indicators": 60,
            "plate_spread": 300,
            "theme_rank": 300,
            "comfort_exit": 300,
            "sentiment": 300,
            "stock_rank": 300,
            "intraday_state": 60,
            "plate_attitude": 60,
            "execution_policy": 60,
            "static_precompute": 1800,
            "fear_greed": 300,
            "herding": 300,
            "resonance": 300,
            "analysis_universe": 300,
            "market_overview": 300,
            "stock_profile": 300,
            "plate_snapshot": 300,
            "plate_profile": 300,
            "market_process_profile": 300,
            "expectation_eval": 1800,  # 30-minute intraday refresh for meaningful fade/rise updates
            "strategy_tags": 300,
            "ab_arbitrage": 180,
            "preopen_plan": 120,
            "open_verify_plan": 120,
            "data_contract_check": 300,
        }

    async def build_candidate_pool(self, today_str: str) -> Set[str]:
        candidate_pool: Set[str] = set()

        # 1) 竞价 0925 TopN
        try:
            top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
            for item in top_amount_list:
                code = item.get("symbol")
                if code and len(code) == 6:
                    candidate_pool.add(code)
        except Exception:
            pass

        # 2) 严格首板池 stock:first_limit_up
        try:
            candidate_pool.update(await self._get_first_limit_codes_cached())
        except Exception:
            pass

        # 3) 昨日涨停 limit_up_{prev_day}
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)
            limit_up_key = f"limit_up_{prev_day}"
            prev_limit_up_data = self.redis_storage.get_data(limit_up_key)
            if prev_limit_up_data:
                for item in prev_limit_up_data:
                    if isinstance(item, dict):
                        code = item.get('股票代码', '') or item.get('code', '')
                        if code and len(code) == 6:
                            candidate_pool.add(code)
        except Exception:
            pass

        # 截断（保持稳定）
        if len(candidate_pool) > self.candidate_pool_size:
            candidate_pool = set(list(candidate_pool)[: self.candidate_pool_size])
            
        # 如果池子还是空的，不管是回放还是实盘，都必须从大版块兜底捞票
        if not candidate_pool:
            logger.warning("⚠️ 竞价核心数据或候选池为空，触发兜底机制从大版块补充股票...")
            # 简单取几个活跃板块的成分股
            main_plates = [k for k, info in self.static_plate_info.items() if info.get("type", "") == "main"]

            count = 0
            # Convert keys to list for indexing
            mp_list = list(main_plates)
            if mp_list:
                for pid in mp_list[:5]:
                    stocks = self.plate_updater.plate_to_stocks.get(pid, [])
                    for s in stocks:
                        candidate_pool.add(s)
                        count += 1
                        if count >= 100:
                            break
                    if count >= 100:
                        break

        cache_key = f"cache:candidate_pool:{today_str}"
        await self.redis.set(cache_key, json.dumps(list(candidate_pool), ensure_ascii=False), ex=86400)
        self._log_event("candidate_pool", f"🏊 候选池更新: {len(candidate_pool)} 只股票", min_interval_sec=300)
        return candidate_pool

    def _safe_pct_from_quote(self, quote: Dict[str, Any], key: str, pre_close: float, fallback: float) -> float:
        v = self._safe_float(quote.get(key), fallback)
        if v and abs(v) < 1e-8:
            return v
        # 部分行情源没有 high/low 的涨幅，只给绝对价格
        price_key = key.replace("_pct", "")
        if pre_close > 0:
            abs_px = self._safe_float(quote.get(price_key), 0.0)
            if abs_px > 0:
                return (abs_px - pre_close) / pre_close * 100.0
        return v

    def _current_phase(self) -> str:
        now = datetime.now().time()
        if now < datetime.strptime("09:15", "%H:%M").time():
            return "pre_open"
        if now < datetime.strptime("09:30", "%H:%M").time():
            return "auction"
        if now <= datetime.strptime("09:40", "%H:%M").time():
            return "opening"
        if now < datetime.strptime("11:30", "%H:%M").time():
            return "intraday_am"
        if now < datetime.strptime("13:00", "%H:%M").time():
            return "lunch_break"
        if now <= datetime.strptime("15:00", "%H:%M").time():
            return "intraday_pm"
        if now <= datetime.strptime("18:10", "%H:%M").time():
            return "post_close"
        return "evening"

    def _normalize_change_pct(self, v: Any, scale: float = 100.0) -> float:
        """格式标准化：将原始值转换为百分比点数 (如 5.0)。
        如果源是比例 (0.05)，则 scale=100.0；如果已经是百分比 (5.0)，则 scale=1.0。
        """
        x = self._safe_float(v, 0.0)
        return float(x * scale)

    def _normalize_change_pct_auto(
        self,
        v: Any,
        *,
        price: Optional[float] = None,
        pre_close: Optional[float] = None,
    ) -> float:
        """
        自动归一化涨跌幅到“百分比点数”（如 5.0）。
        优先用 price / pre_close 计算；否则根据数值范围进行推断。
        """
        x = self._safe_float(v, 0.0)
        p = self._safe_float(price, 0.0) if price is not None else 0.0
        pc = self._safe_float(pre_close, 0.0) if pre_close is not None else 0.0
        if p > 0 and pc > 0:
            return float((p - pc) / pc * 100.0)
        # 推断：A 股常见波动区间内，比例值通常在 0.3 以内
        if abs(x) <= 0.3:
            return float(x * 100.0)
        if abs(x) <= 1.0:
            # 更像是已经是百分比（例如 0.5%）
            return float(x)
        return float(x)

    def _normalize_auction_item(self, it: Dict[str, Any]) -> Dict[str, Any]:
        """???????????????????? 100x ???"""
        symbol = str(it.get('symbol') or it.get('code') or it.get('????') or '').strip()
        name = str(it.get('name') or it.get('????') or '')
        price = it.get('price')
        pre_close = it.get('pre_close') or it.get('last_close')
        change_raw = it.get('change_pct', it.get('bid_change_pct', 0.0))
        change_pct = self._normalize_change_pct_auto(change_raw, price=price, pre_close=pre_close)
        auction_amt = self._safe_float(
            it.get('auction_amount_yuan', it.get('auction_amount', it.get('amount', 0.0))), 0.0
        )
        bid_amt = self._safe_float(it.get('bid_amount_yuan', it.get('bid_amount', auction_amt)), 0.0)
        return {
            'symbol': symbol,
            'name': name,
            'change_pct': change_pct,
            'auction_amount_yuan': auction_amt,
            'bid_amount_yuan': bid_amt,
        }


    def _normalize_auction_list(self, raw_list: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw_list, list):
            return []
        out: List[Dict[str, Any]] = []
        for it in raw_list:
            if not isinstance(it, dict):
                continue
            item = self._normalize_auction_item(it)
            if item["symbol"] and len(item["symbol"]) == 6:
                out.append(item)
        out.sort(key=lambda x: x.get("auction_amount_yuan", 0.0), reverse=True)
        return out

    async def _load_auction_top_amount(self, today_str: str, require_final_0925: bool = False) -> List[Dict[str, Any]]:
        """Load auction list with key compatibility and 09:25 stabilization delay."""
        date_compact = today_str.replace("-", "")
        now = datetime.now()
        diag_key = f"diag:auction_source:{today_str}"

        async def _write_diag(source: str, count: int) -> None:
            try:
                await self.redis.hset(
                    diag_key,
                    mapping={
                        "ts": int(time.time() * 1000),
                        "source": source,
                        "count": int(count),
                        "require_final_0925": int(require_final_0925),
                    },
                )
                await self.redis.expire(diag_key, 7 * 24 * 3600)
            except Exception:
                pass

        # 09:25 completion data can arrive a few seconds late.
        if require_final_0925 and self.manual_date is None and today_str == now.strftime("%Y-%m-%d"):
            t = now.time()
            if datetime.strptime("09:25:00", "%H:%M:%S").time() <= t < datetime.strptime("09:25:08", "%H:%M:%S").time():
                self._log_event(
                    "auction_0925_wait",
                    "等待09:25竞价快照稳定（延时几秒后重试）",
                    min_interval_sec=15,
                    log_on_change=False,
                )
                await _write_diag("wait_0925_stabilizing", 0)
                return []

        # 1) canonical hash snapshot
        key_0925 = f"market:auction:{date_compact}:0925"
        try:
            raw = await self.redis.hget(key_0925, "top_amount")
            if raw:
                items = self._normalize_auction_list(json.loads(raw))
                if items:
                    await _write_diag(f"hash:{key_0925}", len(items))
                    return items
        except Exception as e:
            pass

        # 2) latest pointer hash
        latest_key = f"market:auction:{date_compact}:latest"
        try:
            latest = await self.redis.hgetall(latest_key)
            if latest:
                tag = latest.get("tag") or latest.get(b"tag")
                if tag:
                    tag_str = tag.decode("utf-8") if isinstance(tag, bytes) else str(tag)
                    snap_key = f"market:auction:{date_compact}:{tag_str}"
                    raw = await self.redis.hget(snap_key, "top_amount")
                    if raw:
                        items = self._normalize_auction_list(json.loads(raw))
                        if items:
                            await _write_diag(f"latest:{snap_key}", len(items))
                            return items
        except Exception as e:
            pass

        # 3) fallback snapshots (ordered by quality/recency)
        for tag in ("0925", "0924", "0920", "wencai"):  # Put 'wencai' last to prioritize real snapshots
            try:
                snap_key = f"market:auction:{date_compact}:{tag}"
                raw = await self.redis.hget(snap_key, "top_amount")
                if raw:
                    items = self._normalize_auction_list(json.loads(raw))
                    if items:
                        await _write_diag(f"fallback:{snap_key}", len(items))
                        return items
            except Exception:
                continue

        # 4) replay string key
        try:
            replay_key = f"market:auction:{today_str}:0925"
            try:
                # FIRST TRY HASH
                raw = await self.redis.hget(replay_key, "top_amount")
                if not raw:
                    raise Exception("No HASH top_amount")
            except Exception:
                try:
                    # THEN TRY STRING as fallback
                    raw = await self.redis.get(replay_key)
                except Exception:
                    raw = None

            if raw:
                items = self._normalize_auction_list(json.loads(raw))
                if items:
                    await _write_diag(f"replay:{replay_key}", len(items))
                    return items
        except Exception as e:
            pass

        # 4.5) Active Wencai fetch (Preferred fallback before Quote construction)
        # 仅在所有 Redis 竞价 Key 都缺失且达到硬时间限制时触发
        is_today = (today_str == datetime.now().strftime("%Y-%m-%d"))
        
        # Debug info
        diagnostic_msg = f"[DIAGNOSTIC] _load_auction_top_amount: date={today_str}, is_today={is_today}, manual_date={self.manual_date}, fetcher_ready={UnifiedMarketDataFetcher is not None}"
        logger.info(diagnostic_msg)
        
        now = datetime.now()
        cur_time_str = now.strftime("%H:%M")
        cur_sec = now.second
        
        # 用户需求：9点25分10秒获取不到数据就开始请求
        is_auction_late = (cur_time_str == "09:25" and cur_sec >= 10)
        is_opening_active = ("09:26" <= cur_time_str <= "15:30")

        if (
            UnifiedMarketDataFetcher 
            and (is_today or self.manual_date)
            and (is_opening_active or is_auction_late or self.manual_date)
        ):
            try:
                # 检查冷却与锁 (Internal Lock + Fail Count)
                # 连续两次失败后中断 (熔断机制)
                if self._wencai_fail_count < 2 and not self._wencai_fetch_lock.locked():
                    # 触发异步背景任务
                    asyncio.create_task(self._async_wencai_auction_supplement(today_str, date_compact))
            except Exception as e:
                logger.error(f"❌ 问财竞价数据兜底触发异常: {e}")

        # 5) Quote fallback...


        # 5) Quote fallback: 如果以上所有竞价数据源都缺失，从实时行情构建基础数据
        try:
            candidate_pool = self.candidate_pool_cache
            if candidate_pool:
                fallback_items = []
                pipe = self.redis.pipeline()
                codes_list = list(candidate_pool)[:100]
                for c in codes_list:
                    pipe.hgetall(f"stock:quote:{c}")
                all_quotes = await pipe.execute()
                for i, q in enumerate(all_quotes):
                    if not q:
                        continue
                    change_pct = self._safe_float(q.get("change_pct") or q.get("change", 0), 0.0)
                    amount = self._safe_float(q.get("amount", 0), 0.0)
                    if amount > 0:
                        fallback_items.append({
                            "symbol": codes_list[i],
                            "change_pct": change_pct,
                            "auction_amount_yuan": amount,
                            "bid_amount_yuan": amount * 0.1,  # 粗略估算
                        })
                if fallback_items:
                    fallback_items.sort(key=lambda x: x.get("auction_amount_yuan", 0), reverse=True)
                    await _write_diag("quote_fallback", len(fallback_items))
                    logger.info(f"⚠️ 竞价快照缺失，使用候选池实时行情兜底 ({len(fallback_items)} 只)")
                    return fallback_items[:50]
        except Exception:
            pass

        logger.warning(f"⚠️ 竞价数据为空: 已检查 market:auction:{date_compact}:0925/latest/wencai/0924/0920, 均无数据。请检查 C++ t1.exe 是否在写入竞价快照。")
        await _write_diag("none", 0)
        return []


    async def _async_wencai_auction_supplement(self, today_str: str, date_compact: str):
        """Background task for Wencai auction supplement with retry and downstream trigger."""
        if not UnifiedMarketDataFetcher:
            return

        async with self._wencai_fetch_lock:
            # 1. Check if we ALREADY have good data (native source might have filled it while we were waiting for the lock)
            key_0925 = f"market:auction:{date_compact}:0925"
            try:
                existing_raw = await self.redis.hget(key_0925, "top_amount")
                if existing_raw:
                    existing_items = json.loads(existing_raw)
                    if existing_items and existing_items[0].get("source") != "wencai_sync_reactive":
                        logger.info("✅ [Wencai Sync] Native auction data detected before fetch. Aborting supplement.")
                        return
            except Exception: pass

            logger.warning(f"📉 [Wencai Sync] 竞价数据源缺失 ({today_str})，启动异步抓取流水线...")
            
            fetcher = UnifiedMarketDataFetcher()
            # 使用信号量限制并发，防止分段请求瞬间挤占核心主环资源
            sem = asyncio.Semaphore(1)
            
            mc_segments = [
                f"{today_str}市值>1000亿;竞价涨跌幅;竞价金额;竞价金额>500万;竞价未匹配金额",
                f"{today_str}市值500亿到1000亿;竞价涨跌幅;竞价金额;竞价金额>500万;竞价未匹配金额",
                f"{today_str}市值200亿到500亿;竞价涨跌幅;竞价金额;竞价金额>500万;竞价未匹配金额",
                f"{today_str}市值100亿到200亿;竞价涨跌幅;竞价金额;竞价金额>500万;竞价未匹配金额",
                f"{today_str}市值50亿到100亿;竞价涨幅>0.5%;竞价金额;竞价金额>500万;竞价未匹配金额",
                f"{today_str}市值<50亿;竞价涨幅>1.5%;竞价金额;竞价金额>500万;竞价未匹配金额"
            ]

            async def fetch_seg(i, q):
                async with sem:
                    try:
                        if i > 0: await asyncio.sleep(1.5)
                        # P0: Add strict timeout to prevent event loop starvation
                        df_seg = await asyncio.wait_for(
                            fetcher._get_wencai_stocks(q, loop=True, return_df=True, max_stocks=1000),
                            timeout=45.0
                        )
                        return df_seg
                    except asyncio.TimeoutError:
                        logger.error(f"⌛ [Wencai Sync] 分档 {i+1} 请求超时 (45s)")
                    except Exception as e:
                        logger.error(f"❌ [Wencai Sync] 分档 {i+1} 请求失败: {e}")
                    return None

            items = []
            success = False
            for attempt in range(2):
                try:
                    # Sequential execution via Semaphore(1) is safer for engine stability
                    tasks = [fetch_seg(i, q) for i, q in enumerate(mc_segments)]
                    results = await asyncio.gather(*tasks)
                    all_raw_items = [res for res in results if res is not None and not res.empty]
                    
                    if all_raw_items:
                        df = pd.concat(all_raw_items, ignore_index=True)
                        cols = list(df.columns)
                        def pick_col(keys):
                            for c in cols:
                                if all(k in str(c) for k in keys): return c
                            return None
                        
                        amt_col = pick_col(["竞价金额"]) or pick_col(["开盘成交额"])
                        chg_col = pick_col(["竞价", "涨跌幅"]) or pick_col(["开盘", "涨跌幅"])
                        bid_col = pick_col(["竞价未匹配金额"]) or pick_col(["竞价", "未匹配"])
                        symbol_col = "code" if "code" in cols else (pick_col(["股票代码"]) or cols[0])
                        
                        seen_codes = set()
                        records = df.to_dict('records')
                        for i, row in enumerate(records):
                            if i > 0 and i % 300 == 0: await asyncio.sleep(0.01) # Yield to event loop
                            code = str(row.get(symbol_col, "")).split(".")[0]
                            if len(code) > 6: code = code[-6:]
                            if len(code) != 6: continue
                            if code in seen_codes: continue
                            seen_codes.add(code)
                            amt = float(row.get(amt_col, 0) or 0)
                            items.append({
                                "symbol": code,
                                "change_pct": float(row.get(chg_col, 0) or 0),
                                "auction_amount_yuan": amt,
                                "bid_amount_yuan": float(row.get(bid_col, 0) or 0) or (amt * 0.1),
                                "source": "wencai_sync_reactive"
                            })
                        
                        if items:
                            items.sort(key=lambda x: x["auction_amount_yuan"], reverse=True)
                            success = True
                            self._wencai_fail_count = 0 
                            break
                    
                    if attempt == 0:
                        logger.warning("⚠️ [Wencai Sync] 首轮抓取未获有效数据，10秒后重试...")
                        await asyncio.sleep(10)
                except Exception as e:
                    logger.error(f"❌ [Wencai Sync] 尝试 {attempt+1} 抛出异常: {e}")
                    if attempt == 0: await asyncio.sleep(10)
            
            if success:
                # 2. Redis Conflict Resolution: Only write if native source (t1) hasn't filled it yet
                data_json = json.dumps(items)
                for tag in ("wencai", "0000", "0925", "latest"): # Add 0000 as early bird
                    snap_key = f"market:auction:{date_compact}:{tag}"
                    
                    if tag in ("0925", "latest"):
                        existing_raw = await self.redis.hget(snap_key, "top_amount")
                        if existing_raw:
                            try:
                                existing_items = json.loads(existing_raw)
                                if existing_items and existing_items[0].get("source") != "wencai_sync_reactive":
                                    logger.info(f"⏭️ [Wencai Sync] Key {snap_key} already has native data. Skipping overwrite.")
                                    continue
                            except: pass

                    await self.redis.hset(snap_key, "top_amount", data_json)
                    await self.redis.expire(snap_key, 28800)
                
                # 关键：清除本地缓存，确保下游逻辑立即拾取 Redis 中的新数据
                self._auction_cache.clear()
                
                logger.warning(f"🚀 [Wencai Sync] 补全成功 ({len(items)}只)，开始触发下游推演流水线...")
                
                # 3. 立即触发下游核心分析逻辑
                try:
                    # 路径 A: 竞价预期差评估
                    await self.calculate_expectation_eval(today_str, auction_items=items)
                    # 路径 B: 策略标签与画像生成
                    await self.calculate_strategy_tags(today_str, auction_items=items)
                    # 路径 C: 预案生成 (Signals=0 的解锁关键)
                    await self.build_preopen_plan(today_str, auction_items=items)
                    # 路径 D: 竞价总结写入
                    await self.build_auction_summary(today_str, auction_items=items)
                    
                    logger.warning("✅ [Wencai Sync] 下游流水线触发完毕，竞价阶段逻辑已更新。")
                except Exception as ex:
                    logger.error(f"❌ [Wencai Sync] 触发下游逻辑失败: {ex}")
            else:
                self._wencai_fail_count += 1
                logger.error(f"🚫 [Wencai Sync] 抓取彻底失败 (FailCount={self._wencai_fail_count})，放弃本次竞价补全。")



    async def _get_auction_top_amount_cached(
        self, today_str: str, require_final_0925: bool = False, ttl_sec: int = 5
    ) -> List[Dict[str, Any]]:
        """Short-lived cache for auction snapshot."""
        cache_key = f"{today_str}:{int(require_final_0925)}"
        now_ts = time.time()
        cached = self._auction_cache.get(cache_key)
        if cached and (now_ts - float(cached.get("ts", 0.0)) <= ttl_sec):
            return cached.get("data", [])
        data = await self._load_auction_top_amount(today_str, require_final_0925=require_final_0925)
        self._auction_cache[cache_key] = {"ts": now_ts, "data": data}
        return data

    async def _get_first_limit_codes_cached(self, ttl_sec: int = 5) -> Set[str]:
        """Short-lived cache for strict first-limit set."""
        now_ts = time.time()
        if now_ts - float(self._first_limit_cache.get("ts", 0.0)) <= ttl_sec:
            return set(self._first_limit_cache.get("codes", set()))

        codes: Set[str] = set()
        try:
            items = await self.redis.zrange("stock:first_limit_up", 0, -1)
            for item_json in items:
                try:
                    item = json.loads(item_json)
                    code = str(item.get("symbol") or "").strip()
                    if len(code) == 6:
                        codes.add(code)
                except Exception:
                    continue
        except Exception:
            pass

        self._first_limit_cache = {"ts": now_ts, "codes": codes}
        return codes

    async def _get_auction_profile(self, today_str: str) -> Dict[str, Dict[str, float]]:
        cached = self.auction_profile_cache.get(today_str)
        if cached is not None:
            return cached

        profile: Dict[str, Dict[str, float]] = {}
        try:
            top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
            for idx, it in enumerate(top_amount_list):
                code = it.get("symbol")
                if not code or len(code) != 6:
                    continue
                profile[code] = {
                    "rank": float(idx),
                    "change_pct": self._safe_float(it.get("change_pct", 0), 0.0),
                    "bid_amount_yuan": self._safe_float(it.get("bid_amount_yuan", 0), 0.0),
                }
        except Exception:
            profile = {}

        self.auction_profile_cache[today_str] = profile
        return profile

    async def _fetch_quotes_batch(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch quotes in batch with standardization (P0 Optimization)"""
        if not codes:
            return {}
        uniq_codes = list(dict.fromkeys(codes))
        now_ts = time.time()
        ttl_sec = 2.0

        out: Dict[str, Dict[str, Any]] = {}
        miss: List[str] = []
        for code in uniq_codes:
            c = self._quote_cache.get(code)
            if c and (now_ts - c[0] <= ttl_sec):
                out[code] = c[1]
            else:
                miss.append(code)

        if miss:
            pipe = self.redis.pipeline()
            # P0: Try quote HASH keys
            for code in miss:
                pipe.hgetall(f"stock:quote:{code}")
            rows = await pipe.execute()
            for i, code in enumerate(miss):
                raw_data = rows[i] or {}
                # P2: Standardize format (floats, Yuan, etc)
                data = self.redis_storage._standardize_stock_quote(raw_data, code)
                # ?? change_pct ????????
                cp_raw = data.get("change_pct", data.get("change", 0.0))
                data["change_pct"] = self._normalize_change_pct_auto(
                    cp_raw,
                    price=data.get("price"),
                    pre_close=data.get("pre_close") or data.get("last_close"),
                )
                data["change"] = data.get("change_pct")
                out[code] = data
                self._quote_cache[code] = (now_ts, data)

        return {code: out.get(code, {}) for code in uniq_codes}

    async def build_analysis_universe(self, today_str: str, candidate_pool: Set[str], limit: int = 4000) -> Set[str]:
        """Build a broader universe for market/plate/profile analysis."""
        universe: Set[str] = set(candidate_pool or set())

        # Existing mapping already contains stock<->plate relation; use it first.
        try:
            all_codes = list(getattr(self.plate_updater, "stock_to_plates", {}).keys())
            for code in all_codes:
                if code and len(code) == 6:
                    universe.add(code)
        except Exception:
            pass

        # Auction top list
        try:
            top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
            for item in top_amount_list[:1200]:
                code = item.get("symbol")
                if code and len(code) == 6:
                    universe.add(code)
        except Exception:
            pass

        # First limit pool
        try:
            universe.update(await self._get_first_limit_codes_cached())
        except Exception:
            pass

        # Yesterday limit-up pool
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)
            prev_key = f"limit_up_{prev_day}"
            prev_data = self.redis_storage.get_data(prev_key) or []
            for item in prev_data[:1200]:
                if not isinstance(item, dict):
                    continue
                code = item.get("股票代码", "") or item.get("code", "")
                if code and len(code) == 6:
                    universe.add(code)
        except Exception:
            pass

        # Recent volatile pool
        try:
            volatile_items = await self.redis.zrevrange("stock:volatile_pool", 0, 1200)
            for item_json in volatile_items:
                try:
                    item = json.loads(item_json)
                    code = item.get("symbol")
                    if code and len(code) == 6:
                        universe.add(code)
                except Exception:
                    continue
        except Exception:
            pass

        if len(universe) > limit:
            universe = set(list(universe)[:limit])

        await self.redis.set(
            f"cache:analysis_universe:{today_str}",
            json.dumps(list(universe), ensure_ascii=False),
            ex=86400,
        )
        return universe

    async def _load_static_plate_mappings(self) -> None:
        try:
            now_ts = time.time()
            # 5分钟内不要重复读 Redis，降低并发负担
            if now_ts - self.last_static_plate_sync < 300 and self.static_stock_to_plates:
                return
            s2p_dict = await self.redis.hgetall("config:plate_mapping:s2p")
            info_dict = await self.redis.hgetall("config:plate_mapping:info")
            if s2p_dict:
                self.static_stock_to_plates = {k: json.loads(v) for k, v in s2p_dict.items()}
            if info_dict:
                self.static_plate_info = {k: json.loads(v) for k, v in info_dict.items()}
            self.last_static_plate_sync = now_ts
        except Exception as e:
            logger.warning(f"⚠️ Failed to sync static static plate mappings from Redis: {e}")

    async def _load_chip_peaks_cached(self, today_str: str, allow_prev: bool = True) -> None:
        try:
            now_ts = time.time()
            if now_ts - self.last_chip_peaks_sync < 300 and self.chip_peaks:
                return
            
            # 策略：如果今天没数据，查昨天。
            k1 = f"cache:chip_peaks:{today_str}"
            peaks_dict = await self.redis.hgetall(k1)
            
            if not peaks_dict and allow_prev:
                prev_day = self.calendar.get_previous_trade_day(today_str)
                if prev_day:
                    k2 = f"cache:chip_peaks:{prev_day}"
                    peaks_dict = await self.redis.hgetall(k2)
                    if peaks_dict:
                        logger.info(f"📊 使用前一交易日筹码数据: {prev_day}")

            if peaks_dict:
                self.chip_peaks = {k: json.loads(v) for k, v in peaks_dict.items()}
            self.last_chip_peaks_sync = now_ts
        except Exception as e:
            logger.warning(f"⚠️ Failed to sync chip peaks from Redis: {e}")

    async def _load_stock_extra_cached(self, today_str: str) -> None:
        """加载盘后多因子数据 (change_pct_5d, avg_turnover_5d, limit_up_days_5 等)"""
        try:
            now_ts = time.time()
            if now_ts - self.last_stock_extra_sync < 300 and self.stock_extra:
                return
                
            k1 = f"cache:stock_extra:{today_str}"
            extra_dict = await self.redis.hgetall(k1)
            
            if not extra_dict:
                prev_day = self.calendar.get_previous_trade_day(today_str)
                if prev_day:
                    k2 = f"cache:stock_extra:{prev_day}"
                    extra_dict = await self.redis.hgetall(k2)
                    if extra_dict:
                        logger.info(f"📊 使用前一交易日多因子数据: {prev_day}")

            if extra_dict:
                self.stock_extra = {k: json.loads(v) for k, v in extra_dict.items()}
                logger.info(f"📊 已同步盘后多因子数据: {len(self.stock_extra)} 只股票")
            self.last_stock_extra_sync = now_ts
        except Exception as e:
            logger.warning(f"⚠️ Failed to sync stock extra from Redis: {e}")

    def _weighted_plates_for_code(self, code: str) -> List[Tuple[str, float]]:
        weighted = self.plate_weight_cache.get(code)
        if weighted:
            return weighted
        pids = self.static_stock_to_plates.get(code, []) or []
        if not pids:
            return []
        w = 1.0 / len(pids)
        return [(pid, w) for pid in pids]

    def _build_weighted_plate_map(self, universe: Set[str]) -> Dict[str, List[Tuple[str, float]]]:
        weighted: Dict[str, List[Tuple[str, float]]] = {}
        for code in universe:
            pids = self.static_stock_to_plates.get(code, []) or []
            if not pids:
                continue
            w_raw: List[Tuple[str, float]] = []
            total = 0.0
            for pid in pids:
                info = self.static_plate_info.get(pid, {})
                is_main = info.get("type") == "main"
                w = 1.2 if is_main else 1.0
                total += w
                w_raw.append((pid, w))
            if total <= 0:
                continue
            weighted[code] = [(pid, w / total) for pid, w in w_raw]
        return weighted

    def _theme_match_plate(self, theme: str, plate_name: str) -> bool:
        if not theme or not plate_name:
            return False
        t = str(theme).strip().lower()
        p = str(plate_name).strip().lower()
        if not t or not p:
            return False
        return t in p or p in t

    async def _build_weighted_plate_map_dynamic(
        self, today_str: str, universe: Set[str]
    ) -> Tuple[Dict[str, List[Tuple[str, float]]], Dict[str, List[Dict[str, Any]]]]:
        """动态板块归属权重：
        使用现有事实层（plate_spread/plate_attitude/plate_metrics/theme_evidence）解决个股多板块冲突。
        """
        if not universe:
            return {}, {}

        # 1) 预读取全局板块强弱（扩散+态度）
        spread_pairs = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 500, withscores=True)
        spread_map: Dict[str, float] = {pid: float(score) for pid, score in spread_pairs}
        max_spread = max(spread_map.values()) if spread_map else 1.0

        att_pairs = await self.redis.zrange(f"rank:plate_attitude:{today_str}", 0, -1, withscores=True)
        attitude_map: Dict[str, float] = {pid: float(score) for pid, score in att_pairs}
        max_att = max(abs(v) for v in attitude_map.values()) if attitude_map else 1.0

        # 2) 批量读个股题材证据
        evidence_key = f"cache:stock_theme_evidence:{today_str}"
        pipe = self.redis.pipeline()
        for code in universe:
            pipe.hget(evidence_key, code)
        ev_list = await pipe.execute()
        evidence_map: Dict[str, Dict[str, Any]] = {}
        for idx, code in enumerate(list(universe)):
            raw = ev_list[idx]
            if not raw:
                continue
            try:
                evidence_map[code] = json.loads(raw)
            except Exception:
                continue

        # 2.5) 开盘啦板块热度（当日主线纠偏）
        kaipan_by_id = await self._get_kaipan_plate_by_id_cached()
        kp_total = max(1, len(kaipan_by_id))

        # 3) 对每只股票做归属打分
        weighted: Dict[str, List[Tuple[str, float]]] = {}
        explain_map: Dict[str, List[Dict[str, Any]]] = {}
        for code in universe:
            pids = self.plate_updater.stock_to_plates.get(code, []) or []
            if not pids:
                continue

            ev = evidence_map.get(code, {})
            primary_theme = str(ev.get("primary_theme", "") or "")

            score_items: List[Tuple[str, float]] = []
            score_detail_items: List[Dict[str, Any]] = []
            has_sub_plate = any(self.plate_updater.all_plates.get(pid, {}).get("type") == "sub" for pid in pids)
            for pid in pids:
                pinfo = self.plate_updater.all_plates.get(pid, {})
                pname = pinfo.get("name", pid)
                ptype = pinfo.get("type", "unknown")

                # 基础分：主行业优先，避免子题材在交叉概念中反客为主
                base = 1.25 if ptype == "main" else 0.92
                if ptype == "main" and has_sub_plate:
                    base *= 0.95

                # 扩散热度分（0~1）
                spread_score = spread_map.get(pid, 0.0)
                spread_norm = max(0.0, min(1.0, spread_score / max(1e-6, max_spread)))

                # 板块态度分（-1~1 -> 0~1）
                att = attitude_map.get(pid, 0.0)
                att_norm = max(0.0, min(1.0, 0.5 + 0.5 * (att / max(1e-6, max_att))))

                # 当前板块涨幅分（-2~+2线性到0~1）
                pm = self.plate_updater.get_plate_metrics(pid) or {}
                pchg = self._safe_float(pm.get("change_pct", 0.0), 0.0)
                pchg_norm = max(0.0, min(1.0, (pchg + 2.0) / 4.0))

                # 题材匹配分（降低过强放大，避免单一概念把行业主线压下去）
                theme_boost = 1.0
                if primary_theme and self._theme_match_plate(primary_theme, pname):
                    theme_boost = 1.35

                # 开盘啦热度纠偏（当日炒作主线）
                kp = kaipan_by_id.get(pid)
                kp_rank = int(self._safe_float((kp or {}).get("rank", kp_total), kp_total))
                kp_rank = max(1, min(kp_total, kp_rank))
                kp_rank_pct = 1.0 - (kp_rank - 1) / max(1, kp_total - 1)
                kp_boost = 1.0 + 0.20 * kp_rank_pct if kp else 1.0

                # 4.1) 垃圾属性/烂大街板块惩罚 (Phase E)
                generic_penalty = 1.0
                if any(kw in pname for kw in self.GENERIC_PLATE_KEYWORDS):
                    generic_penalty = 0.15 
                
                # 4.2) 规模/稀释惩罚 (Broadness Penalty)
                # 越大的板块，信号越稀释。150只表为盈亏平衡点，再大则线性折减。
                plate_stock_count = len(self.plate_updater.plate_to_stocks.get(pid, []))
                broadness_penalty = 1.0
                if plate_stock_count > 150:
                    broadness_penalty = max(0.2, 120.0 / plate_stock_count)

                # 综上所述综合打分
                score = (
                    base * 0.45
                    + spread_norm * 0.20
                    + att_norm * 0.15
                    + pchg_norm * 0.20
                ) * theme_boost * kp_boost * generic_penalty * broadness_penalty
                final_score = max(0.0001, float(score))
                score_items.append((pid, final_score))
                score_detail_items.append(
                    {
                        "plate_id": pid,
                        "plate_name": pname,
                        "plate_type": ptype,
                        "base": round(base, 6),
                        "spread_norm": round(spread_norm, 6),
                        "att_norm": round(att_norm, 6),
                        "pchg_norm": round(pchg_norm, 6),
                        "theme_boost": round(theme_boost, 6),
                        "kaipan_rank": kp_rank if kp else 0,
                        "kaipan_boost": round(kp_boost, 6),
                        "generic_penalty": round(generic_penalty, 6),
                        "broadness_penalty": round(broadness_penalty, 6),
                        "plate_stock_count": plate_stock_count,
                        "raw_score": round(final_score, 6),
                    }
                )

            total = sum(s for _, s in score_items)
            if total <= 0:
                n = len(score_items)
                weighted[code] = [(pid, 1.0 / n) for pid, _ in score_items]
            else:
                # 截断长尾，避免一个股票被太多板块分散（保留top3）
                score_items.sort(key=lambda x: x[1], reverse=True)
                score_items = score_items[:3]
                total2 = sum(s for _, s in score_items)
                weighted[code] = [(pid, s / total2) for pid, s in score_items]
            if score_detail_items:
                score_by_pid = dict(weighted.get(code, []))
                for d in score_detail_items:
                    d["weight"] = round(float(score_by_pid.get(d["plate_id"], 0.0)), 6)
                score_detail_items.sort(key=lambda x: x["weight"], reverse=True)
                explain_map[code] = score_detail_items[:5]

        return weighted, explain_map

    async def precompute_static_context(self, today_str: str, universe: Set[str], force: bool = False) -> None:
        # Load the independent Redis static concepts before computing anything
        await self._load_static_plate_mappings()
        
        # 定期刷新股票名称映射 (每10分钟同步一次或强制同步)
        now_ts = time.time()
        last_refresh = getattr(self, 'last_stock_names_refresh', 0)
        # 避免启动阶段短时间重复刷新（force可能来自不同流程入口）
        if (force and now_ts - last_refresh < 120):
            pass
        elif force or now_ts - last_refresh > 600:
            if hasattr(self.plate_updater, 'refresh_all_stocks_data'):
                try:
                    # 运行在线程池中避免同步IO阻塞主循环
                    await asyncio.get_event_loop().run_in_executor(None, self.plate_updater.refresh_all_stocks_data)
                    self.last_stock_names_refresh = now_ts
                    logger.info(f"✅ Stock names cache refreshed. Count: {len(self.plate_updater.stock_names)}")
                except Exception as e:
                    logger.error(f"❌ Failed to refresh stock names: {e}")
            else:
                self.last_stock_names_refresh = now_ts # Skip if not supported

        if self.precomputed_static_date == today_str and self.plate_weight_cache:
            # 如果强制执行，但新的 universe 里的所有股票都已经有缓存了，依然可以跳过（P0 性能优化）
            missing = [c for c in universe if c not in self.plate_weight_cache]
            if not missing:
                return
            if not force and len(missing) < (len(universe) * 0.1): # 非强制下，缺失比例较小时也跳过
                return

        # 优先动态映射，失败回退静态映射
        explain_map: Dict[str, List[Dict[str, Any]]] = {}
        try:
            self.plate_weight_cache, explain_map = await self._build_weighted_plate_map_dynamic(today_str, universe)
            if not self.plate_weight_cache:
                self.plate_weight_cache = self._build_weighted_plate_map(universe)
        except Exception:
            self.plate_weight_cache = self._build_weighted_plate_map(universe)
        self.precomputed_static_date = today_str

        # 落盘缓存：盘中直接复用，减少多对多重复计算
        pipe = self.redis.pipeline()
        weight_key = f"cache:stock_plate_weights:{today_str}"
        explain_key = f"cache:stock_plate_weight_explain:{today_str}"
        pipe.delete(weight_key)
        pipe.delete(explain_key)
        for code, items in self.plate_weight_cache.items():
            payload = [{"plate_id": pid, "w": round(w, 6)} for pid, w in items]
            pipe.hset(weight_key, code, json.dumps(payload, ensure_ascii=False))
            if code in explain_map:
                pipe.hset(explain_key, code, json.dumps(explain_map[code], ensure_ascii=False))
        pipe.expire(weight_key, 86400)
        pipe.expire(explain_key, 86400)
        await pipe.execute()

        self._log_event(
            "plate_weight_precompute",
            f"🧱 板块归属预计算完成: 日期={today_str}, 股票={len(self.plate_weight_cache)}",
            min_interval_sec=1800,
        )

    async def update_intraday_state_machine(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> List[ExpectationSignal]:
        if not candidate_pool:
            return []

        auction_profile = await self._get_auction_profile(today_str)
        signals: List[ExpectationSignal] = []

        # 批量读取行情
        pipe = self.redis.pipeline()
        codes = list(candidate_pool)
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        quotes = await pipe.execute()

        now_ts = int(time.time() * 1000)
        transitions_for_redis: Dict[str, Dict[str, Any]] = {}

        for idx, code in enumerate(codes):
            q = quotes[idx] or {}
            ind = indicators.get(code, {}) or {}
            auction_change = self._safe_float(auction_profile.get(code, {}).get("change_pct", 0.0), 0.0)
            curr_change = self._safe_float(q.get("change_pct", q.get("change", 0.0)), 0.0)
            pre_close = self._safe_float(q.get("pre_close", q.get("last_close", 0.0)), 0.0)
            has_intraday_ohlc = (
                pre_close > 0
                and self._safe_float(q.get("high", 0.0), 0.0) > 0
                and self._safe_float(q.get("low", 0.0), 0.0) > 0
            )

            high_pct = self._safe_pct_from_quote(q, "high_pct", pre_close, curr_change)
            low_pct = self._safe_pct_from_quote(q, "low_pct", pre_close, curr_change)
            rebound_from_low = curr_change - low_pct
            drawdown_from_high = high_pct - curr_change

            snap = {
                "ts": now_ts,
                "auction_change_pct": auction_change,
                "change_pct": curr_change,
                "high_pct": high_pct,
                "low_pct": low_pct,
                "rebound_from_low": rebound_from_low,
                "drawdown_from_high": drawdown_from_high,
                "amount_2min": self._safe_float(ind.get("amount_2min", 0.0), 0.0),
            }
            prev = self.stock_state_cache.get(code)
            self.stock_state_cache[code] = snap

            if prev is None:
                continue

            # 1) 竞价强 -> 拉升爆头
            if has_intraday_ohlc and auction_change >= 3.0 and drawdown_from_high >= 4.0 and curr_change <= auction_change - 2.0:
                sig = ExpectationSignal(
                    code=code,
                    type="auction_strong_then_headshot",
                    change=curr_change,
                    score=3.2,
                    details={
                        "auction": round(auction_change, 2),
                        "high_pct": round(high_pct, 2),
                        "current_pct": round(curr_change, 2),
                        "drawdown": round(drawdown_from_high, 2),
                    },
                    reason=f"竞价强{auction_change:.1f}%后冲高回落，回撤{drawdown_from_high:.1f}%",
                )
                signals.append(sig)

            # 2) 开盘下砸后深V拉回
            if has_intraday_ohlc and low_pct <= -4.0 and rebound_from_low >= 4.0 and curr_change >= -1.0:
                sig = ExpectationSignal(
                    code=code,
                    type="intraday_deep_v_rebound",
                    change=curr_change,
                    score=3.0,
                    details={
                        "low_pct": round(low_pct, 2),
                        "current_pct": round(curr_change, 2),
                        "rebound": round(rebound_from_low, 2),
                    },
                    reason=f"盘中深V修复，低点{low_pct:.1f}%回拉至{curr_change:.1f}%",
                )
                signals.append(sig)

            # 3) 竞价弱 -> 下砸后反核
            if has_intraday_ohlc and auction_change <= 1.0 and low_pct <= -8.0 and curr_change >= -2.0 and rebound_from_low >= 6.0:
                sig = ExpectationSignal(
                    code=code,
                    type="auction_weak_then_floor_reactor",
                    change=curr_change,
                    score=3.5,
                    details={
                        "auction": round(auction_change, 2),
                        "low_pct": round(low_pct, 2),
                        "current_pct": round(curr_change, 2),
                    },
                    reason=f"竞价弱后砸深水再反核，低点{low_pct:.1f}%",
                )
                signals.append(sig)

            # 4) 竞价弱 -> 盘中转强
            if auction_change <= -1.0 and curr_change >= auction_change + 3.0 and curr_change > 0:
                sig = ExpectationSignal(
                    code=code,
                    type="auction_weak_to_intraday_strong",
                    change=curr_change,
                    score=2.4,
                    details={"auction": round(auction_change, 2), "current_pct": round(curr_change, 2)},
                    reason=f"竞价弱转强，{auction_change:.1f}% -> {curr_change:.1f}%",
                )
                signals.append(sig)

            # 去重：同股票同类型 90 秒内只保留一次
            for sig in signals[-4:]:
                dedup_key = f"{today_str}:{sig.code}:{sig.type}"
                last_ts = self.intraday_transition_seen.get(dedup_key, 0)
                if now_ts - last_ts >= 90_000:
                    self.intraday_transition_seen[dedup_key] = now_ts
                    transitions_for_redis[f"{sig.code}:{sig.type}:{now_ts}"] = sig.to_dict()

        if transitions_for_redis:
            key = f"rank:intraday_transition:{today_str}"
            p = self.redis.pipeline()
            p.zadd(key, {json.dumps(v, ensure_ascii=False): now_ts for v in transitions_for_redis.values()})
            p.expire(key, 86400)
            await p.execute()
            type_count: Dict[str, int] = {}
            for item in transitions_for_redis.values():
                t = item.get("type", "unknown")
                type_count[t] = type_count.get(t, 0) + 1
            summary = ",".join([f"{k}:{v}" for k, v in sorted(type_count.items(), key=lambda x: x[1], reverse=True)])
            self._log_event(
                "intraday_transition",
                f"⚡ 盘中转折信号: {len(transitions_for_redis)} 条 | {summary}",
                min_interval_sec=120,
            )

        return list(transitions_for_redis.values())

    async def calculate_plate_attitude(self, today_str: str, transitions: List[Dict[str, Any]]) -> None:
        if not transitions:
            return

        if not self.plate_weight_cache:
            await self.precompute_static_context(today_str, self.candidate_pool_cache, force=True)

        positive_types = {
            "intraday_deep_v_rebound",
            "auction_weak_then_floor_reactor",
            "auction_weak_to_intraday_strong",
        }
        negative_types = {"auction_strong_then_headshot"}

        plate_scores: Dict[str, float] = {}
        plate_event_counts: Dict[str, int] = {}
        for item in transitions:
            code = item.get("code")
            sig_type = item.get("type", "")
            score = self._safe_float(item.get("score", 0.0), 0.0)
            if not code or score <= 0:
                continue
            direction = 0.0
            if sig_type in positive_types:
                direction = 1.0
            elif sig_type in negative_types:
                direction = -1.0
            if abs(direction) < 1e-8:
                continue

            for pid, w in self.plate_weight_cache.get(code, []):
                plate_scores[pid] = plate_scores.get(pid, 0.0) + direction * score * w
                plate_event_counts[pid] = plate_event_counts.get(pid, 0) + 1

        if not plate_scores:
            return

        zkey = f"rank:plate_attitude:{today_str}"
        dkey = f"rank:plate_attitude:details:{today_str}"
        
        # Fetch existing details to accumulate events
        existing_details = {}
        try:
            raw_details = await self.redis.hgetall(dkey)
            if raw_details:
                for k, v in raw_details.items():
                    existing_details[k] = json.loads(v)
        except:
            pass

        p = self.redis.pipeline()
        ts = int(time.time() * 1000)
        
        for pid, score in plate_scores.items():
            name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
            # Accumulate scores using zincrby (ZINCRBY returns the new score, but we pipeline it)
            p.zincrby(zkey, round(score, 4), pid)
            
            # Accumulate events
            prev_events = existing_details.get(pid, {}).get("events", 0)
            new_events = prev_events + plate_event_counts.get(pid, 0)
            
            # We don't read the exact accumulated score for JSON immediately, 
            # we just put the increment amount or 0, the real ranking is in zkey anyway.
            p.hset(
                dkey,
                pid,
                json.dumps(
                    {
                        "ts": ts,
                        "id": pid,
                        "name": name,
                        "latest_delta": round(score, 4),
                        "events": new_events,
                    },
                    ensure_ascii=False,
                ),
            )
            
        p.expire(zkey, 86400)
        p.expire(dkey, 86400)
        await p.execute()

    def _is_quote_fresh(self, quote: Dict[str, Any], today_str: str) -> bool:
        """Verify if a quote's timestamp is from today and not stale."""
        if not quote:
            return False
            
        # 1. 优先检查 update_ts (Unix timestamp in seconds/ms)
        ts = self._safe_float(quote.get("update_ts") or quote.get("ts"), 0)
        if ts > 0:
            if ts < 1e12: ts *= 1000 # convert to ms
            dt = datetime.fromtimestamp(ts / 1000.0)
            # 宽限期：只要是今天的，或者距离现在不到 12 小时的（跨凌晨），视为新鲜
            if dt.strftime("%Y-%m-%d") == today_str:
                return True
            if (time.time() * 1000 - ts) < 12 * 3600 * 1000:
                return True
                
        # 2. 备选检查 last_update 字符串
        lu = quote.get("last_update")
        if isinstance(lu, str) and today_str.replace("-", "") in lu.replace("-", ""):
            return True
            
        return False

    async def calculate_plate_spread(
        self, 
        today_str: str, 
        candidate_pool: Set[str], 
        stock_indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None
    ) -> None:
        """
        计算板块分歧度
        优化：接受 stock_indicators 和 quote_map 参数，避免重复获取
        """
        if not candidate_pool:
            return
            
        # 移除冗余的 indicators 获取逻辑，由 run 循环负责统一传参
        if not stock_indicators:
            return
            
        if not quote_map:
            quote_map = await self._fetch_quotes_batch(list(candidate_pool))

        # Feature 3: Staleness Guard
        # 如果候选池中大部分数据都是旧的（比如开盘前），跳过更新以防止昨天的题材污染今天
        fresh_count = sum(1 for c in candidate_pool if self._is_quote_fresh(quote_map.get(c, {}), today_str))
        if len(candidate_pool) > 10 and (fresh_count / len(candidate_pool)) < 0.2:
            # 只有少量新鲜数据时，不具备板块聚合价值
            return

        STRONG_CHANGE = 1.0
        STRONG_AMT_2MIN = 10_000_000

        plate_stats: Dict[str, Dict[str, Any]] = {}

        for code6 in candidate_pool:
            ind = stock_indicators.get(code6)
            if not ind:
                continue

            plate_ids = self.plate_updater.stock_to_plates.get(code6, [])
            if not plate_ids:
                continue

            is_strong = (
                float(ind.get('change_pct', 0) or 0) > STRONG_CHANGE
                and float(ind.get('amount_2min', 0) or 0) > STRONG_AMT_2MIN
            )

            for pid in plate_ids:
                s = plate_stats.setdefault(pid, {"N_active": 0, "N_strong": 0, "sum_amount_2min": 0.0})
                s["N_active"] += 1
                if is_strong:
                    s["N_strong"] += 1
                    s["sum_amount_2min"] += float(ind.get('amount_2min', 0) or 0)

        zset_key = f"rank:plate_spread:{today_str}"
        details_key = f"rank:plate_spread:details:{today_str}"
        pipe = self.redis.pipeline()
        pipe.delete(zset_key)
        pipe.delete(details_key)

        log_entries = []

        for pid, s in plate_stats.items():
            if s["N_active"] <= 0:
                continue
            spread_ratio = s["N_strong"] / s["N_active"]
            amount_score = float(np.log1p(s["sum_amount_2min"] / 1_000_000))
            spread_score = round(spread_ratio * 50 + amount_score * 5, 2)
            if spread_score <= 0:
                continue

            name = self.plate_updater.all_plates.get(pid, {}).get('name', pid)
            detail = {
                "ts": int(time.time() * 1000),
                "id": pid,
                "name": name,
                "score": spread_score,
                "spread_ratio": round(spread_ratio, 4),
                "N_active": s["N_active"],
                "N_strong": s["N_strong"],
                "sum_amount_2min": s["sum_amount_2min"],
            }

            pipe.zadd(zset_key, {pid: spread_score})
            pipe.hset(details_key, pid, json.dumps(detail, ensure_ascii=False))
            
            log_entries.append({"name": name, "score": spread_score})

        pipe.expire(zset_key, 86400)
        pipe.expire(details_key, 86400)
        await pipe.execute()
        
        # Log top plates
        if log_entries:
            top_plates = sorted(log_entries, key=lambda x: x["score"], reverse=True)[:3]
            plate_strs = [f"{p['name']}({p['score']})" for p in top_plates]
            self._log_event("plate_top3", f"🔥 热门板块 Top3: {', '.join(plate_strs)}", min_interval_sec=300)

    async def calculate_theme_rank(
        self, 
        today_str: str, 
        candidate_pool: Set[str], 
        stock_indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None
    ) -> None:
        """
        计算题材排行
        优化：接受 stock_indicators 和 quote_map 参数
        """
        if not candidate_pool:
            return
            
        if not stock_indicators:
            return

        if not quote_map:
            quote_map = await self._fetch_quotes_batch(list(candidate_pool))
            
        # Feature 3: Theme Lock Guard
        # 在竞价刚开始（09:15之前）或者今天数据还没来时，不应该生成“今日”题材榜
        now_hm = datetime.now().strftime("%H:%M")
        fresh_count = sum(1 for c in candidate_pool if self._is_quote_fresh(quote_map.get(c, {}), today_str))
        
        if len(candidate_pool) > 10 and (fresh_count / len(candidate_pool)) < 0.2:
            # 数据太旧，如果此时已过 09:15，说明底层没推数据，此时应清除今日榜单
            if now_hm >= "09:15":
                await self.redis.delete(f"rank:theme:{today_str}")
                await self.redis.delete(f"rank:theme:details:{today_str}")
            return

        # 严格首板集合
        first_limit_set: Set[str] = set()
        try:
            first_limit_set = await self._get_first_limit_codes_cached()
        except Exception:
            pass

        themes, details, evidence = self.theme_ranker.build(
            today_str=today_str,
            candidate_pool=candidate_pool,
            indicators_by_stock=stock_indicators,
            first_limit_set=first_limit_set,
            top_n=50,
        )

        zset_key = f"rank:theme:{today_str}"
        details_key = f"rank:theme:details:{today_str}"
        evidence_key = f"cache:stock_theme_evidence:{today_str}"

        pipe = self.redis.pipeline()
        pipe.delete(zset_key)
        pipe.delete(details_key)
        pipe.delete(evidence_key)

        for t in themes:
            pipe.zadd(zset_key, {t.theme: t.score})
        for theme, d in details.items():
            pipe.hset(details_key, theme, json.dumps(d, ensure_ascii=False))
        for code6, ev in evidence.items():
            pipe.hset(evidence_key, code6, json.dumps(ev, ensure_ascii=False))

        pipe.expire(zset_key, 86400)
        pipe.expire(details_key, 86400)
        pipe.expire(evidence_key, 86400)
        await pipe.execute()

        if themes:
            top_themes_log = [f"{t.theme}({t.score:.1f})" for t in themes[:3]]
            self._log_event("theme_top3", f"🎭 热门题材 Top3: {', '.join(top_themes_log)}", min_interval_sec=300)

    async def calculate_comfort_exit(self, today_str: str) -> None:
        """ComfortExitScore：昨日赚钱的人今天能否舒服离场。"""
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)

            y_key = f"limit_up_{prev_day}"
            y_list = self.redis_storage.get_data(y_key)
            
            # 降级：如果找不到昨日涨停，尝试找前日的（应对回放数据不全）
            if not y_list:
                prev_prev_day = self.calendar.get_previous_trade_day(prev_day)
                y_key = f"limit_up_{prev_prev_day}"
                y_list = self.redis_storage.get_data(y_key)
                
            y_stocks: Dict[str, Dict[str, Any]] = {}
            if y_list and isinstance(y_list, list):
                for item in y_list:
                    if not isinstance(item, dict):
                        continue
                    code = item.get('股票代码', '') or item.get('code', '')
                    if code and len(code) == 6:
                        y_stocks[code] = {"lb_days": item.get('连板天数', 1)}

            if not y_stocks:
                try:
                    extra_data = await self.redis.hgetall(f"cache:stock_extra:{today_str}")
                    if extra_data and isinstance(extra_data, dict):
                        for code, data_str in extra_data.items():
                            try:
                                if not data_str: continue
                                info = json.loads(str(data_str))
                                if isinstance(info, dict):
                                    lb_days = info.get('lb_days', 0)
                                    try: 
                                        lb_days = int(lb_days)
                                    except: 
                                        lb_days = 0

                                    if lb_days >= 1:
                                        y_stocks[str(code)] = {"lb_days": lb_days}
                            except Exception:
                                pass
                except Exception as e:
                    logger.exception("Error extracting y_stocks from stock_extra in comfort_exit")
        except Exception as e:
            logger.exception("Error in early phase of calculate_comfort_exit")
            y_stocks = {}

        if not y_stocks:
            # Write a neutral default score to avoid STALE strategy lock
            default_result = {
                "ts": str(int(time.time() * 1000)),
                "score": "50.0",
                "support_rate": "0.0",
                "dump_rate": "0.0",
                "high_pos_dump_rate": "0.0",
                "sample_size": "0",
            }
            out_key = f"market:comfort_exit:{today_str}"
            try:
                await self.redis.hset(out_key, mapping=default_result)
                await self.redis.expire(out_key, 86400)
            except Exception as e:
                logger.error(f"Error writing default comfort_exit to Redis: {e}")
            return

        # 0925竞价 top_amount
        top_rank: Dict[str, int] = {}
        top_bid: Dict[str, float] = {}
        try:
            top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
            top_rank = {it.get("symbol"): idx for idx, it in enumerate(top_amount_list) if it.get("symbol")}
            top_bid = {
                it.get("symbol"): float(it.get("bid_amount_yuan", 0) or 0)
                for it in top_amount_list
                if it.get("symbol")
            }
        except Exception:
            pass

        # 批量读取实时行情
        pipe = self.redis.pipeline()
        codes = list(y_stocks.keys())
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        quotes = await pipe.execute()

        scores: List[float] = []
        weights: List[float] = []
        support_count = 0
        dump_count = 0
        high_pos_total = 0
        high_pos_dump_count = 0

        for i, code in enumerate(codes):
            q = quotes[i] or {}

            # change_pct 优先，兼容 change
            change_now = q.get('change_pct', None)
            if change_now is None or change_now == "":
                change_now = q.get('change', 0)
            try:
                change_now = float(change_now or 0)
            except Exception:
                change_now = 0.0

            amount_now = q.get('amount', 0)
            try:
                amount_now = float(amount_now or 0)
            except Exception:
                amount_now = 0.0

            rank = top_rank.get(code, 9999)
            rank_bonus = max(0.0, 20.0 - rank / 50.0)  # Top1000内越靠前越高

            bid = float(top_bid.get(code, 0.0) or 0.0)
            bid_bonus = min(20.0, (bid / 100_000_000.0) * 5.0)  # 每1亿封单≈5分，上限20

            open_bonus = 10.0 if change_now > 0 else -10.0

            dump_penalty = 0.0
            if change_now < -3.0 and amount_now > 50_000_000:
                dump_penalty = 15.0

            score = max(0.0, min(100.0, 50.0 + rank_bonus + bid_bonus + open_bonus - dump_penalty))
            scores.append(score)

            lb_days = y_stocks[code].get('lb_days', 1)
            try:
                lb_days = int(lb_days)
            except Exception:
                lb_days = 1

            weight = 1.0 + 0.3 * (max(1, lb_days) - 1)
            weights.append(weight)

            if score >= 60:
                support_count += 1
            if score <= 40:
                dump_count += 1

            if lb_days >= 3:
                high_pos_total += 1
                if change_now < 0:
                    high_pos_dump_count += 1

        # Log Leaders and Laggards in Yesterday's Limit Up pool
        # Re-construct simple list for logging
        y_pool_performance = []
        for i, code in enumerate(codes):
             q = quotes[i] or {}
             chg = 0.0
             try:
                 # Logic duplications from above, but safe
                 c = q.get('change_pct', q.get('change', 0))
                 chg = float(c) if c else 0.0
             except: pass
             y_pool_performance.append({"code": code, "change": chg})
        
        y_pool_performance.sort(key=lambda x: x['change'], reverse=True)
        leaders = [f"{x['code']}({x['change']:.1f}%)" for x in y_pool_performance if x['change'] > 5.0][:5]
        laggards = [f"{x['code']}({x['change']:.1f}%)" for x in y_pool_performance if x['change'] < -5.0][:5] # Sort reverse? No, need bottom.

        if leaders:
            self._log_event("comfort_leaders", f"📈 昨板-今日领涨: {', '.join(leaders)}", min_interval_sec=300)
        
        # Get bottom ones
        laggards_list = sorted(y_pool_performance, key=lambda x: x['change'])[:5]
        laggards = [f"{x['code']}({x['change']:.1f}%)" for x in laggards_list if x['change'] < -5.0]
        if laggards:
            self._log_event("comfort_laggards", f"📉 昨板-今日领跌: {', '.join(laggards)}", min_interval_sec=300)

        if not scores:
            return

        comfort_exit_score = round(float(np.average(scores, weights=weights)), 2)
        total_stocks = len(codes)

        result = {
            "ts": int(time.time() * 1000),
            "score": comfort_exit_score,
            "support_rate": round(support_count / max(1, total_stocks), 4),
            "dump_rate": round(dump_count / max(1, total_stocks), 4),
            "high_pos_dump_rate": round(high_pos_dump_count / max(1, high_pos_total), 4),
            "sample_size": total_stocks,
        }

        out_key = f"market:comfort_exit:{today_str}"
        await self.redis.hset(out_key, mapping=result)
        await self.redis.expire(out_key, 86400)

    async def calculate_sentiment(self, today_str: str, indicators: Optional[Dict[str, Dict]] = None) -> None:
        """情绪状态机精细版 (5 Phases)：冰点、启动、主升、分歧、退潮。"""
        ts = int(time.time() * 1000)

        comfort = await self.redis.hgetall(f"market:comfort_exit:{today_str}")
        comfort_score = float(comfort.get('score', 50) or 50)

        top_plates = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 9, withscores=True)
        total_score = sum(score for _, score in top_plates)
        consensus_score = 0.0
        if total_score > 0 and len(top_plates) >= 3:
            top3_score = sum(score for _, score in top_plates[:3])
            consensus_score = (top3_score / total_score) * 100.0

        try:
            first_limit_count = int(await self.redis.zcard("stock:first_limit_up"))
        except Exception:
            first_limit_count = 0

        # === 1. Calculate Max Limit-Up Height (空间板高度) ===
        # We must try wencai cache and then fallback to legacy limit_up_[date] from DB
        max_lb = 1
        total_limitup_count = 0
        limitup_lb_data = self.redis_storage.get_data(f"cache:wencai:limitup_lb:{today_str}")
        if limitup_lb_data and isinstance(limitup_lb_data, dict):
            total_limitup_count = len(limitup_lb_data)
            for code, lb in limitup_lb_data.items():
                try: max_lb = max(max_lb, int(lb))
                except: pass
        else:
            # Fallback for previous day/post-market real limits
            lb_list = self.redis_storage.get_data(f"limit_up_{today_str}")
            if not lb_list:
                # If during the day, maybe today's list isn't formulated, check previous trade day's carryover height
                prev_day = self.calendar.get_previous_trade_day(today_str)
                lb_list = self.redis_storage.get_data(f"limit_up_{prev_day}")
                
            if lb_list and isinstance(lb_list, list):
                total_limitup_count = len(lb_list)
                for item in lb_list:
                    if isinstance(item, dict):
                        lb = item.get("连板天数", item.get("lb_days", 0))
                        try: max_lb = max(max_lb, int(lb))
                        except: pass

        # Final robust fallback: scan today's stock_extra cache for lb_days
        if max_lb <= 1:
            try:
                extra_data = await self.redis.hgetall(f"cache:stock_extra:{today_str}")
                if extra_data:
                    for code, data_str in extra_data.items():
                        try:
                            info = json.loads(data_str)
                            if 'lb_days' in info:
                                max_lb = max(max_lb, int(info['lb_days']))
                        except:
                            pass
            except Exception as e:
                logger.error(f"Error fetching max_lb from stock_extra: {e}")

        # === 2. Calculate Market Breadth (红绿比) ===
        up_count, down_count, flat_count = 0, 0, 0
        avg_change_pct = 0.0

        # 优化：从 indicators (4000只) 中直接统计，不再扫描全库 KEYS
        if indicators:
            total_pct = 0.0
            valid_count = 0
            for ind in indicators.values():
                if not ind: continue
                chg_raw = ind.get("change_pct", 0.0)
                # 使用 normalize 逻辑处理单位 (100x scale)
                pct = self._normalize_change_pct(chg_raw)
                if pct > 0.1: up_count += 1
                elif pct < -0.1: down_count += 1
                else: flat_count += 1
                total_pct += pct
                valid_count += 1
            if valid_count > 0:
                avg_change_pct = total_pct / valid_count
        else:
            logger.warning("calculate_sentiment received no indicators! Using empty breadth.")
            
        red_green_ratio = up_count / max(1, down_count)


        # Baseline legacy score just for logging context
        sentiment_score = (
            comfort_score * 0.4
            + min(consensus_score, 100.0) * 0.2
            + min(total_limitup_count * 2.0, 100.0) * 0.2
            + min(first_limit_count * 4.0, 100.0) * 0.2
        )
        sentiment_score = round(float(sentiment_score), 2)

        # === 3. Determine the 5 Phases (5阶段周期算法) ===
        last_emotion_raw = await self.redis.hgetall("market:emotion_state:last")
        last_phase = last_emotion_raw.get("phase", "UNKNOWN")

        phase, pos_cap, allowed, blocked, transition, confidence = self._predict_market_phase(
            st_score=sentiment_score,
            red_green_ratio=red_green_ratio,
            max_lb=max_lb,
            consensus_score=consensus_score,
            comfort_score=comfort_score,
            last_phase=last_phase
        )

        # === 3.1 Adaptive Phase Debouncing (Stability Lock) ===
        # Avoid rapid oscillation between phases (e.g. RETREAT <-> IGNITION) unless the transition is significant
        if not hasattr(self, "_phase_buffer"):
            self._phase_buffer = []  # In-memory history for current session
        
        self._phase_buffer.append(phase)
        if len(self._phase_buffer) > 3:
            self._phase_buffer.pop(0)

        # Only transition if:
        # 1. New phase is retreat (always allow immediate panic detection)
        # 2. New phase has been constant for at least 2 cycles
        # 3. This is the first calculation of the day
        if phase != "retreat" and last_phase != "UNKNOWN" and len(self._phase_buffer) >= 2:
            if self._phase_buffer[-1] != self._phase_buffer[-2]:
                # Temporary fluctuation? Keep last_phase but reduce confidence
                phase = last_phase
                confidence *= 0.8
                transition = f"DEBOUNCED_{transition}"

        out_key = f"market:sentiment:{today_str}"
        payload = {
            "ts": ts,
            "phase": phase,
            "score": sentiment_score,
            "comfort_exit_score": comfort_score,
            "consensus_score": round(consensus_score, 2),
            "total_limitup_count": total_limitup_count,
            "first_limit_count": first_limit_count,
            "max_limit_up": max_lb,
            "red_green_ratio": round(red_green_ratio, 2),
            "avg_change_pct": round(avg_change_pct, 2)
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)

        # 同步更新持久化 Key 和 市场阶段 Key (真相源同步)
        last_date = last_emotion_raw.get("date", "")
        last_age_days = int(last_emotion_raw.get("age_days", 0))
        last_age_bars = int(last_emotion_raw.get("age_bars", 0))

        if phase == last_phase:
            phase_age_intraday_bars = last_age_bars + 1
            phase_age_days = last_age_days + 1 if last_date != today_str else last_age_days
        else:
            phase_age_intraday_bars = 1
            phase_age_days = 1 if last_date != today_str else 0

        await self.redis.hset("market:emotion_state:last", mapping={
            "phase": phase,
            "date": today_str,
            "age_days": phase_age_days,
            "age_bars": phase_age_intraday_bars,
            "ts": int(time.time() * 1000)
        })
        
        # 联动更新 EmotionPhaseResult (保持 downstream execution policy 实时感应)
        truth_key = f"market:emotion_phase:{today_str}"
        try:
            er_raw = await self.redis.hget(truth_key, "payload")
            if er_raw:
                er_dict = json.loads(er_raw)
            else:
                # If missing, create a new baseline structure
                er_dict = {
                    "date": today_str,
                    "emotion_phase": phase,
                    "phase_confidence": confidence,
                    "transition_reason_code": transition,
                    "position_cap": pos_cap,
                    "allowed_setups": allowed,
                    "blocked_setups": blocked,
                    "phase_age_days": phase_age_days,
                    "phase_age_intraday_bars": phase_age_intraday_bars,
                    "leader_candidates": [],
                    "plate_phase_map": {}
                }
            
            er_dict.update({
                "ts": int(time.time() * 1000),
                "emotion_phase": phase,
                "phase_confidence": confidence,
                "transition_reason_code": transition,
                "position_cap": pos_cap,
                "allowed_setups": allowed,
                "blocked_setups": blocked,
                "phase_age_days": phase_age_days,
                "phase_age_intraday_bars": phase_age_intraday_bars
            })
            await self.redis.hset(truth_key, "payload", json.dumps(er_dict, ensure_ascii=False))
        except Exception as e:
            logger.error(f"Error updating EmotionPhaseResult: {e}")

        # 中文映射描述
        phase_cn = {
            "retreat": "🌑 退潮期 (风险爆发/全场杀跌)",
            "ice_point": "🧊 冰点期 (情绪极度低迷/等待反击)",
            "climax": "🔥 主升期 (板块共振/加速上涨)",
            "divergence": "⚡ 分歧期 (高位震荡/炸板激增)",
            "start": "🌱 启动期 (情绪修复/先锋试探)",
            "UNKNOWN": "❓ 未知状态"
        }.get(phase, phase)

        self._log_event(
            "sentiment_phase",
            f"🌊 市场运行画像: {phase_cn} | 涨跌比:{red_green_ratio:.2f}, 空间高度:{max_lb}板, 舒适度:{sentiment_score}",
            min_interval_sec=60,
            log_on_change=True
        )
        return last_phase != phase

    async def calculate_fear_greed(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        """综合贪婪恐惧指数（验证版，基于现有数据源）。"""
        if not candidate_pool:
            return

        if indicators is None:
            logger.warning("calculate_fear_greed 内部触发了冗余的指标获取")
            loop = asyncio.get_event_loop()
            indicators = await loop.run_in_executor(None, self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized, list(candidate_pool))

        changes = [self._safe_float((ind or {}).get("change_pct", 0.0), 0.0) for ind in indicators.values()]
        up = sum(1 for c in changes if c > 0.0)
        down = sum(1 for c in changes if c < 0.0)
        total = max(1, up + down)
        up_down_ratio = up / max(1, down)
        up_down_ratio_norm = self._clamp01(max(0.3, min(3.0, up_down_ratio)) / 3.0)

        first_limit_count = self._safe_int(await self.redis.zcard("stock:first_limit_up"), 0)
        lb_data = self.redis_storage.get_data(f"cache:wencai:limitup_lb:{today_str}") or []
        total_limitup_count = len(lb_data)
        seal_rate = self._clamp01(first_limit_count / max(1, total_limitup_count))

        max_lb = 0
        for item in lb_data:
            if not isinstance(item, dict):
                continue
            lb = item.get("连板天数", item.get("lb_days", 0))
            max_lb = max(max_lb, self._safe_int(lb, 0))

        rolling_key = f"market:lb_rolling_max:{today_str}"
        rolling_max = self._safe_int(await self.redis.get(rolling_key), 0)
        rolling_max = max(rolling_max, max_lb, 1)
        await self.redis.set(rolling_key, rolling_max, ex=86400)
        lb_height_norm = self._clamp01(max_lb / rolling_max)

        profit_breadth = self._clamp01(sum(1 for c in changes if c >= 2.0) / max(1, len(changes)))

        score = round((up_down_ratio_norm + seal_rate + lb_height_norm + profit_breadth) / 4.0, 4)
        state = "neutral"
        if score >= 0.65:
            state = "greed"
        elif score <= 0.35:
            state = "fear"

        extreme_greed = score > 0.85 and total_limitup_count > 100 and max_lb >= 7
        extreme_fear = score < 0.15 and (down / total) > 0.7 and profit_breadth < 0.2

        out_key = f"market:fear_greed:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": int(time.time() * 1000),
                "score": score,
                "state": state,
                "up_down_ratio_norm": round(up_down_ratio_norm, 4),
                "seal_rate": round(seal_rate, 4),
                "lb_height_norm": round(lb_height_norm, 4),
                "profit_breadth": round(profit_breadth, 4),
                "up_count": up,
                "down_count": down,
                "total_limitup_count": total_limitup_count,
                "first_limit_count": first_limit_count,
                "max_lb": max_lb,
                "extreme_greed": int(extreme_greed),
                "extreme_fear": int(extreme_fear),
            },
        )
        await self.redis.expire(out_key, 86400)

    async def calculate_herding(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        """羊群效应（验证版）：集中度、相关性、资金一致性、轮动速度。"""
        if not candidate_pool:
            return

        if indicators is None:
            indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))

        self._update_return_history(indicators)

        amounts = [self._safe_float((ind or {}).get("amount", 0.0), 0.0) for ind in indicators.values()]
        total_amount = sum(a for a in amounts if a > 0.0)
        top_n = max(1, int(len(amounts) * 0.1))
        top10_amount = sum(sorted(amounts, reverse=True)[:top_n]) if amounts else 0.0
        concentration = self._clamp01(top10_amount / max(1.0, total_amount))

        # 基于最近截面的 change_pct 序列估算平均相关性
        series = [vals for vals in self.return_history.values() if len(vals) >= 5]
        avg_corr = 0.0
        if len(series) >= 5:
            min_len = min(10, min(len(s) for s in series))
            mat = np.array([s[-min_len:] for s in series[:80]], dtype=float)
            if mat.shape[0] >= 5 and mat.shape[1] >= 5:
                corr = np.corrcoef(mat)
                upper = corr[np.triu_indices_from(corr, k=1)]
                valid = upper[np.isfinite(upper)]
                if valid.size > 0:
                    avg_corr = float(np.mean(valid))
        corr_norm = self._clamp01((avg_corr + 1.0) / 2.0)

        large_nets = [self._safe_float((ind or {}).get("large_net", 0.0), 0.0) for ind in indicators.values()]
        pos = sum(1 for v in large_nets if v > 0)
        neg = sum(1 for v in large_nets if v < 0)
        flow_consistency = self._clamp01(max(pos, neg) / max(1, pos + neg))

        top_plate = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 0)
        now_ms = int(time.time() * 1000)
        if top_plate:
            self.leading_plate_history.append((now_ms, top_plate[0]))
        # 保留近2小时窗口
        cutoff = now_ms - 2 * 60 * 60 * 1000
        self.leading_plate_history = [(t, p) for t, p in self.leading_plate_history if t >= cutoff]
        distinct_leaders = len(set(p for _, p in self.leading_plate_history))
        samples = max(1, len(self.leading_plate_history))
        rotation_speed = distinct_leaders / samples
        rotation_stability = self._clamp01(1.0 - min(1.0, rotation_speed * 2.0))

        score = round(
            0.35 * concentration + 0.25 * corr_norm + 0.25 * flow_consistency + 0.15 * rotation_stability,
            4,
        )
        state = "neutral"
        if score >= 0.7:
            state = "crowded"
        elif score <= 0.35:
            state = "dispersed"

        out_key = f"market:herding:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": now_ms,
                "score": score,
                "state": state,
                "concentration": round(concentration, 4),
                "avg_corr_norm": round(corr_norm, 4),
                "flow_consistency": round(flow_consistency, 4),
                "rotation_stability": round(rotation_stability, 4),
                "rotation_speed": round(rotation_speed, 4),
                "distinct_leaders": distinct_leaders,
            },
        )
        await self.redis.expire(out_key, 86400)

    async def calculate_resonance(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None,
    ) -> None:
        """共振模型：点(个股)-线(题材)-面(板块)-盘(市场)"""
        if not candidate_pool or not indicators:
            return

        # 点：候选池内强势股占比
        strong_point_count = 0
        for _, ind in indicators.items():
            chg = self._safe_float((ind or {}).get("change_pct", 0.0), 0.0)
            amt2 = self._safe_float((ind or {}).get("amount_2min", 0.0), 0.0)
            if chg >= 2.0 and amt2 >= 1000: # 单位：万元
                strong_point_count += 1
        point_score = self._clamp01(strong_point_count / max(1, len(candidate_pool)))

        # 线：题材前3集中度（题材得分越集中在前3，传导越明确）
        top_themes = await self.redis.zrevrange(f"rank:theme:{today_str}", 0, 9, withscores=True)
        line_score = 0.0
        if top_themes:
            total = sum(float(s) for _, s in top_themes)
            top3 = sum(float(s) for _, s in top_themes[:3])
            if total > 0:
                line_score = self._clamp01(top3 / total)

        # 面：板块扩散（前3板块得分占比）
        top_plates = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 9, withscores=True)
        plane_score = 0.0
        if top_plates:
            total_p = sum(float(s) for _, s in top_plates)
            top3_p = sum(float(s) for _, s in top_plates[:3])
            if total_p > 0:
                plane_score = self._clamp01(top3_p / total_p)

        # 盘：市场情绪（用 fear_greed + sentiment 组合）
        fg = await self.redis.hgetall(f"market:fear_greed:{today_str}")
        st = await self.redis.hgetall(f"market:sentiment:{today_str}")
        fg_score = self._safe_float(fg.get("score", 0.5), 0.5)
        st_score = self._safe_float(st.get("score", 50.0), 50.0) / 100.0
        market_score = self._clamp01(0.6 * fg_score + 0.4 * st_score)

        # 共振：链路最弱环节决定上限 (防御性：排除 0 分维度，避免因数据缺失导致的评分塌陷)
        active_scores = [s for s in [point_score, line_score, plane_score, market_score] if s > 0.001]
        weakest = min(active_scores) if active_scores else 0.0
        
        score = self._clamp01(
            0.20 * point_score + 0.25 * line_score + 0.25 * plane_score + 0.30 * market_score
        )
        
        # 如果有活跃维度，结合木桶效应；否则维持基础分
        if active_scores:
            resonance_score = round(self._clamp01(0.7 * score + 0.3 * weakest), 4)
        else:
            resonance_score = 0.1000

        state = "neutral"
        if resonance_score >= 0.72:
            state = "strong_resonance"
        elif resonance_score <= 0.35:
            state = "weak_resonance"

        out_key = f"market:resonance:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "score": resonance_score,
            "state": state,
            "point_score": round(point_score, 4),
            "line_score": round(line_score, 4),
            "plane_score": round(plane_score, 4),
            "market_score": round(market_score, 4),
            "weakest_link": round(weakest, 4),
            "strong_point_count": strong_point_count,
            "candidate_size": len(candidate_pool),
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)
        self._log_event(
            "resonance",
            f"📡 共振评分: {resonance_score:.2f} ({state}) | 点{point_score:.2f} 线{line_score:.2f} 面{plane_score:.2f} 盘{market_score:.2f}",
            min_interval_sec=180,
        )

    async def calculate_expectation_eval(
        self,
        today_str: str,
        auction_items: Optional[List[Dict[str, Any]]] = None,
        cached_indicators: Dict[str, Dict] = None,
        quote_map: Optional[Dict[str, Dict]] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Evaluate whether auction expectation-gap signals are effective."""
        run_id = self._ensure_run_id(today_str, run_id=run_id, refresh=not bool(run_id))
        top_amount_list = auction_items if auction_items is not None else await self._get_auction_top_amount_cached(
            today_str, require_final_0925=False
        )
        if not top_amount_list:
            return {}

        rows: List[Dict[str, Any]] = []
        for it in top_amount_list[:1000]:
            code = str(it.get("symbol") or "").strip()
            if len(code) != 6:
                continue
            rows.append(
                {
                    "code": code,
                    "auc": self._normalize_change_pct(it.get("change_pct", 0.0), scale=1.0),
                    "bid": self._safe_float(it.get("bid_amount_yuan", 0.0), 0.0),
                    "aamt": self._safe_float(it.get("auction_amount_yuan", 0.0), 0.0),
                }
            )

        if not rows:
            return {}

        if quote_map is None:
            quote_map = await self._fetch_quotes_batch([x["code"] for x in rows])

        evaluated: List[Dict[str, Any]] = []
        for x in rows:
            q = quote_map.get(x["code"]) or {}
            if not q:
                continue
            cur = self._normalize_change_pct(q.get("change_pct", q.get("change", 0.0)))
            cur_price = self._safe_float(q.get("price", 0.0), 0.0)
            gap = cur - x["auc"]
            y = dict(x)
            y["cur"] = cur
            y["cur_price"] = cur_price
            y["gap"] = gap
            evaluated.append(y)

        if not evaluated:
            return {}

        fade = [x for x in evaluated if x["auc"] >= 5.0 and x["gap"] <= -4.0]
        rise = [x for x in evaluated if x["auc"] <= 1.0 and x["gap"] >= 3.0 and x["cur"] > 0.0]
        high_auc = [x for x in evaluated if x["auc"] >= 5.0]
        low_auc = [x for x in evaluated if x["auc"] <= 1.0]
        high_auc_non_fade = [x for x in high_auc if x not in fade]
        low_auc_non_rise = [x for x in low_auc if x not in rise]

        # Additional expectation buckets:
        # - extreme low open: <= -8%
        # - strong high open: >= +5%
        # - one-word limit-up style at auction
        # - auction amount front row
        extreme_low = [x for x in evaluated if x["auc"] <= -8.0]
        strong_high = [x for x in evaluated if x["auc"] >= 5.0]
        amount_front = sorted(evaluated, key=lambda x: x["aamt"], reverse=True)[:20]

        def _limit_up_th(code: str) -> float:
            c = str(code or "")
            if c.startswith(("300", "301", "688", "689")):
                return 19.8
            return 9.8

        one_word_candidates = [
            x for x in evaluated
            if x["auc"] >= _limit_up_th(x["code"]) and x["bid"] > 0
        ]

        # seal ratio: bid / auction_amount
        front20 = sorted(evaluated, key=lambda x: x["aamt"], reverse=True)[:20]
        front20_bid = sum(max(0.0, x["bid"]) for x in front20)
        front20_aamt = sum(max(0.0, x["aamt"]) for x in front20)
        seal_ratio_front20 = front20_bid / max(1.0, front20_aamt)

        one_word_bid = sum(max(0.0, x["bid"]) for x in one_word_candidates)
        one_word_aamt = sum(max(0.0, x["aamt"]) for x in one_word_candidates)
        seal_ratio_one_word = one_word_bid / max(1.0, one_word_aamt)

        def _avg(arr: List[Dict[str, Any]], key: str) -> float:
            if not arr:
                return 0.0
            return float(sum(x[key] for x in arr) / len(arr))

        fade_adv = _avg(high_auc_non_fade, "cur") - _avg(fade, "cur")
        rise_adv = _avg(rise, "cur") - _avg(low_auc_non_rise, "cur")
        effectiveness = self._clamp01((fade_adv + rise_adv) / 20.0)

        # bucket effectiveness
        extreme_low_rebound = [x for x in extreme_low if x["gap"] >= 5.0 and x["cur"] > -2.0]
        strong_high_hold = [x for x in strong_high if x["cur"] >= x["auc"] - 2.0]
        one_word_break = [x for x in one_word_candidates if x["cur"] <= _limit_up_th(x["code"]) - 3.0]

        out_key = f"diag:expectation_eval:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "sample_size": len(evaluated),
            "high_auc_count": len(high_auc),
            "low_auc_count": len(low_auc),
            "fade_count": len(fade),
            "rise_count": len(rise),
            "fade_avg_cur": round(_avg(fade, "cur"), 4),
            "high_auc_non_fade_avg_cur": round(_avg(high_auc_non_fade, "cur"), 4),
            "rise_avg_cur": round(_avg(rise, "cur"), 4),
            "low_auc_non_rise_avg_cur": round(_avg(low_auc_non_rise, "cur"), 4),
            "fade_adv": round(fade_adv, 4),
            "rise_adv": round(rise_adv, 4),
            "effectiveness": round(effectiveness, 4),
            "extreme_low_rebound_count": len(extreme_low_rebound),
        }
        
        extreme_low_rebound_rate = round(len(extreme_low_rebound) / max(1, len(extreme_low)), 4)
        strong_high_hold_rate = round(len(strong_high_hold) / max(1, len(strong_high)), 4)
        one_word_break_rate = round(len(one_word_break) / max(1, len(one_word_candidates)), 4)
        payload = {
            "ts": int(time.time() * 1000),
            "sample_size": len(evaluated),
            "high_auc_count": len(high_auc),
            "low_auc_count": len(low_auc),
            "fade_count": len(fade),
            "rise_count": len(rise),
            "fade_avg_cur": round(_avg(fade, "cur"), 4),
            "high_auc_non_fade_avg_cur": round(_avg(high_auc_non_fade, "cur"), 4),
            "rise_avg_cur": round(_avg(rise, "cur"), 4),
            "low_auc_non_rise_avg_cur": round(_avg(low_auc_non_rise, "cur"), 4),
            "fade_adv": round(fade_adv, 4),
            "rise_adv": round(rise_adv, 4),
            "effectiveness": round(effectiveness, 4),
            "extreme_low_count": len(extreme_low),
            "extreme_low_rebound_count": len(extreme_low_rebound),
            "extreme_low_rebound_rate": extreme_low_rebound_rate,
            "strong_high_count": len(strong_high),
            "strong_high_hold_count": len(strong_high_hold),
            "strong_high_hold_rate": strong_high_hold_rate,
            "one_word_count": len(one_word_candidates),
            "one_word_break_count": len(one_word_break),
            "one_word_break_rate": one_word_break_rate,
            "seal_ratio_front20": round(seal_ratio_front20, 4),
            "seal_ratio_one_word": round(seal_ratio_one_word, 4),
            "amount_front_avg_cur": round(_avg(amount_front, "cur"), 4),
            "amount_front_avg_gap": round(_avg(amount_front, "gap"), 4),
        }

        # --- 套利目标打分评估 ---
        arbitrage_targets = []
        if cached_indicators is None:
            cached_indicators = {}
        # 确保筹码峰和多因子数据加载
        await self._load_chip_peaks_cached(today_str)
        await self._load_stock_extra_cached(today_str)
        
        scored_candidates = []
        for x in evaluated:
            code = x["code"]
            # 合并 real-time indicators 和盘后多因子
            ind = dict(cached_indicators.get(code, {}))
            extra = self.stock_extra.get(code, {})
            ind.update(extra)  # change_pct_5d, limit_up_days_5, real_market_cap 等
            peak = self.chip_peaks.get(code, {})
            score = self._calculate_arbitrage_score(
                code=code,
                current_price=x.get("cur_price", 0),
                auction_amount=x.get("aamt", 0),
                indicators=ind,
                chip_peak=peak
            )
            if score > 0:
                scored_candidates.append({
                    "code": code,
                    "score": score,
                    "auc": x["auc"],
                    "cur": x["cur"],
                    "amount": x["aamt"],
                    "name": self._get_stock_name(code)
                })
        
        if scored_candidates:
            scored_candidates.sort(key=lambda item: item["score"], reverse=True)

        # Add 4-scenario Expectation Gap Analysis
        if top_amount_list and quote_map:
            candidate_pool = {str(x.get("symbol")).strip() for x in top_amount_list[:300] if len(str(x.get("symbol")).strip()) == 6}
            gap_analysis = await self.build_expectation_gap_analysis(today_str, candidate_pool, top_amount_list, quote_map)
            payload["expectation_gap"] = gap_analysis
            wts_count = len(gap_analysis.get("weak_to_strong", []))
            stw_count = len(gap_analysis.get("strong_to_weak", []))
            if wts_count > 0 or stw_count > 0:
                self._log_event(
                    "expectation_gap_signal",
                    f"🔄 预期差信号: 弱转强={wts_count} 强转弱={stw_count}",
                    min_interval_sec=120,
                )
        else:
            self._log_event(
                "expectation_gap_skip",
                f"⚠️ 预期差分析跳过: auction_items={len(top_amount_list or [])}, quote_map={'有' if quote_map else '无'}",
                min_interval_sec=300,
            )


        # --- 新版状态机制: 产出 EmotionPhaseResult ---
        # 1. 判断全局情绪基准
        st_data = await self.redis.hgetall(f"market:sentiment:{today_str}")
        st_score = self._safe_float(st_data.get("score", 50.0), 50.0)
        
        # 2. 统一状态机逻辑判定 (State Machine Logic)
        last_emotion_raw = await self.redis.hgetall("market:emotion_state:last")
        last_phase = last_emotion_raw.get("phase", "UNKNOWN")

        # 竞价时段红绿比 proxy: 涨/跌 比例
        auction_red_green = len(rise) / max(1, len(fade))
        
        # 获取板块共振辅助分 (从昨日缓存)
        plate_consensus = 0.0
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)
            p_data = await self.redis.zrevrange(f"rank:plate_spread:{prev_day}", 0, 0, withscores=True)
            if p_data: plate_consensus = float(p_data[0][1])
        except: pass

        # 获取空间板高度 (max_lb)
        max_lb_today = 1
        try:
            lb_data = self.redis_storage.get_data(f"cache:wencai:limitup_lb:{today_str}")
            if lb_data and isinstance(lb_data, dict):
                max_lb_today = max((int(v) for v in lb_data.values()), default=1)
            else:
                prev_day = self.calendar.get_previous_trade_day(today_str)
                lb_hist = self.redis_storage.get_data(f"limit_up_{prev_day}")
                if lb_hist and isinstance(lb_hist, list):
                    max_lb_today = max((int(x.get("连板天数", 0)) for x in lb_hist if isinstance(x, dict)), default=1)
        except: pass

        phase, pos_cap, allowed, blocked, transition, confidence = self._predict_market_phase(
            st_score=st_score,
            red_green_ratio=auction_red_green,
            max_lb=max_lb_today,
            consensus_score=plate_consensus,
            comfort_score=50.0,
            effectiveness=effectiveness,
            fade_count=len(fade),
            one_word_break_rate=one_word_break_rate,
            seal_ratio_front20=seal_ratio_front20,
            last_phase=last_phase
        )

        # 3. 计算全局骗炮惩罚 (Fakeout Penalty)
        # 简易版宽基普跌惩罚: 如果退潮或冰点，全场+0.4基础惩罚
        risk_off_penalty = 0.4 if phase in ("retreat", "ice_point") else (0.2 if phase == "divergence" else 0.0)
        # 如果赚钱效应低、炸板率高，增加惩罚
        weak_seal_penalty = 0.3 if one_word_break_rate > 0.5 else 0.0
        global_fakeout_penalty = round(risk_off_penalty + weak_seal_penalty, 3)

        # --- 情绪状态机持久化与周期计数 (Phase Age Tracking) ---
        last_date = last_emotion_raw.get("date", "")
        last_age_days = int(last_emotion_raw.get("age_days", 0))
        last_age_bars = int(last_emotion_raw.get("age_bars", 0))

        if phase == last_phase:
            phase_age_intraday_bars = last_age_bars + 1
            phase_age_days = last_age_days + 1 if last_date != today_str else last_age_days
        else:
            phase_age_intraday_bars = 1
            phase_age_days = 1 if last_date != today_str else 0

        # 写回持久化 Key (用于逻辑闭环)
        await self.redis.hset("market:emotion_state:last", mapping={
            "phase": phase,
            "date": today_str,
            "age_days": phase_age_days,
            "age_bars": phase_age_intraday_bars,
            "ts": int(time.time() * 1000)
        })

        # 4. 构建前排池身份 (动态决策模型 StockRoleProfile)
        plate_phase_cache = await self.redis.hgetall(f"market:plate_phase_map:{today_str}") or {}
        plate_phase_detail_raw = await self.redis.hgetall(f"market:plate_phase_detail:{today_str}") or {}
        plate_phase_detail = {k: self._safe_json_dict(v) for k, v in plate_phase_detail_raw.items()}
        plate_strength_map = self._build_plate_strength_map(evaluated)
        aamt_max = max((self._safe_float(x.get("aamt", 0.0), 0.0) for x in evaluated), default=1.0)

        tmp_candidates = []
        for row in evaluated:
            code = row["code"]
            extra = self.stock_extra.get(code, {})
            plate_ctx = plate_strength_map.get(code, {})
            chip_peak = self.chip_peaks.get(code, {})
            current_price = self._safe_float(row.get("cur_price", 0.0), 0.0)

            chip_ctx = self._score_chip_safety(code, current_price, chip_peak)
            scores = self._score_stock_role(
                row=row,
                extra=extra,
                plate_ctx=plate_ctx,
                chip_safety_score=chip_ctx["chip_safety_score"],
                aamt_max=aamt_max,
            )

            tmp_candidates.append({
                "code": code,
                "name": self._get_stock_name(code),
                "plate_key": plate_ctx.get("primary_plate_id", ""),
                "chip_ctx": chip_ctx,
                "scores": scores,
                "extra": extra,
                "plate_ctx": plate_ctx,
            })

        plate_groups = {}
        for item in tmp_candidates:
            plate_groups.setdefault(item["plate_key"], []).append(item)

        leader_pool = []
        plate_phase_map = {}
        for plate_id, group in plate_groups.items():
            # 排序: (领导力得分, 跟风得分, 流通市值) 降序
            group.sort(key=lambda x: (
                -x["scores"]["leadership_score"],
                -x["scores"]["follow_score"],
                -self._safe_float(x["extra"].get("real_market_cap", 0.0), 0.0)
            ))
            
            # --- 板块状态判定 (Simplified Plate State Machine) ---
            if plate_id:
                top_item = group[0]
                plate_change = top_item["plate_ctx"].get("plate_change_pct", 0.0)
                cached_phase = str(plate_phase_cache.get(plate_id, "") or "")
                if cached_phase:
                    plate_phase_map[plate_id] = cached_phase
                elif top_item["scores"]["leadership_score"] >= 0.70 and plate_change >= 2.0:
                    plate_phase_map[plate_id] = "PLATE_MAIN_RISE"
                elif plate_change <= -2.5:
                    plate_phase_map[plate_id] = "PLATE_RETREAT"
                else:
                    plate_phase_map[plate_id] = "PLATE_STABLE"

            for idx, item in enumerate(group, start=1):
                role_type, board_rank = self._classify_stock_role(
                    scores=item["scores"],
                    plate_ctx=item["plate_ctx"],
                    chip_safety_score=item["chip_ctx"]["chip_safety_score"],
                    plate_rank=idx,
                )
                extra = item["extra"]
                lead_prof = StockRoleProfile(
                    code=item["code"],
                    name=item["name"],
                    role_type=role_type,
                    leadership_score=item["scores"]["leadership_score"],
                    follow_score=item["scores"]["follow_score"],
                    chip_safety_score=item["chip_ctx"]["chip_safety_score"],
                    chip_zone_status=item["chip_ctx"]["chip_zone_status"],
                    board_position_rank=board_rank,
                    theme_overlap_count=len(self.plate_updater.stock_to_plates.get(item["code"], [])) if hasattr(self.plate_updater, "stock_to_plates") else 0,
                    real_market_cap=self._safe_float(extra.get("real_market_cap", 0.0), 0.0),
                    limit_up_type=str(extra.get("limit_up_type", "")),
                    primary_plate=item["plate_ctx"].get("primary_plate", ""),
                    primary_plate_id=item["plate_ctx"].get("primary_plate_id", ""),
                    plate_phase=plate_phase_map.get(item["plate_ctx"].get("primary_plate_id", ""), "UNKNOWN"),
                )
                leader_pool.append(lead_prof.to_dict())

        leader_pool.sort(key=lambda x: (x["board_position_rank"], -x["leadership_score"], -x["follow_score"]))

        # 5. 组装 EmotionPhaseResult 契约对象
        er = EmotionPhaseResult(
            ts=int(time.time() * 1000),
            date=today_str,
            emotion_phase=phase,
            phase_confidence=confidence,
            transition_reason_code=transition,
            position_cap=pos_cap,
            allowed_setups=allowed,
            blocked_setups=blocked,
            global_fakeout_penalty=global_fakeout_penalty,
            phase_age_days=phase_age_days,
            phase_age_intraday_bars=phase_age_intraday_bars,
            leader_candidates=leader_pool,
            plate_phase_map=plate_phase_map
        )
        payload["emotion_phase_result"] = asdict(er)
        pattern_matrix = await self._build_pattern_matrix(
            today_str,
            market_phase=er.emotion_phase,
            plate_phase_map=plate_phase_map,
        )
        await self._store_pattern_matrix(today_str, pattern_matrix)
        payload["pattern_matrix"] = pattern_matrix

        # 取前3名最强核心作为兼容旧版 arbitrage_targets的展示位
        if leader_pool:
            payload["arbitrage_targets"] = [
                {
                    "code": lead["code"],
                    "name": lead["name"],
                    "score": lead["leadership_score"],
                    "auc": next((x["auc"] for x in evaluated if x["code"] == lead["code"]), 0),
                    "cur": next((x["cur"] for x in evaluated if x["code"] == lead["code"]), 0),
                    "amount": next((x["aamt"] for x in evaluated if x["code"] == lead["code"]), 0)
                } for lead in leader_pool[:3]
            ]
        else:
            payload["arbitrage_targets"] = []

        # Serialize the list of dicts properly before saving to redis
        payload_str = {k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v) for k, v in payload.items()}
        await self.redis.delete(out_key)
        await self.redis.hset(out_key, mapping=payload_str)
        await self.redis.expire(out_key, 7 * 24 * 3600)
        
        # 写入新的状态机专用 key 库 
        emotion_key = f"market:emotion_phase:{today_str}"
        await self.redis.hset(emotion_key, mapping={"ts": er.ts, "payload": json.dumps(asdict(er), ensure_ascii=False)})
        await self.redis.expire(emotion_key, 7 * 24 * 3600)
        await self._emit_snapshot(
            self._build_market_snapshot(
                today_str=today_str,
                run_id=run_id,
                payload=payload,
                emotion_result=er,
                last_phase=last_phase,
                st_score=st_score,
                max_lb_today=max_lb_today,
                plate_consensus=plate_consensus,
                auction_red_green=auction_red_green,
                effectiveness=effectiveness,
                fade_count=len(fade),
                rise_count=len(rise),
                one_word_break_rate=one_word_break_rate,
                seal_ratio_front20=seal_ratio_front20,
            )
        )

        # --- 统一预期差状态分析 (Phase B Refinement) ---
        # Ensure yesterday's baseline is loaded
        prev_limit_up_set, prev_hot_plates = await self._get_yesterday_baseline(today_str)
        
        # Prepare core stocks (Top 80 by amount) and their quotes
        core_codes = [r["code"] for r in evaluated[:80]]
        core_auction = [next(it for it in top_amount_list if str(it.get("symbol")) == c) for c in core_codes]
        core_quotes = [quote_map.get(c, {}) for c in core_codes]

        transition_analysis = await self.analyze_expectations(
            core_auction, 
            core_quotes, 
            prev_limit_up_set=prev_limit_up_set,
            prev_hot_plates=prev_hot_plates,
            sentiment_phase=er.emotion_phase
        )
        
        # Merge results into payload for strategy consumption
        payload["summary"] = transition_analysis.get("summary").to_dict() if transition_analysis.get("summary") else {}
        payload["transition_stats"] = transition_analysis.get("stats")
        payload["transition_signals"] = [s.to_dict() for s in transition_analysis.get("signals", [])]

        arb_str = " | ".join([f"{a['name']}({a['score']}分)" for a in arbitrage_targets])
        self._log_event(
            "expectation_eval",
            (
                f"🌟 阶段判定: {er.emotion_phase} (Cap:{er.position_cap}|P:{er.global_fakeout_penalty}) | "
                f"预期差: eff={effectiveness:.2f} fade={len(fade)} rise={len(rise)} "
                f"W2S Score: {payload['summary'].get('weak_to_strong_window_score', 0):.2f} | 优选套利: {arb_str}"
            ),
            min_interval_sec=120,
        )
        return payload

    def _evaluate_expectation_state(self, stock_code: str, auction_data: Dict[str, Any], current_data: Dict[str, Any]) -> str:
        """评估预期差状态 (弱转强/强转弱/强更强/弱更弱)"""
        extra = self.stock_extra.get(stock_code, {})
        y_state = extra.get("y_state", "普通")
        change_pct_1d = self._safe_float(extra.get("change_pct_1d", 0.0))
        vol_ratio_1d = self._safe_float(extra.get("vol_ratio_1d", 0.0))
        limit_up_type = extra.get("limit_up_type", "")
        close_strength = self._safe_float(extra.get("close_strength", 0.5))
        upper_shadow_pct = self._safe_float(extra.get("upper_shadow_pct", 0.0))
        
        # 今日竞价表现
        auction_change = self._normalize_change_pct(auction_data.get("change_pct", 0.0), scale=1)
        auction_amount = self._safe_float(auction_data.get("auction_amount_yuan", 0.0))
        auction_bid = self._safe_float(auction_data.get("bid_amount_yuan", 0.0))
        seal_ratio = auction_bid / max(1.0, auction_amount)

        # 今日开盘表现 (若有)
        current_change = self._normalize_change_pct(current_data.get("change_pct", auction_change), scale=1)

        is_strong_yesterday = False
        is_weak_yesterday = False

        if y_state == "涨停":
            if limit_up_type == "烂板/放量":
                is_weak_yesterday = True
            elif limit_up_type in ("一字", "正常缩量封板"):
                is_strong_yesterday = True
        elif y_state == "炸板":
            is_weak_yesterday = True
        elif y_state == "地板":
            if vol_ratio_1d < 0.8:
                is_weak_yesterday = True
        else: # 普通
            # 强化昨日弱判定：缩量跌 或 放量跌，以及收盘偏低位
            if change_pct_1d < -3.0:
                if vol_ratio_1d < 0.8 or vol_ratio_1d > 1.5:
                    is_weak_yesterday = True
            elif close_strength < 0.3 and change_pct_1d < -1.5:
                is_weak_yesterday = True
            
            # 补充强势：昨日连板且收盘强，或大长腿收高位
            if change_pct_1d > 5.0 and close_strength > 0.8 and upper_shadow_pct < 2.0:
                is_strong_yesterday = True

        # 今日竞价强弱判定
        is_strong_today_auc = False
        is_weak_today_auc = False
        if auction_change >= 5.0 and seal_ratio >= 0.2:
            is_strong_today_auc = True
        elif auction_change <= -2.0 or (auction_amount < 5000000 and auction_change < 0):
            is_weak_today_auc = True

        # 结合开盘确认 (如果在盘中) - 收紧逻辑，减少回落误报
        is_strong_today_open = (
            current_change >= auction_change + 1.0  # 开盘显著走强
            or (current_change > 3.0 and current_change >= auction_change - 0.5) # 绝对强
        )
        is_weak_today_open = current_change < auction_change - 2.5 or current_change < -1.0

        # 综合今日强弱
        today_strong = is_strong_today_auc or is_strong_today_open
        today_weak = is_weak_today_auc or is_weak_today_open

        if is_weak_yesterday and today_strong:
            return "weak_to_strong"
        elif is_strong_yesterday and today_weak:
            return "strong_to_weak"
        elif is_strong_yesterday and today_strong:
            return "strong_continue"
        elif is_weak_yesterday and today_weak:
            return "weak_continue"
        
        return "neutral"

    async def build_expectation_gap_analysis(self, today_str: str, candidate_pool: Set[str], auction_items: List[Dict], quote_map: Dict) -> Dict[str, Any]:
        """按四象限分类构建预期差预案"""
        result = {
            "weak_to_strong": [],
            "strong_to_weak": [],
            "strong_continue": [],
            "weak_continue": []
        }
        if not candidate_pool:
            return result
        
        auction_dict = {str(item.get("symbol", "")).strip(): item for item in auction_items if len(str(item.get("symbol", "")).strip()) == 6}
        
        for code in candidate_pool:
            auc_data = auction_dict.get(code, {})
            cur_data = quote_map.get(code, {})
            
            if not auc_data: 
                continue
                
            state = self._evaluate_expectation_state(code, auc_data, cur_data)
            
            if state in result:
                item_info = {
                    "code": code,
                    "name": self._get_stock_name(code),
                    "auc_pct": self._normalize_change_pct(auc_data.get("change_pct", 0.0), scale=1),
                    "cur_pct": self._normalize_change_pct(cur_data.get("change_pct", 0.0), scale=1),
                    "aamt": self._safe_float(auc_data.get("auction_amount_yuan", 0.0)),
                    "plate": self._get_plate_name(self._get_major_plate(code))
                }
                result[state].append(item_info)
        
        # Sort each list by auc_pct descending
        for key in result:
            result[key].sort(key=lambda x: x["auc_pct"], reverse=True)
            
        return result

    def _build_plate_strength_map(self, evaluated: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        plate_strength_map = {}
        for row in evaluated:
            code = row["code"]
            plates = self.plate_updater.stock_to_plates.get(code, []) if hasattr(self.plate_updater, "stock_to_plates") else []
            if not plates:
                continue
            primary_plate = self._get_major_plate(code)
            if not primary_plate:
                continue
            p_metrics = self.plate_updater.get_plate_metrics(primary_plate) if hasattr(self.plate_updater, "get_plate_metrics") else {}
            plate_strength_map[code] = {
                "primary_plate_id": primary_plate,
                "primary_plate": p_metrics.get("name", primary_plate),
                "plate_change_pct": self._safe_float(p_metrics.get("change_pct", 0.0), 0.0),
                "plate_change_rate_1min": self._safe_float(p_metrics.get("change_rate_1min", 0.0), 0.0),
                "plate_amount_2min": self._safe_float(p_metrics.get("total_amount_2min", 0.0), 0.0),
            }
        return plate_strength_map

    def _classify_yesterday_state(self, code: str) -> YesterdayStateProfile:
        """根据昨日 (T-1) 盘后因子和筹码情况，将股票分类为 9 个标准状态。"""
        extra = self.stock_extra.get(code) or {}
        y_state = extra.get("y_state", "普通")
        close_strength = extra.get("close_strength", 0.5)
        limit_up_type = extra.get("limit_up_type", "")
        change_pct_1d = extra.get("change_pct_1d", 0.0)
        vol_ratio_1d = extra.get("vol_ratio_1d", 1.0)
        upper_shadow_pct = extra.get("upper_shadow_pct", 0.0)

        state_type = "NORMAL_NEUTRAL"

        if y_state == "涨停":
            # ZT_STRONG: 一字或缩量封死，强度高，上影线极短
            if (limit_up_type in {"一字", "正常缩量封板"} and 
                close_strength >= 0.82 and 
                upper_shadow_pct <= 1.5):
                state_type = "ZT_STRONG"
            else:
                state_type = "ZT_WEAK"
        elif y_state == "炸板":
            # BOMB_STRONG: 炸板但收盘仍在高位且放量承接
            if change_pct_1d >= 7.0 and close_strength >= 0.65:
                state_type = "BOMB_STRONG"
            else:
                state_type = "BOMB_WEAK"
        elif y_state == "地板":
            # FLOOR_RESCUED: 地板开但被撬起，收盘不惨且有成交量
            if change_pct_1d >= -5.0 and vol_ratio_1d >= 1.2 and close_strength >= 0.35:
                state_type = "FLOOR_RESCUED"
            else:
                state_type = "FLOOR_LOCKED"
        else:
            # 普通股
            if change_pct_1d <= -3.0 or (close_strength <= 0.30 and change_pct_1d <= -1.5):
                state_type = "NORMAL_WEAK"
            elif change_pct_1d >= 3.0 and close_strength >= 0.70:
                state_type = "NORMAL_STRONG"
            else:
                state_type = "NORMAL_NEUTRAL"

        return YesterdayStateProfile(
            code=code,
            state_type=state_type,
            change_pct=change_pct_1d,
            close_strength=close_strength,
            limit_up_type=limit_up_type,
            vol_ratio=vol_ratio_1d,
            upper_shadow=upper_shadow_pct,
            extra=extra
        )

    async def _fetch_stock_dde_cached(self, code: str, today_str: str) -> Dict[str, Any]:
        """[P0 Formalization]: 获取并缓存近期 DDE (主力净额) 流水。"""
        cache_key = f"market:stock:dde:{code}:{today_str}"
        cached = await self.redis.get(cache_key)
        if cached:
            try:
                return json.loads(cached)
            except Exception:
                pass
                
        # Call API
        try:
            from ai.API.StockAnalyzer import StockAnalyzer
            analyzer = StockAnalyzer()
            res = analyzer.get_stock_dde(code)
            if res and isinstance(res, dict):
                await self.redis.set(cache_key, json.dumps(res, ensure_ascii=False), ex=3600*12)
                return res
        except Exception as e:
            logger.warning(f"Fetch DDE failed for {code}: {e}")
        return {}

    def _get_emotion_multiplier(self, sentiment_phase: str, transition_type: str) -> float:
        """根据市场情绪阶段返回评分调节系数 (P2 Formalization)."""
        sentiment_phase = sentiment_phase.lower()
        # 转强系数 (W2S)：冰点、启动期胜率高；退潮期易骗炮；主升期/高位分歧往往缩量买不到或处于后排坑
        w2s_map = {
            "ice_point": 1.25, "start": 1.15, "climax": 0.70, 
            "divergence": 0.80, "retreat": 0.40
        }
        # 转弱系数 (S2W)：退潮和分歧期更容易确认风险
        s2w_map = {
            "retreat": 1.5, "divergence": 1.2, "climax": 0.8, 
            "start": 0.7, "ice_point": 1.0
        }
        
        if transition_type == "W2S":
            return w2s_map.get(sentiment_phase, 1.0)
        return s2w_map.get(sentiment_phase, 1.0)

    def _score_weak_to_strong_transition(
        self, 
        y_profile: YesterdayStateProfile, 
        auction_data: Dict[str, Any], 
        quote_data: Dict[str, Any],
        sentiment_phase: str = "unknown"
    ) -> ExpectationTransitionProfile:
        """计算弱转强分值。只对昨日表现较弱的标的启用。"""
        code = y_profile.code
        
        # 0. 基础弱势权重 (15%)
        y_weak_map = {
            "FLOOR_LOCKED": 1.0, "FLOOR_RESCUED": 0.9, "BOMB_WEAK": 0.8,
            "ZT_WEAK": 0.7, "NORMAL_WEAK": 0.5, "BOMB_STRONG": 0.45
        }
        base_weight = y_weak_map.get(y_profile.state_type, 0.4)
        
        # 1. 竞价突破能量 (25%) -> 对应 Plan 的 "Auction Energy"
        auction_change = self._safe_float(auction_data.get("change_pct", 0.0), 0.0)
        # 根据昨日类型动态调整突破门槛
        thresholds = {
            "FLOOR_LOCKED": (5.0, 8.0), "FLOOR_RESCUED": (3.0, 6.0),
            "BOMB_WEAK": (1.5, 4.0), "ZT_WEAK": (1.0, 3.5),
            "NORMAL_WEAK": (2.0, 5.0)
        }
        low_t, high_t = thresholds.get(y_profile.state_type, (2.0, 4.5))
        if auction_change >= high_t: breakout_score = 1.0
        elif auction_change >= low_t: breakout_score = 0.5 + (auction_change - low_t) / (high_t - low_t) * 0.5
        elif auction_change > 0: breakout_score = 0.2
        else: breakout_score = 0.0
        
        # 2. 竞价成交量/额确认 (25%) -> 核心权重
        auction_amount = self._safe_float(auction_data.get("auction_amount_yuan", 0.0), 0.0)
        # 根据情绪阶段动态调整成交额基准 (5000w/8000w 为典型分水岭)
        amt_base = 80000000 if sentiment_phase.lower() in ("retreat", "divergence") else 50000000
        amount_score = min(1.0, auction_amount / amt_base) if auction_amount > 1e6 else 0.0
        
        # 3. 实时承接/盘中稳健性 (15%)
        current_change = self._safe_float(quote_data.get("change_pct", auction_change), auction_change)
        hold_score = 1.0
        if current_change < auction_change - 1.5: hold_score = 0.6
        if current_change < auction_change - 3.0: hold_score = 0.0
        # 封单修正 (如果有)
        bid_amount = self._safe_float(quote_data.get("bid_amount_yuan", 0.0), 0.0)
        if bid_amount > 5e7: hold_score = min(1.0, hold_score + 0.2)
        
        # 4. 筹码穿越安全度 (20%)
        chip_info = self.chip_peaks.get(code, {})
        chip_safety = 0.5 # 默认中值
        if chip_info and auction_change > 0:
            current_price = self._safe_float(quote_data.get("price", 0.0), 0.0)
            if current_price > 0:
                res = self._score_chip_safety(code, current_price, chip_info)
                chip_safety = res.get("chip_safety_score", 0.5)

        # 加密 DDE 抛压惩罚
        dde_penalty = 0.0
        is_dump = y_profile.extra.get("institutional_dump", False)
        if is_dump:
            # 今日若没有强力的竞价承接，加重处罚
            if breakout_score < 0.6 and amount_score < 0.3:
                dde_penalty = 0.3 # 巨额扣分
                
        # 加权汇总
        total = (
            base_weight * 0.15 +
            breakout_score * 0.25 +
            amount_score * 0.25 +
            hold_score * 0.15 +
            chip_safety * 0.20
        ) - dde_penalty
        total = max(0.0, total)
        
        # 市场阶段修正
        multiplier = self._get_emotion_multiplier(sentiment_phase, "W2S")
        final_score = total * multiplier
        
        # 标签决策
        label = "neutral"
        if final_score >= 0.75 and hold_score >= 0.6: label = "weak_to_strong_confirmed"
        elif final_score >= 0.60: label = "weak_to_strong_watch"
        # 骗炮模型：竞价冲很高但开盘直接走低
        if breakout_score >= 0.8 and hold_score <= 0.2: label = "fake_strength_trap"
        
        return ExpectationTransitionProfile(
            code=code, label=label, total_score=final_score, y_profile=y_profile,
            breakout_score=breakout_score, amount_score=amount_score, 
            hold_score=hold_score, chip_score=chip_safety, plate_score=0.5, # 板块确认暂不作为独立项
            reason=f"W2S({final_score:.2f}): {y_profile.state_type} -> {label}"
        )

    def _score_strong_to_weak_transition(
        self, 
        y_profile: YesterdayStateProfile, 
        auction_data: Dict[str, Any], 
        quote_data: Dict[str, Any],
        sentiment_phase: str = "unknown"
    ) -> ExpectationTransitionProfile:
        """计算强转弱分值。核心识别强势票的不及预期。"""
        code = y_profile.code
        y_strong_score_val = 1.0 if y_profile.state_type == "ZT_STRONG" else 0.7
        
        # 1. 竞价不及预期失败度 (35%)
        auction_change = self._safe_float(auction_data.get("change_pct", 0.0), 0.0)
        # 强势票竞价涨幅低于 2% 即开始标记转弱
        if y_profile.state_type == "ZT_STRONG":
            auction_fail_score = 1.0 if auction_change <= -1.0 else (0.6 if auction_change <= 2.0 else 0.0)
        else:
            auction_fail_score = 1.0 if auction_change <= -3.0 else (0.5 if auction_change <= 0.0 else 0.0)
            
        # 2. 开盘快速杀跌/走低 (30%)
        current_change = self._safe_float(quote_data.get("change_pct", auction_change), auction_change)
        open_fail_score = 0.0
        if current_change < auction_change - 2.0: open_fail_score = 0.7
        if current_change < auction_change - 4.0: open_fail_score = 1.0
        if current_change < 0 and auction_change > 0: open_fail_score = max(open_fail_score, 0.8)
        
        # 3. 实时筹码松动/压力 (15%)
        chip_info = self.chip_peaks.get(code, {})
        chip_fail = 0.5
        if chip_info:
            current_price = self._safe_float(quote_data.get("price", 0.0), 0.0)
            if current_price > 0:
                res = self._score_chip_safety(code, current_price, chip_info)
                # 安全度越低，转弱压力越大
                chip_fail = 1.0 - res.get("chip_safety_score", 0.5)

        # 加总 (P0: 强转弱更多依赖竞价与开盘实时表现)
        total = (
            y_strong_score_val * 0.20 +
            auction_fail_score * 0.35 +
            open_fail_score * 0.30 +
            chip_fail * 0.15
        )
        
        # 情绪阶段修正 (退潮期更应重视转弱)
        multiplier = self._get_emotion_multiplier(sentiment_phase, "S2W")
        final_score = total * multiplier
        
        label = "strong_to_weak_confirmed" if final_score >= 0.72 else ("strong_to_weak_watch" if final_score >= 0.58 else "neutral")
        
        return ExpectationTransitionProfile(
            code=code, label=label, total_score=final_score, y_profile=y_profile,
            metrics={"auction_fail": auction_fail_score, "open_fail": open_fail_score, "chip_fail": chip_fail},
            reason=f"S2W({final_score:.2f}): {y_profile.state_type} -> {label}"
        )

    async def _build_expectation_state_analysis(
        self, 
        today_str: str, 
        core_list: List[Dict], 
        quote_map: Dict[str, Dict],
        sentiment_phase: str = "unknown"
    ) -> ExpectationStateSummary:
        """全量构建今日预期状态分析。"""
        details = {}
        for it in core_list:
            code = it.get('symbol')
            if not code: continue
            
            y_prof = self._classify_yesterday_state(code)
            quote = quote_map.get(code) or {}
            
            # --- DDE LOGIC ---
            # 引入 PyKaipan DDE 动态阈值判定 (近五日均值与五千万绝对回撤)
            try:
                dde_data = await self._fetch_stock_dde_cached(code, today_str)
                ddje_list = dde_data.get("DDJE", [])
                if len(ddje_list) >= 1:
                    y_net_flow = self._safe_float(ddje_list[0], 0.0)
                    past_5_net = [x for x in ddje_list[1:6] if isinstance(x, (int, float))] if len(ddje_list) > 1 else []
                    avg_5_net = sum(past_5_net) / len(past_5_net) if past_5_net else 0.0
                    
                    if y_net_flow < -50000000:  # 绝对阈值：昨日大幅流出超五千万
                        if y_net_flow < (avg_5_net - 30000000):  # 相对阈值：比过去5日平均流出惨重
                            y_prof.extra["institutional_dump"] = True
                            y_prof.extra["y_net_flow"] = y_net_flow
            except Exception as e:
                logger.warning(f"Failed to process DDE for {code}: {e}")
            # -----------------
            
            if y_prof.state_type in ("ZT_STRONG", "NORMAL_STRONG"):
                prof = self._score_strong_to_weak_transition(y_prof, it, quote, sentiment_phase)
            else:
                prof = self._score_weak_to_strong_transition(y_prof, it, quote, sentiment_phase)
            
            details[code] = prof
            
        # 汇总得分
        confirmed_w2s = [p for p in details.values() if p.label == "weak_to_strong_confirmed"]
        watch_w2s = [p for p in details.values() if p.label == "weak_to_strong_watch"]
        fake_traps = [p for p in details.values() if p.label == "fake_strength_trap"]
        confirmed_s2w = [p for p in details.values() if p.label == "strong_to_weak_confirmed"]
        watch_s2w = [p for p in details.values() if p.label == "strong_to_weak_watch"]
        
        sample_size = len(details)
        norm_factor = max(5, sample_size * 0.03)
        
        w2s_score = min(1.0, (len(confirmed_w2s) * 1.0 + len(watch_w2s) * 0.5) / norm_factor)
        s2w_score = min(1.0, (len(confirmed_s2w) * 1.0 + len(watch_s2w) * 0.5) / norm_factor)
        fake_trap_score = len(fake_traps) / max(1, len(confirmed_w2s) + len(watch_w2s) + len(fake_traps))
        
        return ExpectationStateSummary(
            date=today_str, ts=int(time.time() * 1000),
            weak_to_strong_window_score=w2s_score,
            strong_to_weak_pressure_score=s2w_score,
            fake_strength_trap_score=fake_trap_score,
            details_by_code=details,
            weak_to_strong_top=[p.code for p in sorted(confirmed_w2s + watch_w2s, key=lambda x: x.total_score, reverse=True)[:10]],
            strong_to_weak_top=[p.code for p in sorted(confirmed_s2w + watch_s2w, key=lambda x: x.total_score, reverse=True)[:10]],
            fake_strength_top=[p.code for p in sorted(fake_traps, key=lambda x: x.total_score, reverse=True)[:10]]
        )

    def _score_chip_safety(self, code: str, current_price: float, chip_peak: Dict[str, Any]) -> Dict[str, Any]:
        peak_price = self._safe_float(chip_peak.get("peak_price", 0.0), 0.0)
        avg_cost = self._safe_float(chip_peak.get("avg_cost", 0.0), 0.0)
        profit_ratio = self._safe_float(chip_peak.get("profit_ratio", 0.0), 0.0)
        concentration = self._safe_float(chip_peak.get("concentration", 0.0), 0.0)

        breakout_score = 1.0 if peak_price > 0 and current_price >= peak_price else 0.5 if peak_price > 0 and current_price >= peak_price * 0.97 else 0.0
        profit_ratio_score = min(1.0, profit_ratio)
        concentration_score = 1.0 if avg_cost > 0 and concentration > 0 and concentration / avg_cost < 0.10 else 0.6 if avg_cost > 0 and concentration > 0 and concentration / avg_cost < 0.20 else 0.2

        cost_distance_score = 0.0
        if avg_cost > 0:
            dist = abs(current_price - avg_cost) / avg_cost
            if dist <= 0.05:
                cost_distance_score = 1.0
            elif dist <= 0.10:
                cost_distance_score = 0.6
            else:
                cost_distance_score = 0.2

        chip_safety_score = (
            0.35 * breakout_score
            + 0.25 * profit_ratio_score
            + 0.20 * concentration_score
            + 0.20 * cost_distance_score
        )
        chip_safety_score = round(min(1.0, chip_safety_score), 4)

        chip_zone_status = "inside_pressure_zone"
        if peak_price > 0 and current_price >= peak_price and chip_safety_score >= 0.65:
            chip_zone_status = "breakout_safe"
        elif peak_price > 0 and current_price >= peak_price * 0.97 and chip_safety_score >= 0.50:
            chip_zone_status = "safety_near_peak"
        elif avg_cost > 0 and abs(current_price - avg_cost) / avg_cost > 0.12:
            chip_zone_status = "overextended"

        return {
            "chip_safety_score": chip_safety_score,
            "chip_zone_status": chip_zone_status,
            "peak_price": peak_price,
            "avg_cost": avg_cost,
            "profit_ratio": profit_ratio,
            "concentration": concentration,
        }

    def _score_stock_role(self, row, extra, plate_ctx, chip_safety_score, aamt_max) -> Dict[str, float]:
        aamt = self._safe_float(row.get("aamt", 0.0), 0.0)
        bid = self._safe_float(row.get("bid", 0.0), 0.0)
        cur = self._safe_float(row.get("cur", 0.0), 0.0)

        real_cap = self._safe_float(extra.get("real_market_cap", 0.0), 0.0)
        avg_turnover_5d = self._safe_float(extra.get("avg_turnover_5d", 0.0), 0.0)
        change_pct_5d = self._safe_float(extra.get("change_pct_5d", 0.0), 0.0)
        close_strength = self._safe_float(extra.get("close_strength", 0.5), 0.5)
        limit_up_type = str(extra.get("limit_up_type", ""))

        plate_change = self._safe_float(plate_ctx.get("plate_change_pct", 0.0), 0.0)
        plate_speed = self._safe_float(plate_ctx.get("plate_change_rate_1min", 0.0), 0.0)
        plate_amt2 = self._safe_float(plate_ctx.get("plate_amount_2min", 0.0), 0.0)

        auction_amount_rank_score = min(1.0, aamt / max(1.0, aamt_max))
        current_strength_score = min(1.0, max(0.0, cur / 10.0))
        seal_strength_score = min(1.0, bid / max(1.0, aamt))
        plate_strength_score = min(1.0, max(0.0, plate_change / 5.0) * 0.6 + max(0.0, plate_speed / 2.0) * 0.2 + min(1.0, plate_amt2 / 2e8) * 0.2)

        medium_cap_score = 1.0 if 80 <= real_cap <= 500 else 0.5 if 40 <= real_cap < 80 or 500 < real_cap <= 800 else 0.2
        auction_participation_score = min(1.0, aamt / 8e7)
        activity_score = min(1.0, (avg_turnover_5d / 8.0) * 0.4 + max(0.0, change_pct_5d / 15.0) * 0.2 + close_strength * 0.2 + (0.2 if limit_up_type in ("一字", "正常缩量封板", "烂板/放量") else 0.0))

        leadership_score = (
            0.30 * auction_amount_rank_score
            + 0.25 * current_strength_score
            + 0.20 * seal_strength_score
            + 0.15 * plate_strength_score
            + 0.10 * chip_safety_score
        )

        follow_score = (
            0.30 * plate_strength_score
            + 0.20 * medium_cap_score
            + 0.20 * auction_participation_score
            + 0.15 * activity_score
            + 0.15 * chip_safety_score
        )

        return {
            "leadership_score": round(min(1.0, leadership_score), 4),
            "follow_score": round(min(1.0, follow_score), 4),
        }

    def _classify_stock_role(self, scores, plate_ctx, chip_safety_score, plate_rank) -> Tuple[str, int]:
        leadership_score = self._safe_float(scores.get("leadership_score", 0.0), 0.0)
        follow_score = self._safe_float(scores.get("follow_score", 0.0), 0.0)
        plate_change = self._safe_float(plate_ctx.get("plate_change_pct", 0.0), 0.0)

        if plate_rank == 1 and leadership_score >= 0.70 and plate_change > 0 and chip_safety_score >= 0.55:
            return "leader", 1
        if plate_rank <= 2 and leadership_score >= 0.45 and plate_change > 0 and chip_safety_score >= 0.45:
            return "core_anchor", 2
        if follow_score >= 0.30 and plate_change > 0:
            return "relay_candidate", 3
        return "follower", 4

    def _setup_catalog(self) -> List[str]:
        return [
            "LOW_LEVEL_RELAY",
            "CORE_DIP_BUYING",
            "A_DIRECT",
            "A_TO_B",
            "WEAK_TO_STRONG_REPAIR",
            "ICE_REPAIR_CORE",
        ]

    def _normalize_market_phase_for_matrix(self, phase: str) -> str:
        phase = str(phase or "").upper()
        alias = {
            "CONSENSUS_ACCEL": "climax",
            "FIRST_DIVERGE": "divergence",
            "CONSISTENT": "climax",
            "CONSENSUS_HIGH": "climax",
            "REPAIR": "ice_point",
            "RETREAT_ICE": "retreat",
            "ICE_POINT": "ice_point",
            "IGNITION": "start",
            "MAIN_RISE": "climax",
            "DIVERGE": "divergence",
            "RETREAT": "retreat",
        }
        return alias.get(phase, "start")

    def _normalize_plate_phase_for_matrix(self, phase: str) -> str:
        phase = str(phase or "").upper()
        alias = {
            "PLATE_IGNITION": "start",
            "PLATE_MAIN_RISE": "climax",
            "PLATE_DIVERGE": "divergence",
            "PLATE_RETREAT": "retreat",
            "PLATE_STABLE": "climax",
            "IGNITION": "start",
            "MAIN_RISE": "climax",
            "DIVERGE": "divergence",
            "RETREAT": "retreat",
        }
        return alias.get(phase, "start")

    def _default_market_setup_matrix(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        setups = self._setup_catalog()
        phases = ["start", "climax", "divergence", "retreat", "ice_point"]
        allowed_map = {
            "start": {"WEAK_TO_STRONG_REPAIR": 0.65, "A_DIRECT": 0.55},
            "climax": {"LOW_LEVEL_RELAY": 0.90, "A_DIRECT": 0.80, "A_TO_B": 0.78, "WEAK_TO_STRONG_REPAIR": 0.60},
            "divergence": {"CORE_DIP_BUYING": 0.88, "WEAK_TO_STRONG_REPAIR": 0.58},
            "retreat": {"ICE_REPAIR_CORE": 0.35},
            "ice_point": {"ICE_REPAIR_CORE": 0.72, "WEAK_TO_STRONG_REPAIR": 0.52},
        }
        matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for phase in phases:
            matrix[phase] = {}
            for setup in setups:
                weight = allowed_map.get(phase, {}).get(setup, 0.0)
                matrix[phase][setup] = {
                    "allowed": weight > 0,
                    "weight": round(weight, 4),
                    "trigger_count": 0,
                    "win_rate_proxy": 0.0,
                    "avg_mfe": 0.0,
                    "avg_mae": 0.0,
                    "expectation_failure_rate": 0.0,
                    "stale_penalty": 0.0,
                    "confidence": 0.2 if weight > 0 else 0.0,
                }
        return matrix

    def _default_plate_setup_matrix(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        setups = self._setup_catalog()
        phases = ["start", "climax", "divergence", "retreat"]
        allowed_map = {
            "start": {"WEAK_TO_STRONG_REPAIR": 0.72, "A_DIRECT": 0.55},
            "climax": {"LOW_LEVEL_RELAY": 0.92, "A_DIRECT": 0.82, "A_TO_B": 0.82},
            "divergence": {"CORE_DIP_BUYING": 0.90, "WEAK_TO_STRONG_REPAIR": 0.56},
            "retreat": {"ICE_REPAIR_CORE": 0.20},
        }
        matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for phase in phases:
            matrix[phase] = {}
            for setup in setups:
                weight = allowed_map.get(phase, {}).get(setup, 0.0)
                matrix[phase][setup] = {
                    "allowed": weight > 0,
                    "weight": round(weight, 4),
                    "trigger_count": 0,
                    "win_rate_proxy": 0.0,
                    "avg_mfe": 0.0,
                    "avg_mae": 0.0,
                    "expectation_failure_rate": 0.0,
                    "stale_penalty": 0.0,
                    "confidence": 0.2 if weight > 0 else 0.0,
                }
        return matrix

    def _default_role_setup_matrix(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        setups = self._setup_catalog()
        allowed_map = {
            "leader": {"A_DIRECT": 0.88, "CORE_DIP_BUYING": 0.72, "WEAK_TO_STRONG_REPAIR": 0.60},
            "core_anchor": {"CORE_DIP_BUYING": 0.92, "LOW_LEVEL_RELAY": 0.65, "A_DIRECT": 0.70},
            "relay_candidate": {"LOW_LEVEL_RELAY": 0.92, "A_TO_B": 0.86, "WEAK_TO_STRONG_REPAIR": 0.54},
            "follower": {},
        }
        matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for role in ("leader", "core_anchor", "relay_candidate", "follower"):
            matrix[role] = {}
            for setup in setups:
                weight = allowed_map.get(role, {}).get(setup, 0.0)
                matrix[role][setup] = {
                    "allowed": weight > 0,
                    "weight": round(weight, 4),
                    "trigger_count": 0,
                    "win_rate_proxy": 0.0,
                    "avg_mfe": 0.0,
                    "avg_mae": 0.0,
                    "expectation_failure_rate": 0.0,
                    "stale_penalty": 0.0,
                    "confidence": 0.2 if weight > 0 else 0.0,
                }
        return matrix

    def _safe_json_dict(self, raw: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw:
            try:
                val = json.loads(raw)
                if isinstance(val, dict):
                    return val
            except Exception:
                pass
        return default or {}

    def _match_role_to_setup(self, role_type: str, setup_type: str) -> bool:
        role_type = str(role_type or "follower")
        setup_type = str(setup_type or "").upper()
        role_map = {
            "LOW_LEVEL_RELAY": {"relay_candidate", "core_anchor"},
            "CORE_DIP_BUYING": {"leader", "core_anchor"},
            "A_DIRECT": {"leader", "core_anchor"},
            "A_TO_B": {"relay_candidate", "core_anchor"},
            "WEAK_TO_STRONG_REPAIR": {"leader", "core_anchor", "relay_candidate"},
            "ICE_REPAIR_CORE": {"leader", "core_anchor"},
        }
        return role_type in role_map.get(setup_type, set())

    async def _load_recent_pattern_records(self, today_str: str, windows: Optional[List[int]] = None) -> Dict[str, List[Dict[str, Any]]]:
        windows = windows or [5, 10, 20]
        out: Dict[str, List[Dict[str, Any]]] = {str(w): [] for w in windows}
        max_window = max(windows) if windows else 0
        days: List[str] = []
        cur = today_str
        for _ in range(max_window):
            days.append(cur)
            try:
                cur = self.calendar.get_previous_trade_day(cur)
            except Exception:
                break

        for idx, day in enumerate(days, start=1):
            try:
                raw = await self.redis.hgetall(f"market:pattern_records:{day}")
                payload = self._safe_json_list(raw.get("records", "[]")) if raw else []
            except Exception:
                payload = []
            for w in windows:
                if idx <= w:
                    out[str(w)].extend(payload)
        return out

    def _summarize_pattern_records(
        self,
        records: List[Dict[str, Any]],
        *,
        market_phase: Optional[str] = None,
        plate_phase: Optional[str] = None,
        setup_type: Optional[str] = None,
        role_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        filtered: List[Dict[str, Any]] = []
        for rec in records:
            if market_phase and self._normalize_market_phase_for_matrix(rec.get("market_phase")) != self._normalize_market_phase_for_matrix(market_phase):
                continue
            if plate_phase and self._normalize_plate_phase_for_matrix(rec.get("plate_phase")) != self._normalize_plate_phase_for_matrix(plate_phase):
                continue
            if setup_type and str(rec.get("setup_type", "")).upper() != str(setup_type).upper():
                continue
            if role_type and str(rec.get("role_type", "")) != str(role_type):
                continue
            filtered.append(rec)

        trigger_count = len(filtered)
        if trigger_count == 0:
            return {
                "trigger_count": 0,
                "win_rate_proxy": 0.0,
                "avg_mfe": 0.0,
                "avg_mae": 0.0,
                "expectation_failure_rate": 0.0,
                "stale_penalty": 0.0,
                "confidence": 0.0,
            }

        success_count = 0
        mfe_list: List[float] = []
        mae_list: List[float] = []
        fail_count = 0
        score_sum = 0.0
        for rec in filtered:
            outcome = rec.get("outcome", {}) if isinstance(rec.get("outcome"), dict) else {}
            success = outcome.get("success")
            if success is True:
                success_count += 1
            if outcome.get("exit_reason") == "expectation_failure":
                fail_count += 1
            mfe_list.append(self._safe_float(outcome.get("mfe", 0.0), 0.0))
            mae_list.append(self._safe_float(outcome.get("mae", 0.0), 0.0))
            score_sum += self._safe_float(rec.get("signal_score", 0.0), 0.0)

        win_rate_proxy = (success_count / trigger_count) if success_count > 0 else min(1.0, (score_sum / max(1, trigger_count)) / 100.0)
        confidence = min(1.0, trigger_count / 10.0)
        return {
            "trigger_count": trigger_count,
            "win_rate_proxy": round(win_rate_proxy, 4),
            "avg_mfe": round(float(np.mean(mfe_list)) if mfe_list else 0.0, 4),
            "avg_mae": round(float(np.mean(mae_list)) if mae_list else 0.0, 4),
            "expectation_failure_rate": round(fail_count / trigger_count, 4),
            "stale_penalty": 0.0 if trigger_count >= 3 else round((3 - trigger_count) * 0.08, 4),
            "confidence": round(confidence, 4),
        }

    async def _build_pattern_matrix(
        self,
        today_str: str,
        *,
        market_phase: str,
        plate_phase_map: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        market_matrix = self._default_market_setup_matrix()
        plate_matrix = self._default_plate_setup_matrix()
        role_matrix = self._default_role_setup_matrix()
        recent = await self._load_recent_pattern_records(today_str, [5, 10, 20])

        for phase, setup_map in market_matrix.items():
            for setup_type in setup_map.keys():
                stats = self._summarize_pattern_records(recent["10"], market_phase=phase, setup_type=setup_type)
                setup_map[setup_type].update(stats)
                if stats["trigger_count"] > 0:
                    base = setup_map[setup_type]["weight"]
                    setup_map[setup_type]["weight"] = round(self._clamp01(base * 0.65 + stats["win_rate_proxy"] * 0.35), 4)
                    setup_map[setup_type]["allowed"] = setup_map[setup_type]["weight"] >= 0.25

        for phase, setup_map in plate_matrix.items():
            for setup_type in setup_map.keys():
                stats = self._summarize_pattern_records(recent["10"], plate_phase=phase, setup_type=setup_type)
                setup_map[setup_type].update(stats)
                if stats["trigger_count"] > 0:
                    base = setup_map[setup_type]["weight"]
                    setup_map[setup_type]["weight"] = round(self._clamp01(base * 0.7 + stats["win_rate_proxy"] * 0.3), 4)
                    setup_map[setup_type]["allowed"] = setup_map[setup_type]["weight"] >= 0.20

        for role, setup_map in role_matrix.items():
            for setup_type in setup_map.keys():
                stats = self._summarize_pattern_records(recent["10"], role_type=role, setup_type=setup_type)
                setup_map[setup_type].update(stats)
                if stats["trigger_count"] > 0:
                    base = setup_map[setup_type]["weight"]
                    setup_map[setup_type]["weight"] = round(self._clamp01(base * 0.7 + stats["win_rate_proxy"] * 0.3), 4)
                    setup_map[setup_type]["allowed"] = setup_map[setup_type]["weight"] >= 0.20

        current_phase = self._normalize_market_phase_for_matrix(market_phase)
        payload = {
            "ts": int(time.time() * 1000),
            "date": today_str,
            "market_phase": current_phase,
            "plate_phase_map": plate_phase_map or {},
            "market_matrix": market_matrix,
            "plate_matrix": plate_matrix,
            "role_matrix": role_matrix,
            "windows": {
                "5d_records": len(recent["5"]),
                "10d_records": len(recent["10"]),
                "20d_records": len(recent["20"]),
            },
        }
        return payload

    async def _store_pattern_matrix(
        self,
        today_str: str,
        matrix_payload: Dict[str, Any],
    ) -> None:
        out_key = f"market:pattern_matrix:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": matrix_payload.get("ts", int(time.time() * 1000)),
                "market_phase": matrix_payload.get("market_phase", ""),
                "payload": json.dumps(matrix_payload, ensure_ascii=False),
            },
        )
        await self.redis.expire(out_key, 86400)

    async def _load_pattern_matrix(self, today_str: str) -> Dict[str, Any]:
        raw = await self.redis.hgetall(f"market:pattern_matrix:{today_str}")
        if not raw:
            return {}
        return self._safe_json_dict(raw.get("payload", "{}"))

    def _resolve_setup_matrix_weight(
        self,
        matrix: Dict[str, Any],
        *,
        market_phase: str,
        plate_phase: str,
        role_type: str,
        setup_type: str,
    ) -> Dict[str, Any]:
        setup_type = str(setup_type or "").upper()
        market_phase = self._normalize_market_phase_for_matrix(market_phase)
        plate_phase = self._normalize_plate_phase_for_matrix(plate_phase)
        role_type = str(role_type or "follower")

        mm = (((matrix.get("market_matrix") or {}).get(market_phase) or {}).get(setup_type) or {})
        pm = (((matrix.get("plate_matrix") or {}).get(plate_phase) or {}).get(setup_type) or {})
        rm = (((matrix.get("role_matrix") or {}).get(role_type) or {}).get(setup_type) or {})

        market_weight = self._safe_float(mm.get("weight", 0.0), 0.0)
        plate_weight = self._safe_float(pm.get("weight", 0.0), 0.0)
        role_weight = self._safe_float(rm.get("weight", 0.0), 0.0)
        weight = round(market_weight * plate_weight * role_weight, 4)
        confidence = round(min(1.0, (
            self._safe_float(mm.get("confidence", 0.0), 0.0)
            + self._safe_float(pm.get("confidence", 0.0), 0.0)
            + self._safe_float(rm.get("confidence", 0.0), 0.0)
        ) / 3.0), 4)
        allowed = bool(mm.get("allowed")) and bool(pm.get("allowed")) and bool(rm.get("allowed")) and self._match_role_to_setup(role_type, setup_type)
        return {
            "allowed": allowed,
            "weight": weight,
            "confidence": confidence,
            "market_weight": market_weight,
            "plate_weight": plate_weight,
            "role_weight": role_weight,
        }

    async def _record_pattern_signals(
        self,
        today_str: str,
        signal_cards: List[Dict[str, Any]],
    ) -> None:
        if not signal_cards:
            return
        out_key = f"market:pattern_records:{today_str}"
        existing = await self.redis.hgetall(out_key)
        records = self._safe_json_list(existing.get("records", "[]")) if existing else []
        seen = {
            (
                str(x.get("signal_id", "")),
                str(x.get("setup_type", "")),
                str(x.get("code", "")),
            ) for x in records if isinstance(x, dict)
        }
        appended = 0
        for sig in signal_cards:
            key = (str(sig.get("signal_id", "")), str(sig.get("setup_type", "")), str(sig.get("code", "")))
            if key in seen:
                continue
            plate_name = str(sig.get("theme", ""))
            record = {
                "ts": int(time.time() * 1000),
                "date": today_str,
                "market_phase": sig.get("market_phase", ""),
                "plate_id": plate_name,
                "plate_phase": sig.get("plate_phase", ""),
                "setup_type": sig.get("setup_type", ""),
                "code": sig.get("code", ""),
                "role_type": sig.get("role_type", ""),
                "signal_score": self._safe_float(sig.get("signal_score", 0.0), 0.0),
                "chip_zone_status": (sig.get("chip_context") or {}).get("chip_zone_status", ""),
                "entry_context": sig.get("entry_hint", {}),
                "exit_context": sig.get("exit_plan", {}),
                "outcome": {
                    "success": None,
                    "mfe": 0.0,
                    "mae": 0.0,
                    "holding_minutes": 0,
                    "exit_reason": "",
                },
            }
            records.append(record)
            seen.add(key)
            appended += 1
        if appended > 0:
            await self.redis.hset(
                out_key,
                mapping={
                    "ts": int(time.time() * 1000),
                    "record_count": len(records),
                    "records": json.dumps(records, ensure_ascii=False),
                },
            )
            await self.redis.expire(out_key, 30 * 86400)

    def _calculate_arbitrage_score(self, code: str, current_price: float, auction_amount: float, indicators: Dict, chip_peak: Dict) -> float:
        """
        基于多因子的套利打分体系
        1. 板块属性：优先创业板/科创板
        2. 市值资金承接力（使用 f10.csv 真实流通市值）
        3. 涨幅位置：近期累计涨幅较小，位置偏低
        4. 筹码峰压制：当前价格已突破核心筹码峰
        5. 竞价强度：竞价成交量/成金额靠前
        6. 股性活跃：近期换手率/涨停等异动
        """
        score = 0.0
        
        # 1. 创业板/科创板加分
        if code.startswith(("300", "301", "688")):
            score += 20
            
        # 2. 真实市值资金承接力
        real_cap = indicators.get("real_market_cap", 0)  # 亿元
        if real_cap <= 0:
            # fallback: C++ 推送的总市值（单位：分/元），尝试换算为亿元（A股常见量级）
            raw_cap = indicators.get("market_cap", 0)
            if raw_cap > 1000000:       # > 100万元
                real_cap = raw_cap / 1e8
            elif raw_cap > 0:
                real_cap = raw_cap     # 已经是亿元单位
        
        if real_cap >= 50:       # 50亿以上均衡承接
            score += 15
        elif real_cap >= 20:     # 20亿以上
            score += 10
        elif real_cap >= 5:      # 5亿以上
            score += 5
            
        # 3. 近期涨幅（涨幅不能太大，避免追高）
        pct_5d = indicators.get("change_pct_5d", 0)
        if pct_5d < 10:
            score += 15
        elif pct_5d < 20:
            score += 5
        # 涨幅超过20%不加分
            
        # 4. 筹码峰分析（多维度打分）
        if chip_peak and current_price > 0:
            peak_price = chip_peak.get("peak_price", 9999)
            avg_cost = chip_peak.get("avg_cost", 0)
            profit_ratio = chip_peak.get("profit_ratio", 0)
            concentration = chip_peak.get("concentration", 999)
            
            # 4a. 突破核心筹码峰 → 上方无套牢盘压力
            if current_price >= peak_price:
                score += 15
            elif current_price >= peak_price * 0.95:
                score += 5  # 接近筹码峰
            
            # 4b. 盈利筹码比例高 → 持仓者心态稳定、惜售
            if profit_ratio >= 0.8:
                score += 8   # 80%以上筹码盈利
            elif profit_ratio >= 0.6:
                score += 5
            
            # 4c. 筹码集中度高 → 主力控盘特征明显
            # concentration 是70%筹码覆盖的价格宽度，越小越集中
            if avg_cost > 0 and concentration > 0:
                # 相对集中度 = 集中区间宽度 / 平均成本
                relative_conc = concentration / avg_cost
                if relative_conc < 0.10:   # 极度集中（<10%）
                    score += 7
                elif relative_conc < 0.20:  # 较集中（<20%）
                    score += 4
                
        # 5. 竞价强度
        if auction_amount > 50000000:  # 5000万
            score += 15
        elif auction_amount > 20000000:  # 2000万
            score += 10
        elif auction_amount > 10000000:  # 1000万
            score += 5
            
        # 6. 股性活跃 (近期涨停/上影线/高换手率)
        if indicators.get("limit_up_days_5", 0) > 0:
            score += 5
        if indicators.get("upper_shadow_days_5", 0) > 0:
            score += 3
        if indicators.get("avg_turnover_5d", 0) > 3.0:  # 换手率>3%
            score += 5
        elif indicators.get("is_active", 0) > 0:
            score += 3
            
        return score

    async def analyze_expectations(self, core: List[Dict], quotes: List[Dict], prev_limit_up_set: Set[str], prev_hot_plates: Dict[str, float] = None, sentiment_phase: str = "unknown") -> Dict[str, Any]:
        """统一分析市场结构与预期差 (Market Structure & Expectations)"""
        if prev_hot_plates is None: prev_hot_plates = {}
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        # 1. 构建全量分析快照 (Standardized Phase A)
        quote_map = {core[i].get('symbol'): quotes[i] for i in range(len(core)) if i < len(quotes) and core[i].get('symbol')}
        summary = await self._build_expectation_state_analysis(today_str, core, quote_map, sentiment_phase)
        
        # 将结果写入 Redis ( market:expectation_state:{date} )
        out_key = f"market:expectation_state:{today_str}"
        await self.redis.hset(out_key, mapping={
            "ts": summary.ts,
            "payload": json.dumps(summary.to_dict(), ensure_ascii=False)
        })
        await self.redis.expire(out_key, 86400)

        # 2. 核心转换信号 (Compatibility Layer)
        signals: List[ExpectationSignal] = []
        for code, prof in summary.details_by_code.items():
            if prof.label != "neutral":
                sig_type = prof.label
                # 映射标签到旧类型名以便兼容
                if prof.label in ("weak_to_strong_confirmed", "weak_to_strong_watch"):
                    sig_type = "weak_to_strong"
                elif prof.label in ("strong_to_weak_confirmed", "strong_to_weak_watch"):
                    sig_type = "strong_to_weak"
                
                auction_change = self._safe_float(prof.metrics.get("auction_change", 0.0), 0.0)
                open_change = self._safe_float(quote_map.get(code, {}).get("change_pct", 0.0), 0.0)
                
                signals.append(ExpectationSignal(
                    code=code, type=sig_type, change=open_change, score=prof.total_score * 3.0,
                    details=prof.metrics, reason=prof.reason
                ))
        
        # 3. 辅助过程信号 (V-Shape, A-Shape, Price Gap, Plate Divergence)
        plate_scores = {}
        for i, it in enumerate(core):
            code = it.get('symbol')
            if not code or code not in quote_map: continue
            q = quote_map[code]
            
            # --- 基础数据 ---
            auction_change = self._safe_float(it.get('change_pct', 0) or 0)
            open_change = self._safe_float(q.get('change_pct') or q.get('change') or 0)
            
            # --- 扩展数据 (High/Low) ---
            high_pct = open_change 
            low_pct = open_change
            try:
                current_price = float(q.get('price') or 0)
                high_price = float(q.get('high') or current_price)
                low_price = float(q.get('low') or current_price)
                pre_close = float(q.get('last_close') or q.get('pre_close') or 0)
                if pre_close > 0:
                    high_pct = (high_price - pre_close) / pre_close * 100
                    low_pct = (low_price - pre_close) / pre_close * 100
            except: pass

            # A) 过程博弈 (V-Shape / A-Shape)
            if low_pct < auction_change - 3.0 and open_change > low_pct + 4.0:
                 signals.append(ExpectationSignal(
                    code=code, type='process_v_shape', change=open_change, score=2.5,
                    details={'auction': auction_change, 'low': low_pct, 'open': open_change},
                    reason=f"深V反核(最低{low_pct:.1f}% -> 现价{open_change:.1f}%)"
                ))
            if high_pct > auction_change + 3.0 and open_change < high_pct - 5.0:
                 signals.append(ExpectationSignal(
                    code=code, type='process_a_shape', change=open_change, score=2.5,
                    details={'auction': auction_change, 'high': high_pct, 'open': open_change},
                    reason=f"冲高回落(最高{high_pct:.1f}% -> 现价{open_change:.1f}%)"
                ))
            
            # B) 价格预期差 (Price Gap)
            if auction_change > 5.0 and open_change < auction_change - 4.0:
                 signals.append(ExpectationSignal(
                    code=code, type='price_gap_fade', change=open_change, score=2.0,
                    details={'auction': auction_change, 'open': open_change},
                    reason=f"竞价强({auction_change:.1f}%) ->现价弱({open_change:.1f}%)"
                ))
            elif open_change > auction_change + 3.0 and open_change > 0:
                 signals.append(ExpectationSignal(
                    code=code, type='price_gap_rise', change=open_change, score=3.0,
                    details={'auction': auction_change, 'open': open_change},
                    reason=f"竞价弱({auction_change:.1f}%) ->现价强({open_change:.1f}%)"
                ))

            # Plate Strength Logic (Multi-dimensional Aggregation Phase E)
            p_ids = self.plate_updater.stock_to_plates.get(code, [])
            auction_amount = self._safe_float(it.get('auction_amount_yuan', 0.0), 0.0)
            for pid in p_ids:
                if pid not in plate_scores: plate_scores[pid] = []
                plate_scores[pid].append((auction_change, auction_amount))

        # 4. 市场结构统计 (Compatibility Layer)
        struct_stats = {
            "yesterday_strong_promotion": len([p for p in summary.details_by_code.values() if p.y_profile.state_type in ("ZT_STRONG", "NORMAL_STRONG") and "confirmed" in p.label and "strong" in p.label]),
            "yesterday_strong_demotion": len([p for p in summary.details_by_code.values() if p.y_profile.state_type in ("ZT_STRONG", "NORMAL_STRONG") and "weak" in p.label]),
            "core_repair": 0,
            "plate_strength": {},
            "top_bid_ratio": [],
            "metrics": {
                "w2s_window_score": summary.weak_to_strong_window_score,
                "s2w_pressure_score": summary.strong_to_weak_pressure_score,
                "fake_trap_score": summary.fake_strength_trap_score
            }
        }
        
        for pid, data_list in plate_scores.items():
            if len(data_list) >= 3: 
                # Sort by change_pct desc
                data_list.sort(key=lambda x: x[0], reverse=True)
                top3 = data_list[:3]
                avg_auc = sum(x[0] for x in top3) / 3
                total_aamt = sum(x[1] for x in top3)
                aamt_mln = total_aamt / 1e6
                
                # Combined Power Score: Price Energy + Volume Bonus (log scale)
                volume_bonus = min(2.5, max(0.0, np.log10(aamt_mln + 1) * 0.4))
                combined_strength = avg_auc + volume_bonus
                
                if combined_strength > 2.5:
                    p_name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
                    struct_stats["plate_strength"][p_name] = {
                        "strength": round(combined_strength, 2),
                        "avg_auc": round(avg_auc, 2),
                        "total_aamt_mln": round(aamt_mln, 1)
                    }

        return {
            "signals": signals,
            "stats": struct_stats,
            "summary": summary
        }


    async def calculate_open_scenario(
        self, today_str: str, auction_items: Optional[List[Dict[str, Any]]] = None, quote_map: Optional[Dict[str, Dict]] = None, force: bool = False
    ) -> None:
        """9:30-9:40 执行一次：开盘验证全方位预期差。
        Unified Logic: Market Structure & Expectations
        """
        if not force:
            if not self.manual_date:
                now_time = datetime.now().time()
                if not (datetime.strptime("09:30", "%H:%M").time() <= now_time <= datetime.strptime("09:40", "%H:%M").time()):
                    return
            
            if self.opening_verification_done: return

    async def _get_yesterday_baseline(self, today_str: str) -> Tuple[Set[str], Dict[str, float]]:
        """Get yesterday's limit-up stocks and hot plates as a baseline."""
        prev_limit_up_set: Set[str] = set()
        prev_hot_plates: Dict[str, float] = {}
        prev_day = self.calendar.get_previous_trade_day(today_str)

        try:
            # 1. Start from Redis cache for today's session
            hist_cache_key = f"market:prev_day_baseline:{today_str}"
            hist_cache = await self.redis.hgetall(hist_cache_key)
            if hist_cache:
                cached_lu = hist_cache.get("prev_limit_up")
                cached_hp = hist_cache.get("prev_hot_plates")
                if cached_lu:
                    prev_limit_up_set = set(json.loads(cached_lu))
                if cached_hp:
                    prev_hot_plates = json.loads(cached_hp)
                if prev_limit_up_set or prev_hot_plates:
                    return prev_limit_up_set, prev_hot_plates

            # 2. If cache empty, fetch from primary sources
            if prev_day:
                # Path A: pykaipan API
                if UnifiedMarketDataFetcher:
                    try:
                        fetcher = UnifiedMarketDataFetcher()
                        if hasattr(fetcher, 'stock_analyzer') and fetcher.stock_analyzer:
                            analyzer = fetcher.stock_analyzer
                            plates_res = analyzer.get_his_plates(prev_day)
                            if plates_res and 'list' in plates_res:
                                for item in plates_res['list'][:5]:
                                    if len(item) >= 4:
                                        prev_hot_plates[str(item[1])] = float(item[3])
                            bans_res = analyzer.get_his_bans(prev_day)
                            if bans_res and 'list' in bans_res:
                                for item in bans_res['list']:
                                    if len(item) >= 1:
                                        prev_limit_up_set.add(str(item[0]).strip())
                    except Exception as e:
                        logger.warning(f"[_get_yesterday_baseline] pykaipan fetch fail: {e}")

                # Path B: Redis limit_up storage fallback
                if not prev_limit_up_set:
                    limit_up_key = f"limit_up_{prev_day}"
                    prev_limit_up_data = self.redis_storage.get_data(limit_up_key)
                    if prev_limit_up_data:
                        for item in prev_limit_up_data:
                            c = item.get('股票代码', '') or item.get('code', '')
                            if c: prev_limit_up_set.add(str(c))

                # Path C: stock_extra fallback
                if not prev_limit_up_set and self.stock_extra:
                    prev_limit_up_set = {c for c, ex in self.stock_extra.items() if ex.get("y_state") == "涨停"}

                # Write back to cache
                if prev_limit_up_set or prev_hot_plates:
                    await self.redis.hset(hist_cache_key, mapping={
                        "prev_limit_up": json.dumps(list(prev_limit_up_set), ensure_ascii=False),
                        "prev_hot_plates": json.dumps(prev_hot_plates, ensure_ascii=False),
                        "prev_day": prev_day,
                        "ts": str(int(time.time() * 1000)),
                    })
                    await self.redis.expire(hist_cache_key, 86400)
                    
        except Exception as e:
            logger.error(f"[_get_yesterday_baseline] Error: {e}")

        return prev_limit_up_set, prev_hot_plates

    async def calculate_open_scenario(
        self, today_str: str, auction_items: Optional[List[Dict[str, Any]]] = None, quote_map: Optional[Dict[str, Dict]] = None, force: bool = False
    ) -> None:
        """9:30-9:40 执行一次：开盘验证全方位预期差。
        Unified Logic: Market Structure & Expectations
        """
        if not force:
            if not self.manual_date:
                now_time = datetime.now().time()
                if not (datetime.strptime("09:30", "%H:%M").time() <= now_time <= datetime.strptime("09:40", "%H:%M").time()):
                    return
            
            if self.opening_verification_done: return

        # STEP 1: Load Baseline
        prev_limit_up_set, prev_hot_plates = await self._get_yesterday_baseline(today_str)

        # ============================================================
        # STEP 2: 加载今日竞价数据（允许重试）
        # ============================================================
        top_amount_list = list(auction_items or [])
        if not top_amount_list:
            top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=True, ttl_sec=2)
        if not top_amount_list:
            # Retry a few times for delayed final snapshot availability.
            # User requirement: 10s start, 5s interval (align with 3s market tick)
            now = datetime.now()
            if not force and now.hour == 9 and now.minute == 25 and now.second < 10:
                logger.debug(f"⏳ [OpenScenario] Waiting for 09:25:10 stabilization (Current: {now.strftime('%H:%M:%S')})")
                return

            for i in range(3):
                await asyncio.sleep(5)
                top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=True, ttl_sec=5)
                if top_amount_list:
                    logger.info(f"✅ [OpenScenario] Auction data found on retry {i+1}")
                    break

        if not top_amount_list:
            self._log_event(
                "open_scenario_no_auction",
                "open_scenario skipped: auction snapshot unavailable",
                min_interval_sec=60,
                log_on_change=False,
            )
            if force:
                logger.info(f"⚠️ Forcing fallback open_scenario due to missing auction data for {today_str}")
                await self.redis.hset(
                    f"market:open_scenario:{today_str}",
                    mapping={
                        "ts": str(int(time.time() * 1000)),
                        "verification_status": "late_bootstrap",
                        "stale": "true"
                    }
                )
                await self.redis.expire(f"market:open_scenario:{today_str}", 86400)
            # Do not mark done here; allow retries on next loop within opening window.
            return

        # 3. Prepare Core List (Expand to Top 100 for better coverage?)
        # User requested optimize architecture based on "Core Stocks"
        # Let's use Top 80 to catch more plate leaders
        core = top_amount_list[:80]
        codes = [it.get('symbol') for it in core if it.get('symbol')]

        # 4. Fetch Quotes (reuse loop context or fetch missing)
        if quote_map is None:
            quote_map = await self._fetch_quotes_batch(codes)
        else:
            # Ensure all needed codes are in quote_map
            missing = [c for c in codes if c not in quote_map]
            if missing:
                extra = await self._fetch_quotes_batch(missing)
                quote_map.update(extra)
        
        quotes = [quote_map.get(code, {}) for code in codes]


        # 5. Run Unified Analysis
        sentiment_key = f"market:sentiment:{today_str}"
        sentiment_data = await self.redis.hgetall(sentiment_key)
        cycl_phase = sentiment_data.get("phase", "unknown") if sentiment_data else "unknown"
        
        result = await self.analyze_expectations(core, quotes, prev_limit_up_set, prev_hot_plates=prev_hot_plates, sentiment_phase=cycl_phase)
        signals = result["signals"]
        stats = result["stats"]
        
        # 6. Categorize & Log
        if prev_hot_plates:
            logger.info(f"⚓ 挂载昨日热块锚定: {', '.join([f'{k}({v}%)' for k,v in prev_hot_plates.items()])}")
        logger.info(f"📋 【预期差分析报告】 日期: {today_str} | 样本: {len(core)}")
        danger_list = [s.to_dict() for s in signals if s.type == 'price_gap_fade']
        surprise_list = [s.to_dict() for s in signals if s.type == 'price_gap_rise']
        weak_strong_list = [s.to_dict() for s in signals if s.type in ('weak_to_strong', 'hot_plate_to_strong')] 
        strong_weak_list = [s.to_dict() for s in signals if s.type in ('strong_to_weak', 'hot_plate_to_weak')] 

        if not signals:
            logger.info("ℹ️ 未检测到显著的竞价预期差或转折信号。")

        if weak_strong_list: 
            logs = [f"{self._get_stock_name(d['code'])}({d['code']})({d['reason']})" for d in weak_strong_list[:3]]
            logger.info(f"💎 弱转强(核心抢筹): {', '.join(logs)}")

        if strong_weak_list: 
            logs = [f"{self._get_stock_name(d['code'])}({d['code']})({d['reason']})" for d in strong_weak_list[:3]]
            logger.info(f"☠️ 强转弱(核心退潮): {', '.join(logs)}")
            
        # Log Plate Strength
        if stats["plate_strength"]:
            # Sort by strength score
            sorted_plates = sorted(stats["plate_strength"].items(), key=lambda x: x[1].get("strength", 0), reverse=True)
            p_logs = [f"{k}({v.get('strength', 0)})" for k, v in sorted_plates[:3]]
            logger.info(f"🔥 竞价超预期板块: {', '.join(p_logs)}")

        # Log Gap Signals (Existing)
        if danger_list:
            logs = [f"{self._get_stock_name(d['code'])}({d['code']})({d['reason']})" for d in danger_list[:3]]
            logger.info(f"⚠️ 竞价不及预期: {', '.join(logs)}")
        if surprise_list:
            logs = [f"{self._get_stock_name(d['code'])}({d['code']})({d['reason']})" for d in surprise_list[:3]]
            logger.info(f"✨ 竞价超预期: {', '.join(logs)}")

        # Update Redis
        async def update_list(key, items):
            if items:
                p = self.redis.pipeline()
                msg_map = {json.dumps(item, ensure_ascii=False): int(time.time()*1000) for item in items}
                p.zadd(key, msg_map)
                p.expire(key, 86400)
                await p.execute()

        await update_list(f"rank:danger:{today_str}", danger_list)
        await update_list(f"rank:surprise:{today_str}", surprise_list)
        await update_list(f"rank:weak_strong:{today_str}", weak_strong_list) # Frontend needs to support this
        await update_list(f"rank:strong_weak:{today_str}", strong_weak_list)

        # Market Structure Conclusion
        structure_msg = "震荡/轮动"
        if stats["yesterday_strong_promotion"] > stats["yesterday_strong_demotion"] * 2:
            structure_msg = "一致性加强 (做多)"
        elif stats["yesterday_strong_demotion"] > stats["yesterday_strong_promotion"] * 1.5:
            structure_msg = "分歧退潮 (防守)"
        elif weak_strong_list and stats["plate_strength"]:
            structure_msg = "新旧切换 (关注新板块)"

        scenario_data: Dict[str, Any] = {
            "ts": int(time.time() * 1000),
            "verification_status": "confirmed" if "做多" in structure_msg else "rejected",
            "confidence": 0.8,
            "reason": f"{structure_msg} - 昨强晋级:{stats['yesterday_strong_promotion']} vs 淘汰:{stats['yesterday_strong_demotion']} | 领涨板块:{list(stats['plate_strength'].keys())[:2]}"
        }

        out_key = f"market:open_scenario:{today_str}"
        await self.redis.hset(out_key, mapping=scenario_data)
        await self.redis.expire(out_key, 86400)

        self.opening_verification_done = True

    def _safe_float(self, v: Any, default: float = 0.0) -> float:
        try:
            if v is None or v == "":
                return default
            return float(v)
        except Exception:
            return default

    def _safe_int(self, v: Any, default: int = 0) -> int:
        try:
            if v is None or v == "":
                return default
            return int(v)
        except Exception:
            return default

    def _normalize_pct(self, v: float, scale: float) -> float:
        return float(v * scale)

    def _get_major_plate(self, code: str, hot_plate_set: Optional[Set[str]] = None) -> str:
        """
        Unified logic to determine the 'Major' plate for a stock.
        Considers weights, hierarchy, and current market hotness.
        """
        if not hasattr(self.plate_updater, "stock_to_plates"):
            return ""
        
        plates = self.plate_updater.stock_to_plates.get(code, [])
        if not plates:
            return ""
            
        hot_plate_set = hot_plate_set or set()
        
        # 1. Base weights from plate_updater (if available)
        weighted = []
        for pid in plates:
            w = 1.0
            if hasattr(self.plate_updater, "get_plate_metrics"):
                metrics = self.plate_updater.get_plate_metrics(pid)
                w = float(metrics.get("weight", 1.0))
            weighted.append((pid, w))
            
        weighted.sort(key=lambda x: x[1], reverse=True)
        if not weighted: return ""
        top_pid, top_w = weighted[0]

        def _plate_name(pid): return self._get_plate_name(pid)
        def _plate_info(pid): return self.plate_updater.all_plates.get(pid, {}) if hasattr(self.plate_updater, "all_plates") else {}
        def _is_event_concept(pid):
            name = _plate_name(pid)
            return any(x in name for x in ["发布会", "事件", "展会", "开幕"])

        # 2. Refined Selection Logic
        best_pid = top_pid
        best_score = -1e9
        for pid, w in weighted:
            name = _plate_name(pid)
            info = _plate_info(pid)
            is_main = info.get("type") == "main"
            concept_penalty = 0.10 if "概念" in name else 0.0
            event_penalty = 0.25 if _is_event_concept(pid) else 0.0
            hot_bonus = 0.20 if pid in hot_plate_set else 0.0
            main_bonus = 0.12 if is_main else 0.0
            
            # Breadth Bonus: Favor broader industry plates over tiny sub-themes
            breadth = len(self.plate_updater.plate_to_stocks.get(pid, []) or [])
            breadth_bonus = min(0.25, np.log1p(max(1, breadth)) / 18.0)
            sub_penalty = 0.10 if info.get("type") == "sub" else 0.0
            
            s = (1.10 * float(w)) + hot_bonus + main_bonus + breadth_bonus - sub_penalty - concept_penalty - event_penalty
            if s > best_score:
                best_score = s
                best_pid = pid
        
        top_pid = best_pid
        # Hierarchy: If sub-plate is chosen but parent is strong, prefer parent
        top_info = _plate_info(top_pid)
        if top_info.get("type") == "sub":
            parent_pid = top_info.get("parent", "")
            if parent_pid:
                weighted_map = dict(weighted)
                parent_w = float(weighted_map.get(parent_pid, 0.0))
                if parent_w >= weighted_map.get(top_pid, 1.0) * 0.6:
                    top_pid = parent_pid

        # Final Priority Adjustments
        top_name = _plate_name(top_pid)
        if "概念" in top_name:
            w_map = dict(weighted)
            for pid, w in weighted[1:]:
                if "概念" not in _plate_name(pid) and w >= w_map.get(top_pid, 1.0) * 0.85:
                    top_pid = pid
                    break

        return top_pid

    def _get_plate_name(self, plate_id: str) -> str:
        """获取板块名称，优先从 plate_updater 获取"""
        if not plate_id:
            return "未知"
        # 1. 尝试从 plate_updater 获取 (内存加载，最准确)
        if hasattr(self.plate_updater, 'all_plates'):
            p_info = self.plate_updater.all_plates.get(plate_id)
            if p_info and p_info.get('name'):
                return p_info['name']
        
        # 2. 尝试从 static_plate_info 获取 (Redis 同步)
        if self.static_plate_info and plate_id in self.static_plate_info:
            return self.static_plate_info[plate_id].get("name", plate_id)
            
        return plate_id

    def _get_stock_name(self, code: str) -> str:
        """获取股票名称，优先从 plate_updater 获取"""
        if not code:
            return "未知"
        # 1. 尝试从 plate_updater 获取 (内存加载)
        if hasattr(self.plate_updater, 'stock_names'):
            name = self.plate_updater.stock_names.get(code)
            if name:
                # 清理“名称(代码)”重复后缀，避免日志中出现 名称(代码)(代码)
                n = str(name).strip()
                c = str(code).strip()
                if c:
                    n = re.sub(rf"\({re.escape(c)}\)$", "", n).strip()
                n = re.sub(r"\(\d{6}\)$", "", n).strip()
                return n
        
        # 2. 尝试从 cached_indicators 中获取或由外部填入，此处暂作降级处理
        return "股票" + str(code)

    def _is_strategic_plate(self, plate_id: str, score: float = 0.0) -> bool:
        """判断是否为具备战略分析价值的板块（剔除区域/技术型板块）
           优化：对于绝对高分的板块（如打分 > 50），即使命中地域词也给予豁免权。
        """
        name = self._get_plate_name(plate_id)
        if not name:
            return False
        
        # 绝对高分豁免逻辑 (Energy-based Exemption)
        if score > 50.0:
            return True

        # 1. 剔除地理区域（省、市、区、县、自治区、特区）
        if name.endswith(("省", "市", "区", "县", "自治区", "特区")):
            return False
        # 2. 剔除极其宽泛的类型或技术指标板块
        noise = ("昨日涨停", "昨日曾涨停", "昨日首板", "百元股", "低价股", "证金持股", "汇金持股", "标准普尔")
        if any(n in name for n in noise):
            return False
        return True

    def _generate_stock_analysis_reason(self, item: StockRankItem) -> str:
        """合成个人化推荐理由：筹码、K线、活跃度、竞价等多维度"""
        reasons = []
        
        # 1. 筹码维度 (Chip)
        chip = self.chip_peaks.get(item.code) or {}
        profit_ratio = self._safe_float(chip.get("profit_ratio"), 0.0)
        concentration = self._safe_float(chip.get("concentration"), 999.0)
        avg_cost = self._safe_float(chip.get("avg_cost"), 1.0)
        
        if profit_ratio >= 0.9:
            reasons.append("筹码处于黄金真空区（获利盘>90%），上方几乎无套牢压力")
        elif profit_ratio >= 0.7:
            reasons.append("筹码结构健康，多数筹码处于盈利状态，抛压较轻")
            
        if avg_cost > 0 and concentration > 0:
            rel_conc = concentration / avg_cost
            if rel_conc < 0.12:
                reasons.append("筹码极度密集，主力高度控盘迹象明显")

        # 2. K线与位置 (K-Line & Position)
        extra = self.stock_extra.get(item.code) or {}
        lb = int(extra.get("limit_up_days_5") or 0)
        pct_5d = self._safe_float(extra.get("change_pct_5d"), 0.0)
        
        if lb >= 2:
            reasons.append(f"处于{lb}连板强势通道，具备极强的股性惯性")
        elif pct_5d < 15 and item.change_pct > 3:
            reasons.append("属于底部超跌后的中阳起航，位置安全且具备反弹动能")

        # 3. 活跃度与共振 (Activity & Resonance)
        if item.resonance_role == "leader":
            reasons.append(f"当前板块【{self._get_plate_name(item.plate_best)}】的绝对领涨龙一，号召力极强")
        elif item.co_move_score > 15:
            reasons.append("板块整体协同走强，具备明显的群体性赚钱效应")
            
        if item.amount_2min > 50_000_000:
            reasons.append("盘中成交活跃，大资金持续流入迹象明显")

        # 4. 竞价强度 (Auction)
        if item.auction_rank <= 50:
            reasons.append(f"早盘竞价位列全场Top50（{item.auction_rank}名），抢筹意愿坚决")

        if not reasons:
            return "综合打分居前，具备较强的技术面共振效应"
        
        return " | ".join(reasons[:3]) # 取前三条最核心理由

    def _predict_market_phase(
        self,
        st_score: float,
        red_green_ratio: float,
        max_lb: int,
        consensus_score: float,
        comfort_score: float,
        effectiveness: float = 0.5,
        fade_count: int = 0,
        one_word_break_rate: float = 0.0,
        seal_ratio_front20: float = 1.0,
        last_phase: str = "UNKNOWN"
    ) -> Tuple[str, float, List[str], List[str], str, float]:
        """
        统一市场情绪阶段判定逻辑 (State Machine Logic).
        返回: (phase, pos_cap, allowed, blocked, transition_reason, confidence)
        """
        # 默认设定 (start / 启动期作为基准)
        phase = "start"
        pos_cap = 0.35
        allowed = ["new_theme_first_board"]
        blocked = ["blind_relay", "late_rotation"]
        transition = "DEFAULT_STABLE"
        confidence = 0.5

        # 1. 杀跌/退潮期 (RETREAT) - 优先级最高
        # 核心特征：红绿比崩溃 (<0.7) 且 赚钱效应或预期差极差
        if red_green_ratio < 0.7 or (effectiveness < 0.25 and fade_count > 20):
            phase = "retreat"
            pos_cap = 0.15
            allowed = []
            blocked = ["blind_relay", "high_board_chase", "follower_chase", "late_rotation"]
            transition = "PANIC_RETREAT"
            confidence = 0.85
            if red_green_ratio < 0.4: transition = "BROAD_CRASH"

        # 2. 冰点期 (ICE_POINT)
        # 核心特征：红绿比极低 (<0.6) 且 空间板高度被持续压制 (<4板)
        elif red_green_ratio < 0.6 and max_lb < 4:
            phase = "ice_point"
            pos_cap = 0.2
            allowed = ["ice_rebound_core"]
            blocked = ["standard_relay", "high_board_chase"]
            transition = "ICE_POINT_LIMIT"
            confidence = 0.9

        # 3. 主升高潮/分歧修复 (MAIN_RISE)
        # 核心特征：有板块强共振 (consensus > 30) 或 核心封力强，且红绿比健康
        elif (consensus_score > 35 or seal_ratio_front20 > 1.25) and max_lb >= 5 and red_green_ratio > 0.85:
            phase = "climax"
            pos_cap = 0.6
            allowed = ["low_level_relay", "high_board_chase", "new_theme_first_board"]
            blocked = ["late_rotation", "blind_relay"]
            transition = "ACCEL_MAIN_RISE"
            confidence = 0.8
            if one_word_break_rate > 0.3: transition = "ACCEL_WITH_FRICTION"

        # 4. 高位分歧 (DIVERGE)
        # 核心特征：空间板依然在但红绿比走弱，或炸板/大面激增 (fade > 15)
        elif max_lb > 4 and (red_green_ratio < 1.0 or fade_count > 15 or one_word_break_rate > 0.4):
            phase = "divergence"
            pos_cap = 0.4
            allowed = ["core_dip_buying"]
            blocked = ["follower_chase", "late_rotation", "blind_relay"]
            transition = "HIGH_LEVEL_DIVERGE"
            confidence = 0.7
            if fade_count > 25: transition = "DIVERGE_TO_RETREAT"

        # 5. 启动点/修复期 (IGNITION)
        # 核心特征：红绿比大幅回升 (>= 1.25) 且 尚未进入加速状态
        elif red_green_ratio >= 1.25:
            phase = "start"
            pos_cap = 0.45
            allowed = ["new_theme_first_board", "low_level_relay"]
            blocked = ["late_rotation", "blind_relay"]
            transition = "REPAIR_IGNITION"
            confidence = 0.75

        # 状态机惯性修正: 如果新旧状态差异极小，维持现状 (避免抖动)
        if last_phase == phase:
            confidence = min(0.95, confidence + 0.05)
        
        return phase, pos_cap, allowed, blocked, transition, confidence

    def _infer_change_pct_scale(
        self,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
        auction_change: Dict[str, float],
    ) -> float:
        """Infer whether change_pct is ratio (0.2) or percent (20.0)."""
        ratios: List[float] = []
        for code in candidate_pool:
            if code not in auction_change:
                continue
            ind = indicators.get(code) or {}
            c = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            a = self._safe_float(auction_change.get(code, 0.0), 0.0)
            if abs(c) < 1e-6 or abs(a) < 1e-6:
                continue
            ratios.append(abs(a) / abs(c))
        if not ratios:
            return 1.0
        med = float(np.median(ratios))
        if 50.0 <= med <= 150.0:
            return 100.0
        return 1.0

    def _append_change_history(self, code: str, ts_ms: int, change_pct: float) -> None:
        arr = self.code_change_history.setdefault(code, [])
        arr.append((ts_ms, float(change_pct)))
        cutoff = ts_ms - 45 * 60 * 1000
        while arr and arr[0][0] < cutoff:
            arr.pop(0)

    def _window_delta(self, code: str, now_ts_ms: int, window_sec: int) -> float:
        arr = self.code_change_history.get(code) or []
        if len(arr) < 2:
            return 0.0
        start_ts = now_ts_ms - window_sec * 1000
        first_v = None
        last_v = arr[-1][1]
        for ts, v in arr:
            if ts >= start_ts:
                first_v = v
                break
        if first_v is None:
            first_v = arr[0][1]
        return float(last_v - first_v)

    def _sign(self, v: float, th: float) -> int:
        if v >= th:
            return 1
        if v <= -th:
            return -1
        return 0

    def _activation_ts_with_sign(
        self,
        code: str,
        now_ts_ms: int,
        window_sec: int,
        sign: int,
        threshold: float,
    ) -> int:
        if sign == 0:
            return 0
        arr = self.code_change_history.get(code) or []
        if len(arr) < 2:
            return 0
        start_ts = now_ts_ms - window_sec * 1000
        # locate baseline at (or nearest after) window start
        idx = 0
        while idx < len(arr) and arr[idx][0] < start_ts:
            idx += 1
        if idx >= len(arr):
            return 0
        base_v = arr[idx][1]
        for j in range(idx, len(arr)):
            ts, v = arr[j]
            delta = v - base_v
            if sign > 0 and delta >= threshold:
                return ts
            if sign < 0 and delta <= -threshold:
                return ts
        return 0

    def _clamp01(self, v: float) -> float:
        return float(max(0.0, min(1.0, v)))

    def _update_return_history(self, indicators: Dict[str, Dict]) -> None:
        if not indicators:
            return
        for code, ind in indicators.items():
            chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            arr = self.return_history.setdefault(code, [])
            arr.append(chg)
            if len(arr) > 30:
                del arr[:-30]

    def _log_event(
        self,
        key: str,
        msg: str,
        *,
        level: str = "info",
        min_interval_sec: int = 180,
        log_on_change: bool = True,
    ) -> None:
        now = time.time()
        last_msg = self.log_last_payload.get(key, "")
        last_ts = self.log_last_ts.get(key, 0.0)
        changed = (msg != last_msg)
        due = (now - last_ts) >= min_interval_sec
        if (log_on_change and changed) or due:
            if level == "warning":
                logger.warning(msg)
            elif level == "error":
                logger.error(msg)
            else:
                logger.info(msg)
            self.log_last_payload[key] = msg
            self.log_last_ts[key] = now

    def _score_rank(self, rank: int, *, max_rank: int, max_score: float) -> float:
        if rank < 0:
            rank = max_rank
        if rank >= max_rank:
            return 0.0
        # rank=0 -> max_score, rank=max_rank -> 0
        return float(max_score * (1.0 - (rank / max_rank)))

    async def _get_kaipan_plate_by_id_cached(self) -> Dict[str, Dict[str, Any]]:
        """获取开盘啦板块榜单（短缓存，避免频繁请求外部接口）。"""
        if not self.enable_kaipan_plate_blend or fetch_kaipan_plate_rank is None:
            return {}

        now = time.time()
        if now - float(self._kaipan_plate_cache.get("ts", 0.0)) < self.kaipan_plate_cache_ttl_sec:
            return self._kaipan_plate_cache.get("by_id", {}) or {}

        try:
            loop = asyncio.get_event_loop()
            payload = await loop.run_in_executor(None, fetch_kaipan_plate_rank, "0", "80")
            plates = payload.get("plates", []) if isinstance(payload, dict) else []
            by_id = {str(p.get("id", "")).strip(): p for p in plates if str(p.get("id", "")).strip()}
            self._kaipan_plate_cache = {"ts": now, "by_id": by_id, "count": len(by_id)}
            self._log_event("kaipan_plate_cache", f"🛰️ 开盘啦板块快照: {len(by_id)}", min_interval_sec=600)
            return by_id
        except Exception as e:
            self._log_event("kaipan_plate_cache_err", f"⚠️ 开盘啦板块抓取失败: {e}", level="warning", min_interval_sec=600)
            return self._kaipan_plate_cache.get("by_id", {}) or {}

    def _kaipan_plate_bonus(self, kp: Optional[Dict[str, Any]], kp_total: int) -> float:
        """将开盘啦板块信息映射为内部加分项。"""
        if not kp or kp_total <= 0:
            return 0.0
        rank = int(self._safe_float(kp.get("rank", kp_total), kp_total))
        rank = max(1, min(kp_total, rank))
        rank_pct = 1.0 - (rank - 1) / max(1, kp_total - 1)
        strength = self._safe_float(kp.get("strength", 0.0), 0.0)
        change_pct = self._safe_float(kp.get("change_pct", 0.0), 0.0)
        amount = self._safe_float(kp.get("amount", 0.0), 0.0)
        main_net = self._safe_float(kp.get("main_net", 0.0), 0.0)
        big_order_net = self._safe_float(kp.get("big_order_net", 0.0), 0.0)

        rank_term = 10.0 * rank_pct
        strength_term = min(6.0, np.log1p(max(0.0, strength) / 1000.0) * 2.5)
        amount_term = min(5.0, np.log1p(max(0.0, amount) / 100_000_000.0) * 2.0)
        flow_term = min(4.0, np.log1p(max(0.0, main_net + big_order_net) / 100_000_000.0) * 2.0)
        change_term = max(-3.0, min(3.0, change_pct / 2.0))
        return rank_term + strength_term + amount_term + flow_term + change_term

    def _dde_score(self, dde: Optional[Dict[str, Any]]) -> float:
        if not dde or not isinstance(dde, dict):
            return 0.0
        ddx = dde.get('ddx', None)
        if ddx is None:
            ddx = dde.get('DDX', None)
        ddx_v = self._safe_float(ddx, 0.0)
        # 简化：ddx>0 加分，ddx<0 不加分，封顶 20
        return float(max(0.0, min(20.0, ddx_v * 10.0)))

    async def calculate_stock_rank(
        self, 
        today_str: str, 
        candidate_pool: Set[str], 
        indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None
    ) -> None:
        """
        计算个股共振榜
        优化：接受 indicators 和 quote_map 参数
        """
        if not candidate_pool or not indicators:
            return

        if not quote_map:
            quote_map = await self._fetch_quotes_batch(list(candidate_pool))

        # 1) 竞价 Top_amount -> rank/封单/竞价涨幅
        top_amount_list = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
        auction_rank: Dict[str, int] = {}
        auction_bid: Dict[str, float] = {}
        auction_change: Dict[str, float] = {}
        for idx, it in enumerate(top_amount_list):
            code = it.get("symbol")
            if not code or len(code) != 6:
                continue
            auction_rank[code] = idx
            auction_bid[code] = self._safe_float(it.get("bid_amount_yuan", 0), 0.0)
            auction_change[code] = self._safe_float(it.get("change_pct", 0), 0.0)

        pct_scale = self._infer_change_pct_scale(candidate_pool, indicators, auction_change)

        # 2) 题材证据/题材榜
        evidence_key = f"cache:stock_theme_evidence:{today_str}"
        evidence_raw = await self.redis.hgetall(evidence_key)
        theme_rank_key = f"rank:theme:{today_str}"

        # 取题材榜 topN 形成 theme->rank/score 的映射（rank 越小越强）
        top_themes = await self.redis.zrevrange(theme_rank_key, 0, 49, withscores=True)
        theme_score_map: Dict[str, float] = {t: float(s) for t, s in top_themes}
        theme_rank_map: Dict[str, int] = {t: i for i, (t, _) in enumerate(top_themes)}

        # 3) 板块榜：这里优先用 plate_spread（更贴近盘中扩散），没有的话退化用 plate
        plate_rank_key = f"rank:plate_spread:{today_str}"
        top_plates = await self.redis.zrevrange(plate_rank_key, 0, 199, withscores=True)
        if not top_plates:
            top_plates = await self.redis.zrevrange(f"rank:plate:{today_str}", 0, 199, withscores=True)
        plate_score_map: Dict[str, float] = {pid: float(s) for pid, s in top_plates}
        plate_rank_map: Dict[str, int] = {pid: i for i, (pid, _) in enumerate(top_plates)}

        # 4) DDE：可选（如果没有 stock_analyzer 就全 0）
        dde_map: Dict[str, Dict[str, Any]] = {}
        if self.stock_analyzer is not None:
            # 取最近一个交易日作为 dde date（用 today_str-1 天兜底）
            # 注意：回放模式时，today_str 已经是回放日期，dde 应该是前一天数据
            try:
                dde_date = self.calendar.get_previous_trade_day(today_str).replace('-', '')
            except:
                dde_date = (datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")
                
            # 扩充范围到 200 只，或者全量 candidate_pool
            for code in list(candidate_pool)[:200]:
                try:
                    dde = self.stock_analyzer.get_his_stock_dde(code, dde_date) or self.stock_analyzer.get_stock_dde(code)
                    if dde:
                        dde_map[code] = dde
                except Exception:
                    continue

        # Precompute direction by plate for co-move score
        code_change: Dict[str, float] = {}
        code_amount2: Dict[str, float] = {}
        plate_to_codes: Dict[str, List[str]] = {}
        for code in candidate_pool:
            ind = indicators.get(code) or {}
            q = quote_map.get(code) or {}
            chg = self._safe_float(q.get("change_pct", q.get("change", None)), None)
            if chg is None:
                chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
                chg = self._normalize_pct(chg, pct_scale) # 使用自动推测的 scale
                # 或者直接强制用主逻辑的转换
                if abs(chg) <= 1.0: chg *= 100.0
            else:
                chg = self._normalize_change_pct_auto(
                    chg,
                    price=q.get("price"),
                    pre_close=q.get("pre_close", q.get("last_close")),
                )
            amt2 = self._safe_float(ind.get("amount_2min", 0.0), 0.0)
            # 防御：原有逻辑当 amt2<=0 时 fallback 到今日总成交额 q.get("amount")，这会导致夜间/开盘前评分被极大值扭曲。
            # 这里允许 amt2 为 0，具体的防御在后续分数公式中处理。
            code_change[code] = chg
            code_amount2[code] = amt2
            for pid in (self.plate_updater.stock_to_plates.get(code, []) or []):
                plate_to_codes.setdefault(pid, []).append(code)

        # 4) Momentum Decay (Anti-Yesterday Bias)
        # Calculate intraday performance vs open price to penalize "Fade" stocks
        momentum_multiplier: Dict[str, float] = {}
        for code in candidate_pool:
            q = quote_map.get(code) or {}
            open_p = self._safe_float(q.get("open"), 0.0)
            now_p = self._safe_float(q.get("price"), 0.0)
            mult = 1.0
            if open_p > 0 and now_p > 0:
                change_from_open = (now_p - open_p) / open_p
                # If dropped more than 3% from open, penalize auction/yesterday scores
                if change_from_open < -0.03:
                    mult = 0.5
                elif change_from_open < -0.01:
                    mult = 0.8
            momentum_multiplier[code] = mult

        # 扩展：为候选池所在板块的同板块个股也收集涨跌数据和历史
        # 这样 F5/F30 跟随率才有足够的 peer 数据用于计算
        candidate_plates = set(plate_to_codes.keys())
        peer_codes_extended: Set[str] = set()
        for pid in candidate_plates:
            plate_stocks = self.plate_updater.plate_to_stocks.get(pid, []) or []
            for pcode in plate_stocks:
                if pcode not in code_change and pcode not in peer_codes_extended:
                    peer_codes_extended.add(pcode)
        
        # 批量获取 peer 行情（限制数量避免过载）
        if peer_codes_extended:
            peer_list = list(peer_codes_extended)[:500]
            try:
                pipe = self.redis.pipeline()
                for pc in peer_list:
                    pipe.hgetall(f"stock:quote:{pc}")
                peer_quotes = await pipe.execute()
                for i, pc in enumerate(peer_list):
                    pq = peer_quotes[i] or {}
                    if not pq:
                        continue
                    pchg = self._safe_float(pq.get("change_pct", pq.get("change", 0)), 0.0)
                    pchg = self._normalize_change_pct_auto(
                        pchg,
                        price=pq.get("price"),
                        pre_close=pq.get("pre_close", pq.get("last_close")),
                    )
                    code_change[pc] = pchg
                    # 将 peer 也加入 plate_to_codes
                    for pid in (self.plate_updater.stock_to_plates.get(pc, []) or []):
                        if pid in candidate_plates:
                            plate_to_codes.setdefault(pid, []).append(pc)
            except Exception:
                pass

        items: List[StockRankItem] = []
        ts = int(time.time() * 1000)

        # update intraday change history for follow/co-move detection
        # 包含候选池和扩展的 peer，使 F5/F30 有足够历史样本
        for code in candidate_pool:
            self._append_change_history(code, ts, code_change.get(code, 0.0))
        for code in peer_codes_extended:
            if code in code_change:
                self._append_change_history(code, ts, code_change[code])


        for code in candidate_pool:
            amount_2min = code_amount2.get(code, 0.0)
            change_pct = code_change.get(code, 0.0)

            # 题材证据
            ev = evidence_raw.get(code)
            primary_theme = ""
            top_w = 0.0
            conflict = False
            if ev:
                try:
                    evj = json.loads(ev)
                    primary_theme = str(evj.get('primary_theme', '') or '')
                    themes_top3 = evj.get('themes_top3', []) or []
                    if themes_top3 and isinstance(themes_top3, list):
                        top_w = self._safe_float(themes_top3[0].get('w', 0), 0.0)
                    conflict = bool(evj.get('theme_conflict', False))
                except Exception:
                    pass

            # 题材得分：题材榜分数 * top_w（题材越确定贡献越大）
            theme_raw_score = theme_score_map.get(primary_theme, 0.0)
            theme_score = float(theme_raw_score * max(0.2, min(1.0, top_w)))

            # 应用动量权重 (Momentum Re-weighting)
            m_mult = momentum_multiplier.get(code, 1.0)
            
            # 5) Auction/Bid Score with Decay
            bid_score = (auction_bid.get(code, 0.0) / 1000000.0) * m_mult
            
            # 6) Strategy Resonance Calculation (Refined)
            # Final Score combine Theme + Bid + Plate + DDE
            # ... (Existing logic will follow, applying mult to bid and raw components)

            # 板块归属 & 取最强板块
            plate_ids = self.plate_updater.stock_to_plates.get(code, []) or []
            plate_best = ""
            plate_best_score = 0.0
            for pid in plate_ids:
                s = plate_score_map.get(pid, 0.0)
                if s > plate_best_score:
                    plate_best_score = s
                    plate_best = pid

            # Co-move score: same plate peers moving in same direction
            co_move_score = 0.0
            follow_5m_ratio = 0.0
            follow_30m_ratio = 0.0
            co_move_active_peers = 0
            resonance_role = "neutral"
            lead_follow_count = 0
            lead_follow_ratio = 0.0
            if plate_best and plate_best in plate_to_codes:
                peers = plate_to_codes.get(plate_best, [])
                if len(peers) >= 3:
                    d5 = self._window_delta(code, ts, 5 * 60)
                    d30 = self._window_delta(code, ts, 30 * 60)
                    sign5 = self._sign(d5, 0.4)
                    sign30 = self._sign(d30, 1.0)
                    act5_ts = self._activation_ts_with_sign(code, ts, 5 * 60, sign5, 0.4)
                    act30_ts = self._activation_ts_with_sign(code, ts, 30 * 60, sign30, 1.0)

                    active5 = 0
                    follow5 = 0
                    active30 = 0
                    follow30 = 0
                    for pcode in peers:
                        if pcode == code:
                            continue
                        pd5 = self._window_delta(pcode, ts, 5 * 60)
                        pd30 = self._window_delta(pcode, ts, 30 * 60)
                        ps5 = self._sign(pd5, 0.4)
                        ps30 = self._sign(pd30, 1.0)
                        pact5_ts = self._activation_ts_with_sign(pcode, ts, 5 * 60, sign5, 0.4) if sign5 != 0 else 0
                        pact30_ts = self._activation_ts_with_sign(pcode, ts, 30 * 60, sign30, 1.0) if sign30 != 0 else 0
                        if ps5 != 0:
                            active5 += 1
                            if sign5 != 0 and ps5 == sign5:
                                follow5 += 1
                                if act5_ts > 0 and pact5_ts > act5_ts:
                                    lead_follow_count += 1
                        if ps30 != 0:
                            active30 += 1
                            if sign30 != 0 and ps30 == sign30:
                                follow30 += 1
                                if act30_ts > 0 and pact30_ts > act30_ts:
                                    lead_follow_count += 1

                    if active5 > 0:
                        follow_5m_ratio = follow5 / active5
                    if active30 > 0:
                        follow_30m_ratio = follow30 / active30
                    co_move_active_peers = active5 + active30
                    breadth = min(1.0, (active5 + active30) / max(1, (len(peers) - 1) * 2))
                    co_move_score = min(
                        20.0,
                        max(0.0, follow_5m_ratio * 10.0 + follow_30m_ratio * 8.0 + breadth * 2.0),
                    )
                    if co_move_active_peers > 0:
                        lead_follow_ratio = lead_follow_count / co_move_active_peers
                    if lead_follow_count >= 3 and lead_follow_ratio >= 0.25:
                        resonance_role = "leader"
                    elif (follow_5m_ratio >= 0.5 or follow_30m_ratio >= 0.5):
                        resonance_role = "follower"

            # 板块得分：用排名转分（避免不同榜单 score 量纲不一致）
            best_rank = plate_rank_map.get(plate_best, 9999)
            plate_score = self._score_rank(best_rank, max_rank=200, max_score=20.0)

            # 竞价得分：排名 + 封单 (引入动量衰减)
            a_rank = auction_rank.get(code, 9999)
            auction_rank_score = self._score_rank(a_rank, max_rank=500, max_score=20.0) * m_mult
            bid_amt = auction_bid.get(code, 0.0)
            # Fallback: if auction bid info is missing but we have 2-min volume, use that as a proxy for 'active commitment'
            # to prevent A2:0w in logs and rank distortions
            if bid_amt <= 0.1 and amount_2min > 0:
                bid_amt = amount_2min * 0.1 # Conservative estimate of 'bid' context
                
            bid_score = min(20.0, (bid_amt / 100_000_000.0) * 5.0) * m_mult
            a_change = auction_change.get(code, 0.0)

            # 分时强度：涨幅 + 2分钟量
            mom_score = max(0.0, min(20.0, change_pct * 2.0))
            amt_score = max(0.0, min(20.0, np.log1p(amount_2min / 1_000_000.0)))

            # DDE 得分
            dde_score = self._dde_score(dde_map.get(code))

            # 综合：竞价(0.3)+题材(0.3)+板块(0.2)+资金(0.2)，并加少量分时增强
            score = (
                (auction_rank_score + bid_score) * 0.15
                + theme_score * 0.3
                + plate_score * 0.2
                + dde_score * 0.2
                + (mom_score + amt_score) * 0.075
                + co_move_score * 0.075
            )

            # 冲突惩罚：题材不清晰降权
            if conflict:
                score *= 0.9

            items.append(StockRankItem(
                code=code,
                score=round(float(score), 4),
                primary_theme=primary_theme,
                top_theme_weight=round(float(top_w), 4),
                top_theme_conflict=conflict,
                auction_rank=int(a_rank),
                auction_bid_amount_yuan=float(bid_amt),
                auction_change_pct=float(a_change),
                amount_2min=float(amount_2min),
                change_pct=float(change_pct),
                plate_ids=list(plate_ids),
                plate_best=plate_best,
                plate_best_score=float(plate_best_score),
                theme_score=float(theme_score),
                plate_score=float(plate_score),
                dde_score=float(dde_score),
                co_move_score=float(co_move_score),
                follow_5m_ratio=float(follow_5m_ratio),
                follow_30m_ratio=float(follow_30m_ratio),
                co_move_active_peers=int(co_move_active_peers),
                resonance_role=resonance_role,
                lead_follow_count=int(lead_follow_count),
                lead_follow_ratio=float(lead_follow_ratio),
                ts=ts,
            ))

        # 排序 & 输出 topN，并为前N名生成详细理由
        items.sort(key=lambda x: x.score, reverse=True)
        for it in items[:50]:
            it.analysis_reason = self._generate_stock_analysis_reason(it)

        zkey = f"rank:stock:{today_str}"
        dkey = f"rank:stock:details:{today_str}"

        pipe = self.redis.pipeline()
        pipe.delete(zkey)
        pipe.delete(dkey)

        for it in items[:500]:
            pipe.zadd(zkey, {it.code: it.score})
            pipe.hset(dkey, it.code, json.dumps(it.__dict__, ensure_ascii=False))

        pipe.expire(zkey, 86400)
        pipe.expire(dkey, 86400)
        await pipe.execute()
        
        if items:
            # S=综合分, C=当前涨跌幅(%), A2=2分钟额(万元), F=同向跟随分
            top_stocks_log = [
                f"{self._get_stock_name(it.code)}({it.code})(S:{it.score:.1f},C:{it.change_pct:.1f}%,A2:{it.amount_2min/10000:.0f}w,F:{it.co_move_score:.1f},F5:{it.follow_5m_ratio:.0%},F30:{it.follow_30m_ratio:.0%},R:{it.resonance_role},L:{it.lead_follow_count})"
                for it in items[:3]
            ]
            self._log_event("stock_top3", f"🐂 个股共振 Top3: {', '.join(top_stocks_log)}", min_interval_sec=300)

    async def calculate_market_overview(
        self,
        today_str: str,
        analysis_universe: Set[str],
        indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None,
    ) -> None:
        if not analysis_universe or not indicators:
            return

        codes = list(analysis_universe)
        if not quote_map:
            quote_map = await self._fetch_quotes_batch(codes)
        valid_codes = [c for c in codes if (quote_map.get(c) or {})]
        changes: List[float] = []
        amounts: List[float] = []
        for code in valid_codes:
            q = quote_map.get(code) or {}
            ind = indicators.get(code) or {}
            chg = self._safe_float(q.get("change_pct", q.get("change", None)), None)
            if chg is None:
                chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            else:
                chg = self._normalize_change_pct_auto(
                    chg,
                    price=q.get("price"),
                    pre_close=q.get("pre_close", q.get("last_close")),
                )
            amt = self._safe_float(q.get("amount", None), None)
            if amt is None:
                amt = self._safe_float(ind.get("amount", 0.0), 0.0)
            changes.append(chg)
            amounts.append(amt)

        if not valid_codes:
            return

        up_count = sum(1 for v in changes if v > 0)
        down_count = sum(1 for v in changes if v < 0)
        flat_count = len(changes) - up_count - down_count
        limit_up_like = sum(1 for v in changes if v >= 9.8)
        limit_down_like = sum(1 for v in changes if v <= -9.5)
        avg_change = float(np.mean(changes)) if changes else 0.0
        median_change = float(np.median(changes)) if changes else 0.0
        total_amount = sum(a for a in amounts if a > 0)
        top10_amount = sum(sorted(amounts, reverse=True)[: max(1, int(len(amounts) * 0.1))])
        concentration = self._clamp01(top10_amount / max(1.0, total_amount))
        coverage_ratio = len(valid_codes) / max(1, len(codes))

        out_key = f"market:overview:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "universe_size": len(codes),
            "valid_quotes": len(valid_codes),
            "coverage_ratio": round(coverage_ratio, 4),
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "limit_up_like": limit_up_like,
            "limit_down_like": limit_down_like,
            "avg_change_pct": round(avg_change, 4),
            "median_change_pct": round(median_change, 4),
            "total_amount": round(total_amount, 2),
            "top10_amount_concentration": round(concentration, 4),
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)

        if coverage_ratio < 0.4:
            self._log_event(
                "coverage_warn",
                f"⚠️ 行情覆盖率偏低: {len(valid_codes)}/{len(codes)} ({coverage_ratio:.0%})",
                level="warning",
                min_interval_sec=300,
                log_on_change=True,
            )
        else:
            self._log_event(
                "market_overview",
                f"🌡️ 大盘概览: 覆盖{coverage_ratio:.0%} 涨/跌/平={up_count}/{down_count}/{flat_count} 均涨{avg_change:.2f}%",
                min_interval_sec=300,
            )

    async def update_stock_day_profiles(
        self,
        today_str: str,
        analysis_universe: Set[str],
        indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None,
    ) -> None:
        if not analysis_universe or not indicators:
            return
        auction_profile = await self._get_auction_profile(today_str)
        if not quote_map:
            quote_map = await self._fetch_quotes_batch(list(analysis_universe))
        now_ts = int(time.time() * 1000)
        hkey = f"profile:stock:day:{today_str}"
        trans_key = f"rank:profile_transition:{today_str}"

        p = self.redis.pipeline()
        transition_count: Dict[str, int] = {}
        for code in analysis_universe:
            q = quote_map.get(code) or {}
            ind = indicators.get(code) or {}
            
            pre_close = self._safe_float(q.get("pre_close", q.get("last_close", 0.0)), 0.0)
            price = self._safe_float(q.get("price", q.get("last_price", 0.0)), 0.0)
            
            # 1. 自动适配：根据 raw price 优先计算真实涨跌幅
            if price > 0 and pre_close > 0:
                curr = (price - pre_close) / pre_close * 100.0
            else:
                curr_raw = self._safe_float(q.get("change_pct", q.get("change", 0.0)), 0.0)
                # Redis 传过来的很可能是 ratio(0.01) 代替百分比(1.0)，适配补丁：
                curr = curr_raw * 100.0 if 0 < abs(curr_raw) < 0.25 else curr_raw
                
            high_pct = self._safe_pct_from_quote(q, "high_pct", pre_close, curr)
            low_pct = self._safe_pct_from_quote(q, "low_pct", pre_close, curr)
            
            # 2. 也是针对竞价的同一问题安全计算
            auc_dict = auction_profile.get(code) or {}
            auc_price = self._safe_float(auc_dict.get("price", 0.0), 0.0)
            auc_raw = self._safe_float(auc_dict.get("change_pct", 0.0), 0.0)
            if auc_price > 0 and pre_close > 0:
                auction = (auc_price - pre_close) / pre_close * 100.0
            else:
                auction = auc_raw * 100.0 if 0 < abs(auc_raw) < 0.25 else auc_raw
            amount = self._safe_float(ind.get("amount", q.get("amount", 0.0)), 0.0)
            amount_2min = self._safe_float(ind.get("amount_2min", 0.0), 0.0)

            # Derive open_pct via safe fallback (some quotes might lack it or we compute from price)
            open_px = self._safe_float(q.get("open_price", q.get("open", 0.0)), 0.0)
            if open_px > 0 and pre_close > 0:
                open_pct = (open_px - pre_close) / pre_close * 100.0
            else:
                open_pct = auction # Fallback to auction if open quote is missing
                
            prev = self.stock_state_cache.get(code) or {}
            rebound = curr - low_pct
            drawdown = high_pct - curr
            turning_point = "none"
            
            # 严格的全天运行结构画像 (Full-Day Structural Profiling)
            # 1. 过滤掉振幅极小（如白马股平稳波动）造成的假象
            amplitude = high_pct - low_pct
            is_active = amplitude >= 4.0 or amount > 50000000 # 振幅>4%或成交额>5000万才具备形态研判价值
            
            # 读取情感/散户分布相关核心指标 (用于核心股打分机制)
            volume = self._safe_float(q.get("volume", 0.0), 0.0)
            large_net = self._safe_float(q.get("large_net", 0.0), 0.0)

            if is_active:
                # [单边上涨 (Single-sided Up)]
                # 特征: 开盘后几乎没怎么跌，一路拉升，当前价还在高位。
                if low_pct >= open_pct - 1.5 and curr >= high_pct - 2.0 and curr > open_pct + 3.0:
                    turning_point = "single_sided_up"
                    
                # [单边下跌 (Single-sided Down)]
                # 特征: 开盘后几乎没怎么涨，一路下杀，当前价还在低位。
                elif high_pct <= open_pct + 1.5 and curr <= low_pct + 2.0 and curr < open_pct - 3.0:
                    turning_point = "single_sided_down"
                    
                # [V型反核 (Deep V)]
                # 特征: 盘中出现过极度深水区（至少跌破-4%），且目前具备极强的修复力（从底部拉起超过4%），且收盘在相对高位。
                elif low_pct <= -4.0 and rebound >= 4.0 and curr > (high_pct + low_pct) / 2.0:
                    turning_point = "deep_v"
                    
                # [A字爆头/冲高回落 (Headshot)]
                # 特征: 盘中冲高（至少冲破4%），但随后遭到严重砸盘（从高点回落超过4%），且收盘在相对低位。
                elif high_pct >= 4.0 and drawdown >= 4.0 and curr < (high_pct + low_pct) / 2.0:
                    turning_point = "headshot"
                    
                # [弱转强 (Weak to Strong)]
                # 特征: 竞价或开盘极弱(水下)，但随后强力拉升且稳定在高位。
                elif (auction <= -1.0 or open_pct <= -1.0) and curr >= 2.0 and curr >= high_pct - 2.0 and turning_point == "none":
                    turning_point = "weak_to_strong"
                    
                # [强转弱 (Strong to Weak)]
                # 特征: 竞价或开盘极强(高开)，但随后一路走低且稳定在低位。
                elif (auction >= 2.0 or open_pct >= 2.0) and curr <= -1.0 and curr <= low_pct + 2.0 and turning_point == "none":
                    turning_point = "strong_to_weak"

            # transition dedup per stock/type
            if turning_point != "none":
                dedup_key = f"{today_str}:{code}:{turning_point}"
                last = self.profile_transition_seen.get(dedup_key, 0)
                if now_ts - last >= 180_000:
                    self.profile_transition_seen[dedup_key] = now_ts
                    transition = {
                        "ts": now_ts,
                        "code": code,
                        "type": turning_point,
                        "auction_change_pct": round(auction, 3),
                        "change_pct": round(curr, 3),
                        "high_pct": round(high_pct, 3),
                        "low_pct": round(low_pct, 3),
                    }
                    p.zadd(trans_key, {json.dumps(transition, ensure_ascii=False): now_ts})
                    transition_count[turning_point] = transition_count.get(turning_point, 0) + 1

            profile = {
                "ts": now_ts,
                "code": code,
                "auction_change_pct": round(auction, 3),
                "change_pct": round(curr, 3),
                "high_pct": round(high_pct, 3),
                "low_pct": round(low_pct, 3),
                "open_pct": round(open_pct, 3),
                "rebound_from_low": round(rebound, 3),
                "drawdown_from_high": round(drawdown, 3),
                "amplitude": round(amplitude, 3),
                "amount": round(amount, 2),
                "amount_2min": round(amount_2min, 2),
                "volume": int(volume),
                "large_net": round(large_net, 2),
                "price": round(price, 3),
                "pre_close": round(pre_close, 3),
                "turning_point": turning_point,
                "changed": int(abs(curr - self._safe_float(prev.get("change_pct", 0.0), 0.0)) > 0.2),
            }
            self.stock_state_cache[code] = profile
            p.hset(hkey, code, json.dumps(profile, ensure_ascii=False))

        p.expire(hkey, 86400)
        p.expire(trans_key, 86400)
        await p.execute()

        if transition_count:
            summary = ",".join(f"{k}:{v}" for k, v in sorted(transition_count.items(), key=lambda x: x[1], reverse=True))
            self._log_event("profile_transition", f"🔁 画像转折: {summary}", min_interval_sec=180, log_on_change=True)

    async def calculate_plate_stock_snapshot(
        self,
        today_str: str,
        analysis_universe: Set[str],
        indicators: Dict[str, Dict],
        quote_map: Optional[Dict[str, Dict]] = None,
    ) -> None:
        if not analysis_universe or not indicators:
            return

        # Reuse existing plate metrics as base truth for plate change.
        plate_metric_map: Dict[str, Dict[str, Any]] = {}

        if not quote_map:
            quote_map = await self._fetch_quotes_batch(list(analysis_universe))
        plate_agg: Dict[str, Dict[str, Any]] = {}
        for code in analysis_universe:
            q = quote_map.get(code) or {}
            ind = indicators.get(code) or {}
            chg = self._safe_float(q.get("change_pct", q.get("change", None)), None)
            if chg is None:
                chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            amt2 = self._safe_float(ind.get("amount_2min", None), None)
            if amt2 is None:
                amt2 = self._safe_float(q.get("amount", 0.0), 0.0)
            if abs(chg) < 1e-9 and amt2 <= 0:
                continue
            for pid, w in self._weighted_plates_for_code(code):
                rec = plate_agg.setdefault(pid, {"sum_w": 0.0, "sum_chg": 0.0, "sum_amt2": 0.0, "stocks": []})
                rec["sum_w"] += w
                rec["sum_chg"] += chg * w
                rec["sum_amt2"] += amt2 * w
                if len(rec["stocks"]) < 6:
                    rec["stocks"].append({"code": code, "change_pct": round(chg, 3), "amount_2min": round(amt2, 2)})

        zkey = f"rank:plate_snapshot:{today_str}"
        dkey = f"rank:plate_snapshot:details:{today_str}"
        p = self.redis.pipeline()
        p.delete(zkey)
        p.delete(dkey)
        for pid, rec in plate_agg.items():
            denom = max(1e-6, rec["sum_w"])
            avg_chg = rec["sum_chg"] / denom
            plate_m = plate_metric_map.get(pid, {})
            base_chg = self._safe_float(plate_m.get("change_pct", avg_chg), avg_chg)
            score = base_chg * 10.0 + np.log1p(max(0.0, rec["sum_amt2"]) / 1_000_000.0)
            detail = {
                "ts": int(time.time() * 1000),
                "id": pid,
                "name": self._get_plate_name(pid),
                "avg_change_pct": round(avg_chg, 4),
                "base_change_pct": round(base_chg, 4),
                "amount_2min": round(rec["sum_amt2"], 2),
                "score": round(float(score), 4),
                "source": "existing_plate_metrics",
                "sample_stocks": sorted(rec["stocks"], key=lambda x: x["change_pct"], reverse=True)[:6],
            }
            p.zadd(zkey, {pid: float(score)})
            p.hset(dkey, pid, json.dumps(detail, ensure_ascii=False))
        p.expire(dkey, 86400)
        await p.execute()

        all_top = await self.redis.zrevrange(zkey, 0, 10, withscores=True)
        top = []
        for pid, score in all_top:
            if self._is_strategic_plate(pid):
                top.append((pid, score))
            if len(top) >= 3:
                break
        
        if top:
            view = []
            for pid, score in top:
                name = self._get_plate_name(pid)
                view.append(f"{name}({float(score):.2f})")
            self._log_event("plate_snapshot", f"🧭 板块快照Top3: {', '.join(view)}", min_interval_sec=180)

    async def calculate_plate_profiles(self, today_str: str) -> None:
        """Aggregate stock day profiles into plate-level process profiles."""
        profile_key = f"profile:stock:day:{today_str}"
        profile_map = await self.redis.hgetall(profile_key)
        if not profile_map:
            return

        # Reuse existing plate metrics as baseline for plate trend.
        plate_metric_map: Dict[str, Dict[str, Any]] = {}

        plate_stats: Dict[str, Dict[str, float]] = {}
        for code, raw in profile_map.items():
            try:
                pf = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(pf, dict):
                continue
            chg = self._safe_float(pf.get("change_pct", 0.0), 0.0)
            rebound = self._safe_float(pf.get("rebound_from_low", 0.0), 0.0)
            drawdown = self._safe_float(pf.get("drawdown_from_high", 0.0), 0.0)
            amt2 = self._safe_float(pf.get("amount_2min", 0.0), 0.0)
            tp = str(pf.get("turning_point", "none") or "none")

            # --- 构建四大情感核心权重乘数 (Core Weight Multipliers) ---
            amt_total = self._safe_float(pf.get("amount", 0.0), 0.0)
            vol = self._safe_float(pf.get("volume", 0.0), 0.0)
            large_net = self._safe_float(pf.get("large_net", 0.0), 0.0)
            price = self._safe_float(pf.get("price", 0.0), 0.0)
            pre_close = self._safe_float(pf.get("pre_close", 0.0), 0.0)

            # 1. 流动性基调 (Volume Boost)
            volume_boost = np.log1p(amt_total / 100_000_000.0)

            # 2. 极态溢价补偿 (State Premium)
            state_premium = 0.0
            if chg >= 9.5:
                state_premium = 4.0
            elif chg <= -9.5:
                state_premium = 3.5

            base_weight = 1.0 + max(volume_boost, state_premium)

            # 3. 昨日筹码盈亏映射 (Profit Ratio Proxy)
            # 暂用昨日收盘价 pre_close 近似替代 prev_vwap，这反映了昨日买入主力今天被奖励还是被核按带来的极端情绪外溢
            profit_multiplier = 1.0
            if pre_close > 0:
                profit_pct = (price - pre_close) / pre_close * 100.0
                if profit_pct > 3.0:    # 大肉奖励外溢
                    profit_multiplier = 1.35
                elif profit_pct < -4.0: # 核按大面踩踏
                    profit_multiplier = 1.35

            # 4. 散户参与度情绪系数 (Retail Participation)
            # 大成交额 + 主力大单净流出 = 昨日/今日大分歧，散户疯狂接盘/交易
            retail_multiplier = 1.0
            if large_net < 0 and amt_total > 500_000_000:
                retail_multiplier = 1.2

            core_weight = base_weight * profit_multiplier * retail_multiplier
            # -----------------------------------------------------------------

            for pid, base_w in self._weighted_plates_for_code(code):
                w = base_w * core_weight
                rec = plate_stats.setdefault(
                    pid,
                    {
                        "sum_w": 0.0,
                        "stock_n": 0.0,
                        "sum_chg": 0.0,
                        "sum_rebound": 0.0,
                        "sum_drawdown": 0.0,
                        "sum_amt2": 0.0,
                        "deep_v_w": 0.0,
                        "headshot_w": 0.0,
                        "weak_to_strong_w": 0.0,
                        "strong_to_weak_w": 0.0,
                        "single_up_w": 0.0,
                        "single_down_w": 0.0,
                        "up_w": 0.0,
                        "down_w": 0.0,
                    },
                )
                rec["sum_w"] += w
                rec["stock_n"] += 1.0
                rec["sum_chg"] += chg * w
                rec["sum_rebound"] += rebound * w
                rec["sum_drawdown"] += drawdown * w
                rec["sum_amt2"] += amt2 * w
                if chg > 0:
                    rec["up_w"] += w
                elif chg < 0:
                    rec["down_w"] += w
                    
                if tp == "deep_v":
                    rec["deep_v_w"] += w
                elif tp == "headshot":
                    rec["headshot_w"] += w
                elif tp == "weak_to_strong":
                    rec["weak_to_strong_w"] += w
                elif tp == "strong_to_weak":
                    rec["strong_to_weak_w"] += w
                elif tp == "single_sided_up":
                    rec["single_up_w"] += w
                elif tp == "single_sided_down":
                    rec["single_down_w"] += w

        kaipan_by_id = await self._get_kaipan_plate_by_id_cached()
        kp_total = len(kaipan_by_id)

        zkey = f"rank:plate_profile:{today_str}"
        dkey = f"rank:plate_profile:details:{today_str}"
        p = self.redis.pipeline()
        p.delete(zkey)
        p.delete(dkey)

        for pid, s in plate_stats.items():
            denom = max(1e-6, s["sum_w"])
            avg_chg = s["sum_chg"] / denom
            avg_rebound = s["sum_rebound"] / denom
            avg_drawdown = s["sum_drawdown"] / denom
            deep_v_rate = s["deep_v_w"] / denom
            headshot_rate = s["headshot_w"] / denom
            weak_to_strong_rate = s["weak_to_strong_w"] / denom
            strong_to_weak_rate = s["strong_to_weak_w"] / denom
            single_up_rate = s["single_up_w"] / denom
            single_down_rate = s["single_down_w"] / denom
            up_down_ratio = s["up_w"] / max(1e-6, s["down_w"])
            
            base_chg = self._safe_float((plate_metric_map.get(pid) or {}).get("change_pct", avg_chg), avg_chg)

            # [打分维度重构: 基于全天运行结构]
            # 强化绝对强度的加分：单边上涨最佳，深V/弱转强次之。
            repair_strength = 0.5 * deep_v_rate + 0.4 * weak_to_strong_rate + 0.8 * single_up_rate
            # 强化绝对弱势的扣分：单边下跌最惨，爆头次之。
            risk_strength = 0.6 * headshot_rate + 0.5 * strong_to_weak_rate + 0.8 * single_down_rate
            # 成交额（2分钟）对画像进行加权；同时对低样本板块做惩罚，降低噪声板块上榜概率
            liquidity_term = np.log1p(max(0.0, s["sum_amt2"]) / 5_000_000.0)
            active_stock_n = int(round(s.get("stock_n", 0.0)))
            thin_sample_penalty = max(0, 5 - active_stock_n) * 0.8
            
            process_score = (
                base_chg * 10.0 # 基础涨幅依然是核心底座
                + repair_strength * 25.0
                - risk_strength * 20.0
                + 1.2 * liquidity_term
                - thin_sample_penalty
            )
            kp = kaipan_by_id.get(pid)
            kp_bonus_raw = self._kaipan_plate_bonus(kp, kp_total)
            kp_bonus = self.kaipan_plate_blend_weight * kp_bonus_raw
            process_score += kp_bonus

            detail = {
                "ts": int(time.time() * 1000),
                "id": pid,
                "name": self.plate_updater.all_plates.get(pid, {}).get("name", pid),
                "base_change_pct": round(base_chg, 4),
                "avg_change_pct": round(avg_chg, 4),
                "avg_rebound_from_low": round(avg_rebound, 4),
                "avg_drawdown_from_high": round(avg_drawdown, 4),
                "deep_v_rate": round(deep_v_rate, 4),
                "headshot_rate": round(headshot_rate, 4),
                "weak_to_strong_rate": round(weak_to_strong_rate, 4),
                "strong_to_weak_rate": round(strong_to_weak_rate, 4),
                "single_up_rate": round(single_up_rate, 4),
                "single_down_rate": round(single_down_rate, 4),
                "up_down_ratio": round(up_down_ratio, 4),
                "active_stock_n": active_stock_n,
                "amount_2min": round(s["sum_amt2"], 2),
                "liquidity_term": round(float(liquidity_term), 4),
                "thin_sample_penalty": round(float(thin_sample_penalty), 4),
                "kaipan_rank": int(self._safe_float((kp or {}).get("rank", 0), 0)),
                "kaipan_strength": round(self._safe_float((kp or {}).get("strength", 0.0), 0.0), 4),
                "kaipan_change_pct": round(self._safe_float((kp or {}).get("change_pct", 0.0), 0.0), 4),
                "kaipan_amount": round(self._safe_float((kp or {}).get("amount", 0.0), 0.0), 2),
                "kaipan_bonus": round(float(kp_bonus), 4),
                "process_score": round(float(process_score), 4),
            }
            p.zadd(zkey, {pid: float(process_score)})
            p.hset(dkey, pid, json.dumps(detail, ensure_ascii=False))

        p.expire(dkey, 86400)
        await p.execute()

        # 获取排名并过滤区域性板块，确保展示的是核心行业/题材
        all_top = await self.redis.zrevrange(zkey, 0, 10, withscores=True)
        top = []
        for pid, score in all_top:
            if self._is_strategic_plate(pid):
                top.append((pid, score))
            if len(top) >= 3:
                break

        if top:
            msg = []
            for pid, score in top:
                name = self._get_plate_name(pid)
                msg.append(f"{name}({float(score):.2f})")
            self._log_event("plate_profile", f"🧩 板块画像Top3: {', '.join(msg)}", min_interval_sec=180)

    async def calculate_market_process_profile(self, today_str: str) -> None:
        """Aggregate plate profiles into market-level process profile."""
        plate_detail_key = f"rank:plate_profile:details:{today_str}"
        details = await self.redis.hgetall(plate_detail_key)
        if not details:
            logger.info(f"⚠️ Forcing fallback process_profile due to missing plate details for {today_str}")
            out_key = f"market:process_profile:{today_str}"
            await self.redis.hset(
                out_key,
                mapping={
                    "ts": int(time.time() * 1000),
                    "score": 0.5,
                    "state": "mixed",
                    "market_base_change_pct": 0.0,
                    "market_deep_v_rate": 0.0,
                    "market_headshot_rate": 0.0,
                    "market_weak_to_strong_rate": 0.0,
                    "market_avg_rebound_from_low": 0.0,
                    "market_avg_drawdown_from_high": 0.0,
                    "repair_strength": 0.0,
                    "risk_strength": 0.0,
                    "rotation_pressure": 0.0,
                    "strong_plate_count": 0,
                    "weak_plate_count": 0,
                }
            )
            await self.redis.expire(out_key, 86400)
            return

        w_sum = 0.0
        sum_base = 0.0
        sum_deep_v = 0.0
        sum_headshot = 0.0
        sum_w2s = 0.0
        sum_s2w = 0.0
        sum_single_up = 0.0
        sum_single_down = 0.0
        sum_rebound = 0.0
        sum_drawdown = 0.0
        strong_count = 0
        weak_count = 0

        for _, raw in details.items():
            try:
                d = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(d, dict):
                continue
            w = max(1.0, self._safe_float(d.get("amount_2min", 0.0), 0.0) / 10_000_000.0)
            w_sum += w
            base = self._safe_float(d.get("base_change_pct", 0.0), 0.0)
            deep_v = self._safe_float(d.get("deep_v_rate", 0.0), 0.0)
            headshot = self._safe_float(d.get("headshot_rate", 0.0), 0.0)
            w2s = self._safe_float(d.get("weak_to_strong_rate", 0.0), 0.0)
            s2w = self._safe_float(d.get("strong_to_weak_rate", 0.0), 0.0)
            sup = self._safe_float(d.get("single_up_rate", 0.0), 0.0)
            sdn = self._safe_float(d.get("single_down_rate", 0.0), 0.0)
            reb = self._safe_float(d.get("avg_rebound_from_low", 0.0), 0.0)
            dd = self._safe_float(d.get("avg_drawdown_from_high", 0.0), 0.0)
            
            sum_base += base * w
            sum_deep_v += deep_v * w
            sum_headshot += headshot * w
            sum_w2s += w2s * w
            sum_s2w += s2w * w
            sum_single_up += sup * w
            sum_single_down += sdn * w
            sum_rebound += reb * w
            sum_drawdown += dd * w
            if base >= 1.0:
                strong_count += 1
            elif base <= -1.0:
                weak_count += 1

        if w_sum <= 0:
            return

        market_base = sum_base / w_sum
        market_deep_v = sum_deep_v / w_sum
        market_headshot = sum_headshot / w_sum
        market_w2s = sum_w2s / w_sum
        market_s2w = sum_s2w / w_sum
        market_single_up = sum_single_up / w_sum
        market_single_down = sum_single_down / w_sum
        market_rebound = sum_rebound / w_sum
        market_drawdown = sum_drawdown / w_sum
        
        # 同样在市场级别强化单边运行结构的权重
        repair_strength = 0.5 * market_deep_v + 0.4 * market_w2s + 0.8 * market_single_up
        risk_strength = 0.6 * market_headshot + 0.5 * market_s2w + 0.8 * market_single_down
        rotation_pressure = max(0.0, risk_strength - repair_strength)

        score = self._clamp01(
            0.35 * (0.5 + max(-5.0, min(5.0, market_base)) / 10.0)
            + 0.30 * max(0.0, min(1.0, repair_strength))
            + 0.20 * (1.0 - max(0.0, min(1.0, risk_strength)))
            + 0.15 * max(0.0, min(1.0, (strong_count + 1) / max(1, strong_count + weak_count + 1))),
        )

        state = "mixed"
        if score >= 0.65 and repair_strength >= risk_strength:
            state = "risk_on"
        elif score <= 0.4 or risk_strength > repair_strength * 1.3:
            state = "risk_off"

        out_key = f"market:process_profile:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "score": round(score, 4),
            "state": state,
            "market_base_change_pct": round(market_base, 4),
            "market_deep_v_rate": round(market_deep_v, 4),
            "market_headshot_rate": round(market_headshot, 4),
            "market_weak_to_strong_rate": round(market_w2s, 4),
            "market_avg_rebound_from_low": round(market_rebound, 4),
            "market_avg_drawdown_from_high": round(market_drawdown, 4),
            "repair_strength": round(repair_strength, 4),
            "risk_strength": round(risk_strength, 4),
            "rotation_pressure": round(rotation_pressure, 4),
            "strong_plate_count": strong_count,
            "weak_plate_count": weak_count,
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)

        self._log_event(
            "market_process_profile",
            f"🧠 大盘过程画像: {state} score={score:.2f} 修复={repair_strength:.2f} 风险={risk_strength:.2f}",
            min_interval_sec=180,
            log_on_change=True,
        )

    async def calculate_all_plate_phases(self, today_str: str, run_id: Optional[str] = None) -> None:
        if not self.plate_updater:
            return
        run_id = self._ensure_run_id(today_str, run_id=run_id, refresh=not bool(run_id))
        plate_phase_engine_version = "v2_unified"

        top_pids = await self.redis.zrevrange(f"rank:plate_profile:{today_str}", 0, 49)
        if not top_pids:
            top_pids = list(self.plate_updater.all_plates.keys())[:30]

        spread_details_raw = await self.redis.hgetall(f"rank:plate_spread:details:{today_str}") or {}
        attitude_details_raw = await self.redis.hgetall(f"rank:plate_attitude:details:{today_str}") or {}
        profile_details_raw = await self.redis.hgetall(f"rank:plate_profile:details:{today_str}") or {}
        kaipan_by_id = await self._get_kaipan_plate_by_id_cached()

        spread_details = {k: self._safe_json_dict(v) for k, v in spread_details_raw.items()}
        attitude_details = {k: self._safe_json_dict(v) for k, v in attitude_details_raw.items()}
        profile_details = {k: self._safe_json_dict(v) for k, v in profile_details_raw.items()}

        phase_map: Dict[str, str] = {}
        phase_details: Dict[str, Dict[str, Any]] = {}
        for pid in top_pids:
            detail = self._calculate_plate_emotion_state(
                pid,
                today_str,
                spread_detail=spread_details.get(pid, {}),
                attitude_detail=attitude_details.get(pid, {}),
                profile_detail=profile_details.get(pid, {}),
                kaipan_info=kaipan_by_id.get(pid),
            )
            phase = detail.get("phase", "UNKNOWN")
            if phase != "UNKNOWN":
                phase_map[pid] = phase
                phase_details[pid] = detail

        if phase_map:
            out_key = f"market:plate_phase_map:{today_str}"
            detail_key = f"market:plate_phase_detail:{today_str}"
            await self.redis.hset(out_key, mapping=phase_map)
            await self.redis.hset(detail_key, mapping={k: json.dumps(v, ensure_ascii=False) for k, v in phase_details.items()})
            await self.redis.expire(out_key, 86400)
            await self.redis.expire(detail_key, 86400)
            for pid in top_pids[:15]:
                detail = phase_details.get(pid)
                if not detail:
                    continue
                await self._emit_snapshot(
                    self._build_plate_snapshot(
                        today_str=today_str,
                        run_id=run_id,
                        module="calculate_all_plate_phases",
                        detail=detail,
                        is_consumed_plate=False,
                    )
                )
                await self._mark_plate_snapshot_seen(today_str, pid)
            logger.info(f"Plate emotion phases computed ({len(phase_map)} plates) [{plate_phase_engine_version}]")
            self._log_event(
                "plate_phase_status",
                f"板块阶段引擎={plate_phase_engine_version} plates={len(phase_map)}",
                min_interval_sec=300,
                log_on_change=True,
            )

    def _calculate_plate_emotion_phase(self, plate_id: str, today_str: str, kaipan_info: Optional[Dict] = None) -> str:
        """Deprecated thin wrapper. Use _calculate_plate_emotion_state for structured output."""
        detail = self._calculate_plate_emotion_state(plate_id, today_str, kaipan_info=kaipan_info)
        return detail.get("phase", "UNKNOWN")

    def _calculate_plate_emotion_state(
        self,
        plate_id: str,
        today_str: str,
        *,
        spread_detail: Optional[Dict[str, Any]] = None,
        attitude_detail: Optional[Dict[str, Any]] = None,
        profile_detail: Optional[Dict[str, Any]] = None,
        kaipan_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.plate_updater or not plate_id:
            return {"phase": "UNKNOWN", "confidence": 0.0, "leader_code": "", "reason": "missing_plate"}

        stocks = self.plate_updater.plate_to_stocks.get(plate_id, [])
        if not stocks:
            return {"phase": "UNKNOWN", "confidence": 0.0, "leader_code": "", "reason": "empty_plate"}

        lb_counts: Dict[int, int] = {}
        max_lb = 0
        leader_code = ""
        for code in stocks:
            extra = self.stock_extra.get(code, {})
            lb = int(extra.get("consecutive_up_days", 0))
            if lb > 0:
                lb_counts[lb] = lb_counts.get(lb, 0) + 1
                if lb > max_lb:
                    max_lb = lb
                    leader_code = code

        spread_detail = spread_detail or {}
        attitude_detail = attitude_detail or {}
        profile_detail = profile_detail or {}
        spread_ratio = self._safe_float(spread_detail.get("spread_ratio", 0.0), 0.0)
        attitude_score = self._safe_float(attitude_detail.get("latest_delta", 0.0), 0.0)
        profile_score = self._safe_float(profile_detail.get("process_score", 0.0), 0.0)
        headshot_rate = self._safe_float(profile_detail.get("headshot_rate", 0.0), 0.0)
        strong_to_weak_rate = self._safe_float(profile_detail.get("strong_to_weak_rate", 0.0), 0.0)
        weak_to_strong_rate = self._safe_float(profile_detail.get("weak_to_strong_rate", 0.0), 0.0)
        single_down_rate = self._safe_float(profile_detail.get("single_down_rate", 0.0), 0.0)
        single_up_rate = self._safe_float(profile_detail.get("single_up_rate", 0.0), 0.0)
        active_stock_n = self._safe_int(profile_detail.get("active_stock_n", 0), 0)
        main_net = self._safe_float((kaipan_info or {}).get("main_net", 0.0), 0.0)
        strength = self._safe_float((kaipan_info or {}).get("strength", 0.0), 0.0)
        first_board_count = lb_counts.get(1, 0)
        second_board_count = lb_counts.get(2, 0)
        high_board_count = sum(v for k, v in lb_counts.items() if k >= 3)
        has_echelon = max_lb >= 2 and first_board_count > 0 and second_board_count > 0

        phase = "UNKNOWN"
        confidence = 0.35
        reason = "fallback"
        if (
            (max_lb <= 1 and first_board_count <= 1 and spread_ratio < 0.18)
            or single_down_rate >= 0.35
            or (strength < 30 and main_net < -20_000_000)
            or profile_score <= -3.0
        ):
            phase = "retreat"
            confidence = 0.78 if profile_score <= -3.0 or single_down_rate >= 0.35 else 0.62
            reason = "retreat_structure"
        elif (
            max_lb >= 3
            and (has_echelon or high_board_count >= 1)
            and spread_ratio >= 0.28
            and headshot_rate <= 0.18
            and strong_to_weak_rate <= 0.22
            and main_net > -50_000_000
        ):
            phase = "climax"
            confidence = 0.82
            reason = "main_rise_echelon"
        elif (
            max_lb >= 2
            and (
                headshot_rate >= 0.20
                or strong_to_weak_rate >= 0.22
                or (main_net < -50_000_000 and strength > 40)
                or (spread_ratio < 0.25 and max_lb >= 3)
            )
        ):
            phase = "divergence"
            confidence = 0.76
            reason = "diverge_breakdown"
        elif (
            max_lb <= 2
            and first_board_count >= 2
            and spread_ratio >= 0.18
            and weak_to_strong_rate >= headshot_rate
            and single_up_rate >= single_down_rate
        ):
            phase = "start"
            confidence = 0.70
            reason = "ignition_breadth"

        return {
            "ts": int(time.time() * 1000),
            "date": today_str,
            "plate_phase_engine_version": "v2_unified",
            "plate_id": plate_id,
            "plate_name": self.plate_updater.all_plates.get(plate_id, {}).get("name", plate_id),
            "phase": phase,
            "confidence": round(confidence, 4),
            "leader_code": leader_code,
            "max_lb": max_lb,
            "spread_ratio": round(spread_ratio, 4),
            "attitude_score": round(attitude_score, 4),
            "profile_score": round(profile_score, 4),
            "headshot_rate": round(headshot_rate, 4),
            "strong_to_weak_rate": round(strong_to_weak_rate, 4),
            "weak_to_strong_rate": round(weak_to_strong_rate, 4),
            "single_down_rate": round(single_down_rate, 4),
            "single_up_rate": round(single_up_rate, 4),
            "main_net": round(main_net, 2),
            "strength": round(strength, 4),
            "first_board_count": first_board_count,
            "second_board_count": second_board_count,
            "high_board_count": high_board_count,
            "active_stock_n": active_stock_n,
            "reason": reason,
        }

    async def calculate_strategy_tags(
        self,
        today_str: str,
        expectation_eval: Optional[Dict[str, Any]] = None,
        auction_items: Optional[List[Dict[str, Any]]] = None,
        first_limit_codes: Optional[Set[str]] = None,
    ) -> None:
        """Build strategy tags from cycle context + auction/open validation + plate competition."""
        ts = int(time.time() * 1000)
        prev_day = self.calendar.get_previous_trade_day(today_str)

        # Yesterday regime context
        prev_sentiment = await self.redis.hgetall(f"market:sentiment:{prev_day}")
        prev_process = await self.redis.hgetall(f"market:process_profile:{prev_day}")
        prev_resonance = await self.redis.hgetall(f"market:resonance:{prev_day}")
        prev_phase = prev_sentiment.get("phase", "unknown")
        prev_process_state = prev_process.get("state", "unknown")
        prev_resonance_state = prev_resonance.get("state", "unknown")

        if prev_phase == "consistent" and prev_process_state == "risk_on":
            regime_y = "consensus_high"
        elif prev_phase == "retreat" or prev_process_state == "risk_off":
            regime_y = "retreat"
        elif prev_phase in ("repair", "ice_point"):
            # Treat legacy 'repair' phase (old code) as 'ice_point' for compatibility
            regime_y = "repair"
        else:
            regime_y = "mixed"

        # Auction regime — anchored to 09:25 morning snapshot (write-once per day)
        # After 09:30, freeze regime_a from the first computed value to avoid live-data oscillation.
        regime_a_cache_key = f"market:strategy_tags:{today_str}:regime_a"
        cached_regime_a = await self.redis.get(regime_a_cache_key)
        if cached_regime_a:
            regime_a = cached_regime_a
            # Still load auction rows for other metrics (high_open_ratio etc.) used below
            auction_items = auction_items if auction_items is not None else await self._get_auction_top_amount_cached(
                today_str, require_final_0925=False
            )
            auc_rows = []
            for it in auction_items[:200]:
                code = str(it.get("symbol") or "").strip()
                if len(code) != 6:
                    continue
                auc = self._normalize_change_pct(it.get("change_pct", 0.0), scale=1.0)
                bid = self._safe_float(it.get("bid_amount_yuan", 0.0), 0.0)
                aamt = self._safe_float(it.get("auction_amount_yuan", 0.0), 0.0)
                auc_rows.append({"code": code, "auc": auc, "bid": bid, "aamt": aamt})
        else:
            auction_items = auction_items if auction_items is not None else await self._get_auction_top_amount_cached(
                today_str, require_final_0925=False
            )
            auc_rows = []
            for it in auction_items[:200]:
                code = str(it.get("symbol") or "").strip()
                if len(code) != 6:
                    continue
                auc = self._normalize_change_pct(it.get("change_pct", 0.0), scale=1.0)
                bid = self._safe_float(it.get("bid_amount_yuan", 0.0), 0.0)
                aamt = self._safe_float(it.get("auction_amount_yuan", 0.0), 0.0)
                auc_rows.append({"code": code, "auc": auc, "bid": bid, "aamt": aamt})

            def _compute_regime_a(rows):
                n = max(1, len(rows))
                hor = sum(1 for x in rows if x["auc"] >= 5.0) / n
                dlr = sum(1 for x in rows if x["auc"] <= -8.0) / n
                if hor >= 0.20 and dlr <= 0.05:
                    return "high_open_consensus"
                elif dlr >= 0.15:
                    return "panic_low_open"
                elif hor >= 0.10 and dlr >= 0.10:
                    return "divergence_auction"
                else:
                    return "neutral_auction"

            if auc_rows:
                regime_a = _compute_regime_a(auc_rows)
                # Cache as write-once: freeze for the rest of the day
                await self.redis.set(regime_a_cache_key, regime_a, ex=86400)
            else:
                regime_a = "neutral_auction"

        def _limit_up_th(code: str) -> float:
            c = str(code or "")
            if c.startswith(("300", "301", "688", "689")):
                return 19.8
            return 9.8

        n_auc = max(1, len(auc_rows))
        high_open_ratio = sum(1 for x in auc_rows if x["auc"] >= 5.0) / n_auc
        deep_low_ratio = sum(1 for x in auc_rows if x["auc"] <= -8.0) / n_auc
        one_word_ratio = sum(1 for x in auc_rows if x["auc"] >= _limit_up_th(x["code"]) and x["bid"] > 0) / n_auc
        top20 = sorted(auc_rows, key=lambda x: x["aamt"], reverse=True)[:20]
        auction_concentration = (
            sum(x["aamt"] for x in top20) / max(1.0, sum(x["aamt"] for x in auc_rows))
            if auc_rows else 0.0
        )

        # Note: regime_a is now anchored to morning snapshot (already set above)

        # Open validation from expectation eval
        ee = expectation_eval if expectation_eval is not None else await self.redis.hgetall(
            f"diag:expectation_eval:{today_str}"
        )
        eff = self._safe_float(ee.get("effectiveness", 0.0), 0.0)
        fade_count = self._safe_int(ee.get("fade_count", 0), 0)
        rise_count = self._safe_int(ee.get("rise_count", 0), 0)
        hold_rate = self._safe_float(ee.get("strong_high_hold_rate", 0.0), 0.0)
        rebound_rate = self._safe_float(ee.get("extreme_low_rebound_rate", 0.0), 0.0)
        one_word_break_rate = self._safe_float(ee.get("one_word_break_rate", 0.0), 0.0)
        seal_ratio_front20 = self._safe_float(ee.get("seal_ratio_front20", 0.0), 0.0)
        seal_ratio_one_word = self._safe_float(ee.get("seal_ratio_one_word", 0.0), 0.0)

        if hold_rate >= 0.65 and one_word_break_rate <= 0.25:
            regime_o = "open_holding"
        elif rebound_rate >= 0.35 and rise_count > fade_count:
            regime_o = "open_repair"
        elif one_word_break_rate >= 0.40 or fade_count > max(1, int(rise_count * 1.2)):
            regime_o = "open_fade"
        else:
            regime_o = "open_neutral"

        if regime_o == "open_holding" and seal_ratio_front20 < 0.20:
            regime_o = "open_holding_weak_seal"
        elif regime_o in ("open_fade", "open_neutral") and seal_ratio_front20 >= 0.35:
            regime_o = "open_neutral_strong_seal"

        # Limit-up seal ratio from realtime pool: seal = book1_amount_yuan / amount
        limit_up_codes: Set[str] = set(first_limit_codes or set())
        if not limit_up_codes:
            try:
                limit_up_codes = await self._get_first_limit_codes_cached()
            except Exception:
                pass

        if not limit_up_codes:
            for x in auc_rows:
                c = x.get("code", "")
                if c and x.get("auc", 0.0) >= _limit_up_th(c):
                    limit_up_codes.add(c)

        limit_up_seal_ratios: List[float] = []
        if limit_up_codes:
            qmap = await self._fetch_quotes_batch(list(limit_up_codes)[:300])
            for _, q in qmap.items():
                if not q:
                    continue
                book1 = self._safe_float(q.get("book1_amount_yuan", 0.0), 0.0)
                amount = self._safe_float(q.get("amount", 0.0), 0.0)
                if book1 > 0 and amount > 0:
                    limit_up_seal_ratios.append(book1 / amount)

        limit_up_seal_ratio_avg = (
            float(sum(limit_up_seal_ratios) / len(limit_up_seal_ratios))
            if limit_up_seal_ratios
            else 0.0
        )
        limit_up_strong_seal_rate = (
            float(sum(1 for x in limit_up_seal_ratios if x >= 0.15) / max(1, len(limit_up_seal_ratios)))
            if limit_up_seal_ratios
            else 0.0
        )
        if regime_o in ("open_holding", "open_holding_weak_seal") and len(limit_up_codes) >= 3 and limit_up_seal_ratio_avg < 0.05:
            regime_o = "open_holding_weak_seal"
        elif regime_o in ("open_neutral", "open_fade") and len(limit_up_codes) >= 3 and limit_up_seal_ratio_avg >= 0.15:
            regime_o = "open_neutral_strong_seal"

        # Freeze regime_o after 10:00
        regime_o_cache_key = f"market:strategy_tags:{today_str}:regime_o"
        now_time = datetime.now().strftime("%H:%M")
        cached_regime_o = await self.redis.get(regime_o_cache_key)
        if cached_regime_o and now_time >= "10:00":
            regime_o = cached_regime_o
        elif now_time >= "10:00":
            await self.redis.set(regime_o_cache_key, regime_o, ex=86400)

        # Plate competition — anchored to morning snapshot (write-once per day)
        # plate_comp 依赖 rank:plate_profile 实时分数，每5分钟变化会导致主标签振荡。
        # 首次计算后写入 Redis 冻结，余下时间读取缓存，只有 regime_o 允许盘中实时更新。
        top_plates = await self.redis.zrevrange(f"rank:plate_profile:{today_str}", 0, 2, withscores=True)
        s1 = float(top_plates[0][1]) if len(top_plates) >= 1 else 0.0
        s2 = float(top_plates[1][1]) if len(top_plates) >= 2 else 0.0
        s3 = float(top_plates[2][1]) if len(top_plates) >= 3 else 0.0
        lead_ratio = s1 / max(1e-6, s2) if s2 > 0 else 0.0
        spread_top3 = (max(s1, s2, s3) - min(s1, s2, s3)) if len(top_plates) >= 3 else 0.0

        plate_comp_cache_key = f"market:strategy_tags:{today_str}:plate_comp"
        cached_plate_comp = await self.redis.get(plate_comp_cache_key)
        if cached_plate_comp:
            plate_comp = cached_plate_comp
        else:
            if len(top_plates) < 2:
                plate_comp = "unknown"
            elif lead_ratio >= 1.25 and (s1 - s2) >= 0.05:
                plate_comp = "mainline_clear"
            elif abs(s1 - s2) <= 0.03:
                plate_comp = "double_mainline_compete"
            elif len(top_plates) >= 3 and spread_top3 <= 0.05:
                plate_comp = "multi_line_rotation"
            else:
                plate_comp = "mixed_compete"

            if plate_comp != "unknown":
                # 冻结当天的板块竞争格局，避免盘中实时分数导致主标签反复跳变
                await self.redis.set(plate_comp_cache_key, plate_comp, ex=86400)

        ab_data = await self.redis.hgetall(f"market:ab_arbitrage:{today_str}")
        ab_pair_count = self._safe_int(ab_data.get("pair_count", 0), 0)
        ab_aa_count = self._safe_int(ab_data.get("aa_count", 0), 0)

        # Tag set
        tags: List[str] = []
        if regime_a == "high_open_consensus" and regime_o == "open_holding":
            tags.append("一致加速日")
        if regime_o == "open_holding_weak_seal":
            tags.append("高开弱封单日")
        if regime_o == "open_neutral_strong_seal":
            tags.append("分歧强承接日")
        if limit_up_seal_ratio_avg >= 0.15 and len(limit_up_codes) >= 3:
            tags.append("封单承接强日")
        if limit_up_seal_ratio_avg <= 0.05 and len(limit_up_codes) >= 3:
            tags.append("封单承接弱日")
        if regime_a in ("divergence_auction", "neutral_auction") and regime_o == "open_repair":
            tags.append("分歧转一致日")
        if regime_a == "high_open_consensus" and regime_o == "open_fade":
            tags.append("一致转分歧日")
        if regime_y == "retreat" and regime_o == "open_repair":
            tags.append("冰点修复日")
        if regime_y in ("consensus_high", "repair") and regime_o == "open_fade":
            tags.append("退潮确认日")
        if plate_comp == "mainline_clear":
            tags.append("主线清晰日")
        if plate_comp == "double_mainline_compete":
            tags.append("双主线竞争日")
        if plate_comp == "multi_line_rotation":
            tags.append("多主线轮动日")
        if regime_a == "panic_low_open":
            tags.append("恐慌低开日")
        w2s_window_score = self._safe_float(ee.get("weak_to_strong_window_score", 0.0), 0.0)
        s2w_pressure_score = self._safe_float(ee.get("strong_to_weak_pressure_score", 0.0), 0.0)
        fake_trap_score = self._safe_float(ee.get("fake_strength_trap_score", 0.0), 0.0)

        if w2s_window_score >= 0.60:
            tags.append("弱转强窗口日")
        if s2w_pressure_score >= 0.60:
            tags.append("强转弱风险日")
        if fake_trap_score >= 0.40:
            tags.append("疑似诱多陷阱")
        if eff >= 0.60:
            tags.append("预期差有效日")
        if one_word_ratio >= 0.08 and one_word_break_rate >= 0.35:
            tags.append("高位炸板风险日")
        if ab_aa_count >= 3:
            tags.append("A直做窗口")
        if ab_pair_count >= 3:
            tags.append("A带B套利窗口")
        if not tags:
            tags.append("中性观察日")

        primary_tag = tags[0]
        out_key = f"market:strategy_tags:{today_str}"
        payload = {
            "ts": ts,
            "primary_tag": primary_tag,
            "secondary_tags": json.dumps(tags[1:], ensure_ascii=False),
            "regimes": json.dumps(
                {
                    "yesterday": regime_y,
                    "auction": regime_a,
                    "open": regime_o,
                    "plate_competition": plate_comp,
                    "prev_phase": prev_phase,
                    "prev_process_state": prev_process_state,
                    "prev_resonance_state": prev_resonance_state,
                },
                ensure_ascii=False,
            ),
            "metrics": json.dumps(
                {
                    "effectiveness": round(eff, 4),
                    "fade_count": fade_count,
                    "rise_count": rise_count,
                    "high_open_ratio": round(high_open_ratio, 4),
                    "deep_low_ratio": round(deep_low_ratio, 4),
                    "one_word_ratio": round(one_word_ratio, 4),
                    "one_word_break_rate": round(one_word_break_rate, 4),
                    "seal_ratio_front20": round(seal_ratio_front20, 4),
                    "seal_ratio_one_word": round(seal_ratio_one_word, 4),
                    "limit_up_count_from_pool": len(limit_up_codes),
                    "limit_up_seal_ratio_avg": round(limit_up_seal_ratio_avg, 4),
                    "limit_up_strong_seal_rate": round(limit_up_strong_seal_rate, 4),
                    "strong_high_hold_rate": round(hold_rate, 4),
                    "extreme_low_rebound_rate": round(rebound_rate, 4),
                    "auction_concentration": round(auction_concentration, 4),
                    "lead_ratio": round(lead_ratio, 4),
                    "ab_aa_count": ab_aa_count,
                    "ab_pair_count": ab_pair_count,
                    "weak_to_strong_window_score": round(w2s_window_score, 4),
                    "strong_to_weak_pressure_score": round(s2w_pressure_score, 4),
                    "fake_strength_trap_score": round(fake_trap_score, 4),
                },
                ensure_ascii=False,
            ),
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)

        self._log_event(
            "strategy_tags",
            f"策略标签: {primary_tag} | y={regime_y} a={regime_a} o={regime_o} plate={plate_comp}",
            min_interval_sec=300,
            log_on_change=True,
        )

    async def calculate_ab_arbitrage(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict[str, Any]],
        auction_items: Optional[List[Dict[str, Any]]] = None,
        quote_map: Optional[Dict[str, Dict]] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """A->B套利信号：
        - 看A：竞价强 + 封成比高 + 盘中不弱
        - 做B：同板块内低位/跟随转强个股
        """
        if not candidate_pool:
            return
        run_id = self._ensure_run_id(today_str, run_id=run_id)

        # 预热开盘啦板块快照缓存，供主板块判定使用
        await self._get_kaipan_plate_by_id_cached()

        def _confidence(v: float) -> str:
            if v >= 0.75:
                return "high"
            if v >= 0.60:
                return "medium"
            return "low"

        thresholds = {
            "a_min_auction_change_pct": 3.0,
            "a_min_auction_seal_ratio": 0.20,
            "a_min_hold": 0.35,
            "b_max_under_a_gap_pct": -0.3,  # b_cur <= a_cur - 0.3
            "b_min_current_change_pct": -2.0,  # 过滤过弱B，避免抄过深下跌
            "b_min_change_rate_1min": 0.0,
            "b_min_amount_2min": 5_000_000.0,
        }

        auction_items = auction_items if auction_items is not None else await self._get_auction_top_amount_cached(
            today_str, require_final_0925=False
        )
        if not auction_items:
            return

        auction_map: Dict[str, Dict[str, float]] = {}
        for it in auction_items[:300]:
            code = str(it.get("symbol") or "").strip()
            if len(code) != 6:
                continue
            # 特殊处理：竞价数据如果是经过 _normalize_auction_item 处理过的，已经是百分数，scale=1.0 即可
            auc = self._normalize_change_pct(it.get("change_pct", 0.0), scale=1.0)
            bid = self._safe_float(it.get("bid_amount_yuan", 0.0), 0.0)
            aamt = self._safe_float(it.get("auction_amount_yuan", 0.0), 0.0)
            auction_map[code] = {
                "auc": auc,
                "bid": bid,
                "aamt": aamt,
                "seal": bid / max(1.0, aamt),
            }

        if not auction_map:
            return

        if not quote_map:
            quote_map = await self._fetch_quotes_batch(list(candidate_pool))

        plate_rank = await self.redis.zrevrange(f"rank:plate_profile:{today_str}", 0, 9, withscores=True)
        if not plate_rank:
            plate_rank = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 9, withscores=True)
        hot_plate_set = {pid for pid, _ in plate_rank}

        # 获取开盘啦涨停原因/概念映射 (用于主板块识别加分)
        # 如果缓存缺失，对 candidate_pool 中的关键标的进行实时增量同步
        stock_reasons = {}
        leaders = []
        if candidate_pool:
            try:
                s2p_cache = await self.redis.hgetall("config:plate_mapping:s2p") or {}
                # 【P0修复】将 bytes 键转为 str，避免 hgetall 返回 bytes 导致永远找不到缓存
                s2p_keys_str = {k.decode("utf-8") if isinstance(k, bytes) else str(k) for k in s2p_cache.keys()}
                # 统计缺失情况 (仅限当前分析池中的核心关注标的)
                missing_candidates = [c for c in candidate_pool if c not in s2p_keys_str]
                
                # 仅针对分析池中少量缺失标的补充，避免盘中大规模网络请求 (每次循环限额10只)
                if 0 < len(missing_candidates) <= 25 and self.stock_analyzer:
                    logger.info(f"MarketEdge: 检测到分析池中 {len(missing_candidates)} 只股票缺少题材映射，开始实时增量同步...")
                    for c in missing_candidates[:10]:
                        try:
                            res = self.stock_analyzer.get_ban_reasons(c)
                            themes = []
                            if res and res.get('List'):
                                for item in res['List']:
                                    reason_text = item.get('Reason', '')
                                    if '；' in reason_text:
                                        themes.extend([t.strip() for t in reason_text.split('；')[0].split('+') if t.strip()])
                                    else:
                                        themes.extend([t.strip() for t in reason_text.split('+') if t.strip()])
                            
                            # 无论是否找到题材，只要执行了查询就写回 Redis 实现“负向缓存”
                            # 这样下一轮循环中 missing_candidates 就不会再包含该股票
                            stock_reasons[c] = themes
                            await self.redis.hset("config:plate_mapping:s2p", c, json.dumps(themes, ensure_ascii=False))
                            
                            await asyncio.sleep(0.1) # 遵守频率限制
                        except Exception as e:
                            logger.error(f"Error syncing theme for {c}: {e}")
                
                # 整合缓存与实时获取的数据
                for c in candidate_pool:
                    if c in s2p_cache:
                        try:
                            stock_reasons[c] = json.loads(s2p_cache[c])
                        except: pass
                    elif c in stock_reasons: 
                        pass 
            except Exception as e:
                logger.error(f"Error fetching/updating s2p mapping: {e}")

        # Identify leaders (A)
        for code, a in auction_map.items():
            if code not in candidate_pool:
                continue

            # Fetch indicators
            ind = indicators.get(code) or {}
            cur = self._normalize_change_pct(ind.get("change_pct", 0.0))
            hold = self._safe_float(ind.get("hold_score", 0.0), 0.0)
            auc_strength = self._safe_float(ind.get("auc_strength", 0.0), 0.0)
            book1_amount = self._safe_float(ind.get("book1_amount_yuan", 0.0), 0.0)
            book1_strength = self._safe_float(ind.get("book1_strength", 0.0), 0.0)
            seal = a.get("seal", 0.0)

            plate_id = self._get_major_plate(code, hot_plate_set)
            plate_hot = 1.0 if plate_id and plate_id in hot_plate_set else 0.0

            score = (
                0.35 * seal
                + 0.30 * hold
                + 0.20 * auc_strength
                + 0.05 * plate_hot
                + 0.10 * book1_strength
            )
            
            # === 筹码诊断 (P2 Chip Integration) ===
            # 如果是高获利盘且处于突破位，额外加分
            chip = self.chip_peaks.get(code)
            profit_ratio = 0.0
            if chip:
                try:
                    if isinstance(chip, str): chip = json.loads(chip)
                    profit_ratio = chip.get("profit_ratio", 0.0)
                    if profit_ratio > 0.9 and cur > 0:
                        score += 0.10 # 筹码真空区奖励
                except: pass

            if (
                a["auc"] >= thresholds["a_min_auction_change_pct"]
                and a["seal"] >= thresholds["a_min_auction_seal_ratio"]
                and hold >= thresholds["a_min_hold"]
            ):
                leaders.append(
                    {
                        "code": code,
                        "plate_id": plate_id,
                        "auction_change_pct": round(a["auc"], 3),
                        "current_change_pct": round(cur, 3),
                        "auction_seal_ratio": round(a["seal"], 4),
                        "book1_amount_yuan": round(book1_amount, 2),
                        "book1_strength": round(book1_strength, 4),
                        "hold_score": round(hold, 4),
                        "profit_ratio": profit_ratio,
                        "score": round(float(score), 4),
                        "confidence": _confidence(score),
                    }
                )


        if not leaders:
            out_key = f"market:ab_arbitrage:{today_str}"
            await self.redis.hset(
                out_key,
                mapping={
                    "ts": int(time.time() * 1000),
                    "leader_count": 0,
                    "aa_count": 0,
                    "pair_count": 0,
                    "aa_candidates": "[]",
                    "leaders": "[]",
                    "pairs": "[]",
                    "thresholds": json.dumps(thresholds, ensure_ascii=False),
                },
            )
            await self.redis.expire(out_key, 86400)
            return

        leaders.sort(key=lambda x: x["score"], reverse=True)
        leaders = leaders[:10]

        # A->A: direct candidates (buy A)
        aa_candidates: List[Dict[str, Any]] = []
        for a in leaders:
            aa_score = 0.60 * self._safe_float(a.get("score", 0.0), 0.0) + 0.40 * self._safe_float(a.get("hold_score", 0.0), 0.0)
            aa_candidates.append(
                {
                    "code": a["code"],
                    "plate_id": a.get("plate_id", ""),
                    "plate_name": self.plate_updater.all_plates.get(a.get("plate_id", ""), {}).get("name", a.get("plate_id", "")),
                    "auction_change_pct": a["auction_change_pct"],
                    "current_change_pct": a["current_change_pct"],
                    "auction_seal_ratio": a["auction_seal_ratio"],
                    "book1_amount_yuan": a.get("book1_amount_yuan", 0.0),
                    "score": round(float(aa_score), 4),
                    "confidence": _confidence(aa_score),
                    "mode": "A_DIRECT",
                    "reason": "A强承接，直接做A",
                }
            )
        aa_candidates.sort(key=lambda x: x["score"], reverse=True)
        aa_candidates = aa_candidates[:10]

        pairs: List[Dict[str, Any]] = []
        for a in leaders:
            a_code = a["code"]
            a_plate = a.get("plate_id", "")
            if not a_plate:
                continue
            a_cur = self._safe_float(a.get("current_change_pct", 0.0), 0.0)
            peer_codes = [
                c for c in (self.plate_updater.plate_to_stocks.get(a_plate, []) or [])
                if c in candidate_pool and c != a_code
            ]
            if not peer_codes:
                continue

            b_candidates: List[Dict[str, Any]] = []
            for b_code in peer_codes:
                bq = quote_map.get(b_code) or {}
                bind = indicators.get(b_code) or {}
                b_cur = self._normalize_change_pct(bq.get("change_pct", bq.get("change", bind.get("change_pct", 0.0))))
                b_cr1 = self._safe_float(bind.get("change_rate_1min", 0.0), 0.0)
                b_amt2 = self._safe_float(bind.get("amount_2min", 0.0), 0.0)

                # B：同板块、相对A有补涨空间、且出现跟随动能
                if b_cur >= a_cur + thresholds["b_max_under_a_gap_pct"]:
                    continue
                if b_cur < thresholds["b_min_current_change_pct"]:
                    continue
                
                # --- 增强逻辑：位置与筹码判断 ---
                b_extra = self.stock_extra.get(b_code, {})
                # B端近5日涨幅过大不选（防止追高）
                if self._safe_float(b_extra.get("change_pct_5d"), 0) > 15.0:
                    continue
                # B端市值过大不选（套利优先小盘/中盘）
                if self._safe_float(b_extra.get("real_market_cap"), 0) > 500:
                    continue
                # B端上方若有筹码压制，打分减小
                b_chip = self.chip_peaks.get(b_code, {})
                chip_bonus = 0.0
                if b_chip and b_cur > 0:
                    peak_p = self._safe_float(b_chip.get("peak_price"), 999)
                    if (bq.get("price", 0) or 0) < peak_p:
                        chip_bonus = -0.2 # 在主力成本下方，有压力

                if b_cr1 <= thresholds["b_min_change_rate_1min"] and b_amt2 < thresholds["b_min_amount_2min"]:
                    continue

                dislocation = self._clamp01((a_cur - b_cur) / 10.0)
                momentum = self._clamp01((b_cr1 + 1.0) / 3.0)
                amount_norm = self._clamp01(np.log1p(max(0.0, b_amt2) / 1_000_000.0) / 4.0)
                b_score = 0.50 * dislocation + 0.35 * momentum + 0.15 * amount_norm + chip_bonus
                b_candidates.append(
                    {
                        "code": b_code,
                        "change_pct": round(b_cur, 3),
                        "change_rate_1min": round(b_cr1, 4),
                        "amount_2min": round(b_amt2, 2),
                        "score": round(max(0, float(b_score)), 4),
                    }
                )

            if not b_candidates:
                continue
            b_candidates.sort(key=lambda x: x["score"], reverse=True)
            for b in b_candidates[:2]:
                pairs.append(
                    {
                        "a_code": a_code,
                        "b_code": b["code"],
                        "plate_id": a_plate,
                        "plate_name": self.plate_updater.all_plates.get(a_plate, {}).get("name", a_plate),
                        "a_score": a["score"],
                        "b_score": b["score"],
                        "a_auction_seal_ratio": a["auction_seal_ratio"],
                        "a_auction_change_pct": a["auction_change_pct"],
                        "a_current_change_pct": a["current_change_pct"],
                        "b_current_change_pct": b["change_pct"],
                        "b_change_rate_1min": b["change_rate_1min"],
                        "reason": "A强承接，B同板块补涨/跟随",
                    }
                )

        for i in range(len(pairs)):
            a_score = self._safe_float(pairs[i].get("a_score", 0.0), 0.0)
            b_score = self._safe_float(pairs[i].get("b_score", 0.0), 0.0)
            pair_score = 0.60 * a_score + 0.40 * b_score
            pairs[i]["pair_score"] = round(float(pair_score), 4)
            pairs[i]["confidence"] = _confidence(pair_score)
            pairs[i]["mode"] = "A_TO_B"
        pairs.sort(key=lambda x: x.get("pair_score", 0.0), reverse=True)

        # 【P0修复】直接在日志输出 A/B 套利候选，方便人工观察
        if aa_candidates:
            aa_log = []
            for a in aa_candidates[:3]:
                nm = self._get_stock_name(a['code'])
                aa_log.append(f"{nm}({a['code']}) 竟{a['auction_change_pct']}% 封{a['auction_seal_ratio']:.0%}")
            self._log_event("ab_aa_top", "💰 A直接候选: " + " | ".join(aa_log), min_interval_sec=60)
        if pairs:
            pairs_log = [
                f"{p.get('plate_name','')} {self._get_stock_name(p['a_code'])}[A]→{self._get_stock_name(p['b_code'])}[B] "
                f"A竟:{p['a_auction_change_pct']}% B当:{p['b_current_change_pct']}% 分:{p.get('pair_score',0):.2f}"
                for p in pairs[:3]
            ]
            self._log_event("ab_pairs_top", f"🎯 A→B套利 Top3:\n" + "\n".join(pairs_log), min_interval_sec=60)

        out_key = f"market:ab_arbitrage:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": int(time.time() * 1000),
                "leader_count": len(leaders),
                "aa_count": len(aa_candidates),
                "pair_count": len(pairs),
                "aa_candidates": json.dumps(aa_candidates, ensure_ascii=False),
                "leaders": json.dumps(leaders, ensure_ascii=False),
                "pairs": json.dumps(pairs[:30], ensure_ascii=False),
                "thresholds": json.dumps(thresholds, ensure_ascii=False),
            },
        )
        await self.redis.expire(out_key, 86400)

        for aa_item in aa_candidates:
            await self._ensure_plate_snapshot(
                today_str,
                str(aa_item.get("plate_id", "")),
                run_id,
                module="calculate_ab_arbitrage",
                is_consumed_plate=True,
            )
            await self._emit_snapshot(
                self._build_ab_pair_snapshot(
                    today_str=today_str,
                    run_id=run_id,
                    module="calculate_ab_arbitrage",
                    pair=aa_item,
                    pair_kind="AA",
                    thresholds=thresholds,
                )
            )

        for pair in pairs[:30]:
            await self._ensure_plate_snapshot(
                today_str,
                str(pair.get("plate_id", "")),
                run_id,
                module="calculate_ab_arbitrage",
                is_consumed_plate=True,
            )
            await self._emit_snapshot(
                self._build_ab_pair_snapshot(
                    today_str=today_str,
                    run_id=run_id,
                    module="calculate_ab_arbitrage",
                    pair=pair,
                    pair_kind="A_TO_B",
                    thresholds=thresholds,
                )
            )

        if aa_candidates:
            top_aa = aa_candidates[0]
            # 调试：输出A票的候选板块权重Top3，便于排查归因是否偏题材
            try:
                a_code = top_aa["code"]
                cands = sorted((self.plate_weight_cache.get(a_code) or []), key=lambda x: x[1], reverse=True)[:3]
                if cands:
                    txt = " | ".join(
                        f"{self.plate_updater.all_plates.get(pid, {}).get('name', pid)}:{w:.2f}"
                        for pid, w in cands
                    )
                    self._log_event("aa_plate_candidates", f"🧪 A票板块候选: {self._get_stock_name(a_code)}({a_code}) -> {txt}", min_interval_sec=120)
            except Exception:
                pass
            self._log_event(
                "aa_direct",
                (
                    f"A直做: A={self._get_stock_name(top_aa['code'])}({top_aa['code']}) 板块={top_aa['plate_name']} "
                    f"封成比={top_aa['auction_seal_ratio']:.2f} 现涨幅={top_aa['current_change_pct']:.1f}% "
                    f"L1={self._safe_float(top_aa.get('book1_amount_yuan', 0.0), 0.0)/10000:.0f}万 "
                    f"置信度={top_aa['confidence']}"
                ),
                min_interval_sec=180,
                log_on_change=True,
            )

        if pairs:
            top = pairs[0]
            self._log_event(
                "ab_arbitrage",
                (
                    f"🎯 弱转强套利(A->B): A={self._get_stock_name(top['a_code'])}({top['a_code']}) B={self._get_stock_name(top['b_code'])}({top['b_code']}) "
                    f"板块={top['plate_name']} A封成比={top['a_auction_seal_ratio']:.2f} "
                    f"A涨幅={top['a_current_change_pct']:.1f}% B涨幅={top['b_current_change_pct']:.1f}% "
                    f"置信度={top['confidence']}"
                ),
                min_interval_sec=180,
                log_on_change=True,
            )
        else:
            self._log_event(
                "ab_arbitrage",
                f"A->B套利: 当前无可用配对（leaders={len(leaders)}）",
                min_interval_sec=300,
                log_on_change=True,
            )

    def _safe_json_list(self, raw: Any) -> List[Dict[str, Any]]:
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if not raw:
            return []
        try:
            obj = json.loads(raw)
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
        except Exception:
            pass
        return []

    async def calculate_data_contract_health(self, today_str: str) -> Dict[str, Any]:
        """Check key quote/auction field completeness for today's live flow."""
        items = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
        if not items:
            return {}

        codes: List[str] = []
        for it in items[:80]:
            code = str(it.get("symbol") or "").strip()
            if len(code) == 6:
                codes.append(code)
        if not codes:
            return {}

        quote_map = await self._fetch_quotes_batch(codes)
        required = ["change_pct", "amount", "book1_amount_yuan", "price"]
        process_required = ["high", "low", "pre_close", "last_close"]
        miss: Dict[str, int] = {k: 0 for k in required + process_required}
        found = 0
        for c in codes:
            q = quote_map.get(c) or {}
            if not q:
                continue
            found += 1
            for k in required + process_required:
                if q.get(k, "") in ("", None):
                    miss[k] += 1

        payload = {
            "ts": int(time.time() * 1000),
            "sample_codes": len(codes),
            "quotes_found": found,
            "missing_count": miss,
            "missing_rate": {
                k: round(miss[k] / max(1, found), 4) for k in miss.keys()
            },
        }
        out_key = f"diag:data_contract:{today_str}"
        await self.redis.hset(out_key, mapping={"ts": payload["ts"], "payload": json.dumps(payload, ensure_ascii=False)})
        await self.redis.expire(out_key, 86400)

        ohlc_missing = max(miss["high"], miss["low"], miss["pre_close"], miss["last_close"])
        if found > 0 and ohlc_missing / found >= 0.3:
            self._log_event(
                "data_contract_warn",
                f"DataContract: OHLC/preclose missing rate high ({ohlc_missing}/{found})",
                level="warning",
                min_interval_sec=300,
                log_on_change=True,
            )
        return payload

    async def build_preopen_plan(
        self,
        today_str: str,
        *,
        auction_items: Optional[List[Dict[str, Any]]] = None,
        strategy_tags: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build pre-open plan after auction snapshot is available."""
        items = auction_items if auction_items is not None else await self._get_auction_top_amount_cached(
            today_str, require_final_0925=False
        )
        if not items:
            return {}

        def _limit_th(code: str) -> float:
            code = str(code or "")
            return 19.8 if code.startswith(("300", "301", "688", "689")) else 9.8

        rows: List[Dict[str, Any]] = []
        for it in items[:300]:
            code = str(it.get("symbol") or "").strip()
            if len(code) != 6:
                continue
            auc = self._normalize_change_pct(it.get("change_pct", 0.0), scale=1.0)
            bid = self._safe_float(it.get("bid_amount_yuan", 0.0), 0.0)
            aamt = self._safe_float(it.get("auction_amount_yuan", 0.0), 0.0)
            seal = bid / max(1.0, aamt)
            rows.append({"code": code, "auc": auc, "bid": bid, "aamt": aamt, "seal": seal})

        if not rows:
            return {}

        up_limit = [x for x in rows if x["auc"] >= _limit_th(x["code"])]
        down_limit = [x for x in rows if x["auc"] <= -_limit_th(x["code"])]
        high_open = [x for x in rows if x["auc"] >= 5.0]
        deep_low = [x for x in rows if x["auc"] <= -8.0]
        top_amount = sorted(rows, key=lambda x: x["aamt"], reverse=True)[:20]
        top_bid = sorted(rows, key=lambda x: x["bid"], reverse=True)[:20]

        plate_stats: Dict[str, Dict[str, float]] = {}
        for x in rows:
            code = x["code"]
            pid = self._get_major_plate(code)
            if not pid:
                continue
            rec = plate_stats.setdefault(pid, {"count": 0.0, "sum_auc": 0.0, "sum_bid": 0.0, "sum_seal": 0.0})
            rec["count"] += 1.0
            rec["sum_auc"] += x["auc"]
            rec["sum_bid"] += x["bid"]
            rec["sum_seal"] += x["seal"]

        plate_view: List[Dict[str, Any]] = []
        for pid, rec in plate_stats.items():
            n = max(1.0, rec["count"])
            score = (rec["sum_bid"] / 100_000_000.0) + (rec["sum_auc"] / n) + (rec["sum_seal"] * 3.0)
            plate_view.append(
                {
                    "plate_id": pid,
                    "plate_name": self.plate_updater.all_plates.get(pid, {}).get("name", pid),
                    "score": round(float(score), 4),
                    "count": int(rec["count"]),
                    "avg_auc_change_pct": round(rec["sum_auc"] / n, 4),
                    "avg_seal_ratio": round(rec["sum_seal"] / n, 4),
                    "sum_bid_yuan": round(rec["sum_bid"], 2),
                }
            )
        plate_view.sort(key=lambda x: x["score"], reverse=True)

        ab = await self.redis.hgetall(f"market:ab_arbitrage:{today_str}")
        aa = self._safe_json_list(ab.get("aa_candidates", "[]"))
        preopen_candidates = aa[:8]
        
        # Inject Sentiment Cycle Context
        sentiment_key = f"market:sentiment:{today_str}"
        sentiment_data = await self.redis.hgetall(sentiment_key)
        cycl_phase = sentiment_data.get("phase", "unknown") if sentiment_data else "unknown"
        
        # Adapt risk thresholds based on market cycle
        min_auc = 3.0
        min_amt = 0.0
        if cycl_phase.lower() in ("ice_point", "retreat"):
            min_auc = 5.0      # 高要求：冷市需要更强竞价确认
            min_amt = 20000000.0 # 过滤诱多假强

        if not preopen_candidates:
            preopen_candidates = [
                {
                    "code": x["code"],
                    "auction_change_pct": round(x["auc"], 3),
                    "auction_seal_ratio": round(x["seal"], 4),
                    "mode": "WATCH",
                }
                for x in sorted(rows, key=lambda y: (y["seal"], y["aamt"]), reverse=True)
                if x["auc"] >= min_auc and x["aamt"] >= min_amt
            ][:8]

        st = strategy_tags if strategy_tags is not None else await self.redis.hgetall(f"market:strategy_tags:{today_str}")
        primary_tag = st.get("primary_tag", "")

        payload = {
            "ts": int(time.time() * 1000),
            "date": today_str,
            "primary_tag": primary_tag,
            "auction_stats": {
                "sample_size": len(rows),
                "up_limit_count": len(up_limit),
                "down_limit_count": len(down_limit),
                "high_open_ge5_count": len(high_open),
                "deep_low_le8_count": len(deep_low),
                "avg_auc_change_pct_top20_amount": round(
                    (sum(x["auc"] for x in top_amount) / max(1, len(top_amount))), 4
                ),
                "avg_seal_ratio_top20_bid": round(
                    (sum(x["seal"] for x in top_bid) / max(1, len(top_bid))), 4
                ),
            },
            "top_plates": plate_view[:5],
            "candidates": preopen_candidates,
        }
        panel_summary = (
            f"tag={primary_tag or 'NA'} "
            f"sample={payload['auction_stats']['sample_size']} "
            f"up={payload['auction_stats']['up_limit_count']} "
            f"down={payload['auction_stats']['down_limit_count']} "
            f"high_open={payload['auction_stats']['high_open_ge5_count']} "
            f"deep_low={payload['auction_stats']['deep_low_le8_count']}"
        )
        out_key = f"market:plan:preopen:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": payload["ts"],
                "primary_tag": primary_tag,
                "summary": panel_summary,
                "candidate_count": len(preopen_candidates),
                "top_plate_1": (payload["top_plates"][0]["plate_name"] if payload["top_plates"] else ""),
                "auction_stats": json.dumps(payload["auction_stats"], ensure_ascii=False),
                "top_plates": json.dumps(payload["top_plates"], ensure_ascii=False),
                "candidates": json.dumps(payload["candidates"], ensure_ascii=False),
                "payload": json.dumps(payload, ensure_ascii=False),
            },
        )
        await self.redis.expire(out_key, 86400)
        self._log_event(
            "preopen_plan",
            f"PreOpenPlan: tag={primary_tag or 'NA'} candidates={len(preopen_candidates)} top_plate={payload['top_plates'][0]['plate_name'] if payload['top_plates'] else 'NA'}",
            min_interval_sec=120,
            log_on_change=True,
        )
        return payload

    async def build_open_verify_plan(
        self,
        today_str: str,
        *,
        expectation_eval: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build open/intraday verify plan converting Emotive Phases into Actionable Scenario/Signal Cards."""
        self.last_open_verify_plan_update = time.time()
        run_id = self._ensure_run_id(today_str, run_id=run_id)
        
        # 1. Load the Universal Truth State Machine
        emotion_raw = await self.redis.hgetall(f"market:emotion_phase:{today_str}")
        if not emotion_raw or "payload" not in emotion_raw:
            logger.warning(f"⚠️ [OpenVerify] 无法获取 EmotionPhaseResult, 退化处理.")
            return {}
            
        try:
            er_dict = json.loads(emotion_raw["payload"])
        except Exception as e:
            logger.error(f"❌ [OpenVerify] EmotionPhase 解析失败: {e}")
            return {}
            
        phase = er_dict.get("emotion_phase", "start")
        allowed = er_dict.get("allowed_setups", [])
        blocked = er_dict.get("blocked_setups", [])
        pos_cap = self._safe_float(er_dict.get("position_cap", 0.0))
        penalty = self._safe_float(er_dict.get("global_fakeout_penalty", 0.0))
        leaders = er_dict.get("leader_candidates", [])
        plate_phase_map = er_dict.get("plate_phase_map", {}) or {}
        plate_phase_detail_raw = await self.redis.hgetall(f"market:plate_phase_detail:{today_str}") or {}
        plate_phase_detail = {k: self._safe_json_dict(v) for k, v in plate_phase_detail_raw.items()}
        plate_phase_conf_by_name = {
            str(v.get("plate_name", "")): self._safe_float(v.get("confidence", 0.0), 0.0)
            for v in plate_phase_detail.values()
            if isinstance(v, dict) and v.get("plate_name")
        }
        pattern_matrix = await self._load_pattern_matrix(today_str)
        if not pattern_matrix:
            pattern_matrix = await self._build_pattern_matrix(today_str, market_phase=phase, plate_phase_map=plate_phase_map)
            await self._store_pattern_matrix(today_str, pattern_matrix)

        # 1.1 Load Expectation Eval from Redis if missing (Intraday Fallback)
        if expectation_eval is None:
            try:
                ee_raw = await self.redis.hgetall(f"diag:expectation_eval:{today_str}")
                if ee_raw:
                    expectation_eval = {
                        k: (float(v) if '.' in v else int(v)) if isinstance(v, str) and v.replace('.','',1).isdigit() else v
                        for k, v in ee_raw.items()
                    }
            except Exception:
                pass
        
        # 1.2 Load Expectation State Summary (for W2S scenarios)
        expectation_summary = {}
        try:
            es_raw = await self.redis.hgetall(f"market:expectation_state:{today_str}")
            if es_raw and "payload" in es_raw:
                expectation_summary = json.loads(es_raw["payload"])
        except Exception:
            pass

        # Ensure fallback defaults for risk indicators
        ev = expectation_eval or {}
        fade_count = self._safe_int(ev.get("fade_count", 0), 0)
        rise_count = self._safe_int(ev.get("rise_count", 0), 0)
        one_word_break_rate = self._safe_float(ev.get("one_word_break_rate", 0.0), 0.0)
        hold_rate = self._safe_float(ev.get("strong_high_hold_rate", 0.5), 0.5)
        rebound_rate = self._safe_float(ev.get("extreme_low_rebound_rate", 0.0), 0.0)
        eff = self._safe_float(ev.get("effectiveness", 0.5), 0.5)

        # Load supporting data needed for Scoring
        ab = await self.redis.hgetall(f"market:ab_arbitrage:{today_str}")
        advice_raw = await self.redis.hgetall(f"market:operator_advice:{today_str}")
        
        scenario_cards: List[Dict[str, Any]] = []
        signal_cards: List[Dict[str, Any]] = []
        now_ts = int(time.time() * 1000)

        # --------------------------------------------------------------------------------
        # 场景 A: 前排一字加速的板块内“高切低” (low_level_relay)
        # --------------------------------------------------------------------------------
        if "low_level_relay" in allowed and phase in ("climax", "start"):
            pairs = self._safe_json_list(ab.get("pairs", "[]"))
            target_signals = []
            
            for p in pairs[:5]:
                # 提取 B 的打分与 A 的带动
                score_a = self._safe_float(p.get("a_score", 0.0), 0.0)
                score_b = self._safe_float(p.get("b_score", 0.0), 0.0)
                
                plate_id = self._get_major_plate(p.get("b_code", ""))
                plate_name = self._get_plate_name(plate_id)
                plate_phase = plate_phase_map.get(plate_id, "start")
                await self._ensure_plate_snapshot(
                    today_str,
                    plate_id,
                    run_id,
                    module="build_open_verify_plan",
                    is_consumed_plate=True,
                )
                matrix_info = self._resolve_setup_matrix_weight(
                    pattern_matrix,
                    market_phase=phase,
                    plate_phase=plate_phase,
                    role_type="relay_candidate",
                    setup_type="LOW_LEVEL_RELAY",
                )
                sig_score = (0.35 * score_a) + (0.25 * score_b) + (0.15 * 0.8) + (0.15 * 0.8) - (0.10 * penalty)
                sig_score = max(0.0, round(sig_score * 100, 2))
                
                if sig_score < 60:
                    continue
                    
                suggested_pos = min(pos_cap, 0.20) if sig_score >= 80 else min(pos_cap, 0.10)
                
                sc = SignalCard(
                    signal_id=f"sig_ll_relay_{p.get('b_code')}",
                    scenario_type="low_level_relay",
                    code=p.get('b_code', ''),
                    name=self._get_stock_name(p.get('b_code', '')),
                    theme=plate_name,
                    role_type="relay_candidate",
                    board_position_rank=3,
                    signal_score=sig_score,
                    confidence=self._safe_float(p.get("confidence", 0.5)),
                    suggested_position=suggested_pos,
                    entry_hint={"condition": "龙头未炸板，B票出现承接买盘放量"},
                    exit_plan={
                        "failed_expectation": "龙头炸板或板块跳水",
                        "stop_loss": "-3.0%",
                        "take_profit_half": "+6.0%",
                        "take_profit_all": "+10.0%",
                        "timeout": "45m_no_break"
                    },
                    invalid_after_ts=now_ts + (45 * 60 * 1000),
                    reason=f"A股强封印证板块资金，回流中军/补涨，防偏惩罚-{penalty}",
                    risk_flags=["A炸板风险", "板块轮动超时"],
                    chip_context={"chip_zone_status": "unknown"},
                    market_phase=phase,
                    plate_phase=plate_phase,
                    plate_phase_confidence=plate_phase_conf_by_name.get(plate_name, 0.0),
                    setup_type="LOW_LEVEL_RELAY",
                    setup_matrix_weight=matrix_info["weight"],
                    setup_confidence=matrix_info["confidence"],
                )
                sc_dict = sc.to_dict()
                input_bundle = {
                    "a_score": score_a,
                    "b_score": score_b,
                    "penalty": penalty,
                    "position_cap": pos_cap,
                    "setup_matrix_weight": matrix_info["weight"],
                    "plate_phase_confidence": plate_phase_conf_by_name.get(plate_name, 0.0),
                }
                await self._emit_snapshot(
                    self._build_signal_snapshot(
                        today_str=today_str,
                        run_id=run_id,
                        signal_card=sc_dict,
                        primary_plate_id=plate_id,
                        primary_plate_name=plate_name,
                        input_bundle=input_bundle,
                    )
                )
                target_signals.append(sc_dict)
                
            if target_signals:
                card = ScenarioCard(
                    scenario_id=f"scn_ll_relay_{today_str}",
                    scenario_type="low_level_relay",
                    title="🔥 主升/高潮期: 板块高度确认，发掘核心低位补涨",
                    priority=1,
                    confidence=0.8,
                    trigger_conditions={"phase_in": ["climax"], "leader_sealed": True},
                    cancel_condition={"leader_crack": True, "plate_sharp_drop": True},
                    invalid_after_ts=now_ts + (120 * 60 * 1000),
                    risk_cut_trigger={"drop_from_entry": -0.03},
                    candidate_codes=[x["code"] for x in target_signals],
                    candidate_roles=leaders,
                    position_hint={"max_total_cap": pos_cap},
                    phase_binding=phase,
                    notes="严禁去排队买买不到的一字板龙头，寻找后排有承接的换手板。"
                )
                scenario_cards.append(asdict(card))
                signal_cards.extend(target_signals)

        # --------------------------------------------------------------------------------
        # 场景 B: 首次大分歧的“核心深水低吸” (core_dip_buying)
        # --------------------------------------------------------------------------------
        if "core_dip_buying" in allowed and phase == "divergence":
            aa = self._safe_json_list(ab.get("aa_candidates", "[]"))
            target_signals = []
            
            # Initiate K-Line Service for Capacity Core Check
            from web.services.stock_kline_service import StockKLineService
            kline_svc = StockKLineService()
            
            for a in aa[:3]:
                # 仅触碰 Leader/CoreAnchor (模拟实现: A直做列表里的顶部核心)
                if self._safe_float(a.get("score"), 0.0) < 0.6: continue
                
                code = a.get("code", "")
                # --- P5: Capacity Core Constraint (15亿中军验证) ---
                k_data = kline_svc.fetch_kline_data(code, frequency="d", start_date=None, end_date=today_str)
                if len(k_data) >= 2:
                    yesterday_k = k_data[-2] if k_data[-1].get('time') == today_str else k_data[-1]
                    yesterday_amount = self._safe_float(yesterday_k.get('amount', 0), 0.0)
                    if yesterday_amount < 1500000000.0:  # 必须大于 15 亿
                        continue  # 情感投机/容量不足，在退潮/分歧期拒绝接飞刀
                # ---------------------------------------------------
                    
                role_type = "core_anchor"
                plate_id = self._get_major_plate(code)
                plate_name = self._get_plate_name(plate_id)
                plate_phase = plate_phase_map.get(plate_id, "divergence")
                await self._ensure_plate_snapshot(
                    today_str,
                    plate_id,
                    run_id,
                    module="build_open_verify_plan",
                    is_consumed_plate=True,
                )
                matrix_info = self._resolve_setup_matrix_weight(
                    pattern_matrix,
                    market_phase=phase,
                    plate_phase=plate_phase,
                    role_type=role_type,
                    setup_type="CORE_DIP_BUYING",
                )
                sig_score = max(0.0, round(((0.6 * self._safe_float(a.get("score")) + 0.4) - (0.10 * penalty)) * 100, 2))
                suggested_pos = min(pos_cap, 0.30) if sig_score >= 80 else min(pos_cap, 0.15)
                
                sc = SignalCard(
                    signal_id=f"sig_core_dip_{a.get('code')}",
                    scenario_type="core_dip_buying",
                    code=a.get('code', ''),
                    name=self._get_stock_name(a.get('code', '')),
                    theme=plate_name,
                    role_type=role_type,
                    board_position_rank=1,
                    signal_score=sig_score,
                    confidence=self._safe_float(a.get("confidence", 0.5)),
                    suggested_position=suggested_pos,
                    entry_hint={"condition": "开盘/早盘下探深水区(-3%~-8%)，且抛压衰竭量缩放缓"},
                    exit_plan={
                        "failed_expectation": "下杀超过半小时无量且持续暴跌",
                        "stop_loss": "-5.0%",
                        "take_profit_half": "+5.0%",
                        "timeout": "afternoon_no_recovery"  # 午后无效
                    },
                    invalid_after_ts=int(datetime.now().replace(hour=13, minute=0, second=0).timestamp() * 1000), # 13:00 作废
                    reason=f"首次分歧核心承接，拒绝跟风，防骗惩罚-{penalty} [大容量中军确认>15亿]",
                    risk_flags=["瀑布杀量未尽", "板块彻底瓦解"],
                    chip_context={"chip_zone_status": "safety_near_peak"},
                    market_phase=phase,
                    plate_phase=plate_phase,
                    plate_phase_confidence=plate_phase_conf_by_name.get(plate_name, 0.0),
                    setup_type="CORE_DIP_BUYING",
                    setup_matrix_weight=matrix_info["weight"],
                    setup_confidence=matrix_info["confidence"],
                )
                sc_dict = sc.to_dict()
                input_bundle = {
                    "a_score": self._safe_float(a.get("score", 0.0), 0.0),
                    "penalty": penalty,
                    "position_cap": pos_cap,
                    "setup_matrix_weight": matrix_info["weight"],
                    "plate_phase_confidence": plate_phase_conf_by_name.get(plate_name, 0.0),
                }
                await self._emit_snapshot(
                    self._build_signal_snapshot(
                        today_str=today_str,
                        run_id=run_id,
                        signal_card=sc_dict,
                        primary_plate_id=plate_id,
                        primary_plate_name=plate_name,
                        input_bundle=input_bundle,
                    )
                )
                target_signals.append(sc_dict)
                
            if target_signals:
                card = ScenarioCard(
                    scenario_id=f"scn_core_dip_{today_str}",
                    scenario_type="core_dip_buying",
                    title="🌊 首强分歧期: 放弃跟风，聚焦绝对龙头/中军深水低吸",
                    priority=2,
                    confidence=0.75,
                    trigger_conditions={"phase_in": ["divergence"], "core_deep_water": True},
                    cancel_condition={"continuous_heavy_selling": True},
                    invalid_after_ts=int(datetime.now().replace(hour=11, minute=30, second=0).timestamp() * 1000), # 午前收盘作废
                    risk_cut_trigger={"drop_from_entry": -0.05},
                    candidate_codes=[x["code"] for x in target_signals],
                    candidate_roles=leaders,
                    position_hint={"max_total_cap": pos_cap},
                    phase_binding=phase,
                    notes="仅限辨识度最高的前2名中军核心，任何无地位跟风一律屏蔽。"
                )
                scenario_cards.append(asdict(card))
                signal_cards.extend(target_signals)

        # --------------------------------------------------------------------------------
        # 场景 C: 弱转强核心修复 (weak_to_strong_repair)
        # P2 Formalization: Filter 2 (Confirmed Open + Phase Restraints)
        # --------------------------------------------------------------------------------
        if "weak_to_strong_repair" in allowed:
            w2s_top_codes = expectation_summary.get("weak_to_strong_top", [])[:5]
            w2s_details = expectation_summary.get("details_by_code", {})
            target_signals = []
            
            # --- P4 Fallback Strategy ---
            # 1. 首轮严格筛选: 过滤符合绝对阈值 (Weak to Strong Confirmed) 的标的
            valid_codes = [c for c in w2s_top_codes if w2s_details.get(c, {}).get("label") == "weak_to_strong_confirmed"]
            
            # 2. 次轮兜底筛选 (TOP 策略): 如果所有票都达不到严格标准 (全军覆没), 则启动相对模式抓取最强前沿
            is_fallback = False
            if not valid_codes and w2s_top_codes:
                for c in w2s_top_codes:
                    if w2s_details.get(c, {}).get("total_score", 0.0) >= 0.50:
                        valid_codes.append(c)
                        is_fallback = True
                        break  # 兜底模式下，只选相对第一名的一只票作为活口
                        
            for code in valid_codes:
                prof = w2s_details.get(code, {})
                
                # Fetch quote to verify it hasn't collapsed after expectation analysis
                q = await self.redis.hgetall(f"stock:quote:{code}") or {}
                open_change = self._safe_float(q.get("change_pct") or q.get("change", 0.0), 0.0)
                auction_change = self._safe_float(prof.get("metrics", {}).get("auction_change", 0.0), 0.0)
                
                # Confirm it is holding the weak-to-strong transition dynamically
                # 兜底模式下放宽 0.5% 的回撤容忍度
                drop_limit = 2.5 if is_fallback else 2.0
                if open_change < auction_change - drop_limit:
                    continue  # Failed intraday hold
                    
                plate_id = self._get_major_plate(code)
                plate_name = self._get_plate_name(plate_id)
                plate_phase = plate_phase_map.get(plate_id, "start")

                await self._ensure_plate_snapshot(
                    today_str,
                    plate_id,
                    run_id,
                    module="build_open_verify_plan",
                    is_consumed_plate=True,
                )
                matrix_info = self._resolve_setup_matrix_weight(
                    pattern_matrix,
                    market_phase=phase,
                    plate_phase=plate_phase,
                    role_type="repair_candidate",
                    setup_type="WEAK_TO_STRONG_REPAIR",
                )
                
                sig_score = min(100.0, max(0.0, prof.get("total_score", 0.5) * 100.0))
                suggested_pos = min(pos_cap, 0.25) if sig_score >= 80 else min(pos_cap, 0.15)
                
                # Dynamic Risk Flags
                d_flags = ["早盘即遭爆量砸盘"]
                if is_fallback:
                    d_flags.append("退守TOP战略(相对强度兜底)，谨防诱多")
                y_extra = prof.get('y_profile', {}).get('extra', {})
                if y_extra.get('institutional_dump'):
                    d_flags.append(f"昨日大单爆砸{self._safe_float(y_extra.get('y_net_flow', 0))/100000000:.1f}亿, 防抛压骤降")
                
                sc = SignalCard(
                    signal_id=f"sig_w2s_repair_{code}",
                    scenario_type="weak_to_strong_repair",
                    code=code,
                    name=self._get_stock_name(code),
                    theme=plate_name,
                    role_type="repair_candidate",
                    board_position_rank=1,
                    signal_score=sig_score,
                    confidence=0.75,
                    suggested_position=suggested_pos,
                    entry_hint={"condition": "弱转强开盘确认，且盘中缩量抗跌，企稳半小时以上可低吸"},
                    exit_plan={
                        "failed_expectation": "跌破竞价涨幅甚至翻绿",
                        "stop_loss": "-4.0%",
                        "take_profit_half": "+5.0%"
                    },
                    invalid_after_ts=int(datetime.now().replace(hour=10, minute=30, second=0).timestamp() * 1000),
                    reason=f"昨日弱势被强力修正({prof.get('y_profile', {}).get('state_type')}->W2S)，情绪反转确认",
                    risk_flags=d_flags,
                    chip_context={"chip_zone_status": "rebound_from_support"},
                    market_phase=phase,
                    plate_phase=plate_phase,
                    plate_phase_confidence=plate_phase_conf_by_name.get(plate_name, 0.0),
                    setup_type="WEAK_TO_STRONG_REPAIR",
                    setup_matrix_weight=matrix_info["weight"],
                    setup_confidence=matrix_info["confidence"],
                )
                sc_dict = sc.to_dict()
                input_bundle = {
                    "w2s_score": prof.get("total_score", 0.0),
                    "penalty": penalty,
                    "position_cap": pos_cap,
                    "setup_matrix_weight": matrix_info["weight"],
                    "plate_phase_confidence": plate_phase_conf_by_name.get(plate_name, 0.0),
                }
                await self._emit_snapshot(
                    self._build_signal_snapshot(
                        today_str=today_str,
                        run_id=run_id,
                        signal_card=sc_dict,
                        primary_plate_id=plate_id,
                        primary_plate_name=plate_name,
                        input_bundle=input_bundle,
                    )
                )
                target_signals.append(sc_dict)
                
            if target_signals:
                card = ScenarioCard(
                    scenario_id=f"scn_w2s_repair_{today_str}",
                    scenario_type="weak_to_strong_repair",
                    title="🔥 情绪冰点/启动期: 弱转强节点修复确认",
                    priority=3,
                    confidence=0.70,
                    trigger_conditions={"phase_in": ["ice_point", "start"], "w2s_repair_confirmed": True},
                    cancel_condition={"sharp_opening_drop": True},
                    invalid_after_ts=int(datetime.now().replace(hour=10, minute=30, second=0).timestamp() * 1000),
                    risk_cut_trigger={"drop_from_entry": -0.04},
                    candidate_codes=[x["code"] for x in target_signals],
                    candidate_roles=leaders,
                    position_hint={"max_total_cap": pos_cap},
                    phase_binding=phase,
                    notes="严禁无脑追高，开盘15分钟若跌破竞价且无承接应立刻放弃。"
                )
                scenario_cards.append(asdict(card))
                signal_cards.extend(target_signals)

        # --------------------------------------------------------------------------------
        # 场景 D: 核心容量趋势 (core_trend)
        # P3 Formalization: MACD/RSI/MA Technical Filtering
        # --------------------------------------------------------------------------------
        if "core_trend" in allowed and phase == "climax":
            # fetch potential candidates from previously loaded 'ab' dict
            tc_candidates = self._safe_json_list(ab.get("aa_candidates", "[]"))
            trend_candidates = [
                x for x in tc_candidates
                if self._safe_float(x.get("score"), 0.0) >= 0.65 
                and self._get_major_plate(x.get("code", "")) != ""
            ][:3]
            
            target_signals = []
            if trend_candidates:
                from web.services.stock_kline_service import StockKLineService
                kline_svc = StockKLineService()
                
                for tc in trend_candidates:
                    code = tc.get("code", "")
                    
                    # Fetch recent K-lines (60 days is enough for MACD/MA30)
                    k_data = kline_svc.fetch_kline_data(code, frequency="d", start_date=None, end_date=today_str)
                    
                    # --- P5: Capacity Core Constraint (15亿中军验证) ---
                    if len(k_data) >= 2:
                        yesterday_k = k_data[-2] if k_data[-1].get('time') == today_str else k_data[-1]
                        yesterday_amount = self._safe_float(yesterday_k.get('amount', 0), 0.0)
                        if yesterday_amount < 1500000000.0:  # 必须大于 15 亿
                            continue  # 主升期拒绝缩量/小市值逼空陷阱，只做大票
                    # ---------------------------------------------------
                    
                    tech_inds = kline_svc.calculate_technical_indicators(k_data)
                    
                    macd_data = tech_inds.get("macd", [])
                    ma_data = tech_inds.get("ma", [])
                    rsi_data = tech_inds.get("rsi", [])
                    
                    # Filter logic: MACD is positive (upward trend), MA5 > MA10
                    if not macd_data or not ma_data or not rsi_data:
                        continue
                        
                    latest_macd = macd_data[-1]
                    latest_ma = ma_data[-1]
                    latest_rsi = rsi_data[-1]
                    
                    # Check MACD
                    macd_hist = self._safe_float(latest_macd.get("macd"), 0.0)
                    if macd_hist < 0: continue # Requires upward momentum
                    
                    # Check MA
                    ma5 = self._safe_float(latest_ma.get("ma5"), 0.0)
                    ma10 = self._safe_float(latest_ma.get("ma10"), 0.0)
                    ma20 = self._safe_float(latest_ma.get("ma20"), 0.0)
                    if not (ma5 > ma10 and ma5 > ma20): continue # Requires basic upward MA sequence
                    
                    # Check RSI (not overbought yet)
                    rsi_val = self._safe_float(latest_rsi.get("rsi"), 50.0)
                    if rsi_val > 85.0: continue # Too overbought
                    
                    role_type = "core_trend"
                    plate_id = self._get_major_plate(code)
                    plate_name = self._get_plate_name(plate_id)
                    plate_phase = plate_phase_map.get(plate_id, "climax")
                    
                    await self._ensure_plate_snapshot(
                        today_str,
                        plate_id,
                        run_id,
                        module="build_open_verify_plan",
                        is_consumed_plate=True,
                    )
                    matrix_info = self._resolve_setup_matrix_weight(
                        pattern_matrix,
                        market_phase=phase,
                        plate_phase=plate_phase,
                        role_type=role_type,
                        setup_type="CORE_TREND",
                    )
                    
                    sig_score = min(100.0, max(0.0, self._safe_float(tc.get("score", 0.0)) * 100.0 + 10.0))
                    suggested_pos = min(pos_cap, 0.35) # Core trend allows slightly higher holding
                    
                    sc = SignalCard(
                        signal_id=f"sig_core_trend_{code}",
                        scenario_type="core_trend",
                        code=code,
                        name=self._get_stock_name(code),
                        theme=plate_name,
                        role_type=role_type,
                        board_position_rank=2,
                        signal_score=sig_score,
                        confidence=0.85,
                        suggested_position=suggested_pos,
                        entry_hint={"condition": "均线多头排列且MACD红柱，沿5日线低吸，回避加速缩量秒板"},
                        exit_plan={
                            "failed_expectation": "放量跌破10日线且形态破坏",
                            "stop_loss": "-5.0%",
                            "take_profit_half": "+8.0%",
                            "timeout": "hold_until_trend_breaks"
                        },
                        invalid_after_ts=int(datetime.now().replace(hour=14, minute=30, second=0).timestamp() * 1000),
                        reason=f"主升期核心容量趋势确认 (MACD>0, MA5>MA10, RSI={rsi_val:.1f}) [中军容量>15亿]",
                        risk_flags=["板块高潮次日接力风险", "极端加速缩量风险"],
                        chip_context={"chip_zone_status": "trend_support"},
                        market_phase=phase,
                        plate_phase=plate_phase,
                        plate_phase_confidence=plate_phase_conf_by_name.get(plate_name, 0.0),
                        setup_type="CORE_TREND",
                        setup_matrix_weight=matrix_info["weight"],
                        setup_confidence=matrix_info["confidence"],
                    )
                    sc_dict = sc.to_dict()
                    input_bundle = {
                        "trend_score": tc.get("score"),
                        "rsi": rsi_val,
                        "macd": macd_hist,
                        "position_cap": pos_cap,
                        "setup_matrix_weight": matrix_info["weight"],
                        "plate_phase_confidence": plate_phase_conf_by_name.get(plate_name, 0.0),
                    }
                    await self._emit_snapshot(
                        self._build_signal_snapshot(
                            today_str=today_str,
                            run_id=run_id,
                            signal_card=sc_dict,
                            primary_plate_id=plate_id,
                            primary_plate_name=plate_name,
                            input_bundle=input_bundle,
                        )
                    )
                    target_signals.append(sc_dict)
                    
            if target_signals:
                card = ScenarioCard(
                    scenario_id=f"scn_core_trend_{today_str}",
                    scenario_type="core_trend",
                    title="📈 主升期: 核心容量趋势确认",
                    priority=2,
                    confidence=0.80,
                    trigger_conditions={"phase_in": ["climax"], "technical_support": True},
                    cancel_condition={"market_wide_dump": True},
                    invalid_after_ts=int(datetime.now().replace(hour=14, minute=50, second=0).timestamp() * 1000),
                    risk_cut_trigger={"drop_from_entry": -0.05},
                    candidate_codes=[x["code"] for x in target_signals],
                    candidate_roles=leaders,
                    position_hint={"max_total_cap": pos_cap},
                    phase_binding=phase,
                    notes="聚焦逻辑最正的核心中军，围绕均线做波段，切忌连板接力手法。"
                )
                scenario_cards.append(asdict(card))
                signal_cards.extend(target_signals)

        aa = self._safe_json_list(ab.get("aa_candidates", "[]"))
        pairs = self._safe_json_list(ab.get("pairs", "[]"))
        buy_list = [
            x for x in aa
            if str(x.get("confidence", "")) in ("high", "medium")
            and self._safe_float(x.get("current_change_pct", 0.0), 0.0) <= 8.0
        ][:5]
        watch_list = pairs[:5]

        avoid_plates: List[str] = []
        reasons: List[str] = []
        try:
            payload_raw = advice_raw.get("payload", "")
            if payload_raw:
                ap = json.loads(payload_raw)
                if isinstance(ap, dict):
                    avoid_plates = [str(x) for x in (ap.get("avoid_plates", []) or [])][:5]
                    reasons = [str(x) for x in (ap.get("reason", []) or [])][:5]
        except Exception:
            pass

        risk_flags: List[str] = []
        if fade_count > rise_count:
            risk_flags.append("fade_dominant")
        if one_word_break_rate >= 0.35:
            risk_flags.append("one_word_break")
        if hold_rate < 0.45:
            risk_flags.append("weak_hold")
        if rebound_rate < 0.20 and eff < 0.45:
            risk_flags.append("weak_rebound")

        payload = {
            "ts": now_ts,
            "date": today_str,
            "emotion_phase": phase,
            "position_cap": pos_cap,
            "allowed_setups": allowed,
            "blocked_setups": blocked,
            "scenario_cards": scenario_cards,
            "signal_cards": signal_cards
        }

        await self._record_pattern_signals(today_str, signal_cards)
        pattern_matrix = await self._build_pattern_matrix(today_str, market_phase=phase, plate_phase_map=plate_phase_map)
        await self._store_pattern_matrix(today_str, pattern_matrix)
        payload["pattern_matrix"] = pattern_matrix
        
        panel_summary = (
            f"Phase={phase} "
            f"Cap={pos_cap:.1f} "
            f"Cards={len(scenario_cards)} "
            f"Signals={len(signal_cards)}"
        )
        
        out_key = f"market:plan:open_verify:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": payload["ts"],
                "emotion_phase": phase,
                "summary": panel_summary,
                "card_count": len(scenario_cards),
                "signal_count": len(signal_cards),
                "scenario_cards": json.dumps(scenario_cards, ensure_ascii=False),
                "signal_cards": json.dumps(signal_cards, ensure_ascii=False),
                "payload": json.dumps(payload, ensure_ascii=False),
            },
        )
        await self.redis.expire(out_key, 86400)
        self._log_event(
            "open_verify_plan",
            f"🃏 场景派发: {phase} | 允许={','.join(allowed)} | Cards={len(scenario_cards)} Signals={len(signal_cards)}",
            min_interval_sec=120,
            log_on_change=True,
        )
        return payload

    async def calculate_execution_policy(self, today_str: str, run_id: Optional[str] = None) -> None:
        ts = int(time.time() * 1000)
        stale = False
        run_id = self._ensure_run_id(today_str, run_id=run_id)

        # --- 核心改动：全盘接管自上游“真相源” EmotionPhaseResult ---
        phase = "start"
        position_max = 0.0
        mode_allow = []
        ban_conditions = []
        blocked_setups: List[str] = []
        pattern_matrix: Dict[str, Any] = {}
        
        try:
            emotion_raw = await self.redis.hgetall(f"market:emotion_phase:{today_str}")
            if emotion_raw and "payload" in emotion_raw:
                er_dict = json.loads(emotion_raw["payload"])
                phase = er_dict.get("emotion_phase", "start")
                position_max = self._safe_float(er_dict.get("position_cap", 0.0))
                mode_allow = er_dict.get("allowed_setups", [])
                
                # P1: Explicit Phase-Pattern Binding Supplemental Append
                mapped_modes = self.PHASE_MODE_MAPPING.get(phase, [])
                for m in mapped_modes:
                    ml = m.lower()
                    if ml not in mode_allow:
                        mode_allow.append(ml)
                
                # 将 blocked_setups 转化为 ban_conditions 供系统拦截
                blocked_setups = er_dict.get("blocked_setups", [])
                for b in blocked_setups:
                    ban_conditions.append(f"blocked_by_phase:{b}")
            else:
                stale = True
        except Exception as e:
            logger.error(f"❌ [ExecutionPolicy] 无法解析 EmotionPhaseResult: {e}")
            stale = True

        try:
            pattern_matrix = await self._load_pattern_matrix(today_str)
            if not pattern_matrix:
                pattern_matrix = await self._build_pattern_matrix(today_str, market_phase=phase)
                await self._store_pattern_matrix(today_str, pattern_matrix)
        except Exception:
            pattern_matrix = {}

        # --- 二级修正数据拾取 (仅用于在基准 position_max 上做减法) ---
        fear_greed: Dict[str, Any] = {}
        resonance: Dict[str, Any] = {}
        process_profile: Dict[str, Any] = {}
        strategy_tags: Dict[str, Any] = {}

        try:
            fear_greed = await self.redis.hgetall(f"market:fear_greed:{today_str}")
            resonance = await self.redis.hgetall(f"market:resonance:{today_str}")
            process_profile = await self.redis.hgetall(f"market:process_profile:{today_str}")
            strategy_tags = await self.redis.hgetall(f"market:strategy_tags:{today_str}")
        except Exception:
            stale = True

        fear_greed_score = self._safe_float(fear_greed.get("score", 0.5), 0.5)
        extreme_greed = self._safe_int(fear_greed.get("extreme_greed", 0), 0)
        extreme_fear = self._safe_int(fear_greed.get("extreme_fear", 0), 0)
        
        resonance_score = self._safe_float(resonance.get("score", 0.5), 0.5)
        resonance_state = resonance.get("state", "neutral")
        
        process_state = process_profile.get("state", "mixed")
        process_score = self._safe_float(process_profile.get("score", 0.5), 0.5)
        process_risk_strength = self._safe_float(process_profile.get("risk_strength", 0.0), 0.0)

        try:
            expectation_eval = await self.redis.hgetall(f"diag:expectation_eval:{today_str}")
        except Exception:
            expectation_eval = {}

        expectation_sample_size = self._safe_int(expectation_eval.get("sample_size", 0), 0)
        expectation_eff = self._safe_float(expectation_eval.get("effectiveness", 0.0), 0.0)

        strategy_primary_tag = strategy_tags.get("primary_tag", "")

        plate_attitude_bias = 0.0
        danger_key = f"rank:danger:{today_str}"
        try:
            danger_count = await self.redis.zcard(danger_key)
        except Exception:
            danger_count = 0
            stale = True

        if danger_count >= 20:
            position_max = min(position_max, 0.2)
            ban_conditions.append('danger_count>=20')

        # 板块态度的保守修正
        if plate_attitude_bias <= -2.0:
            position_max = min(position_max, 0.2)
            ban_conditions.append('plate_attitude_negative')

        # 情绪与拥挤修正
        if extreme_greed and resonance_score >= 0.7:
            position_max = min(position_max, 0.2)
            ban_conditions.append("extreme_greed_crowded")
            
        # 过程画像直接影响仓位上限
        if process_state == "risk_off" or process_risk_strength >= 0.5:
            position_max = min(position_max, 0.2)
            ban_conditions.append("process_risk_off")

        # 宽基普跌修正
        market_overview_key = f"market:overview:{today_str}"
        try:
            overview_data = await self.redis.hgetall(market_overview_key)
            if overview_data:
                up_count = self._safe_int(overview_data.get("up_count", 0), 0)
                down_count = self._safe_int(overview_data.get("down_count", 0), 0)
                avg_change = self._safe_float(overview_data.get("avg_change_pct", 0), 0.0)
                up_down_ratio = up_count / max(1, down_count)
                
                # 弱势市场修正
                if up_down_ratio < 0.5 and avg_change <= -1.0:
                    position_max = min(position_max, 0.15)
                    if "market_breadth_weak" not in ban_conditions:
                        ban_conditions.append("market_breadth_weak")
        except Exception:
            pass

        if position_max <= 0.1 and not mode_allow:
            mode_allow = ['wait']

        active_setup_weights: Dict[str, float] = {}
        for setup in mode_allow:
            setup_key = str(setup or "").upper()
            resolved = self._resolve_setup_matrix_weight(
                pattern_matrix,
                market_phase=phase,
                plate_phase="climax",
                role_type="core_anchor",
                setup_type=setup_key,
            ) if pattern_matrix else {"weight": 0.0, "allowed": False}
            active_setup_weights[setup_key] = self._safe_float(resolved.get("weight", 0.0), 0.0)

        if active_setup_weights:
            avg_setup_weight = float(np.mean(list(active_setup_weights.values())))
        else:
            avg_setup_weight = 0.0

        policy = ExecutionPolicy(
            ts=ts,
            position_max=float(position_max),
            mode_allow=mode_allow,
            candidate_pool_key=f"cache:candidate_pool:{today_str}",
            ban_conditions=ban_conditions,
            risk_budget={"max_daily_drawdown": 0.01, "max_attempts": 3},
            explain={
                "stale": stale,
                "phase": phase,
                "danger_count": danger_count,
                "plate_attitude_bias": round(plate_attitude_bias, 4),
                "fear_greed_score": round(fear_greed_score, 4),
                "resonance_score": round(resonance_score, 4),
                "process_risk_strength": round(process_risk_strength, 4),
                "strategy_primary_tag": strategy_primary_tag,
                "pattern_setup_weights": active_setup_weights,
                "pattern_avg_setup_weight": round(avg_setup_weight, 4),
                "pattern_matrix_observation_only": True,
                "expectation_sample_size": 0,
                "expectation_effectiveness": 0.0,
            },
        )

        out_key = f"market:execution_policy:{today_str}"
        await self.redis.hset(out_key, mapping={
            "ts": policy.ts,
            "position_max": policy.position_max,
            "mode_allow": json.dumps(policy.mode_allow, ensure_ascii=False),
            "candidate_pool_key": policy.candidate_pool_key,
            "ban_conditions": json.dumps(policy.ban_conditions, ensure_ascii=False),
            "risk_budget": json.dumps(policy.risk_budget, ensure_ascii=False),
            "explain": json.dumps(policy.explain, ensure_ascii=False),
        })
        await self.redis.expire(out_key, 86400)
        
        self._log_event("policy_status", f"🛡️ 执行锁更新: Phase={phase}, MAX_POS={policy.position_max:.2f}, Allows={','.join(mode_allow)}", min_interval_sec=120)

        # 平滑与粘滞机制 (Hysteresis & Smoothing)
        if self.last_position_max > 0:
            smoothed_position = 0.7 * self.last_position_max + 0.3 * position_max
            position_max = round(smoothed_position * 20) / 20.0
        self.last_position_max = position_max
        
        long_plate_suggestions = []
        avoid_plate_suggestions = []

        # 交易建议
        action = "WAIT"
        risk_level = "MEDIUM"
        reason = []
        if position_max <= 0.05:
            action = "CLOSE"
            risk_level = "HIGH"
            reason.append("策略仓位接近0，绝对防守")
        elif position_max < 0.25:
            action = "REDUCE"
            risk_level = "HIGH" if danger_count >= 20 else "MEDIUM"
            reason.append("仓位上限偏低，控制回撤")
        elif len(mode_allow) > 0:
            action = "OPEN"
            risk_level = "LOW"
            reason.append(f"情绪环境许可 {','.join(mode_allow)}，严格按仓位上限开仓")
        else:
            reason.append("允许模式列表为空，等待发牌")

        # Action Hysteresis (防止临界震荡)
        if self.last_trading_action == "OPEN" and action in ("WAIT", "REDUCE") and position_max >= 0.2:
            self.action_stable_count += 1
            if self.action_stable_count < 3:
                action = "OPEN"
                reason.append("(粘滞平滑: 维持近期OPEN建议)")
        elif self.last_trading_action in ("CLOSE", "REDUCE") and action == "OPEN" and position_max <= 0.35:
             self.action_stable_count += 1
             if self.action_stable_count < 3:
                 action = self.last_trading_action
                 reason.append(f"(粘滞平滑: 维持近期{action}建议, 需更多一致性确认开仓)")
        else:
             self.action_stable_count = 0
             
        self.last_trading_action = action

        if "extreme_greed_crowded" in ban_conditions:
            reason.append("极端贪婪且拥挤，防冲高回落")
        if "plate_attitude_negative" in ban_conditions:
            reason.append("板块态度偏弱，规避弱势板块")
        if "process_risk_off" in ban_conditions:
            reason.append("过程画像风险偏高（爆头率高/修复弱），降低仓位")
        if "expectation_ineffective" in ban_conditions:
            reason.append("竞价预期差近期有效性偏低，降低追价力度")
        elif expectation_sample_size >= 60 and expectation_eff >= 0.6:
            reason.append("竞价预期差有效性较高，可适度提高试错")
        # 联动加载竞价画像状态 (verification_status)
        verification_status = "unknown"
        try:
            open_scn = await self.redis.hgetall(f"market:open_scenario:{today_str}")
            if open_scn:
                verification_status = open_scn.get("verification_status", "unknown")
        except Exception:
            pass

        if strategy_primary_tag:
            reason.append(f"策略标签: {strategy_primary_tag}")

        if resonance_state == "strong_resonance" and action in ("WAIT", "REDUCE") and verification_status != "rejected":
            action = "OPEN"
            risk_level = "中低"
            reason.append("💎 核心机会: 点线面强共振触发！市场主线确认，允许积极试错。")
            position_max = max(position_max, 0.35)
        elif resonance_state == "weak_resonance" and action == "OPEN":
            action = "REDUCE"
            risk_level = "极高"
            reason.append("🧨 致命分歧: 市场共振断裂，板块协同性极差，严防冲高回撤。")
            position_max = min(position_max, 0.2)
        if not long_plate_suggestions:
            long_plate_suggestions = ["等待主线明确"]
        if not avoid_plate_suggestions:
            avoid_plate_suggestions = ["无明显负向板块"]

        # 优先使用板块画像输出的板块建议
        try:
            profile_key = f"rank:plate_profile:{today_str}"
            top_cand_with_scores = await self.redis.zrevrange(profile_key, 0, 15, withscores=True)
            bot_cand_with_scores = await self.redis.zrange(profile_key, 0, 10, withscores=True)
            
            # 使用带分的过滤器 (Apply High-Energy Exemption)
            top_filtered = [pid for pid, score in top_cand_with_scores if self._is_strategic_plate(pid, score)][:3]
            bot_filtered = [pid for pid, score in bot_cand_with_scores if self._is_strategic_plate(pid, score)][:2]
            
            # 动态建议清理 (Garbage Collection): 只有得分 > 5 且排名在前 100 的才保留在 long_plate_suggestions
            score_map = {pid: score for pid, score in top_cand_with_scores}
            
            if top_filtered:
                # 获取实时个股排名，用于提取推荐板块中的龙头个股
                top_stock_codes = await self.redis.zrevrange(f"rank:stock:{today_str}", 0, 199)
                stock_to_rank = {s: i for i, s in enumerate(top_stock_codes)}
                
                current_suggestions = []
                new_history = {}
                for pid in top_filtered:
                    # Hysteresis Logic: 提高上榜门槛，减少频繁切换 (Point 5)
                    self.plate_recommendation_history[pid] = self.plate_recommendation_history.get(pid, 0) + 1
                    
                    # 只有连续 2 次出现在 Top 且分数 > 5 或是绝对高分 (>50) 才正式建议
                    score = score_map.get(pid, 0.0)
                    if self.plate_recommendation_history[pid] >= 2 or score > 50.0:
                        current_suggestions.append(pid)
                    
                # 衰减不存在于当前 top_filtered 的历史记录 (GC)
                for pid in list(self.plate_recommendation_history.keys()):
                    if pid not in top_filtered:
                        self.plate_recommendation_history[pid] -= 1
                        if self.plate_recommendation_history[pid] <= 0:
                            del self.plate_recommendation_history[pid]
                        elif pid in self.active_long_plates and score_map.get(pid, 0) < 0:
                             # 动态清理 (Point 2): 如果已经在建议中但分转负，立即移除
                             pass 

                # 重新构建最终建议内容
                long_plate_suggestions = []
                final_plates = []
                for pid in current_suggestions:
                    score = score_map.get(pid, 0.0)
                    if score < 0 and pid not in top_filtered: continue # 额外保护
                    final_plates.append(pid)
                    
                    pname = self._get_plate_name(pid)
                    pstocks = self.plate_updater.plate_to_stocks.get(pid, [])
                    # 寻找该板块内排名靠前的个股 (使用已扩大的 500 名池子, Point 3)
                    stocks_with_rank = []
                    for s in pstocks:
                        if s in stock_to_rank:
                            stocks_with_rank.append((s, stock_to_rank[s]))
                    
                    stocks_with_rank.sort(key=lambda x: x[1])
                    top_pstocks = stocks_with_rank[:2]
                    
                    if top_pstocks:
                        snames_with_reason = []
                        for code, _ in top_pstocks:
                            sname = self._get_stock_name(code)
                            # 从 Redis 详细信息中提取分析理由
                            det_raw = await self.redis.hget(f"rank:stock:details:{today_str}", code)
                            reason_str = ""
                            if det_raw:
                                try:
                                    det = json.loads(det_raw)
                                    reason_str = det.get("analysis_reason", "")
                                except: pass
                            
                            if reason_str:
                                snames_with_reason.append(f"{sname}(分析: {reason_str})")
                            else:
                                snames_with_reason.append(sname)
                        
                        long_plate_suggestions.append(f"{pname}[{', '.join(snames_with_reason)}]")
                    else:
                        long_plate_suggestions.append(pname)
                
                self.active_long_plates = final_plates

            if bot_filtered:
                avoid_plate_suggestions = [self._get_plate_name(pid) for pid in bot_filtered]
        except Exception as e:
            logger.error(f"❌ 获取板块建议个股推荐失败: {e}")

        advice = {
            "ts": ts,
            "action": action,
            "risk_level": risk_level,
            "position_max": float(position_max),
            "reason": reason,
            "long_plates": long_plate_suggestions,
            "avoid_plates": avoid_plate_suggestions,
            "ban_conditions": ban_conditions,
        }
        advice_key = f"market:operator_advice:{today_str}"
        await self.redis.hset(advice_key, mapping={"ts": ts, "payload": json.dumps(advice, ensure_ascii=False)})
        await self.redis.expire(advice_key, 86400)
        # 翻译操作类型
        action_cn = {"WAIT": "持币观望", "CLOSE": "清仓避险", "REDUCE": "减仓防御", "OPEN": "开仓试错"}.get(action, action)
        
        self._log_event(
            "operator_advice",
            f"📌 指挥部指令: 【{action_cn}】 | 当前风险: {risk_level} | 核心主线: {','.join(long_plate_suggestions[:2])} | 禁区: {','.join(avoid_plate_suggestions[:2])}\n   决策原因: {' | '.join(reason)}",
            min_interval_sec=120,
        )

        await self._emit_snapshot(
            self._build_execution_snapshot(
                today_str=today_str,
                run_id=run_id,
                phase=phase,
                position_cap=self._safe_float(er_dict.get("position_cap", 0.0), 0.0) if 'er_dict' in locals() else 0.0,
                allowed_setups=list(mode_allow or []),
                blocked_setups=list(blocked_setups or []),
                fear_greed_score=fear_greed_score,
                resonance_score=resonance_score,
                process_state=process_state,
                process_risk_strength=process_risk_strength,
                danger_count=danger_count,
                active_setup_weights=active_setup_weights,
                avg_setup_weight=avg_setup_weight,
                policy=policy,
                action=action,
                risk_level=risk_level,
                reason_list=reason,
            )
        )

    def _calculate_next_sleep_interval(self, now: datetime, today_str: str) -> float:
        """Calculate seconds to sleep until the next relevant market event to avoid unnecessary waking."""
        is_trading_day = self.calendar.is_trade_day(today_str)
        current_time_str = now.strftime('%H:%M')

        # Scenario A: Not a trading day (Weekend/Holiday)
        if not is_trading_day:
            next_day = self.calendar.get_next_trade_day(today_str)
            if not next_day:
                return 300  # Fallback: sleep 5 mins if calendar is empty
            next_wake = datetime.strptime(f"{next_day} 09:10:00", "%Y-%m-%d %H:%M:%S")
            return max(60, (next_wake - now).total_seconds())

        # Scenario B: Trading day, but early morning (00:00 - 09:10)
        if current_time_str < '09:10':
            next_wake = now.replace(hour=9, minute=10, second=0, microsecond=0)
            return max(60, (next_wake - now).total_seconds())

        # Scenario C: Lunch break (11:32 - 12:56)
        if '11:32' <= current_time_str < '12:56':
            next_wake = now.replace(hour=12, minute=56, second=0, microsecond=0)
            return max(60, (next_wake - now).total_seconds())

        # Scenario D: Late night after market close (15:31 - 23:59)
        if current_time_str > '15:31':
            next_day = self.calendar.get_next_trade_day(today_str)
            if not next_day:
                return 300
            next_wake = datetime.strptime(f"{next_day} 09:10:00", "%Y-%m-%d %H:%M:%S")
            return max(60, (next_wake - now).total_seconds())

        # Scenario E: Active market hours
        return 0

    async def run_slow_analysis(self, today_str: str, cached_indicators: Dict, quote_map: Dict):
        """Background analysis tasks."""
        try:
            # P1: Move sentiment/resonance to background if needed, but here we just keep them separate from core loop
            await self.calculate_comfort_exit(today_str)
            phase_changed = await self.calculate_sentiment(today_str, indicators=cached_indicators)
            await self.calculate_resonance(today_str, self.candidate_pool_cache, cached_indicators, quote_map=quote_map)
            
            # 🔄 Corrective Logic: If phase changed, immediately regenerate the scenario cards
            if phase_changed:
                logger.info("📡 [Intraday Correction] Phase changed, refreshing trading plan...")
                await self.build_open_verify_plan(today_str)
        except Exception as e:
            logger.error(f"❌ slow_analysis failed: {e}")


    async def run_profiling(self, today_str: str, cached_indicators: Dict, quote_map: Dict):
        """Background profiling tasks."""
        try:
            if self.analysis_universe_cache:
                await self.calculate_market_overview(today_str, self.analysis_universe_cache, cached_indicators, quote_map=quote_map)
                await self.update_stock_day_profiles(today_str, self.analysis_universe_cache, cached_indicators, quote_map=quote_map)
                await self.calculate_plate_stock_snapshot(today_str, self.analysis_universe_cache, cached_indicators, quote_map=quote_map)
                await self.calculate_plate_profiles(today_str)
                await self.calculate_market_process_profile(today_str)
        except Exception as e:
            logger.error(f"❌ profiling failed: {e}")

    async def _calc_chip_and_extra_if_missing(self, target_day: str):
        """盘后/盘前：若缓存缺失则批算筹码峰+多因子（耗时放到线程池）"""
        if not target_day:
            return
        try:
            chip_key = f"cache:chip_peaks:{target_day}"
            extra_key = f"cache:stock_extra:{target_day}"
            
            # 1. 检查 Redis 个股数量
            extra_count = await self.redis.hlen(extra_key)
            chip_count = await self.redis.hlen(chip_key)
            
            # 2. 检查 TDengine K线数量
            kline_count = await asyncio.to_thread(self.advanced_indicators.tdengine.get_daily_count, target_day)
            
            # 如果三者都比较完整 ( > 4000 )，则真正可跳过
            if extra_count > 4000 and chip_count > 4000 and kline_count > 4000:
                logger.info(f"✅ 盘后筹码/因子数据已完整 (K线:{kline_count}, 因子:{extra_count}, 筹码:{chip_count})，跳过批算。")
                return

            logger.info(f"🛠️ 盘后批算筹码峰/多因子: {target_day} (K线:{kline_count}, 因子:{extra_count}, 筹码:{chip_count})")

            loop = asyncio.get_running_loop()

            def _run_batch():
                from web.services.chip_batch_runner import ChipBatchRunner
                runner = ChipBatchRunner()
                # 控制线程数，避免过载；max_kline_days沿用默认
                runner.run_batch(target_date=target_day, max_workers=8)

            await loop.run_in_executor(None, _run_batch)
        except Exception as e:
            logger.error(f"❌ 批算筹码峰/多因子失败: {e}")

    async def run_daily_profiles(self, target_day: str):
        """盘后/盘前批处理：补齐日级画像与盘后指标"""
        try:
            if not target_day:
                return
            # 已完成则跳过
            if self.daily_profiles_done_for == target_day:
                self.pending_eod_calc = False
                return
            logger.info(f"🧮 日级批处理开始: {target_day}")

            # 1. 确定范围与分析宇宙 (获取股票代码列表)
            self.candidate_pool_cache = await self.build_candidate_pool(target_day)
            self.analysis_universe_cache = await self.build_analysis_universe(target_day, self.candidate_pool_cache)

            codes = set(self.candidate_pool_cache or [])
            codes.update(self.analysis_universe_cache or [])
            indicators: Dict[str, Dict] = {}
            quote_map: Dict[str, Dict] = {}
            
            # 2. 获取行情数据 (获取 indicators)
            if codes:
                logger.info(f"📊 正在获取 {len(codes)} 只个股的最新行情指标...")
                codes_list = list(codes)
                indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(codes_list)
                
                # Fallback: If in non-trading time/weekend, real-time indicators might be empty.
                if not indicators and self.stock_extra:
                    logger.info(f"ℹ️ 行情缺失，从缓存加载指标副本")
                    indicators = {code: dict(val) for code, val in self.stock_extra.items() if code in codes}
                
                quote_map = indicators

            # 3. 这里的执行顺序很关键：在获取行情后开始高强度的筹码/因子批算 (耗时工作)
            logger.info(f"🛠️ [Data Dependency] 行情已就绪，开始执行高强度筹码/因子计算任务...")
            await self._calc_chip_and_extra_if_missing(target_day)

            # 4. 加载刚刚计算好的因子/筹码到内存
            await self._load_chip_peaks_cached(target_day)
            await self._load_stock_extra_cached(target_day)
            
            logger.info(f"✅ [Data Dependency] 筹码与因子加载完毕，进入核心分析环节")

            await self.calculate_market_overview(target_day, self.analysis_universe_cache or codes, indicators, quote_map=quote_map)
            await self.update_stock_day_profiles(target_day, self.analysis_universe_cache or codes, indicators, quote_map=quote_map)
            await self.calculate_plate_stock_snapshot(target_day, self.analysis_universe_cache or codes, indicators, quote_map=quote_map)
            await self.calculate_plate_profiles(target_day)
            await self.calculate_all_plate_phases(target_day)
            
            # Ensure comfort and sentiment exist immediately at start
            await self.calculate_comfort_exit(target_day)
            await self.calculate_sentiment(target_day, indicators=indicators)
            await self.calculate_resonance(target_day, self.candidate_pool_cache, indicators, quote_map=quote_map)
            
            # Ensure fear_greed and herding are populated to prevent STALE execution policy
            await self.calculate_fear_greed(target_day, self.candidate_pool_cache, indicators)
            await self.calculate_herding(target_day, self.candidate_pool_cache, indicators)
            
            await self.calculate_market_process_profile(target_day)
            try:
                phase_raw = await self.redis.hgetall(f"market:sentiment:{target_day}")
                market_phase = phase_raw.get("phase", "start")
                plate_phase_map = await self.redis.hgetall(f"market:plate_phase_map:{target_day}") or {}
                pattern_matrix = await self._build_pattern_matrix(
                    target_day,
                    market_phase=market_phase,
                    plate_phase_map=plate_phase_map,
                )
                await self._store_pattern_matrix(target_day, pattern_matrix)
            except Exception:
                pass

            # Ensure open_scenario exists
            try:
                open_scenario_exists = await self.redis.exists(f"market:open_scenario:{target_day}")
                if not open_scenario_exists:
                    await self.calculate_open_scenario(target_day, force=True)
                
                # Generate auction summary to populate "Yesterday's Sector Feedback"
                await self.build_auction_summary(target_day)
            except Exception:
                pass

            # Clear STALE strategy if the fallback correctly populated data
            try:
                await self.calculate_execution_policy(target_day)
            except Exception:
                pass

            self.daily_profiles_done_for = target_day
            self.pending_eod_calc = False
            logger.info(f"✅ 日级批处理完成: {target_day}")
        except Exception as e:
            logger.error(f"❌ 日级批处理失败: {e}")

    async def build_auction_summary(
        self,
        today_str: str,
        auction_items: Optional[List[Dict[str, Any]]] = None,
        expectation_eval: Optional[Dict[str, Any]] = None,
        open_scenario: Optional[Dict[str, Any]] = None,
    ):
        """生成竞价总结，汇总到 Redis 与日志"""
        try:
            if auction_items is None:
                auction_items = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
            
            expectation_eval = expectation_eval or await self.redis.hgetall(f"diag:expectation_eval:{today_str}")
            open_scenario = open_scenario or await self.redis.hgetall(f"market:open_scenario:{today_str}")
            strategy_tags = await self.redis.hgetall(f"market:strategy_tags:{today_str}")
            prev_day = self.calendar.get_previous_trade_day(today_str)

            sample = len(auction_items or [])
            if sample == 0:
                self._log_event(
                    "auction_data_missing",
                    f"⚠️ 竞价数据缺失 (market:auction:{today_str.replace('-', '')}:0925)，请检查是否正常推送数据。",
                    min_interval_sec=300,
                )
                return

            # 1. 基础概览
            changes = [self._safe_float(it.get("change_pct", 0.0), 0.0) for it in auction_items or []]
            amounts = [self._safe_float(it.get("auction_amount_yuan", 0.0), 0.0) for it in auction_items or []]
            high_open = sum(1 for c in changes if c >= 5.0)
            deep_low = sum(1 for c in changes if c <= -8.0)
            avg_change = float(np.mean(changes)) if changes else 0.0
            total_amt = sum(amounts)
            
            overview = {
                "sample": sample,
                "high_open_ge5_ratio": round(high_open / sample, 4) if sample else 0.0,
                "deep_low_le8_ratio": round(deep_low / sample, 4) if sample else 0.0,
                "avg_change_pct": round(avg_change, 4),
                "total_amount_yuan": round(total_amt, 2),
            }

            # 2. 板块反馈
            plate_top = []
            try:
                zkey = f"rank:plate_snapshot:{today_str}"
                dkey = f"rank:plate_snapshot:details:{today_str}"
                top = await self.redis.zrevrange(zkey, 0, 4, withscores=True)
                for pid, score in top:
                    detail_raw = await self.redis.hget(dkey, pid)
                    plate_top.append({
                        "id": pid,
                        "name": self._get_plate_name(pid),
                        "score": float(score),
                        "detail": json.loads(detail_raw) if detail_raw else {},
                    })
            except Exception: pass

            prev_feedback_plates = []
            if prev_day:
                try:
                    prev_top = await self.redis.zrevrange(f"rank:plate_profile:{prev_day}", 0, 4, withscores=False)
                    snap_raw = await self.redis.hgetall(f"rank:plate_snapshot:details:{today_str}")
                    today_snapshot = {pid: json.loads(v) if v else {} for pid, v in snap_raw.items()}
                    for pid in prev_top:
                        detail = today_snapshot.get(pid, {})
                        prev_feedback_plates.append({
                            "id": pid,
                            "name": self._get_plate_name(pid),
                            "today_change": detail.get("avg_change_pct"),
                            "today_amount_2min": detail.get("amount_2min"),
                        })
                except Exception: pass

            # 3. 昨日热门票反馈 (炸板、首板失败、大成交额)
            hot_feedback = {
                "broken_limit_up": {"count": 0, "avg_auc": 0.0, "avg_amt_w": 0.0},
                "first_failed": {"count": 0, "avg_auc": 0.0, "avg_amt_w": 0.0},
                "top_50_amount": {"count": 0, "avg_auc": 0.0, "avg_amt_w": 0.0},
            }
            
            # 池子源：默认使用 stock_extra (已在 context 中)，若缺失则问财保底
            broken_codes, first_failed_codes, top_50_codes = [], [], []
            
            if self.stock_extra:
                broken_codes = [c for c, s in self.stock_extra.items() if s.get("y_state") == "炸板"]
                first_failed_codes = [c for c, s in self.stock_extra.items() if s.get("y_state") == "炸板" and s.get("consecutive_up_days", 0) <= 0]
                top_50_codes = sorted(self.stock_extra.keys(), key=lambda c: self.stock_extra[c].get("amount_1d", 0), reverse=True)[:50]
            elif UnifiedMarketDataFetcher:
                # 问财保底方案
                try:
                    fetcher = UnifiedMarketDataFetcher()
                    logger.info("⚠️ stock_extra 缺失，启用问财保底获取热门池...")
                    broken_codes = await fetcher.get_wencai_broken_boards()
                    first_failed_codes = await fetcher.get_wencai_first_failed()
                    top_50_codes = await fetcher.get_wencai_top_amount()
                except Exception as e:
                    logger.warning(f"⚠️ 问财保底获取失败: {e}")

            if broken_codes or first_failed_codes or top_50_codes:
                auc_map = {str(it.get("symbol") or ""): it for it in (auction_items or [])}
                def _calc_grp(codes):
                    matched = [auc_map[c] for c in (codes or []) if c in auc_map]
                    if not matched: return 0, 0.0, 0.0
                    avg_chg = np.mean([float(it.get("change_pct", 0.0)) for it in matched])
                    avg_amt = np.mean([float(it.get("auction_amount_yuan", 0.0)) for it in matched]) / 10000
                    return len(matched), round(float(avg_chg), 2), round(float(avg_amt), 2)
                
                cnt1, auc1, amt1 = _calc_grp(broken_codes)
                hot_feedback["broken_limit_up"] = {"count": cnt1, "avg_auc": auc1, "avg_amt_w": amt1}
                cnt2, auc2, amt2 = _calc_grp(first_failed_codes)
                hot_feedback["first_failed"] = {"count": cnt2, "avg_auc": auc2, "avg_amt_w": amt2}
                cnt3, auc3, amt3 = _calc_grp(top_50_codes)
                hot_feedback["top_50_amount"] = {"count": cnt3, "avg_auc": auc3, "avg_amt_w": amt3}

            # 4. 风险警报
            risks = []
            if sample < 20:
                risks.append("CRITICAL: Auction data sample too small")
            if hot_feedback["top_50_amount"]["avg_auc"] <= -1.5:
                risks.append("WARNING: 大资金票集体低开，注意流动性减损")
            if hot_feedback["broken_limit_up"]["avg_auc"] <= -3.0:
                risks.append("NEGATIVE: 昨日炸板票反馈极差，接力需谨慎")
            if self._safe_float(expectation_eval.get("effectiveness", 0.0)) < 0.35:
                risks.append("INFO: 竞价预期差近期低效")

            # 5. 开盘啦板块聚合反馈 (Kaipanla Plates)
            kaipan_plate_feedback = []
            try:
                if fetch_kaipan_plate_rank:
                    loop = asyncio.get_event_loop()
                    kp_res = await loop.run_in_executor(None, fetch_kaipan_plate_rank, "0", "20")
                    if kp_res and kp_res.get("ok"):
                        kp_plates = kp_res.get("plates", [])
                        top_inflow = [p for p in kp_plates if p.get("main_net", 0) > 0]
                        # Sort by strength
                        top_inflow.sort(key=lambda x: x.get("strength", 0.0), reverse=True)
                        # Load plate phase mapping for current day context
                        phase_map = await self.redis.hgetall(f"market:plate_phase_map:{today_str}") or {}
                        
                        for p in top_inflow[:5]:
                            pid = p.get("id")
                            kaipan_plate_feedback.append({
                                "id": pid,
                                "name": p.get("name"),
                                "strength": p.get("strength"),
                                "change_pct": p.get("change_pct"),
                                "main_net": p.get("main_net"),
                                "phase": phase_map.get(pid, self._calculate_plate_emotion_phase(pid, today_str, p)),
                            })
                        
                        # Check for extreme divergence (Top 1 by strength but massive outflow)
                        if kp_plates:
                            top_str_plate = max(kp_plates, key=lambda x: x.get("strength", 0.0))
                            pid = top_str_plate.get("id")
                            phase = phase_map.get(pid, self._calculate_plate_emotion_phase(pid, today_str, top_str_plate))
                            if top_str_plate.get("main_net", 0) < -100_000_000 and top_str_plate.get("change_pct", 0) > 2.0:
                                risks.append(f"WARNING: 板块[{top_str_plate['name']}]处于{phase}期，强度虽高但主力净流出过亿，防范诱多")
            except Exception as e:
                logger.warning(f"⚠️ Kaipanla plate feedback extraction failed: {e}")

            # 6. 组装最终画像
            preopen_plan = await self.redis.hgetall(f"market:plan:preopen:{today_str}")
            open_verify_plan = await self.redis.hgetall(f"market:plan:open_verify:{today_str}")
            potentials = []
            try:
                top_codes = await self.redis.zrevrange(f"rank:stock:{today_str}", 0, 9, withscores=True)
                if top_codes:
                    details_map = await self.redis.hgetall(f"rank:stock:details:{today_str}")
                    for code, score in top_codes:
                        raw = details_map.get(code)
                        potentials.append({"code": code, "score": float(score), "detail": json.loads(raw) if raw else {}})
            except Exception: pass

            summary = {
                "ts": int(time.time() * 1000),
                "data_source": auction_items[0].get("source", "t1") if auction_items else "unknown",
                "overview": overview,
                "plate_hot": plate_top,
                "prev_plate_feedback": prev_feedback_plates,
                "hot_feedback": hot_feedback,
                "kaipan_plate_feedback": kaipan_plate_feedback,
                "preopen_plan": preopen_plan,
                "open_verify_plan": open_verify_plan,
                "potentials": potentials,
                "sentiment": {
                    "phase": strategy_tags.get("primary_tag"),
                    "expectation_eff": self._safe_float(expectation_eval.get("effectiveness", 0.0)),
                    "fade_count": self._safe_int(expectation_eval.get("fade_count", 0)),
                    "rise_count": self._safe_int(expectation_eval.get("rise_count", 0)),
                },
                "risks": risks,
            }

            out_key = f"market:auction_summary:{today_str}"
            await self.redis.hset(out_key, mapping={"ts": summary["ts"], "payload": json.dumps(summary, ensure_ascii=False)})
            await self.redis.expire(out_key, 86400)

            self._log_event(
                "auction_summary",
                f"🛰️ 竞价总结 sample={sample} high5={overview['high_open_ge5_ratio']:.0%} hot_broken_auc={hot_feedback['broken_limit_up']['avg_auc']}% pot={len(potentials)}",
                min_interval_sec=180,
            )
        except Exception as e:
            logger.error(f"❌ [Auction Summary] 异常: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _clear_intraday_caches(self):
        """Clears all unbounded intraday caches to prevent memory accretion across days."""
        self.auction_profile_cache.clear()
        self.stock_state_cache.clear()
        self.plate_weight_cache.clear()
        self.return_history.clear()
        self.leading_plate_history.clear()
        self.log_last_payload.clear()
        self.log_last_ts.clear()
        self.analysis_universe_cache.clear()
        self.profile_transition_seen.clear()
        self.code_change_history.clear()
        self.intraday_transition_seen.clear()
        self._auction_cache.clear()
        self._first_limit_cache = {"ts": 0.0, "codes": set()}
        self._quote_cache.clear()
        self._kaipan_plate_cache = {"ts": 0.0, "by_id": {}, "count": 0}
        logger.info("🗑️ [Memory Optimization] Intraday memory caches cleared successfully.")

    async def has_eod_data(self, target_day: str) -> bool:
        """检查指定日期的盘后批处理数据是否存在且完整 (TDengine K线 + 情感画像 + 多因子缓存)"""
        try:
            # 1. 检查 TDengine 日线数据量 (最底层的行情数据)
            kline_count = await asyncio.to_thread(self.advanced_indicators.tdengine.get_daily_count, target_day)
            
            # 2. 检查多因子缓存中的个股数量 (派生指标)
            extra_count = await self.redis.hlen(f"cache:stock_extra:{target_day}")

            # 3. 检查情感画像 (流程的最终产出)
            sentiment_exists = await self.redis.exists(f"market:sentiment:{target_day}")
            
            # 如果 K线 > 4000 且 多因子 > 4000 且 画像存在，认为数据完整
            is_complete = bool(sentiment_exists and extra_count > 4000 and kline_count > 4000)
            
            if sentiment_exists:
                if not is_complete:
                    logger.warning(f"⚠️ [Startup Recovery] 检测到 {target_day} 数据不完整 (K线: {kline_count}, 多因子: {extra_count})")
                else:
                    logger.info(f"✅ [Startup Recovery] 检测到 {target_day} 数据完整 (K线: {kline_count}, 多因子: {extra_count})")
            
            return is_complete
        except Exception as e:
            logger.error(f"❌ [has_eod_data] 检查异常: {e}")
            return False

    async def _pre_open_inspection_task(self):
        """09:00 安检：检查因子与历史数据完整性，设置降级标志"""
        logger.info("🛡️ [Pre-Open Inspection] 开始盘前数据完整性校验...")
        
        # 释放前一交易日所有积压的内存缓存
        self._clear_intraday_caches()
        
        try:
            today_str = datetime.now().strftime('%Y-%m-%d')
            prev_day = self.calendar.get_previous_trade_day(today_str)
            
            # Check 1: Multi-factors
            extra_count = await self.redis.hlen(f"cache:stock_extra:{prev_day}")
            if extra_count < 4000:
                logger.warning(f"⚠️ [Pre-Open Inspection] 昨日多因子数据不完整 ({extra_count} < 4000)。触发补偿加载。")
                self.sys_health_status["data_integrity"] = False
                self.sys_health_status["reason"] = "missing_stock_extra"
            else:
                self.sys_health_status["data_integrity"] = True
                self.sys_health_status["reason"] = "ok"

            # Loading regardless to warm up cache
            await self._load_stock_extra_cached(prev_day)
            
            logger.info(f"✅ [Pre-Open Inspection] 完成。系统状态: {self.sys_health_status}")
        except Exception as e:
            logger.error(f"❌ [Pre-Open Inspection] 异常: {e}")

    async def _eod_batch_task(self):
        """15:10 盘后：触发重负载的批量计算"""
        logger.info("🧮 [EOD Batch Task] 触发盘后批量处理...")
        today_str = datetime.now().strftime('%Y-%m-%d')
        if not self.calendar.is_trade_day(today_str):
            today_str = self.calendar.get_previous_trade_day(today_str)
        try:
            await self.run_daily_profiles(today_str)
        except Exception as e:
            logger.error(f"❌ [EOD Batch Task] 异常: {e}")

    async def _auction_window_task(self):
        """09:25 - 09:30 竞价核心处理：处理昨日反馈，预置今日计划"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        if not self.calendar.is_trade_day(today_str) and self.manual_date is None:
            return
            
        logger.info("🔔 [Auction Window Task] 检查/生成竞价画像...")
        try:
            # 1. 获取竞价数据 (如果 t1.exe 未推送，可能会返回空，需多重试几次)
            auction_items = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
            if not auction_items:
                logger.debug("⏳ [Auction Window Task] 尚未获取到竞价数据，等待下一跳。")
                return

            # 2. 检查是否需要重新分析 (P6: 支持 09:30 前数据修正)
            summary_raw = await self.redis.hget(f"market:auction_summary:{today_str}", "payload") 
            if summary_raw:
                try:
                    summary_dict = json.loads(summary_raw)
                    existing_source = summary_dict.get("data_source", "unknown")
                    current_source = auction_items[0].get("source", "t1") if auction_items else "unknown"
                    
                    # 如果现有总结是问财出的，而现在有了更正宗的数据（t1），则重新跑
                    if existing_source == "wencai_sync_reactive" and current_source != "wencai_sync_reactive":
                        logger.warning("⚡ [Auction Window Task] 检测到正宗 09:25 竞价数据已到达，重新触发分析流水线...")
                    else:
                        return # 已经有最终版或同级别数据了，跳过
                except Exception:
                    return

            # 2. 生成预期差评估
            cached_indicators = {}
            ctx_quote_map = {}
            expectation_eval = await self.calculate_expectation_eval(today_str, auction_items=auction_items, cached_indicators=cached_indicators, quote_map=ctx_quote_map)
            
            # 3. 生成策略标签与画像
            await self.calculate_strategy_tags(today_str, expectation_eval=expectation_eval, auction_items=auction_items)
            strategy_ctx = await self.redis.hgetall(f"market:strategy_tags:{today_str}")
            
            # 4. 生成竞价计划、预案、及总结
            await self.build_preopen_plan(today_str, auction_items=auction_items, strategy_tags=strategy_ctx or None)
            await self.build_open_verify_plan(today_str, expectation_eval=expectation_eval)
            await self.calculate_open_scenario(today_str, auction_items=auction_items, quote_map=ctx_quote_map)
            await self.build_auction_summary(today_str, auction_items=auction_items, expectation_eval=expectation_eval)
            
            logger.info("✅ [Auction Window Task] 竞价总结及预案生成完毕。")
        except Exception as e:
            logger.error(f"❌ [Auction Window Task] 异常: {e}")

    async def run(self, interval_seconds: int = 60):
        """Main engine loop with adaptive heartbeat and shared context."""
        mode = "live" if self.manual_date is None else "replay"
        logger.info(f"🚀 Market Edge Engine started (Mode: {mode})")
        
        if mode == "live":
            # 挂载 APScheduler 独立调度任务 (解耦盘前/盘后大负载，设置容错并允许积压)
            self.scheduler.add_job(self._pre_open_inspection_task, CronTrigger(hour=9, minute=0, timezone="Asia/Shanghai"), max_instances=5, misfire_grace_time=300)
            self.scheduler.add_job(self._eod_batch_task, CronTrigger(hour=16, minute=0, timezone="Asia/Shanghai"), max_instances=1, misfire_grace_time=3600)
            # 竞价时间段高频轮询 (每5秒检查一次数据是否就绪)
            self.scheduler.add_job(self._auction_window_task, CronTrigger(hour=9, minute="25-30", second="*/5", timezone="Asia/Shanghai"), max_instances=10, misfire_grace_time=60)
            self.scheduler.start()
            logger.info("⏱️ APScheduler 任务调度器已启动。")

            # [Startup Recovery] 检查是否错过了今日盘后处理 (例如 16:00 时关机，现在是 19:00 启动)
            try:
                latest_day = self.calendar.get_latest_trade_day()
                current_phase = self._current_phase()
                eod_exists = await self.has_eod_data(latest_day)
                logger.info(f"🔍 [Startup Recovery] Check: Day={latest_day}, Phase={current_phase}, DataExists={eod_exists}")
                
                # 如果当前是“盘后”、“晚上”或“凌晨收盘后”（即 15:30 以后到次日 09:15 之前），且该交易日的数据还没跑过，则立即追赶
                if current_phase in ("post_close", "evening", "pre_open"):
                    if not eod_exists:
                        logger.info(f"📅 [Startup Recovery] 检测到 {latest_day} 盘后数据缺失，触发自动补处理...")
                        asyncio.create_task(self.run_daily_profiles(latest_day))
                    else:
                        logger.info(f"✅ [Startup Recovery] {latest_day} 盘后数据已存在，跳过补处理。")
            except Exception as e:
                logger.error(f"❌ [Startup Recovery] 检查失败: {e}")
                traceback.print_exc()
        
        cached_indicators = {}
        loop_counter = 0
        last_gc_time = time.time()
        
        while True:
            try:
                loop_counter += 1
                now = datetime.now()
                today_str = self.manual_date if self.manual_date else now.strftime('%Y-%m-%d')
                
                # Critical Feature: In live mode, treat non-trading days (weekends/holidays) as the last trading day.
                if self.manual_date is None:
                    if not self.calendar.is_trade_day(today_str):
                        prev_day = self.calendar.get_previous_trade_day(today_str)
                        if prev_day:
                            today_str = prev_day
                            
                now_ts = time.time()
                is_live_mode = self.manual_date is None
                phase = self._current_phase()

                # Heartbeat log (Enhanced visibility during auction/opening)
                # Note: loop_counter is use here for heartbeat frequency control
                log_interval = 2 if phase in ("auction", "opening") else 10
                if loop_counter % log_interval == 1:
                    logger.info(f"🔄 MarketEdgeEngine Intraday Heartbeat | Phase: {phase} | Pool: {len(self.candidate_pool_cache or [])}")

                # 1) Non-trading period adaptive sleep (P1)
                # Skip sleep in replay mode
                if is_live_mode:
                    if phase in ("intraday_am", "intraday_pm"):
                        self.pending_eod_calc = True

                    # 只在盘中时段（含竞价、开盘）保持活跃，其他时间大幅度休眠，避免冗余运算
                    if phase not in ("intraday_am", "intraday_pm", "opening", "auction"):
                        sleep_sec = self._calculate_next_sleep_interval(now, today_str)
                        if sleep_sec > 0:
                            logger.debug(f"💤 Non-trading time ({phase}), skipping real-time ops, sleeping for {int(sleep_sec)}s")
                            await asyncio.sleep(sleep_sec)
                            continue
                
                # --- System Health Watchdog (P0) ---
                if is_live_mode and loop_counter % 5 == 0: # Check every 5 cycles (~5-10s)
                    if phase in ("opening", "intraday_am", "intraday_pm", "auction"):
                        # Check 1: Data Pulse (Check a few quotes from candidate pool)
                        is_stale = False
                        if self.candidate_pool_cache:
                            sample_codes = list(self.candidate_pool_cache)[:5]
                            stale_count = 0
                            for scode in sample_codes:
                                q_ts = await self.redis.hget(f"stock:quote:{scode}", "timestamp")
                                if q_ts:
                                    try:
                                        # Handle both s and ms timestamps
                                        ts_val = float(q_ts)
                                        if ts_val > 2e9: ts_val /= 1000.0
                                        if time.time() - ts_val > 120:
                                            stale_count += 1
                                    except: pass
                                else:
                                    stale_count += 1
                            if stale_count >= len(sample_codes) and len(sample_codes) > 0:
                                is_stale = True
                        
                        # Check 2: Global Tick Update
                        last_tick = await self.redis.get("market:tick:last_update")
                        if last_tick:
                            try: 
                                if time.time() - float(last_tick) > 120: is_stale = True
                            except: pass
                        
                        if is_stale:
                            self.sys_health_status["data_integrity"] = False
                            msg = "🚨 [DATA DEATH] 行情数据停滞(>120s)"
                            if time.time() - self.log_last_ts.get("data_death_alert", 0) > 120:
                                logger.critical(f"{msg} | 请检查底层 t1.exe 运行状态。")
                                self.log_last_ts["data_death_alert"] = time.time()
                        else:
                            self.sys_health_status["data_integrity"] = True

                loop_start = time.time()
                
                # 2) Context Refresh (P0/P1 Optimization)
                # Refresh Candidate Pool (300s)
                context_changed = False
                if now_ts - self.last_candidate_pool_update >= self.task_intervals["candidate_pool"]:
                    self.candidate_pool_cache = await self.build_candidate_pool(today_str)
                    self.last_candidate_pool_update = now_ts
                    context_changed = True
                    await self._load_chip_peaks_cached(today_str)
                    await self._load_stock_extra_cached(today_str)
                
                # Refresh Analysis Universe (300s)
                if now_ts - self.last_analysis_universe_update >= self.task_intervals["analysis_universe"]:
                    self.analysis_universe_cache = await self.build_analysis_universe(today_str, self.candidate_pool_cache)
                    self.last_analysis_universe_update = now_ts
                    context_changed = True

                if context_changed:
                    combined_universe = set(self.candidate_pool_cache or [])
                    if self.analysis_universe_cache:
                        combined_universe.update(self.analysis_universe_cache)
                    if combined_universe:
                        await self.precompute_static_context(today_str, combined_universe, force=True)

                # 3) Data Fetching (Centralized P0/P1 Reuse)
                all_codes = set(self.candidate_pool_cache or [])
                if self.analysis_universe_cache:
                    all_codes.update(self.analysis_universe_cache)
                
                # Fetch indicators and quotes ONCE per cycle (Shared context)
                if all_codes:
                    codes_list = list(all_codes)
                    loop = asyncio.get_event_loop()
                    cached_indicators = await loop.run_in_executor(None, self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized, codes_list)
                    ctx_quote_map = cached_indicators  # Reuse: same Redis HASH source
                else:
                    ctx_quote_map = {}
                    cached_indicators = {}

                # 4) CORE Processing (Synchronous sequence)
                if self.candidate_pool_cache:
                    # Plate and Theme analysis (Centralized Reuse)
                    await self.calculate_plate_spread(today_str, self.candidate_pool_cache, cached_indicators, quote_map=ctx_quote_map)
                    await self.calculate_theme_rank(today_str, self.candidate_pool_cache, cached_indicators, quote_map=ctx_quote_map)

                    # Update State Machine
                    latest_transitions = await self.update_intraday_state_machine(today_str, self.candidate_pool_cache, cached_indicators)
                    if latest_transitions:
                        await self.calculate_plate_attitude(today_str, latest_transitions)

                # 6) Analysis & Arbitrage Tasks
                if self.candidate_pool_cache:
                    auction_items = await self._get_auction_top_amount_cached(today_str, require_final_0925=False)
                    await self.calculate_ab_arbitrage(today_str, self.candidate_pool_cache, cached_indicators, auction_items=auction_items, quote_map=ctx_quote_map)
                    await self.calculate_stock_rank(today_str, self.candidate_pool_cache, cached_indicators, quote_map=ctx_quote_map)
                
                await self.calculate_execution_policy(today_str)


                # 7) Background Tasks (Fire and forget, P1)
                
                # Critical check: Force run_slow_analysis if comfort_exit is missing for today (avoids STALE execution policy)
                try:
                    comfort_exists = await self.redis.exists(f"market:comfort_exit:{today_str}")
                    open_scenario_exists = await self.redis.exists(f"market:open_scenario:{today_str}")
                    if (not comfort_exists or not open_scenario_exists) and now_ts - self.last_sentiment_update >= 10:
                        logger.info(f"⚠️ Missing critical state keys. Forcing population.")
                        if not comfort_exists:
                            asyncio.create_task(self.run_slow_analysis(today_str, cached_indicators, ctx_quote_map))
                        if not open_scenario_exists:
                            asyncio.create_task(self.calculate_open_scenario(today_str, force=True))
                        self.last_sentiment_update = now_ts
                except Exception:
                    pass

                if now_ts - self.last_sentiment_update >= self.task_intervals["sentiment"]:
                    asyncio.create_task(self.run_slow_analysis(today_str, cached_indicators, ctx_quote_map))
                    self.last_sentiment_update = now_ts
                
                if now_ts - self.last_market_overview_update >= self.task_intervals["market_overview"]:
                    asyncio.create_task(self.run_profiling(today_str, cached_indicators, ctx_quote_map))
                    self.last_market_overview_update = now_ts
                
                # 🔄 Periodically refresh trading plan even without phase changes (to keep SignalCard rankings fresh)
                if now_ts - self.last_open_verify_plan_update >= self.task_intervals["open_verify_plan"]:
                    asyncio.create_task(self.build_open_verify_plan(today_str))
                    # Note: last_open_verify_plan_update is set inside build_open_verify_plan

                # 8) 收盘后如有待处理批量任务，立即执行
                if is_live_mode and phase == "post_close" and self.pending_eod_calc and self.daily_profiles_done_for != today_str:
                    await self.run_daily_profiles(today_str)

                # Cycle control
                elapsed = time.time() - loop_start
                # Adaptive heartbeat: 15s during active transitions, otherwise 60s
                heartbeat = 15 if phase in ("auction", "opening") else interval_seconds
                sleep_time = max(1, heartbeat - elapsed)
                
                logger.debug(f"✨ Cycle completed in {elapsed:.2f}s (Phase: {phase}). Next in {sleep_time:.1f}s")
                
                # Memory optimization: periodic GC
                if now_ts - last_gc_time > 300: # Every 5 minutes
                    import gc
                    gc.collect()
                    last_gc_time = now_ts
                
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                logger.info("🛑 Engine stopping...")
                break
            except Exception as e:
                logger.error(f"❌ Main loop error: {e}", exc_info=True)
                await asyncio.sleep(10)
