from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .enums import (
    ExecutionEnvironment,
    ExposureState,
    FeedbackState,
    FetchIntent,
    FailedPromotionType,
    LeaderTier,
    OperatorStyleHint,
    RunPhase,
    SourceName,
    StartupAction,
    StartupReadinessLevel,
    StockArchetype,
    StockStage,
    StorageTier,
    TradeWindowState,
)


@dataclass(frozen=True)
class RuntimeEventSpec:
    time_window: str
    phase: RunPhase
    component: str
    action: str
    source_refs: tuple[str, ...] = ()
    notes: str = ""


@dataclass(frozen=True)
class IntradayNetworkRule:
    name: str
    intent: FetchIntent
    allowed_phases: tuple[RunPhase, ...]
    preferred_sources: tuple[SourceName, ...]
    max_symbols_per_request: int
    max_requests_per_minute: int
    redis_write_key: Optional[str]
    reason: str


@dataclass(frozen=True)
class IntradayRequestDecision:
    allowed: bool
    intent: FetchIntent
    phase: RunPhase
    chosen_source: Optional[SourceName]
    redis_write_key: Optional[str]
    notes: str = ""


@dataclass(frozen=True)
class StorageRoutingRule:
    dataset: str
    has_trade_date_dimension: bool
    primary_storage: StorageTier
    secondary_storage: Optional[StorageTier]
    bucket_by: str
    notes: str = ""


@dataclass(frozen=True)
class RedisWritebackPlan:
    key: str
    fields: tuple[str, ...]
    ttl_seconds: Optional[int]
    notes: str = ""


@dataclass(frozen=True)
class RuntimeTimelineSummary:
    events: List[RuntimeEventSpec] = field(default_factory=list)

    def group_by_phase(self) -> Dict[RunPhase, List[RuntimeEventSpec]]:
        grouped: Dict[RunPhase, List[RuntimeEventSpec]] = {}
        for event in self.events:
            grouped.setdefault(event.phase, []).append(event)
        return grouped


@dataclass(frozen=True)
class StockStateSnapshot:
    symbol: str
    name: str = ""
    plate: str = ""
    lb_days: int = 0
    leader_rank_in_theme: int = 999
    board_time_rank: int = 999
    open_pct: float = 0.0
    current_pct: float = 0.0
    change_pct: float = 0.0
    auction_amount: float = 0.0
    volume_intensity: float = 1.0
    vol_ratio: float = 0.0
    speed_1m: float = 0.0
    amount_2m: float = 0.0
    amount_5m: float = 0.0
    vector_3m: float = 0.0
    vector_5m: float = 0.0
    resonance_factor: float = 1.0
    resistance_gap: float = 0.0
    concentration: float = 0.0
    profit_ratio: float = 0.0
    bias_20: float = 0.0
    market_cap_yi: float = 0.0
    amount_day_yi: float = 0.0
    plate_persistence_score: float = 0.0
    hot_plate_days: int = 0
    ths_hot_rank: int | None = None
    t2_lb_days: int = 0
    t2_pct: float = 0.0
    yday_broken_board: bool = False
    day_before_limit_up: bool = False
    touched_limit_today: bool = False
    is_yest_limit: bool = False
    is_locked: bool = False
    real_plate_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class StockProfileAssessment:
    symbol: str
    archetype: StockArchetype
    leader_tier: LeaderTier
    stage: StockStage
    failed_promotion_type: FailedPromotionType
    operator_style_hint: OperatorStyleHint
    feedback_state: FeedbackState
    exposure_state: ExposureState
    trade_window: TradeWindowState
    darkness_exposure_score: int
    continuation_score: int
    retail_attention_proxy: int
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class AuctionLadderDecision:
    symbol: str
    setup_id: str
    action: str
    confidence: int
    kelly_position_pct: float
    risk_reward_ratio: float
    profile: StockProfileAssessment
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class PersistenceWritePlan:
    dataset: str
    primary_storage: str
    secondary_storage: str | None
    bucket_key: str
    row_count: int
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RedisViewMaterialization:
    dataset: str
    trade_date: str
    redis_key: str
    field_count: int
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class TeacherFamilyAnnotation:
    family_name: str
    fit_level: str
    use_stage: str
    veto_hint: str = ""
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class StartupDatasetStatus:
    dataset: str
    phase: RunPhase
    target_date: str
    required: bool
    ready: bool
    source: str
    action: StartupAction
    missing_count: int = 0
    total_count: int = 0
    freshness_date: str = ""
    severity: str = "info"
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class StartupSelfCheckReport:
    phase: RunPhase
    readiness: StartupReadinessLevel
    target_trade_date: str
    formal_offline_date: str
    statuses: tuple[StartupDatasetStatus, ...]
    recommended_actions: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def by_dataset(self) -> Dict[str, StartupDatasetStatus]:
        return {status.dataset: status for status in self.statuses}


@dataclass(frozen=True)
class IntradayContext:
    phase: RunPhase
    trade_date: str
    offline_context_date: str
    stock_snapshots: tuple[StockStateSnapshot, ...]
    market_summary: "IntradayMarketSummary"
    hot_plate_map: Dict[str, dict]
    yesterday_hot_plate_map: Dict[str, dict]
    yest_limit_map: Dict[str, dict]
    auction_map: Dict[str, dict]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class IntradayMarketSummary:
    top_turnover_symbols: tuple[str, ...]
    top_plate_name: str = ""
    top_plate_strength: float = 0.0
    top_plate_migration_type: str = ""
    mainline_sector: str = ""
    mainline_net_inflow_yi: float = 0.0
    top_sector_pct: float = 0.0
    resonance_score: float = 0.0
    hot_plate_count: int = 0
    yest_hot_plate_count: int = 0
    yest_hot_plate_match_count: int = 0
    persistent_plate_count: int = 0
    emerging_plate_count: int = 0
    fading_plate_count: int = 0
    mainline_switch: bool = False
    total_yest_limit_count: int = 0
    context_auc_amt: float = 0.0
    context_avg_5d_vol: float = 0.0
    context_symbol_count: int = 0
    context_coverage_factor: float = 1.0
    market_full_auc_amt: float = 0.0
    market_predicted_full_day_amount: float = 0.0
    market_avg_5d_vol: float = 0.0
    market_volume_level: str = ""
    promotion_rate: float = 0.0
    red_open_rate: float = 0.0
    headshot_rate: float = 0.0
    sentiment_score: float = 0.0
    red_green_ratio: float = 0.0
    avg_bid_amt: float = 0.0
    battle_status: str = ""
    notes: tuple[str, ...] = ()
