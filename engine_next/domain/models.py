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
    rsi_6: float = 0.0
    ddje: float = 0.0
    ddx: float = 0.0
    ddy: float = 0.0
    ddz: float = 0.0
    structure_score_base: float = 0.0
    shape_platform_ready: float = 0.0
    shape_breakout_ready: float = 0.0
    shape_repair_ready: float = 0.0
    shape_overheat_risk: float = 0.0
    shape_chip_cleanliness: float = 0.0
    shape_trend_health: float = 0.0
    shape_t2_repair_bias: float = 0.0
    theme_core_base: float = 0.0
    market_cap_yi: float = 0.0
    amount_day_yi: float = 0.0
    plate_persistence_score: float = 0.0
    hot_plate_days: int = 0
    ths_hot_rank: int | None = None
    ths_hot_heat: float = 0.0
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
class HotPlateFact:
    plate_name: str
    rank: int = 999
    strength: float = 0.0
    change_pct: float = 0.0
    net_inflow_yi: float = 0.0
    hot: float = 0.0


@dataclass(frozen=True)
class PlateMigrationFact:
    plate_name: str
    today_strength: float = 0.0
    yesterday_strength: float = 0.0
    strength_delta: float = 0.0
    today_change_pct: float = 0.0
    yesterday_change_pct: float = 0.0
    change_pct_delta: float = 0.0
    today_net_inflow_yi: float = 0.0
    yesterday_net_inflow_yi: float = 0.0
    net_inflow_yi_delta: float = 0.0
    present_today: bool = False
    present_yesterday: bool = False


@dataclass(frozen=True)
class ThemeFact:
    plate_name: str
    leader_symbol: str = ""
    top3_symbols: tuple[str, ...] = ()
    symbol_count: int = 0
    auction_amount: float = 0.0
    yest_limit_count: int = 0
    leader_count: int = 0


@dataclass(frozen=True)
class ThemeTradeFact:
    plate_name: str
    yest_hot_rank: int = 999
    yest_limit_count: int = 0
    yest_high_board_count: int = 0
    auction_amount: float = 0.0
    red_open_count: int = 0
    red_open_rate: float = 0.0
    avg_open_pct: float = 0.0
    front_row_count: int = 0
    front_row_red_count: int = 0
    leader_count: int = 0
    amount_2m_sum: float = 0.0
    amount_5m_sum: float = 0.0
    front_row_2m_pass_count: int = 0
    high_open_fail_count: int = 0
    low_open_repair_count: int = 0
    expansion_count: int = 0


@dataclass(frozen=True)
class LadderFact:
    key: str
    total_count: int = 0
    red_open_count: int = 0
    promoted_count: int = 0
    representative_symbol: str = ""


@dataclass(frozen=True)
class SessionFacts:
    fact_set_id: str = ""
    hot_plate_today: tuple[HotPlateFact, ...] = ()
    hot_plate_today_map: Dict[str, HotPlateFact] = field(default_factory=dict)
    hot_plate_yesterday: tuple[HotPlateFact, ...] = ()
    hot_plate_yesterday_map: Dict[str, HotPlateFact] = field(default_factory=dict)
    plate_migration: tuple[PlateMigrationFact, ...] = ()
    plate_migration_map: Dict[str, PlateMigrationFact] = field(default_factory=dict)
    theme_facts: tuple[ThemeFact, ...] = ()
    theme_fact_map: Dict[str, ThemeFact] = field(default_factory=dict)
    theme_trade_facts: tuple[ThemeTradeFact, ...] = ()
    theme_trade_fact_map: Dict[str, ThemeTradeFact] = field(default_factory=dict)
    ladder_facts: tuple[LadderFact, ...] = ()
    ladder_fact_map: Dict[str, LadderFact] = field(default_factory=dict)


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
class ThemeSelectionContext:
    plate_name: str
    e_score: float = 0.0
    a_score: float = 0.0
    x_score: float = 0.0
    market_regime: str = "neutral"
    theme_trade_label: str = "unknown"
    trade_conclusion: str = "unknown"
    fakeout_level: str = "unknown"
    cohesion_level: str = "unknown"
    tradable: bool = False
    bias_action: str = "observe_only"
    open_confirm_state: str = "unknown"
    plate_strength_rank_pct: float = 1.0
    plate_delta_rank_pct: float = 1.0
    plate_breadth_score: float = 0.0
    plate_follow_through_score: float = 0.0
    plate_resistance_score: float = 0.0
    plate_role: str = "neutral"
    rotation_bias: str = "neutral"
    phase_priority_bias: float = 0.0
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class StockSelectionContext:
    symbol: str
    plate_name: str = ""
    theme_trade_label: str = "unknown"
    hot_rank: int = 999
    hot_heat: float = 0.0
    is_active_pool: bool = False
    is_true_leader: bool = False
    is_front_row: bool = False
    leader_bucket: str = "unknown"
    heat_flow_score: float = 0.0
    turnover_quality_score: float = 0.0
    activity_score: float = 0.0
    theme_core_score: float = 0.0
    kline_pattern: str = "unknown"
    auction_open_bucket: str = "unknown"
    open_follow_state: str = "unknown"
    kline_score: float = 0.0
    structure_score: float = 0.0
    chip_score: float = 0.0
    auction_score: float = 0.0
    timing_score: float = 0.0
    open_undertake_score: float = 0.0
    shape_quality_score: float = 0.0
    execution_quality_score: float = 0.0
    theme_tradable: bool = False
    theme_fakeout_level: str = "unknown"
    theme_x_score: float = 0.0
    open_confirm_state: str = "unknown"
    daily_height_bucket: str = "mid"
    stock_amount_2m_rank_in_theme_pct: float = 1.0
    stock_amount_ratio_2m_rank_in_theme_pct: float = 1.0
    stock_execution_rank_in_theme_pct: float = 1.0
    stock_shape_rank_in_theme_pct: float = 1.0
    total_score: float = 0.0
    notes: tuple[str, ...] = ()


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
    actionable_missing_count: int = 0
    structural_gap_count: int = 0
    cache_gap_count: int = 0
    dead_symbol_count: int = 0
    current_trade_ready_count: int = 0
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
    session_facts: SessionFacts = field(default_factory=SessionFacts)
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
    auction_top10_amount: float = 0.0
    auction_top20_amount: float = 0.0
    auction_top10_vs_prev_ratio: float = 1.0
    auction_top20_vs_prev_ratio: float = 1.0
    open_2m_top10_amount: float = 0.0
    open_2m_top20_amount: float = 0.0
    open_2m_top10_vs_prev_ratio: float = 1.0
    open_2m_top20_vs_prev_ratio: float = 1.0
    battle_status: str = ""
    notes: tuple[str, ...] = ()
