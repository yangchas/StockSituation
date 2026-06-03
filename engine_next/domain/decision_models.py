from __future__ import annotations

from dataclasses import dataclass, field

from engine_next.domain.local_strategy_models import LocalStrategyEvidencePack, LocalStrategyGraph


MAX_EVIDENCE_REFS = 5
MAX_REASON_CODES = 3
MAX_RISK_TAGS = 3
MAX_INVALIDATION_POINTS = 2
MAX_DECISION_METRICS = 12
MAX_DECISION_METRIC_VALUES = 16


def _limit_tuple(values: tuple[str, ...] | list[str] | None, limit: int) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(str(item) for item in tuple(values)[:limit] if str(item))


def _limit_metric_values(values: tuple[tuple[str, float], ...] | list[tuple[str, float]] | None) -> tuple[tuple[str, float], ...]:
    if not values:
        return ()
    output: list[tuple[str, float]] = []
    for name, value in tuple(values)[:MAX_DECISION_METRIC_VALUES]:
        text = str(name or "")
        if not text:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        output.append((text, number))
    return tuple(output)


@dataclass(frozen=True)
class DecisionTrace:
    """Minimal audit trail shared by local, hypothesis, and global decisions."""

    decision_id: str
    decision_type: str
    scope: str
    phase: str
    trade_date: str
    state: str = "unknown"
    action_hint: str = "watch"
    confidence_bucket: str = "unknown"
    evidence_refs: tuple[str, ...] = ()
    lower_decision_refs: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()
    reject_reason: str = ""
    invalidation_points: tuple[str, ...] = ()
    metrics: tuple[str, ...] = ()
    metric_values: tuple[tuple[str, float], ...] = ()
    evidence_summary: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_refs", _limit_tuple(self.evidence_refs, MAX_EVIDENCE_REFS))
        object.__setattr__(self, "reason_codes", _limit_tuple(self.reason_codes, MAX_REASON_CODES))
        object.__setattr__(self, "risk_tags", _limit_tuple(self.risk_tags, MAX_RISK_TAGS))
        object.__setattr__(self, "invalidation_points", _limit_tuple(self.invalidation_points, MAX_INVALIDATION_POINTS))
        object.__setattr__(self, "metrics", _limit_tuple(self.metrics, MAX_DECISION_METRICS))
        object.__setattr__(self, "metric_values", _limit_metric_values(self.metric_values))
        object.__setattr__(self, "lower_decision_refs", tuple(str(item) for item in self.lower_decision_refs if str(item)))
        object.__setattr__(self, "evidence_summary", tuple(str(item) for item in self.evidence_summary[:5] if str(item)))


@dataclass(frozen=True)
class StockLocalDecision:
    trace: DecisionTrace
    symbol: str
    theme_name: str = ""
    role_hint: str = "unknown"
    entry_behavior: str = "unknown"
    entry_behavior_label: str = ""
    evidence_text: str = ""
    evidence_labels: tuple[str, ...] = ()
    local_rank: int = 999


@dataclass(frozen=True)
class HighFocusDecision:
    trace: DecisionTrace
    feedback_state: str = "unknown"
    promotion_quality: str = "unknown"
    risk_spread_level: str = "unknown"
    leader_drive_themes: tuple[str, ...] = ()
    failed_high_themes: tuple[str, ...] = ()


@dataclass(frozen=True)
class FocusAssetStressDecision:
    trace: DecisionTrace
    stress_state: str = "unknown"
    spread_level: str = "unknown"
    stressed_themes: tuple[str, ...] = ()
    dragon_alone_themes: tuple[str, ...] = ()
    retreat_symbols: tuple[str, ...] = ()
    core_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "stressed_themes", _limit_tuple(self.stressed_themes, 8))
        object.__setattr__(self, "dragon_alone_themes", _limit_tuple(self.dragon_alone_themes, 6))
        object.__setattr__(self, "retreat_symbols", _limit_tuple(self.retreat_symbols, 8))
        object.__setattr__(self, "core_symbols", _limit_tuple(self.core_symbols, 8))


@dataclass(frozen=True)
class ThemeLocalDecision:
    trace: DecisionTrace
    theme_name: str
    local_script_hint: str = "mixed"
    local_validation_hint: str = "watch_like"
    spread_level: str = "unknown"
    leader_drive_type: str = "unknown"
    top_local_candidates: tuple[str, ...] = ()


@dataclass(frozen=True)
class ThemeRelativeDecision:
    trace: DecisionTrace
    leading_themes: tuple[str, ...] = ()
    rising_themes: tuple[str, ...] = ()
    fading_themes: tuple[str, ...] = ()
    fake_rotation_themes: tuple[str, ...] = ()
    migration_candidates: tuple[str, ...] = ()
    mainline_candidates: tuple[str, ...] = ()
    rotation_candidates: tuple[str, ...] = ()
    risk_themes: tuple[str, ...] = ()


@dataclass(frozen=True)
class TimeframeEvidence:
    timeframe: str
    scope_type: str
    scope: str
    state: str = "unknown"
    action_hint: str = "watch"
    rank: int = 999
    metrics: tuple[str, ...] = ()
    metric_values: tuple[tuple[str, float], ...] = ()
    evidence_refs: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", _limit_tuple(self.metrics, 6))
        object.__setattr__(self, "metric_values", _limit_metric_values(self.metric_values))
        object.__setattr__(self, "evidence_refs", _limit_tuple(self.evidence_refs, MAX_EVIDENCE_REFS))
        object.__setattr__(self, "risk_tags", _limit_tuple(self.risk_tags, MAX_RISK_TAGS))


@dataclass(frozen=True)
class TemporalMemoryLine:
    plate_name: str
    process_state: str = "observe"
    previous_process_state: str = ""
    transition_state: str = "steady"
    sample_count: int = 1
    hot_rank: int = 999
    previous_hot_rank: int = 999
    amount_2m: float = 0.0
    previous_amount_2m: float = 0.0
    amount_5m: float = 0.0
    previous_amount_5m: float = 0.0
    net_inflow_yi_delta: float = 0.0
    previous_net_inflow_yi_delta: float = 0.0
    macro_confirmed: bool = False
    macro_score: int = 0


@dataclass(frozen=True)
class TemporalMigrationDecision:
    trace: DecisionTrace
    hot_plate_anchor: str = ""
    exchange_state: str = "unknown"
    target_themes: tuple[str, ...] = ()
    source_themes: tuple[str, ...] = ()
    fading_themes: tuple[str, ...] = ()
    timeframe_evidence: tuple[TimeframeEvidence, ...] = ()
    chain_summary: tuple[str, ...] = ()
    memory_lines: tuple[TemporalMemoryLine, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "target_themes", _limit_tuple(self.target_themes, 6))
        object.__setattr__(self, "source_themes", _limit_tuple(self.source_themes, 6))
        object.__setattr__(self, "fading_themes", _limit_tuple(self.fading_themes, 6))
        object.__setattr__(self, "timeframe_evidence", tuple(self.timeframe_evidence[:24]))
        object.__setattr__(self, "chain_summary", _limit_tuple(self.chain_summary, 8))
        object.__setattr__(self, "memory_lines", tuple(self.memory_lines[:16]))


@dataclass(frozen=True)
class HotPlateMetricLine:
    plate_name: str
    rank: int = 999
    yest_rank: int = 999
    change_pct: float = 0.0
    strength: float = 0.0
    net_inflow_yi: float = 0.0
    hot_value: float = 0.0
    amount_2m: float = 0.0
    front_2m_count: int = 0
    high_open_fail_count: int = 0
    net_inflow_yi_delta: float = 0.0
    spread_level: str = "unknown"
    strength_rank_pct: float = 1.0
    change_rank_pct: float = 1.0
    inflow_rank_pct: float = 1.0
    hot_rank_pct: float = 1.0
    amount_2m_rank_pct: float = 1.0
    state: str = "observe"


@dataclass(frozen=True)
class HotPlateAnchorDecision:
    trace: DecisionTrace
    anchor_state: str = "unknown"
    primary_themes: tuple[str, ...] = ()
    continuation_themes: tuple[str, ...] = ()
    rotation_themes: tuple[str, ...] = ()
    fading_themes: tuple[str, ...] = ()
    fakeout_themes: tuple[str, ...] = ()
    hot_evidence: tuple[str, ...] = ()
    metric_lines: tuple[HotPlateMetricLine, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "primary_themes", _limit_tuple(self.primary_themes, 6))
        object.__setattr__(self, "continuation_themes", _limit_tuple(self.continuation_themes, 6))
        object.__setattr__(self, "rotation_themes", _limit_tuple(self.rotation_themes, 6))
        object.__setattr__(self, "fading_themes", _limit_tuple(self.fading_themes, 6))
        object.__setattr__(self, "fakeout_themes", _limit_tuple(self.fakeout_themes, 6))
        object.__setattr__(self, "hot_evidence", _limit_tuple(self.hot_evidence, 8))
        object.__setattr__(self, "metric_lines", tuple(self.metric_lines[:12]))


@dataclass(frozen=True)
class MarketHypothesis:
    hypothesis_id: str
    script: str
    claim: str
    phase: str
    scope: str = "market"
    playbook: str = "unknown"
    psychology: str = ""
    risk_constraint: str = ""
    microstructure_requirement: str = ""
    trigger_refs: tuple[str, ...] = ()
    source_local_decision_refs: tuple[str, ...] = ()
    required_validations: tuple[str, ...] = ()
    invalidation_points: tuple[str, ...] = ()
    evidence_summary: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "trigger_refs", _limit_tuple(self.trigger_refs, MAX_EVIDENCE_REFS))
        object.__setattr__(self, "invalidation_points", _limit_tuple(self.invalidation_points, MAX_INVALIDATION_POINTS))
        object.__setattr__(self, "required_validations", tuple(str(item) for item in self.required_validations[:5] if str(item)))
        object.__setattr__(self, "source_local_decision_refs", tuple(str(item) for item in self.source_local_decision_refs if str(item)))
        object.__setattr__(self, "evidence_summary", tuple(str(item) for item in self.evidence_summary[:5] if str(item)))


@dataclass(frozen=True)
class HypothesisValidation:
    hypothesis_id: str
    result: str = "pending"
    passed_checks: tuple[str, ...] = ()
    failed_checks: tuple[str, ...] = ()
    missing_checks: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    lower_decision_refs: tuple[str, ...] = ()
    next_action_hint: str = "watch"

    def __post_init__(self) -> None:
        object.__setattr__(self, "passed_checks", tuple(str(item) for item in self.passed_checks[:5] if str(item)))
        object.__setattr__(self, "failed_checks", tuple(str(item) for item in self.failed_checks[:5] if str(item)))
        object.__setattr__(self, "missing_checks", tuple(str(item) for item in self.missing_checks[:5] if str(item)))
        object.__setattr__(self, "evidence_refs", _limit_tuple(self.evidence_refs, MAX_EVIDENCE_REFS))
        object.__setattr__(self, "lower_decision_refs", tuple(str(item) for item in self.lower_decision_refs if str(item)))


@dataclass(frozen=True)
class GlobalMarketDecision:
    trace: DecisionTrace
    market_script: str = "unknown"
    main_attack_theme: str = ""
    secondary_themes: tuple[str, ...] = ()
    watch_themes: tuple[str, ...] = ()
    avoid_themes: tuple[str, ...] = ()
    position_cap: float = 0.0


@dataclass(frozen=True)
class FinalCandidateDecision:
    trace: DecisionTrace
    symbol: str
    theme_name: str = ""
    bucket: str = "watch"
    action: str = "watch"
    path_type: str = "unknown"
    playbook: str = "watch"
    priority_rank: int = 999
    risk_level: str = "normal"


@dataclass(frozen=True)
class ShadowTakeoverDecision:
    trace: DecisionTrace
    allowed: bool = False
    mode: str = "shadow_only"
    primary_symbols: tuple[str, ...] = ()
    block_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "primary_symbols", _limit_tuple(self.primary_symbols, 5))
        object.__setattr__(self, "block_reasons", _limit_tuple(self.block_reasons, 5))


@dataclass(frozen=True)
class PlaybookControlRow:
    playbook: str
    enabled: bool = False
    action_hint: str = "watch"
    cap: float = 0.0
    phase: str = ""
    reason: str = ""
    risk_tags: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "risk_tags", _limit_tuple(self.risk_tags, MAX_RISK_TAGS))
        object.__setattr__(self, "evidence_refs", _limit_tuple(self.evidence_refs, MAX_EVIDENCE_REFS))


@dataclass(frozen=True)
class PlaybookControlMatrix:
    phase: str
    rows: tuple[PlaybookControlRow, ...] = ()
    active_playbooks: tuple[str, ...] = ()
    blocked_playbooks: tuple[str, ...] = ()
    max_cap: float = 0.0
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "active_playbooks", _limit_tuple(self.active_playbooks, 8))
        object.__setattr__(self, "blocked_playbooks", _limit_tuple(self.blocked_playbooks, 8))
        object.__setattr__(self, "notes", _limit_tuple(self.notes, 8))


@dataclass(frozen=True)
class PlaybookCandidateView:
    symbol: str
    source: str = "legacy"
    playbook: str = ""
    path_type: str = ""
    action_hint: str = "watch"
    priority_rank: int = 999
    display_bucket: str = "unclassified"
    primary_allowed: bool = False
    blocked: bool = False
    cap: float = 0.0
    reason: str = ""
    risk_tags: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "risk_tags", _limit_tuple(self.risk_tags, MAX_RISK_TAGS))
        object.__setattr__(self, "evidence_refs", _limit_tuple(self.evidence_refs, MAX_EVIDENCE_REFS))


@dataclass(frozen=True)
class PlaybookCandidateSlice:
    primary: tuple[PlaybookCandidateView, ...] = ()
    watch: tuple[PlaybookCandidateView, ...] = ()
    inactive: tuple[PlaybookCandidateView, ...] = ()
    blocked: tuple[PlaybookCandidateView, ...] = ()
    unclassified: tuple[PlaybookCandidateView, ...] = ()

    @property
    def total_count(self) -> int:
        return len(self.primary) + len(self.watch) + len(self.inactive) + len(self.blocked) + len(self.unclassified)


@dataclass(frozen=True)
class PlaybookOutputSummary:
    primary_symbols: tuple[str, ...] = ()
    watch_symbols: tuple[str, ...] = ()
    inactive_symbols: tuple[str, ...] = ()
    blocked_symbols: tuple[str, ...] = ()
    primary_actions: tuple[str, ...] = ()
    watch_actions: tuple[str, ...] = ()
    repair_actions: tuple[str, ...] = ()
    avoid_actions: tuple[str, ...] = ()
    mode_note: str = ""
    primary_reasons: tuple[str, ...] = ()
    watch_reasons: tuple[str, ...] = ()
    reject_reasons: tuple[str, ...] = ()
    invalidation_points: tuple[str, ...] = ()
    narrative_lines: tuple[str, ...] = ()
    migration_lines: tuple[str, ...] = ()
    quant_lines: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "primary_symbols", _limit_tuple(self.primary_symbols, 5))
        object.__setattr__(self, "watch_symbols", _limit_tuple(self.watch_symbols, 8))
        object.__setattr__(self, "inactive_symbols", _limit_tuple(self.inactive_symbols, 8))
        object.__setattr__(self, "blocked_symbols", _limit_tuple(self.blocked_symbols, 8))
        object.__setattr__(self, "primary_actions", _limit_tuple(self.primary_actions, 5))
        object.__setattr__(self, "watch_actions", _limit_tuple(self.watch_actions, 8))
        object.__setattr__(self, "repair_actions", _limit_tuple(self.repair_actions, 5))
        object.__setattr__(self, "avoid_actions", _limit_tuple(self.avoid_actions, 8))
        object.__setattr__(self, "primary_reasons", _limit_tuple(self.primary_reasons, 5))
        object.__setattr__(self, "watch_reasons", _limit_tuple(self.watch_reasons, 5))
        object.__setattr__(self, "reject_reasons", _limit_tuple(self.reject_reasons, 8))
        object.__setattr__(self, "invalidation_points", _limit_tuple(self.invalidation_points, 8))
        object.__setattr__(self, "narrative_lines", _limit_tuple(self.narrative_lines, 6))
        object.__setattr__(self, "migration_lines", _limit_tuple(self.migration_lines, 8))
        object.__setattr__(self, "quant_lines", _limit_tuple(self.quant_lines, 8))
        object.__setattr__(self, "risk_tags", _limit_tuple(self.risk_tags, 8))


@dataclass(frozen=True)
class FocusDisplayPayload:
    symbol: str
    view: PlaybookCandidateView | None = None
    legacy_source: str = ""
    display_group: str = "watch"


@dataclass(frozen=True)
class DecisionBundle:
    stock_local_decisions: tuple[StockLocalDecision, ...] = ()
    local_strategy_graph: LocalStrategyGraph | None = None
    local_strategy_evidence_pack: LocalStrategyEvidencePack | None = None
    high_focus_decision: HighFocusDecision | None = None
    focus_asset_stress_decision: FocusAssetStressDecision | None = None
    theme_local_decisions: tuple[ThemeLocalDecision, ...] = ()
    hot_plate_anchor_decision: HotPlateAnchorDecision | None = None
    temporal_migration_decision: TemporalMigrationDecision | None = None
    theme_relative_decision: ThemeRelativeDecision | None = None
    hypotheses: tuple[MarketHypothesis, ...] = ()
    hypothesis_validations: tuple[HypothesisValidation, ...] = ()
    global_decision: GlobalMarketDecision | None = None
    final_candidates: tuple[FinalCandidateDecision, ...] = ()
    shadow_takeover_decision: ShadowTakeoverDecision | None = None
    playbook_control_matrix: PlaybookControlMatrix | None = None
    playbook_candidate_slice: PlaybookCandidateSlice = field(default_factory=PlaybookCandidateSlice)
    playbook_output_summary: PlaybookOutputSummary = field(default_factory=PlaybookOutputSummary)
    notes: tuple[str, ...] = field(default_factory=tuple)
