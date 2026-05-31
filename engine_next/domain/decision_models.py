from __future__ import annotations

from dataclasses import dataclass, field

from engine_next.domain.local_strategy_models import LocalStrategyEvidencePack, LocalStrategyGraph


MAX_EVIDENCE_REFS = 5
MAX_REASON_CODES = 3
MAX_RISK_TAGS = 3
MAX_INVALIDATION_POINTS = 2


def _limit_tuple(values: tuple[str, ...] | list[str] | None, limit: int) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(str(item) for item in tuple(values)[:limit] if str(item))


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
    evidence_summary: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_refs", _limit_tuple(self.evidence_refs, MAX_EVIDENCE_REFS))
        object.__setattr__(self, "reason_codes", _limit_tuple(self.reason_codes, MAX_REASON_CODES))
        object.__setattr__(self, "risk_tags", _limit_tuple(self.risk_tags, MAX_RISK_TAGS))
        object.__setattr__(self, "invalidation_points", _limit_tuple(self.invalidation_points, MAX_INVALIDATION_POINTS))
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
    theme_local_decisions: tuple[ThemeLocalDecision, ...] = ()
    theme_relative_decision: ThemeRelativeDecision | None = None
    hypotheses: tuple[MarketHypothesis, ...] = ()
    hypothesis_validations: tuple[HypothesisValidation, ...] = ()
    global_decision: GlobalMarketDecision | None = None
    final_candidates: tuple[FinalCandidateDecision, ...] = ()
    shadow_takeover_decision: ShadowTakeoverDecision | None = None
    playbook_control_matrix: PlaybookControlMatrix | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)
