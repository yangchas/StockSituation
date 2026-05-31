from __future__ import annotations

from dataclasses import dataclass, field


def _limited(values: tuple[str, ...] | list[str] | None, limit: int = 8) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(str(item) for item in tuple(values)[:limit] if str(item))


def _action_priority(action_hint: str) -> int:
    priority = {
        "support": 0,
        "probe": 1,
        "avoid": 2,
        "avoid_chase": 3,
        "watch": 4,
    }
    return priority.get(action_hint, 5)


@dataclass(frozen=True)
class LocalMetric:
    name: str
    value: float | int | str
    unit: str = ""
    rank_pct: float | None = None
    relation: str = ""


@dataclass(frozen=True)
class LocalSignal:
    signal_id: str
    node_id: str
    scope_type: str
    scope: str
    state: str
    action_hint: str = "watch"
    strength_bucket: str = "unknown"
    metrics: tuple[LocalMetric, ...] = ()
    evidence: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", _limited(self.evidence, 8))
        object.__setattr__(self, "risk_tags", _limited(self.risk_tags, 6))
        object.__setattr__(self, "depends_on", _limited(self.depends_on, 8))


@dataclass(frozen=True)
class LocalStrategyNodeResult:
    node_id: str
    layer: str
    summary_state: str
    action_hint: str = "watch"
    signals: tuple[LocalSignal, ...] = ()
    evidence: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", _limited(self.evidence, 8))
        object.__setattr__(self, "risk_tags", _limited(self.risk_tags, 6))
        object.__setattr__(self, "depends_on", _limited(self.depends_on, 8))


@dataclass(frozen=True)
class LocalStrategyScopeSummary:
    scope_type: str
    scope: str
    action_hint: str = "watch"
    support_count: int = 0
    probe_count: int = 0
    avoid_count: int = 0
    watch_count: int = 0
    states: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()
    risk_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "states", _limited(self.states, 8))
        object.__setattr__(self, "evidence", _limited(self.evidence, 8))
        object.__setattr__(self, "risk_tags", _limited(self.risk_tags, 8))


@dataclass(frozen=True)
class LocalStrategyEvidencePack:
    theme_opportunities: tuple[LocalStrategyScopeSummary, ...] = ()
    theme_risks: tuple[LocalStrategyScopeSummary, ...] = ()
    high_pressure_alerts: tuple[LocalStrategyScopeSummary, ...] = ()
    stock_alignments: tuple[LocalStrategyScopeSummary, ...] = ()
    stock_risks: tuple[LocalStrategyScopeSummary, ...] = ()
    emotion_alerts: tuple[LocalStrategyScopeSummary, ...] = ()
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "notes", _limited(self.notes, 10))


@dataclass(frozen=True)
class LocalStrategyGraph:
    trade_date: str
    phase: str
    nodes: tuple[LocalStrategyNodeResult, ...] = ()
    node_index: dict[str, LocalStrategyNodeResult] = field(default_factory=dict)
    signal_index: dict[str, LocalSignal] = field(default_factory=dict)
    scope_signal_index: dict[str, tuple[str, ...]] = field(default_factory=dict)
    state_signal_index: dict[str, tuple[str, ...]] = field(default_factory=dict)
    dependency_issues: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def node(self, node_id: str) -> LocalStrategyNodeResult | None:
        return self.node_index.get(node_id)

    def signals_for_node(self, node_id: str) -> tuple[LocalSignal, ...]:
        node = self.node_index.get(node_id)
        return tuple(node.signals) if node is not None else ()

    def signals_for_scope(self, scope_type: str, scope: str) -> tuple[LocalSignal, ...]:
        key = f"{scope_type}:{scope}"
        return tuple(self.signal_index[item] for item in self.scope_signal_index.get(key, ()) if item in self.signal_index)

    def signals_for_state(self, state: str) -> tuple[LocalSignal, ...]:
        return tuple(self.signal_index[item] for item in self.state_signal_index.get(state, ()) if item in self.signal_index)

    def scope_summary(self, scope_type: str, scope: str) -> LocalStrategyScopeSummary:
        signals = self.signals_for_scope(scope_type, scope)
        support = sum(1 for item in signals if item.action_hint == "support")
        probe = sum(1 for item in signals if item.action_hint == "probe")
        avoid = sum(1 for item in signals if item.action_hint in {"avoid", "avoid_chase"})
        watch = sum(1 for item in signals if item.action_hint == "watch")
        if avoid and avoid >= support + probe:
            action = "avoid"
        elif support:
            action = "support"
        elif probe:
            action = "probe"
        else:
            action = "watch"
        evidence: list[str] = []
        risks: list[str] = []
        states: list[str] = []
        for signal in signals:
            states.append(f"{signal.node_id}:{signal.state}")
            evidence.extend(signal.evidence[:2])
            risks.extend(signal.risk_tags)
        return LocalStrategyScopeSummary(
            scope_type=scope_type,
            scope=scope,
            action_hint=action,
            support_count=support,
            probe_count=probe,
            avoid_count=avoid,
            watch_count=watch,
            states=tuple(states),
            evidence=tuple(evidence),
            risk_tags=tuple(risks),
        )

    def top_signals(
        self,
        *,
        scope_type: str = "",
        node_id: str = "",
        action_hints: tuple[str, ...] = (),
        states: tuple[str, ...] = (),
        limit: int = 10,
    ) -> tuple[LocalSignal, ...]:
        action_filter = set(action_hints)
        state_filter = set(states)
        rows = tuple(self.signal_index.values())
        if scope_type:
            rows = tuple(item for item in rows if item.scope_type == scope_type)
        if node_id:
            rows = tuple(item for item in rows if item.node_id == node_id)
        if action_filter:
            rows = tuple(item for item in rows if item.action_hint in action_filter)
        if state_filter:
            rows = tuple(item for item in rows if item.state in state_filter)
        return tuple(
            sorted(
                rows,
                key=lambda item: (
                    _action_priority(item.action_hint),
                    item.node_id,
                    item.scope,
                    item.signal_id,
                ),
            )[: max(0, limit)]
        )
