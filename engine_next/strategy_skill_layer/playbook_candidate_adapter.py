from __future__ import annotations

from engine_next.domain.decision_models import (
    PlaybookCandidateSlice,
    PlaybookCandidateView,
    PlaybookControlRow,
)
from engine_next.strategy_skill_layer.playbook_control import playbook_row_blocks_attack, playbook_row_enabled


NON_ATTACK_ACTIONS = {
    "observe_only",
    "hold_only",
    "avoid_after_failed_promotion",
    "failed_promo_guard",
    "do_not_chase",
    "watch",
    "avoid",
    "avoid_chase",
    "disabled",
}


def build_playbook_candidate_view(
    *,
    symbol: str,
    raw_action: str,
    row: PlaybookControlRow | None,
    source: str,
    playbook: str,
    path_type: str = "",
    action_hint: str = "watch",
    priority_rank: int = 999,
    evidence_refs: tuple[str, ...] = (),
) -> PlaybookCandidateView:
    blocked = playbook_row_blocks_attack(row)
    enabled = playbook_row_enabled(row)
    primary_allowed = bool(
        row is not None
        and enabled
        and not blocked
        and raw_action not in NON_ATTACK_ACTIONS
    )
    if row is None:
        bucket = "unclassified" if not playbook else "inactive"
        reason = "no_playbook_row" if playbook else "no_playbook_owner"
        cap = 0.0
        risks: tuple[str, ...] = ()
    elif blocked:
        bucket = "blocked"
        reason = row.reason or row.action_hint
        cap = row.cap
        risks = row.risk_tags
    elif enabled:
        bucket = "primary" if primary_allowed else "watch"
        reason = row.reason or row.action_hint
        cap = row.cap
        risks = row.risk_tags
    else:
        bucket = "inactive"
        reason = row.reason or row.action_hint
        cap = row.cap
        risks = row.risk_tags
    return PlaybookCandidateView(
        symbol=symbol,
        source=source,
        playbook=playbook,
        path_type=path_type,
        action_hint=action_hint,
        priority_rank=priority_rank,
        display_bucket=bucket,
        primary_allowed=primary_allowed,
        blocked=blocked,
        cap=cap,
        reason=reason,
        risk_tags=risks,
        evidence_refs=evidence_refs,
    )


def playbook_candidate_order_bucket(view: PlaybookCandidateView, *, matrix_ready: bool) -> int:
    if not view.playbook:
        return 3 if matrix_ready else 1
    if view.blocked:
        return 4
    if view.primary_allowed:
        return 0
    if view.display_bucket == "watch":
        return 1
    if view.display_bucket == "inactive":
        return 2
    return 3


def slice_playbook_candidate_views(
    views: tuple[PlaybookCandidateView, ...],
) -> PlaybookCandidateSlice:
    primary = []
    watch = []
    inactive = []
    blocked = []
    unclassified = []
    for view in views:
        if not view.playbook or view.display_bucket == "unclassified":
            unclassified.append(view)
        elif view.blocked or view.display_bucket == "blocked":
            blocked.append(view)
        elif view.primary_allowed or view.display_bucket == "primary":
            primary.append(view)
        elif view.display_bucket == "watch":
            watch.append(view)
        else:
            inactive.append(view)
    return PlaybookCandidateSlice(
        primary=tuple(primary),
        watch=tuple(watch),
        inactive=tuple(inactive),
        blocked=tuple(blocked),
        unclassified=tuple(unclassified),
    )
