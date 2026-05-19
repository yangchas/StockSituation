from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from engine_next.domain.models import StockStateSnapshot
from engine_next.runtime.theme_consistency_audit import (
    ThemeConsistencyAuditReport,
    ThemeConsistencyIssue,
    build_theme_consistency_audit_report,
    classify_theme_consistency_issue,
)


@dataclass(frozen=True)
class ThemeTradeImpactIssue:
    symbol: str
    name: str
    impact_codes: tuple[str, ...]
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class ThemeTradeImpactAuditReport:
    total_symbols: int
    consistency_issue_symbols: int
    leader_grouping_impact_count: int
    theme_fact_impact_count: int
    plate_bucket_impact_count: int
    trade_label_impact_count: int
    high_priority_impact_count: int
    issues: tuple[ThemeTradeImpactIssue, ...] = ()


def build_theme_trade_impact_audit_report(
    snapshots: Iterable[StockStateSnapshot],
    *,
    max_issues: int = 50,
) -> ThemeTradeImpactAuditReport:
    snapshots = tuple(snapshots)
    consistency = build_theme_consistency_audit_report(snapshots, max_issues=max_issues)

    leader_grouping_impact_count = 0
    theme_fact_impact_count = 0
    plate_bucket_impact_count = 0
    trade_label_impact_count = 0
    high_priority_impact_count = 0
    consistency_issue_symbols = 0
    issues: list[ThemeTradeImpactIssue] = []

    for snapshot in snapshots:
        issue = classify_theme_consistency_issue(snapshot)
        if issue is None:
            continue
        consistency_issue_symbols += 1
        impact_codes = _classify_trade_impacts(issue, snapshot)
        if not impact_codes:
            continue
        if "leader_grouping_impact" in impact_codes:
            leader_grouping_impact_count += 1
        if "theme_fact_impact" in impact_codes:
            theme_fact_impact_count += 1
        if "plate_bucket_impact" in impact_codes:
            plate_bucket_impact_count += 1
        if "trade_label_impact" in impact_codes:
            trade_label_impact_count += 1
        if "high_priority_impact" in impact_codes:
            high_priority_impact_count += 1
        if len(issues) < max_issues:
            issues.append(
                ThemeTradeImpactIssue(
                    symbol=issue.symbol,
                    name=issue.name,
                    impact_codes=impact_codes,
                    reason_codes=issue.issue_codes,
                )
            )

    return ThemeTradeImpactAuditReport(
        total_symbols=consistency.total_symbols,
        consistency_issue_symbols=consistency_issue_symbols,
        leader_grouping_impact_count=leader_grouping_impact_count,
        theme_fact_impact_count=theme_fact_impact_count,
        plate_bucket_impact_count=plate_bucket_impact_count,
        trade_label_impact_count=trade_label_impact_count,
        high_priority_impact_count=high_priority_impact_count,
        issues=tuple(issues),
    )


def _classify_trade_impacts(
    issue: ThemeConsistencyIssue,
    snapshot: StockStateSnapshot | None,
) -> tuple[str, ...]:
    impacts: list[str] = []
    issue_codes = set(issue.issue_codes)
    is_front = bool(snapshot is not None and (snapshot.leader_rank_in_theme <= 3 or snapshot.lb_days >= 1))

    if "runtime_primary_mismatch" in issue_codes and is_front:
        impacts.append("leader_grouping_impact")
    if "runtime_primary_mismatch" in issue_codes or "generic_runtime_fallback" in issue_codes:
        impacts.append("theme_fact_impact")
        impacts.append("trade_label_impact")
    if "multi_token_stat_risk" in issue_codes:
        impacts.append("plate_bucket_impact")
        impacts.append("theme_fact_impact")
    if is_front and ("runtime_primary_mismatch" in issue_codes or "generic_runtime_fallback" in issue_codes):
        impacts.append("high_priority_impact")
    elif is_front and "multi_token_stat_risk" in issue_codes:
        impacts.append("high_priority_impact")

    deduped: list[str] = []
    for code in impacts:
        if code not in deduped:
            deduped.append(code)
    return tuple(deduped)
