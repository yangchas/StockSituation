from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from engine_next.domain.models import StockStateSnapshot
from engine_next.runtime.plate_mapping_registry import is_generic_plate, normalize_plate_name, split_plate_tokens
from engine_next.runtime.theme_name_resolver import resolve_primary_theme_name, resolve_theme_names


@dataclass(frozen=True)
class ThemeConsistencyIssue:
    symbol: str
    name: str
    runtime_plate: str
    resolved_primary: str
    resolved_names: tuple[str, ...]
    statistic_tokens: tuple[str, ...]
    issue_codes: tuple[str, ...]


@dataclass(frozen=True)
class ThemeConsistencyAuditReport:
    total_symbols: int
    resolved_symbols: int
    unresolved_symbols: int
    generic_runtime_plate_count: int
    generic_primary_fallback_count: int
    multi_token_stat_risk_count: int
    runtime_primary_mismatch_count: int
    issue_count: int
    issue_signal_total: int
    issues: tuple[ThemeConsistencyIssue, ...] = ()


def build_theme_consistency_audit_report(
    snapshots: Iterable[StockStateSnapshot],
    *,
    max_issues: int = 50,
) -> ThemeConsistencyAuditReport:
    total_symbols = 0
    resolved_symbols = 0
    unresolved_symbols = 0
    generic_runtime_plate_count = 0
    generic_primary_fallback_count = 0
    multi_token_stat_risk_count = 0
    runtime_primary_mismatch_count = 0
    issue_symbol_count = 0
    issues: list[ThemeConsistencyIssue] = []

    for snapshot in snapshots:
        total_symbols += 1
        runtime_plate = normalize_plate_name(snapshot.plate)
        if runtime_plate and is_generic_plate(runtime_plate):
            generic_runtime_plate_count += 1
        resolved_primary = resolve_primary_theme_name(snapshot)
        if resolved_primary:
            resolved_symbols += 1
        else:
            unresolved_symbols += 1
        issue = classify_theme_consistency_issue(snapshot)
        if issue is None:
            continue
        issue_symbol_count += 1
        issue_code_set = set(issue.issue_codes)
        if "generic_runtime_fallback" in issue_code_set:
            generic_primary_fallback_count += 1
        if "runtime_primary_mismatch" in issue_code_set:
            runtime_primary_mismatch_count += 1
        if "multi_token_stat_risk" in issue_code_set:
            multi_token_stat_risk_count += 1
        if len(issues) < max_issues:
            issues.append(issue)

    return ThemeConsistencyAuditReport(
        total_symbols=total_symbols,
        resolved_symbols=resolved_symbols,
        unresolved_symbols=unresolved_symbols,
        generic_runtime_plate_count=generic_runtime_plate_count,
        generic_primary_fallback_count=generic_primary_fallback_count,
        multi_token_stat_risk_count=multi_token_stat_risk_count,
        runtime_primary_mismatch_count=runtime_primary_mismatch_count,
        issue_count=issue_symbol_count,
        issue_signal_total=(
            unresolved_symbols
            + generic_primary_fallback_count
            + multi_token_stat_risk_count
            + runtime_primary_mismatch_count
        ),
        issues=tuple(issues),
    )


def classify_theme_consistency_issue(snapshot: StockStateSnapshot) -> ThemeConsistencyIssue | None:
    runtime_plate = normalize_plate_name(snapshot.plate)
    resolved_primary = resolve_primary_theme_name(snapshot)
    resolved_names = resolve_theme_names(snapshot)
    statistic_tokens = _resolve_statistic_tokens(snapshot)
    issue_codes: list[str] = []

    if not resolved_primary:
        issue_codes.append("unresolved")

    if runtime_plate and is_generic_plate(runtime_plate):
        if resolved_primary and resolved_primary != runtime_plate:
            issue_codes.append("generic_runtime_fallback")

    if runtime_plate and resolved_primary and runtime_plate != resolved_primary and not is_generic_plate(runtime_plate):
        issue_codes.append("runtime_primary_mismatch")

    if len(statistic_tokens) > 1:
        issue_codes.append("multi_token_stat_risk")

    if not issue_codes:
        return None

    return ThemeConsistencyIssue(
        symbol=str(snapshot.symbol or ""),
        name=str(snapshot.name or ""),
        runtime_plate=runtime_plate,
        resolved_primary=resolved_primary,
        resolved_names=resolved_names,
        statistic_tokens=statistic_tokens,
        issue_codes=tuple(issue_codes),
    )


def _resolve_statistic_tokens(snapshot: StockStateSnapshot) -> tuple[str, ...]:
    names: list[str] = []
    for raw_name in snapshot.real_plate_names or (snapshot.plate,):
        for token in split_plate_tokens(raw_name):
            cleaned = normalize_plate_name(token)
            if not cleaned or is_generic_plate(cleaned) or cleaned in names:
                continue
            names.append(cleaned)
    if not names and snapshot.plate:
        cleaned = normalize_plate_name(snapshot.plate)
        if cleaned and not is_generic_plate(cleaned):
            names.append(cleaned)
    return tuple(names)
