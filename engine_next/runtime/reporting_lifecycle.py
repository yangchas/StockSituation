"""Small scheduler/report lifecycle helpers; no scheduling framework."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReportExecutionDecision:
    trade_date: str
    event_name: str
    execution_mode: str
    send_eligibility: bool
    reason: str
    dedupe_key: str


def decide_report_execution(
    *,
    trade_date: str,
    event_name: str,
    historical_replay: bool,
    recovery: bool = False,
    manual_audit: bool = False,
    already_claimed: bool = False,
    reporting_enabled: bool = False,
) -> ReportExecutionDecision:
    """Return an auditable decision without sending or touching storage."""
    if manual_audit:
        mode = "manual_audit"
    elif recovery:
        mode = "recovery"
    else:
        mode = "normal"
    key = f"{str(trade_date).strip()}:{str(event_name).strip()}"
    if historical_replay:
        return ReportExecutionDecision(trade_date, event_name, mode, False, "historical_replay", key)
    if not reporting_enabled:
        return ReportExecutionDecision(trade_date, event_name, mode, False, "reporting_disabled", key)
    if manual_audit or recovery:
        return ReportExecutionDecision(trade_date, event_name, mode, False, "non_normal_execution", key)
    if already_claimed:
        return ReportExecutionDecision(trade_date, event_name, mode, False, "application_dedup_claimed", key)
    return ReportExecutionDecision(trade_date, event_name, mode, True, "eligible", key)
