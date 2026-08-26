"""Small scheduler/report lifecycle helpers; no scheduling framework."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportExecutionDecision:
    trade_date: str
    event_name: str
    execution_mode: str
    send_eligibility: bool
    reason: str
    dedupe_key: str


@dataclass(frozen=True)
class ReportingEvent:
    trade_date: str
    event_name: str
    scheduled_time: datetime
    actual_time: datetime
    execution_mode: str = "normal"


@dataclass(frozen=True)
class DeliveryClaim:
    allowed: bool
    status: str
    dedupe_key: str


class ReportingLifecycle:
    """Own event delivery eligibility and the application-level dedupe claim."""

    ALLOWED_EVENTS = {"auction_facts_0926", "opening_facts_0932"}
    DEDUPE_TTL_SECONDS = 2 * 24 * 60 * 60

    def __init__(self, *, redis_client: Any | None, enabled: bool = True) -> None:
        self._redis = redis_client
        self._enabled = bool(enabled)

    @staticmethod
    def _key(event: ReportingEvent) -> str:
        return f"{event.trade_date}:{event.event_name}"

    def execution_mode(self, *, event_name: str, actual_time: datetime, requested_mode: str | None = None) -> str:
        if requested_mode in {"manual_audit", "recovery"}:
            return requested_mode
        if event_name == "auction_facts_0926" and time(9, 26) <= actual_time.time() < time(9, 27):
            return "normal"
        if event_name == "opening_facts_0932" and time(9, 32, 10) <= actual_time.time() < time(9, 33):
            return "normal"
        return "recovery"

    def claim(self, event: ReportingEvent, *, report_digest: str) -> DeliveryClaim:
        key = self._key(event)
        if event.event_name not in self.ALLOWED_EVENTS:
            return DeliveryClaim(False, "FAILED", key)
        if not self._enabled or event.execution_mode != "normal":
            return DeliveryClaim(False, "SKIP_" + event.execution_mode.upper(), key)
        if self._redis is None or not hasattr(self._redis, "setnx"):
            # A local-memory claim cannot survive restart and is therefore not
            # safe for production reporting.  Fail closed before SMTP.
            return DeliveryClaim(False, "FAILED", key)
        marker = f"CLAIMED:{report_digest}"
        try:
            claimed = bool(self._redis.setnx(key, marker))
            if claimed and hasattr(self._redis, "expire"):
                self._redis.expire(key, self.DEDUPE_TTL_SECONDS)
            return DeliveryClaim(claimed, "CLAIMED" if claimed else "SKIP_ALREADY_CLAIMED", key)
        except Exception:
            logger.exception("reporting dedupe claim failed closed | key=%s", key)
            return DeliveryClaim(False, "FAILED", key)


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
