"""Minimal production reporting orchestration.

The coordinator is intentionally callback based: production-specific Redis/TD
readers are supplied by the runtime, while report construction and delivery
remain pure/reusable.  It is not a scheduler or a new provider framework.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping

from engine_next.runtime.auction_email_report import AuctionEmailReport, build_auction_email_report
from engine_next.runtime.notification_service import RuntimeNotificationService
from engine_next.runtime.open_confirmation import build_open_confirmation_observation
from engine_next.runtime.production_fact_assembly import (
    MappingNotReadyError,
    load_mapping_snapshot,
    freeze_mapping_snapshot,
)
from engine_next.runtime.reporting_lifecycle import ReportingEvent, ReportingLifecycle
from engine_next.runtime.startup_static_loader import StartupStaticDataLoader


logger = logging.getLogger(__name__)
_AUCTION_REPORT_STATUSES = {"COMPLETE", "PARTIAL", "DATA_UNAVAILABLE"}


@dataclass(frozen=True)
class OpeningFactsReport:
    subject: str
    text_body: str
    html_body: str
    html_sha256: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ReportingOutcome:
    event_name: str
    report_status: str
    delivery_status: str
    report_hash: str | None
    dedupe_key: str
    mapping_sha: str | None
    notification_status: str
    trade_date: str = ""
    fact_status: str = "unavailable"
    execution_mode: str = "normal"
    observation_cutoff: str | None = None


def build_opening_facts_report(observation: Mapping[str, Any]) -> OpeningFactsReport:
    """Render the existing OpenConfirmation observation without strategy text."""
    trade_date = str(observation.get("trade_date") or "").strip()
    if len(trade_date) != 10:
        raise ValueError("opening observation trade_date is required")
    market = dict(observation.get("market") or {})
    auction = dict(market.get("auction") or {})
    opening = dict(market.get("open") or {})
    open_source = dict(observation.get("open_source") or {})
    cutoff = str(open_source.get("observation_cutoff") or "unavailable")
    lines = [
        f"# 【开盘事实观察】{trade_date}",
        f"截至：{cutoff}",
        f"数据来源：{observation.get('data_origin', 'unavailable')}；映射一致性：{observation.get('mapping_consistency', 'unavailable')}",
        "",
        "## 全市场事实",
        f"- 09:25 上涨/下跌/平盘：{auction.get('positive_count', 'unavailable')} / {auction.get('negative_count', 'unavailable')} / {auction.get('flat_count', 'unavailable')}",
        f"- 开盘窗口上涨/下跌/平盘：{opening.get('open_up_count', 'unavailable')} / {opening.get('open_down_count', 'unavailable')} / {opening.get('open_flat_count', 'unavailable')}",
        f"- 开盘窗口有效价格股票：{opening.get('open_valid_count', 'unavailable')}",
        f"- 开盘窗口成交金额（amt2m）：{opening.get('open_window_amount_yuan', 'unavailable')}",
        "",
        "## 板块事实变化",
        "|板块|竞价上涨覆盖|开盘上涨覆盖|变化|竞价中位涨幅|开盘中位涨幅|变化|",
        "|---|---:|---:|---|---:|---:|---|",
    ]
    for row in observation.get("plates") or []:
        row = dict(row)
        lines.append(
            f"|{row.get('plate', 'unavailable')}|{row.get('auction_positive_ratio', 'unavailable')}|"
            f"{row.get('open_positive_ratio', 'unavailable')}|{row.get('positive_ratio_delta', 'unavailable')}|"
            f"{row.get('auction_median_change_pct', 'unavailable')}|{row.get('open_median_change_pct', 'unavailable')}|"
            f"{row.get('median_change_pct_delta', 'unavailable')}|"
        )
    lines.extend(["", "## 变化观察"])
    observations = observation.get("observations") or [{"text": "unavailable"}]
    lines.extend(f"- {dict(row).get('text', 'unavailable')}" for row in observations)
    text_body = "\n".join(lines).strip() + "\n"
    subject = f"【开盘事实观察】{trade_date} 截至 {cutoff}"
    html_body = "<html><body><pre>" + text_body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre></body></html>"
    digest = hashlib.sha256(html_body.encode("utf-8")).hexdigest()
    metadata = dict(observation)
    metadata.update({"format": "OpeningFactsReportV1", "subject": subject, "html_sha256": digest, "strategy_impact": "none", "decision_bundle": None})
    return OpeningFactsReport(subject=subject, text_body=text_body, html_body=html_body, html_sha256=digest, metadata=metadata)


class ProductionReportingCoordinator:
    """One-shot report assembly plus optional existing notification delivery."""

    def __init__(
        self,
        *,
        auction_fact_loader: Callable[[str], Any],
        opening_fact_loader: Callable[[str, datetime], Any] | None = None,
        notification_service: RuntimeNotificationService | None = None,
        lifecycle: ReportingLifecycle | None = None,
        mapping_directory: Path | None = None,
        mapping_source: str = "market:stock_plate",
        redis_client: Any | None = None,
        mapping_min_records: int = StartupStaticDataLoader.MIN_EXPECTED_STOCK_PLATE_ROWS,
    ) -> None:
        self._auction_fact_loader = auction_fact_loader
        self._opening_fact_loader = opening_fact_loader
        self._notification_service = notification_service
        self._lifecycle = lifecycle or ReportingLifecycle(redis_client=None, enabled=False)
        self._mapping_directory = Path(mapping_directory) if mapping_directory is not None else None
        self._mapping_source = mapping_source
        self._redis = redis_client
        self._mapping_min_records = int(mapping_min_records)

    @property
    def notification_service(self) -> RuntimeNotificationService | None:
        return self._notification_service

    def prepare_mapping(self, *, trade_date: str, now: datetime) -> dict[str, Any] | None:
        """Freeze once before 09:25 and reload only that artifact afterwards."""
        if self._mapping_directory is None:
            return None
        try:
            existing = load_mapping_snapshot(
                directory=self._mapping_directory,
                trade_date=trade_date,
                minimum_record_count=self._mapping_min_records,
            )
        except Exception as exc:
            logger.error(
                "mapping_ready=false | mapping snapshot invalid; fail closed | trade_date=%s | reason=%s",
                trade_date,
                exc,
            )
            return None
        if existing is not None:
            logger.info(
                "mapping ready | trade_date=%s | record_count=%s | mapping_sha=%s",
                trade_date,
                existing.get("record_count"),
                existing.get("sha256"),
            )
            return existing
        if now.strftime("%H:%M:%S") < "08:30:00" or now.strftime("%H:%M:%S") >= "09:25:00":
            return None
        redis_client = self._redis
        if redis_client is None and self._notification_service is not None:
            redis_client = getattr(self._notification_service, "_redis", None)
        if redis_client is None:
            return None
        try:
            snapshot = freeze_mapping_snapshot(
                redis_client=redis_client,
                directory=self._mapping_directory,
                trade_date=trade_date,
                effective_time=now.isoformat(),
                source=self._mapping_source,
                minimum_record_count=self._mapping_min_records,
            )
            logger.info(
                "mapping ready | trade_date=%s | record_count=%s | mapping_sha=%s",
                trade_date,
                snapshot.get("record_count"),
                snapshot.get("sha256"),
            )
            return snapshot
        except MappingNotReadyError as exc:
            logger.info(
                "mapping_not_ready | trade_date=%s | actual_record_count=%s | required_min_count=%s | reason=%s",
                trade_date,
                getattr(exc, "actual_record_count", "unavailable"),
                self._mapping_min_records,
                exc,
            )
            return None
        except Exception as exc:
            logger.error(
                "mapping freeze failed; fail closed | trade_date=%s | reason=%s",
                trade_date,
                exc,
            )
            return None

    def _mapping_for_event(self, event: ReportingEvent) -> dict[str, Any] | None:
        if self._mapping_directory is None:
            return None
        try:
            snapshot = load_mapping_snapshot(
                directory=self._mapping_directory,
                trade_date=event.trade_date,
                minimum_record_count=self._mapping_min_records,
            )
            if snapshot is None:
                logger.info(
                    "mapping_ready=false | mapping snapshot missing after cutoff | trade_date=%s | event=%s",
                    event.trade_date,
                    event.event_name,
                )
            return snapshot
        except Exception as exc:
            logger.error(
                "mapping snapshot unavailable for reporting event; fail closed | trade_date=%s | event=%s | reason=%s",
                event.trade_date,
                event.event_name,
                exc,
            )
            return None

    @staticmethod
    def _call_loader(loader: Callable[..., Any], trade_date: str, mapping: Mapping[str, Any] | None) -> Any:
        try:
            signature = inspect.signature(loader)
        except (TypeError, ValueError):
            # No safe arity inspection is available: invoke exactly once with
            # the production argument shape and let any error reach handle().
            return loader(trade_date, mapping) if mapping is not None else loader(trade_date)
        if mapping is not None:
            try:
                signature.bind(trade_date, mapping)
            except TypeError:
                signature.bind(trade_date)
                return loader(trade_date)
            return loader(trade_date, mapping)
        try:
            signature.bind(trade_date)
        except TypeError:
            signature.bind(trade_date, None)
            return loader(trade_date, None)
        return loader(trade_date)

    def build_auction(self, *, trade_date: str) -> AuctionEmailReport:
        bundle = self._call_loader(self._auction_fact_loader, trade_date, None)
        if bundle is None:
            return self.build_unavailable_auction(trade_date=trade_date)
        return build_auction_email_report(
            plate_shadow=bundle.plate_shadow,
            auction_evidence={"market_summary": bundle.market_summary, "data_origin": bundle.data_origin},
            market_context={"data_origin": bundle.data_origin, "trade_date": trade_date},
        )

    def _build_auction_for_event(self, event: ReportingEvent, mapping: Mapping[str, Any] | None) -> tuple[AuctionEmailReport, str]:
        # Mapping is authoritative only for plate facts.  The loader still
        # gets one chance to assemble an independent A2 market overview when
        # the frozen snapshot is unavailable; it must not refreeze live Redis
        # mapping after the cutoff.
        bundle = self._call_loader(self._auction_fact_loader, event.trade_date, mapping)
        if bundle is None:
            return self.build_unavailable_auction(trade_date=event.trade_date), "DATA_UNAVAILABLE"
        market_summary = getattr(bundle, "market_summary", None) or {}
        component_statuses = getattr(bundle, "component_statuses", None) or {
            "market_overview": getattr(bundle, "market_summary_status", None) or ("available" if market_summary.get("status") == "available" else "unavailable"),
            "plate_facts": getattr(bundle, "plate_facts_status", None) or ("available" if getattr(bundle, "status", "") == "normal" else "unavailable"),
            "mapping": getattr(bundle, "mapping_status", None) or ("available" if mapping else "unavailable"),
        }
        reasons = list(getattr(bundle, "unavailable_reasons", ()) or ())
        report = build_auction_email_report(
            plate_shadow=bundle.plate_shadow,
            auction_evidence={"market_summary": market_summary, "data_origin": bundle.data_origin, "component_statuses": component_statuses, "unavailable_reasons": reasons},
            market_context={"data_origin": bundle.data_origin, "trade_date": event.trade_date, "mapping_origin": bundle.mapping_origin, "component_statuses": component_statuses, "unavailable_reasons": reasons},
        )
        # The builder/resolver is the sole Auction report-status authority.
        # Coordinator only validates and forwards the final rendered value;
        # it must never resurrect a stale bundle/pre-build status.
        final_status = report.metadata.get("report_status")
        if final_status not in _AUCTION_REPORT_STATUSES:
            raise ValueError(f"invalid auction report_status from builder: {final_status!r}")
        return report, str(final_status)

    @staticmethod
    def _unavailable_shadow(*, trade_date: str) -> dict[str, Any]:
        return {
            "format": "PlateAuctionShadowV1",
            "contract_version": "PlateAuctionShadowV1",
            "trade_date": trade_date,
            "data_origin": "production_realtime",
            "historical_valid": False,
            "mapping_origin": {"canonical": "market:stock_plate", "status": "unavailable"},
            "status": "unavailable",
            "plate_stats": {"0924_to_0925": {}},
            "symbol_details": {"0924_to_0925": {"detail_rows": []}},
            "source_provenance": {"status": "DATA_UNAVAILABLE"},
            "strategy_impact": "none",
            "decision_bundle": None,
        }

    def build_unavailable_auction(self, *, trade_date: str) -> AuctionEmailReport:
        return build_auction_email_report(
            plate_shadow=self._unavailable_shadow(trade_date=trade_date),
            auction_evidence={"data_origin": "production_realtime"},
        )

    def build_opening(self, *, trade_date: str, observation_cutoff: datetime) -> OpeningFactsReport:
        if self._opening_fact_loader is None:
            raise RuntimeError("opening production fact loader is not configured")
        observation = self._opening_fact_loader(trade_date, observation_cutoff)
        return build_opening_facts_report(observation)

    def _build_opening_for_event(self, event: ReportingEvent, mapping: Mapping[str, Any] | None) -> tuple[OpeningFactsReport, str]:
        if self._opening_fact_loader is None or mapping is None:
            return self.build_opening_unavailable_report(trade_date=event.trade_date, observation_cutoff=event.actual_time), "DATA_UNAVAILABLE"
        observation = self._call_opening_loader(self._opening_fact_loader, event.trade_date, event.actual_time, mapping)
        report = build_opening_facts_report(observation)
        status = "COMPLETE" if str(observation.get("open_source", {}).get("status")) == "available" else "PARTIAL"
        return report, status

    def build_opening_unavailable_report(self, *, trade_date: str, observation_cutoff: datetime) -> OpeningFactsReport:
        observation = {
            "format": "OpenConfirmationObservationV1",
            "trade_date": trade_date,
            "data_origin": "production_realtime",
            "historical_valid": False,
            "mapping_consistency": "unavailable",
            "market": {"auction": {}, "open": {"status": "DATA_UNAVAILABLE", "observation_time": observation_cutoff.isoformat()}},
            "plates": [],
            "observations": [{"text": "开盘事实不可用，未使用替代数据。"}],
            "open_source": {"observation_cutoff": observation_cutoff.isoformat(), "status": "DATA_UNAVAILABLE"},
            "strategy_impact": "none",
            "decision_bundle": None,
        }
        return build_opening_facts_report(observation)

    @staticmethod
    def _call_opening_loader(loader: Callable[..., Any], trade_date: str, observation_cutoff: datetime, mapping: Mapping[str, Any]) -> Any:
        try:
            signature = inspect.signature(loader)
        except (TypeError, ValueError):
            return loader(trade_date, observation_cutoff, mapping)
        try:
            signature.bind(trade_date, observation_cutoff, mapping)
        except TypeError:
            signature.bind(trade_date, observation_cutoff)
            return loader(trade_date, observation_cutoff)
        return loader(trade_date, observation_cutoff, mapping)

    @staticmethod
    def _fact_status(report_status: str) -> str:
        return {
            "COMPLETE": "available",
            "PARTIAL": "partial",
            "DATA_UNAVAILABLE": "unavailable",
            "FAILED": "failed",
        }.get(report_status, "unavailable")

    @staticmethod
    def _outcome(
        event: ReportingEvent,
        *,
        report_status: str,
        delivery_status: str,
        report_hash: str | None,
        dedupe_key: str,
        mapping_sha: str | None,
        notification_status: str,
        fact_status: str | None = None,
        execution_mode: str | None = None,
    ) -> ReportingOutcome:
        return ReportingOutcome(
            event_name=event.event_name,
            report_status=report_status,
            delivery_status=delivery_status,
            report_hash=report_hash,
            dedupe_key=dedupe_key,
            mapping_sha=mapping_sha,
            notification_status=notification_status,
            trade_date=event.trade_date,
            fact_status=fact_status or ProductionReportingCoordinator._fact_status(report_status),
            execution_mode=execution_mode or event.execution_mode,
            observation_cutoff=event.actual_time.isoformat() if event.event_name == "opening_facts_0932" else None,
        )

    def handle(self, event: ReportingEvent, *, request: Any | None = None) -> ReportingOutcome:
        """唯一 production reporting entry for auction/open events."""
        if request is None:
            request = SimpleNamespace(trade_date=event.trade_date, now=event.actual_time, historical_replay=False)
        key = f"{event.trade_date}:{event.event_name}"
        mapping = None
        mapping_sha = None
        try:
            effective_mode = self._lifecycle.execution_mode(
                event_name=event.event_name,
                actual_time=event.actual_time,
                requested_mode=event.execution_mode,
            )
        except Exception as exc:
            logger.exception(
                "reporting execution mode resolution failed; isolated from engine loop | event=%s | trade_date=%s | error=%s",
                event.event_name,
                event.trade_date,
                exc,
            )
            return self._outcome(
                event,
                report_status="FAILED",
                delivery_status="NOT_ATTEMPTED",
                report_hash=None,
                dedupe_key=key,
                mapping_sha=None,
                notification_status="NOT_ATTEMPTED",
                fact_status="failed",
                execution_mode=event.execution_mode,
            )
        try:
            mapping = self._mapping_for_event(event)
            mapping_sha = str(mapping.get("sha256")) if mapping else None
            if event.event_name == "auction_facts_0926":
                report, report_status = self._build_auction_for_event(event, mapping)
            elif event.event_name == "opening_facts_0932":
                report, report_status = self._build_opening_for_event(event, mapping)
            else:
                return self._outcome(
                    event,
                    report_status="FAILED",
                    delivery_status="SKIPPED",
                    report_hash=None,
                    dedupe_key=key,
                    mapping_sha=mapping_sha,
                    notification_status="unsupported_event",
                    fact_status="failed",
                    execution_mode=effective_mode,
                )
        except Exception as exc:
            logger.exception(
                "reporting build failed; isolated from engine loop | event=%s | trade_date=%s | mapping_sha=%s | error=%s",
                event.event_name,
                event.trade_date,
                mapping_sha,
                exc,
            )
            return self._outcome(
                event,
                report_status="FAILED",
                delivery_status="NOT_ATTEMPTED",
                report_hash=None,
                dedupe_key=key,
                mapping_sha=mapping_sha,
                notification_status="NOT_ATTEMPTED",
                fact_status="failed",
                execution_mode=effective_mode,
            )
        # Building is independent from delivery capability.  Manual audits and
        # recovery runs may render a report even when no notifier is configured;
        # they must never inspect notifier availability or claim the live key.
        if effective_mode in {"manual_audit", "recovery"}:
            status = "SKIP_" + effective_mode.upper()
            return self._outcome(
                event,
                report_status=report_status,
                delivery_status=status,
                report_hash=report.html_sha256,
                dedupe_key=key,
                mapping_sha=mapping_sha,
                notification_status=status,
                execution_mode=effective_mode,
            )
        if event.event_name == "auction_facts_0926":
            notify = self._notification_service.notify_auction_report if self._notification_service else None
        else:
            notify = self._notification_service.notify_open_confirmation_report if self._notification_service else None
        # A disabled/unconfigured delivery path must not consume the day's
        # claim.  Otherwise a later runtime reconfiguration could be blocked
        # by a claim created before any notification was possible.
        if notify is None or not bool(getattr(self._notification_service, "enabled", True)):
            return self._outcome(
                event,
                report_status=report_status,
                delivery_status="FAILED",
                report_hash=report.html_sha256,
                dedupe_key=key,
                mapping_sha=mapping_sha,
                notification_status="notification_unavailable",
                execution_mode=effective_mode,
            )
        try:
            claim = self._lifecycle.claim(event, report_digest=report.html_sha256)
        except Exception as exc:
            logger.exception("reporting delivery decision failed; isolated from engine loop | event=%s | error=%s", event.event_name, exc)
            return self._outcome(
                event,
                report_status="FAILED",
                delivery_status="NOT_ATTEMPTED",
                report_hash=report.html_sha256,
                dedupe_key=key,
                mapping_sha=mapping_sha,
                notification_status="NOT_ATTEMPTED",
                fact_status="failed",
                execution_mode=effective_mode,
            )
        if not claim.allowed:
            return self._outcome(
                event,
                report_status=report_status,
                delivery_status=claim.status,
                report_hash=report.html_sha256,
                dedupe_key=claim.dedupe_key,
                mapping_sha=mapping_sha,
                notification_status=claim.status,
                execution_mode=effective_mode,
            )
        try:
            delivered = notify(report=report, request=request, preclaimed=True)
        except Exception as exc:
            logger.exception(
                "reporting notification failed after claim; no retry | event=%s | dedupe_key=%s | error=%s",
                event.event_name,
                claim.dedupe_key,
                exc,
            )
            self._lifecycle.record_delivery(event, status="FAILED")
            return self._outcome(
                event,
                report_status=report_status,
                delivery_status="FAILED",
                report_hash=report.html_sha256,
                dedupe_key=claim.dedupe_key,
                mapping_sha=mapping_sha,
                notification_status="FAILED",
                execution_mode=effective_mode,
            )
        status = "ACCEPTED" if delivered else "FAILED"
        self._lifecycle.record_delivery(event, status=status)
        return self._outcome(
            event,
            report_status=report_status,
            delivery_status=status,
            report_hash=report.html_sha256,
            dedupe_key=claim.dedupe_key,
            mapping_sha=mapping_sha,
            notification_status=status,
            execution_mode=effective_mode,
        )
