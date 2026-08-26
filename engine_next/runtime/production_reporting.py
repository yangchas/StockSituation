"""Minimal production reporting orchestration.

The coordinator is intentionally callback based: production-specific Redis/TD
readers are supplied by the runtime, while report construction and delivery
remain pure/reusable.  It is not a scheduler or a new provider framework.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping

from engine_next.runtime.auction_email_report import AuctionEmailReport, build_auction_email_report
from engine_next.runtime.notification_service import RuntimeNotificationService
from engine_next.runtime.open_confirmation import build_open_confirmation_observation
from engine_next.runtime.production_fact_assembly import load_mapping_snapshot, freeze_mapping_snapshot
from engine_next.runtime.reporting_lifecycle import ReportingEvent, ReportingLifecycle


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
    ) -> None:
        self._auction_fact_loader = auction_fact_loader
        self._opening_fact_loader = opening_fact_loader
        self._notification_service = notification_service
        self._lifecycle = lifecycle or ReportingLifecycle(redis_client=None, enabled=False)
        self._mapping_directory = Path(mapping_directory) if mapping_directory is not None else None
        self._mapping_source = mapping_source
        self._redis = redis_client

    @property
    def notification_service(self) -> RuntimeNotificationService | None:
        return self._notification_service

    def prepare_mapping(self, *, trade_date: str, now: datetime) -> dict[str, Any] | None:
        """Freeze once before 09:25 and reload only that artifact afterwards."""
        if self._mapping_directory is None:
            return None
        existing = load_mapping_snapshot(directory=self._mapping_directory, trade_date=trade_date)
        if existing is not None:
            return existing
        if now.strftime("%H:%M:%S") < "08:30:00" or now.strftime("%H:%M:%S") >= "09:25:00":
            return None
        redis_client = self._redis
        if redis_client is None and self._notification_service is not None:
            redis_client = getattr(self._notification_service, "_redis", None)
        if redis_client is None:
            return None
        try:
            return freeze_mapping_snapshot(
                redis_client=redis_client,
                directory=self._mapping_directory,
                trade_date=trade_date,
                effective_time=now.isoformat(),
                source=self._mapping_source,
            )
        except Exception:
            return None

    def _mapping_for_event(self, event: ReportingEvent) -> dict[str, Any] | None:
        if self._mapping_directory is None:
            return None
        try:
            return load_mapping_snapshot(directory=self._mapping_directory, trade_date=event.trade_date)
        except Exception:
            return None

    @staticmethod
    def _call_loader(loader: Callable[..., Any], trade_date: str, mapping: Mapping[str, Any] | None) -> Any:
        if mapping is None:
            return loader(trade_date)
        try:
            return loader(trade_date, mapping)
        except TypeError as exc:
            # Keep the pre-Release-B one-argument test/adapter contract.
            if "positional" not in str(exc) and "argument" not in str(exc):
                raise
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
        if self._mapping_directory is not None and mapping is None:
            return self.build_unavailable_auction(trade_date=event.trade_date), "DATA_UNAVAILABLE"
        try:
            bundle = self._call_loader(self._auction_fact_loader, event.trade_date, mapping)
            if bundle is None:
                return self.build_unavailable_auction(trade_date=event.trade_date), "DATA_UNAVAILABLE"
            report = build_auction_email_report(
                plate_shadow=bundle.plate_shadow,
                auction_evidence={"market_summary": bundle.market_summary, "data_origin": bundle.data_origin},
                market_context={"data_origin": bundle.data_origin, "trade_date": event.trade_date, "mapping_origin": bundle.mapping_origin},
            )
            status = "COMPLETE" if getattr(bundle, "status", "") == "normal" else "PARTIAL"
            return report, status
        except Exception:
            return self.build_unavailable_auction(trade_date=event.trade_date), "DATA_UNAVAILABLE"

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

    def send_auction(self, *, trade_date: str, request: Any, send_eligibility: bool) -> tuple[str, str]:
        report = self.build_auction(trade_date=trade_date)
        if not send_eligibility or self._notification_service is None:
            return "built", report.html_sha256
        delivered = self._notification_service.notify_auction_report(report=report, request=request)
        return ("sent" if delivered else "not_sent"), report.html_sha256

    def send_auction_unavailable(self, *, trade_date: str, request: Any, send_eligibility: bool) -> tuple[str, str]:
        report = self.build_unavailable_auction(trade_date=trade_date)
        if not send_eligibility or self._notification_service is None:
            return "built", report.html_sha256
        delivered = self._notification_service.notify_auction_report(report=report, request=request)
        return ("sent" if delivered else "not_sent"), report.html_sha256

    def build_opening(self, *, trade_date: str, observation_cutoff: datetime) -> OpeningFactsReport:
        if self._opening_fact_loader is None:
            raise RuntimeError("opening production fact loader is not configured")
        observation = self._opening_fact_loader(trade_date, observation_cutoff)
        return build_opening_facts_report(observation)

    def _build_opening_for_event(self, event: ReportingEvent, mapping: Mapping[str, Any] | None) -> tuple[OpeningFactsReport, str]:
        if self._opening_fact_loader is None or mapping is None:
            return self.send_opening_unavailable_report(trade_date=event.trade_date, observation_cutoff=event.actual_time), "DATA_UNAVAILABLE"
        try:
            observation = self._opening_fact_loader(event.trade_date, event.actual_time, mapping)
        except TypeError as exc:
            if "positional" not in str(exc) and "argument" not in str(exc):
                raise
            observation = self._opening_fact_loader(event.trade_date, event.actual_time)
        report = build_opening_facts_report(observation)
        status = "COMPLETE" if str(observation.get("open_source", {}).get("status")) == "available" else "PARTIAL"
        return report, status

    def send_opening_unavailable_report(self, *, trade_date: str, observation_cutoff: datetime) -> OpeningFactsReport:
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

    def handle(self, event: ReportingEvent, *, request: Any | None = None) -> ReportingOutcome:
        """唯一 production reporting entry for auction/open events."""
        mapping = self._mapping_for_event(event)
        mapping_sha = str(mapping.get("sha256")) if mapping else None
        if request is None:
            request = SimpleNamespace(trade_date=event.trade_date, now=event.actual_time, historical_replay=False)
        if event.event_name == "auction_facts_0926":
            report, report_status = self._build_auction_for_event(event, mapping)
            notify = self._notification_service.notify_auction_report if self._notification_service else None
        elif event.event_name == "opening_facts_0932":
            report, report_status = self._build_opening_for_event(event, mapping)
            notify = self._notification_service.notify_open_confirmation_report if self._notification_service else None
        else:
            key = f"{event.trade_date}:{event.event_name}"
            return ReportingOutcome(event.event_name, "FAILED", "SKIPPED", None, key, mapping_sha, "unsupported_event")
        # A disabled/unconfigured delivery path must not consume the day's
        # claim.  Otherwise a later runtime reconfiguration could be blocked
        # by a claim created before any notification was possible.
        if notify is None or not bool(getattr(self._notification_service, "enabled", True)):
            return ReportingOutcome(
                event.event_name,
                report_status,
                "FAILED",
                report.html_sha256,
                f"{event.trade_date}:{event.event_name}",
                mapping_sha,
                "notification_unavailable",
            )
        claim = self._lifecycle.claim(event, report_digest=report.html_sha256)
        if not claim.allowed:
            return ReportingOutcome(event.event_name, report_status, claim.status, report.html_sha256, claim.dedupe_key, mapping_sha, claim.status)
        try:
            delivered = notify(report=report, request=request, preclaimed=True)
        except TypeError:
            delivered = notify(report=report, request=request)
        status = "ACCEPTED" if delivered else "FAILED"
        return ReportingOutcome(event.event_name, report_status, status, report.html_sha256, claim.dedupe_key, mapping_sha, status)

    def send_opening(self, *, trade_date: str, request: Any, observation_cutoff: datetime, send_eligibility: bool) -> tuple[str, str]:
        report = self.build_opening(trade_date=trade_date, observation_cutoff=observation_cutoff)
        if not send_eligibility or self._notification_service is None:
            return "built", report.html_sha256
        delivered = self._notification_service.notify_open_confirmation_report(report=report, request=request)
        return ("sent" if delivered else "not_sent"), report.html_sha256

    def send_opening_unavailable(self, *, trade_date: str, request: Any, observation_cutoff: datetime, send_eligibility: bool) -> tuple[str, str]:
        report = self.send_opening_unavailable_report(trade_date=trade_date, observation_cutoff=observation_cutoff)
        if not send_eligibility or self._notification_service is None:
            return "built", report.html_sha256
        delivered = self._notification_service.notify_open_confirmation_report(report=report, request=request)
        return ("sent" if delivered else "not_sent"), report.html_sha256
