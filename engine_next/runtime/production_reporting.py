"""Minimal production reporting orchestration.

The coordinator is intentionally callback based: production-specific Redis/TD
readers are supplied by the runtime, while report construction and delivery
remain pure/reusable.  It is not a scheduler or a new provider framework.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping

from engine_next.runtime.auction_email_report import AuctionEmailReport, build_auction_email_report
from engine_next.runtime.notification_service import RuntimeNotificationService


@dataclass(frozen=True)
class OpeningFactsReport:
    subject: str
    text_body: str
    html_body: str
    html_sha256: str
    metadata: dict[str, Any]


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
    ) -> None:
        self._auction_fact_loader = auction_fact_loader
        self._opening_fact_loader = opening_fact_loader
        self._notification_service = notification_service

    def build_auction(self, *, trade_date: str) -> AuctionEmailReport:
        bundle = self._auction_fact_loader(trade_date)
        return build_auction_email_report(
            plate_shadow=bundle.plate_shadow,
            auction_evidence={"market_summary": bundle.market_summary, "data_origin": bundle.data_origin},
            market_context={"data_origin": bundle.data_origin, "trade_date": trade_date},
        )

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

    def send_opening(self, *, trade_date: str, request: Any, observation_cutoff: datetime, send_eligibility: bool) -> tuple[str, str]:
        report = self.build_opening(trade_date=trade_date, observation_cutoff=observation_cutoff)
        if not send_eligibility or self._notification_service is None:
            return "built", report.html_sha256
        delivered = self._notification_service.notify_open_confirmation_report(report=report, request=request)
        return ("sent" if delivered else "not_sent"), report.html_sha256

    def send_opening_unavailable(self, *, trade_date: str, request: Any, observation_cutoff: datetime, send_eligibility: bool) -> tuple[str, str]:
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
        report = build_opening_facts_report(observation)
        if not send_eligibility or self._notification_service is None:
            return "built", report.html_sha256
        delivered = self._notification_service.notify_open_confirmation_report(report=report, request=request)
        return ("sent" if delivered else "not_sent"), report.html_sha256
