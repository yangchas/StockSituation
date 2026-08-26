from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.runtime.production_fact_assembly import write_mapping_snapshot
from engine_next.runtime.production_reporting import ProductionReportingCoordinator
from engine_next.runtime.reporting_lifecycle import ReportingEvent, ReportingLifecycle
from engine_next.domain.enums import RunPhase


class FakeRedis:
    def __init__(self):
        self.hashes = {"q2:active:20260825": {"000001"}}
        self.q2 = {
            "q2:000001": {
                "mk": "sz", "px": "10100", "pc": "10000", "amt": "20",
                "amt2m": "12", "ls": "0", "ts": str(1787621520000),
            }
        }
        self.claims = {}

    def smembers(self, key):
        return self.hashes.get(key, set())

    def hgetall(self, key):
        if key in self.q2:
            return self.q2[key]
        return {}

    def setnx(self, key, value):
        if key in self.claims:
            return False
        self.claims[key] = value
        return True

    def expire(self, key, ttl):
        return True


class FakeNotifier:
    def __init__(self, redis):
        self._redis = redis
        self.enabled = True
        self.sent = []

    def notify_auction_report(self, **kwargs):
        self.sent.append(("auction", kwargs.get("preclaimed")))
        return True

    def notify_open_confirmation_report(self, **kwargs):
        self.sent.append(("open", kwargs.get("preclaimed")))
        return True


def _shadow():
    return {
        "format": "PlateAuctionShadowV1",
        "trade_date": "2026-08-25",
        "data_origin": "production_realtime",
        "historical_valid": False,
        "mapping_origin": {"canonical": "market:stock_plate", "status": "runtime_owned_snapshot"},
        "plate_stats": {"0924_to_0925": {"AI": {
            "stock_count": 1,
            "valid_auction_stock_count": 1,
            "auction_amount_total_yuan": 100.0,
            "change_pct_distribution": {"positive_count": 1, "negative_count": 0, "zero_count": 0, "median_pct": 1.0},
            "evidence_usable_stock_count": 1,
            "evidence_unavailable_stock_count": 0,
            "top3_amount_concentration": 1.0,
            "pressure_yuan": 0.0,
            "withdrawal_yuan": 0.0,
        }}},
        "symbol_details": {"0924_to_0925": {"detail_rows": [
            {"symbol": "000001", "plate": "AI", "status": "resolved", "price_status": "valid", "auction_amount_yuan": 100.0, "change_pct": 1.0}
        ]}},
        "automatic_analysis": {"auction_locked_orders": {}},
    }


def test_reporting_lifecycle_claim_is_fail_closed_without_redis():
    event = ReportingEvent("2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26), datetime(2026, 8, 25, 9, 26, 1))
    claim = ReportingLifecycle(redis_client=None).claim(event, report_digest="x")
    assert claim.allowed is False
    assert claim.status == "FAILED"


def test_coordinator_owns_claim_and_passes_preclaimed_notification(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    write_mapping_snapshot(mapping={"000001": "AI"}, trade_date="2026-08-25", effective_time="09:10:00", source="market:stock_plate", directory=tmp_path)
    bundle = SimpleNamespace(
        plate_shadow=_shadow(),
        market_summary={"source": "a2_0925_summary", "status": "available", "trade_date": "2026-08-25", "positive_count": 1},
        data_origin="production_realtime",
        mapping_origin={"canonical": "market:stock_plate"},
        status="normal",
    )
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda trade_date, mapping=None: bundle,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    event = ReportingEvent("2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26), datetime(2026, 8, 25, 9, 26, 1))
    first = coordinator.handle(event)
    second = coordinator.handle(event)
    assert first.delivery_status == "ACCEPTED"
    assert second.delivery_status == "SKIP_ALREADY_CLAIMED"
    assert notifier.sent == [("auction", True)]


def test_online_q2_path_does_not_fallback_to_legacy_quotes():
    hub = IntradayDataHub(redis_client=FakeRedis())
    result = hub.fetch_online_q2_rows("2026-08-25", datetime(2026, 8, 25, 9, 32, 10))
    assert result.source == "production_online_q2"
    assert [row["symbol"] for row in result.rows] == ["000001"]


def test_coordinator_rejects_unsupported_event_before_notification():
    notifier = FakeNotifier(FakeRedis())
    coordinator = ProductionReportingCoordinator(auction_fact_loader=lambda _: None, notification_service=notifier)
    event = ReportingEvent("2026-08-25", "postmarket_recap", datetime(2026, 8, 25, 17, 40), datetime(2026, 8, 25, 17, 40))
    outcome = coordinator.handle(event)
    assert outcome.delivery_status == "SKIPPED"
    assert notifier.sent == []


def test_generic_notifier_resolver_has_no_auction_or_open_ownership():
    from engine_next.runtime.notification_service import RuntimeNotificationService

    service = RuntimeNotificationService.__new__(RuntimeNotificationService)
    assert service._resolve_category(result=SimpleNamespace(phase=RunPhase.AUCTION), summary_text="当前阶段：竞价") == ""
    assert service._resolve_category(result=SimpleNamespace(phase=RunPhase.INTRADAY), summary_text="当前阶段：开盘确认") == ""


def test_missing_same_day_mapping_after_cutoff_is_unavailable_without_loader_call(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    calls = []

    def auction_loader(*args):
        calls.append(args)
        return None

    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=auction_loader,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    event = ReportingEvent(
        "2026-08-25",
        "auction_facts_0926",
        datetime(2026, 8, 25, 9, 26),
        datetime(2026, 8, 25, 9, 26, 1),
    )
    outcome = coordinator.handle(event)
    assert outcome.report_status == "DATA_UNAVAILABLE"
    assert calls == []
    assert notifier.sent == [("auction", True)]


def test_disabled_notification_does_not_consume_dedup_claim(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    notifier.enabled = False
    write_mapping_snapshot(mapping={"000001": "AI"}, trade_date="2026-08-25", effective_time="09:10:00", source="market:stock_plate", directory=tmp_path)
    bundle = SimpleNamespace(
        plate_shadow=_shadow(),
        market_summary={"source": "a2_0925_summary", "status": "available", "trade_date": "2026-08-25"},
        data_origin="production_realtime",
        mapping_origin={"canonical": "market:stock_plate"},
        status="normal",
    )
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda trade_date, mapping=None: bundle,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    event = ReportingEvent("2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26), datetime(2026, 8, 25, 9, 26, 1))
    outcome = coordinator.handle(event)
    assert outcome.delivery_status == "FAILED"
    assert redis.claims == {}
