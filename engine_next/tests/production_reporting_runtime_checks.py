from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.runtime.production_fact_assembly import load_mapping_snapshot, write_mapping_snapshot
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

    def set(self, key, value, ex=None):
        self.claims[key] = value
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


def _ready_mapping(tmp_path: Path, trade_date: str = "2026-08-25") -> dict:
    mapping = {f"{index:06d}": "AI" for index in range(1, 1001)}
    write_mapping_snapshot(
        mapping=mapping,
        trade_date=trade_date,
        effective_time="09:10:00",
        source="market:stock_plate",
        directory=tmp_path,
    )
    return mapping


def _bundle():
    return SimpleNamespace(
        plate_shadow=_shadow(),
        market_summary={"source": "a2_0925_summary", "status": "available", "trade_date": "2026-08-25", "positive_count": 1},
        data_origin="production_realtime",
        mapping_origin={"canonical": "market:stock_plate"},
        status="normal",
    )


class MappingRedis:
    def __init__(self, count: int):
        self.mapping = {f"{index:06d}": "AI" for index in range(1, count + 1)}

    def hgetall(self, key):
        return self.mapping if key == "market:stock_plate" else {}


class RaisingNotifier(FakeNotifier):
    def notify_auction_report(self, **kwargs):
        self.sent.append(("auction", kwargs.get("preclaimed")))
        raise RuntimeError("notification failure")


class ExplodingAvailabilityNotifier(FakeNotifier):
    def __init__(self, redis):
        self._redis = redis
        self.sent = []

    @property
    def enabled(self):
        raise AssertionError("build-only path must not inspect notifier availability")


def _opening_observation() -> dict:
    return {
        "format": "OpenConfirmationObservationV1",
        "trade_date": "2026-08-25",
        "data_origin": "production_realtime",
        "mapping_consistency": "consistent",
        "market": {
            "auction": {"positive_count": 1, "negative_count": 0, "flat_count": 0},
            "open": {
                "status": "available",
                "observation_time": "2026-08-25T09:32:10",
                "open_up_count": 1,
                "open_down_count": 0,
                "open_flat_count": 0,
                "open_valid_count": 1,
                "open_window_amount_yuan": 100.0,
            },
        },
        "plates": [],
        "observations": [],
        "open_source": {"status": "available", "observation_cutoff": "2026-08-25T09:32:10"},
    }


def _auction_event(actual_time: datetime | None = None) -> ReportingEvent:
    actual = actual_time or datetime(2026, 8, 25, 9, 26, 1)
    return ReportingEvent("2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26), actual)


def test_mapping_below_readiness_does_not_freeze(tmp_path: Path):
    redis = MappingRedis(999)
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: None,
        redis_client=redis,
        mapping_directory=tmp_path,
    )
    assert coordinator.prepare_mapping(trade_date="2026-08-25", now=datetime(2026, 8, 25, 9, 10)) is None
    assert not (tmp_path / "2026-08-25" / "stock_plate_snapshot.json").exists()


def test_mapping_at_readiness_freezes_and_restart_reuses_sha(tmp_path: Path):
    redis = MappingRedis(1000)
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: None,
        redis_client=redis,
        mapping_directory=tmp_path,
    )
    first = coordinator.prepare_mapping(trade_date="2026-08-25", now=datetime(2026, 8, 25, 9, 10))
    assert first is not None and first["record_count"] == 1000
    restarted = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: None,
        redis_client=MappingRedis(2000),
        mapping_directory=tmp_path,
    )
    second = restarted.prepare_mapping(trade_date="2026-08-25", now=datetime(2026, 8, 25, 9, 11))
    assert second["sha256"] == first["sha256"]
    assert load_mapping_snapshot(directory=tmp_path, trade_date="2026-08-25", minimum_record_count=1000)["sha256"] == first["sha256"]


@pytest.mark.parametrize("mutation", ["trade_date", "sha256", "record_count"])
def test_existing_invalid_mapping_snapshot_fails_closed_without_refreeze(tmp_path: Path, mutation: str):
    _ready_mapping(tmp_path)
    path = tmp_path / "2026-08-25" / "stock_plate_snapshot.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if mutation == "trade_date":
        payload["trade_date"] = "2026-08-24"
    elif mutation == "sha256":
        payload["sha256"] = "invalid"
    else:
        payload["record_count"] = 999
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: None,
        redis_client=MappingRedis(2000),
        mapping_directory=tmp_path,
    )
    assert coordinator.prepare_mapping(trade_date="2026-08-25", now=datetime(2026, 8, 25, 9, 10)) is None
    assert json.loads(path.read_text(encoding="utf-8"))["trade_date"] == payload["trade_date"]


def test_existing_mapping_below_readiness_fails_closed_without_refreeze(tmp_path: Path):
    write_mapping_snapshot(
        mapping={f"{index:06d}": "AI" for index in range(1, 1000)},
        trade_date="2026-08-25",
        effective_time="09:10:00",
        source="market:stock_plate",
        directory=tmp_path,
    )
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: None,
        redis_client=MappingRedis(2000),
        mapping_directory=tmp_path,
    )
    assert coordinator.prepare_mapping(trade_date="2026-08-25", now=datetime(2026, 8, 25, 9, 10)) is None


def test_auction_loader_exception_is_failed_before_claim_or_notification():
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    calls = []

    def boom(trade_date):
        calls.append(trade_date)
        raise RuntimeError("auction loader failure")

    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=boom,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
    )
    outcome = coordinator.handle(_auction_event())
    assert outcome.report_status == "FAILED"
    assert outcome.delivery_status == "NOT_ATTEMPTED"
    assert outcome.fact_status == "failed"
    assert calls == ["2026-08-25"]
    assert redis.claims == {}
    assert notifier.sent == []


def test_opening_loader_exception_is_failed_before_claim_or_notification(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    _ready_mapping(tmp_path)

    def boom(*args):
        raise RuntimeError("opening loader failure")

    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: _bundle(),
        opening_fact_loader=boom,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    event = ReportingEvent("2026-08-25", "opening_facts_0932", datetime(2026, 8, 25, 9, 32, 10), datetime(2026, 8, 25, 9, 32, 11))
    outcome = coordinator.handle(event)
    assert outcome.report_status == "FAILED"
    assert outcome.delivery_status == "NOT_ATTEMPTED"
    assert redis.claims == {}
    assert notifier.sent == []


def test_loader_internal_typeerror_is_not_retried():
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    calls = []

    def boom(trade_date, mapping=None):
        calls.append((trade_date, mapping))
        raise TypeError("argument conversion failed")

    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=boom,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
    )
    outcome = coordinator.handle(_auction_event())
    assert outcome.report_status == "FAILED"
    assert calls == [("2026-08-25", None)]
    assert redis.claims == {}


def test_notification_exception_keeps_claim_and_is_not_retried():
    redis = FakeRedis()
    notifier = RaisingNotifier(redis)
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: _bundle(),
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
    )
    outcome = coordinator.handle(_auction_event())
    assert outcome.report_status == "COMPLETE"
    assert outcome.delivery_status == "FAILED"
    assert outcome.notification_status == "FAILED"
    assert notifier.sent == [("auction", True)]
    assert redis.claims["2026-08-25:auction_facts_0926"] == "FAILED"
def test_reporting_lifecycle_claim_is_fail_closed_without_redis():
    event = ReportingEvent("2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26), datetime(2026, 8, 25, 9, 26, 1))
    claim = ReportingLifecycle(redis_client=None).claim(event, report_digest="x")
    assert claim.allowed is False
    assert claim.status == "FAILED"


def test_reporting_lifecycle_relabels_late_default_event_as_recovery():
    redis = FakeRedis()
    event = ReportingEvent(
        "2026-08-25",
        "auction_facts_0926",
        datetime(2026, 8, 25, 9, 26),
        datetime(2026, 8, 25, 9, 30),
    )
    claim = ReportingLifecycle(redis_client=redis).claim(event, report_digest="late")
    assert claim.allowed is False
    assert claim.status == "SKIP_RECOVERY"
    assert redis.claims == {}


def test_coordinator_owns_claim_and_passes_preclaimed_notification(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    _ready_mapping(tmp_path)
    bundle = _bundle()
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
    assert redis.claims["2026-08-25:auction_facts_0926"] == "ACCEPTED"


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


def test_missing_same_day_mapping_after_cutoff_calls_loader_once_without_refreeze(tmp_path: Path):
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
    assert calls == [("2026-08-25",)]
    assert notifier.sent == [("auction", True)]


def test_a2_available_without_mapping_sends_truthful_partial_report(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    bundle = _bundle()
    bundle.component_statuses = {"market_overview": "available", "plate_facts": "unavailable", "mapping": "unavailable"}
    bundle.market_summary_status = "available"
    bundle.plate_facts_status = "unavailable"
    bundle.mapping_status = "unavailable"
    bundle.unavailable_reasons = ("frozen mapping unavailable",)
    bundle.report_status = "PARTIAL"
    calls = []

    def auction_loader(*args):
        calls.append(args)
        assert args == ("2026-08-25",)
        return bundle

    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=auction_loader,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    outcome = coordinator.handle(_auction_event())
    assert outcome.report_status == "PARTIAL"
    assert outcome.delivery_status == "ACCEPTED"
    assert calls == [("2026-08-25",)]
    assert notifier.sent == [("auction", True)]
    assert redis.claims["2026-08-25:auction_facts_0926"] == "ACCEPTED"


def test_disabled_notification_does_not_consume_dedup_claim(tmp_path: Path):
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    notifier.enabled = False
    _ready_mapping(tmp_path)
    bundle = _bundle()
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda trade_date, mapping=None: bundle,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    event = ReportingEvent("2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26), datetime(2026, 8, 25, 9, 26, 1))
    outcome = coordinator.handle(event)
    assert outcome.delivery_status == "FAILED"
    assert outcome.report_status == "COMPLETE"
    assert redis.claims == {}


def test_manual_audit_builds_without_inspecting_notifier_or_claiming() -> None:
    redis = FakeRedis()
    notifier = ExplodingAvailabilityNotifier(redis)
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: _bundle(),
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
    )
    event = ReportingEvent(
        "2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26),
        datetime(2026, 8, 25, 9, 26, 1), "manual_audit",
    )
    outcome = coordinator.handle(event)
    assert outcome.report_status == "COMPLETE"
    assert outcome.delivery_status == "SKIP_MANUAL_AUDIT"
    assert outcome.execution_mode == "manual_audit"
    assert redis.claims == {}
    assert notifier.sent == []


def test_recovery_builds_without_inspecting_notifier_or_claiming() -> None:
    redis = FakeRedis()
    notifier = ExplodingAvailabilityNotifier(redis)
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda _: _bundle(),
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
    )
    event = ReportingEvent(
        "2026-08-25", "auction_facts_0926", datetime(2026, 8, 25, 9, 26),
        datetime(2026, 8, 25, 9, 30),
    )
    outcome = coordinator.handle(event)
    assert outcome.report_status == "COMPLETE"
    assert outcome.delivery_status == "SKIP_RECOVERY"
    assert outcome.execution_mode == "recovery"
    assert redis.claims == {}
    assert notifier.sent == []


@pytest.mark.parametrize("event_name", ["auction_facts_0926", "opening_facts_0932"])
def test_build_only_modes_are_notifier_independent_for_opening_and_auction(tmp_path: Path, event_name: str) -> None:
    redis = FakeRedis()
    notifier = ExplodingAvailabilityNotifier(redis)
    _ready_mapping(tmp_path)
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda *args: _bundle(),
        opening_fact_loader=lambda *args: _opening_observation(),
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    event = ReportingEvent(
        "2026-08-25", event_name,
        datetime(2026, 8, 25, 9, 26) if event_name == "auction_facts_0926" else datetime(2026, 8, 25, 9, 32, 10),
        datetime(2026, 8, 25, 9, 26, 1) if event_name == "auction_facts_0926" else datetime(2026, 8, 25, 9, 32, 11),
        "manual_audit",
    )
    outcome = coordinator.handle(event)
    assert outcome.delivery_status == "SKIP_MANUAL_AUDIT"
    assert outcome.report_status == "COMPLETE"
    assert redis.claims == {}
    assert notifier.sent == []


def test_normal_notifier_unavailable_preserves_partial_report_status(tmp_path: Path) -> None:
    redis = FakeRedis()
    notifier = FakeNotifier(redis)
    notifier.enabled = False
    _ready_mapping(tmp_path)
    bundle = _bundle()
    bundle.component_statuses = {"market_overview": "available", "plate_facts": "unavailable", "mapping": "unavailable"}
    bundle.market_summary_status = "available"
    bundle.plate_facts_status = "unavailable"
    bundle.mapping_status = "unavailable"
    bundle.unavailable_reasons = ("frozen mapping unavailable",)
    bundle.report_status = "PARTIAL"
    coordinator = ProductionReportingCoordinator(
        auction_fact_loader=lambda *args: bundle,
        notification_service=notifier,
        lifecycle=ReportingLifecycle(redis_client=redis),
        mapping_directory=tmp_path,
    )
    outcome = coordinator.handle(_auction_event())
    assert outcome.report_status == "PARTIAL"
    assert outcome.delivery_status == "FAILED"
    assert redis.claims == {}
