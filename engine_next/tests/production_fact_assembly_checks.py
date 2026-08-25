from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from engine_next.runtime.production_fact_assembly import (
    build_production_auction_facts,
    freeze_mapping_snapshot,
    load_mapping_snapshot,
    normalize_td_auction_row,
    write_mapping_snapshot,
)
from engine_next.runtime.notification_service import RuntimeNotificationService
from engine_next.runtime.reporting_lifecycle import decide_report_execution
from engine_next.app_main import EngineApp
from engine_next.runtime.production_reporting import ProductionReportingCoordinator


class FakeRedis:
    def __init__(self):
        self.hashes = {
            "market:stock_plate": {"000001": "AI", "000002": "AI"},
            "market:auction:20260825:0925": {
                "summary": '{"total_stocks":2,"valid_stock_count":2,"high_open_count":2,"low_open_count":0,"flat_open_count":0,"total_auction_amount_yuan":3000000,"limit_up_count":0,"limit_down_count":0,"observation_time":"2026-08-25T09:25:00+08:00"}'
            },
        }

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)


def _td_rows(trade_date: str, tag: str):
    return [
        {"symbol": "000001", "px_milli": 10100, "chg_bp": 100, "match_amt_yuan": 1000000, "rest_bid_amt_yuan": 300000, "rest_ask_amt_yuan": 0, "limit_state": 0, "ts": f"{trade_date} {tag}"},
        {"symbol": "000002", "px_milli": 10200, "chg_bp": 200, "match_amt_yuan": 2000000, "rest_bid_amt_yuan": 500000, "rest_ask_amt_yuan": 0, "limit_state": 0, "ts": f"{trade_date} {tag}"},
    ]


def test_normalize_td_row_preserves_existing_units():
    row = normalize_td_auction_row({"symbol": "SZ.000001", "px_milli": 10123, "chg_bp": 125, "match_amt_yuan": 10, "rest_bid_amt_yuan": 2, "rest_ask_amt_yuan": 3, "limit_state": 1}, tag="0925")
    assert row["symbol"] == "000001"
    assert row["price"] == 10.123
    assert row["change_pct"] == 1.25
    assert row["auction_amount_yuan"] == 10
    assert row["ask_amount_present"] is True


def test_production_fact_assembly_uses_redis_summary_and_td_full_rows():
    facts = build_production_auction_facts(
        trade_date="2026-08-25",
        redis_client=FakeRedis(),
        td_query=_td_rows,
        data_origin="production_realtime",
    )
    assert facts.status == "normal"
    assert facts.market_summary["source"] == "a2_0925_summary"
    assert len(facts.snapshot_rows) == 6
    assert facts.plate_shadow["format"] == "PlateAuctionShadowV1"
    assert facts.plate_shadow["mapping_origin"]["canonical"] == "market:stock_plate"
    assert facts.plate_shadow["strategy_impact"] == "none"


def test_missing_snapshot_tag_is_partial_and_does_not_fallback():
    def incomplete(date, tag):
        return _td_rows(date, tag) if tag != "0920" else []

    facts = build_production_auction_facts(
        trade_date="2026-08-25", redis_client=FakeRedis(), td_query=incomplete
    )
    assert facts.status == "partial"
    assert "0920" in facts.provenance["missing_tags"]
    assert facts.plate_shadow["status"] == "partial"


def test_mapping_snapshot_is_atomic_and_hash_bound(tmp_path):
    path = write_mapping_snapshot(
        mapping={"000002": "AI", "000001": "AI"},
        trade_date="2026-08-25",
        effective_time="2026-08-25T09:10:00+08:00",
        source="market:stock_plate",
        directory=tmp_path,
    )
    payload = __import__("json").loads(path.read_text(encoding="utf-8"))
    assert payload["record_count"] == 2
    assert payload["sha256"]
    assert not path.with_suffix(".json.tmp").exists()


def test_mapping_snapshot_is_reused_after_restart_and_rejects_wrong_date(tmp_path):
    first = freeze_mapping_snapshot(
        redis_client=FakeRedis(), directory=tmp_path, trade_date="2026-08-25", effective_time="09:10:00"
    )
    fake = FakeRedis()
    fake.hashes["market:stock_plate"]["000001"] = "changed-after-freeze"
    second = freeze_mapping_snapshot(
        redis_client=fake, directory=tmp_path, trade_date="2026-08-25", effective_time="09:20:00"
    )
    assert second["sha256"] == first["sha256"]
    assert second["mapping"]["000001"] == "AI"
    assert load_mapping_snapshot(directory=tmp_path, trade_date="2026-08-24") is None


def test_open_confirmation_notification_uses_existing_dedup(monkeypatch, tmp_path):
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_ENABLED", "1")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_SMTP_HOST", "smtp.invalid")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_EMAIL_FROM", "from@example.com")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_EMAIL_TO", "to@example.com")
    monkeypatch.chdir(tmp_path)
    service = RuntimeNotificationService()
    delivered = []
    monkeypatch.setattr(service, "_send_email", lambda payload: delivered.append(payload) or True)
    report = SimpleNamespace(
        subject="开盘事实",
        text_body="截至 09:32:10",
        html_body="<p>截至 09:32:10</p>",
        html_sha256="open-hash",
        metadata={"trade_date": "2026-08-25", "data_origin": "production_realtime"},
    )
    request = SimpleNamespace(trade_date="2026-08-25", historical_replay=False)
    assert service.notify_open_confirmation_report(report=report, request=request) is True
    assert service.notify_open_confirmation_report(report=report, request=request) is False
    assert delivered[0].category == "opening_facts"


def test_notification_claim_is_at_most_once_even_after_provider_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_ENABLED", "1")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_SMTP_HOST", "smtp.invalid")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_EMAIL_FROM", "from@example.com")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_EMAIL_TO", "to@example.com")
    monkeypatch.chdir(tmp_path)
    service = RuntimeNotificationService()
    attempts = []
    monkeypatch.setattr(service, "_send_email", lambda payload: attempts.append(payload) or False)
    report = SimpleNamespace(
        subject="开盘事实", text_body="facts", html_body="<p>facts</p>", html_sha256="first",
        metadata={"trade_date": "2026-08-25", "data_origin": "production_realtime"},
    )
    request = SimpleNamespace(trade_date="2026-08-25", historical_replay=False)
    assert service.notify_open_confirmation_report(report=report, request=request) is False
    report.html_sha256 = "changed"
    assert service.notify_open_confirmation_report(report=report, request=request) is False
    assert len(attempts) == 1


def test_reporting_lifecycle_is_fail_closed_for_recovery_and_disabled():
    disabled = decide_report_execution(
        trade_date="2026-08-25", event_name="auction_facts_0926", historical_replay=False
    )
    assert disabled.send_eligibility is False
    assert disabled.reason == "reporting_disabled"
    recovery = decide_report_execution(
        trade_date="2026-08-25", event_name="auction_facts_0926", historical_replay=False,
        recovery=True, reporting_enabled=True,
    )
    assert recovery.execution_mode == "recovery"
    assert recovery.send_eligibility is False
    normal = decide_report_execution(
        trade_date="2026-08-25", event_name="auction_facts_0926", historical_replay=False,
        reporting_enabled=True,
    )
    assert normal.send_eligibility is True


def test_engine_scheduler_declares_opening_fact_slot_after_cutoff():
    app = EngineApp.__new__(EngineApp)
    app._startup_bootstrap = SimpleNamespace(last_audit_trade_date="2026-08-25", last_audit_token="checkpoint")
    decision = app._build_loop_decision(datetime(2026, 8, 25, 9, 32, 10), "2026-08-25")
    assert decision.scheduled_event_name == "opening_facts_0932"
    before = app._build_loop_decision(datetime(2026, 8, 25, 9, 32, 9), "2026-08-25")
    assert before.scheduled_event_name == ""
    recovery = app._build_loop_decision(datetime(2026, 8, 25, 9, 33, 0), "2026-08-25")
    assert recovery.scheduled_event_name == "opening_facts_0932"


def test_unavailable_reports_are_explicit_and_strategy_free():
    coordinator = ProductionReportingCoordinator(auction_fact_loader=lambda _: None)
    auction = coordinator.build_unavailable_auction(trade_date="2026-08-25")
    assert auction.metadata["market_overview"]["status"] == "unavailable"
    assert auction.metadata["strategy_impact"] == "none"
