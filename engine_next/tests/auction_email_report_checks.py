from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import engine_next.runtime.auction_email_report as auction_email_report
from engine_next.runtime.auction_email_report import build_auction_email_report
from engine_next.runtime.notification_service import RuntimeNotificationPayload, RuntimeNotificationService


def _plate_shadow(*, data_origin: str = "replay_fixture_only", capture_time: str | None = None) -> dict:
    return {
        "format": "PlateAuctionShadowV1",
        "trade_date": "2026-08-21",
        "data_origin": data_origin,
        "capture_time": capture_time,
        "historical_valid": False,
        "historical_valid_reasons": ["mapping_is_current_cache_only"],
        "mapping_origin": {"canonical": "market:stock_plate", "status": "current_cache_only"},
        "mapping_coverage": {"multi_label_conflict_symbols": 1},
        "source_provenance": {"auction_evidence": {"contract_version": "AuctionShadowEvidenceV1"}},
        "plate_stats": {
            "0924_to_0925": {
                "机器人": {
                    "stock_count": 2,
                    "valid_auction_stock_count": 2,
                    "unavailable_auction_stock_count": 0,
                    "auction_amount_total_yuan": 12_000_000,
                    "change_pct_distribution": {
                        "positive_count": 1,
                        "negative_count": 1,
                        "zero_count": 0,
                        "median_pct": 0.5,
                    },
                    "pressure_yuan": 3_000_000,
                    "withdrawal_yuan": 500_000,
                    "evidence_usable_stock_count": 2,
                    "evidence_unavailable_stock_count": 0,
                    "top3_amount_concentration": 1.0,
                    "top3_pressure_concentration": 1.0,
                    "limit_up_count": 1,
                    "limit_down_count": 0,
                    "limit_up_seal_amount_yuan": 8_000_000,
                    "limit_down_sell_pressure_yuan": 0,
                    "multi_theme_conflict_count": 1,
                    "auction_locked_order_impact": {
                        "limit_up_locked_amount_yuan": 8_000_000,
                        "limit_down_locked_amount_yuan": 0,
                        "q2_to_anchor_locked_amount_ratio": 0.75,
                    },
                }
            }
        },
        "automatic_analysis": {
            "plate_observations": [
                {
                    "plate": "机器人",
                    "reason_codes": ["positive_breadth_leads", "auction_limit_locked_order_present"],
                    "generated_summary": "机器人：竞价金额0.12亿元；这里只是客观观察。",
                    "key_metrics": {},
                }
            ],
            "auction_locked_orders": {
                "limit_up": [
                    {
                        "symbol": "000001",
                        "plate": "机器人",
                        "change_pct": 10.0,
                        "anchor_locked_amount_yuan": 8_000_000,
                        "q2_observed_locked_amount_yuan": 6_000_000,
                        "q2_to_anchor_locked_amount_ratio": 0.75,
                        "status": "available",
                        "full_day_one_word_status": "unavailable",
                    }
                ],
                "limit_down": [],
            },
        },
        "symbol_details": {
            "0924_to_0925": {
                "detail_rows": [
                    {"symbol": "000001", "plate": "机器人", "status": "resolved", "price_status": "valid", "auction_amount_yuan": 8_000_000, "change_pct": 10.0},
                    {"symbol": "000002", "plate": "机器人", "status": "resolved", "price_status": "valid", "auction_amount_yuan": 4_000_000, "change_pct": -1.0},
                ]
            }
        },
    }


def _market_context() -> dict:
    return {
        "trade_date": "2026-08-21",
        "data_origin": "replay_fixture_only",
        "capture_time": "2026-08-21T09:25:30+08:00",
        "contract_version": "MarketStateSnapshotV1",
        "limit_pool": [{"symbol": "000001", "source_trade_date": "2026-08-20"}],
        "hot_plates_today": [{"plate": "机器人"}],
    }


def test_fixture_report_is_deterministic_and_read_only() -> None:
    ledger = b'{"candidate":[],"decision":"unchanged"}\n'
    ledger_hash = hashlib.sha256(ledger).hexdigest()
    first = build_auction_email_report(plate_shadow=_plate_shadow(), market_context=_market_context())
    second = build_auction_email_report(plate_shadow=_plate_shadow(), market_context=_market_context())
    assert first.metadata["data_origin"] == "replay_fixture_only"
    assert first.metadata["strategy_impact"] == "none"
    assert first.metadata["decision_bundle"] is None
    assert first.metadata["provenance"]["market_context_origin"] == "replay_fixture_only"
    assert first.metadata["plate_rows"][0]["quality"] == "complete"
    assert first.html_body == second.html_body
    assert first.html_sha256 == second.html_sha256
    assert hashlib.sha256(ledger).hexdigest() == ledger_hash


def test_empty_optional_inputs_render_unavailable() -> None:
    shadow = _plate_shadow(data_origin="current_cache_only", capture_time="2026-08-21T09:25:30+08:00")
    shadow["plate_stats"] = {"0924_to_0925": {}}
    shadow["automatic_analysis"] = {"plate_observations": [], "auction_locked_orders": {}}
    report = build_auction_email_report(plate_shadow=shadow)
    assert report.metadata["market_context"]["status"] == "unavailable"
    assert report.metadata["open_confirmation"]["status"] == "unavailable"
    assert report.metadata["market_overview"].get("valid_stock_count") is None
    assert "unavailable" in report.html_body


def test_live_delivery_is_0926_only_deduped_and_rejects_replay(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_ENABLED", "1")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_SMTP_HOST", "smtp.invalid")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_EMAIL_FROM", "from@example.com")
    monkeypatch.setenv("ENGINE_NEXT_NOTIFY_EMAIL_TO", "to@example.com")
    monkeypatch.chdir(tmp_path)
    service = RuntimeNotificationService()
    delivered: list[RuntimeNotificationPayload] = []
    monkeypatch.setattr(service, "_send_email", lambda payload: delivered.append(payload) or True)
    ledger = {"candidate": [], "decision": "unchanged"}
    ledger_before = json.dumps(ledger, ensure_ascii=False, sort_keys=True)
    request = SimpleNamespace(
        trade_date="2026-08-21",
        historical_replay=False,
        now=datetime(2026, 8, 21, 9, 26, 0),
    )
    report = build_auction_email_report(
        plate_shadow=_plate_shadow(data_origin="production_capture", capture_time="2026-08-21T09:25:30+08:00")
    )
    assert service.notify_auction_report(report=report, request=request) is True
    assert json.dumps(ledger, ensure_ascii=False, sort_keys=True) == ledger_before
    assert delivered[0].category == "auction_evidence"
    assert delivered[0].html_body and "Provenance" in delivered[0].html_body
    assert service.notify_auction_report(report=report, request=request) is False

    replay_report = build_auction_email_report(plate_shadow=_plate_shadow())
    assert service.notify_auction_report(report=replay_report, request=request) is False


def test_existing_smtp_sender_adds_html_alternative(monkeypatch) -> None:
    sent_messages = []

    class FakeSmtp:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def login(self, *args) -> None:
            pass

        def send_message(self, message) -> None:
            sent_messages.append(message)

    monkeypatch.setattr("engine_next.runtime.notification_service.smtplib.SMTP_SSL", FakeSmtp)
    service = RuntimeNotificationService.__new__(RuntimeNotificationService)
    service._smtp_host = "smtp.invalid"
    service._smtp_port = 465
    service._smtp_starttls = False
    service._smtp_user = ""
    service._smtp_password = ""
    service._smtp_from = "from@example.com"
    service._smtp_to = ("to@example.com",)
    payload = RuntimeNotificationPayload(
        category="auction_evidence",
        phase_label="竞价事实",
        subject="test",
        body="plain",
        digest="digest",
        signal_digest="digest",
        html_body="<html><body>evidence</body></html>",
    )
    assert service._send_email(payload) is True
    assert sent_messages[0].get_content_type() == "multipart/alternative"
    assert any(part.get_content_type() == "text/html" for part in sent_messages[0].walk())


def test_report_settings_are_split_and_display_limits_do_not_change_facts(monkeypatch) -> None:
    first = build_auction_email_report(plate_shadow=_plate_shadow())
    settings = first.metadata["report_settings"]
    assert settings["analysis_guardrails"] == {"cross_section_rank_min_valid_count": 10}
    assert "cross_section_rank_min_valid_count" not in settings["display_limits"]
    before = first.metadata["plate_rows"][0]
    monkeypatch.setattr(auction_email_report, "MAIN_PLATE_LIMIT", 0)
    second = build_auction_email_report(plate_shadow=_plate_shadow())
    assert second.metadata["plate_rows"][0]["auction_amount_yuan"] == before["auction_amount_yuan"]
    assert second.metadata["fact_view_sha256"] == first.metadata["fact_view_sha256"]
    assert "机器人" not in second.text_body.split("## 重点板块事实Top10", 1)[1].split("## 涨跌停封单", 1)[0]


def test_a2_summary_is_authoritative_for_market_overview() -> None:
    context = {
        "format": "EngineNextStaticFactSnapshotV1",
        "data_origin": "replay_fixture_only",
        "facts": [
            {
                "key": "market:auction:20260821:0925",
                "value": {
                    "summary": json.dumps({
                        "tag": "0925", "ts": 1787275500000, "total_stocks": 2,
                        "high_open_count": 1, "low_open_count": 1, "flat_open_count": 0,
                        "total_auction_amount_yuan": 12_000_000, "limit_up_count": 1,
                        "limit_down_count": 0, "total_limit_up_bid_amount_yuan": 8_000_000,
                    })
                },
            }
        ],
    }
    report = build_auction_email_report(plate_shadow=_plate_shadow(), market_context=context)
    overview = report.metadata["market_overview"]
    assert overview["source"] == "a2_0925_summary"
    assert overview["auction_amount_yuan"] == 12_000_000
    assert report.metadata["provenance"]["market_summary_source"] == "a2_0925_summary"


def test_a2_summary_source_table_is_preserved_in_provenance() -> None:
    context = {
        "format": "AuctionMarketSummaryV1",
        "trade_date": "2026-08-21",
        "source_table": "market_data1.auction_summary_v2",
        "observation_time": "2026-08-21T09:25:00+08:00",
        "anchors": {
            "0925": {
                "total_stocks": 2,
                "high_open_count": 1,
                "low_open_count": 1,
                "flat_open_count": 0,
                "total_auction_amount_yuan": 12_000_000,
                "limit_up_count": 1,
                "limit_down_count": 0,
                "total_limit_up_bid_amount_yuan": 8_000_000,
            }
        },
    }
    report = build_auction_email_report(plate_shadow=_plate_shadow(), market_context=context)
    assert report.metadata["market_overview"]["source_table"] == "market_data1.auction_summary_v2"
    assert report.metadata["provenance"]["market_summary_source_table"] == "market_data1.auction_summary_v2"
    assert "market_data1.auction_summary_v2" in report.text_body


def test_core_observations_keep_price_pressure_out_when_full() -> None:
    shadow = _plate_shadow()
    shadow["plate_stats"]["0924_to_0925"]["机器人"]["pressure_yuan"] = -3_000_000
    shadow["plate_stats"]["0924_to_0925"]["机器人"]["change_pct_distribution"] = {
        "positive_count": 2, "negative_count": 0, "zero_count": 0, "count": 2, "median_pct": 1.0,
    }
    report = build_auction_email_report(plate_shadow=shadow)
    assert len(report.metadata["observations"]) >= 1
    assert all(item["observation_type"] != "price_pressure_relation" for item in report.metadata["observations"][:6])


def test_strategy_phrase_scope_does_not_scan_provenance_words() -> None:
    report = build_auction_email_report(
        plate_shadow=_plate_shadow(),
        market_context={"data_origin": "replay_fixture_only", "contract_version": "anchor confirmation status"},
    )
    body = "\n".join(item["text"] for item in report.metadata["observations"])
    assert "主线确认" not in body
    assert "anchor confirmation status" in report.metadata["market_context"]["contract_version"]


def test_no_information_appendix_rankings_are_hidden_but_main_zero_fact_remains() -> None:
    shadow = _plate_shadow()
    stats = shadow["plate_stats"]["0924_to_0925"]["机器人"]
    stats["pressure_yuan"] = 0
    stats["withdrawal_yuan"] = 0
    stats["change_pct_distribution"]["positive_count"] = 1
    stats["change_pct_distribution"]["negative_count"] = 1
    report = build_auction_email_report(plate_shadow=shadow)
    assert report.metadata["appendix"]["withdrawal TopN"] == []
    assert "|0.00亿|" in report.text_body


def test_equal_metric_appendix_is_hidden() -> None:
    shadow = _plate_shadow()
    extra = deepcopy(shadow["plate_stats"]["0924_to_0925"]["机器人"])
    extra["auction_amount_total_yuan"] = 6_000_000
    extra["valid_auction_stock_count"] = 2
    extra["change_pct_distribution"] = {"positive_count": 1, "negative_count": 1, "zero_count": 0, "count": 2, "median_pct": 0.1}
    shadow["plate_stats"]["0924_to_0925"]["AI"] = extra
    shadow["symbol_details"]["0924_to_0925"]["detail_rows"].extend([
        {"symbol": "000003", "plate": "AI", "price_status": "valid", "auction_amount_yuan": 4_000_000, "change_pct": 0.2},
        {"symbol": "000004", "plate": "AI", "price_status": "valid", "auction_amount_yuan": 2_000_000, "change_pct": 0.0},
    ])
    report = build_auction_email_report(plate_shadow=shadow)
    assert report.metadata["appendix"]["Top1集中度TopN"] == []


def test_concentration_core_requires_sample_but_appendix_keeps_small_sample() -> None:
    shadow = _plate_shadow()
    small = shadow["plate_stats"]["0924_to_0925"]["机器人"]
    small.update({"stock_count": 1, "valid_auction_stock_count": 1, "auction_amount_total_yuan": 8_000_000})
    small["change_pct_distribution"] = {"positive_count": 1, "negative_count": 0, "zero_count": 0, "count": 1, "median_pct": 1.0}
    shadow["symbol_details"]["0924_to_0925"]["detail_rows"] = [shadow["symbol_details"]["0924_to_0925"]["detail_rows"][0]]
    large = deepcopy(small)
    large.update({"stock_count": 20, "valid_auction_stock_count": 20, "auction_amount_total_yuan": 10_000_000})
    large["change_pct_distribution"] = {"positive_count": 12, "negative_count": 8, "zero_count": 0, "count": 20, "median_pct": 0.2}
    shadow["plate_stats"]["0924_to_0925"]["大板块"] = large
    shadow["symbol_details"]["0924_to_0925"]["detail_rows"].extend([
        {"symbol": "000003", "plate": "大板块", "price_status": "valid", "auction_amount_yuan": 7_000_000, "change_pct": 0.3},
        {"symbol": "000004", "plate": "大板块", "price_status": "valid", "auction_amount_yuan": 3_000_000, "change_pct": 0.1},
    ])
    report = build_auction_email_report(plate_shadow=shadow)
    concentration = [item for item in report.metadata["core_observations"] if item["observation_type"] == "concentration"]
    assert concentration and concentration[0]["evidence_values"]["plate"] == "大板块"
    appendix = report.metadata["appendix"]["Top1集中度TopN"]
    assert any(row["plate"] == "机器人" and row["valid_price_count"] == 1 for row in appendix)
    assert "高度集中" not in report.text_body and "单股驱动" not in report.text_body


def test_no_eligible_concentration_does_not_fill_core_slot() -> None:
    shadow = _plate_shadow()
    report = build_auction_email_report(plate_shadow=shadow)
    assert all(item["observation_type"] != "concentration" for item in report.metadata["core_observations"])


def test_missing_a2_valid_count_is_explicitly_reported() -> None:
    context = {
        "format": "EngineNextStaticFactSnapshotV1", "data_origin": "replay_fixture_only",
        "facts": [{"key": "market:auction:20260821:0925", "value": {"summary": json.dumps({
            "total_stocks": 2, "high_open_count": 1, "low_open_count": 1, "flat_open_count": 0,
            "total_auction_amount_yuan": 1_000_000, "limit_up_count": 0, "limit_down_count": 0,
        })}}],
    }
    report = build_auction_email_report(plate_shadow=_plate_shadow(), market_context=context)
    assert "A2 summary未提供" in report.text_body
    assert "有效价格股票：unavailable" not in report.text_body


def test_structure_budget_and_compact_provenance() -> None:
    report = build_auction_email_report(plate_shadow=_plate_shadow(), market_context=_market_context())
    settings = report.metadata["report_settings"]["display_limits"]
    assert len(report.metadata["core_observations"]) <= settings["core_observation_limit"]
    assert len(report.metadata["plate_rows"]) >= 1
    assert all(len(row["contributors"]) <= settings["contributor_limit"] for row in report.metadata["plate_rows"])
    assert "板块映射" in report.metadata["provenance_display"]
    assert report.metadata["provenance"] != report.metadata["provenance_display"]


def test_core_display_limit_does_not_change_full_observation_semantics(monkeypatch) -> None:
    first = build_auction_email_report(plate_shadow=_plate_shadow())
    monkeypatch.setattr(auction_email_report, "CORE_OBSERVATION_LIMIT", 1)
    second = build_auction_email_report(plate_shadow=_plate_shadow())
    assert second.metadata["observations"] == first.metadata["observations"]
    assert second.metadata["observations_sha256"] == first.metadata["observations_sha256"]
    assert len(second.metadata["core_observations"]) == 1
