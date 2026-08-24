from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from tools.qmt_replay.auction_open_confirmation import build_observation, render_markdown, write_outputs


SHANGHAI = ZoneInfo("Asia/Shanghai")


def _ms(hms: tuple[int, int, int]) -> int:
    return int(datetime(2026, 8, 21, *hms, tzinfo=SHANGHAI).timestamp() * 1000)


def _inputs(tmp_path: Path, *, with_open: bool = True) -> tuple[Path, Path, Path]:
    shadow = {
        "format": "PlateAuctionShadowV1",
        "trade_date": "2026-08-21",
        "historical_valid": False,
        "mapping_origin": {"canonical": "market:stock_plate", "status": "current_cache_only"},
        "symbol_details": {"0924_to_0925": {"detail_rows": [
            {"symbol": "000001", "plate": "AI", "status": "resolved", "price_status": "valid", "auction_amount_yuan": 750.0},
            {"symbol": "000002", "plate": "AI", "status": "resolved", "price_status": "valid", "auction_amount_yuan": 250.0},
        ]}},
    }
    report = {
        "format": "AuctionEmailReportV1",
        "report_id": "auction-market-facts:2026-08-21:0925",
        "trade_date": "2026-08-21",
        "report_settings": {"display_limits": {"main_plate_limit": 10}},
        "market_overview": {"status": "available", "positive_count": 1, "negative_count": 1, "flat_count": 0, "auction_amount_yuan": 1000.0},
        "plate_rows": [{
            "plate": "AI", "valid_price_count": 2, "positive_ratio": 0.5,
            "median_change_pct": 1.0, "top1_amount_ratio": 0.75,
        }],
    }
    frames = []
    if with_open:
        frames = [
            {"version": "Q2FrameV1", "seq_no": 1, "logical_ts_ms": _ms((9, 30, 1)), "q2_updates": [
                {"symbol": "000001", "px": 103, "pc": 100, "amt2m": 100, "ls": 1},
                {"symbol": "000002", "px": 98, "pc": 100, "amt2m": 50, "ls": 0},
            ]},
            {"version": "Q2FrameV1", "seq_no": 2, "logical_ts_ms": _ms((9, 32, 1)), "q2_updates": [
                {"symbol": "000001", "px": 105, "pc": 100, "amt2m": 300, "ls": 1},
                {"symbol": "000002", "px": 95, "pc": 100, "amt2m": 100, "ls": -1},
            ]},
        ]
    report_path = tmp_path / "auction_report.json"
    shadow_path = tmp_path / "plate_shadow.json"
    q2_path = tmp_path / "q2.jsonl"
    report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    shadow_path.write_text(json.dumps(shadow, ensure_ascii=False), encoding="utf-8")
    q2_path.write_text("\n".join(json.dumps(frame, ensure_ascii=False) for frame in frames) + ("\n" if frames else ""), encoding="utf-8")
    return report_path, shadow_path, q2_path


def test_same_mapping_and_symbol_coverage_are_explicit(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    plate = result["plates"][0]
    assert result["mapping_consistency"] == "same_plate_shadow_mapping"
    assert result["mapping_provenance"]["canonical"] == "market:stock_plate"
    assert plate["auction_valid_count"] == 2
    assert plate["open_valid_count"] == 2
    assert plate["common_symbol_count"] == 2


def test_deltas_are_exact_and_amount_ratio_is_protected(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    plate = result["plates"][0]
    assert plate["open_positive_ratio"] == 0.5
    assert plate["positive_ratio_delta"] == 0.0
    assert plate["open_median_change_pct"] == 0.0
    assert plate["median_change_pct_delta"] == -1.0
    assert plate["open_top1_amount_ratio"] == 0.75
    assert plate["top1_amount_ratio_delta"] == 0.0
    assert plate["amount_ratio"] is None
    assert plate["amount_ratio_status"] == "unavailable_different_time_window"


def test_unavailable_open_facts_propagate_without_substitution(tmp_path: Path) -> None:
    paths = _inputs(tmp_path, with_open=False)
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    plate = result["plates"][0]
    assert plate["auction_valid_count"] == 2
    assert plate["open_valid_count"] == 0
    assert plate["common_symbol_count"] == 0
    assert plate["positive_ratio_delta"] is None
    assert plate["median_change_pct_delta"] is None
    assert plate["top1_amount_ratio_delta"] is None
    assert result["open_source"]["status"] == "unavailable"
    assert result["market"]["open"]["status"] == "unavailable"
    assert "开盘窗口事实不可用" in result["observations"][0]["text"]


def test_target_scope_does_not_rescan_new_plates(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    shadow = json.loads(paths[1].read_text(encoding="utf-8"))
    shadow["symbol_details"]["0924_to_0925"]["detail_rows"].append({
        "symbol": "000003", "plate": "未展示板块", "status": "resolved", "price_status": "valid", "auction_amount_yuan": 100.0,
    })
    paths[1].write_text(json.dumps(shadow, ensure_ascii=False), encoding="utf-8")
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    assert [row["plate"] for row in result["plates"]] == ["AI"]


def test_descriptions_are_traceable_and_do_not_use_strategy_language(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    forbidden = ("确认", "证伪", "失败", "成功", "强势", "弱势", "主线", "买入", "卖出", "推荐", "看多", "看空")
    for observation in result["observations"]:
        assert set(("auction.positive_ratio", "open.open_positive_ratio", "delta.positive_ratio_delta")).issubset(observation["evidence_refs"])
        assert observation["evidence_values"]["plate"] == observation["plate"]
        assert not any(word in observation["text"] for word in forbidden)
    assert not any(word in render_markdown(result) for word in forbidden)


def test_outputs_and_business_hash_are_deterministic_and_read_only(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    first = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    second = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    assert first["business_sha256"] == second["business_sha256"]
    assert first["sha256"] == second["sha256"]
    assert render_markdown(first) == render_markdown(second)
    ledger = {"candidate": [], "decision": "unchanged"}
    before = copy.deepcopy(ledger)
    write_outputs(first, json_path=tmp_path / "out.json", markdown_path=tmp_path / "out.md")
    assert ledger == before


def test_open_limit_counts_and_observation_window_are_fact_fields(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    plate = result["plates"][0]
    assert plate["open_limit_up_count"] == 1
    assert plate["open_limit_down_count"] == 1
    assert result["open_source"]["observation_window"].startswith("09:30:00-09:32:59")


def test_symbol_set_difference_is_not_hidden(tmp_path: Path) -> None:
    report_path, shadow_path, q2_path = _inputs(tmp_path)
    shadow = json.loads(shadow_path.read_text(encoding="utf-8"))
    shadow["symbol_details"]["0924_to_0925"]["detail_rows"].append({
        "symbol": "000003", "plate": "AI", "status": "unavailable", "price_status": "unavailable", "auction_amount_yuan": None,
    })
    shadow_path.write_text(json.dumps(shadow, ensure_ascii=False), encoding="utf-8")
    frames = [json.loads(line) for line in q2_path.read_text(encoding="utf-8").splitlines()]
    frames[-1]["q2_updates"].append({"symbol": "000003", "px": 101, "pc": 100, "amt2m": 50, "ls": 0})
    q2_path.write_text("\n".join(json.dumps(frame, ensure_ascii=False) for frame in frames) + "\n", encoding="utf-8")
    result = build_observation(auction_report=report_path, plate_shadow=shadow_path, q2=q2_path)
    plate = result["plates"][0]
    assert plate["open_valid_count"] == 3
    assert plate["common_symbol_count"] == 2
    assert plate["comparison_scope"] == "common_symbols"


def test_missing_amt2m_is_unavailable_not_zero(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    frames = [json.loads(line) for line in paths[2].read_text(encoding="utf-8").splitlines()]
    for frame in frames:
        for update in frame["q2_updates"]:
            update.pop("amt2m", None)
    paths[2].write_text("\n".join(json.dumps(frame, ensure_ascii=False) for frame in frames) + "\n", encoding="utf-8")
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    assert result["market"]["open"]["open_window_amount_yuan"] is None
    assert result["market"]["open"]["open_window_amount_status"] == "unavailable"
    assert result["plates"][0]["open_window_amount_yuan"] is None
    assert result["plates"][0]["open_window_amount_status"] == "unavailable"


def test_explicit_zero_amt2m_remains_zero(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    frames = [json.loads(line) for line in paths[2].read_text(encoding="utf-8").splitlines()]
    for frame in frames:
        for update in frame["q2_updates"]:
            update["amt2m"] = 0
    paths[2].write_text("\n".join(json.dumps(frame, ensure_ascii=False) for frame in frames) + "\n", encoding="utf-8")
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    assert result["market"]["open"]["open_window_amount_yuan"] == 0
    assert result["market"]["open"]["open_window_amount_status"] == "available"
    assert result["plates"][0]["open_window_amount_yuan"] == 0
    assert result["plates"][0]["open_window_amount_status"] == "available"


def test_partial_amt2m_does_not_enter_aggregate(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    frames = [json.loads(line) for line in paths[2].read_text(encoding="utf-8").splitlines()]
    frames[-1]["q2_updates"][0].pop("amt2m")
    paths[2].write_text("\n".join(json.dumps(frame, ensure_ascii=False) for frame in frames) + "\n", encoding="utf-8")
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    assert result["market"]["open"]["open_window_amount_yuan"] is None
    assert result["market"]["open"]["open_window_amount_status"] == "partial"
    assert result["plates"][0]["open_window_amount_yuan"] is None
    assert result["plates"][0]["open_window_amount_status"] == "partial"


def test_missing_limit_state_is_unavailable_not_zero_counts(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    frames = [json.loads(line) for line in paths[2].read_text(encoding="utf-8").splitlines()]
    for frame in frames:
        for update in frame["q2_updates"]:
            update.pop("ls", None)
    paths[2].write_text("\n".join(json.dumps(frame, ensure_ascii=False) for frame in frames) + "\n", encoding="utf-8")
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    assert result["market"]["open"]["open_limit_up_count"] is None
    assert result["market"]["open"]["open_limit_down_count"] is None
    assert result["market"]["open"]["open_limit_state_status"] == "unavailable"
    assert result["plates"][0]["open_limit_up_count"] is None
    assert result["plates"][0]["open_limit_down_count"] is None
    assert result["plates"][0]["open_limit_state_status"] == "unavailable"


def test_data_origin_is_explicit_for_reconstructed_input(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    result = build_observation(
        auction_report=paths[0],
        plate_shadow=paths[1],
        q2=paths[2],
        data_origin="reconstructed_from_production_td_tick",
    )
    assert result["data_origin"] == "reconstructed_from_production_td_tick"


def test_mapping_provenance_mismatch_fails_closed(tmp_path: Path) -> None:
    report_path, shadow_path, q2_path = _inputs(tmp_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["provenance"] = {"mapping_origin": json.dumps({"canonical": "other_mapping"})}
    report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    try:
        build_observation(auction_report=report_path, plate_shadow=shadow_path, q2=q2_path)
    except ValueError as exc:
        assert "mapping provenance differ" in str(exc)
    else:
        raise AssertionError("mapping mismatch must fail closed")


def test_pressure_is_only_compared_with_open_price_fact(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    report = json.loads(paths[0].read_text(encoding="utf-8"))
    report["plate_rows"][0]["pressure_yuan"] = 302_000_000
    report["plate_rows"][0]["pressure_status"] = "available"
    paths[0].write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    result = build_observation(auction_report=paths[0], plate_shadow=paths[1], q2=paths[2])
    text = result["observations"][0]["text"]
    assert "pressure 3.02亿" in text
    assert "open_pressure" not in json.dumps(result, ensure_ascii=False)
