from __future__ import annotations

from engine_next.runtime.auction_shadow import build_anchor_shadow_evidence, build_plate_shadow_from_snapshot_rows
from engine_next.runtime.intraday_data_hub import IntradayDataHub


def _rows(*, ask_available: bool = True) -> list[dict[str, object]]:
    rows = [
        {
            "symbol": "000001",
            "tag": "0924",
            "price": 10.00,
            "change_pct": 0.01,
            "amount": 1000.0,
            "bid_amount": 800.0,
            "ask_amount": 100.0,
            "ask_amount_present": ask_available,
        },
        {
            "symbol": "000001",
            "tag": "0925",
            "price": 10.10,
            "change_pct": 0.02,
            "amount": 1500.0,
            "bid_amount": 1000.0,
            "ask_amount": 50.0,
            "ask_amount_present": ask_available,
        },
        {
            "symbol": "000002",
            "tag": "0924",
            "price": 9.90,
            "change_pct": -0.01,
            "amount": 500.0,
            "bid_amount": 200.0,
            "ask_amount": 300.0,
            "ask_amount_present": ask_available,
        },
        {
            "symbol": "000002",
            "tag": "0925",
            "price": 9.80,
            "change_pct": -0.02,
            "amount": 800.0,
            "bid_amount": 150.0,
            "ask_amount": 400.0,
            "ask_amount_present": ask_available,
        },
    ]
    return rows


def test_snapshot_rows_build_fact_only_plate_shadow() -> None:
    result = build_plate_shadow_from_snapshot_rows(
        _rows(),
        trade_date="2026-08-25",
        stock_plate={"000001": "AI", "000002": "AI"},
        multi_labels={"000001": ("AI", "robot")},
        data_origin="production_capture",
        mapping_origin={"canonical": "market:stock_plate", "status": "production_capture"},
        historical_valid=True,
        change_pct_unit="ratio",
    )
    stats = result["plate_stats"]["0924_to_0925"]["AI"]
    assert result["format"] == "PlateAuctionShadowV1"
    assert result["contract_version"] == "PlateAuctionShadowV1"
    assert result["mapping_coverage"]["canonical_mapped_count"] == 2
    assert result["strategy_impact"] == "none"
    assert result["decision_bundle"] is None
    assert stats["stock_count"] == 2
    assert stats["valid_auction_stock_count"] == 2
    assert stats["auction_amount_total_yuan"] == 2300.0
    assert stats["multi_theme_conflict_count"] == 1
    assert stats["evidence_usable_stock_count"] == 2


def test_missing_ask_amount_keeps_pressure_unavailable() -> None:
    result = build_plate_shadow_from_snapshot_rows(
        _rows(ask_available=False),
        trade_date="2026-08-25",
        stock_plate={"000001": "AI", "000002": "AI"},
        data_origin="production_capture",
    )
    stats = result["plate_stats"]["0924_to_0925"]["AI"]
    assert stats["evidence_usable_stock_count"] == 0
    assert stats["evidence_unavailable_stock_count"] == 2
    assert stats["pressure_yuan"] == 0.0


def test_replay_shadow_runner_anchor_adapter_is_available() -> None:
    rows = _rows()
    evidence = build_anchor_shadow_evidence(rows, from_tag="0924", to_tag="0925")
    assert len(evidence) == 2
    assert evidence[0]["from_anchor"] == "0924"
    assert evidence[0]["to_anchor"] == "0925"
    assert evidence[0]["status"] == "resolved"
    assert evidence[0]["amount_reference_bucket"] == "lt_500k"


def test_intraday_snapshot_standardizer_preserves_ask_provenance() -> None:
    row = IntradayDataHub._standardize_auction_snapshot_row(
        {"symbol": "000001", "price": 10.0, "auction_amount_yuan": 1000, "bid_amount_yuan": 800, "ask_amount_yuan": 100},
        tag="0925",
        summary={},
    )
    assert row["ask_amount_yuan"] == 100.0
    assert row["ask_amount_present"] is True
