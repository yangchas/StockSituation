"""Pure auction fact and plate-shadow helpers.

The input rows are already normalized by the existing auction snapshot path.
This module only derives evidence and objective plate aggregates; it does not
read Redis/TDengine, choose a strategy theme, or write a DecisionBundle.
"""

from __future__ import annotations

import math
from collections import defaultdict
from statistics import median
from typing import Any, Iterable, Mapping


def build_anchor_shadow_evidence(
    rows: Iterable[Mapping[str, Any]],
    *,
    from_tag: str,
    to_tag: str,
) -> tuple[dict[str, Any], ...]:
    """Build deterministic per-symbol evidence for two auction anchors.

    ``run_engine_next`` already exposes this optional Shadow output path.  Keep
    the adapter here so the tracked replay runner and the production snapshot
    assembler share one conservative delta implementation.  The rows are
    expected to be normalized snapshot rows; this function does no I/O and
    does not alter any production decision path.
    """

    grouped: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in rows:
        symbol = _symbol(row.get("symbol") or row.get("s"))
        tag = str(row.get("tag") or "").strip()
        if not symbol or tag not in {from_tag, to_tag}:
            continue
        grouped.setdefault(symbol, {})[tag] = row
    return tuple(
        build_anchor_delta_evidence(
            pair.get(from_tag),
            pair.get(to_tag),
            symbol=symbol,
            from_tag=from_tag,
            to_tag=to_tag,
        )
        for symbol, pair in sorted(grouped.items())
    )


def build_anchor_delta_evidence(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any] | None,
    *,
    symbol: str = "",
    from_tag: str = "0924",
    to_tag: str = "0925",
) -> dict[str, Any]:
    """Derive the existing conservative two-anchor evidence semantics."""

    result: dict[str, Any] = {
        "symbol": symbol,
        "from_anchor": from_tag,
        "to_anchor": to_tag,
        "amount_delta_yuan": None,
        "price_delta_milli": None,
        "rest_bid_delta_yuan": None,
        "rest_ask_delta_yuan": None,
        "pressure_delta_yuan": None,
        "amount_ratio": None,
        "withdrawal_yuan": 0.0,
        "auction_directional_pressure_yuan": 0.0,
        "direction": "unresolved",
        "status": "unavailable",
        "labels": [],
        "amount_reference_bucket": None,
        "reference_labels": [],
    }
    previous_values, previous_status = _snapshot_values(previous)
    current_values, current_status = _snapshot_values(current)
    if previous_status == "invalid" or current_status == "invalid":
        result["status"] = "invalid"
        return result
    if previous_status != "valid" or current_status != "valid":
        return result

    amount_delta = current_values["amount"] - previous_values["amount"]
    price_delta = current_values["price"] - previous_values["price"]
    bid_delta = current_values["bid"] - previous_values["bid"]
    ask_delta = current_values["ask"] - previous_values["ask"]
    pressure_delta = (current_values["bid"] - current_values["ask"]) - (
        previous_values["bid"] - previous_values["ask"]
    )
    result.update(
        {
            "amount_delta_yuan": amount_delta,
            "price_delta_milli": price_delta,
            "rest_bid_delta_yuan": bid_delta,
            "rest_ask_delta_yuan": ask_delta,
            "pressure_delta_yuan": pressure_delta,
            "amount_ratio": amount_delta / previous_values["amount"] + 1.0
            if previous_values["amount"] > 0
            else None,
            "withdrawal_yuan": max(-amount_delta, 0.0),
            "amount_reference_bucket": amount_reference_bucket(current_values["amount"]),
        }
    )

    direction = "unresolved"
    labels: list[str] = []
    pressure = 0.0
    if amount_delta > 0:
        if price_delta > 0:
            direction, pressure = "positive", amount_delta
            labels.append("volume_price_strengthening")
        elif price_delta < 0:
            direction, pressure = "negative", -amount_delta
            labels.append("volume_price_weakening")
        elif pressure_delta > 0:
            direction, pressure = "positive", amount_delta
            labels.append("buy_pressure_building")
        elif pressure_delta < 0:
            direction, pressure = "negative", -amount_delta
            labels.append("sell_pressure_building")
        else:
            labels.append("unresolved_direction")
    elif amount_delta < 0:
        labels.append("withdrawal_or_cooling")
    elif pressure_delta > 0:
        labels.append("buy_pressure_building")
    elif pressure_delta < 0:
        labels.append("sell_pressure_building")

    result["direction"] = direction
    result["status"] = "resolved" if direction != "unresolved" else (
        "unresolved" if amount_delta != 0 or pressure_delta != 0 else "balanced"
    )
    result["auction_directional_pressure_yuan"] = pressure
    result["labels"] = labels
    if result["amount_reference_bucket"] == "lt_500k":
        result["reference_labels"] = ["small_volume_unconfirmed"]
    return result


REFERENCE_AMOUNT_BUCKETS = (
    (500_000.0, "lt_500k"),
    (2_000_000.0, "500k_2m"),
    (5_000_000.0, "2m_5m"),
    (math.inf, "gte_5m"),
)


def amount_reference_bucket(amount_yuan: float) -> str:
    """Return a non-binding display bucket for an amount in yuan."""

    value = _first_number({"value": amount_yuan}, ("value",))
    if value is None or value < 0:
        return "invalid"
    for upper_bound, name in REFERENCE_AMOUNT_BUCKETS:
        if value < upper_bound:
            return name
    return "gte_5m"


def build_plate_shadow_from_snapshot_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    trade_date: str,
    stock_plate: Mapping[str, str],
    multi_labels: Mapping[str, Iterable[str]] | None = None,
    data_origin: str = "production_realtime",
    mapping_origin: Mapping[str, Any] | None = None,
    historical_valid: bool = False,
    observation_time: str | None = None,
    source_provenance: Mapping[str, Any] | None = None,
    change_pct_unit: str = "ratio",
) -> dict[str, Any]:
    """Build the minimal ``PlateAuctionShadowV1`` production input envelope.

    No theme weighting or strategy score is applied.  Each symbol contributes
    once to its canonical ``stock_plate``; ``multi_labels`` only records
    ambiguity.  Rows must be the normalized 0924/0925 snapshot rows already
    produced by ``IntradayDataHub``.
    """

    if len(str(trade_date).strip()) != 10:
        raise ValueError("trade_date is required")
    if change_pct_unit not in {"ratio", "percent"}:
        raise ValueError("change_pct_unit must be ratio or percent")
    allowed_origins = {"production_realtime", "production_capture", "current_cache_only", "replay_fixture_only"}
    if data_origin not in allowed_origins:
        raise ValueError(f"unsupported data_origin: {data_origin}")

    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for raw in rows:
        symbol = _symbol(raw.get("symbol") or raw.get("s"))
        tag = str(raw.get("tag") or "").strip()
        if symbol and tag in {"0924", "0925"}:
            grouped[symbol][tag] = dict(raw)

    labels_map = {str(symbol): tuple(str(item).strip() for item in labels if str(item).strip()) for symbol, labels in (multi_labels or {}).items()}
    buckets: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    detail_rows: list[dict[str, Any]] = []

    for symbol in sorted(grouped):
        plate = str(stock_plate.get(symbol) or "").strip()
        if not plate:
            detail_rows.append({"symbol": symbol, "status": "unavailable", "reason": "stock_plate_missing"})
            continue
        bucket = buckets[plate]
        bucket["symbols"].add(symbol)
        labels = labels_map.get(symbol, ())
        conflict = len(labels) > 1 or (labels and plate not in labels)
        bucket["multi_theme_conflict_count"] += int(bool(conflict))
        previous = grouped[symbol].get("0924")
        current = grouped[symbol].get("0925")
        if current is None:
            bucket["unavailable_count"] += 1
            detail_rows.append({"symbol": symbol, "plate": plate, "status": "unavailable", "reason": "0925_missing"})
            continue

        amount = _first_number(current, ("auction_amount_yuan", "amount", "am"))
        change_pct = _change_pct_points(current, unit=change_pct_unit)
        valid_price = _price_milli(current) is not None and (_price_milli(current) or 0) > 0
        if valid_price and change_pct is not None:
            bucket["valid_symbols"].add(symbol)
            bucket["change_pcts"].append(change_pct)
            if amount is not None and amount >= 0:
                bucket["amounts"].append(amount)
        else:
            bucket["unavailable_count"] += 1
            if amount is not None and amount >= 0:
                bucket["price_unavailable_amounts"].append(amount)

        evidence = build_anchor_delta_evidence(previous, current, symbol=symbol)
        if evidence["status"] in {"resolved", "balanced", "unresolved"}:
            bucket["evidence_usable_count"] += 1
            bucket["pressures"].append(float(evidence["auction_directional_pressure_yuan"]))
            bucket["withdrawals"].append(float(evidence["withdrawal_yuan"]))
        else:
            bucket["evidence_unavailable_count"] += 1

        limit_state = _first_int(current, ("limit_state", "ls"))
        if limit_state is None:
            bucket["limit_state_unavailable_count"] += 1
            bucket["seal_unavailable_count"] += 1
        elif limit_state == 1:
            bucket["limit_up_count"] += 1
            seal = _first_number(current, ("limit_bid_amount_yuan", "bid_amount_yuan", "bid_amount", "br"))
            if seal is None:
                bucket["seal_unavailable_count"] += 1
            else:
                bucket["limit_up_seal_amount_yuan"] += seal
                bucket["seal_amount_yuan"] += seal
        elif limit_state == -1:
            bucket["limit_down_count"] += 1
            seal = _first_number(current, ("limit_ask_amount_yuan", "ask_amount_yuan", "ask_amount", "ar"))
            if seal is None:
                bucket["seal_unavailable_count"] += 1
            else:
                bucket["limit_down_sell_pressure_yuan"] += seal
                bucket["seal_amount_yuan"] += seal

        detail_rows.append(
            {
                "symbol": symbol,
                "plate": plate,
                "status": evidence["status"],
                "price_status": "valid" if valid_price and change_pct is not None else "unavailable",
                "auction_amount_yuan": amount,
                "change_pct": change_pct,
                "limit_state": limit_state,
                "pressure_yuan": evidence["auction_directional_pressure_yuan"] if evidence["status"] != "unavailable" else None,
                "withdrawal_yuan": evidence["withdrawal_yuan"] if evidence["status"] != "unavailable" else None,
                "multi_theme_conflict": bool(conflict),
            }
        )

    stats: dict[str, Any] = {}
    for plate, bucket in sorted(buckets.items()):
        amounts = list(bucket["amounts"])
        pressures = list(bucket["pressures"])
        changes = list(bucket["change_pcts"])
        stats[plate] = {
            "stock_count": len(bucket["symbols"]),
            "valid_auction_stock_count": len(bucket["valid_symbols"]),
            "unavailable_auction_stock_count": bucket["unavailable_count"],
            "auction_amount_total_yuan": _rounded(sum(amounts)),
            "price_unavailable_auction_amount_yuan": _rounded(sum(bucket["price_unavailable_amounts"])),
            "change_pct_distribution": _distribution(changes, bucket["unavailable_count"]),
            "limit_up_count": bucket["limit_up_count"],
            "limit_down_count": bucket["limit_down_count"],
            "limit_state_unavailable_count": bucket["limit_state_unavailable_count"],
            "limit_up_seal_amount_yuan": _rounded(bucket["limit_up_seal_amount_yuan"]),
            "limit_down_sell_pressure_yuan": _rounded(bucket["limit_down_sell_pressure_yuan"]),
            "seal_amount_yuan": _rounded(bucket["seal_amount_yuan"]),
            "seal_unavailable_count": bucket["seal_unavailable_count"],
            "pressure_yuan": _rounded(sum(pressures)),
            "pressure_positive_yuan": _rounded(sum(max(value, 0.0) for value in pressures)),
            "pressure_negative_yuan": _rounded(sum(min(value, 0.0) for value in pressures)),
            "withdrawal_yuan": _rounded(sum(bucket["withdrawals"])),
            "evidence_usable_stock_count": bucket["evidence_usable_count"],
            "evidence_unavailable_stock_count": bucket["evidence_unavailable_count"],
            "top3_amount_concentration": _concentration(amounts),
            "top3_pressure_concentration": _concentration(pressures, absolute=True),
            "multi_theme_conflict_count": bucket["multi_theme_conflict_count"],
            "accounting": "canonical_stock_plate_only; multi_labels are audit-only",
        }

    return {
        "format": "PlateAuctionShadowV1",
        "mapping_origin": dict(mapping_origin or {"canonical": "market:stock_plate"}),
        "data_origin": data_origin,
        "trade_date": str(trade_date),
        "historical_valid": bool(historical_valid),
        "observation_time": observation_time,
        "source_provenance": dict(source_provenance or {}),
        "plate_stats": {"0924_to_0925": stats},
        "symbol_details": {"0924_to_0925": {"detail_rows": detail_rows}},
        "automatic_analysis": {"auction_locked_orders": {"limit_up": [], "limit_down": []}},
        "strategy_impact": "none",
        "decision_bundle": None,
    }


def _new_bucket() -> dict[str, Any]:
    return {
        "symbols": set(), "valid_symbols": set(), "amounts": [], "pressures": [], "withdrawals": [],
        "price_unavailable_amounts": [], "change_pcts": [], "unavailable_count": 0,
        "limit_up_count": 0, "limit_down_count": 0, "limit_state_unavailable_count": 0,
        "limit_up_seal_amount_yuan": 0.0, "limit_down_sell_pressure_yuan": 0.0,
        "seal_amount_yuan": 0.0, "seal_unavailable_count": 0,
        "multi_theme_conflict_count": 0, "evidence_usable_count": 0, "evidence_unavailable_count": 0,
    }


def _snapshot_values(row: Mapping[str, Any] | None) -> tuple[dict[str, float], str]:
    if row is None:
        return {}, "unavailable"
    price = _price_milli(row)
    amount = _first_number(row, ("auction_amount_yuan", "amount", "am"))
    bid = _first_number(row, ("bid_amount_yuan", "bid_amount", "br"))
    ask = _first_number(row, ("ask_amount_yuan", "ask_amount", "ar"))
    if row.get("ask_amount_present", True) is False:
        ask = None
    values = {"price": price, "amount": amount, "bid": bid, "ask": ask}
    if any(value is None for value in values.values()):
        return {}, "unavailable"
    if any(not math.isfinite(float(value)) or float(value) < 0 for value in values.values()):
        return {}, "invalid"
    if float(values["price"]) <= 0:
        return {}, "invalid"
    return {name: float(value) for name, value in values.items()}, "valid"


def _price_milli(row: Mapping[str, Any]) -> float | None:
    value = _first_number(row, ("price_milli", "px"))
    if value is not None:
        return value
    price = _first_number(row, ("price",))
    return price * 1000.0 if price is not None else None


def _change_pct_points(row: Mapping[str, Any], *, unit: str) -> float | None:
    value = _first_number(row, ("change_pct", "open_pct"))
    if value is None:
        return None
    return value * 100.0 if unit == "ratio" else value


def _first_number(row: Mapping[str, Any], names: tuple[str, ...]) -> float | None:
    for name in names:
        if name in row and row[name] is not None and str(row[name]).strip() != "":
            try:
                value = float(row[name])
            except (TypeError, ValueError):
                continue
            return value if math.isfinite(value) else None
    return None


def _first_int(row: Mapping[str, Any], names: tuple[str, ...]) -> int | None:
    value = _first_number(row, names)
    return int(value) if value is not None else None


def _symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".", 1)[0]
    return text if len(text) == 6 and text.isdigit() else ""


def _rounded(value: float | None) -> float | None:
    return round(float(value), 2) if value is not None else None


def _distribution(values: list[float], unavailable_count: int) -> dict[str, Any]:
    return {
        "count": len(values),
        "min_pct": _rounded(min(values)) if values else None,
        "max_pct": _rounded(max(values)) if values else None,
        "median_pct": _rounded(median(values)) if values else None,
        "positive_count": sum(value > 0 for value in values),
        "negative_count": sum(value < 0 for value in values),
        "zero_count": sum(value == 0 for value in values),
        "unavailable_count": unavailable_count,
    }


def _concentration(values: list[float], *, absolute: bool = False) -> float | None:
    normalized = [abs(value) if absolute else value for value in values if math.isfinite(value)]
    denominator = sum(normalized)
    return round(sum(sorted(normalized, reverse=True)[:3]) / denominator, 4) if denominator > 0 else None
