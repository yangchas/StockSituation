"""Build a read-only 09:25-to-open facts comparison.

This is deliberately a replay/report-side adapter.  It consumes the frozen
auction report facts and existing Q2 observations from 09:30--09:32:59.  It
does not read or write Redis, does not classify strategy outcomes, and never
recomputes auction facts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, time
from itertools import chain
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
OPEN_START = time(9, 30)
OPEN_END = time(9, 32, 59, 999999)
FORMAT = "OpenConfirmationObservationV1"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _rows(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:  # NaN/inf
        return None
    return number


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    payload = value if isinstance(value, (bytes, bytearray)) else _canonical(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _fmt_pct(value: Any) -> str:
    number = _number(value)
    return "unavailable" if number is None else f"{number:.2f}%"


def _fmt_yi(value: Any) -> str:
    number = _number(value)
    return "unavailable" if number is None else f"{number / 100_000_000.0:.2f}亿"


def _delta(after: Any, before: Any) -> float | None:
    after_number, before_number = _number(after), _number(before)
    return after_number - before_number if after_number is not None and before_number is not None else None


def _optional_sum(rows: Iterable[Mapping[str, Any]], field: str) -> tuple[float | None, str, int, int]:
    """Sum only complete values; missing is never treated as numeric zero."""

    values = list(rows)
    if not values:
        return None, "unavailable", 0, 0
    present = [_number(row.get(field)) for row in values]
    available = [value for value in present if value is not None]
    if len(available) != len(values):
        return None, "partial" if available else "unavailable", len(available), len(values)
    return sum(available), "available", len(available), len(values)


def _state(delta: Any) -> str:
    value = _number(delta)
    if value is None:
        return "unavailable"
    if value > 0:
        return "expanded"
    if value < 0:
        return "contracted"
    return "unchanged"


def _sign_state(before: Any, after: Any) -> str:
    before_number, after_number = _number(before), _number(after)
    if before_number is None or after_number is None:
        return "unavailable"
    if before_number * after_number < 0:
        return "reversed"
    return _state(_delta(after_number, before_number))


def _parse_report(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("auction report must be a JSON object")
    report = dict(payload)
    if report.get("format") not in {"AuctionEmailReportV1", "AuctionEmailReport"}:
        raise ValueError("unsupported auction report format")
    return report


def _parse_shadow(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or payload.get("format") != "PlateAuctionShadowV1":
        raise ValueError("plate shadow must be PlateAuctionShadowV1")
    return dict(payload)


def _auction_symbols_by_plate(shadow: Mapping[str, Any]) -> dict[str, set[str]]:
    transition = _mapping(_mapping(shadow.get("symbol_details")).get("0924_to_0925"))
    result: dict[str, set[str]] = {}
    for raw in _rows(transition.get("detail_rows")):
        symbol = str(raw.get("symbol") or "").strip()
        plate = str(raw.get("plate") or "").strip()
        status = str(raw.get("status") or "unavailable")
        amount = _number(raw.get("auction_amount_yuan"))
        price_status = str(raw.get("price_status") or "valid")
        if not symbol or not plate or status in {"invalid", "unavailable"} or price_status in {"invalid", "unavailable"} or amount is None:
            continue
        result.setdefault(plate, set()).add(symbol)
    return result


def _mapped_symbols_by_plate(shadow: Mapping[str, Any]) -> dict[str, set[str]]:
    transition = _mapping(_mapping(shadow.get("symbol_details")).get("0924_to_0925"))
    result: dict[str, set[str]] = {}
    for raw in _rows(transition.get("detail_rows")):
        symbol = str(raw.get("symbol") or "").strip()
        plate = str(raw.get("plate") or "").strip()
        if symbol and plate:
            result.setdefault(plate, set()).add(symbol)
    return result


def _open_rows(q2_path: Path, trade_date: str) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Read Q2 frames once, retaining only first/latest rows in the open window."""
    frames: list[Mapping[str, Any]] = []
    with q2_path.open("r", encoding="utf-8", newline="") as handle:
        for line_no, line in enumerate(handle, 1):
            text = line.rstrip("\r\n")
            if not text:
                continue
            try:
                frame = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid Q2Frame JSON at line {line_no}") from exc
            if not isinstance(frame, Mapping):
                raise ValueError(f"Q2Frame line {line_no} is not an object")
            frames.append(frame)
    return _open_rows_from_frames(frames, trade_date=trade_date)


def _open_rows_from_frames(
    frames: Iterable[Mapping[str, Any]],
    *,
    trade_date: str,
    observation_cutoff: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Normalize online or replay Q2 frames without changing Q2 semantics."""
    first: dict[str, dict[str, Any]] = {}
    latest: dict[str, dict[str, Any]] = {}
    latest_ts = 0
    frame_count = 0
    normalized_cutoff = None
    if observation_cutoff is not None:
        normalized_cutoff = (
            observation_cutoff.replace(tzinfo=SHANGHAI)
            if observation_cutoff.tzinfo is None
            else observation_cutoff.astimezone(SHANGHAI)
        )
    cutoff_ms = int(normalized_cutoff.timestamp() * 1000) if normalized_cutoff is not None else None
    for frame in frames:
        timestamp_ms = _number(frame.get("logical_ts_ms"))
        if timestamp_ms is None or timestamp_ms <= 0 or (cutoff_ms is not None and timestamp_ms > cutoff_ms):
            continue
        local = datetime.fromtimestamp(timestamp_ms / 1000.0, SHANGHAI)
        if local.date().isoformat() != trade_date or not (OPEN_START <= local.time() <= OPEN_END):
            continue
        frame_count += 1
        latest_ts = max(latest_ts, int(timestamp_ms))
        for raw in _rows(frame.get("q2_updates")):
            symbol = str(raw.get("symbol") or "").strip()
            if not symbol:
                continue
            row = {
                "symbol": symbol,
                "timestamp_ms": int(timestamp_ms),
                "price_milli": _number(raw.get("px")),
                "previous_close_milli": _number(raw.get("pc")),
                "amount_2m_yuan": _number(raw.get("amt2m")),
                "limit_state": _number(raw.get("ls")),
            }
            first.setdefault(symbol, row)
            latest[symbol] = row
    latest_rows = list(latest.values())
    amount_present = sum(_number(row.get("amount_2m_yuan")) is not None for row in latest_rows)
    limit_present = sum(_number(row.get("limit_state")) is not None for row in latest_rows)
    return latest, {
        "source": "Q2FrameV1",
        "status": "available" if frame_count else "unavailable",
        "observation_window": "09:30:00-09:32:59.999999 Asia/Shanghai",
        "frame_count": frame_count,
        "first_symbol_count": len(first),
        "latest_symbol_count": len(latest),
        "latest_amount_2m_present_count": amount_present,
        "latest_limit_state_present_count": limit_present,
        "latest_amount_2m_status": "available" if latest_rows and amount_present == len(latest_rows) else ("partial" if amount_present else "unavailable"),
        "latest_limit_state_status": "available" if latest_rows and limit_present == len(latest_rows) else ("partial" if limit_present else "unavailable"),
        "observation_time": datetime.fromtimestamp(latest_ts / 1000.0, SHANGHAI).isoformat() if latest_ts else None,
        "fields": ["px", "pc", "amt2m", "ls"],
    }


def _open_rows_from_online_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    trade_date: str,
    observation_cutoff: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Normalize production Q2 rows returned by Redis/IntradayDataHub.

    Production rows are not Q2Frame envelopes.  Accept the raw Q2 field names
    (``ts/px/pc/amt2m/ls``) and the existing IntradayDataHub names
    (``timestamp/price/pre_close/amount_2m/limit_state``) without changing
    their business semantics.
    """

    first: dict[str, dict[str, Any]] = {}
    latest: dict[str, dict[str, Any]] = {}
    row_count = 0
    latest_ts = 0
    normalized_cutoff = None
    if observation_cutoff is not None:
        normalized_cutoff = (
            observation_cutoff.replace(tzinfo=SHANGHAI)
            if observation_cutoff.tzinfo is None
            else observation_cutoff.astimezone(SHANGHAI)
        )
    cutoff_ms = int(normalized_cutoff.timestamp() * 1000) if normalized_cutoff is not None else None

    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        symbol = str(raw.get("symbol") or raw.get("s") or "").strip()
        timestamp_ms = _online_timestamp_ms(raw)
        if not symbol or timestamp_ms is None or timestamp_ms <= 0:
            continue
        if cutoff_ms is not None and timestamp_ms > cutoff_ms:
            continue
        local = datetime.fromtimestamp(timestamp_ms / 1000.0, SHANGHAI)
        if local.date().isoformat() != trade_date or not (OPEN_START <= local.time() <= OPEN_END):
            continue
        row = {
            "symbol": symbol,
            "timestamp_ms": timestamp_ms,
            "price_milli": _online_price_milli(raw, ("price_milli", "px"), ("price",)),
            "previous_close_milli": _online_price_milli(raw, ("previous_close_milli", "pc"), ("pre_close",)),
            "amount_2m_yuan": _first_number(raw, ("amount_2m_yuan", "amount_2m", "amt2m")),
            "limit_state": _first_number(raw, ("limit_state", "ls")),
        }
        row_count += 1
        latest_ts = max(latest_ts, timestamp_ms)
        first.setdefault(symbol, row)
        latest[symbol] = row

    latest_rows = list(latest.values())
    amount_present = sum(_number(row.get("amount_2m_yuan")) is not None for row in latest_rows)
    limit_present = sum(_number(row.get("limit_state")) is not None for row in latest_rows)
    return latest, {
        "source": "production_online_q2",
        "status": "available" if row_count else "unavailable",
        "observation_window": "09:30:00-09:32:59.999999 Asia/Shanghai",
        "row_count": row_count,
        "first_symbol_count": len(first),
        "latest_symbol_count": len(latest),
        "latest_amount_2m_present_count": amount_present,
        "latest_limit_state_present_count": limit_present,
        "latest_amount_2m_status": "available" if latest_rows and amount_present == len(latest_rows) else ("partial" if amount_present else "unavailable"),
        "latest_limit_state_status": "available" if latest_rows and limit_present == len(latest_rows) else ("partial" if limit_present else "unavailable"),
        "observation_time": datetime.fromtimestamp(latest_ts / 1000.0, SHANGHAI).isoformat() if latest_ts else None,
        "fields": ["px", "pc", "amt2m", "ls"],
        "input_shape": "online_q2_rows",
        "observation_cutoff": normalized_cutoff.isoformat() if normalized_cutoff is not None else None,
    }


def _first_number(row: Mapping[str, Any], names: tuple[str, ...]) -> float | None:
    for name in names:
        if name in row and row[name] is not None and str(row[name]).strip() != "":
            return _number(row[name])
    return None


def _online_price_milli(
    row: Mapping[str, Any],
    milli_names: tuple[str, ...],
    price_names: tuple[str, ...],
) -> float | None:
    value = _first_number(row, milli_names)
    if value is not None:
        return value
    price = _first_number(row, price_names)
    return round(price * 1000.0) if price is not None else None


def _online_timestamp_ms(row: Mapping[str, Any]) -> int | None:
    value = _first_number(row, ("timestamp_ms", "ts", "timestamp"))
    if value is None or value < 1_000_000_000_000:
        return None
    return int(value)


def _open_fact(row: Mapping[str, Any]) -> dict[str, Any]:
    price, previous = _number(row.get("price_milli")), _number(row.get("previous_close_milli"))
    valid = price is not None and previous is not None and price > 0 and previous > 0
    change_pct = ((price / previous) - 1.0) * 100.0 if valid else None
    limit_state = _number(row.get("limit_state"))
    return {
        "symbol": str(row.get("symbol") or ""),
        "timestamp_ms": row.get("timestamp_ms"),
        "change_pct": change_pct,
        "amount_2m_yuan": _number(row.get("amount_2m_yuan")),
        "limit_state": int(limit_state) if limit_state is not None and limit_state.is_integer() else limit_state,
        "status": "available" if valid else "unavailable",
    }


def _plate_targets(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    settings = _mapping(report.get("report_settings"))
    limits = _mapping(settings.get("display_limits"))
    limit = int(_number(limits.get("main_plate_limit")) or 10)
    rows = _rows(report.get("plate_rows"))
    by_plate = {str(row.get("plate") or ""): row for row in rows}
    selected: list[dict[str, Any]] = []
    selected_names: set[str] = set()

    def add(plate: Any) -> None:
        name = str(plate or "").strip()
        if name and name in by_plate and name not in selected_names:
            selected.append(dict(by_plate[name]))
            selected_names.add(name)

    for row in rows[:limit]:
        add(row.get("plate"))
    for raw in _rows(report.get("core_observations") or report.get("observations")):
        add(_mapping(raw.get("evidence_values")).get("plate"))
    for raw in _rows(report.get("locked_order_rows")):
        add(raw.get("plate"))
    return selected


def _auction_market_facts(report: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(report.get("market_overview"))

    def first(*keys: str) -> Any:
        for key in keys:
            if raw.get(key) is not None:
                return raw.get(key)
        return None

    return {
        "status": raw.get("status", "unavailable"),
        "source": raw.get("source", "unavailable"),
        "observation_time": raw.get("observation_time"),
        "stock_count": first("stock_count", "candidate_count"),
        "valid_stock_count": first("valid_stock_count", "valid_count"),
        "positive_count": first("positive_count", "valid_change_positive_count"),
        "negative_count": first("negative_count", "valid_change_negative_count"),
        "flat_count": first("flat_count", "valid_change_flat_count"),
        "auction_amount_yuan": first("auction_amount_yuan", "auction_amount_total_yuan", "valid_auction_amount_yuan"),
        "valid_auction_amount_yuan": first("valid_auction_amount_yuan", "auction_amount_yuan"),
        "limit_up_count": raw.get("limit_up_count"),
        "limit_down_count": raw.get("limit_down_count"),
        "limit_up_seal_amount_yuan": raw.get("limit_up_seal_amount_yuan"),
    }


def _mapping_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
        return parsed
    return value


def _plate_open_fact(auction_row: Mapping[str, Any], auction_symbols: set[str], mapped_open_symbols: set[str], open_facts: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    open_symbol_set = mapped_open_symbols & set(open_facts)
    common_symbol_set = auction_symbols & open_symbol_set
    open_rows = [dict(open_facts[symbol]) for symbol in sorted(open_symbol_set)]
    comparison_rows = [dict(open_facts[symbol]) for symbol in sorted(common_symbol_set)]
    valid_open = [row for row in open_rows if row.get("status") == "available"]
    valid_comparison = [row for row in comparison_rows if row.get("status") == "available"]
    changes = [_number(row.get("change_pct")) for row in valid_comparison]
    changes = [value for value in changes if value is not None]
    open_amount_total, open_amount_status, open_amount_present, open_amount_total_count = _optional_sum(valid_open, "amount_2m_yuan")
    amount_total, comparison_amount_status, comparison_amount_present, comparison_amount_total_count = _optional_sum(valid_comparison, "amount_2m_yuan")
    open_amounts = [_number(row.get("amount_2m_yuan")) for row in valid_open]
    open_amounts = [value for value in open_amounts if value is not None and value >= 0]
    amounts = [_number(row.get("amount_2m_yuan")) for row in valid_comparison]
    amounts = [value for value in amounts if value is not None and value >= 0]
    if open_amount_status != "available":
        open_amount_total = None
    if comparison_amount_status != "available":
        amount_total = None
    ordered_amounts = sorted(amounts, reverse=True)
    limit_states = [row.get("limit_state") for row in valid_comparison]
    limit_present = sum(value is not None for value in limit_states)
    limit_status = "available" if limit_states and limit_present == len(limit_states) else ("partial" if limit_present else "unavailable")
    limit_available = limit_status == "available"
    open_top1 = ordered_amounts[0] / amount_total if amount_total not in (None, 0) and ordered_amounts else None
    open_top3 = sum(ordered_amounts[:3]) / amount_total if amount_total not in (None, 0) and ordered_amounts else None
    auction_positive = _number(auction_row.get("positive_ratio"))
    auction_median = _number(auction_row.get("median_change_pct"))
    auction_top1 = _number(auction_row.get("top1_amount_ratio"))
    return {
        "plate": str(auction_row.get("plate") or ""),
        "auction_valid_count": int(_number(auction_row.get("valid_price_count")) or 0),
        "open_valid_count": len(valid_open),
        "common_symbol_count": len(common_symbol_set),
        "comparison_valid_count": len(valid_comparison),
        "comparison_scope": "common_symbols",
        "open_up_count": sum(1 for value in changes if value > 0),
        "open_down_count": sum(1 for value in changes if value < 0),
        "open_flat_count": sum(1 for value in changes if value == 0),
        "open_positive_ratio": (sum(1 for value in changes if value > 0) / len(changes)) if changes else None,
        "open_negative_ratio": (sum(1 for value in changes if value < 0) / len(changes)) if changes else None,
        "open_median_change_pct": median(changes) if changes else None,
        "open_window_amount_yuan": open_amount_total,
        "open_window_amount_status": open_amount_status,
        "open_amount_present_count": open_amount_present,
        "open_amount_total_count": open_amount_total_count,
        "comparison_amount_status": comparison_amount_status,
        "comparison_amount_present_count": comparison_amount_present,
        "comparison_amount_total_count": comparison_amount_total_count,
        "open_limit_up_count": sum(1 for value in limit_states if value == 1) if limit_available else None,
        "open_limit_down_count": sum(1 for value in limit_states if value == -1) if limit_available else None,
        "open_limit_state_status": limit_status,
        "open_limit_state_present_count": limit_present,
        "open_limit_state_total_count": len(limit_states),
        "open_top1_amount_ratio": open_top1,
        "open_top3_amount_ratio": open_top3,
        "auction_positive_ratio": auction_positive,
        "auction_median_change_pct": auction_median,
        "auction_top1_amount_ratio": auction_top1,
        "auction_pressure_yuan": _number(auction_row.get("pressure_yuan")),
        "auction_pressure_status": str(auction_row.get("pressure_status") or ("available" if auction_row.get("pressure_yuan") is not None else "unavailable")),
        "positive_ratio_delta": _delta((sum(1 for value in changes if value > 0) / len(changes)) if changes else None, auction_positive),
        "median_change_pct_delta": _delta(median(changes) if changes else None, auction_median),
        "top1_amount_ratio_delta": _delta(open_top1, auction_top1),
        "amount_ratio": None,
        "amount_ratio_status": "unavailable_different_time_window",
        "price_breadth_state": _state(_delta((sum(1 for value in changes if value > 0) / len(changes)) if changes else None, auction_positive)),
        "median_change_state": _sign_state(auction_median, median(changes) if changes else None),
        "concentration_state": _state(_delta(open_top1, auction_top1)),
        "open_symbols": sorted(row["symbol"] for row in valid_open),
    }


def _observation(row: Mapping[str, Any]) -> dict[str, Any]:
    parts: list[str] = []
    positive = row.get("positive_ratio_delta")
    median_delta = row.get("median_change_pct_delta")
    if row.get("auction_positive_ratio") is not None and row.get("open_positive_ratio") is not None and positive is not None:
        parts.append(f"上涨覆盖{_fmt_pct(float(row['auction_positive_ratio']) * 100)}→{_fmt_pct(float(row['open_positive_ratio']) * 100)}，变化{_fmt_pct(float(positive) * 100)}")
    if row.get("auction_median_change_pct") is not None and row.get("open_median_change_pct") is not None and median_delta is not None:
        parts.append(f"中位涨幅{_fmt_pct(row['auction_median_change_pct'])}→{_fmt_pct(row['open_median_change_pct'])}，变化{_fmt_pct(median_delta)}")
    if row.get("auction_top1_amount_ratio") is not None and row.get("open_top1_amount_ratio") is not None and row.get("top1_amount_ratio_delta") is not None:
        parts.append(f"Top1金额占比{_fmt_pct(float(row['auction_top1_amount_ratio']) * 100)}→{_fmt_pct(float(row['open_top1_amount_ratio']) * 100)}，变化{_fmt_pct(float(row['top1_amount_ratio_delta']) * 100)}")
    if row.get("auction_pressure_status") == "available" and row.get("auction_pressure_yuan") is not None and row.get("open_median_change_pct") is not None:
        parts.append(f"09:25 pressure {_fmt_yi(row['auction_pressure_yuan'])}，开盘中位涨幅{_fmt_pct(row['open_median_change_pct'])}")
    text = f"{row['plate']}：" + "；".join(parts) if parts else f"{row['plate']}：开盘窗口事实不可用。"
    refs = [
        "auction.positive_ratio", "open.open_positive_ratio", "delta.positive_ratio_delta",
        "auction.median_change_pct", "open.open_median_change_pct", "delta.median_change_pct_delta",
        "auction.top1_amount_ratio", "open.open_top1_amount_ratio", "delta.top1_amount_ratio_delta",
        "auction.pressure_yuan", "open.open_median_change_pct",
    ]
    return {"plate": row["plate"], "text": text, "evidence_refs": refs, "evidence_values": dict(row)}


def _build_observation_from_payloads(
    *,
    report: Mapping[str, Any],
    shadow: Mapping[str, Any],
    q2_rows: Mapping[str, Mapping[str, Any]],
    q2_meta: Mapping[str, Any],
    data_origin: str,
) -> dict[str, Any]:
    report = dict(report)
    shadow = dict(shadow)
    trade_date = str(report.get("trade_date") or shadow.get("trade_date") or "").strip()
    if not trade_date:
        raise ValueError("trade_date is required")
    if str(shadow.get("trade_date") or "") != trade_date:
        raise ValueError("auction report and plate shadow trade_date differ")
    shadow_mapping = _mapping_value(shadow.get("mapping_origin"))
    report_mapping = _mapping(_mapping_value(_mapping(report.get("provenance")).get("mapping_origin")))
    if report_mapping and shadow_mapping and report_mapping != shadow_mapping:
        raise ValueError("auction report and plate shadow mapping provenance differ")
    auction_symbols_by_plate = _auction_symbols_by_plate(shadow)
    mapped_symbols_by_plate = _mapped_symbols_by_plate(shadow)
    open_facts = {symbol: _open_fact(row) for symbol, row in q2_rows.items()}
    targets = _plate_targets(report)
    plate_rows: list[dict[str, Any]] = []
    for auction_row in targets:
        plate = str(auction_row.get("plate") or "")
        row = _plate_open_fact(
            auction_row,
            auction_symbols_by_plate.get(plate, set()),
            mapped_symbols_by_plate.get(plate, set()),
            open_facts,
        )
        row["mapping_provenance"] = shadow.get("mapping_origin") or "unavailable"
        plate_rows.append(row)
    market_open = [row for row in open_facts.values() if row.get("status") == "available"]
    market_changes = [_number(row.get("change_pct")) for row in market_open]
    market_changes = [value for value in market_changes if value is not None]
    market_amount, market_amount_status, market_amount_present, market_amount_total = _optional_sum(market_open, "amount_2m_yuan")
    market_limit_values = [row.get("limit_state") for row in market_open]
    market_limit_present = sum(value is not None for value in market_limit_values)
    market_limit_status = "available" if market_limit_values and market_limit_present == len(market_limit_values) else ("partial" if market_limit_present else "unavailable")
    market = {
        "status": q2_meta.get("status", "unavailable"),
        "open_valid_count": len(market_changes),
        "open_up_count": sum(value > 0 for value in market_changes),
        "open_down_count": sum(value < 0 for value in market_changes),
        "open_flat_count": sum(value == 0 for value in market_changes),
        "open_window_amount_yuan": market_amount,
        "open_window_amount_status": market_amount_status,
        "open_amount_present_count": market_amount_present,
        "open_amount_total_count": market_amount_total,
        "open_limit_up_count": sum(1 for value in market_limit_values if value == 1) if market_limit_status == "available" else None,
        "open_limit_down_count": sum(1 for value in market_limit_values if value == -1) if market_limit_status == "available" else None,
        "open_limit_state_status": market_limit_status,
        "open_limit_state_present_count": market_limit_present,
        "open_limit_state_total_count": len(market_limit_values),
        "observation_time": q2_meta.get("observation_time"),
        "source": q2_meta.get("source"),
    }
    mapping = shadow_mapping or "unavailable"
    auction_market = _auction_market_facts(report)
    result: dict[str, Any] = {
        "format": FORMAT,
        "trade_date": trade_date,
        "data_origin": data_origin,
        "historical_valid": bool(shadow.get("historical_valid")),
        "mapping_provenance": mapping,
        "mapping_consistency": "same_plate_shadow_mapping",
        "auction_source": {"source": "AuctionEmailReportV1.report_fact_view", "report_id": report.get("report_id"), "observation_time": _mapping(report.get("market_overview")).get("observation_time")},
        "open_source": q2_meta,
        "market": {"auction": auction_market, "open": market},
        "plates": plate_rows,
        "observations": [_observation(row) for row in plate_rows],
        "definitions": {
            "open_window": "latest valid Q2 observation per symbol in 09:30:00-09:32:59.999999 Asia/Shanghai",
            "open_window_amount_yuan": "sum of existing Q2 amt2m values at the latest observation; not ratio-compared with auction amount",
            "open_change_pct": "existing Q2 px/pc semantics, expressed as percent; no strategy formula",
            "concentration": "Top1/Top3 of latest open-window amt2m among valid common symbols",
        },
        "strategy_impact": "none",
        "decision_bundle": None,
    }
    result["business_sha256"] = _sha256({key: result[key] for key in ("trade_date", "market", "plates", "observations")})
    result["sha256"] = _sha256(result)
    return result


def build_observation(*, auction_report: Path, plate_shadow: Path, q2: Path, data_origin: str = "replay_fixture_only") -> dict[str, Any]:
    """Compatibility wrapper for the existing file-based replay CLI."""

    report = _parse_report(auction_report)
    shadow = _parse_shadow(plate_shadow)
    trade_date = str(report.get("trade_date") or shadow.get("trade_date") or "").strip()
    if not trade_date:
        raise ValueError("trade_date is required")
    q2_rows, q2_meta = _open_rows(q2, trade_date)
    return _build_observation_from_payloads(
        report=report,
        shadow=shadow,
        q2_rows=q2_rows,
        q2_meta=q2_meta,
        data_origin=data_origin,
    )


def build_observation_from_inputs(
    *,
    auction_evidence: Mapping[str, Any],
    plate_shadow: Mapping[str, Any],
    open_q2_rows: Iterable[Mapping[str, Any]],
    observation_cutoff: datetime | None = None,
    data_origin: str = "production_capture",
    open_q2_format: str = "auto",
) -> dict[str, Any]:
    """Build the same observation from live-compatible mappings and Q2 inputs.

    ``PlateAuctionShadowV1`` is the sole plate/symbol auction fact input.  The
    report builder is reused only to create the shared auction fact view; the
    production path never reads a report or Q2Frame file.  ``open_q2_format``
    may be ``q2frame`` for replay envelopes or ``online_rows`` for the raw or
    already-standardized rows returned by the production quote path.
    """

    from engine_next.runtime.auction_email_report import build_auction_email_report

    shadow = dict(plate_shadow)
    report = build_auction_email_report(plate_shadow=shadow, auction_evidence=dict(auction_evidence)).metadata
    trade_date = str(report.get("trade_date") or shadow.get("trade_date") or "").strip()
    if not trade_date:
        raise ValueError("trade_date is required")
    input_format = str(open_q2_format or "auto").strip().lower()
    if input_format == "auto":
        iterator = iter(open_q2_rows)
        first_row = next(iterator, None)
        rows = chain((first_row,), iterator) if first_row is not None else ()
        input_format = "q2frame" if isinstance(first_row, Mapping) and ("q2_updates" in first_row or "logical_ts_ms" in first_row) else "online_rows"
    else:
        rows = open_q2_rows
    if input_format == "q2frame":
        q2_rows, q2_meta = _open_rows_from_frames(
            rows,
            trade_date=trade_date,
            observation_cutoff=observation_cutoff,
        )
    elif input_format == "online_rows":
        q2_rows, q2_meta = _open_rows_from_online_rows(
            rows,
            trade_date=trade_date,
            observation_cutoff=observation_cutoff,
        )
    else:
        raise ValueError("open_q2_format must be auto, q2frame, or online_rows")
    q2_meta = dict(q2_meta)
    if input_format == "online_rows":
        q2_meta["source"] = "production_online_q2"
    if observation_cutoff is None:
        normalized_cutoff = None
    elif observation_cutoff.tzinfo is None:
        normalized_cutoff = observation_cutoff.replace(tzinfo=SHANGHAI)
    else:
        normalized_cutoff = observation_cutoff.astimezone(SHANGHAI)
    q2_meta["observation_cutoff"] = normalized_cutoff.isoformat() if normalized_cutoff is not None else None
    return _build_observation_from_payloads(
        report=report,
        shadow=shadow,
        q2_rows=q2_rows,
        q2_meta=q2_meta,
        data_origin=data_origin,
    )


def build_open_confirmation_observation(
    *,
    auction_evidence: Mapping[str, Any],
    plate_shadow: Mapping[str, Any],
    open_q2_rows: Iterable[Mapping[str, Any]],
    observation_cutoff: datetime | None = None,
    data_origin: str = "production_realtime",
    open_q2_format: str = "online_rows",
) -> dict[str, Any]:
    """Stable production-facing name for the existing pure transform.

    This is deliberately a thin compatibility wrapper.  Production supplies
    online Q2 rows; replay may continue calling ``build_observation_from_inputs``
    directly with an explicit ``q2frame`` format.
    """
    return build_observation_from_inputs(
        auction_evidence=auction_evidence,
        plate_shadow=plate_shadow,
        open_q2_rows=open_q2_rows,
        observation_cutoff=observation_cutoff,
        data_origin=data_origin,
        open_q2_format=open_q2_format,
    )


def _fmt_count(value: Any) -> str:
    return "unavailable" if value is None else str(value)


def render_markdown(result: Mapping[str, Any]) -> str:
    market = _mapping(result.get("market"))
    auction = _mapping(market.get("auction"))
    open_market = _mapping(market.get("open"))
    lines = [
        f"# 【09:31 开盘事实对照】{result.get('trade_date', '')}",
        f"数据来源：{result.get('data_origin', 'unavailable')}；历史有效：{result.get('historical_valid')}",
        f"映射一致性：{result.get('mapping_consistency', 'unavailable')}",
        "",
        "## 全市场变化",
        f"- 09:25 上涨/下跌/平盘：{_fmt_count(auction.get('positive_count'))} / {_fmt_count(auction.get('negative_count'))} / {_fmt_count(auction.get('flat_count'))}",
        f"- 09:25 总预撮合金额：{_fmt_yi(auction.get('auction_amount_yuan'))}",
        f"- 开盘窗口上涨/下跌/平盘：{_fmt_count(open_market.get('open_up_count'))} / {_fmt_count(open_market.get('open_down_count'))} / {_fmt_count(open_market.get('open_flat_count'))}",
        f"- 开盘窗口有效价格股票：{_fmt_count(open_market.get('open_valid_count'))}",
        f"- 开盘窗口成交口径（amt2m）：{_fmt_yi(open_market.get('open_window_amount_yuan'))}",
        "",
        "## 09:26重点板块后续表现",
        "|板块|竞价有效|开盘有效|共同股票|竞价上涨覆盖|开盘上涨覆盖|Δ|竞价中位|开盘中位|中位Δ|竞价Top1|开盘Top1|Top1Δ|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _rows(result.get("plates")):
        lines.append(
            f"|{row.get('plate')}|{_fmt_count(row.get('auction_valid_count'))}|{_fmt_count(row.get('open_valid_count'))}|{_fmt_count(row.get('common_symbol_count'))}|"
            f"{_fmt_pct(float(row['auction_positive_ratio']) * 100) if row.get('auction_positive_ratio') is not None else 'unavailable'}|"
            f"{_fmt_pct(float(row['open_positive_ratio']) * 100) if row.get('open_positive_ratio') is not None else 'unavailable'}|"
            f"{_fmt_pct(float(row['positive_ratio_delta']) * 100) if row.get('positive_ratio_delta') is not None else 'unavailable'}|"
            f"{_fmt_pct(row.get('auction_median_change_pct'))}|{_fmt_pct(row.get('open_median_change_pct'))}|"
            f"{_fmt_pct(row.get('median_change_pct_delta'))}|"
            f"{_fmt_pct(float(row['auction_top1_amount_ratio']) * 100) if row.get('auction_top1_amount_ratio') is not None else 'unavailable'}|"
            f"{_fmt_pct(float(row['open_top1_amount_ratio']) * 100) if row.get('open_top1_amount_ratio') is not None else 'unavailable'}|"
            f"{_fmt_pct(float(row['top1_amount_ratio_delta']) * 100) if row.get('top1_amount_ratio_delta') is not None else 'unavailable'}|"
        )
    lines.extend(["", "## 数据缺口", f"- 开盘事实观察时间：{open_market.get('observation_time') or 'unavailable'}", f"- 开盘来源字段：{', '.join(_mapping(result.get('open_source')).get('fields') or []) or 'unavailable'}", "- 金额比例：unavailable（竞价金额与开盘amt2m不是同一时间口径）", "", "## 变化描述"])
    lines.extend(f"- {row.get('text')}" for row in _rows(result.get("observations")) or [{"text": "unavailable"}])
    return "\n".join(lines).strip() + "\n"


def write_outputs(result: Mapping[str, Any], *, json_path: Path, markdown_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    with markdown_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(render_markdown(result))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only auction-to-open facts comparison")
    parser.add_argument("--auction-report", type=Path, required=True)
    parser.add_argument("--plate-shadow", type=Path, required=True)
    parser.add_argument("--q2", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    parser.add_argument("--data-origin", default="replay_fixture_only")
    args = parser.parse_args()
    result = build_observation(
        auction_report=args.auction_report,
        plate_shadow=args.plate_shadow,
        q2=args.q2,
        data_origin=args.data_origin,
    )
    write_outputs(result, json_path=args.output_json, markdown_path=args.output_md)
    print(json.dumps({"format": result["format"], "trade_date": result["trade_date"], "plate_count": len(result["plates"]), "business_sha256": result["business_sha256"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
