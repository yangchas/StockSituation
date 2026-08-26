"""Deterministic, read-only presentation for frozen auction facts."""

from __future__ import annotations

import hashlib
import html
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


ALLOWED_DATA_ORIGINS = {"production_realtime", "production_capture", "replay_fixture_only", "current_cache_only"}
DEFAULT_TEMPLATE = Path(__file__).resolve().parents[1] / "templates" / "auction_report.html"
CROSS_SECTION_RANK_MIN_VALID_COUNT = 10
CORE_OBSERVATION_LIMIT = 6
MAIN_PLATE_LIMIT = 10
APPENDIX_LIMIT = 10
LOCKED_ORDER_LIMIT = 10
CONTRIBUTOR_LIMIT = 3
OBSERVATION_TYPES = {
    "market_summary", "auction_amount", "price_breadth", "price_distribution", "concentration",
    "pressure", "withdrawal", "price_pressure_relation", "limit_order_concentration", "anchor_change",
}
STRATEGY_PHRASES = (
    "主线确认", "主攻确认", "强势确认", "买点确认", "卖点确认", "资金确认", "趋势确认",
    "主线升级", "强度升级", "推荐升级", "值得关注", "建议关注", "看多", "看空", "买入", "卖出",
    "主力抢筹", "主力流入", "主力净流入", "主力撤退", "主力跑路", "资金出逃", "资金认可",
    "资金净流入", "抢筹资金", "真实资金流入", "综合最强", "今日最强", "首选方向",
)


@dataclass(frozen=True)
class AuctionEmailReport:
    subject: str
    text_body: str
    html_body: str
    html_sha256: str
    metadata: dict[str, Any]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _rows(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and result not in {float("inf"), float("-inf")} else None


def _int_or_none(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _origin(value: Any) -> str:
    text = str(value or "").strip()
    if text in ALLOWED_DATA_ORIGINS:
        return text
    raise ValueError(f"unsupported auction email data_origin: {text or '<missing>'}")


def _optional_origin(value: Any) -> str:
    text = str(value or "").strip()
    return text if text in ALLOWED_DATA_ORIGINS else "unavailable"


def _money_yi(value: Any) -> str:
    number = _number(value)
    return "unavailable" if number is None else f"{number / 100_000_000.0:.2f}亿"


def _pct(value: Any, *, ratio: bool = False) -> str:
    number = _number(value)
    if number is None:
        return "unavailable"
    if ratio:
        number *= 100.0
    return f"{number:.2f}%"


def _display(value: Any) -> str:
    return "unavailable" if value is None or value == "" else str(value)


def _valid_price_display(overview: Mapping[str, Any]) -> str:
    """Do not infer A2 coverage from the three price buckets."""
    if overview.get("status") == "available" and overview.get("valid_stock_count") is None:
        return "A2 summary未提供"
    return _display(overview.get("valid_stock_count"))


def _json_load(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _iso_from_ms(value: Any) -> str | None:
    number = _number(value)
    if number is None or number <= 0:
        return None
    try:
        return datetime.fromtimestamp(number / 1000.0, tz=ZoneInfo("Asia/Shanghai")).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _report_settings() -> dict[str, Any]:
    return {
        "analysis_guardrails": {"cross_section_rank_min_valid_count": CROSS_SECTION_RANK_MIN_VALID_COUNT},
        "display_limits": {
            "core_observation_limit": CORE_OBSERVATION_LIMIT,
            "main_plate_limit": MAIN_PLATE_LIMIT,
            "appendix_limit": APPENDIX_LIMIT,
            "locked_order_limit": LOCKED_ORDER_LIMIT,
            "contributor_limit": CONTRIBUTOR_LIMIT,
        },
    }


def resolve_auction_report_status(
    component_statuses: Mapping[str, Any] | None = None,
    **status_kwargs: Any,
) -> str:
    """Resolve Auction report availability from factual component statuses.

    ``mapping`` is provenance/readiness metadata, not an independent factual
    domain.  A2 market facts therefore remain reportable when only the frozen
    mapping is unavailable; plate facts are the component that degrades.
    """
    statuses = dict(component_statuses or {})
    statuses.update(status_kwargs)
    market_status = str(statuses.get("market_overview", statuses.get("market_overview_status")) or "unavailable").lower()
    plate_status = str(statuses.get("plate_facts", statuses.get("plate_facts_status")) or "unavailable").lower()
    factual = (market_status, plate_status)
    if all(status == "available" for status in factual):
        return "COMPLETE"
    if any(status in {"available", "partial"} for status in factual):
        return "PARTIAL"
    return "DATA_UNAVAILABLE"


def _market_summary_from_payload(payload: Mapping[str, Any], trade_date: str) -> dict[str, Any] | None:
    """Read an explicitly supplied A2 summary; never infer one from plate rows."""
    date_tag = trade_date.replace("-", "")
    expected_key = f"market:auction:{date_tag}:0925"
    direct = _mapping(payload.get("market_summary"))
    payload_trade_date = str(payload.get("trade_date") or direct.get("trade_date") or "").strip()
    if payload_trade_date and payload_trade_date != trade_date:
        return None
    if direct:
        if str(direct.get("source") or "") in {"q2_observation", "a2_0925_summary"}:
            return dict(direct)
        if direct.get("format") == "AuctionMarketSummaryV1":
            row = _mapping(_mapping(direct.get("anchors")).get("0925"))
            if row:
                return {
                    **row,
                    "auction_amount_yuan": row.get("auction_amount_yuan", row.get("total_auction_amount_yuan")),
                    "limit_up_seal_amount_yuan": row.get("limit_up_seal_amount_yuan", row.get("total_limit_up_bid_amount_yuan")),
                    "status": "available", "source": "a2_0925_summary",
                    "source_table": direct.get("source_table") or "unavailable",
                    "observation_time": direct.get("observation_time") or "unavailable",
                }
    if payload.get("format") == "AuctionMarketSummaryV1":
        row = _mapping(_mapping(payload.get("anchors")).get("0925"))
        if row:
            return {
                **row,
                "auction_amount_yuan": row.get("auction_amount_yuan", row.get("total_auction_amount_yuan")),
                "limit_up_seal_amount_yuan": row.get("limit_up_seal_amount_yuan", row.get("total_limit_up_bid_amount_yuan")),
                "status": "available", "source": "a2_0925_summary",
                "source_table": payload.get("source_table") or "unavailable",
                "observation_time": payload.get("observation_time") or "unavailable",
            }
    facts = payload.get("facts")
    if isinstance(facts, list):
        for item in facts:
            record = _mapping(item)
            if str(record.get("key") or "") != expected_key:
                continue
            value = _mapping(record.get("value"))
            summary = _json_load(value.get("summary"))
            if not summary:
                continue
            timestamp = _json_load(value.get("meta")).get("ts") or summary.get("ts")
            meta = _json_load(value.get("meta"))
            return {
                "status": "available", "source": "a2_0925_summary",
                "source_table": summary.get("source_table") or value.get("source_table") or meta.get("source_table") or record.get("source_table") or "unavailable",
                "observation_time": _iso_from_ms(timestamp) or "unavailable",
                "stock_count": summary.get("total_stocks"),
                "valid_stock_count": summary.get("valid_stock_count"),
                "unavailable_stock_count": summary.get("unavailable_stock_count"),
                "positive_count": summary.get("high_open_count"),
                "negative_count": summary.get("low_open_count"),
                "flat_count": summary.get("flat_open_count"),
                "auction_amount_yuan": summary.get("total_auction_amount_yuan"),
                "limit_up_count": summary.get("limit_up_count"),
                "limit_down_count": summary.get("limit_down_count"),
                "limit_up_seal_amount_yuan": summary.get("total_limit_up_bid_amount_yuan"),
            }
    return None


def _market_overview(shadow: Mapping[str, Any], evidence: Mapping[str, Any], context: Mapping[str, Any], trade_date: str) -> dict[str, Any]:
    payloads = (context, shadow, evidence)
    for payload in payloads:
        summary = _market_summary_from_payload(payload, trade_date)
        if summary and summary.get("source") != "q2_observation":
            return summary
    for payload in payloads:
        summary = _market_summary_from_payload(payload, trade_date)
        if summary and summary.get("source") == "q2_observation":
            return summary
    return {"status": "unavailable", "source": "unavailable", "observation_time": "unavailable"}


def _symbol_detail_rows(shadow: Mapping[str, Any]) -> list[dict[str, Any]]:
    transition = _mapping(_mapping(shadow.get("symbol_details")).get("0924_to_0925"))
    return _rows(transition.get("detail_rows"))


def _plate_rows(shadow: Mapping[str, Any]) -> list[dict[str, Any]]:
    stats_map = _mapping(_mapping(shadow.get("plate_stats")).get("0924_to_0925"))
    details = _symbol_detail_rows(shadow)
    result: list[dict[str, Any]] = []
    for plate, raw in stats_map.items():
        stats = _mapping(raw)
        distribution = _mapping(stats.get("change_pct_distribution"))
        valid = int(stats.get("valid_auction_stock_count") or distribution.get("count") or 0)
        up = int(distribution.get("positive_count") or 0)
        down = int(distribution.get("negative_count") or 0)
        flat = int(distribution.get("zero_count") or 0)
        amount = _number(stats.get("auction_amount_total_yuan")) or 0.0
        contributors = [row for row in details if str(row.get("plate") or "") == str(plate) and row.get("price_status") not in {"unavailable", "invalid"} and _number(row.get("auction_amount_yuan")) is not None]
        contributors.sort(key=lambda row: (-float(row.get("auction_amount_yuan") or 0.0), str(row.get("symbol") or "")))
        top1 = float(contributors[0].get("auction_amount_yuan") or 0.0) / amount if contributors and amount > 0 else None
        if top1 is not None and top1 > 1.0:
            top1 = None
        unavailable = stats.get("evidence_unavailable_stock_count")
        usable = int(stats.get("evidence_usable_stock_count") or 0)
        pressure = _number(stats.get("pressure_yuan"))
        if pressure is None or unavailable is None:
            pressure_status = "unavailable"
        elif int(unavailable or 0) == 0 and usable > 0:
            pressure_status = "available"
        elif usable > 0:
            pressure_status = "partial"
        else:
            pressure_status = "unavailable"
        result.append({
            "plate": str(plate), "stock_count": int(stats.get("stock_count") or 0), "valid_price_count": valid,
            "unavailable_price_count": int(stats.get("unavailable_auction_stock_count") or 0),
            "up_count": up, "down_count": down, "flat_count": flat,
            "positive_ratio": up / valid if valid else None, "negative_ratio": down / valid if valid else None,
            "median_change_pct": _number(distribution.get("median_pct")), "auction_amount_yuan": amount,
            "top1_amount_ratio": top1, "top3_amount_ratio": _number(stats.get("top3_amount_concentration")),
            "pressure_yuan": pressure if pressure_status != "unavailable" else None, "pressure_status": pressure_status,
            "withdrawal_yuan": _number(stats.get("withdrawal_yuan")) if pressure_status != "unavailable" else None,
            "withdrawal_status": pressure_status,
            # ``None`` means the source did not provide the fact.  Preserve it
            # so the renderer cannot turn unknown limit counts into a factual
            # zero; explicit numeric zero remains zero.
            "limit_up_count": _int_or_none(stats.get("limit_up_count")), "limit_down_count": _int_or_none(stats.get("limit_down_count")),
            "limit_up_seal_amount_yuan": _number(stats.get("limit_up_seal_amount_yuan")),
            "limit_down_sell_pressure_yuan": _number(stats.get("limit_down_sell_pressure_yuan")),
            "multi_theme_conflict_count": int(stats.get("multi_theme_conflict_count") or 0),
            "contributors": [{
                "symbol": str(row.get("symbol") or "unavailable"), "name": str(row.get("name") or "").strip(),
                "auction_amount_yuan": _number(row.get("auction_amount_yuan")), "change_pct": _number(row.get("change_pct")),
                "amount_share": float(row.get("auction_amount_yuan") or 0.0) / amount if amount > 0 else None,
            } for row in contributors[:CONTRIBUTOR_LIMIT]],
            "quality": "complete" if valid and int(stats.get("unavailable_auction_stock_count") or 0) == 0 else ("partial" if valid else "unavailable"),
        })
    result.sort(key=lambda row: (-float(row["auction_amount_yuan"]), row["plate"]))
    for rank, row in enumerate(result, start=1):
        row["amount_rank"] = rank
    return result


def _locked_order_rows(shadow: Mapping[str, Any]) -> list[dict[str, Any]]:
    locked = _mapping(_mapping(shadow.get("automatic_analysis")).get("auction_locked_orders"))
    rows: list[dict[str, Any]] = []
    for direction in ("limit_up", "limit_down"):
        source_rows = locked.get(direction)
        for raw in _rows(source_rows):
            row = _mapping(raw)
            rows.append({
                "direction": direction, "symbol": str(row.get("symbol") or "unavailable"),
                "name": str(row.get("name") or "").strip(), "plate": str(row.get("plate") or "unavailable"),
                "change_pct": _number(row.get("change_pct")), "seal_amount_yuan": _number(row.get("anchor_locked_amount_yuan")),
                "status": str(row.get("status") or "unavailable"),
            })
    rows.sort(key=lambda row: (row.get("seal_amount_yuan") is None, -(row.get("seal_amount_yuan") or 0.0), row["direction"], row["symbol"]))
    return rows


def _locked_detail_status(source: Mapping[str, Any], direction: str) -> str:
    """Return completeness of one locked-order direction from source evidence."""
    values = source.get(direction)
    if not isinstance(values, list):
        return "unavailable"
    if any(
        not isinstance(row, Mapping)
        or str(row.get("status") or "").lower() not in {"available", "complete"}
        for row in values
    ):
        return "unavailable"
    marker = source.get(f"{direction}_complete")
    if marker is True:
        return "available"
    marker = source.get(f"{direction}_status")
    if str(marker or "").lower() in {"available", "complete"}:
        return "available"
    completeness = source.get("completeness")
    if isinstance(completeness, Mapping):
        marker = completeness.get(direction)
        if str(marker or "").lower() in {"available", "complete"}:
            return "available"
    unavailable = source.get("unavailable")
    if isinstance(unavailable, list):
        for row in unavailable:
            if not isinstance(row, Mapping):
                return "unavailable"
            row_direction = str(row.get("direction") or "").strip()
            if not row_direction or row_direction == direction:
                return "unavailable"
        return "available"
    return "unavailable"


def _integer_yuan(value: Any) -> int | None:
    """Convert an explicitly integral yuan value without tolerance/rounding."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not number.is_finite() or number != number.to_integral_value():
        return None
    return int(number)


def _locked_summary(
    rows: list[Mapping[str, Any]],
    overview: Mapping[str, Any],
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Keep missing locked-order facts distinct from explicit zero values."""
    source = source or {}
    up = [row for row in rows if row.get("direction") == "limit_up"]
    down = [row for row in rows if row.get("direction") == "limit_down"]
    up_status = _locked_detail_status(source, "limit_up")
    down_status = _locked_detail_status(source, "limit_down")
    known_up_amounts = [float(row["seal_amount_yuan"]) for row in up if _number(row.get("seal_amount_yuan")) is not None]
    amounts_complete = up_status == "available" and len(known_up_amounts) == len(up)
    total = sum(known_up_amounts) if amounts_complete else None
    overview_up_count = overview.get("limit_up_count")
    overview_down_count = overview.get("limit_down_count")
    overview_total = overview.get("limit_up_seal_amount_yuan")
    overview_up_count_int = _integer_yuan(overview_up_count)
    overview_down_count_int = _integer_yuan(overview_down_count)
    up_count = overview_up_count_int if overview_up_count is not None else (len(up) if up_status == "available" else None)
    down_count = overview_down_count_int if overview_down_count is not None else (len(down) if down_status == "available" else None)
    seal_total = _number(overview_total) if overview_total is not None else total
    conflict = False
    if up_status == "available" and overview_up_count_int is not None and overview_up_count_int != len(up):
        conflict = True
    if down_status == "available" and overview_down_count_int is not None and overview_down_count_int != len(down):
        conflict = True
    detail_amounts = [_integer_yuan(row.get("seal_amount_yuan")) for row in up]
    overview_amount = _integer_yuan(overview_total)
    if up_status == "available" and all(value is not None for value in detail_amounts) and overview_amount is not None:
        if sum(value for value in detail_amounts if value is not None) != overview_amount:
            conflict = True
    if conflict:
        return {
            "locked_order_status": "conflict",
            "limit_up_count": None,
            "limit_down_count": None,
            "limit_up_seal_amount_yuan": None,
            "top1_ratio": None,
            "top3_ratio": None,
        }
    locked_status = "available" if up_status == "available" and down_status == "available" else "unavailable"
    if locked_status == "unavailable":
        return {
            "locked_order_status": "unavailable",
            "limit_up_count": None,
            "limit_down_count": None,
            "limit_up_seal_amount_yuan": None,
            "top1_ratio": None,
            "top3_ratio": None,
        }
    return {
        "locked_order_status": locked_status,
        "limit_up_count": up_count,
        "limit_down_count": down_count,
        "limit_up_seal_amount_yuan": seal_total,
        "top1_ratio": known_up_amounts[0] / total if amounts_complete and known_up_amounts and total and total > 0 else None,
        "top3_ratio": sum(known_up_amounts[:3]) / total if amounts_complete and known_up_amounts and total and total > 0 else None,
    }


def _anchor_changes(evidence: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(evidence.get("anchor_changes"))
    rows = _rows(payload.get("rows"))
    return {"status": "complete" if payload.get("status") == "complete" and rows else "unavailable", "rows": rows}


def _market_context_summary(context: Mapping[str, Any], trade_date: str) -> dict[str, Any]:
    origin = _optional_origin(context.get("data_origin"))
    return {
        "status": "available" if origin != "unavailable" else "unavailable", "data_origin": origin,
        "source_trade_date": context.get("source_trade_date") or context.get("previous_trade_date"),
        "capture_time": context.get("capture_time"), "contract_version": context.get("contract_version"),
    }


def _ratio(value: Any) -> str:
    return _pct(value, ratio=True)


def _core_observations(plates: list[Mapping[str, Any]], overview: Mapping[str, Any], locked: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    def add(kind: str, text: str, refs: list[str], values: Mapping[str, Any], key: str) -> None:
        result.append({"observation_type": kind, "text": text, "evidence_refs": refs, "evidence_values": dict(values), "key": key})

    if overview.get("status") == "available":
        add("market_summary", f"全市场上涨/下跌/平盘={overview.get('positive_count', 'unavailable')}/{overview.get('negative_count', 'unavailable')}/{overview.get('flat_count', 'unavailable')}，预撮合金额{_money_yi(overview.get('auction_amount_yuan'))}。", ["market_summary.positive_count", "market_summary.negative_count", "market_summary.flat_count", "market_summary.auction_amount_yuan"], overview, "market_summary")
    if plates:
        row = plates[0]
        add("auction_amount", f"{row['plate']}竞价金额{_money_yi(row['auction_amount_yuan'])}，为已统计板块金额首位。", [f"plate_stats.0924_to_0925.{row['plate']}.auction_amount_total_yuan"], row, f"amount:{row['plate']}")
    eligible = [row for row in plates if int(row.get("valid_price_count") or 0) >= CROSS_SECTION_RANK_MIN_VALID_COUNT]
    positive = max(eligible, key=lambda row: (float(row.get("positive_ratio") or 0.0), float(row.get("auction_amount_yuan") or 0.0), str(row.get("plate"))), default=None)
    negative = max(eligible, key=lambda row: (float(row.get("negative_ratio") or 0.0), float(row.get("auction_amount_yuan") or 0.0), str(row.get("plate"))), default=None)
    if positive:
        add("price_breadth", f"{positive['plate']}有效价格股票{positive['valid_price_count']}只，上涨{positive['up_count']}只，上涨覆盖{_ratio(positive['positive_ratio'])}，中位涨幅{_pct(positive['median_change_pct'])}。", [f"plate_stats.0924_to_0925.{positive['plate']}.change_pct_distribution"], positive, f"positive:{positive['plate']}")
    if negative:
        add("price_breadth", f"{negative['plate']}有效价格股票{negative['valid_price_count']}只，下跌{negative['down_count']}只，下跌覆盖{_ratio(negative['negative_ratio'])}，中位涨幅{_pct(negative['median_change_pct'])}。", [f"plate_stats.0924_to_0925.{negative['plate']}.change_pct_distribution"], negative, f"negative:{negative['plate']}")
    concentrated = max((row for row in eligible if row.get("top1_amount_ratio") is not None), key=lambda row: (float(row.get("top1_amount_ratio") or 0.0), str(row.get("plate"))), default=None)
    if concentrated:
        add("concentration", f"{concentrated['plate']} Top1金额占板块{_ratio(concentrated['top1_amount_ratio'])}，Top3占{_ratio(concentrated['top3_amount_ratio'])}。", [f"plate_stats.0924_to_0925.{concentrated['plate']}.auction_amount_total_yuan", f"symbol_details.0924_to_0925.{concentrated['plate']}.top1_amount_ratio"], concentrated, f"concentration:{concentrated['plate']}")
    if locked.get("top1_ratio") is not None:
        add("limit_order_concentration", f"竞价涨停封单Top1占总涨停封单{_ratio(locked['top1_ratio'])}，Top3占{_ratio(locked['top3_ratio'])}。", ["auction_locked_orders.limit_up", "auction_locked_orders.top1_ratio", "auction_locked_orders.top3_ratio"], locked, "seal_concentration")

    relations = []
    for row in plates:
        if row.get("pressure_status") != "available" or row.get("pressure_yuan") in (None, 0):
            continue
        price_positive = float(row.get("positive_ratio") or 0.0) > float(row.get("negative_ratio") or 0.0) and float(row.get("median_change_pct") or 0.0) > 0
        price_negative = float(row.get("negative_ratio") or 0.0) > float(row.get("positive_ratio") or 0.0) and float(row.get("median_change_pct") or 0.0) < 0
        pressure = float(row.get("pressure_yuan") or 0.0)
        if (price_positive and pressure < 0) or (price_negative and pressure > 0):
            relations.append(row)
    relation = max(relations, key=lambda row: (abs(float(row.get("pressure_yuan") or 0.0)), str(row.get("plate"))), default=None)
    if relation and not any(relation["plate"] in str(item.get("key")) for item in result):
        add("price_pressure_relation", f"{relation['plate']}价格分布方向与pressure方向相反：中位涨幅{_pct(relation['median_change_pct'])}，pressure {_money_yi(relation['pressure_yuan'])}。", [f"plate_stats.0924_to_0925.{relation['plate']}.change_pct_distribution", f"plate_stats.0924_to_0925.{relation['plate']}.pressure_yuan"], relation, f"relation:{relation['plate']}")
    return result


def _appendix(plates: list[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    eligible = [row for row in plates if int(row.get("valid_price_count") or 0) >= CROSS_SECTION_RANK_MIN_VALID_COUNT]
    relations = []
    for row in plates:
        if row.get("pressure_status") != "available" or row.get("pressure_yuan") in (None, 0):
            continue
        price_positive = float(row.get("positive_ratio") or 0.0) > float(row.get("negative_ratio") or 0.0) and float(row.get("median_change_pct") or 0.0) > 0
        price_negative = float(row.get("negative_ratio") or 0.0) > float(row.get("positive_ratio") or 0.0) and float(row.get("median_change_pct") or 0.0) < 0
        pressure = float(row.get("pressure_yuan") or 0.0)
        if (price_positive and pressure < 0) or (price_negative and pressure > 0):
            relations.append(row)
    relations.sort(key=lambda row: (-abs(float(row.get("pressure_yuan") or 0.0)), str(row.get("plate"))))

    def distinct_rows(rows: list[Mapping[str, Any]], metric: str, *, nonzero: bool = False) -> list[dict[str, Any]]:
        usable = [dict(row) for row in rows if _number(row.get(metric)) is not None]
        values = [float(row[metric]) for row in usable]
        if not usable or len(set(values)) <= 1 or (nonzero and not any(value != 0 for value in values)):
            return []
        return usable[:APPENDIX_LIMIT]

    def project(rows: list[Mapping[str, Any]], name: str) -> list[dict[str, Any]]:
        fields = {
            "上涨覆盖率TopN": ("plate", "valid_price_count", "positive_ratio", "median_change_pct"),
            "下跌覆盖率TopN": ("plate", "valid_price_count", "negative_ratio", "median_change_pct"),
            "Top1集中度TopN": ("plate", "valid_price_count", "top1_amount_ratio", "top3_amount_ratio"),
            "pressure绝对值TopN": ("plate", "pressure_yuan", "positive_ratio", "negative_ratio"),
            "withdrawal TopN": ("plate", "withdrawal_yuan"),
            "price/pressure关系": ("plate", "valid_price_count", "median_change_pct", "up_count", "down_count", "pressure_yuan"),
        }[name]
        return [{field: row.get(field) for field in fields} for row in rows]

    up_rows = project(distinct_rows(sorted(eligible, key=lambda row: (-float(row.get("positive_ratio") or 0.0), -float(row.get("auction_amount_yuan") or 0.0), str(row.get("plate")))), "positive_ratio"), "上涨覆盖率TopN")
    down_rows = project(distinct_rows(sorted(eligible, key=lambda row: (-float(row.get("negative_ratio") or 0.0), -float(row.get("auction_amount_yuan") or 0.0), str(row.get("plate")))), "negative_ratio"), "下跌覆盖率TopN")
    concentration_rows = project(distinct_rows(sorted((row for row in plates if row.get("top1_amount_ratio") is not None), key=lambda row: (-float(row.get("top1_amount_ratio") or 0.0), str(row.get("plate")))), "top1_amount_ratio"), "Top1集中度TopN")
    pressure_rows = project(distinct_rows(sorted((row for row in plates if row.get("pressure_status") != "unavailable" and row.get("pressure_yuan") is not None), key=lambda row: (-abs(float(row.get("pressure_yuan") or 0.0)), str(row.get("plate")))), "pressure_yuan", nonzero=True), "pressure绝对值TopN")
    withdrawal_rows = project(distinct_rows(sorted((row for row in plates if row.get("withdrawal_status") != "unavailable" and row.get("withdrawal_yuan") is not None), key=lambda row: (-float(row.get("withdrawal_yuan") or 0.0), str(row.get("plate")))), "withdrawal_yuan", nonzero=True), "withdrawal TopN")
    return {
        "上涨覆盖率TopN": up_rows,
        "下跌覆盖率TopN": down_rows,
        "Top1集中度TopN": concentration_rows,
        "pressure绝对值TopN": pressure_rows,
        "withdrawal TopN": withdrawal_rows,
        "price/pressure关系": project(relations[:APPENDIX_LIMIT], "price/pressure关系"),
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _provenance_display(provenance: Mapping[str, Any]) -> dict[str, str]:
    """Compact human-readable provenance; raw provenance remains untouched."""
    labels = {
        "data_origin": "数据来源",
        "market_summary_source": "市场概览来源",
        "market_summary_source_table": "市场概览数据表",
        "market_summary_observation_time": "市场概览时间",
        "plate_observation_source": "板块观察来源",
        "plate_observation_time": "板块观察时间",
        "anchor_source": "三锚点来源",
        "auction_evidence_origin": "竞价证据来源",
        "mapping_origin": "板块映射",
        "historical_valid": "历史有效",
        "market_context_origin": "市场背景来源",
    }
    display: dict[str, str] = {}
    for key, value in provenance.items():
        if key == "mapping_origin":
            mapping = _json_load(value)
            if mapping:
                value = f"{mapping.get('canonical', 'unavailable')} ({mapping.get('status', 'unavailable')})"
        display[labels.get(key, key)] = str(value)
    return display


def _appendix_html_rows(name: str, rows: list[Mapping[str, Any]]) -> str:
    cells: dict[str, str] = {
        "上涨覆盖率TopN": "<th>板块</th><th>有效价格</th><th>上涨覆盖</th><th>中位涨幅</th>",
        "下跌覆盖率TopN": "<th>板块</th><th>有效价格</th><th>下跌覆盖</th><th>中位涨幅</th>",
        "Top1集中度TopN": "<th>板块</th><th>有效价格</th><th>Top1</th><th>Top3</th>",
        "pressure绝对值TopN": "<th>板块</th><th>pressure</th><th>上涨覆盖</th><th>下跌覆盖</th>",
        "withdrawal TopN": "<th>板块</th><th>withdrawal</th>",
        "price/pressure关系": "<th>板块</th><th>有效价格</th><th>中位涨幅</th><th>涨/跌</th><th>pressure</th>",
    }
    body: list[str] = []
    for row in rows:
        if name == "上涨覆盖率TopN":
            values = [row.get("plate"), row.get("valid_price_count"), _ratio(row.get("positive_ratio")), _pct(row.get("median_change_pct"))]
        elif name == "下跌覆盖率TopN":
            values = [row.get("plate"), row.get("valid_price_count"), _ratio(row.get("negative_ratio")), _pct(row.get("median_change_pct"))]
        elif name == "Top1集中度TopN":
            values = [row.get("plate"), row.get("valid_price_count"), _ratio(row.get("top1_amount_ratio")), _ratio(row.get("top3_amount_ratio"))]
        elif name == "pressure绝对值TopN":
            values = [row.get("plate"), _money_yi(row.get("pressure_yuan")), _ratio(row.get("positive_ratio")), _ratio(row.get("negative_ratio"))]
        elif name == "withdrawal TopN":
            values = [row.get("plate"), _money_yi(row.get("withdrawal_yuan"))]
        else:
            values = [row.get("plate"), row.get("valid_price_count"), _pct(row.get("median_change_pct")), f"{row.get('up_count', 'unavailable')}/{row.get('down_count', 'unavailable')}", _money_yi(row.get("pressure_yuan"))]
        body.append("<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in values) + "</tr>")
    return f"<h4>{html.escape(name)}</h4><table><thead><tr>{cells.get(name, '<th>板块</th>')}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _appendix_text_row(name: str, row: Mapping[str, Any]) -> str:
    if name == "上涨覆盖率TopN":
        return f"{row.get('plate')} | 有效价格={row.get('valid_price_count')} | 上涨覆盖={_ratio(row.get('positive_ratio'))} | 中位涨幅={_pct(row.get('median_change_pct'))}"
    if name == "下跌覆盖率TopN":
        return f"{row.get('plate')} | 有效价格={row.get('valid_price_count')} | 下跌覆盖={_ratio(row.get('negative_ratio'))} | 中位涨幅={_pct(row.get('median_change_pct'))}"
    if name == "Top1集中度TopN":
        return f"{row.get('plate')} | 有效价格={row.get('valid_price_count')} | Top1={_ratio(row.get('top1_amount_ratio'))} | Top3={_ratio(row.get('top3_amount_ratio'))}"
    if name == "pressure绝对值TopN":
        return f"{row.get('plate')} | pressure={_money_yi(row.get('pressure_yuan'))} | 上涨覆盖={_ratio(row.get('positive_ratio'))} | 下跌覆盖={_ratio(row.get('negative_ratio'))}"
    if name == "withdrawal TopN":
        return f"{row.get('plate')} | withdrawal={_money_yi(row.get('withdrawal_yuan'))}"
    return f"{row.get('plate')} | 有效价格={row.get('valid_price_count')} | 中位涨幅={_pct(row.get('median_change_pct'))} | 涨/跌={row.get('up_count', 'unavailable')}/{row.get('down_count', 'unavailable')} | pressure={_money_yi(row.get('pressure_yuan'))}"


def _render_html(template: str, report: Mapping[str, Any]) -> str:
    overview, locked = report["market_overview"], report["locked_summary"]
    plate_rows = []
    for row in report["plate_rows"][:MAIN_PLATE_LIMIT]:
        contributors = "<br>".join(f"{html.escape(item['symbol'])} {_money_yi(item['auction_amount_yuan'])} {_pct(item['change_pct'])} ({_ratio(item['amount_share'])})" for item in row["contributors"]) or "unavailable"
        plate_rows.append("<tr>" + f"<td>{html.escape(row['plate'])}</td><td>{row['stock_count']}</td><td>{row['valid_price_count']}</td><td>{row['up_count']}/{row['down_count']}/{row['flat_count']}</td><td>{_ratio(row['positive_ratio'])}</td><td>{_ratio(row['negative_ratio'])}</td><td>{_pct(row['median_change_pct'])}</td><td>{_money_yi(row['auction_amount_yuan'])}</td><td>{_ratio(row['top1_amount_ratio'])}</td><td>{_ratio(row['top3_amount_ratio'])}</td><td>{_money_yi(row['pressure_yuan']) if row['pressure_yuan'] is not None else 'unavailable'} ({html.escape(row['pressure_status'])})</td><td>{_money_yi(row['withdrawal_yuan']) if row['withdrawal_yuan'] is not None else 'unavailable'}</td><td>{contributors}</td>" + "</tr>")
    locked_rows = ["<tr>" + f"<td>{html.escape(row['symbol'])}</td><td>{html.escape(row['plate'])}</td><td>{'涨停' if row['direction'] == 'limit_up' else '跌停'}</td><td>{_money_yi(row['seal_amount_yuan'])}</td><td>{_pct(row['change_pct'])}</td>" + "</tr>" for row in report["locked_order_rows"][:LOCKED_ORDER_LIMIT]]
    appendix = []
    for name, rows in report["appendix"].items():
        if rows:
            appendix.append(_appendix_html_rows(name, rows))
    replacements = {
        "{{SUBJECT}}": html.escape(report["subject"]), "{{TRADE_DATE}}": html.escape(report["trade_date"]), "{{DATA_ORIGIN}}": html.escape(report["data_origin"]),
        "{{REPORT_STATUS_LINE}}": html.escape(
            "" if report.get("report_status") == "COMPLETE" else (
                f"报告状态={report.get('report_status', 'DATA_UNAVAILABLE')} · "
                f"事实域：market={report.get('component_statuses', {}).get('market_overview', 'unavailable')} · "
                f"plate={report.get('component_statuses', {}).get('plate_facts', 'unavailable')} · "
                f"mapping={report.get('component_statuses', {}).get('mapping', 'unavailable')} · "
                f"不可用原因={'；'.join(str(item) for item in report.get('unavailable_reasons', [])) or '无'}"
            )
        ),
        "{{MARKET_STATUS}}": html.escape(str(overview.get("status", "unavailable"))), "{{MARKET_SOURCE}}": html.escape(str(overview.get("source", "unavailable"))), "{{MARKET_OBSERVATION_TIME}}": html.escape(str(overview.get("observation_time", "unavailable"))),
        "{{MARKET_FACT_STATUS}}": html.escape(str(report.get("component_statuses", {}).get("market_overview", "unavailable"))),
        "{{PLATE_FACT_STATUS}}": html.escape(str(report.get("component_statuses", {}).get("plate_facts", "unavailable"))),
        "{{MAPPING_STATUS}}": html.escape(str(report.get("component_statuses", {}).get("mapping", "unavailable"))),
        "{{UNAVAILABLE_REASONS}}": html.escape("；".join(str(item) for item in report.get("unavailable_reasons", [])) or "无"),
        "{{MARKET_STOCK_COUNT}}": html.escape(_display(overview.get("stock_count"))), "{{MARKET_VALID_COUNT}}": html.escape(_valid_price_display(overview)),
        "{{MARKET_RISE_FALL_FLAT}}": html.escape(f"{_display(overview.get('positive_count'))} / {_display(overview.get('negative_count'))} / {_display(overview.get('flat_count'))}"), "{{MARKET_AMOUNT}}": _money_yi(overview.get("auction_amount_yuan")),
        "{{MARKET_LIMITS}}": html.escape(f"{_display(overview.get('limit_up_count'))} / {_display(overview.get('limit_down_count'))}"), "{{MARKET_SEAL}}": _money_yi(overview.get("limit_up_seal_amount_yuan")),
        "{{CORE_OBSERVATIONS}}": "".join(f"<li>{html.escape(item['text'])}</li>" for item in report.get("core_observations", report["observations"][:CORE_OBSERVATION_LIMIT])) or "<li>unavailable</li>",
        "{{PLATE_ROWS}}": "".join(plate_rows) or "<tr><td colspan='13'>unavailable</td></tr>", "{{LOCKED_STATUS}}": html.escape(str(locked.get("locked_order_status", "unavailable"))), "{{LOCKED_COUNTS}}": html.escape(f"{_display(locked.get('limit_up_count'))} / {_display(locked.get('limit_down_count'))}"),
        "{{LOCKED_TOTAL}}": _money_yi(locked.get("limit_up_seal_amount_yuan")), "{{LOCKED_TOP}}": html.escape(f"Top1 {_ratio(locked.get('top1_ratio'))} · Top3 {_ratio(locked.get('top3_ratio'))}"),
        "{{LOCKED_ROWS}}": "".join(locked_rows) or "<tr><td colspan='5'>unavailable</td></tr>", "{{ANCHOR_STATUS}}": html.escape(str(report["anchor_changes"].get("status", "unavailable"))),
        "{{ANCHOR_ROWS}}": "".join(f"<li>{html.escape(str(row))}</li>" for row in report["anchor_changes"].get("rows", [])) or "<li>unavailable</li>", "{{APPENDIX}}": "".join(appendix) or "<p>unavailable</p>",
        "{{PROVENANCE_ROWS}}": "".join(f"<tr><td>{html.escape(str(key))}</td><td>{html.escape(str(value))}</td></tr>" for key, value in report.get("provenance_display", report["provenance"]).items()),
    }
    rendered = template
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered.replace("\r\n", "\n")


def _render_text(report: Mapping[str, Any]) -> str:
    overview, locked = report["market_overview"], report["locked_summary"]
    statuses = report.get("component_statuses", {})
    reasons = "；".join(str(item) for item in report.get("unavailable_reasons", [])) or "无"
    lines = [f"# {report['subject']}", f"数据状态：{_display(overview.get('status'))}；来源：{_display(overview.get('source'))}；观察时间：{_display(overview.get('observation_time'))}"]
    if report.get("report_status") != "COMPLETE":
        lines.extend([f"报告状态：{report.get('report_status', 'DATA_UNAVAILABLE')}", f"事实域：市场概览={statuses.get('market_overview', 'unavailable')}；板块聚合={statuses.get('plate_facts', 'unavailable')}；当日映射={statuses.get('mapping', 'unavailable')}", f"不可用原因：{reasons}"])
    lines.extend(["", "## 09:25市场概览", f"- 有效价格股票：{_valid_price_display(overview)}", f"- 上涨/下跌/平盘：{_display(overview.get('positive_count'))} / {_display(overview.get('negative_count'))} / {_display(overview.get('flat_count'))}", f"- 总预撮合金额：{_money_yi(overview.get('auction_amount_yuan'))}", f"- 涨停/跌停：{_display(overview.get('limit_up_count'))} / {_display(overview.get('limit_down_count'))}", f"- 涨停封单：{_money_yi(overview.get('limit_up_seal_amount_yuan'))}", "", "## 核心事实观察"])
    lines.extend(f"- {item['text']}" for item in report.get("core_observations", report["observations"][:CORE_OBSERVATION_LIMIT]))
    lines.extend(["", "## 重点板块事实Top10", "", "|板块|有效|涨/跌/平|上涨覆盖|下跌覆盖|中位涨幅|金额|Top1|Top3|pressure|withdrawal|Top3贡献|", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"])
    for row in report["plate_rows"][:MAIN_PLATE_LIMIT]:
        contributors = ", ".join(f"{item['symbol']} {_ratio(item['amount_share'])}" for item in row["contributors"]) or "unavailable"
        lines.append(f"|{row['plate']}|{row['valid_price_count']}|{row['up_count']}/{row['down_count']}/{row['flat_count']}|{_ratio(row['positive_ratio'])}|{_ratio(row['negative_ratio'])}|{_pct(row['median_change_pct'])}|{_money_yi(row['auction_amount_yuan'])}|{_ratio(row['top1_amount_ratio'])}|{_ratio(row['top3_amount_ratio'])}|{_money_yi(row['pressure_yuan']) if row['pressure_yuan'] is not None else 'unavailable'}|{_money_yi(row['withdrawal_yuan']) if row['withdrawal_yuan'] is not None else 'unavailable'}|{contributors}|")
    lines.extend(["", "## 涨跌停封单", f"- 状态：{_display(locked.get('locked_order_status'))}", f"- 涨停/跌停：{_display(locked.get('limit_up_count'))} / {_display(locked.get('limit_down_count'))}", f"- 涨停封单总额：{_money_yi(locked.get('limit_up_seal_amount_yuan'))}；Top1/Top3：{_ratio(locked.get('top1_ratio'))} / {_ratio(locked.get('top3_ratio'))}", "", "## 三锚点变化", f"- 状态：{report['anchor_changes'].get('status', 'unavailable')}"])
    lines.extend(f"- {row}" for row in report["anchor_changes"].get("rows", []))
    lines.extend(["", "## 附录：其他客观排序", ""])
    for name, rows in report["appendix"].items():
        if rows:
            lines.append(f"### {name}")
            lines.extend(f"- {_appendix_text_row(name, row)}" for row in rows)
    lines.extend(["", "## Provenance"])
    lines.extend(f"- {key}: {value}" for key, value in report.get("provenance_display", report["provenance"]).items())
    return "\n".join(lines).strip() + "\n"


def build_auction_email_report(*, plate_shadow: Mapping[str, Any], auction_evidence: Mapping[str, Any] | None = None, market_context: Mapping[str, Any] | None = None, open_confirmation: Mapping[str, Any] | None = None, component_statuses: Mapping[str, Any] | None = None, unavailable_reasons: list[str] | tuple[str, ...] | None = None, report_status: str | None = None, template_path: Path = DEFAULT_TEMPLATE) -> AuctionEmailReport:
    shadow = _mapping(plate_shadow)
    if shadow.get("format") != "PlateAuctionShadowV1":
        raise ValueError("auction email requires PlateAuctionShadowV1")
    trade_date = str(shadow.get("trade_date") or "").strip()
    if len(trade_date) != 10:
        raise ValueError("auction email trade_date is required")
    data_origin = _origin(shadow.get("data_origin"))
    evidence, context = _mapping(auction_evidence), _mapping(market_context)
    overview = _market_overview(shadow, evidence, context, trade_date)
    supplied_statuses = _mapping(component_statuses) or _mapping(context.get("component_statuses")) or _mapping(evidence.get("component_statuses"))
    resolved_component_statuses = {
        "market_overview": str(supplied_statuses.get("market_overview") or ("available" if overview.get("status") == "available" else "unavailable")),
        "plate_facts": str(supplied_statuses.get("plate_facts") or ("available" if _plate_rows(shadow) else "unavailable")),
        "mapping": str(supplied_statuses.get("mapping") or ("unavailable" if str(_mapping(shadow.get("mapping_origin")).get("status")) == "unavailable" else "available")),
    }
    raw_plate_rows = _plate_rows(shadow)
    plate_rows = raw_plate_rows if resolved_component_statuses["plate_facts"] in {"available", "partial"} else []
    locked_source = _mapping(_mapping(shadow.get("automatic_analysis")).get("auction_locked_orders"))
    locked_rows = _locked_order_rows(shadow)
    locked, anchor_changes = _locked_summary(locked_rows, overview, locked_source), _anchor_changes(evidence)
    resolved_status = str(report_status or context.get("report_status") or evidence.get("report_status") or resolve_auction_report_status(resolved_component_statuses))
    reasons = list(unavailable_reasons or context.get("unavailable_reasons") or evidence.get("unavailable_reasons") or [])
    if locked.get("locked_order_status") in {"conflict", "unavailable"}:
        locked_rows = []
    if locked.get("locked_order_status") == "conflict":
        conflict_reason = "A2 summary conflicts with locked-order detail"
        if conflict_reason not in reasons:
            reasons.append(conflict_reason)
        if resolved_status in {"COMPLETE", "PARTIAL"}:
            resolved_status = "PARTIAL"
    context_summary = _market_context_summary(context, trade_date)
    capture_time = shadow.get("capture_time") or context_summary.get("capture_time")
    observations = _core_observations(plate_rows, overview, locked)
    provenance = {
        "data_origin": data_origin, "market_summary_source": overview.get("source", "unavailable"), "market_summary_source_table": overview.get("source_table", "unavailable"), "market_summary_observation_time": overview.get("observation_time", "unavailable"),
        "plate_observation_source": "PlateAuctionShadowV1.plate_stats.0924_to_0925", "plate_observation_time": shadow.get("observation_time") or "unavailable",
        "anchor_source": evidence.get("source") or "unavailable", "auction_evidence_origin": _optional_origin(evidence.get("data_origin")),
        "mapping_origin": json.dumps(shadow.get("mapping_origin") or {}, ensure_ascii=False, sort_keys=True), "historical_valid": bool(shadow.get("historical_valid")),
        "market_context_origin": context_summary.get("data_origin", "unavailable"),
    }
    report: dict[str, Any] = {
        "format": "AuctionEmailReportV1", "report_id": f"auction-market-facts:{trade_date}:0925", "subject": f"【竞价市场事实观察】{trade_date} 09:25", "trade_date": trade_date, "data_origin": data_origin,
        "capture_time": capture_time,
        "report_status": resolved_status,
        "fact_status": {"COMPLETE": "available", "PARTIAL": "partial", "DATA_UNAVAILABLE": "unavailable", "FAILED": "failed"}.get(resolved_status, "unavailable"),
        "component_statuses": resolved_component_statuses,
        "market_overview_status": resolved_component_statuses["market_overview"],
        "plate_facts_status": resolved_component_statuses["plate_facts"],
        "mapping_status": resolved_component_statuses["mapping"],
        "unavailable_reasons": reasons,
        "market_overview": overview, "plate_rows": plate_rows, "locked_order_rows": locked_rows, "locked_summary": locked,
        "observations": observations, "core_observations": observations[:CORE_OBSERVATION_LIMIT], "appendix": _appendix(plate_rows), "anchor_changes": anchor_changes,
        "market_context": context_summary, "open_confirmation": _mapping(open_confirmation) or {"status": "unavailable"},
        "report_settings": _report_settings(), "strategy_impact": "none", "decision_bundle": None,
        "provenance": provenance, "provenance_display": _provenance_display(provenance),
    }
    html_body = _render_html(template_path.read_text(encoding="utf-8"), report)
    text_body = _render_text(report)
    report["fact_view_sha256"] = hashlib.sha256(_canonical_json({key: report[key] for key in ("market_overview", "plate_rows", "locked_summary", "anchor_changes")}).encode("utf-8")).hexdigest()
    report["observations_sha256"] = hashlib.sha256(_canonical_json(report["observations"]).encode("utf-8")).hexdigest()
    report["html_sha256"] = hashlib.sha256(html_body.encode("utf-8")).hexdigest()
    report["text_sha256"] = hashlib.sha256(text_body.encode("utf-8")).hexdigest()
    report["markdown_sha256"] = report["text_sha256"]
    return AuctionEmailReport(subject=report["subject"], text_body=text_body, html_body=html_body, html_sha256=report["html_sha256"], metadata=report)


def load_json_mapping(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"auction email input must be a JSON object: {path}")
    return dict(payload)
