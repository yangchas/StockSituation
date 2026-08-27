"""Read-only production fact assembly for the 09:26 facts report.

This module deliberately contains orchestration only.  Auction math remains in
the existing C++ producer and the pure ``PlateAuctionShadowV1`` transform.
The TD query is injected so tests and capture tools cannot accidentally share
the production write-capable TD service.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from engine_next.runtime.auction_email_report import resolve_auction_report_status
from engine_next.runtime.auction_shadow import build_plate_shadow_from_snapshot_rows


class MappingNotReadyError(RuntimeError):
    """Raised when the canonical mapping is readable but below readiness."""

    def __init__(self, actual_record_count: int, required_min_count: int) -> None:
        self.actual_record_count = int(actual_record_count)
        self.required_min_count = int(required_min_count)
        super().__init__(f"mapping_ready=false: {self.actual_record_count} < {self.required_min_count}")


def _json(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        parts = text.split(".")
        text = next((part for part in parts if len(part) >= 6 and part[-6:].isdigit()), parts[0])
    return text[-6:]


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _text(value: Any) -> str:
    return str(value or "").strip()


def build_readonly_td_auction_query() -> Callable[[str, str], Iterable[Mapping[str, Any]]]:
    """Build the small read-only TD adapter used by production reporting.

    A connection is opened only when a reporting event needs it and is closed
    after that tag.  The adapter executes the existing snapshot query shape;
    it does not create tables, write rows, or provide a fallback source.
    """

    def query(trade_date: str, tag: str) -> Iterable[Mapping[str, Any]]:
        database = os.environ.get("TDENGINE_DATABASE", "market_data1")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
            raise ValueError("invalid TDENGINE_DATABASE identifier")
        normalized_date = str(trade_date or "").replace("-", "")
        normalized_tag = str(tag or "").strip()
        if not re.fullmatch(r"\d{8}", normalized_date) or normalized_tag not in {"0920", "0924", "0925"}:
            raise ValueError("invalid auction query arguments")
        try:
            import taos
        except ImportError:
            return ()
        connection = taos.connect(
            host=os.environ.get("TDENGINE_HOST", "127.0.0.1"),
            user=os.environ.get("TDENGINE_USER", "root"),
            password=os.environ.get("TDENGINE_PASSWORD", "taosdata"),
            database=database,
        )
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT * FROM {database}.auction_snapshot_v2 "
                f"WHERE trade_date='{normalized_date}' AND auction_tag='{normalized_tag}' "
                "ORDER BY ts, symbol"
            )
            columns = [str(item[0]) for item in (cursor.description or ())]
            rows: list[dict[str, Any]] = []
            while True:
                batch = cursor.fetchmany(5000)
                if not batch:
                    break
                rows.extend(dict(zip(columns, values)) for values in batch)
            try:
                cursor.close()
            except Exception:
                pass
            return rows
        finally:
            connection.close()

    return query


def _sha256(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hget(redis_client: Any, key: str, field: str) -> Any:
    if redis_client is None or not hasattr(redis_client, "hget"):
        return None
    return redis_client.hget(key, field)


def _hgetall(redis_client: Any, key: str) -> dict[str, Any]:
    if redis_client is None or not hasattr(redis_client, "hgetall"):
        return {}
    raw = redis_client.hgetall(key) or {}
    return {str(k): v for k, v in raw.items()} if isinstance(raw, Mapping) else {}


def _anchor_symbols(redis_client: Any, *, trade_date: str) -> set[str]:
    """Return the symbol universe declared by the frozen 09:25 anchor.

    The anchor is a Redis JSON string written by the existing production
    auction path.  It is used only for an availability/effective-universe
    check; no values are copied into the TD fact rows.
    """
    if redis_client is None or not hasattr(redis_client, "get"):
        return set()
    raw = redis_client.get(f"market:auction:anchor:{trade_date.replace('-', '')}")
    payload = _json(raw)
    if not isinstance(payload, Mapping):
        return set()
    return {
        normalized
        for value in payload.keys()
        for normalized in (_symbol(value),)
        if normalized
    }


def _load_mapping(redis_client: Any, *, key: str = "market:stock_plate") -> dict[str, str]:
    raw = _hgetall(redis_client, key)
    result: dict[str, str] = {}
    for symbol, plate in raw.items():
        normalized = _symbol(symbol)
        value = _json(plate)
        if isinstance(value, list):
            value = value[0] if value else ""
        if normalized and _text(value):
            result[normalized] = _text(value)
    return result


def _summary(redis_client: Any, *, trade_date: str) -> dict[str, Any]:
    key = f"market:auction:{trade_date.replace('-', '')}:0925"
    raw = _hget(redis_client, key, "summary")
    value = _json(raw)
    return dict(value) if isinstance(value, Mapping) else {}


def _redis_summary_payload(summary: Mapping[str, Any], *, trade_date: str) -> dict[str, Any]:
    """Map the existing A2 summary to the report's explicit market source."""
    return {
        "format": "AuctionMarketSummaryV1",
        "status": "available" if summary else "unavailable",
        "source": "a2_0925_summary" if summary else "unavailable",
        "source_table": _text(summary.get("source_table")) or "redis:market:auction:0925",
        "trade_date": trade_date,
        "observation_time": summary.get("observation_time") or summary.get("ts"),
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


def normalize_td_auction_row(row: Mapping[str, Any], *, tag: str) -> dict[str, Any]:
    """Normalize existing TD columns without reimplementing auction formulas."""
    px_milli = _number(row.get("px_milli", row.get("price_milli", row.get("px"))))
    chg_bp = _number(row.get("chg_bp", row.get("change_bp")))
    change_pct = (chg_bp / 100.0) if chg_bp is not None else _number(row.get("change_pct"))
    match_amt_yuan = _number(row.get("match_amt_yuan", row.get("auction_amount_yuan", row.get("amount"))))
    rest_bid_amt_yuan = _number(row.get("rest_bid_amt_yuan", row.get("bid_amount_yuan", row.get("br"))))
    rest_ask_amt_yuan = _number(row.get("rest_ask_amt_yuan", row.get("ask_amount_yuan", row.get("ar"))))
    ask_raw = row.get("rest_ask_amt_yuan", row.get("ask_amount_yuan", row.get("ar")))
    return {
        "symbol": _symbol(row.get("symbol") or row.get("code")),
        "tag": _text(tag),
        "timestamp": row.get("ts") or row.get("timestamp"),
        "price_milli": px_milli,
        "price": (px_milli / 1000.0) if px_milli is not None else _number(row.get("price")),
        "change_pct": change_pct,
        "auction_amount_yuan": match_amt_yuan,
        "amount": match_amt_yuan,
        "bid_amount_yuan": rest_bid_amt_yuan,
        "bid_amount": rest_bid_amt_yuan,
        "ask_amount_yuan": rest_ask_amt_yuan,
        "ask_amount": rest_ask_amt_yuan,
        # Preserve the source-contract fields needed to classify TD-only
        # rows.  They are diagnostics only; auction formulas remain owned by
        # the existing producer.
        "match_amt_yuan": match_amt_yuan,
        "chg_bp": chg_bp,
        "rest_bid_amt_yuan": rest_bid_amt_yuan,
        "rest_ask_amt_yuan": rest_ask_amt_yuan,
        "ask_amount_present": ask_raw is not None and str(ask_raw).strip() != "",
        "limit_state": row.get("limit_state", row.get("ls")),
        "source": "tdengine:auction_snapshot_v2",
    }


_TD_INACTIVITY_FIELDS = ("match_amt_yuan", "chg_bp", "rest_bid_amt_yuan", "rest_ask_amt_yuan")


def _classify_td_inactivity(row: Mapping[str, Any]) -> str:
    """Classify one TD-only row using the existing effective-universe contract.

    The producer's zero-valued activity fields are the only evidence that a
    raw TD row is inactive.  Unknown or malformed fields must stay in the
    effective universe so an unexplained difference remains fail-closed.
    """
    values = [_number(row.get(field)) for field in _TD_INACTIVITY_FIELDS]
    if any(value is None for value in values):
        return "unknown"
    return "inactive" if all(value == 0.0 for value in values) else "non_inactive"


@dataclass(frozen=True)
class ProductionAuctionFacts:
    trade_date: str
    data_origin: str
    mapping: dict[str, str]
    mapping_origin: dict[str, Any]
    market_summary: dict[str, Any]
    snapshot_rows: tuple[dict[str, Any], ...]
    plate_shadow: dict[str, Any]
    status: str
    provenance: dict[str, Any]
    market_summary_status: str = "unavailable"
    plate_facts_status: str = "unavailable"
    mapping_status: str = "unavailable"
    unavailable_reasons: tuple[str, ...] = ()
    report_status: str = "DATA_UNAVAILABLE"

    @property
    def component_statuses(self) -> dict[str, str]:
        return {
            "market_overview": self.market_summary_status,
            "plate_facts": self.plate_facts_status,
            "mapping": self.mapping_status,
        }


def build_production_auction_facts(
    *,
    trade_date: str,
    redis_client: Any,
    td_query: Callable[[str, str], Iterable[Mapping[str, Any]]],
    mapping_key: str = "market:stock_plate",
    data_origin: str = "production_realtime",
    historical_valid: bool = False,
    mapping_snapshot: Mapping[str, Any] | None = None,
) -> ProductionAuctionFacts:
    """Assemble one read-only production fact bundle for a 09:26 report.

    ``td_query`` receives ``(trade_date, tag)`` and must return rows from the
    existing ``auction_snapshot_v2`` table.  No fallback query or network
    repair is attempted here.
    """
    normalized_date = str(trade_date).strip()
    if len(normalized_date) != 10:
        raise ValueError("trade_date must be YYYY-MM-DD")
    if data_origin not in {"production_realtime", "production_capture", "current_cache_only"}:
        raise ValueError("production fact assembly rejects replay origins")

    mapping_supplied = mapping_snapshot is not None
    mapping_payload = dict(mapping_snapshot or {})
    # Production reporting is deliberately bound to the runtime-owned frozen
    # artifact.  A missing snapshot must not silently refreeze live
    # ``market:stock_plate`` after the cutoff (or during an audit preview).
    mapping = dict(mapping_payload.get("mapping") or {}) if mapping_supplied else {}
    mapping_status = "available" if mapping else "unavailable"
    mapping_origin = {
        "canonical": mapping_key,
        "status": "runtime_owned_snapshot" if mapping_supplied and mapping else "unavailable",
        "trade_date": mapping_payload.get("trade_date") if mapping_supplied else normalized_date,
        "effective_time": mapping_payload.get("effective_time", "unavailable") if mapping_supplied else "unavailable",
        "sha256": mapping_payload.get("sha256") if mapping_supplied and mapping else None,
    }
    summary = _summary(redis_client, trade_date=normalized_date)
    rows: list[dict[str, Any]] = []
    # These TD rows exist solely to form mapping-dependent plate facts and the
    # effective-universe check.  Do not spend a full-market TD read when the
    # authoritative mapping itself is unavailable.
    if mapping:
        for tag in ("0920", "0924", "0925"):
            rows.extend(normalize_td_auction_row(row, tag=tag) for row in td_query(normalized_date, tag))
    rows = [row for row in rows if row.get("symbol")]
    tags = {str(row.get("tag")) for row in rows}
    missing_tags = sorted({"0920", "0924", "0925"} - tags)
    anchor_symbols = _anchor_symbols(redis_client, trade_date=normalized_date)
    td_raw_symbols = {
        str(row.get("symbol"))
        for row in rows
        if str(row.get("tag")) == "0925" and str(row.get("symbol"))
    }
    td_rows_0925 = {
        str(row.get("symbol")): row
        for row in rows
        if str(row.get("tag")) == "0925" and str(row.get("symbol"))
    }
    td_only_symbols = td_raw_symbols - anchor_symbols if anchor_symbols else set()
    td_only_classification = {
        symbol: _classify_td_inactivity(td_rows_0925[symbol])
        for symbol in sorted(td_only_symbols)
        if symbol in td_rows_0925
    }
    inactive_symbols = {
        symbol for symbol, classification in td_only_classification.items() if classification == "inactive"
    }
    td_effective_symbols = td_raw_symbols - inactive_symbols
    anchor_only_symbols = anchor_symbols - td_raw_symbols if anchor_symbols else set()
    if not anchor_symbols:
        effective_universe_status = "unavailable"
    elif td_effective_symbols == anchor_symbols:
        effective_universe_status = "match"
    else:
        effective_universe_status = "mismatch"
    provenance = {
        "market_summary_source": "redis:market:auction:0925" if summary else "unavailable",
        "snapshot_source": "tdengine:auction_snapshot_v2" if rows else "unavailable",
        "snapshot_rows": len(rows),
        "snapshot_tags": sorted(tags),
        "missing_tags": missing_tags,
        "anchor_universe_count": len(anchor_symbols),
        "td_raw_universe_count": len(td_raw_symbols),
        "td_inactive_universe_count": len(inactive_symbols),
        "td_effective_universe_count": len(td_effective_symbols),
        "td_only_count": len(td_only_symbols),
        "td_only_inactive_count": sum(value == "inactive" for value in td_only_classification.values()),
        "td_only_unknown_count": sum(value == "unknown" for value in td_only_classification.values()),
        "td_only_non_inactive_count": sum(value == "non_inactive" for value in td_only_classification.values()),
        "td_only_inactive_sample": sorted(symbol for symbol, value in td_only_classification.items() if value == "inactive")[:10],
        "td_only_unknown_sample": sorted(symbol for symbol, value in td_only_classification.items() if value == "unknown")[:10],
        "td_only_non_inactive_sample": sorted(symbol for symbol, value in td_only_classification.items() if value == "non_inactive")[:10],
        "anchor_only_count": len(anchor_only_symbols),
        "anchor_only_sample": sorted(anchor_only_symbols)[:10],
        "effective_universe_status": effective_universe_status,
        "historical_valid": bool(historical_valid),
    }
    universe_valid = effective_universe_status == "match"
    reasons: list[str] = []
    if not summary:
        reasons.append("A2 summary unavailable")
    if not mapping:
        reasons.append("frozen mapping unavailable")
    if mapping and not rows:
        reasons.append("auction snapshot rows unavailable")
    if mapping and missing_tags:
        reasons.append("auction snapshot anchor tag unavailable")
    if mapping and not universe_valid:
        reasons.append("effective auction universe mismatch or unavailable")

    effective_rows = [row for row in rows if str(row.get("symbol")) in td_effective_symbols]
    if not rows or not mapping or missing_tags or not universe_valid:
        shadow = {
            "format": "PlateAuctionShadowV1",
            "contract_version": "PlateAuctionShadowV1",
            "mapping_origin": mapping_origin,
            "data_origin": data_origin,
            "trade_date": normalized_date,
            "historical_valid": bool(historical_valid),
            "status": "unavailable" if not rows else "partial",
            "source_provenance": provenance,
            "plate_stats": {"0924_to_0925": {}},
            "symbol_details": {"0924_to_0925": {"detail_rows": []}},
            "strategy_impact": "none",
            "decision_bundle": None,
        }
        status = "unavailable" if not rows else "partial"
    else:
        shadow = build_plate_shadow_from_snapshot_rows(
            effective_rows,
            trade_date=normalized_date,
            stock_plate=mapping,
            data_origin=data_origin,
            mapping_origin=mapping_origin,
            historical_valid=historical_valid,
            source_provenance=provenance,
            observation_time=str(summary.get("observation_time") or summary.get("ts") or "") or None,
            change_pct_unit="percent",
        )
        status = "normal" if summary and universe_valid else "partial"
    market_summary_status = "available" if summary else "unavailable"
    # A plate shadow is only authoritative when the mapping-dependent rows
    # passed the existing completeness/universe contract.
    plate_facts_status = "available" if rows and mapping and not missing_tags and universe_valid else "unavailable"
    component_statuses = {
        "market_overview": market_summary_status,
        "plate_facts": plate_facts_status,
        "mapping": mapping_status,
    }
    resolved_report_status = resolve_auction_report_status(component_statuses)
    if resolved_report_status == "PARTIAL" and not reasons:
        reasons.append("one factual component unavailable")
    status = {"COMPLETE": "normal", "PARTIAL": "partial", "DATA_UNAVAILABLE": "unavailable"}[resolved_report_status]
    return ProductionAuctionFacts(
        trade_date=normalized_date,
        data_origin=data_origin,
        mapping=mapping,
        mapping_origin=mapping_origin,
        market_summary=_redis_summary_payload(summary, trade_date=normalized_date),
        snapshot_rows=tuple(rows),
        plate_shadow=shadow,
        status=status,
        provenance=provenance,
        market_summary_status=market_summary_status,
        plate_facts_status=plate_facts_status,
        mapping_status=mapping_status,
        unavailable_reasons=tuple(reasons),
        report_status=resolved_report_status,
    )


def report_inputs(bundle: ProductionAuctionFacts) -> dict[str, Any]:
    """Return the explicit inputs expected by ``build_auction_email_report``."""
    return {
        "trade_date": bundle.trade_date,
        "data_origin": bundle.data_origin,
        "market_summary": bundle.market_summary,
        "plate_shadow": bundle.plate_shadow,
        "mapping_origin": bundle.mapping_origin,
        "status": bundle.status,
        "report_status": bundle.report_status,
        "component_statuses": {
            "market_overview": bundle.market_summary_status,
            "plate_facts": bundle.plate_facts_status,
            "mapping": bundle.mapping_status,
        },
        "unavailable_reasons": list(bundle.unavailable_reasons),
        "provenance": bundle.provenance,
    }


def write_mapping_snapshot(
    *,
    mapping: Mapping[str, str],
    trade_date: str,
    effective_time: str,
    source: str,
    directory: Path,
) -> Path:
    """Atomically write the small runtime-owned mapping artifact."""
    payload = {
        "schema_version": "RuntimeStockPlateSnapshotV1",
        "trade_date": trade_date,
        "effective_time": effective_time,
        "source": source,
        "record_count": len(mapping),
        "mapping": dict(sorted((str(k), str(v)) for k, v in mapping.items())),
    }
    payload["sha256"] = _sha256(payload["mapping"])
    target_dir = directory / trade_date
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "stock_plate_snapshot.json"
    temporary = target.with_suffix(".json.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.replace(target)
    return target


def load_mapping_snapshot(
    *,
    directory: Path,
    trade_date: str,
    minimum_record_count: int = 0,
) -> dict[str, Any] | None:
    """Load and verify the runtime-owned snapshot for exactly ``trade_date``."""
    target = directory / str(trade_date) / "stock_plate_snapshot.json"
    if not target.exists():
        return None
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or str(payload.get("trade_date") or "") != str(trade_date):
        raise ValueError("mapping snapshot trade_date mismatch")
    mapping = payload.get("mapping")
    if not isinstance(mapping, Mapping) or payload.get("sha256") != _sha256(mapping):
        raise ValueError("mapping snapshot sha256 mismatch")
    if int(payload.get("record_count") or 0) != len(mapping):
        raise ValueError("mapping snapshot record_count mismatch")
    if len(mapping) < int(minimum_record_count or 0):
        raise ValueError("mapping snapshot is below readiness threshold")
    return dict(payload)


def freeze_mapping_snapshot(
    *,
    redis_client: Any,
    directory: Path,
    trade_date: str,
    effective_time: str,
    source: str = "market:stock_plate",
    minimum_record_count: int = 0,
) -> dict[str, Any]:
    """Create once, then reuse the same daily mapping snapshot after restart."""
    existing = load_mapping_snapshot(
        directory=directory,
        trade_date=trade_date,
        minimum_record_count=minimum_record_count,
    )
    if existing is not None:
        return existing
    mapping = _load_mapping(redis_client, key=source)
    if len(mapping) < int(minimum_record_count or 0):
        raise MappingNotReadyError(len(mapping), int(minimum_record_count or 0))
    if not mapping:
        raise RuntimeError("runtime mapping snapshot unavailable")
    path = write_mapping_snapshot(
        mapping=mapping,
        trade_date=trade_date,
        effective_time=effective_time,
        source=source,
        directory=directory,
    )
    return json.loads(path.read_text(encoding="utf-8"))
