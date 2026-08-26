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

from engine_next.runtime.auction_shadow import build_plate_shadow_from_snapshot_rows


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
    ask_raw = row.get("rest_ask_amt_yuan", row.get("ask_amount_yuan", row.get("ar")))
    return {
        "symbol": _symbol(row.get("symbol") or row.get("code")),
        "tag": _text(tag),
        "timestamp": row.get("ts") or row.get("timestamp"),
        "price_milli": px_milli,
        "price": (px_milli / 1000.0) if px_milli is not None else _number(row.get("price")),
        "change_pct": change_pct,
        "auction_amount_yuan": _number(row.get("match_amt_yuan", row.get("auction_amount_yuan", row.get("amount")))),
        "amount": _number(row.get("match_amt_yuan", row.get("auction_amount_yuan", row.get("amount")))),
        "bid_amount_yuan": _number(row.get("rest_bid_amt_yuan", row.get("bid_amount_yuan", row.get("br")))),
        "bid_amount": _number(row.get("rest_bid_amt_yuan", row.get("bid_amount_yuan", row.get("br")))),
        "ask_amount_yuan": _number(row.get("rest_ask_amt_yuan", row.get("ask_amount_yuan", row.get("ar")))),
        "ask_amount": _number(row.get("rest_ask_amt_yuan", row.get("ask_amount_yuan", row.get("ar")))),
        "ask_amount_present": ask_raw is not None and str(ask_raw).strip() != "",
        "limit_state": row.get("limit_state", row.get("ls")),
        "source": "tdengine:auction_snapshot_v2",
    }


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

    mapping_payload = dict(mapping_snapshot or {})
    mapping = dict(mapping_payload.get("mapping") or {}) if mapping_payload else _load_mapping(redis_client, key=mapping_key)
    mapping_origin = {
        "canonical": mapping_key,
        "status": "runtime_owned_snapshot" if mapping_snapshot else "production_realtime",
        "trade_date": mapping_payload.get("trade_date") if mapping_payload else normalized_date,
        "effective_time": mapping_payload.get("effective_time", "unavailable") if mapping_payload else "unavailable",
        "sha256": mapping_payload.get("sha256") if mapping_payload else _sha256(mapping),
    }
    summary = _summary(redis_client, trade_date=normalized_date)
    rows: list[dict[str, Any]] = []
    for tag in ("0920", "0924", "0925"):
        rows.extend(normalize_td_auction_row(row, tag=tag) for row in td_query(normalized_date, tag))
    rows = [row for row in rows if row.get("symbol")]
    tags = {str(row.get("tag")) for row in rows}
    missing_tags = sorted({"0920", "0924", "0925"} - tags)
    provenance = {
        "market_summary_source": "redis:market:auction:0925" if summary else "unavailable",
        "snapshot_source": "tdengine:auction_snapshot_v2" if rows else "unavailable",
        "snapshot_rows": len(rows),
        "snapshot_tags": sorted(tags),
        "missing_tags": missing_tags,
        "effective_universe_status": "candidate_pending_ground_truth",
        "historical_valid": bool(historical_valid),
    }
    if not rows or not mapping or missing_tags:
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
            rows,
            trade_date=normalized_date,
            stock_plate=mapping,
            data_origin=data_origin,
            mapping_origin=mapping_origin,
            historical_valid=historical_valid,
            source_provenance=provenance,
            observation_time=str(summary.get("observation_time") or summary.get("ts") or "") or None,
            change_pct_unit="percent",
        )
        status = "normal" if summary else "partial"
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


def load_mapping_snapshot(*, directory: Path, trade_date: str) -> dict[str, Any] | None:
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
    return dict(payload)


def freeze_mapping_snapshot(
    *,
    redis_client: Any,
    directory: Path,
    trade_date: str,
    effective_time: str,
    source: str = "market:stock_plate",
) -> dict[str, Any]:
    """Create once, then reuse the same daily mapping snapshot after restart."""
    existing = load_mapping_snapshot(directory=directory, trade_date=trade_date)
    if existing is not None:
        return existing
    mapping = _load_mapping(redis_client, key=source)
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
