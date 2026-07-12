from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import pandas as pd

from engine_next.connectors.wencai_connector import WencaiConnector, WencaiQuerySpec


SOURCE = "wencai_intraday_repair"
DEFAULT_EXPIRE_SECONDS = 3 * 24 * 60 * 60
DEFAULT_LOCK_SECONDS = 15 * 60
FILTERED_SCOPE = "filtered_effective_auction"


@dataclass(frozen=True)
class SegmentSpec:
    name: str
    query: str
    max_stocks: int
    timeout_seconds: float = 45.0


def _date_compact(trade_date: str) -> str:
    text = str(trade_date or "").strip()
    if re.fullmatch(r"\d{8}", text):
        return text
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text.replace("-", "")
    raise ValueError(f"trade_date must be YYYY-MM-DD or YYYYMMDD, got {trade_date!r}")


def _date_dash(trade_date: str) -> str:
    tag = _date_compact(trade_date)
    return f"{tag[:4]}-{tag[4:6]}-{tag[6:]}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text or text in {"--", "-", "None", "nan"}:
        return default
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 100000000.0
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 10000.0
        text = text[:-1]
    try:
        return float(text) * multiplier
    except Exception:
        match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
        if not match:
            return default
        try:
            return float(match.group(0)) * multiplier
        except Exception:
            return default


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    match = re.search(r"(\d{6})(?:\.(?:SH|SZ|BJ))?$", text)
    if match:
        return match.group(1)
    digits = re.sub(r"\D", "", text)
    if len(digits) == 6:
        return digits
    return ""


def _pick_col(columns: Iterable[str], *needles: str) -> str | None:
    for column in columns:
        text = str(column)
        if all(needle in text for needle in needles):
            return column
    return None


def _first_present(row: dict[str, Any], columns: Iterable[str | None], default: Any = None) -> Any:
    for column in columns:
        if column and column in row:
            value = row.get(column)
            if value is not None and str(value).strip() != "":
                return value
    return default


def _build_segments(trade_date: str) -> list[SegmentSpec]:
    day = _date_dash(trade_date)
    return [
        SegmentSpec(
            name="cap_gt_1000e",
            query=f"{day}市值>1000亿;竞价涨跌幅;竞价金额;竞价金额>300万;竞价未匹配金额",
            max_stocks=300,
        ),
        SegmentSpec(
            name="cap_500_1000e",
            query=f"{day}市值500亿到1000亿;竞价涨跌幅;竞价金额;竞价金额>300万;竞价未匹配金额",
            max_stocks=400,
        ),
        SegmentSpec(
            name="cap_200_500e",
            query=f"{day}市值200亿到500亿;竞价涨跌幅;竞价金额;竞价金额>500万;竞价未匹配金额",
            max_stocks=700,
        ),
        SegmentSpec(
            name="cap_100_200e",
            query=f"{day}市值100亿到200亿;竞价涨跌幅;竞价金额;竞价金额>800万;竞价未匹配金额",
            max_stocks=900,
        ),
        SegmentSpec(
            name="cap_50_100e",
            query=f"{day}市值50亿到100亿;竞价涨跌幅;竞价金额;竞价金额>1000万;竞价未匹配金额",
            max_stocks=900,
        ),
        SegmentSpec(
            name="cap_lt_50e",
            query=f"{day}市值<50亿;竞价涨跌幅;竞价金额;竞价金额>1500万;竞价未匹配金额",
            max_stocks=900,
        ),
        SegmentSpec(
            name="small_cap_active",
            query=f"{day}市值<100亿;竞价涨幅>1.5%;竞价金额;竞价金额>500万;竞价未匹配金额",
            max_stocks=700,
        ),
    ]


async def _fetch_segment(connector: WencaiConnector, spec: SegmentSpec, index: int, *, delay_seconds: float) -> pd.DataFrame:
    if index > 0:
        await asyncio.sleep(delay_seconds)
    return await asyncio.wait_for(
        connector.fetch_dataframe(WencaiQuerySpec(query=spec.query, max_stocks=spec.max_stocks)),
        timeout=spec.timeout_seconds,
    )


def _normalize_dataframe(df: pd.DataFrame, *, segment: str) -> list[dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    columns = list(df.columns)
    symbol_col = (
        "code"
        if "code" in columns
        else _pick_col(columns, "股票代码")
        or _pick_col(columns, "代码")
        or columns[0]
    )
    name_col = _pick_col(columns, "股票简称") or _pick_col(columns, "名称") or "name"
    amount_col = _pick_col(columns, "竞价金额") or _pick_col(columns, "开盘成交额")
    change_col = (
        _pick_col(columns, "竞价", "涨跌幅")
        or _pick_col(columns, "竞价", "涨幅")
        or _pick_col(columns, "开盘", "涨跌幅")
        or _pick_col(columns, "开盘", "涨幅")
    )
    bid_col = _pick_col(columns, "竞价未匹配金额") or _pick_col(columns, "竞价", "未匹配")
    price_col = _pick_col(columns, "竞价匹配价") or _pick_col(columns, "竞价", "价格") or _pick_col(columns, "开盘价")

    rows: list[dict[str, Any]] = []
    for raw in df.to_dict("records"):
        symbol = _normalize_symbol(_first_present(raw, (symbol_col, "code", "股票代码")))
        if not symbol:
            continue
        amount = _safe_float(_first_present(raw, (amount_col, "auction_amount_yuan", "amount"), 0.0))
        if amount <= 0:
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": str(_first_present(raw, (name_col, "name", "股票简称"), "") or ""),
                "tag": "0925",
                "timestamp": 92500,
                "price": _safe_float(_first_present(raw, (price_col, "price", "open_price"), 0.0)),
                "change_pct": _safe_float(_first_present(raw, (change_col, "change_pct", "open_pct"), 0.0)),
                "amount": amount,
                "auction_amount_yuan": amount,
                "bid_amount": _safe_float(_first_present(raw, (bid_col, "bid_amount_yuan", "bid_amount"), 0.0)),
                "bid_amount_yuan": _safe_float(_first_present(raw, (bid_col, "bid_amount_yuan", "bid_amount"), 0.0)),
                "source": SOURCE,
                "segment": segment,
            }
        )
    return rows


def _dedupe_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = _normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        current = merged.get(symbol)
        if current is None or float(row.get("auction_amount_yuan", 0.0) or 0.0) > float(
            current.get("auction_amount_yuan", 0.0) or 0.0
        ):
            row["symbol"] = symbol
            merged[symbol] = row
    return sorted(merged.values(), key=lambda item: float(item.get("auction_amount_yuan", 0.0) or 0.0), reverse=True)


def _summary(rows: list[dict[str, Any]], *, repaired_at: str) -> dict[str, Any]:
    high_open = sum(1 for row in rows if float(row.get("change_pct", 0.0) or 0.0) > 0.01)
    low_open = sum(1 for row in rows if float(row.get("change_pct", 0.0) or 0.0) < -0.01)
    flat_open = max(0, len(rows) - high_open - low_open)
    return {
        "tag": "0925",
        "ts": 92500,
        "coverage_scope": FILTERED_SCOPE,
        "sample_total_stocks": len(rows),
        "total_stocks": len(rows),
        "high_open_count": high_open,
        "low_open_count": low_open,
        "flat_open_count": flat_open,
        "limit_up_count": sum(1 for row in rows if float(row.get("change_pct", 0.0) or 0.0) >= 9.8),
        "limit_down_count": sum(1 for row in rows if float(row.get("change_pct", 0.0) or 0.0) <= -9.8),
        "total_auction_amount_yuan": round(sum(float(row.get("auction_amount_yuan", 0.0) or 0.0) for row in rows), 2),
        "total_limit_up_bid_amount_yuan": round(
            sum(
                float(row.get("bid_amount_yuan", 0.0) or 0.0)
                for row in rows
                if float(row.get("change_pct", 0.0) or 0.0) >= 9.8
            ),
            2,
        ),
        "source": SOURCE,
        "repaired_at": repaired_at,
        "degraded": True,
        "degraded_reason": "intraday_repair_has_filtered_0925_only_no_0920_0924_delta",
    }


def _anchor_payload(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payload: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = _normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        payload[symbol] = {
            "name": str(row.get("name", "") or ""),
            "change_pct": float(row.get("change_pct", 0.0) or 0.0),
            "amount": float(row.get("auction_amount_yuan", row.get("amount", 0.0)) or 0.0),
            "bid_amount": float(row.get("bid_amount_yuan", row.get("bid_amount", 0.0)) or 0.0),
            "tag": "0925",
            "source": SOURCE,
        }
    return payload


def _redis_client() -> Any:
    import redis

    url = os.getenv("REDIS_URL", "").strip()
    if url:
        return redis.Redis.from_url(url, decode_responses=True)
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
    )


def _auction_source(redis_client: Any, date_tag: str) -> str:
    raw = redis_client.hget(f"market:auction:{date_tag}:0925", "top_amount")
    if not raw:
        return ""
    try:
        rows = json.loads(raw)
    except Exception:
        return "existing_unreadable_source"
    if not rows:
        return ""
    first_source = str(rows[0].get("source", "") if isinstance(rows[0], dict) else "")
    return first_source or "existing_unknown_source"


def _write_redis(redis_client: Any, trade_date: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    date_tag = _date_compact(trade_date)
    repaired_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = _summary(rows, repaired_at=repaired_at)
    top_json = json.dumps(rows, ensure_ascii=False)
    summary_json = json.dumps(summary, ensure_ascii=False)
    anchor_json = json.dumps(_anchor_payload(rows), ensure_ascii=False)

    snapshot_key = f"market:auction:{date_tag}:0925"
    latest_key = f"market:auction:{date_tag}:latest"
    anchor_key = f"market:auction:anchor:{date_tag}"
    meta_key = f"diag:auction_repair:{date_tag}"

    pipe = redis_client.pipeline()
    pipe.hset(snapshot_key, mapping={"top_amount": top_json, "summary": summary_json, "source": SOURCE, "tag": "0925"})
    pipe.expire(snapshot_key, DEFAULT_EXPIRE_SECONDS)
    pipe.hset(latest_key, mapping={"tag": "0925", "source": SOURCE, "repaired_at": repaired_at, "degraded": "1"})
    pipe.expire(latest_key, DEFAULT_EXPIRE_SECONDS)
    pipe.set(anchor_key, anchor_json, ex=DEFAULT_EXPIRE_SECONDS)
    pipe.hset(
        meta_key,
        mapping={
            "source": SOURCE,
            "rows": str(len(rows)),
            "coverage_scope": FILTERED_SCOPE,
            "sample_total_stocks": str(len(rows)),
            "repaired_at": repaired_at,
            "snapshot_key": snapshot_key,
            "anchor_key": anchor_key,
            "degraded_reason": "filtered_0925_only_no_0920_0924_delta",
        },
    )
    pipe.expire(meta_key, DEFAULT_EXPIRE_SECONDS)
    pipe.execute()
    return {"snapshot_key": snapshot_key, "latest_key": latest_key, "anchor_key": anchor_key, "summary": summary}


async def repair(trade_date: str, *, force: bool = False, dry_run: bool = False, delay_seconds: float = 1.5) -> dict[str, Any]:
    date_tag = _date_compact(trade_date)
    redis_client = _redis_client()
    lock_key = f"repair:auction:{date_tag}:lock"
    lock_value = f"{os.getpid()}:{time.time()}"

    existing_source = _auction_source(redis_client, date_tag)
    if existing_source and existing_source != SOURCE:
        return {"ok": True, "skipped": True, "reason": "native_auction_0925_exists", "rows": 0}
    if existing_source == SOURCE and not force:
        return {"ok": True, "skipped": True, "reason": "repair_auction_0925_exists", "rows": 0}

    locked = False
    if not dry_run:
        locked = bool(redis_client.set(lock_key, lock_value, nx=True, ex=DEFAULT_LOCK_SECONDS))
        if not locked:
            return {"ok": False, "skipped": True, "reason": "repair_lock_exists", "rows": 0}

    try:
        connector = WencaiConnector()
        all_rows: list[dict[str, Any]] = []
        segment_results: list[dict[str, Any]] = []
        for index, spec in enumerate(_build_segments(trade_date)):
            try:
                df = await _fetch_segment(connector, spec, index, delay_seconds=delay_seconds)
                rows = _normalize_dataframe(df, segment=spec.name)
                all_rows.extend(rows)
                segment_results.append({"segment": spec.name, "rows": len(rows), "ok": True})
            except Exception as exc:
                segment_results.append({"segment": spec.name, "rows": 0, "ok": False, "error": str(exc)[:200]})

        rows = _dedupe_rows(all_rows)
        result: dict[str, Any] = {
            "ok": bool(rows),
            "trade_date": _date_dash(trade_date),
            "source": SOURCE,
            "coverage_scope": FILTERED_SCOPE,
            "rows": len(rows),
            "segments": segment_results,
            "dry_run": dry_run,
        }
        if not rows:
            result["reason"] = "no_usable_wencai_rows"
            return result
        if not dry_run:
            appeared_source = _auction_source(redis_client, date_tag)
            if appeared_source and appeared_source != SOURCE:
                result.update({"skipped": True, "reason": "native_auction_appeared_before_write"})
                return result
            if appeared_source == SOURCE and not force:
                result.update({"skipped": True, "reason": "repair_auction_appeared_before_write"})
                return result
            result["redis"] = _write_redis(redis_client, trade_date, rows)
        return result
    finally:
        if locked:
            try:
                if redis_client.get(lock_key) == lock_value:
                    redis_client.delete(lock_key)
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repair missing 09:25 auction anchor with segmented Wencai queries.")
    parser.add_argument("trade_date", help="Trade date, YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing repaired data; never overwrites native non-repair data.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and normalize but do not write Redis.")
    parser.add_argument("--delay-seconds", type=float, default=1.5, help="Delay between Wencai segment requests.")
    args = parser.parse_args(argv)
    result = asyncio.run(
        repair(args.trade_date, force=bool(args.force), dry_run=bool(args.dry_run), delay_seconds=float(args.delay_seconds))
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") or result.get("skipped") else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
