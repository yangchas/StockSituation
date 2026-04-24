#!/usr/bin/env python3
"""Diagnose auction keys format, change_pct scale, and summary consistency."""
import os
import json
import datetime
import redis


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _normalize_change_pct_auto(v):
    x = _safe_float(v, 0.0)
    if abs(x) <= 0.3:
        return x * 100.0
    if abs(x) <= 1.0:
        return x
    return x


def _load_top_amount(r, key):
    raw = r.hget(key, "top_amount")
    if not raw:
        return []
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
    except Exception:
        return []
    return []


def _summary_from_items(items):
    changes = [_normalize_change_pct_auto(it.get("change_pct", 0.0)) for it in items]
    sample = len(changes)
    if not changes:
        return {
            "sample": 0,
            "max": 0.0,
            "min": 0.0,
            "avg": 0.0,
            "high_open_ge5": 0,
            "deep_low_le8": 0,
            "limit_up_ge98": 0,
            "limit_down_le95": 0,
        }
    return {
        "sample": sample,
        "max": max(changes),
        "min": min(changes),
        "avg": sum(changes) / max(1, sample),
        "high_open_ge5": sum(1 for x in changes if x >= 5.0),
        "deep_low_le8": sum(1 for x in changes if x <= -8.0),
        "limit_up_ge98": sum(1 for x in changes if x >= 9.8),
        "limit_down_le95": sum(1 for x in changes if x <= -9.5),
    }


def main():
    date_str = os.getenv("DATE") or datetime.date.today().strftime("%Y-%m-%d")
    date_compact = date_str.replace("-", "")

    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    keys = sorted(r.keys(f"market:auction:{date_compact}:*"))
    if not keys:
        print(f"No auction keys for {date_str}")
        return

    latest = r.hgetall(f"market:auction:{date_compact}:latest")
    if latest:
        print(f"latest: tag={latest.get('tag')} ts={latest.get('ts')}")

    for key in keys:
        ktype = r.type(key)
        if ktype != "hash":
            print(f"{key}: type={ktype} (skip)")
            continue
        fields = set(r.hkeys(key))
        items = _load_top_amount(r, key)
        summary = _summary_from_items(items)
        scale_suspect = summary["max"] >= 50 or summary["min"] <= -50
        missing_fields = [f for f in ("top_amount", "meta", "summary") if f not in fields]
        print(
            f"{key}: sample={summary['sample']} max={summary['max']:.2f}% "
            f"min={summary['min']:.2f}% avg={summary['avg']:.2f}% "
            f"high5={summary['high_open_ge5']} deep-8={summary['deep_low_le8']} "
            f"missing={','.join(missing_fields) or 'none'} "
            f"scale_suspect={scale_suspect}"
        )


if __name__ == "__main__":
    main()
