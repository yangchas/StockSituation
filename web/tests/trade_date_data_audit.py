import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from engine_next.connectors import KaipanConnector, WencaiConnector
from engine_next.runtime.plate_mapping_registry import build_plate_candidates_from_reason
from web.services.trading_calendar_service import TradingCalendarService


def _normalize_symbol(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    return raw[-6:]


def _sample_symbols(rows: list[dict[str, Any]], *, limit: int = 6) -> list[str]:
    samples: list[str] = []
    for row in rows[:limit]:
        symbol = _normalize_symbol(row.get("symbol") or row.get("code") or "")
        lb_days = row.get("lb_days")
        plate = str(row.get("plate") or "").strip()
        if lb_days is not None:
            samples.append(f"{symbol}:{lb_days}B:{plate or '-'}")
        else:
            samples.append(symbol)
    return samples


def _plate_counter_from_limit_pool(
    connector: KaipanConnector,
    pool_rows: list[dict[str, Any]],
    *,
    trade_date: str,
    top_n: int = 10,
) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for row in pool_rows:
        symbol = _normalize_symbol(row.get("symbol") or "")
        pool_plate = str(row.get("plate") or "").strip()
        themes: list[str] = []
        if symbol:
            try:
                reason_rows = connector.normalize_ban_reasons(connector.fetch_ban_reasons(symbol))
            except Exception:
                reason_rows = []
            for reason_row in reason_rows:
                themes.extend(
                    build_plate_candidates_from_reason(
                        reason=str(reason_row.get("reason") or ""),
                        group_str=str(reason_row.get("group_str") or ""),
                        gnsm=str(reason_row.get("gnsm") or ""),
                    )
                )
        dedup = list(dict.fromkeys(str(item).strip() for item in themes if str(item).strip()))
        if not dedup and pool_plate:
            dedup = [pool_plate]
        for plate_name in dedup[:2]:
            counter[plate_name] += 1
    return counter.most_common(top_n)


async def build_trade_date_audit_report(
    *,
    trade_date: str,
    max_ban: int = 10,
) -> Dict[str, Any]:
    calendar = TradingCalendarService()
    previous_trade_date = calendar.get_previous_trading_day(trade_date)
    kaipan = KaipanConnector()
    wencai = WencaiConnector()

    today_limit_df = await wencai.fetch_limitup_with_lb_days(max_stocks=500)
    today_limit_rows = wencai.to_tdengine_rows("limit_truth", today_limit_df, trade_date)

    raw_yest_pool = await asyncio.to_thread(kaipan.fetch_yesterday_bans_pool, previous_trade_date, max_ban)
    yest_pool_rows = kaipan.to_tdengine_rows("yest_limit_pool", raw_yest_pool, previous_trade_date)

    raw_today_hot = await asyncio.to_thread(kaipan.fetch_hot_plates, trade_date)
    today_hot_rows = kaipan.to_tdengine_rows("hot_plates", raw_today_hot, trade_date)

    raw_yesterday_hot = await asyncio.to_thread(kaipan.fetch_hot_plates, previous_trade_date)
    yesterday_hot_rows = kaipan.to_tdengine_rows("hot_plates", raw_yesterday_hot, previous_trade_date)

    yest_pool_symbols = {_normalize_symbol(row.get("symbol") or "") for row in yest_pool_rows if row.get("symbol")}
    today_limit_symbols = {_normalize_symbol(row.get("symbol") or "") for row in today_limit_rows if row.get("symbol")}
    promoted = sorted(symbol for symbol in yest_pool_symbols & today_limit_symbols if symbol)

    today_limit_plate_top = _plate_counter_from_limit_pool(kaipan, yest_pool_rows, trade_date=previous_trade_date)

    return {
        "trade_date": trade_date,
        "previous_trade_date": previous_trade_date,
        "today_limit_truth": {
            "rows": len(today_limit_rows),
            "samples": _sample_symbols(today_limit_rows),
        },
        "today_hot_plates": {
            "rows": len(today_hot_rows),
            "samples": [
                f"{row.get('plate_name')}#rank={row.get('rank')}#hot={row.get('hot')}"
                for row in today_hot_rows[:6]
            ],
        },
        "yesterday_limit_pool": {
            "rows": len(yest_pool_rows),
            "samples": _sample_symbols(yest_pool_rows),
        },
        "yesterday_hot_plates": {
            "rows": len(yesterday_hot_rows),
            "samples": [
                f"{row.get('plate_name')}#rank={row.get('rank')}#hot={row.get('hot')}"
                for row in yesterday_hot_rows[:6]
            ],
        },
        "yesterday_limit_today_promoted": {
            "count": len(promoted),
            "samples": promoted[:12],
        },
        "yesterday_limit_plate_top": today_limit_plate_top,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and audit one trade date using existing Kaipan/Wencai helpers")
    parser.add_argument("--date", required=True, help="Trade date to audit, e.g. 2026-04-28")
    parser.add_argument("--max-ban", type=int, default=10, help="Yesterday-limit board depth to fetch from Kaipan")
    args = parser.parse_args(argv)

    report = asyncio.run(
        build_trade_date_audit_report(
            trade_date=args.date,
            max_ban=args.max_ban,
        )
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
