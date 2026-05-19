import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from engine_next.connectors import KaipanConnector
from engine_next.runtime.plate_mapping_registry import build_plate_candidates_from_reason


def compare_yest_limit_and_ban_reason(
    *,
    trade_date: str,
    symbol: str = "603278",
    max_ban: int = 10,
) -> Dict[str, Any]:
    connector = KaipanConnector()
    normalized_symbol = str(symbol).strip()[-6:]

    raw_pool = connector.fetch_yesterday_bans_pool(trade_date, max_ban=max_ban)
    normalized_pool = connector.to_tdengine_rows("yest_limit_pool", raw_pool, trade_date)
    pool_row = next((row for row in normalized_pool if str(row.get("symbol") or "")[-6:] == normalized_symbol), None)

    reasons = connector.fetch_ban_reasons(normalized_symbol)
    normalized_reasons = connector.normalize_ban_reasons(reasons)
    writebacks = connector.build_runtime_writebacks(
        reasons,
        trade_date,
        existing_themes=(),
        fallback_plate="",
        pool_plate=str((pool_row or {}).get("plate") or ""),
    )

    reason_candidates = []
    reason_heads = []
    for row in normalized_reasons:
        reason_text = str(row.get("reason") or "").strip()
        if reason_text:
            reason_heads.append(reason_text.split("；", 1)[0].strip())
        reason_candidates.extend(
            build_plate_candidates_from_reason(
                reason=reason_text,
                group_str=str(row.get("group_str") or ""),
                gnsm=str(row.get("gnsm") or ""),
            )
        )

    # Keep insertion order while removing duplicates.
    dedup_reason_candidates = list(dict.fromkeys(reason_candidates))

    return {
        "trade_date": trade_date,
        "symbol": normalized_symbol,
        "pool_found": pool_row is not None,
        "pool_entry": pool_row,
        "ban_reason_count": len(normalized_reasons),
        "ban_reason_heads": reason_heads,
        "ban_reason_candidates": dedup_reason_candidates,
        "runtime_primary_plate": writebacks.get("market:stock_plate", {}).get(normalized_symbol),
        "runtime_theme_list": writebacks.get("config:plate_mapping:s2p", {}).get(normalized_symbol, []),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Compare Kaipan yesterday-limit plate with Kaipan ban-reason themes")
    parser.add_argument("--date", required=True, help="Yesterday-limit trade date, e.g. 2026-05-07")
    parser.add_argument("--symbol", default="603278", help="Stock symbol, default is 603278 (大业股份)")
    parser.add_argument("--max-ban", type=int, default=10, help="Max board depth to scan in yesterday-limit pool")
    args = parser.parse_args(argv)

    result = compare_yest_limit_and_ban_reason(
        trade_date=args.date,
        symbol=args.symbol,
        max_ban=args.max_ban,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
