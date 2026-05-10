#!/usr/bin/env bash
set -euo pipefail

TRADE_DATE="${1:-${TRADE_DATE:-$(date -d 'last friday' +%F)}}"
REPLAY_TABLE="${REPLAY_TABLE:-stock_data}"
BIN="${BIN:-/tmp/t1_v2_all_live}"
TOP_N="${REDIS_AUCTION_TOP_N:-200}"
START_TIME="${REPLAY_START_TIME:-$TRADE_DATE 09:20:00}"
END_TIME="${REPLAY_END_TIME:-$TRADE_DATE 09:25:06}"
SPEED="${REPLAY_SPEED:-10000}"
DATE_TAG="${TRADE_DATE//-/}"

cd "$(dirname "$0")"

if [[ ! -x "$BIN" ]]; then
  ./make.sh --out="$BIN"
fi

echo "[verify] trade_date=$TRADE_DATE table=$REPLAY_TABLE bin=$BIN"
echo "[verify] clearing historical auction keys for $DATE_TAG"
redis-cli --scan --pattern "market:auction:${DATE_TAG}:09*" | xargs -r redis-cli DEL >/dev/null
redis-cli DEL \
  "market:auction:${DATE_TAG}:0920" \
  "market:auction:${DATE_TAG}:0924" \
  "market:auction:${DATE_TAG}:0925" \
  "market:auction:${DATE_TAG}:latest" \
  "market:auction:anchor:${DATE_TAG}" >/dev/null

echo "[verify] replaying $START_TIME -> $END_TIME"
REDIS_AUCTION_TOP_N="$TOP_N" \
REPLAY_START_TIME="$START_TIME" \
REPLAY_END_TIME="$END_TIME" \
REPLAY_SPEED="$SPEED" \
"$BIN" --replay --replay-table "$REPLAY_TABLE"

cd /root/work
python - "$TRADE_DATE" <<'PY'
import sys
from collections import Counter

from engine_next.runtime.intraday_data_hub import IntradayDataHub

trade_date = sys.argv[1]
hub = IntradayDataHub()
result = hub.load_auction_snapshots(trade_date)
counts = Counter(str(row.get("tag") or "") for row in result.rows)
delta_rows = [
    row for row in result.rows
    if row.get("tag") == "0925" and row.get("previous_tag") == "0924"
]

print(f"[verify] source={result.source} rows={len(result.rows)} keys={result.redis_keys_written}")
print(f"[verify] counts={dict(counts)} delta_0925={len(delta_rows)}")

missing = [tag for tag in ("0920", "0924", "0925") if counts.get(tag, 0) <= 0]
if missing:
    raise SystemExit(f"missing auction tags: {missing}")
if not delta_rows:
    raise SystemExit("missing 0924->0925 delta rows")

required = ("amount_delta", "amount_ratio", "bid_amount_delta", "change_pct_delta")
bad = [row.get("symbol") for row in delta_rows[:20] if not all(field in row for field in required)]
if bad:
    raise SystemExit(f"delta fields missing in sample rows: {bad}")

top = max(delta_rows, key=lambda row: float(row.get("amount", 0.0) or 0.0))
print(
    "[verify] top_delta "
    f"symbol={top.get('symbol')} amount={top.get('amount')} "
    f"delta={top.get('amount_delta')} ratio={top.get('amount_ratio')} "
    f"change_delta={top.get('change_pct_delta')}"
)
PY

if ! redis-cli EXISTS "market:auction:anchor:${DATE_TAG}" | grep -q '^1$'; then
  echo "[verify] missing market:auction:anchor:${DATE_TAG}" >&2
  exit 1
fi

echo "[verify] auction replay verification passed"
