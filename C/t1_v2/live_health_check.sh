#!/usr/bin/env bash
set -euo pipefail

TRADE_DATE="${1:-${TRADE_DATE:-$(date +%F)}}"
DATE_TAG="${TRADE_DATE//-/}"
Q2_PREFIX="${REDIS_Q2_PREFIX:-q2:}"

cd /root/work

echo "[health] trade_date=$TRADE_DATE date_tag=$DATE_TAG q2_prefix=$Q2_PREFIX"

echo "[health] redis"
redis-cli PING

echo "[health] t1_v2 process"
if pgrep -af "t1_v2|t1_v2_all_live" >/tmp/t1_v2_health_pids.$$; then
  cat /tmp/t1_v2_health_pids.$$
  PID="$(awk 'NR==1 {print $1}' /tmp/t1_v2_health_pids.$$)"
  if [[ -n "${PID:-}" ]]; then
    ps -o pid,ppid,stat,%cpu,%mem,etime,cmd -p "$PID"
  fi
else
  echo "[health][warn] no t1_v2 process found"
fi
rm -f /tmp/t1_v2_health_pids.$$

echo "[health] redis auction keys"
for tag in 0920 0924 0925 latest; do
  key="market:auction:${DATE_TAG}:${tag}"
  exists="$(redis-cli EXISTS "$key")"
  echo "  $key exists=$exists"
  if [[ "$exists" == "1" ]]; then
    redis-cli HGET "$key" summary | python -c 'import json,sys; raw=sys.stdin.read().strip(); print(raw[:240] if raw else "-")'
  fi
done
anchor_key="market:auction:anchor:${DATE_TAG}"
echo "  $anchor_key exists=$(redis-cli EXISTS "$anchor_key")"

echo "[health] q2 sample"
python - "$Q2_PREFIX" <<'PY'
import sys
import redis

prefix = sys.argv[1]
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
samples = ("000001", "300001", "600000", "688001")
for symbol in samples:
    key = f"{prefix}{symbol}"
    row = r.hgetall(key)
    if not row:
        print(f"  {key}: missing")
        continue
    fields = {name: row.get(name, "-") for name in (
        "mk", "px", "pc", "amt", "vol", "ts", "ph", "ls",
        "spd1m", "amt2m", "amt5m", "vec3m", "vec5m", "am", "br", "ar",
    )}
    print(f"  {key}: {fields}")
PY

echo "[health] python auction snapshot loader"
python - "$TRADE_DATE" <<'PY'
import sys
from collections import Counter

from engine_next.runtime.intraday_data_hub import IntradayDataHub

trade_date = sys.argv[1]
hub = IntradayDataHub()
result = hub.load_auction_snapshots(trade_date)
counts = Counter(str(row.get("tag") or "") for row in result.rows)
delta_0925 = sum(1 for row in result.rows if row.get("tag") == "0925" and row.get("previous_tag") == "0924")
print(f"  source={result.source} rows={len(result.rows)} counts={dict(counts)} delta_0925={delta_0925}")
if result.rows:
    top = max(result.rows, key=lambda row: float(row.get("amount", 0.0) or 0.0))
    print(
        "  top "
        f"tag={top.get('tag')} symbol={top.get('symbol')} amount={top.get('amount')} "
        f"amount_delta={top.get('amount_delta', '-')}"
    )
PY

echo "[health] done"
