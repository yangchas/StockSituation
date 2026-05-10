# t1_v2 Live Acceptance Checklist

This checklist is for the next live trading session. It verifies whether
`t1_v2` can replace the old `t1.cpp` market-data path without breaking auction,
opening, Redis consumers, or strategy output.

## Before 09:15

- Build the live binary on the server:

```bash
cd /root/work/C/t1_v2
./make.sh --out=/tmp/t1_v2_all_live
```

- Start the binary with no extra arguments. No-arg mode must be live RabbitMQ
  mode with Redis and TDengine writes enabled.

```bash
/tmp/t1_v2_all_live
```

- Confirm CPU and memory are stable before auction starts.
- Confirm Redis is responsive:

```bash
redis-cli PING
```

## 09:15-09:19

- Confirm RabbitMQ consumption has started and no backlog grows abnormally.
- Confirm `q2:*` keys are being updated for active equities.
- Confirm index pollution is not written into stock aliases, especially:

```bash
redis-cli HGETALL q2:000001
```

Expected: if present as an equity quote, `mk` should be `sz`, not `sh`.

## 09:20 Snapshot

- Confirm the 09:20 preview key exists:

```bash
redis-cli EXISTS market:auction:$(date +%Y%m%d):0920
redis-cli HGET market:auction:$(date +%Y%m%d):0920 summary
redis-cli HGET market:auction:$(date +%Y%m%d):0920 top_amount
```

- Check that `market:auction:$(date +%Y%m%d):latest` points to or contains the
  latest auction snapshot.
- Confirm auction amount is not zero for top rows.

## 09:24 Snapshot

- Confirm the 09:24 preview key exists:

```bash
redis-cli EXISTS market:auction:$(date +%Y%m%d):0924
redis-cli HGET market:auction:$(date +%Y%m%d):0924 top_amount
```

- Compare 09:20 and 09:24 top rows manually for several symbols. Amount after
  09:20 should generally increase, not decrease.
- Confirm auction seal semantics:
  - Auction matched amount uses level 1 matched quantity.
  - Auction unmatched seal queue uses level 2.

## 09:25 Anchor

- Confirm the final auction key and anchor exist:

```bash
redis-cli EXISTS market:auction:$(date +%Y%m%d):0925
redis-cli EXISTS market:auction:anchor:$(date +%Y%m%d)
redis-cli HGET market:auction:$(date +%Y%m%d):0925 top_amount
```

- Run Python snapshot load:

```bash
cd /root/work
python - <<'PY'
from engine_next.runtime.intraday_data_hub import IntradayDataHub
hub = IntradayDataHub()
result = hub.load_auction_snapshots(__import__("datetime").date.today().isoformat())
print(result.source, len(result.rows), result.redis_keys_written)
print(sum(1 for row in result.rows if row.get("tag") == "0925" and row.get("previous_tag") == "0924"))
PY
```

Expected:
- `source=redis_snapshots`
- rows exist for `0920`, `0924`, and `0925`
- at least some `0925` rows have `amount_delta`, `amount_ratio`,
  `bid_amount_delta`, and `change_pct_delta`.

## 09:26 Strategy Output

- Run or inspect `engine_next` auction output.
- Required sections:
  - `【竞价总览】`
  - `【竞价结构】`
  - `【数据对撞】`
  - `【竞价边际】`
  - `【EAX预期差】`
  - `【昨日涨停反馈】`
  - `【梯队映射】`

- Audit whether every mentioned stock has open change, current change, and
  theme unless already grouped inside a theme bucket.

## 09:30-09:32 Opening Confirmation

- Confirm `q2` fields update after open:
  - `spd1m`
  - `amt2m`
  - `amt5m`
  - `vec3m`
  - `vec5m`
  - `ls`

- Confirm涨停/跌停 requires seal order:
  - Limit up requires price at limit and buy-side seal.
  - Limit down requires price at limit and sell-side seal.
  - Auction uses level 2 as unmatched seal queue.
  - Continuous trading uses level 1 as visible seal queue.

## Performance Watch

- Watch these for at least 30 minutes:

```bash
top -p $(pgrep -f t1_v2 | head -1),$(pgrep redis-server | head -1)
redis-cli INFO stats | egrep 'instantaneous_ops_per_sec|total_commands_processed'
redis-cli INFO memory | egrep 'used_memory_human|mem_fragmentation_ratio'
```

Acceptance targets:
- No stuck RabbitMQ consumption.
- Redis CPU does not return to old high-load behavior.
- No recurring pipeline flush failures.
- No TDengine `Sync not ready to propose` loop causing the process to stop
  consuming.

## Rollback

- Stop `t1_v2`.
- Restart old `t1.cpp` binary if live validation fails.
- Use the local git commit made before live validation as the code rollback
  point.
