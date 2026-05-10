# t1_v2

`t1_v2` is the isolated high-performance replacement path for `C/t1.cpp`.

## Production

```bash
cd /root/work/C/t1_v2
./make.sh
/tmp/t1_v2_all_live
```

Default build opens all production features: zlib, protobuf, TDengine, Redis,
and RabbitMQ.

Default run is live mode: consume RabbitMQ, compute v2 metrics, write TDengine,
write Redis, then ack source messages after downstream success.

## Replay

```bash
/tmp/t1_v2_all_live --replay --start "2026-04-29 09:25:00" --end "2026-04-29 09:25:03"
```

Replay reads `stock_tick_v2` by default and writes Redis by default so Python
can consume the same `q2:/a2:/market:auction:` path as live mode. It does not
write TDengine during replay unless `--replay-write-tdengine` is explicitly set.

Use a test Redis namespace for write verification so production q2/a2 and
legacy auction anchor keys are not touched:

```bash
REDIS_Q2_PREFIX=test:q2: \
REDIS_A2_PREFIX=test:a2: \
REDIS_M2_PREFIX=test:m2: \
REDIS_LEGACY_AUCTION_PREFIX=test:market:auction: \
REDIS_LEGACY_ANCHOR_PREFIX=test:market:auction:anchor: \
/tmp/t1_v2_all_live --replay --replay-write-redis \
  --start "2026-04-29 09:25:00" --end "2026-04-29 09:25:03"
```

## Self-Test

`self-test` is an internal semantic check. It does not replace live validation;
it only verifies conversions, minute buckets, auction math, Redis command
formatting, and runtime ack/reject rules.

Run it manually before publishing a binary:

```bash
./make.sh --self-test
```

## Live Health Check

Read-only health check during the live session:

```bash
cd /root/work/C/t1_v2
bash live_health_check.sh
```

It checks the `t1_v2` process, Redis responsiveness, today's auction keys,
sample `q2` fields, and Python auction snapshot loading. It does not mutate
Redis or TDengine.
