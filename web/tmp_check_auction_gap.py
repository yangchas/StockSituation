import json
from datetime import datetime
import redis

r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

def norm_pct(v):
    try:
        x = float(v)
    except Exception:
        return 0.0
    return x * 100.0 if abs(x) <= 1.0 else x

d_compact = datetime.now().strftime('%Y%m%d')
d_dash = datetime.now().strftime('%Y-%m-%d')

raw = None
source = 'none'

raw = r.hget(f'market:auction:{d_compact}:0925', 'top_amount')
if raw:
    source = '0925'

if not raw:
    latest = r.hgetall(f'market:auction:{d_compact}:latest')
    tag = latest.get('tag') if latest else None
    if tag:
        raw = r.hget(f'market:auction:{d_compact}:{tag}', 'top_amount')
        if raw:
            source = f'latest->{tag}'

if not raw:
    for tag in ('0925', '0924', '0920'):
        raw = r.hget(f'market:auction:{d_compact}:{tag}', 'top_amount')
        if raw:
            source = f'fallback->{tag}'
            break

if not raw:
    raw = r.get(f'market:auction:{d_dash}:0925')
    if raw:
        source = 'replay-string'

print('SOURCE', source)
if not raw:
    print('NO_AUCTION_DATA')
    raise SystemExit(0)

arr = json.loads(raw)
items = []
for it in arr:
    if not isinstance(it, dict):
        continue
    code = str(it.get('symbol') or it.get('code') or '').strip()
    if len(code) != 6:
        continue
    items.append({
        'code': code,
        'auc_chg': norm_pct(it.get('change_pct', it.get('bid_change_pct', 0))),
        'bid_amt': float(it.get('bid_amount_yuan', it.get('bid_amount', 0)) or 0),
        'auc_amt': float(it.get('auction_amount_yuan', it.get('auction_amount', it.get('amount', 0))) or 0),
    })

items.sort(key=lambda x: x['auc_amt'], reverse=True)
items = items[:120]

quote_ok = 0
fades = []
rises = []
for it in items:
    q = r.hgetall(f"stock:quote:{it['code']}")
    if not q:
        continue
    quote_ok += 1
    cur = norm_pct(q.get('change_pct', q.get('change', 0)))
    gap = cur - it['auc_chg']
    if it['auc_chg'] >= 5 and gap <= -4:
        fades.append((it['code'], it['auc_chg'], cur, gap, it['bid_amt']))
    if it['auc_chg'] <= 1 and gap >= 3 and cur > 0:
        rises.append((it['code'], it['auc_chg'], cur, gap, it['bid_amt']))

print('TOP_N', len(items), 'QUOTE_OK', quote_ok)
print('AUC>=5', sum(1 for x in items if x['auc_chg'] >= 5), 'AUC<=1', sum(1 for x in items if x['auc_chg'] <= 1))
print('FADE', len(fades), 'RISE', len(rises))
for s in fades[:10]:
    print('FADE_SIG', s[0], f'auc={s[1]:.2f}', f'cur={s[2]:.2f}', f'gap={s[3]:.2f}', f'bid={s[4]:.0f}')
for s in rises[:10]:
    print('RISE_SIG', s[0], f'auc={s[1]:.2f}', f'cur={s[2]:.2f}', f'gap={s[3]:.2f}', f'bid={s[4]:.0f}')
