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

def load_auction(today_dash: str):
    d = today_dash.replace('-', '')
    raw = r.hget(f'market:auction:{d}:0925', 'top_amount')
    if raw:
        return json.loads(raw), f'hash:market:auction:{d}:0925'
    latest = r.hgetall(f'market:auction:{d}:latest')
    tag = latest.get('tag') if latest else None
    if tag:
        raw = r.hget(f'market:auction:{d}:{tag}', 'top_amount')
        if raw:
            return json.loads(raw), f'latest:{tag}'
    for tag in ('0925', '0924', '0920'):
        raw = r.hget(f'market:auction:{d}:{tag}', 'top_amount')
        if raw:
            return json.loads(raw), f'fallback:{tag}'
    raw = r.get(f'market:auction:{today_dash}:0925')
    if raw:
        return json.loads(raw), 'replay:string'
    return [], 'none'

def load_recent_volatile_codes(limit=2000):
    codes = set()
    try:
        arr = r.zrevrange('stock:volatile_pool', 0, limit-1)
        for s in arr:
            try:
                d = json.loads(s)
            except Exception:
                continue
            c = str(d.get('symbol') or '').strip()
            if len(c) == 6:
                codes.add(c)
    except Exception:
        pass
    return codes

today = datetime.now().strftime('%Y-%m-%d')
arr, source = load_auction(today)
items = []
for it in arr:
    if not isinstance(it, dict):
        continue
    code = str(it.get('symbol') or it.get('code') or '').strip()
    if len(code) != 6:
        continue
    auc = norm_pct(it.get('change_pct', it.get('bid_change_pct', 0)))
    bid = float(it.get('bid_amount_yuan', it.get('bid_amount', 0)) or 0)
    aamt = float(it.get('auction_amount_yuan', it.get('auction_amount', it.get('amount', 0))) or 0)
    items.append({'code': code, 'auc': auc, 'bid': bid, 'aamt': aamt})
items.sort(key=lambda x: x['aamt'], reverse=True)
items = items[:200]

vol_codes = load_recent_volatile_codes()

rows = []
for it in items:
    q = r.hgetall(f"stock:quote:{it['code']}")
    if not q:
        continue
    cur = norm_pct(q.get('change_pct', q.get('change', 0)))
    gap = cur - it['auc']
    rows.append({**it, 'cur': cur, 'gap': gap, 'in_volatile': int(it['code'] in vol_codes)})

if not rows:
    print('NO_ROWS')
    raise SystemExit(0)

fade = [x for x in rows if x['auc'] >= 5 and x['gap'] <= -4]
rise = [x for x in rows if x['auc'] <= 1 and x['gap'] >= 3 and x['cur'] > 0]

high_auc = [x for x in rows if x['auc'] >= 5]
low_auc = [x for x in rows if x['auc'] <= 1]

high_auc_non_fade = [x for x in high_auc if x not in fade]
low_auc_non_rise = [x for x in low_auc if x not in rise]

def avg(xs, k):
    return sum(x[k] for x in xs) / len(xs) if xs else 0.0

def ratio(xs, k):
    return sum(x[k] for x in xs) / len(xs) if xs else 0.0

print('DATE', today)
print('SOURCE', source)
print('AUCTION_TOP', len(items), 'QUOTE_OK', len(rows))
print('HIGH_AUC(>=5)', len(high_auc), 'LOW_AUC(<=1)', len(low_auc))
print('FADE', len(fade), 'RISE', len(rise))
print('FADE_avg_cur', round(avg(fade,'cur'),2), 'HIGH_AUC_non_FADE_avg_cur', round(avg(high_auc_non_fade,'cur'),2))
print('RISE_avg_cur', round(avg(rise,'cur'),2), 'LOW_AUC_non_RISE_avg_cur', round(avg(low_auc_non_rise,'cur'),2))
print('FADE_in_volatile_ratio', round(ratio(fade,'in_volatile'),3), 'RISE_in_volatile_ratio', round(ratio(rise,'in_volatile'),3))
print('BASE_in_volatile_ratio', round(ratio(rows,'in_volatile'),3))

for t, xs in [('FADE', fade), ('RISE', rise)]:
    xs = sorted(xs, key=lambda x: abs(x['gap']), reverse=True)[:8]
    for x in xs:
        print(t, x['code'], f"auc={x['auc']:.2f}", f"cur={x['cur']:.2f}", f"gap={x['gap']:.2f}", f"bid={x['bid']:.0f}")
