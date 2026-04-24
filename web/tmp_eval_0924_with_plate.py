import json
from datetime import datetime
import redis
import pandas as pd
from pathlib import Path

r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

def norm_pct(v):
    try:
        x = float(v)
    except Exception:
        return 0.0
    return x * 100.0 if abs(x) <= 1.0 else x

def load_auction_0924(dash):
    dc = dash.replace('-', '')
    raw = r.hget(f'market:auction:{dc}:0924', 'top_amount')
    if not raw:
        return []
    arr = json.loads(raw)
    out = []
    for it in arr:
        if not isinstance(it, dict):
            continue
        code = str(it.get('symbol') or it.get('code') or '').strip()
        if len(code) != 6:
            continue
        out.append({
            'code': code,
            'auc': norm_pct(it.get('change_pct', it.get('bid_change_pct', 0))),
            'bid': float(it.get('bid_amount_yuan', it.get('bid_amount', 0)) or 0),
            'auc_amt': float(it.get('auction_amount_yuan', it.get('auction_amount', it.get('amount', 0))) or 0),
        })
    out.sort(key=lambda x: x['auc_amt'], reverse=True)
    return out[:200]

def load_plate_map():
    # try common project paths
    paths = [
        Path('/root/work/web/data/个股板块.csv'),
        Path('/root/work/web/data/stock_plate.csv'),
        Path('/root/work/web/data/个股板块关系.csv'),
    ]
    for p in paths:
        if not p.exists():
            continue
        for enc in ('utf-8', 'gbk', 'utf-8-sig'):
            try:
                df = pd.read_csv(p, encoding=enc)
                cols = {c: c for c in df.columns}
                code_col = None
                plate_col = None
                for c in df.columns:
                    lc = str(c).lower()
                    if code_col is None and (('代码' in str(c)) or ('stock' in lc and 'code' in lc) or lc in ('code', '股票代码')):
                        code_col = c
                    if plate_col is None and (('板块' in str(c)) or ('plate' in lc) or ('行业' in str(c))):
                        plate_col = c
                if code_col and plate_col:
                    m = {}
                    for _, row in df[[code_col, plate_col]].dropna().iterrows():
                        code = str(row[code_col]).strip()
                        plate = str(row[plate_col]).strip()
                        if code and plate:
                            m.setdefault(code, set()).add(plate)
                    return {k: sorted(v) for k, v in m.items()}, str(p)
            except Exception:
                continue
    return {}, ''

def top_plate_from_rank(today):
    zkey = f'rank:plate_profile:{today}'
    dkey = f'rank:plate_profile:details:{today}'
    rows = r.zrevrange(zkey, 0, 4, withscores=True)
    out = []
    for pid, score in rows:
        d = r.hget(dkey, pid)
        name = pid
        base = None
        if d:
            try:
                j = json.loads(d)
                name = j.get('name', pid)
                base = j.get('base_change_pct')
            except Exception:
                pass
        out.append({'plate_id': pid, 'name': name, 'score': round(float(score), 3), 'base_change_pct': base})
    return out

today = '2026-02-12'
items = load_auction_0924(today)
if not items:
    print(json.dumps({'ok': False, 'reason': 'no market:auction:*:0924'}, ensure_ascii=False))
    raise SystemExit

plate_map, plate_src = load_plate_map()

rows = []
for it in items:
    q = r.hgetall(f"stock:quote:{it['code']}")
    if not q:
        continue
    cur = norm_pct(q.get('change_pct', q.get('change', 0)))
    gap = cur - it['auc']
    row = dict(it)
    row['cur'] = cur
    row['gap'] = gap
    row['name'] = q.get('name', '')
    row['plates'] = plate_map.get(it['code'], [])[:3]
    rows.append(row)

fade = [x for x in rows if x['auc'] >= 5 and x['gap'] <= -4]
rise = [x for x in rows if x['auc'] <= 1 and x['gap'] >= 3 and x['cur'] > 0]

fade.sort(key=lambda x: abs(x['gap']), reverse=True)
rise.sort(key=lambda x: abs(x['gap']), reverse=True)

plate_cnt = {}
for x in fade + rise:
    for p in x.get('plates', []):
        plate_cnt[p] = plate_cnt.get(p, 0) + 1
plate_hot = sorted(plate_cnt.items(), key=lambda kv: kv[1], reverse=True)[:8]

# effectiveness
high = [x for x in rows if x['auc'] >= 5]
low = [x for x in rows if x['auc'] <= 1]
high_non = [x for x in high if x not in fade]
low_non = [x for x in low if x not in rise]

def avg(arr, k):
    return sum(x[k] for x in arr)/len(arr) if arr else 0.0
fade_adv = avg(high_non, 'cur') - avg(fade, 'cur')
rise_adv = avg(rise, 'cur') - avg(low_non, 'cur')
eff = max(0.0, min(1.0, (fade_adv + rise_adv) / 20.0))

res = {
    'ok': True,
    'date': today,
    'auction_source': f'market:auction:{today.replace("-","")}:0924',
    'plate_map_source': plate_src or 'not_found',
    'summary': {
        'sample': len(rows),
        'fade_count': len(fade),
        'rise_count': len(rise),
        'fade_adv': round(fade_adv, 3),
        'rise_adv': round(rise_adv, 3),
        'effectiveness': round(eff, 4),
    },
    'stock_analysis': {
        'fade_top': [
            {
                'code': x['code'], 'name': x.get('name',''), 'auc_pct': round(x['auc'],2),
                'cur_pct': round(x['cur'],2), 'gap_pct': round(x['gap'],2),
                'bid_amt_yuan': int(x['bid']), 'plates': x.get('plates', [])
            } for x in fade[:10]
        ],
        'rise_top': [
            {
                'code': x['code'], 'name': x.get('name',''), 'auc_pct': round(x['auc'],2),
                'cur_pct': round(x['cur'],2), 'gap_pct': round(x['gap'],2),
                'bid_amt_yuan': int(x['bid']), 'plates': x.get('plates', [])
            } for x in rise[:10]
        ]
    },
    'plate_analysis': {
        'signal_hot_plates': [{'plate': p, 'count': c} for p, c in plate_hot],
        'rank_plate_profile_top5': top_plate_from_rank(today),
    }
}
print(json.dumps(res, ensure_ascii=False, indent=2))
