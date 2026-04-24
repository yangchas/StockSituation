import redis
import json
import pandas as pd
import codecs

def norm_pct(v):
    try:
        x = float(v)
    except:
        return 0.0
    return x * 100.0 if abs(x) <= 1.0 else x

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=False)

    date = '20260225'
    raw = r.hget(f'market:auction:{date}:0925'.encode(), b'top_amount')
    if not raw:
        raw = r.get(f'market:auction:2026-02-25:0925'.encode())
    if not raw:
        print("No auction data")
        return

    items = json.loads(raw.decode('utf-8'))

    plate_map = {}
    try:
        df = pd.read_csv('/root/work/web/data/个股板块.csv', encoding='gbk')
        for _, row in df[['代码', '板块']].dropna().iterrows():
            c = str(row['代码']).strip().zfill(6)
            p = str(row['板块']).strip()
            plate_map.setdefault(c, []).append(p)
    except:
        pass

    plate_map_raw = r.hgetall(b"config:plate_mapping:s2p")
    for k, v in plate_map_raw.items():
        try:
            plates_list = json.loads(v.decode())
            code = k.decode()
            if code not in plate_map:
                plate_map[code] = []
            for p in plates_list:
                if p not in plate_map[code]:
                    plate_map[code].append(p)
        except:
            pass

    rows = []
    for it in items:
        if not isinstance(it, dict): continue
        code = str(it.get('symbol') or it.get('code') or '').strip()
        if len(code) != 6: continue
        
        auc = norm_pct(it.get('change_pct', it.get('bid_change_pct', 0)))
        bid = float(it.get('bid_amount_yuan', it.get('bid_amount', 0)) or 0)
        aamt = float(it.get('auction_amount_yuan', it.get('amount', 0)) or 0)
        
        q_raw = r.hgetall(f"stock:quote:{code}".encode())
        q = {k.decode(): v.decode() for k,v in q_raw.items()} if q_raw else {}
        
        name = q.get('name', '')
        if not name: name = it.get('name', code)
            
        cur = norm_pct(q.get('change_pct', q.get('change', auc)))
            
        gap = cur - auc
        
        plates = plate_map.get(code, [])
        safe_plates = [p for p in plates if isinstance(p, str) and not ('昨日' in p)]

        rows.append({
            'code': code,
            'name': name,
            'auc': auc,
            'cur': cur,
            'gap': gap,
            'aamt': aamt,
            'bid': bid,
            'plates': safe_plates
        })

    rows.sort(key=lambda x: x['aamt'], reverse=True)

    md = ["# 2026-02-25 盘后竞价预期差分析 (按板块分类)\n\n"]
    md.append(f"共加载 {len(rows)} 只竞价样本股。\n")
    md.append("*涨跌幅已修正为真实百分比格式（10%为涨停）。预期差(Gap) = 盘后涨幅 - 竞价涨幅。*\n\n")

    strong_bids = [x for x in rows if x['bid'] > x['aamt'] * 1.5 and x['aamt'] > 10000000]
    strong_bids.sort(key=lambda x: x['bid'], reverse=True)
    md.append("## 💪 强封单 (封单>竞价额1.5倍 且 竞价额>1000万)\n")
    md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差(日内) | 竞价(万) | 封单(万) | 所属板块 |\n")
    md.append("|---|---|---|---|---|---|---|---|\n")
    for x in strong_bids[:15]:
        plates_str = ",".join(x['plates'][:2])
        md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {x['bid']/10000:.0f}万 | {plates_str} |\n")

    plate_stats = {}
    
    # Process all 1000 rows for plate stats
    for x in rows: 
        # If no plate, put in '未分类'
        plates = x['plates'] if x['plates'] else ['未分类']
        primary_plate = plates[0]
        
        if primary_plate not in plate_stats:
            plate_stats[primary_plate] = {'count': 0, 'gap_sum': 0.0, 'up': 0, 'down': 0, 'stocks': []}
        plate_stats[primary_plate]['count'] += 1
        plate_stats[primary_plate]['gap_sum'] += x['gap']
        if x['gap'] > 0: plate_stats[primary_plate]['up'] += 1
        elif x['gap'] < 0: plate_stats[primary_plate]['down'] += 1
        plate_stats[primary_plate]['stocks'].append(x)

    valid_plates = []
    for p, stats in plate_stats.items():
        if stats['count'] >= 1:
            avg_gap = stats['gap_sum'] / stats['count']
            win_rate = stats['up'] / stats['count']
            
            # Sort stocks in this plate by auction amount
            sorted_stocks = sorted(stats['stocks'], key=lambda x: x['aamt'], reverse=True)
            top_stocks = sorted(stats['stocks'], key=lambda x: x['gap'], reverse=True)[:3]
            top_str = "<br>".join([f"{s['name']}({s['gap']:+.1f}%)" for s in top_stocks])
            valid_plates.append({
                'plate': p, 'count': stats['count'], 'avg_gap': avg_gap, 
                'win_rate': win_rate, 'top_str': top_str, 'all_stocks': sorted_stocks
            })

    # Sort plates by average gap
    valid_plates.sort(key=lambda x: x['avg_gap'], reverse=True)

    md.append("\n## 📊 板块概览与核心日内预期差\n")
    md.append("| 板块 | 股票数 | 平均预期差 | 正反馈率 | 领涨核心股 (日内Gap) |\n")
    md.append("|---|---|---|---|---|\n")
    for p in valid_plates:
        md.append(f"| {p['plate']} | {p['count']} | **{p['avg_gap']:+.2f}%** | {p['win_rate']:.0%} | {p['top_str']} |\n")

    md.append("\n## 📋 全量竞价预期差明细 (按板块汇总, 共 1000 只)\n")
    for p in valid_plates:
        md.append(f"\n### 板块: {p['plate']} (共 {p['count']} 只，平均预期差 {p['avg_gap']:+.2f}%)\n")
        md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差(日内) | 竞价额(万) | 封单(万) |\n")
        md.append("|---|---|---|---|---|---|---|\n")
        for x in p['all_stocks']:
            md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {x['bid']/10000:.0f}万 |\n")

    with codecs.open('remote_report.md', 'w', 'utf-8') as f:
        f.writelines(md)

if __name__ == "__main__":
    main()
