import redis
import json
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
    
    # 1. Load Auction 09:25
    raw = r.hget(f'market:auction:{date}:0925'.encode(), b'top_amount')
    if not raw:
        print("No 0925 auction data.")
        return
    items = json.loads(raw.decode('utf-8'))
    snap_0925 = {str(it.get('symbol') or it.get('code'))[-6:]: it for it in items}

    # 2. Load Plates
    plate_map_raw = r.hgetall(b"config:plate_mapping:s2p")
    plate_map = {}
    for k, v in plate_map_raw.items():
        try: plate_map[k.decode()] = json.loads(v.decode())
        except: pass

    # 3. Process each stock against current/close status
    stocks_meta = []
    
    for code, it_25 in snap_0925.items():
        if len(code) != 6: continue
        
        pct_25 = norm_pct(it_25.get('change_pct', 0))
        amt_25 = float(it_25.get('auction_amount_yuan', it_25.get('amount', 0)) or 0)
        
        q_raw = r.hgetall(f"stock:quote:{code}".encode())
        if not q_raw: continue
        q = {k.decode(): v.decode() for k,v in q_raw.items()}
        
        name = q.get('name', '') or it_25.get('name', code)
        cur_pct = norm_pct(q.get('change_pct', q.get('change', pct_25)))
        
        plates = plate_map.get(code, ['未分类'])
        primary_plate = plates[0] if plates else '未分类'
        
        stocks_meta.append({
            'code': code,
            'name': name,
            'auc_pct': pct_25,
            'cur_pct': cur_pct,
            'amt_25': amt_25,
            'gap': cur_pct - pct_25, # 日内走势
            'plate': primary_plate
        })

    # Sort & Filter
    # 1. 竞价最强核心股验证 (竞价很高，最后怎么走的？)
    strong_auction = sorted([x for x in stocks_meta if x['auc_pct'] > 5.0 and x['amt_25'] > 20000000], key=lambda x: x['amt_25'], reverse=True)
    
    # 2. 深核反转英雄榜 (早盘水下，盘中直接V型反弹)
    v_reversals = sorted([x for x in stocks_meta if x['auc_pct'] < -3.0 and x['cur_pct'] > 0.0], key=lambda x: x['cur_pct'], reverse=True)
    
    # 3. 板块日内异动全景 (按照板块日内Gap总和排序，找出隐藏盘中暗线)
    plate_stats = {}
    for x in stocks_meta:
        p = x['plate']
        if p not in plate_stats:
            plate_stats[p] = {'count': 0, 'gap_sum': 0.0, 'up': 0}
        plate_stats[p]['count'] += 1
        plate_stats[p]['gap_sum'] += x['gap']
        if x['gap'] > 0: plate_stats[p]['up'] += 1

    valid_plates = []
    for p, stats in plate_stats.items():
        if stats['count'] >= 2:
            avg_gap = stats['gap_sum'] / stats['count']
            valid_plates.append({'plate': p, 'avg_gap': avg_gap, 'up_ratio': stats['up']/stats['count']})
            
    valid_plates.sort(key=lambda x: x['avg_gap'], reverse=True)

    # 4. Generate Report
    md = ["# 【阶段二】 开盘后动向与剧本验证 (Intraday Verification)\n\n"]
    md.append("*分析开盘后的真实承接，剔除诱多陷阱，挖掘盘中资金轮动与深水反包节点。*\n\n")

    md.append("## 🛡️ 竞价最强核心股：真强延续 vs 诱多出货\n")
    md.append("| 代码 | 名称 | 竞价开幅 | 收盘(当前) | 日内振幅(真伪) | 竞价额(万) | 所属板块 |\n")
    md.append("|---|---|---|---|---|---|---|\n")
    for x in strong_auction[:15]:
        trend = "✅ 延续强势" if x['gap'] > -2.0 else "❌ 遭遇砸盘"
        md.append(f"| {x['code']} | {x['name']} | {x['auc_pct']:.2f}% | **{x['cur_pct']:.2f}%** | {x['gap']:+.2f}% ({trend}) | {x['amt_25']/10000:.0f}万 | {x['plate']} |\n")

    md.append("\n## 🚀 深水反核英雄榜 (深水V型捞月)\n")
    md.append("| 代码 | 名称 | 竞价水下 | 收盘(当前) | 日内拉升 | 所属板块 |\n")
    md.append("|---|---|---|---|---|---|\n")
    for x in v_reversals[:15]:
        md.append(f"| {x['code']} | {x['name']} | {x['auc_pct']:.2f}% | **{x['cur_pct']:.2f}%** | **+{x['gap']:.2f}%** | {x['plate']} |\n")

    md.append("\n## 🌐 日内板块异动暗线全景 (超出竞价预期)\n")
    md.append("| 板块名称 | 板块概念内股票数 | 盘中正反馈率 | 盘中平均拉升(超预期幅度) |\n")
    md.append("|---|---|---|---|\n")
    for p in valid_plates[:10]:
        md.append(f"| **{p['plate']}** | {plate_stats[p['plate']]['count']} | {p['up_ratio']:.0%} | **{p['avg_gap']:+.2f}%** |\n")

    with codecs.open('intraday_verification_report.md', 'w', 'utf-8') as f:
        f.writelines(md)

if __name__ == "__main__":
    main()
