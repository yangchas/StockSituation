import redis
import json
import codecs
import time
from datetime import datetime, timedelta
import os
import sys

# Hack to import analyzer remotely if possible
sys.path.append('/root/work')
sys.path.append('/root/work/ai')
try:
    from API.StockAnalyzer import StockAnalyzer
except:
    StockAnalyzer = None

def get_yesterday_bans():
    if not StockAnalyzer: return {}
    analyzer = StockAnalyzer()
    # Find last trading day
    d = datetime.now()
    if d.weekday() == 0:
        d = d - timedelta(days=3)
    elif d.weekday() == 6:
        d = d - timedelta(days=2)
    else:
        d = d - timedelta(days=1)
    
    date_str = d.strftime("%Y%m%d")
    
    try:
        res = analyzer.get_his_bans(date_str)
        list_data = res.get('list', res.get('List', res.get('info', [])))
        ban_map = {}
        for row in list_data:
            code = ""
            reason = ""
            desc = ""
            if isinstance(row, list) and len(row) > 12:
                code = str(row[0])
                desc = str(row[12])
            elif isinstance(row, dict):
                code = str(row.get('StockID', row.get('code', '')))
                desc = str(row.get('Reason', '')) + " " + str(row.get('Detail', ''))
            
            if code:
                if len(code) > 6: code = code[-6:]
                ban_map[code] = desc
        return ban_map
    except:
        return {}

def norm_pct(v):
    try:
        x = float(v)
    except:
        return 0.0
    return x * 100.0 if abs(x) <= 1.0 else x

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=False)
    
    date = '20260225' # Hardcoded for backtest, ideally datetime.now().strftime("%Y%m%d")
    
    # Load 3 snapshots
    def load_snapshot(tag):
        raw = r.hget(f'market:auction:{date}:{tag}'.encode(), b'top_amount')
        if not raw: return {}
        try:
            items = json.loads(raw.decode('utf-8'))
            return {str(it.get('symbol') or it.get('code'))[-6:]: it for it in items}
        except:
            return {}
            
    snap_0920 = load_snapshot('0920')
    snap_0924 = load_snapshot('0924')
    snap_0925 = load_snapshot('0925')
    
    if not snap_0925:
        print("No 0925 data found.")
        return

    # Load plate map
    plate_map_raw = r.hgetall(b"config:plate_mapping:s2p")
    plate_map = {}
    for k, v in plate_map_raw.items():
        try:
            plate_map[k.decode()] = json.loads(v.decode())
        except: pass

    # get yesterday bans (only works if analyzer is reachable remotely)
    ban_map = get_yesterday_bans()
    
    results = []
    
    for code, it_25 in snap_0925.items():
        if len(code) != 6: continue
        
        name = it_25.get('name', '')
        if not name:
            q_raw = r.hgetall(f"stock:quote:{code}".encode())
            if q_raw:
                q = {k.decode(): v.decode() for k,v in q_raw.items()}
                name = q.get('name', code)
        
        pct_25 = norm_pct(it_25.get('change_pct', 0))
        amt_25 = float(it_25.get('auction_amount_yuan', it_25.get('amount', 0)) or 0)
        bid_25 = float(it_25.get('bid_amount_yuan', it_25.get('bid_amount', 0)) or 0)
        
        it_20 = snap_0920.get(code, {})
        pct_20 = norm_pct(it_20.get('change_pct', 0))
        
        it_24 = snap_0924.get(code, {})
        pct_24 = norm_pct(it_24.get('change_pct', 0))
        
        # Get yesterday's final price change to know background
        yest_pct = 0.0
        q_raw = r.hgetall(f"stock:kline:day:{code}".encode()) # Usually holds historical, if not we rely on quote
        # For simplicity, if ban_map has it, yesterday was quite strong. 
        # If not, let's assume it was weak unless we have actual K-line. 
        # (In a real system, you'd calculate yest_pct accurately from TDengine or Redis history)
        
        is_yest_limit_up = code in ban_map
        is_lanban = is_yest_limit_up and ("烂" in ban_map[code] or "炸" in ban_map[code] or "分歧" in ban_map[code])
        
        # 模式A：烂板/分歧转一致 (昨日烂板，今日高开且资金巨大)
        # 简化：只要昨天涨停，今天9:25高开>4%，且竞价额>1000万，且24到25没抢跑
        mode_a = is_yest_limit_up and pct_25 > 4.0 and amt_25 > 10000000 and (pct_25 >= pct_24 - 1.0)
        
        # 模式B：水下大跌弱转强 (这里我们近似用今天强行高开、爆量、非昨日涨停股代替)
        mode_b = not is_yest_limit_up and pct_25 > 2.0 and amt_25 > 20000000 and bid_25 > amt_25 * 0.5
        
        # 模式C：诱多强转弱 (9:20高开，9:25突然跳水)
        mode_c = pct_20 > 5.0 and pct_25 < pct_20 - 3.0
        
        plates = plate_map.get(code, ['未分类'])
        primary_plate = plates[0] if plates else '未分类'
        
        mode = "无"
        if mode_c: mode = "C" # 诱多跳水
        elif mode_a: mode = "A" # 弱转强(连板预期)
        elif mode_b: mode = "B" # 水下抢筹反包
        
        if mode != "无":
            results.append({
                'code': code,
                'name': name,
                'pct_20': pct_20,
                'pct_24': pct_24,
                'pct_25': pct_25,
                'amt_25': amt_25,
                'bid_25': bid_25,
                'mode': mode,
                'plate': primary_plate
            })

    # Sort and Group
    mode_a_list = sorted([x for x in results if x['mode'] == 'A'], key=lambda x: x['amt_25'], reverse=True)
    mode_b_list = sorted([x for x in results if x['mode'] == 'B'], key=lambda x: x['amt_25'], reverse=True)
    mode_c_list = sorted([x for x in results if x['mode'] == 'C'], key=lambda x: x['pct_20'] - x['pct_25'], reverse=True)

    # Plate consensus based on Mode A & B count
    plate_counts = {}
    for x in mode_a_list + mode_b_list:
        p = x['plate']
        plate_counts[p] = plate_counts.get(p, 0) + 1
        
    top_plates = sorted(plate_counts.items(), key=lambda x: x[1], reverse=True)

    # Generate MD
    md = ["# 【阶段一】 09:25 盘前竞价真理预期雷达\n\n"]
    md.append("*通过比对 09:20, 09:24, 09:25 三维时序，自动捕捉场外大单的真实意图。*\n\n")
    
    md.append("## 🏆 今日早盘核心主线预测 (按抢筹/弱转强个股数共振排序)\n")
    md.append("| 板块名称 | 核心异动个股数 | 代表个股 |\n")
    md.append("|---|---|---|\n")
    for p, c in top_plates[:10]:
        reps = [x['name'] for x in mode_a_list + mode_b_list if x['plate'] == p][:3]
        md.append(f"| **{p}** | {c} 只 | {','.join(reps)} |\n")
        
    md.append("\n## 🔥 [模式A] 烂板弱转强 / 连板超预期套利核心\n")
    md.append("| 代码 | 名称 | 09:20估幅 | 09:24估幅 | 落盘开幅 | 竞价(万) | 封单(万) | 所属板块 |\n")
    md.append("|---|---|---|---|---|---|---|---|\n")
    for x in mode_a_list[:15]:
        md.append(f"| {x['code']} | {x['name']} | {x['pct_20']:.2f}% | {x['pct_24']:.2f}% | **{x['pct_25']:.2f}%** | {x['amt_25']/10000:.0f}万 | {x['bid_25']/10000:.0f}万 | {x['plate']} |\n")

    md.append("\n## 🌊 [模式B] 水下巨量抢筹 / 反包试错位\n")
    md.append("| 代码 | 名称 | 09:20估幅 | 09:24估幅 | 落盘开幅 | 竞价(万) | 封单(万) | 所属板块 |\n")
    md.append("|---|---|---|---|---|---|---|---|\n")
    for x in mode_b_list[:15]:
        md.append(f"| {x['code']} | {x['name']} | {x['pct_20']:.2f}% | {x['pct_24']:.2f}% | **{x['pct_25']:.2f}%** | {x['amt_25']/10000:.0f}万 | {x['bid_25']/10000:.0f}万 | {x['plate']} |\n")

    md.append("\n## 💣 [模式C] 诱多核按钮危险区 (警惕昨天一字今日骗炮)\n")
    md.append("| 代码 | 名称 | 09:20估幅 (诱多) | 09:24估幅 | 落盘开幅 (核按钮) | 砸盘撤单幅度 | 竞价(万) | 所属板块 |\n")
    md.append("|---|---|---|---|---|---|---|---|\n")
    for x in mode_c_list[:15]:
        drop = x['pct_20'] - x['pct_25']
        md.append(f"| {x['code']} | {x['name']} | {x['pct_20']:.2f}% | {x['pct_24']:.2f}% | **{x['pct_25']:.2f}%** | **-{drop:.2f}%** | {x['amt_25']/10000:.0f}万 | {x['plate']} |\n")

    with codecs.open('true_expectation_report.md', 'w', 'utf-8') as f:
        f.writelines(md)

if __name__ == "__main__":
    main()
