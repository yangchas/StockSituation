import sys
import json
import os
import redis

# 路径对齐
sys.path.append("/usr/local/lib/python3.9/site-packages")
from pykaipan import pykaipan

def final_arbitrage():
    date_yesterday = "2026-03-26"
    date_today = "2026-03-27"
    date_compact = date_today.replace("-", "")
    
    # 1. 获取周四(3/26) 底座 (通过全量快照识别涨停)
    print(f"🚀 正在拉取 {date_yesterday} 市场快照以筛选首板底座...")
    snap_yest = pykaipan.getHisStock(date_yesterday)
    list_yest = snap_yest.get('List') or snap_yest.get('list')
    if not list_yest:
        print("❌ 无法获取底座快照")
        return

    # 筛选周四涨停首板 (假设当日涨停即为首板或我们需要套利的对象)
    yest_ban_codes = []
    for item in list_yest:
        try:
            if float(item[6]) > 9.8: # 涨幅 > 9.8%
                yest_ban_codes.append({"code": str(item[0])[-6:].zfill(6), "name": item[1], "theme": str(item[4])})
        except: continue
    
    print(f"✅ 识别周四涨停底座: {len(yest_ban_codes)} 只")

    # 2. 从 Redis 提取周五(3/27) 09:25 竞价数据
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auction_raw = r.hget(f"market:auction:{date_compact}:0925", "top_amount")
    auction_map = {}
    if auction_raw:
        auction_list = json.loads(auction_raw)
        auction_map = {str(item.get('code', ''))[-6:].zfill(6): item for item in auction_list}

    # 3. 获取周五(3/27) 收盘快照 (验证晋级)
    print(f"🚀 正在刺透周五 {date_today} 闭市结果...")
    snap_today = pykaipan.getHisStock(date_today)
    list_today = snap_today.get('List') or snap_today.get('list')
    final_res_map = {str(i[0])[-6:].zfill(6): i for i in list_today} if list_today else {}

    # 4. 分析 1进2 表现与龙头带动
    print("\n" + "="*80)
    print(f"{'代码':<10} {'名称':<10} {'题材板块':<12} {'09:25涨幅':<10} {'结果':<10} {'日内盈亏'}")
    print("-" * 85)
    
    for s in yest_ban_codes:
        code = s['code']
        auc = auction_map.get(code, {})
        fin = final_res_map.get(code, [])
        if not fin: continue
        
        auc_pct = auc.get('change_pct', 0) * 100
        auc_amt = auc.get('amount', 0) / 10000 # 万
        final_pct = float(fin[6])
        
        status = "✅ 晋级" if final_pct > 9.8 else "❌ 失败"
        profit = final_pct - auc_pct
        theme = s['theme'].split('、')[0][:10]
        
        # 我们只看周五最终成功的套利者 (1转2)
        if status == "✅ 晋级":
            print(f"{code:<12} {s['name']:<10} {theme:<14} {auc_pct:>+7.2f}%    {status:<12} {profit:>+7.2f}%")

    # 5. 龙头套利深度分析 (新能泰山 带动的电力板块)
    print("\n🔥 [龙头套利实战：新能泰山 (电力) 强势下的同板块首板表现]")
    for s in yest_ban_codes:
        if "电力" in s['theme'] or "能源" in s['theme']:
            code = s['code']
            fin_info = final_res_map.get(code)
            if fin_info and float(fin_info[6]) > 9.8:
                auc_pct = auction_map.get(code, {}).get('change_pct', 0) * 100
                print(f"🎯 成功套利: {s['name']:<10} ({s['theme'][:10]}) | 竞价溢价: {auc_pct:>+5.2f}% | 结果:稳步晋级2板")

if __name__ == "__main__":
    final_arbitrage()
