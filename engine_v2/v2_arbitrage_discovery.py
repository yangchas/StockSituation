import sys
import json
import os
import redis

# 路径对齐
sys.path.append("/usr/local/lib/python3.9/site-packages")
from pykaipan import pykaipan

def arbitrage_discovery():
    date_yesterday = "2026-03-26" # 底座日期
    date_today = "2026-03-27"    # 套利日期
    date_compact = date_today.replace("-", "")
    
    # 1. 获取周四(3/26)的全量涨停 (底座)
    print(f"🚀 正在拉取 {date_yesterday} 底座涨停榜单...")
    yest_bans = pykaipan.getHisBans(date_yesterday)
    if not yest_bans or 'List' not in yest_bans:
        print("❌ 无法获取底座数据")
        return

    # 筛选出周四的“首板” (梯队等级: 1)
    # 结构: [ID, Name, Close, Change%, Reason, ..., DayCount, ...]
    # 注意: getHisBans 返回项中, 连板数通常在索引 11 或通过 Reason 解析
    first_bans = []
    for item in yest_bans['List']:
        day_count = item[11] if len(item) > 11 else 1 # 降级处理
        if day_count == 1:
            first_bans.append({"code": str(item[0])[-6:].zfill(6), "name": item[1], "theme": item[4]})
    
    print(f"✅ 识别周四首板底座: {len(first_bans)} 只")

    # 2. 从 Redis 提取周五(3/27) 09:25 竞价数据 (核心: 封单金额)
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auction_raw = r.hget(f"market:auction:{date_compact}:0925", "top_amount")
    auction_map = {}
    if auction_raw:
        auction_list = json.loads(auction_raw)
        auction_map = {str(item.get('code', ''))[-6:].zfill(6): item for item in auction_list}

    # 3. 获取周五(3/27) 的真实收盘快照 (验证是否封板)
    print(f"🚀 正在核对周五 {date_today} 的结果...")
    snapshot = pykaipan.getHisStock(date_today)
    snap_list = snapshot.get('List') or snapshot.get('list')
    final_res_map = {str(i[0])[-6:].zfill(6): i for i in snap_list} if snap_list else {}

    # 4. 统计 1进2 的表现
    print("\n" + "="*70)
    print(f"{'代码':<10} {'名称':<10} {'题材':<12} {'竞价强度':<10} {'结果':<10} {'日内盈亏'}")
    print("-" * 75)
    
    for s in first_bans:
        code = s['code']
        auc = auction_map.get(code, {})
        fin = final_res_map.get(code, [])
        
        if not fin: continue
        
        # 竞价表现
        auc_pct = auc.get('change_pct', 0) * 100
        auc_amt = auc.get('amount', 0) / 10000 # 万
        
        # 收盘表现
        final_pct = float(fin[6])
        status = "✅ 晋级" if final_pct > 9.8 else "❌ 失败"
        
        # 套利盈亏 (竞价买入 -> 收盘)
        profit = final_pct - auc_pct
        
        # 题材简化
        theme = s['theme'].split('；')[0][:10]
        
        if status == "✅ 晋级":
            print(f"{code:<12} {s['name']:<10} {theme:<14} {auc_pct:>+7.2f}%    {status:<12} {profit:>+7.2f}%")

    # 5. 特别分析: 龙头带领下的套利 (新能泰山 000720)
    print("\n🔥 [龙头套利对撞：新能泰山 (电力) 走强时的 1进2 表现]")
    # 筛选题材包含'电力'或'绿电'的晋级标的
    for s in first_bans:
        if "电力" in s['theme'] or "能源" in s['theme']:
            code = s['code']
            if final_res_map.get(code) and float(final_res_map[code][6]) > 9.8:
                print(f"🎯 成功套利: {s['name']} ({s['theme'][:15]}) | 竞价溢价: {auction_map.get(code, {}).get('change_pct',0)*100:>+5.2f}%")

if __name__ == "__main__":
    arbitrage_discovery()
