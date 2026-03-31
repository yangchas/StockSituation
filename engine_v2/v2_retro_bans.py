import sys
import json
import os
import redis

# 路径对齐
sys.path.append("/usr/local/lib/python3.9/site-packages")
from pykaipan import pykaipan

def retro():
    date_str = "2026-03-27"
    date_compact = date_str.replace("-", "")
    
    # 1. 获取周五全量涨停个股 (尝试多种日期格式)
    print(f"🚀 正在探测 {date_str} / {date_compact} 闭市全量涨停榜单...")
    bans_res = None
    for d in [date_str, date_compact]:
        bans_res = pykaipan.getHisBans(d)
        if bans_res and 'List' in bans_res and bans_res['List']:
            print(f"✅ 成功获取数据 (使用格式: {d})")
            break
            
    if not bans_res or 'List' not in bans_res:
        print(f"❌ 无法获取当日涨停列表。原始响应: {bans_res}")
        return

    # 2. 从 Redis 提取 09:25 竞价数据 (用于对照)
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auction_raw = r.hget(f"market:auction:{date_compact}:0925", "top_amount")
    auction_map = {}
    if auction_raw:
        auction_list = json.loads(auction_raw)
        auction_map = {str(item.get('code', ''))[-6:].zfill(6): item for item in auction_list}

    # 3. 按题材归类并分析竞价表现
    # 题材 -> [涨停个股]
    theme_to_bans = {}
    
    print(f"\n{'题材板块':<12} {'个股':<10} {'收盘涨幅':<10} {'09:25涨幅':<10} {'竞价额':<10}")
    print("-" * 65)
    
    for item in bans_res['List']:
        # 结构示例: [ID, Name, Close, Change%, Reason, ...]
        code = str(item[0])[-6:].zfill(6)
        name = item[1]
        reason = item[4] if len(item) > 4 else "其他"
        
        # 提取核心题材名词 (创新药+医药；1.xxx -> 创新药)
        main_theme = reason.split('+')[0].split('；')[0].split('(')[0].strip()
        
        # 查找该股早上的竞价表现
        auction_info = auction_map.get(code, {})
        auc_pct = auction_info.get('change_pct', 0) * 100
        auc_amt = auction_info.get('amount', 0) / 100000000
        
        # 记录
        if main_theme not in theme_to_bans: theme_to_bans[main_theme] = []
        theme_to_bans[main_theme].append((name, auc_pct, auc_amt))
        
        print(f"{main_theme:<12} {name:<10} +10.00%    {auc_pct:>+7.2f}%    {auc_amt:6.2f}亿")

    # 4. 统计胜出者
    print("\n🏆 [复盘总结：周五热门板块胜出者]")
    sorted_themes = sorted(theme_to_bans.items(), key=lambda x: len(x[1]), reverse=True)
    for theme, stocks in sorted_themes[:3]:
        total_auc = sum(s[2] for s in stocks)
        print(f"🔥 {theme:<10} | 涨停数: {len(stocks):<2} | 竞价总动能: {total_auc:5.2f}亿")

if __name__ == "__main__":
    retro()
