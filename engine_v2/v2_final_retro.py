import sys
import json
import os
import redis

# 路径对齐
sys.path.append("/usr/local/lib/python3.9/site-packages")
from pykaipan import pykaipan

def final_retro():
    date_str = "2026-03-27"
    date_compact = date_str.replace("-", "")
    
    # 1. 获取周五全量行情快照 (这是最靠谱的源)
    print(f"🚀 正在从全量快照中提取 {date_str} 涨停榜单...")
    snapshot = pykaipan.getHisStock(date_str)
    
    data_list = snapshot.get('List') or snapshot.get('list')
    if not data_list:
        print("❌ 无法获取当日行情快照")
        return

    # 2. 从 Redis 提取 09:25 竞价数据 (用于对照)
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auction_raw = r.hget(f"market:auction:{date_compact}:0925", "top_amount")
    auction_map = {}
    if auction_raw:
        auction_list = json.loads(auction_raw)
        auction_map = {str(item.get('code', ''))[-6:].zfill(6): item for item in auction_list}

    # 3. 筛选涨停个股并归位题材
    winners = []
    theme_stats = {} # theme -> count
    
    print(f"\n{'题材板块':<12} {'个股':<10} {'收盘涨幅':<10} {'09:25涨幅':<10} {'竞价额':<10}")
    print("-" * 65)
    
    for item in data_list:
        # 索引对焦: 0=代码, 1=名称, 4=题材, 5=收盘, 6=涨幅%
        try:
            change_pct = float(item[6])
            if change_pct > 9.8:
                code = str(item[0])[-6:].zfill(6)
                name = item[1]
                theme_raw = item[4] if len(item) > 4 else "其他"
                main_theme = theme_raw.split('、')[0].split('；')[0].split('+')[0].strip()
                
                # 对应竞价
                auc_info = auction_map.get(code, {})
                auc_pct = auc_info.get('change_pct', 0) * 100
                auc_amt = auc_info.get('amount', 0) / 100000000
                
                winners.append({
                    "code": code, "name": name, "theme": main_theme, 
                    "auc_pct": auc_pct, "auc_amt": auc_amt
                })
                
                if main_theme not in theme_stats: theme_stats[main_theme] = 0
                theme_stats[main_theme] += 1
                
                print(f"{main_theme:<12} {name:<10} {change_pct:>+7.2f}%    {auc_pct:>+7.2f}%    {auc_amt:6.2f}亿")
        except:
            continue

    # 4. 总结胜出题材
    print("\n🏆 [2026-03-27 胜出板块总结]")
    sorted_themes = sorted(theme_stats.items(), key=lambda x: x[1], reverse=True)
    for theme, count in sorted_themes[:5]:
        print(f"🔥 {theme:<10} | 涨停数: {count:<2} ")

    # 5. 发现竞价阶段的“高确定性肉”
    print("\n💎 [竞价阶段已露头的盈利捕捉点]")
    # 筛选标准: 涨停标的 + 竞价涨幅 > 4% 或 竞价额 > 0.1亿
    for w in winners:
        if w['auc_pct'] > 4.0 or w['auc_amt'] > 0.1:
            print(f"🎯 {w['name']:<10} ({w['theme']}) | 竞价:+{w['auc_pct']:>5.2f}% | 竞价额:{w['auc_amt']:5.2f}亿 | 结果:✅收板")

if __name__ == "__main__":
    final_retro()
