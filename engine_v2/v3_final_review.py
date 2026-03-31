import sys
import json
import redis
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FinalReview")

# Paths for remote execution
sys.path.append("/usr/local/lib/python3.9/site-packages")
from pykaipan import pykaipan

def get_friday_and_monday_dates():
    # Hardcoded for the current market state (2026-03-30 is Monday)
    return "2026-03-27", "2026-03-30"

def v3_final_review():
    friday, monday = get_friday_and_monday_dates()
    monday_compact = monday.replace("-", "")
    
    print(f"\n{'='*100}")
    print(f"📊 [智库 V5.0] 1进2 晋级矩阵与竞价特征分析 ({monday})")
    print(f"{'='*100}")
    
    # 1. 获取周五 (3/27) 首板名单
    print(f"🔍 正在检索 {friday} 首板底座...")
    friday_bans = []
    # Fetch 1-B from kaipan
    res_yest = pykaipan.getHisBans(date=friday, ban='1', size=200)
    # The structure of getHisBans is often nested in 'info' or 'List'
    list_yest = []
    for page in res_yest.get('info', []):
        list_yest.extend(page)
    
    if not list_yest:
        print("❌ 无法获取周五首板数据")
        return
        
    for item in list_yest:
        # Expected Format: [Code, Name, Close, Pct, Reason, ..., LB_Days]
        # In getHisBans, LB_Days is at index 15
        if len(item) < 16: continue
        code = str(item[0])[-6:].zfill(6)
        name = item[1]
        lb = int(item[15])
        if lb == 1:
            friday_bans.append({"code": code, "name": name, "theme": str(item[12])})
    
    print(f"✅ 识别到 {friday} 首板底座: {len(friday_bans)} 只")
    
    # 2. 获取周一 (3/30) 收盘快照以确认晋级
    print(f"🔍 正在刺透 {monday} 闭市全量表现...")
    res_today = pykaipan.getHisStock(date=monday)
    list_today = res_today.get('List') or res_today.get('list') or []
    monday_map = {str(i[0])[-6:].zfill(6): i for i in list_today}
    
    # 3. 提取今日 09:25 竞价快照 (关键特征点)
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    auction_map = {}
    
    # Try top_amount first
    auction_raw = r.hget(f"market:auction:{monday_compact}:0925", "top_amount")
    if auction_raw:
        auction_list = json.loads(auction_raw)
        # The key is 'symbol' not 'code' in the actual JSON
        if isinstance(auction_list, list):
            auction_map = {str(item.get('symbol', item.get('code', '')))[-6:].zfill(6): item for item in auction_list}
        elif isinstance(auction_list, dict):
            auction_map = {str(k)[-6:].zfill(6): v for k, v in auction_list.items()}
        all_items = r.hgetall(f"market:auction:{monday_compact}:0925")
        for k, v in all_items.items():
            if k == 'top_amount': continue
            try:
                item = json.loads(v)
                code = str(item.get('symbol', item.get('code', k)))[-6:].zfill(6)
                auction_map[code] = item
            except: continue
    
    # 4. 生成矩阵表格
    print("\n" + "-"*110)
    print(f"{'代码':<10} {'名称':<10} {'核心题材':<16} {'09:25溢价':<12} {'竞价额':<12} {'收盘结果':<12} {'信号判定'}")
    print("-"*110)
    
    survivors = []
    failures = []
    
    for s in friday_bans:
        code = s['code']
        # Auction info
        auc = auction_map.get(code, {})
        
        # Robust field extraction
        auc_pct = auc.get('change_pct', 0)
        # If change_pct is missing, try to calculate from price and pre_close
        if auc_pct == 0 and 'price' in auc and 'pre_close' in auc:
            pc = float(auc['pre_close'])
            if pc > 0:
                auc_pct = (float(auc['price']) / pc - 1.0)
        
        auc_pct_val = float(auc_pct) * 100
        auc_amt = float(auc.get('amount', auc.get('auction_amount_yuan', 0))) / 100000000 # 亿
        
        # Final result from kaipan
        fin = monday_map.get(code, [])
        if not fin: continue
        
        # Today's result: index 6 is change_pct in getHisStock
        final_pct = float(fin[6])
        status = "✅ 2B 晋级" if final_pct > 9.8 else "❌ 掉队"
        
        # Strategy Logic: "Weak-to-Strong" detection
        # If Friday was a late board but Monday has a massive gap
        signal = "---"
        if status == "✅ 2B 晋级":
            survivors.append(s)
            if auc_pct > 5.0: signal = "🚀 强承接"
            elif auc_pct > 2.0: signal = "🔥 弱转强"
            else: signal = "💎 偷袭板"
        else:
            failures.append(s)
            if auc_pct > 7.0: signal = "⚠️ 诱多杀人"
            elif auc_pct < -2.0: signal = "📉 恐慌抛售"
            
        theme_short = s['theme'].split('、')[0][:12]
        print(f"{code:<12} {s['name']:<10} {theme_short:<18} {auc_pct_val:>+7.2f}%      {auc_amt:6.2f}亿      {status:<12} {signal}")

    # 5. 核心指标汇报
    promo_rate = len(survivors) / max(1, len(friday_bans)) * 100
    print("-" * 110)
    print(f"📈 [1进2 行业统计] 样本量: {len(friday_bans)} | 晋级成功: {len(survivors)} | 晋级率: {promo_rate:.1f}%")
    
    if survivors:
        best_survivor = sorted(survivors, key=lambda x: auction_map.get(x['code'], {}).get('amount', 0), reverse=True)[0]
        print(f"👑 [1进2 旗手] {best_survivor['name']} | 题材: {best_survivor['theme']}")
    
    print(f"{'='*100}\n")

if __name__ == "__main__":
    v3_final_review()
