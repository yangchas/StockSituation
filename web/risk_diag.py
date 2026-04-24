
import sys
import os
import json
from datetime import datetime

sys.path.append('/root/work/web')
from redis_storage import RedisStorageManager

r = RedisStorageManager()
today = '2026-03-06'
prev = '2026-03-05'

def check():
    print(f"--- Global Diagnostic: {today} ---")
    
    # 1. max_lb Fallback Chain Check
    wc_lb = r.get_data(f"cache:wencai:limitup_lb:{today}") or {}
    kpl_lb = r.get_data(f"limit_up_{today}") or []
    prev_lb = r.get_data(f"limit_up_{prev}") or []
    
    m_wc = max([int(v) for v in wc_lb.values()] + [0])
    m_kpl = max([int(i.get('lb_days', i.get('连板天数', 0))) for i in kpl_lb] + [0])
    m_prev = max([int(i.get('lb_days', i.get('连板天数', 0))) for i in prev_lb] + [0])
    
    print(f"MaxLB: Wencai={m_wc}, KPL(Today)={m_kpl}, KPL(Prev)={m_prev}")
    if m_kpl == 0 and m_prev > 0:
        print("⚠️  Risk: Today's limit-up list is empty. Engine might fallback to yesterday's height, masking a board collapse.")

    # 2. First Limit Consistency
    fl_zset = r.redis.zrange("stock:first_limit_up", 0, -1)
    prev_set = set()
    for it in prev_lb:
        c = it.get('code', it.get('symbol'))
        if c: prev_set.add(c)
    
    conflicts = []
    for s in fl_zset:
        try:
            d = json.loads(s)
            if d.get('symbol') in prev_set: conflicts.append(d.get('symbol'))
        except: pass
    print(f"First-Limit Conflicts: {len(conflicts)}/{len(fl_zset)} (Stocks in FirstLimit but were LimitUp yesterday)")

    # 3. Auction Data Integrity
    auc_data = r.get_data(f"auction_top_amount_{today}") or []
    zeros = [x.get('symbol') for x in auc_data if not x.get('bid_amount_yuan') or x.get('bid_amount_yuan') == 0]
    print(f"Auction Integrity: {len(auc_data)} total, {len(zeros)} missing bid_amount (one-word detection blind spot).")
    if len(zeros) > 50:
        print("🚨 High Risk: More than 50% of auction stocks missing bid data. Gap-up validation is unreliable.")

if __name__ == "__main__":
    check()
