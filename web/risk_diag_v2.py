
import sys
import os
import json
import zlib
from datetime import datetime

sys.path.append('/root/work/web')
from redis_storage import RedisStorageManager

r = RedisStorageManager()
today = '2026-03-06'
prev = '2026-03-05'

def decompress_val(val_hex):
    if not val_hex: return None
    try:
        return json.loads(zlib.decompress(bytes.fromhex(val_hex)))
    except:
        try: return json.loads(val_hex)
        except: return val_hex

def check():
    print(f"--- Technical Multi-Source Risk Audit: {today} ---")
    
    # 1. max_lb Fallback Chain Check
    wc_lb_raw = r.redis.get(f"cache:cache:wencai:limitup_lb:{today}") # double prefix? No.
    # Actually let's just use r.get_data
    wc_lb = r.get_data(f"wencai:limitup_lb:{today}") or {}
    kpl_lb = r.get_data(f"limit_up_{today}") or []
    prev_lb = r.get_data(f"limit_up_{prev}") or []
    
    m_wc = max([int(v) for v in wc_lb.values()] + [0]) if isinstance(wc_lb, dict) else 0
    m_kpl = max([int(i.get('lb_days', i.get('连板天数', 0))) for i in kpl_lb if isinstance(i, dict)] + [0])
    m_prev = max([int(i.get('lb_days', i.get('连板天数', 0))) for i in prev_lb if isinstance(i, dict)] + [0])
    
    print(f"Ladder Height: Wencai={m_wc}, Today_KPL={m_kpl}, Prev_KPL={m_prev}")
    
    # Risk: Sentiment phase uses max_lb. 
    # If Today_KPL is empty, it might use local calculation logic in calculate_sentiment.
    
    # 2. Advancement Consistency (晋级)
    fl_zset = r.redis.zrange("stock:first_limit_up", 0, -1)
    prev_codes = {it.get('code', it.get('symbol')) for it in prev_lb if isinstance(it, dict)}
    
    conflicts = []
    for s in fl_zset:
        try:
            d = json.loads(s)
            c = d.get('symbol')
            if c in prev_codes: conflicts.append(c)
        except: pass
    print(f"Advancement Integrity: Found {len(conflicts)} stocks in First-Limit pool that were Limit-Up yesterday.")
    if conflicts:
        print(f"  Sample Conflicts: {conflicts[:5]}")

    # 3. Auction Data Blind Spots
    # The source is in 'diag:auction_source:2026-03-06' hash
    auc_diag = r.redis.hgetall(f"diag:auction_source:{today}")
    source_key = auc_diag.get('source', '')
    if isinstance(source_key, bytes): source_key = source_key.decode()
    
    print(f"Auction Data Source used by Engine: {source_key}")
    
    # Load actual auction data to check bid_amount
    # Based on MarketEdgeEngine._load_auction_top_amount
    # It checks market:auction:20260306:0925 -> top_amount
    compact = today.replace("-","")
    raw_0925 = r.redis.hget(f"market:auction:{compact}:0925", "top_amount")
    if raw_0925:
        items = json.loads(raw_0925)
        missing_bid = [x.get('symbol') for x in items if not x.get('bid_amount_yuan') or x.get('bid_amount_yuan') == 0]
        print(f"Auction Completeness: {len(items)} items, {len(missing_bid)} missing BID volume.")
        if len(missing_bid) > len(items) // 2:
            print("🚨 Risk: Majority of auction items missing bid data. Leader detection is BLIND.")

if __name__ == "__main__":
    check()
