
import sys
import os
import json
import time
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.path.abspath('.'))

from redis_storage import RedisStorageManager
from trade_calendar import TradeCalendar

def diagnose_ladder_logic():
    redis_mgr = RedisStorageManager()
    calendar = TradeCalendar()
    
    today = "2026-03-06" # Friday, our last trade day
    prev_day = calendar.get_previous_trade_day(today)
    
    print(f"--- Ladder Logic Diagnosis (Reference: {today}) ---")
    
    # 1. Check Highest Board Position (max_lb)
    # Source A: wencai cache
    wc_data = redis_mgr.get_data(f"cache:wencai:limitup_lb:{today}")
    max_wc = 0
    if wc_data:
        max_wc = max([int(v) for v in wc_data.values() if str(v).isdigit()] + [0])
    
    # Source B: limit_up list
    lb_list = redis_mgr.get_data(f"limit_up_{today}")
    max_lb = 0
    if lb_list:
        max_lb = max([int(item.get("lb_days", item.get("连板天数", 0))) for item in lb_list if isinstance(item, dict)] + [0])
        
    print(f"Max LB Height - Wencai Cache: {max_wc} | LimitUp Key: {max_lb}")
    
    # 2. Check individual stock alignment
    # Pick a few potentially high-board stocks
    if lb_list:
        high_boards = sorted(lb_list, key=lambda x: int(x.get("lb_days", 0)), reverse=True)[:5]
        print("\nTop 5 Ladder Candidates:")
        for hb in high_boards:
            code = hb.get("code", hb.get("symbol", "UNKNOWN"))
            lb = hb.get("lb_days", 0)
            
            # Cross-check with stock_extra
            extra_raw = redis_mgr.redis.hget(f"cache:stock_extra:{today}", code)
            extra_lb = "N/A"
            if extra_raw:
                try:
                    extra_lb = json.loads(extra_raw).get("lb_days", "N/A")
                except: pass
                
            print(f"Code: {code} | LB: {lb} | Extra-LB: {extra_lb} | Match: {str(lb) == str(extra_lb)}")

    # 3. Check for "Advancement" Risk (晋级)
    # Logic: If item in first_limit_up but WAS in yesterday's limit_up, that's a contradiction for FirstLimit
    first_limit = redis_mgr.redis.zrange("stock:first_limit_up", 0, -1)
    prev_lb = redis_mgr.get_data(f"limit_up_{prev_day}")
    prev_set = set()
    if prev_lb:
        for item in prev_lb:
            c = item.get("code", item.get("symbol"))
            if c: prev_set.add(c)
            
    contradictive = []
    for fl_item in first_limit:
        try:
            info = json.loads(fl_item)
            c = info.get("symbol")
            if c in prev_set:
                contradictive.append(c)
        except: pass
        
    print(f"\nLadder Advancement Integrity:")
    print(f"Contradictive Stocks (In FirstLimit but were LimitUp yesterday): {len(contradictive)}")
    if contradictive:
        print(f"Sample: {contradictive[:5]}")

if __name__ == "__main__":
    diagnose_ladder_logic()
