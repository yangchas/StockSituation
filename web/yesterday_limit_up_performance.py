
import sys
import os
import json
import time
from datetime import datetime, timedelta

# Add paths
sys.path.append('/root/work/web')
sys.path.append('/root/work/ai')

from services.tdengine_service import TDengineService
from API.StockAnalyzer import StockAnalyzer
from trade_calendar import TradeCalendar

def analyze():
    analyzer = StockAnalyzer()
    td = TDengineService()
    calendar = TradeCalendar()
    
    today = "2026-03-06"
    yesterday = "2026-03-05"
    
    print(f"--- Analysis: Yesterday ({yesterday}) Limit-Up Performance in Today ({today}) Auction ---")
    
    # 1. Fetch Yesterday Limit Ups
    print(f"Fetching {yesterday} limit-up stocks from Kaipanla...")
    res = analyzer.get_his_bans(yesterday.replace('-',''))
    if not res or not isinstance(res, dict):
        print("Failed to fetch limit-up data.")
        return
    
    list_data = res.get('info') or res.get('List') or res.get('list') or []
    if not list_data:
        print(f"No limit-up stocks found for {yesterday}.")
        return
        
    # Flatten rows
    limit_stocks = []
    for item in list_data:
        if isinstance(item, list) and len(item) > 0 and isinstance(item[0], list):
            limit_stocks.extend(item)
        else:
            limit_stocks.append(item)
            
    # Map to codes
    # For his_bans, often row[0] is code, row[1] is name, row[5] is plate, row[11] is lb_days
    yest_set = {}
    for row in limit_stocks:
        if isinstance(row, list) and len(row) > 11:
            code = str(row[0])[-6:]
            name = str(row[1])
            lb = int(row[11]) if str(row[11]).isdigit() else 1
            yest_set[code] = {"name": name, "lb": lb}
        elif isinstance(row, dict):
            code = str(row.get('StockID', row.get('code', '')))[-6:]
            name = row.get('StockName', row.get('name', ''))
            lb = int(row.get('LimitCount', row.get('lb_days', 1)))
            yest_set[code] = {"name": name, "lb": lb}

    print(f"Total {yesterday} Limit Ups: {len(yest_set)}")
    
    # 2. Query Today Auction Data (TDengine)
    # We look for the 09:25:00 tick as it represents the opening aggregate
    print(f"Querying {today} auction data from TDengine...")
    sql = f"""
    SELECT symbol, lp, v, a, pre_close 
    FROM stock_data 
    WHERE ts >= '{today} 09:24:55' AND ts <= '{today} 09:25:05'
    """
    
    cursor = td.execute_query(sql)
    auction_map = {}
    if cursor:
        rows = cursor.fetchall()
        for r in rows:
            sym, price, vol, amt, pre_close = r
            if sym not in auction_map:
                auction_map[sym] = {"price": price, "vol": vol, "amt": amt, "pre_close": pre_close}
    
    # 3. Correlate and Summarize
    results = []
    for code, info in yest_set.items():
        auc = auction_map.get(code)
        if not auc:
            continue
            
        pre = auc['pre_close']
        cur = auc['price']
        if pre and pre > 0:
            gap = (cur - pre) / pre * 100
        else:
            gap = 0.0
            
        results.append({
            "code": code,
            "name": info['name'],
            "lb": info['lb'],
            "gap": gap,
            "amt": auc['amt']
        })
        
    results.sort(key=lambda x: x['gap'], reverse=True)
    
    # 4. Print Summary
    print("\n[ Top 15 Highest Gaps ]")
    print(f"{'Code':<10} {'Name':<10} {'LB':<4} {'Gap%':<8} {'Amount(W)':<10}")
    for r in results[:15]:
        print(f"{r['code']:<10} {r['name']:<10} {r['lb']:<4} {r['gap']:>7.2f}% {r['amt']/10000:>10.2f}")
        
    print("\n[ Bottom 10 Lowest Gaps ]")
    for r in results[-10:]:
        print(f"{r['code']:<10} {r['name']:<10} {r['lb']:<4} {r['gap']:>7.2f}% {r['amt']/10000:>10.2f}")
        
    # Stats
    if results:
        gaps = [r['gap'] for r in results]
        avg_gap = sum(gaps) / len(gaps)
        pos = len([g for g in gaps if g > 0])
        neg = len([g for g in gaps if g < 0])
        print(f"\n--- Statistics ---")
        print(f"Matched Stocks: {len(results)}")
        print(f"Average Gap: {avg_gap:.2f}%")
        print(f"High Open: {pos} | Low Open: {neg}")

if __name__ == "__main__":
    analyze()
