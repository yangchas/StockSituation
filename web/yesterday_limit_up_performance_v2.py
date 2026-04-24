
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

def analyze():
    analyzer = StockAnalyzer()
    td = TDengineService()
    
    today = "2026-03-06"
    yesterday = "2026-03-05"
    
    print(f"--- Global Performance Analysis: Yesterday ({yesterday}) -> Today ({today}) ---")
    
    # 1. Fetch Yesterday Limit Ups
    print(f"Fetching {yesterday} limit-up stocks from Kaipanla...")
    res = analyzer.get_his_bans(yesterday.replace('-',''))
    if not res:
        print("Failed to fetch limit-up data.")
        return
        
    list_data = res.get('info') or res.get('List') or res.get('list') or []
    if not list_data:
        print(f"No limit-up stocks found for {yesterday}.")
        return

    yest_set = {}
    for item in list_data:
        # Standardize format (list or dict)
        if isinstance(item, list) and len(item) > 11:
            code = str(item[0])[-6:]
            name = str(item[1])
            lb = int(item[11]) if str(item[11]).isdigit() else 1
            yest_set[code] = {"name": name, "lb": lb}
        elif isinstance(item, dict):
            code = str(item.get('StockID', item.get('code', '')))[-6:]
            name = item.get('StockName', item.get('name', ''))
            lb = int(item.get('LimitCount', item.get('lb_days', 1)))
            yest_set[code] = {"name": name, "lb": lb}

    print(f"Total Yesterday Limit Ups: {len(yest_set)}")
    
    # 2. Get Today's Auction Prices (09:25)
    print(f"Querying {today} auction prices...")
    sql_today = f"""
    SELECT symbol, lp, a FROM stock_data 
    WHERE ts >= '{today} 09:24:55' AND ts <= '{today} 09:25:05'
    """
    cursor = td.execute_query(sql_today)
    today_map = {}
    if cursor:
        rows = cursor.fetchall()
        for r in rows:
            today_map[r[0]] = {"price": r[1], "amt": r[2]}
            
    # 3. Get Yesterday's Close Prices (15:00)
    print(f"Querying {yesterday} close prices...")
    sql_yest = f"""
    SELECT symbol, lp FROM stock_data 
    WHERE ts >= '{yesterday} 14:59:50' AND ts <= '{yesterday} 15:00:10'
    """
    cursor = td.execute_query(sql_yest)
    yest_close_map = {}
    if cursor:
        rows = cursor.fetchall()
        for r in rows:
            yest_close_map[r[0]] = r[1]
            
    # 4. Correlate
    results = []
    for code, info in yest_set.items():
        pre = yest_close_map.get(code)
        cur_data = today_map.get(code)
        
        if pre and cur_data:
            gap = (cur_data['price'] - pre) / pre * 100
            results.append({
                "code": code,
                "name": info['name'],
                "lb": info['lb'],
                "gap": round(gap, 2),
                "amt_w": round(cur_data['amt'] / 10000, 2)
            })
            
    results.sort(key=lambda x: x['gap'], reverse=True)
    
    # 5. Output
    print(f"\n[ Performance Summary ({len(results)} Matched) ]")
    print(f"{'Code':<8} {'Name':<10} {'LB':<4} {'Gap%':<8} {'Amt(w)':<10}")
    print("-" * 50)
    for r in results[:20]: # Show top 20
        print(f"{r['code']:<8} {r['name']:<10} {r['lb']:<4} {r['gap']:>7.2f}% {r['amt_w']:>10.2f}")
        
    if len(results) > 30:
        print("\n...")
        for r in results[-10:]:
            print(f"{r['code']:<8} {r['name']:<10} {r['lb']:<4} {r['gap']:>7.2f}% {r['amt_w']:>10.2f}")

    # Aggregates
    if results:
        avg_gap = sum(r['gap'] for r in results) / len(results)
        strong = [r for r in results if r['gap'] >= 5.0]
        one_word = [r for r in results if r['gap'] >= 9.8]
        failed = [r for r in results if r['gap'] <= 0]
        
        print("\n--- Statistics ---")
        print(f"Average Gap: {avg_gap:.2f}%")
        print(f"Strong Open (>=5%): {len(strong)}")
        print(f"One-word Style (>=9.8%): {len(one_word)}")
        print(f"Weak/Low Open (<=0%): {len(failed)}")

if __name__ == "__main__":
    analyze()
