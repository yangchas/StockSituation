
import sys
import os
import json
import asyncio
import time
from datetime import datetime

# Add paths
sys.path.append('/root/work/web')
sys.path.append('/root/work/ai')

from services.tdengine_service import TDengineService
from API.api import UnifiedMarketDataFetcher

async def analyze():
    fetcher = UnifiedMarketDataFetcher()
    td = TDengineService()
    
    today = "2026-03-06"
    yesterday = "2026-03-05"
    
    # 涨停: \u6da8\u505c, 非st: \u975est, 非新股: \u975e\u65b0\u80a1
    zt_str = "\u6da8\u505c"
    no_st_str = "\u975est"
    no_new_str = "\u975e\u65b0\u80a1"
    
    # 1. Fetch Yesterday Limit Ups from Wencai
    query = f"{yesterday}{zt_str},{no_st_str},{no_new_str}"
    df = await fetcher._get_wencai_stocks(query, loop=True, return_df=True)
    
    if df is None or df.empty:
        return {"error": "No data"}
        
    yest_codes = []
    cols = list(df.columns)
    code_col = "code" if "code" in cols else next((c for c in cols if "\u4ee3\u7801" in str(c)), cols[0])
    name_col = next((c for c in cols if "\u540d\u79f0" in str(c)), None)
    lb_col = next((c for c in cols if "\u8de3\u505c" in str(c)), None)
    
    stock_meta = {}
    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).split(".")[0]
        if len(code) > 6: code = code[-6:]
        if len(code) != 6: continue
        
        yest_codes.append(code)
        lb = 1
        if lb_col:
            val = str(row.get(lb_col, "1"))
            if val.isdigit(): lb = int(val)
            
        stock_meta[code] = {
            "name": row.get(name_col, ""),
            "lb": lb
        }

    # 2. Get Today's Auction Prices (09:25)
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
    for code in yest_codes:
        pre = yest_close_map.get(code)
        cur_data = today_map.get(code)
        
        if pre and cur_data:
            gap = (cur_data['price'] - pre) / pre * 100
            results.append({
                "code": code,
                "name": stock_meta[code]['name'],
                "lb": stock_meta[code]['lb'],
                "gap": round(gap, 2),
                "amt_w": round(cur_data['amt'] / 10000, 2)
            })
            
    results.sort(key=lambda x: x['gap'], reverse=True)
    
    final_data = {
        "yesterday": yesterday,
        "today": today,
        "count": len(results),
        "avg_gap": round(sum(r['gap'] for r in results) / len(results), 2) if results else 0,
        "top_20": results[:20],
        "stats": {
            "one_word": len([r for r in results if r['gap'] >= 9.8]),
            "high": len([r for r in results if r['gap'] >= 5.0]),
            "low": len([r for r in results if r['gap'] <= 0])
        }
    }
    print("ANALYSIS_RESULT_START")
    print(json.dumps(final_data))
    print("ANALYSIS_RESULT_END")

if __name__ == "__main__":
    asyncio.run(analyze())
