
import sys
import os
import json
import asyncio
from datetime import datetime

# Add paths
sys.path.append('/root/work/web')
sys.path.append('/root/work/ai')

from services.tdengine_service import TDengineService
from API.api import UnifiedMarketDataFetcher

async def analyze():
    fetcher = UnifiedMarketDataFetcher()
    td = TDengineService()
    today, yesterday = "2026-03-06", "2026-03-05"
    zt, no_st, no_new = "\u6da8\u505c", "\u975est", "\u975e\u65b0\u80a1"
    
    df = await fetcher._get_wencai_stocks(f"{yesterday}{zt},{no_st},{no_new}", loop=True, return_df=True)
    if df is None or df.empty: return
    
    yest_meta = {}
    for _, row in df.iterrows():
        c = str(row.get("code", list(row.values)[0])).split(".")[0][-6:]
        if len(c) == 6: yest_meta[c] = {"n": str(row.get("\u540d\u79f0", "N/A")), "lb": 1}

    def get_prices(dt, window):
        sql = f"SELECT symbol, lp, a FROM stock_data WHERE ts >= '{dt} {window[0]}' AND ts <= '{dt} {window[1]}'"
        rows = td.execute_query(sql).fetchall() if td.execute_query(sql) else []
        return {r[0]: (r[1], r[2]) for r in rows}

    today_p = get_prices(today, ("09:24:55", "09:25:05"))
    yest_p = get_prices(yesterday, ("14:59:50", "15:00:10"))
    
    res = []
    for c, meta in yest_meta.items():
        if c in today_p and c in yest_p:
            g = round((today_p[c][0] - yest_p[c][0]) / yest_p[c][0] * 100, 2)
            res.append({"c": c, "n": meta["n"], "g": g, "a": round(today_p[c][1]/10000, 1)})
    
    res.sort(key=lambda x: x["g"], reverse=True)
    print("##FINAL_REPORT##")
    print(json.dumps(res[:30])) # Top 30 enough

if __name__ == "__main__":
    asyncio.run(analyze())
