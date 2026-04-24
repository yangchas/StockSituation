import asyncio
import sys
import os
import json

# Setup paths
BASE_DIR = "d:/work/Go"
sys.path.append(BASE_DIR)

from ai.API.StockAnalyzer import StockAnalyzer

async def gather_truth():
    api = StockAnalyzer()
    date = '2026-04-09'
    
    print(f"--- Fetching Truth for {date} ---")
    
    # 1. Hot Plates
    loop = asyncio.get_event_loop()
    plates = await loop.run_in_executor(None, api._call_api, 'getHisPlates', date)
    
    # 2. Limit Up Stocks
    bans = await loop.run_in_executor(None, api._call_api, 'getHisBans', {'date': date, 'ban': '1,2,3,4,5,6,7,8,9,10'})
    
    # 3. Specific Stock Audit: Zhongjia Bochuang (000889)
    # Using Sina for fast quote if needed, but here we check its presence in bans
    
    results = {
        "top_plates": [],
        "ladder": []
    }
    
    if plates and 'list' in plates:
        # Index 1: Name, Index 2: Hot/Strength
        results["top_plates"] = [{"name": p[1], "strength": p[2]} for p in plates['list'][:10]]
        
    if bans and 'info' in bans:
        # Index 1: Name, Index 6: Pct, Index 12: Reason/Concept, Index 15: Days
        first_page = bans['info'][0] if bans['info'] else []
        for b in first_page:
            results["ladder"].append({
                "code": b[0],
                "name": b[1],
                "days": b[15],
                "concept": b[12]
            })
            
    print("\n[TOP 10 PLATES]")
    for p in results["top_plates"]:
        print(f"{p['name']}: {p['strength']}")
        
    print("\n[LADDER EXAMPLES (3B+)]")
    for b in results["ladder"]:
        if int(b['days']) >= 3:
            print(f"{b['name']} ({b['days']}B) - {b['concept']}")

    # Search for Zhongjia Bochuang
    zjbc = [b for b in results["ladder"] if "中嘉博创" in b['name']]
    if zjbc:
        print(f"\n[TARGET AUDIT] 中嘉博创 FOUND: {zjbc[0]}")
    else:
        # Check if it hit the board at all
        print("\n[TARGET AUDIT] 中嘉博创 NOT in limit-up list at close.")

if __name__ == "__main__":
    asyncio.run(gather_truth())
