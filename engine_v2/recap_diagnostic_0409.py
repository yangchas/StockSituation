import asyncio
import sys
import os
import json

# Setup paths
BASE_DIR = "d:/work/Go"
sys.path.append(BASE_DIR)

from ai.API.StockAnalyzer import StockAnalyzer

async def gather_diagnostic_truth():
    api = StockAnalyzer()
    date = '2026-04-09'
    
    print(f"--- Deep Diagnostic for {date} ---")
    
    loop = asyncio.get_event_loop()
    # 1. Final Plate Strengths
    plates = await loop.run_in_executor(None, api._call_api, 'getHisPlates', date)
    
    # 2. Daily Limit Ladder (Full)
    bans = await loop.run_in_executor(None, api._call_api, 'getHisBans', {'date': date, 'ban': '1,2,3,4,5,6,7,8,9,10'})
    
    results = {
        "plates": [],
        "bans": []
    }
    
    if plates and 'list' in plates:
        results["plates"] = [{"name": p[1], "strength": p[2]} for p in plates['list'][:15]]
        
    if bans and 'info' in bans:
        paged_info = bans['info']
        if isinstance(paged_info, list) and len(paged_info) > 0:
            for page in paged_info:
                if not isinstance(page, list): continue
                for b in page:
                    if not isinstance(b, (list, tuple)) or len(b) < 16: continue
                    results["bans"].append({
                        "code": b[0],
                        "name": b[1],
                        "days": b[15],
                        "pct": b[6],
                        "concept": b[12]
                    })

    # Summary Generation
    print("\n[Final Top 5 Sectors]")
    for p in results["plates"][:5]:
        print(f"-> {p['name']}: {p['strength']}")
        
    # Analysis
    zjbc = [b for b in results["bans"] if "中嘉博创" in b['name']]
    low_price_bans = [b for b in results["bans"] if int(b['days']) == 2] # 1进2标的
    
    print(f"\n[Audit] 中嘉博创 Status: {'LOCKED' if zjbc else 'MISS'}")
    if zjbc:
        print(f"Detail: {zjbc[0]}")
        
    print(f"\n[Audit] 1st-to-2nd Board Count: {len(low_price_bans)}")
    # Check for general themes in successes
    concepts = {}
    for b in results["bans"]:
        c_list = str(b['concept']).split('、')
        for c in c_list:
            concepts[c] = concepts.get(c, 0) + 1
    
    sorted_concepts = sorted(concepts.items(), key=lambda x: x[1], reverse=True)
    print("\n[Concept Success Frequency (Top 5)]")
    for c, count in sorted_concepts[:5]:
        print(f"-> {c}: {count} bans")

if __name__ == "__main__":
    asyncio.run(gather_diagnostic_truth())
