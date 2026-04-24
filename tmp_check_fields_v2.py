import os
import sys
import json

CWD = r"d:\work\Go"
os.chdir(CWD)
sys.path.append(CWD)

from ai.API.StockAnalyzer import StockAnalyzer
analyzer = StockAnalyzer()

def probe(func_name, date):
    print(f"\n--- Probing {func_name} for {date} ---")
    if func_name == "bans":
        res = analyzer.get_his_bans(date)
    else:
        res = analyzer.get_his_plates(date)
        
    if not res:
        print("Empty response")
        return

    print(f"Keys: {list(res.keys())}")
    
    # Check for List or list_son
    data_key = None
    for k in ["List", "list_son", "info", "data"]:
        if k in res:
            data_key = k
            break
            
    if data_key:
        data = res[data_key]
        print(f"Found data in key: '{data_key}' (type: {type(data)})")
        if isinstance(data, list) and len(data) > 0:
            print(f"Sample item: {data[0]}")
        elif isinstance(data, dict):
            print(f"Sample keys in dict: {list(data.keys())[:5]}")
    else:
        print("No standard data key found. Raw sample:")
        print(json.dumps({k: str(v)[:100] for k, v in res.items()}, indent=2))

probe("bans", "2026-03-30")
probe("plates", "2026-03-30")
