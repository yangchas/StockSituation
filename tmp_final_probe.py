import os
import sys
import json

CWD = r"d:\work\Go"
os.chdir(CWD)
sys.path.append(CWD)

from ai.API.StockAnalyzer import StockAnalyzer
analyzer = StockAnalyzer()

def log_probe(func_name, date):
    if func_name == "bans":
        res = analyzer.get_his_bans(date)
    else:
        res = analyzer.get_his_plates(date)
        
    with open(f"d:/work/Go/probe_{func_name}.json", "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)

log_probe("bans", "2026-03-30")
log_probe("plates", "2026-03-30")
print("Probe logs written to probe_bans.json and probe_plates.json")
