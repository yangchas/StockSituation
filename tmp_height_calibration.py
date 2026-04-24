import os
import sys
import json
from types import SimpleNamespace

CWD = r"d:\work\Go"
os.chdir(CWD)
sys.path.append(CWD)

from ai.API.StockAnalyzer import StockAnalyzer

def board_height_calibration():
    analyzer = StockAnalyzer()
    date = "2026-03-30"
    print(f"--- 📊 深度分析昨日涨停数组结构 ({date}) ---")
    
    try:
        # 获取首板 (PidType=1) 和 连板 (PidType=2+)
        for b_type in [1, 2, 5]:
            res = analyzer._call_api('getHisBans', date=date, ban=str(b_type), size=5)
            if not res or 'info' not in res or not res['info']: continue
            
            stocks = res['info'][0]
            if not stocks: continue
            
            s0 = stocks[0]
            print(f"\n[梯队 {b_type}] 样板股: {s0[1]} ({s0[0]}) | 数组长度: {len(s0)}")
            for i, val in enumerate(s0):
                print(f"   Index {i:2}: {val}")
    except Exception as e:
        print(f"Probe Error: {e}")

board_height_calibration()
