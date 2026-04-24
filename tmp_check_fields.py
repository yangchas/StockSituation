import os
import sys
import json
from datetime import datetime

# 锁定当前工作目录
CWD = r"d:\work\Go"
os.chdir(CWD)
sys.path.append(CWD)

try:
    from ai.API.StockAnalyzer import StockAnalyzer
    analyzer = StockAnalyzer()
    
    test_date = "20260330"
    print(f"🔍 [Probe] 正在探测 {test_date} 的涨停列表...")
    
    # 强制尝试 2026-03-30
    bans_res = analyzer.get_his_bans(test_date)
    print(f"✅ [Bans Response] 结构: {list(bans_res.keys()) if bans_res else 'None'}")
    if bans_res and 'List' in bans_res and len(bans_res['List']) > 0:
        print(f"📌 [First Item] Sample: {bans_res['List'][0]}")
    else:
        print(f"⚠️ [Bans] 'List' 字段缺失或为空")
        
    print(f"\n🔍 [Probe] 正在探测 {test_date} 的热门板块...")
    plates_res = analyzer.get_his_plates(test_date)
    print(f"✅ [Plates Response] 结构: {list(plates_res.keys()) if plates_res else 'None'}")
    if plates_res and 'List' in plates_res and len(plates_res['List']) > 0:
        print(f"📌 [First Item] Sample: {plates_res['List'][0]}")
    else:
        print(f"⚠️ [Plates] 'List' 字段缺失或为空")

except Exception as e:
    import traceback
    print(f"❌ [Error] 探测失败: {e}")
    traceback.print_exc()
