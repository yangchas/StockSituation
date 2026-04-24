import sys
import os

# 环境对焦
_project_root = r"d:\work\Go"
sys.path.insert(0, os.path.join(_project_root, "ai"))

from API.StockAnalyzer import StockAnalyzer

def audit():
    analyzer = StockAnalyzer()
    date_str = "2026-04-07"
    print(f"================ [Kaipanla 物理审计 - {date_str}] ================")
    print(f"{'代码':<10} {'名称':<10} {'梯队':<6} {'题材':<12} {'封板时间'}")
    print("-" * 60)
    
    res = analyzer.get_history_bans_pool(date_str, max_ban=10)
    if not res:
        print("❌ 无法获取 Kaipanla 涨停数据，请确认 API Key 或网络连通性。")
        return
        
    for it in sorted(res, key=lambda x: x['lb_days'], reverse=True):
        print(f"{it['code']:<10} {it['name']:<10} {it['lb_days']:<6} {it['plate']:<12} {it['seal_time']}")

if __name__ == "__main__":
    audit()
