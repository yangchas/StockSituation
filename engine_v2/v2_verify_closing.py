import sys
import json
import os

# 路径对齐
sys.path.append("/usr/local/lib/python3.9/site-packages")
from pykaipan import pykaipan

def verify():
    # 目标核心标的 (针对周五回放的重点个股)
    target_stocks = ["000720", "603538", "300750", "688525", "603986", "002902", "603601", "300383"]
    date_str = "2026-03-27"
    
    print(f"🚀 正在拉取 {date_str} 全量历史快照...")
    res = pykaipan.getHisStock(date_str)
    
    if not res:
        print("❌ API 返回为空")
        return
    
    # 获取列表键
    data_list = res.get('List') or res.get('list')
    
    if not data_list:
        print(f"❌ 提取失败：未找到列表键。返回键名: {list(res.keys())}")
        return

    print("\n" + "="*60)
    # 索引对焦: 0=代码, 1=名称, 5=收盘价, 6=涨幅%
    print(f"{'代码':<10} {'名称':<10} {'收盘':<10} {'涨幅':<10} {'表现评价'}")
    print("-" * 65)
    
    found_count = 0
    for item in data_list:
        try:
            code = str(item[0])[-6:].zfill(6)
            if code in target_stocks:
                name = item[1]
                close = item[5]
                change = float(item[6])
                
                # 表现评价标准
                eval_str = "🌟 涨停" if change > 9.9 else ("❌ 大跌" if change < -5 else "✅ 走稳")
                print(f"{code:<12} {name:<10} {close:<12} {change :>+6.2f}%    {eval_str}")
                found_count += 1
        except:
            continue
            
    if found_count == 0:
        print("⚠️ 未在快照中找到目标个股。")

if __name__ == "__main__":
    verify()
