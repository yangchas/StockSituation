
import sys
import os
import json

# 向对准 pykaipan 路径 (Windows Anaconda 环境)
sys.path.append(r'D:\software\anaconda3\lib\site-packages\pykaipan')
try:
    from pykaipan import getHisBans, getHisPlates
except ImportError:
    # 尝试备用路径
    sys.path.append(r'C:\Users\yangxuezhen\AppData\Roaming\Python\Python39\site-packages')
    from pykaipan import getHisBans, getHisPlates

def probe():
    date_str = "2026-04-08"
    print(f"\n{'='*80}\n[MarketEdge] 2026-04-08 物理数据探针报告\n{'='*80}")
    
    # 1. 抓取板块
    print("🔍 正在拉取热门板块数据...")
    res_plates = getHisPlates(date=date_str)
    if res_plates and 'list' in res_plates:
        print("\n[📊 官方热门板块 Top 10]")
        for i, p in enumerate(res_plates['list'][:10]):
            # 索引根据 Kaipanla 协议: [1] plate_name, [2] hot_value
            print(f"Rank {i+1}: {p[1]:<15} | 热度: {p[2]}")
    
    # 2. 抓取涨停股并筛选关键对比标的
    print("\n🔍 正在拉取实盘涨停对账单...")
    res_bans = getHisBans(date=date_str, ban='1')
    locked_codes = set()
    all_bans = []
    
    if res_bans and 'info' in res_bans:
        for page in res_bans['info']:
            for stock in page:
                # [0][0] code, [0][1] name
                code = stock[0][0][-6:]
                locked_codes.add(code)
                all_bans.append(stock)
                
    print(f"✅ 全市场封死涨停总数: {len(locked_codes)}")

    # 3. 策略对撞与对账
    targets = {
        '002957': '科瑞技术', '603687': '大胜达', '000062': '深圳华强',
        '002980': '华盛昌', '002119': '康强电子', '000586': '汇源通信',
        '600488': '津药药业'
    }
    
    print("\n[🎯 策略复盘与基因重组]")
    print(f"{'代码':<8} {'名称':<10} | {'封死':<6} | {'板块归属'}")
    print("-" * 60)
    
    for code, name in targets.items():
        is_lock = "YES" if code in locked_codes else "NO"
        plate = "Other"
        # 尝试在当日涨停池里找对应的板块标签
        for b in all_bans:
            if b[0][0][-6:] == code:
                plate = b[15] if len(b) > 15 else "N/A"
                break
        print(f"{code:<8} {name:<10} | {is_lock:<6} | {plate}")

if __name__ == "__main__":
    probe()
