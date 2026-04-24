"""
解析 KPL 历史板块数据 - 完整字段映射
字段顺序解析（基于原始数据探测）:
[0]  plate_code  板块代码
[1]  plate_name  板块名称
[2]  hot         热门度
[3]  change_pct  板块涨幅(%)
[4]  (未知)
[5]  total_amount 成交总额(元)
[6]  net_inflow  主力净流入(元)  ← 关键字段！
[7]  big_inflow  大单买入
[8]  big_outflow 大单卖出(负数)
[9]  (比率)
[10] market_cap  流通市值
[11] change_pct2 (同[3])
[12] net_inflow2 (净额确认)
[13] total_market_cap
[14] (未知)
[15] (unknown)
[16] (unknown)
[17] hot2
[18] change_pct3
"""
import asyncio
import json
import os
import sys

sys.path.append(os.getcwd())

async def analyze_plates(date_str="2026-04-17"):
    from ai.API.StockAnalyzer import StockAnalyzer
    api = StockAnalyzer()
    
    print(f"\n{'='*70}")
    print(f"  KPL 热门板块深度解析 | 日期: {date_str}")
    print(f"{'='*70}")
    
    res = await asyncio.get_event_loop().run_in_executor(
        None, api.get_his_plates, date_str
    )
    
    data_list = res.get('list', res.get('List', []))
    if not data_list:
        print("❌ 未获取到板块数据")
        return

    print(f"  {'排名':<4} {'板块':<12} {'热度':>8} {'涨幅%':>7} {'主力净额(亿)':>12} {'成交额(亿)':>10} {'净额评级'}")
    print(f"  {'-'*75}")
    
    results = []
    for i, it in enumerate(data_list[:15]):
        name   = it[1]
        hot    = it[2]
        chg    = float(it[3])          # 涨幅
        amount = float(it[5]) / 1e8    # 成交额（亿元）
        net    = float(it[6]) / 1e8    # 主力净额（亿元）
        
        # 评级
        if net > 50:
            rating = "超强净买"
        elif net > 10:
            rating = "强净买"
        elif net > 0:
            rating = "净买入"
        elif net > -10:
            rating = "轻微流出"
        else:
            rating = "净流出"
        
        print(f"  [{i+1:02d}] {name:<10} {hot:>8} {chg:>+7.2f}% {net:>10.1f}Y {amount:>8.0f}Y {rating}")

        results.append({"rank": i+1, "name": name, "hot": hot, "chg": chg, 
                        "net_b": round(net, 2), "amount_b": round(amount, 1)})
    
    # 机会分析
    print(f"\n{'='*70}")
    print(f"  🔬 机会甄别（强度 + 净额 双验证）")
    print(f"{'='*70}")
    
    opportunities = [r for r in results if r['chg'] > 1.0 and r['net_b'] > 0]
    risks         = [r for r in results if r['net_b'] < -5]
    
    print(f"\n  ✅ 机会标的（涨幅>1% & 主力净买）:")
    for r in opportunities:
        print(f"     [{r['rank']:02d}] {r['name']:<12} 涨幅: {r['chg']:+.2f}%  净额: {r['net_b']:+.1f}亿")
    
    print(f"\n  ❌ 危险板块（主力净流出>5亿）:")
    for r in risks:
        print(f"     [{r['rank']:02d}] {r['name']:<12} 涨幅: {r['chg']:+.2f}%  净额: {r['net_b']:+.1f}亿")

if __name__ == "__main__":
    asyncio.run(analyze_plates())
