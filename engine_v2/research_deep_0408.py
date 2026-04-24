
import asyncio
import sys
import os
import json
import pandas as pd

# 强制将当前目录加入路径
sys.path.append(os.getcwd())

async def run_deep_audit():
    try:
        from engine_v2.v2_orc_final import AuctionOrchestrator
        orc = AuctionOrchestrator()
        date_str = "2026-04-08"
        
        print(f"\n{'='*85}\n[MarketEdge] 2026-04-08 策略对撞深度审计报告\n{'='*85}")
        
        # 1. 抓取当日全市场热门板块
        print(f"🔍 正在从官方网关提取 {date_str} 热门板块排行...")
        hot_plates = await orc._fetch_kaipan_hot_plates(date_str)
        top_5_plates = {p[0] for p in hot_plates[:5]}
        
        print("\n[📊 官方热门板块 Top 10]")
        for name, rank in hot_plates[:10]:
            print(f"Rank {rank}: {name}")
            
        # 2. 抓取当日封死涨停的王者名单
        print(f"\n🔍 正在通过 API 获取 {date_str} 实盘封死涨停池...")
        from engine_v2.v2_api_discover import getHisBans
        raw_bans = getHisBans(date=date_str, ban='1')
        
        actual_locks = []
        if raw_bans and 'info' in raw_bans:
            # Kaipanla API 结构解析
            for page in raw_bans['info']:
                for stock in page:
                    # 索引映射: [0][0] code, [0][1] name, [11] lb_days, [15] plate
                    actual_locks.append({
                        "code": str(stock[0][0][-6:]),
                        "name": stock[0][1],
                        "plate": stock[15] if len(stock) > 15 else "Other"
                    })
        
        lock_codes = {s['code'] for s in actual_locks}
        print(f"✅ 捕获实盘封死涨停共计: {len(actual_locks)} 只")

        # 3. 核心对撞：审计我们的 Alpha 信号
        # 系统之前的推荐名单 (基于日志识别)
        my_targets = [
            ("002957", "科瑞技术"), # 成功
            ("603687", "大胜达"),   # 回落
            ("000062", "深圳华强"), # 回落
            ("002980", "华盛昌"),   # 回落
            ("002119", "康强电子"), # 回落
            ("000586", "汇源通信")  # 成功 (封板状态)
        ]
        
        print("\n[🧬 Alpha 信号共振基因诊断]")
        print(f"{'代码':<8} {'名称':<10} | {'是否封死':<8} | {'所属板块':<15} | {'板块热度排名'}")
        print("-" * 85)
        
        for code, name in my_targets:
            is_lock = "✅ YES" if code in lock_codes else "❌ NO"
            # 查找板块
            plate = "N/A"
            rank = "Outside Top 10"
            for stock in actual_locks:
                if stock['code'] == code:
                    plate = stock['plate']
                    break
            
            # 手动对齐板块排名
            if plate != "N/A":
                for p_name, p_rank in hot_plates:
                    if p_name in plate:
                        rank = f"Rank {p_rank}"
                        break
            
            print(f"{code:<8} {name:<10} | {is_lock:<8} | {plate:<15} | {rank}")

        await orc._session.close()
    except Exception as e:
        print(f"审计执行失败: {e}")

if __name__ == "__main__":
    asyncio.run(run_deep_audit())
