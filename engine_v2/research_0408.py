
import asyncio
import sys
import os
import json

# 动态校准 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def deep_research():
    try:
        from engine_v2.v2_api_discover import KaipanlaAPI
        api = KaipanlaAPI()
        date_str = "2026-04-08"
        
        print(f"\n{'='*75}\n[MarketEdge] 2026-04-08 深度复盘数据分析\n{'='*75}")
        
        # 1. 获取最强板块
        plates = await api.get_hot_plates(date_str)
        print("\n[⚡ 真实热门板块排行榜 (Top 10)]")
        if plates:
            count = 0
            for p in plates:
                if count >= 10: break
                print(f"- {p.get('plate_name', 'Unknown'):<12} | 涨停数: {p.get('limit_up_count', 0)}")
                count += 1
        
        # 2. 获取当天所有封死涨停的票 (核心)
        bans = await api.get_limit_up_list(date_str)
        if bans:
            print(f"\n[💎 成功封死涨停总数: {len(bans)}]")
            # 找到高度板
            heights = sorted(bans, key=lambda x: x.get('lb_days', 0), reverse=True)
            print("\n[🏆 今日领涨高标 (封死名单)]")
            for b in heights[:8]:
                print(f"- {b['name']:<8} ({b['symbol']}) | 连板: {b.get('lb_days', 0)} | 板块: {b.get('plate', 'N/A')}")

        await api._session.close()
    except Exception as e:
        print(f"研究分析失败: {e}")

if __name__ == "__main__":
    asyncio.run(deep_research())
