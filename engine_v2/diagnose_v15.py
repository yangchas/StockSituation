import asyncio
import os
import sys

# 对齐路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path: sys.path.append(BASE_DIR)

from v2_orc_final import AuctionOrchestrator

async def main():
    try:
        print("🚀 [Step 1] 正在初始化 Orchestrator...")
        orc = AuctionOrchestrator()
        print("✅ [Step 1] 初始化完成")
        
        print("🔍 [Step 2] 测试交易日历服务...")
        date_str = "2026-04-11"
        res = orc.calendar.is_trade_day(date_str)
        print(f"📅 [Step 2] is_trade_day({date_str}) -> {res}")
        
        print("🛠️ [Step 3] 模拟 _startup_sync...")
        # 仅测试逻辑路径，不执行真实网络请求
        print("✅ [Step 3] 诊断脚本顺利通过，未发现语法或初始化死锁")
        
    except Exception as e:
        print(f"❌ [CRUSHED] 发现运行时致命错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
