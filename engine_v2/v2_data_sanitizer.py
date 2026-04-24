import asyncio
import sys
import os
import argparse
from datetime import datetime

# 环境对齐
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path: sys.path.append(BASE_DIR)

from web.services.tdengine_service import TDengineService

async def main():
    parser = argparse.ArgumentParser(description="MarketEdge Data Sanitizer (V40.6)")
    parser.add_argument("--since", type=str, required=True, help="Start date/time to clean (e.g. 2026-04-16)")
    args = parser.parse_args()

    print(f"🧹 [Sanitizer] Starting cleanup from: {args.since}")
    
    # 初始化 TDengine 服务
    td = TDengineService()
    
    # 执行清洗
    # 如果只传了日期，加上 00:00:00 以防万一
    start_point = args.since
    if len(start_point) <= 10:
        start_point += " 00:00:00"
        
    results = td.cleanup_polluted_data(start_point)
    
    print("\n📊 [Cleanup Results]")
    for table, status in results.items():
        state = "✅ SUCCESS" if status == 1 else "❌ FAILED"
        print(f" - {table}: {state}")
        
    print("\n✨ Sanitization complete. You can now restart the Orchestrator for a clean Atomic Sync.")

if __name__ == "__main__":
    asyncio.run(main())
