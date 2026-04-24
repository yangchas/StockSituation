import asyncio
import json
import logging
from datetime import datetime
import os
import sys

# 注入项目路径
sys.path.append(os.getcwd())

from ai.API.StockAnalyzer import StockAnalyzer
from engine_v2.v2_orc_final import AuctionOrchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PostAudit")

async def run_audit():
    date_str = "2026-04-07"
    logger.info(f"🔍 正在启动 2026-04-07 收盘审计...")
    
    # 初始化核心组件
    orc = AuctionOrchestrator()
    # 手动触发初始化
    await orc.metadata.initialize()
    
    # 待核审标的池 (根据 nohup.txt 提取)
    review_codes = ["605303", "600488", "000586", "000752", "603306", "600654", "603123", "300006"]
    review_names = {
        "605303": "园林股份", "600488": "津药药业", "000586": "汇源通信", 
        "000752": "新能泰山", "603123": "翠微股份", "300006": "莱美药业",
        "600654": "中安科", "603306": "圣泉集团"
    }

    print("\n--- [个股审计：全天表现对撞] ---")
    print(f"{'代码':<8} {'名称':<10} {'早盘评级':<12} {'收盘涨幅':<10} {'评价'}")
    
    # 1. 获取最新个股收盘数据 (通过问财)
    q = f"2026-04-07收盘涨幅;2026-04-07成交额;代码 {' '.join(review_codes)}"
    res_df = await orc.wencai.get_wencai_data(q)
    
    if res_df is not None and not res_df.empty:
        for _, row in res_df.iterrows():
            code = str(row.get('code', '')).strip()[-6:]
            name = review_names.get(code, "Unknown")
            close_pct = float(row.get('收盘涨幅', 0))
            
            # 逻辑分层评价
            eval_tag = "✅ 命中" if close_pct > 8.0 else ("⚠️ 略弱" if close_pct > 4.0 else "❌ 证伪")
            if name == "津药药业" and close_pct < 8.0: eval_tag = "⚠️ 炸板验证"
            
            print(f"{code:<8} {name:<10} {'暴力抢筹' if close_pct > 0 else '套利'} {close_pct:>8.2f}%    {eval_tag}")
    
    print("\n--- [板块审计：Kaipanla 真实强度] ---")
    # 2. 获取 Kaipanla 热门板块数据
    logger.info(f"正在拉取 Kaipanla 热门板块 (Date: {date_str})...")
    plates = await orc._fetch_kaipan_hot_plates(date_str)
    
    if plates:
        for name, rank in plates:
            print(f"排名 {rank}: {name}")
    else:
        print("❌ 未获取到热门板块数据")

    # 释放资源
    await orc._session.close()

if __name__ == "__main__":
    asyncio.run(run_audit())
