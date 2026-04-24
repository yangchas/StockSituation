import baostock as bs
import pandas as pd
import logging
import asyncio
import sys
import os

# 导入项目路径以便使用现有的 API
sys.path.append(os.getcwd())
from engine_v2.v2_orc_final import AuctionOrchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NetworkAudit")

def get_baostock_data(code_list, date_str):
    """通过 Baostock 网络接口获取个股真实行情"""
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"Baostock login failed: {lg.error_msg}")
        return None
    
    results = []
    for code in code_list:
        # Baostock 需要带前缀 sh.xxxxxx 或 sz.xxxxxx
        bs_code = f"sh.{code}" if code.startswith('6') else f"sz.{code}"
        rs = bs.query_history_k_data_plus(bs_code,
            "date,code,open,high,low,close,preclose,pctChg,amount",
            start_date=date_str, end_date=date_str, 
            frequency="d", adjustflag="3")
        
        while (rs.error_code == '0') & rs.next():
            results.append(rs.get_row_data())
            
    bs.logout()
    return results

async def audit_recap():
    date_str = "2026-04-07"
    # 从 nohup.txt 中提取的核心观察标的
    targets = {
        "600488": "津药药业", "605303": "园林股份", "000586": "汇源通信", 
        "000752": "新能泰山", "603306": "圣泉集团", "600654": "中安科",
        "603123": "翠微股份", "300006": "莱美药业"
    }
    
    logger.info(f"🚀 启动 Baostock 网络行情复盘 (Date: {date_str})...")
    raw_data = get_baostock_data(list(targets.keys()), date_str)
    
    if not raw_data:
        logger.error("❌ 未能获取到 Baostock 数据")
        return

    print("\n" + "="*80)
    print(f"{'代码':<8} {'名称':<10} {'开盘%':<8} {'最高%':<8} {'最低%':<8} {'收盘%':<10} {'总额(亿)':<10}")
    print("-" * 80)
    
    for row in raw_data:
        # row: [date, code, open, high, low, close, preclose, pctChg, amount]
        code = row[1].split('.')[-1]
        name = targets.get(code, "Unknown")
        pre_close = float(row[6])
        open_pct = (float(row[2])/pre_close - 1) * 100
        high_pct = (float(row[3])/pre_close - 1) * 100
        low_pct = (float(row[4])/pre_close - 1) * 100
        close_pct = float(row[7])
        amount_e = float(row[8]) / 1e8
        
        print(f"{code:<8} {name:<10} {open_pct:>7.2f}% {high_pct:>7.2f}% {low_pct:>7.2f}% {close_pct:>9.2f}% {amount_e:>10.2f}亿")

    print("="*80)
    
    # 获取板块数据 (使用网络请求)
    logger.info("🔍 正在通过网络接口获取 Kaipanla 热门板块排行...")
    orc = AuctionOrchestrator()
    plates = await orc._fetch_kaipan_hot_plates(date_str)
    if plates:
        print("\n[盘后官方热门板块排行]")
        for p_name, rank in plates[:5]:
            print(f"Top {rank}: {p_name}")
    
    await orc._session.close()

if __name__ == "__main__":
    asyncio.run(audit_recap())
