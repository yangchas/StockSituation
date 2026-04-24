import baostock as bs
import pandas as pd
import logging
import asyncio
import sys
import os

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
        bs_code = f"sh.{code}" if code.startswith('6') else f"sz.{code}"
        # 改用 5分钟K线 以绕过 Baostock 日线尚未更新的延迟
        rs = bs.query_history_k_data_plus(bs_code,
            "date,time,code,open,high,low,close,volume,amount",
            start_date="2026-04-07", end_date=date_str, 
            frequency="5", adjustflag="3")
        
        stock_rows = []
        while (rs.error_code == '0') & rs.next():
            stock_rows.append(rs.get_row_data())
            
        if stock_rows:
            # 取该股当天的最后一根 5 分钟 K 线作为收盘参考，并汇总金额
            last_bar = stock_rows[-1]
            total_amount = sum(float(r[8]) if r[8] else 0 for r in stock_rows)
            # 为了兼容后续逻辑，将数据组装为 [date, code, open, high, low, close, preclose, pctChg, amount]
            # 注意：1分钟/5分钟接口没有 preclose 和 pctChg，我们需要近似计算（假设当日开盘价为基准，或者直接输出绝对价）
            results.append({
                "code": code,
                "close": float(last_bar[5]),
                "total_amount": total_amount
            })
            
    bs.logout()
    return results

async def audit_recap():
    date_str = "2026-04-08"
    # 从 09:36 到 12:00 nohup 日志中提取的核心观察标的
    targets = {
        "600488": "津药药业", "000586": "汇源通信", "000720": "新能泰山", 
        "603687": "大胜达", "000062": "深圳华强", "002957": "科瑞技术",
        "002980": "华盛昌", "002119": "康强电子", "000890": "法尔胜",
        "000990": "诚志股份", "300927": "江天化学", "002109": "兴化股份",
        "002408": "齐翔腾达", "603968": "醋化股份"
    }
    
    logger.info(f"🚀 启动 Baostock 网络行情复盘 (Date: {date_str})...")
    raw_data = get_baostock_data(list(targets.keys()), date_str)
    
    if not raw_data:
        logger.error("❌ 未能获取到 Baostock 数据")
        return

    print("\n" + "="*85)
    print(f"{'代码':<8} {'名称':<10} {'收盘价(最新)':<15} {'总额(日内累计)':<15}")
    print("-" * 85)
    
    for row in raw_data:
        code = row["code"]
        name = targets.get(code, "Unknown")
        close_price = row["close"]
        amount_e = row["total_amount"] / 1e8
        
        print(f"{code:<8} {name:<10} ¥{close_price:<13.2f} {amount_e:>8.2f}亿")

    print("="*85)
    
    # 获取板块数据注释掉，以免需要内部模块
    # logger.info("🔍 正在通过网络接口获取 Kaipanla 热门板块排行...")

if __name__ == "__main__":
    asyncio.run(audit_recap())
