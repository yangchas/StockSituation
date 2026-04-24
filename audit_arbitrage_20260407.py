import baostock as bs
import pandas as pd
from collections import defaultdict

def run_arbitrage_audit():
    bs.login()
    # 2026-04-07 昨日涨停核心池 (56只)
    # 这里我们选取已识别的核心标的进行深度审计
    codes = [
        "sh.600671", "sh.600129", "sz.002261", "sz.000690", "sz.000520", 
        "sh.603339", "sz.000030", "sz.002123", "sh.600586", "sz.002759"
    ]
    query_date = "2026-04-07"
    
    print(f"================ [V6.3 套利机会审计 - {query_date}] ================")
    print(f"   标的        题材      昨收     开盘     现价(定)   套利逻辑")
    print(f"   ----------------------------------------------------------------")
    
    for code in codes:
        rs = bs.query_history_k_data_plus(code, "date,code,open,close,preclose,pctChg", 
                                          start_date=query_date, end_date=query_date, 
                                          frequency="d")
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            name = "未知"
            if "600671" in code: name, plate, lb = "天目药业", "医药", "1B"
            elif "600129" in code: name, plate, lb = "太极集团", "医药", "1B"
            elif "002261" in code: name, plate, lb = "拓维信息", "算力", "0B"
            elif "000690" in code: name, plate, lb = "宝新能源", "电力", "1B"
            elif "000520" in code: name, plate, lb = "长航凤凰", "物流", "1B"
            elif "603339" in code: name, plate, lb = "四方股份", "电力", "1B"
            elif "000030" in code: name, plate, lb = "富奥股份", "汽配", "0B"
            elif "002123" in code: name, plate, lb = "荣信文化", "传媒", "1B"
            elif "600586" in code: name, plate, lb = "金晶科技", "光伏", "1B"
            elif "002759" in code: name, plate, lb = "天际股份", "锂电", "1B"
            else: continue

            open_p, close_p, pre_c = float(row[2]), float(row[3]), float(row[4])
            open_pct = (open_p / pre_c - 1) * 100
            current_pct = (close_p / pre_c - 1) * 100
            
            status = "黄金买点" if (open_pct < 8 and current_pct > 9.8) else ("封死" if open_pct > 9.8 else "分歧")
            print(f"   {name:<10} {plate:<8} {open_pct:>+6.1f}%   {current_pct:>+6.1f}%   {status:<8} ({lb})")

    bs.logout()

if __name__ == "__main__":
    run_arbitrage_audit()
