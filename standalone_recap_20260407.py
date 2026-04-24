import baostock as bs
import pandas as pd
import logging
import sys

def run_standalone_audit():
    date_str = "2026-04-07"
    # [V2.0 纠错版]
    targets = {
        "600488": "津药药业", "605303": "园林股份", "000586": "汇源通信", 
        "000720": "新能泰山", "605589": "圣泉集团", "600654": "中安科",
        "603123": "翠微股份", "300006": "莱美药业"
    }

    lg = bs.login()
    if lg.error_code != '0':
        return

    report_lines = []
    report_lines.append("\n" + "!"*80)
    report_lines.append(f"!!! 2026-04-07 实战对账审计报告 V2.0 (基准数据: Baostock) !!!")
    report_lines.append("="*80)
    report_lines.append(f"{'代码':<8} {'名称':<10} {'开盘%':<8} {'最高%':<8} {'最低%':<8} {'收盘%':<10} {'成交额(亿)':<10}")
    report_lines.append("-" * 80)

    for code in targets.keys():
        bs_code = f"sh.{code}" if code.startswith('6') else f"sz.{code}"
        rs = bs.query_history_k_data_plus(bs_code,
            "date,code,open,high,low,close,preclose,pctChg,amount",
            start_date=date_str, end_date=date_str, 
            frequency="d", adjustflag="3")
        
        if (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            name = targets[code]
            pre_close = float(row[6])
            o_pct = (float(row[2])/pre_close - 1) * 100
            h_pct = (float(row[3])/pre_close - 1) * 100
            l_pct = (float(row[4])/pre_close - 1) * 100
            c_pct = float(row[7])
            amt_e = float(row[8]) / 1e8
            
            res = "PASS (封板)" if c_pct > 9.5 else ("WARN (炸板/冲高回落)" if h_pct > 7 and c_pct < 5 else "FAIL (弱势)")
            if code == "600488": 
                res = "BOOM (炸板确认)" if h_pct > 9 and c_pct < 8 else res

            report_lines.append(f"{code:<8} {name:<10} {o_pct:>7.2f}% {h_pct:>7.2f}% {l_pct:>7.2f}% {c_pct:>9.2f}% {amt_e:>10.2f}亿  | {res}")
            
    report_lines.append("="*80)
    bs.logout()
    
    with open("audit_report_v2.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

if __name__ == "__main__":
    run_standalone_audit()
