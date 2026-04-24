import baostock as bs
import pandas as pd

def run_chemical_audit():
    bs.login()
    # 2026-04-07 化工/甲醇核心板块标的
    codes = [
        'sz.002109', # 兴化股份 (甲醇/煤化工)
        'sh.600722', # 金牛化工 (甲醇/化工)
        'sh.600227', # 赤天化 (尿素/化工)
        'sz.002092', # 中泰化学 (聚氯乙烯/甲醇)
        'sz.000830', # 鲁西化工 (有机硅/煤化工)
        'sh.600075', # 新疆天业 (化工/烧碱)
        'sh.600596', # 新安股份 (有机硅/化工)
        'sz.002427'  # 尤夫股份 (涤纶/化工)
    ]
    query_date = "2026-04-07"
    
    print(f"================ [化工/甲醇 真实的对账审计 - {query_date}] ================")
    print(f"{'代码':<12} {'开盘%':<8} {'封板%':<8} {'开盘价':<8} {'昨收价':<8} {'状态'}")
    print("-" * 70)
    
    for code in codes:
        rs = bs.query_history_k_data_plus(code, "date,code,open,close,preclose,pctChg", 
                                          start_date=query_date, end_date=query_date, 
                                          frequency="d")
        if rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
            open_p, close_p, pre_c = float(row[2]), float(row[3]), float(row[4])
            open_pct = (open_p / pre_c - 1) * 100
            current_pct = (close_p / pre_c - 1) * 100
            
            # 分类逻辑
            if open_pct > 9.8: status = "一字封死"
            elif current_pct > 9.8: status = "回封涨停"
            else: status = "分歧回落"
            
            print(f"{code:<12} {open_pct:>+7.2f}% {current_pct:>+7.2f}% {open_p:<8.2f} {pre_c:<8.2f} {status}")

    bs.logout()

if __name__ == "__main__":
    run_chemical_audit()
