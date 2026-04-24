import baostock as bs, sys, csv

OUT = "d:/work/Go/audit_0408.csv"

TARGETS = {
    '600488': 'JinYaoYY',  '000586': 'HuiYuanTX', '000720': 'XinNengTS',
    '603687': 'DaShengDa', '000062': 'SZHuaQiang', '002957': 'KeRuiJS',
    '002980': 'HuaShengC', '002119': 'KangQiangDZ','000890': 'FaErSheng'
}

bs.login()
rows = [["code","name","pre_close","open","high","low","close","pct_chg","status"]]
for code, name in TARGETS.items():
    prefix = 'sh' if code.startswith('6') else 'sz'
    rs = bs.query_history_k_data_plus(f"{prefix}.{code}",
        "date,open,high,low,close,preclose,pctChg",
        "2026-04-08","2026-04-08","d","3")
    if rs.next():
        r = rs.get_row_data()
        op,hi,lo,cl,pc,pct = float(r[1]),float(r[2]),float(r[3]),float(r[4]),float(r[5]),float(r[6])
        lim = round(pc*1.10, 2)
        if pct <= -9.9:     st = "LIMIT_DOWN"
        elif cl >= lim-0.01: st = "LIMIT_UP_LOCKED"
        elif hi >= lim-0.01: st = "HIT_THEN_DROP"
        elif pct > 0:        st = f"UP_{pct:.2f}"
        else:                st = f"DOWN_{pct:.2f}"
        rows.append([code, name, pc, op, hi, lo, cl, pct, st])

bs.logout()

with open(OUT, "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(rows)

print(f"Saved to {OUT}")


def perform_audit():
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Login failed: {lg.error_msg}")
        return

    print("\n" + "="*78)
    print(f"{'Code':<10} {'Name':<13} {'PreCls':>7} {'High':>7} {'Close':>7} {'Chg%':>7}  Status")
    print("-"*78)

    for code, name in TARGETS.items():
        prefix = 'sh' if code.startswith('6') else 'sz'
        rs = bs.query_history_k_data_plus(
            f"{prefix}.{code}",
            "date,open,high,low,close,preclose,pctChg",
            "2026-04-08", "2026-04-08", "d", "3"
        )
        if rs.next():
            row = rs.get_row_data()
            high      = float(row[2])
            close     = float(row[4])
            pre_close = float(row[5])
            pct_chg   = float(row[6])
            limit_up  = round(pre_close * 1.10, 2)

            if pct_chg <= -9.9:
                status = "LIMIT_DOWN [-]"
            elif close >= limit_up - 0.01:
                status = "LIMIT_UP_LOCKED [+]"
            elif high >= limit_up - 0.01:
                status = "HIT_LIMIT_THEN_DROPPED [~]"
            elif pct_chg > 0:
                status = f"UP +{pct_chg:.1f}%"
            else:
                status = f"DOWN {pct_chg:.1f}%"

            print(f"{code:<10} {name:<13} {pre_close:>7.2f} {high:>7.2f} {close:>7.2f} {pct_chg:>7.2f}%  {status}")
        else:
            print(f"{code:<10} {name:<13} NO DATA")

    bs.logout()
    print("="*78)

if __name__ == "__main__":
    perform_audit()
