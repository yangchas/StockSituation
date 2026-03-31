import urllib.request
import sys

def get_sina_data():
    # 目标核心标的 (Sina 格式)
    stocks_meta = [
        ("sz000720", "新能泰山"),
        ("sz300750", "宁德时代"),
        ("sh688525", "佰维存储"),
        ("sh603986", "兆易创新"),
        ("sz002902", "铭普光磁"),
        ("sh603601", "再升科技"),
        ("sz002192", "融捷股份"),
        ("sh603538", "美诺华")
    ]
    
    symbols = [s[0] for s in stocks_meta]
    url = f"http://hq.sinajs.cn/list={','.join(symbols)}"
    
    req = urllib.request.Request(url)
    req.add_header("Referer", "http://finance.sina.com.cn")
    
    try:
        resp = urllib.request.urlopen(req)
        data = resp.read().decode("gbk")
        
        print(f"\n{'代码':<10} {'名称':<10} {'昨日收盘':<10} {'今日收盘':<10} {'涨幅':<10} {'现状'}")
        print("-" * 65)
        
        lines = data.strip().split("\n")
        for i, line in enumerate(lines):
            try:
                # 示例: var hq_str_sz000720="新能泰山,8.450,...";
                raw = line.split('"')[1].split(',')
                if len(raw) < 4: continue
                
                name = raw[0]
                yest_close = float(raw[2])
                now_price = float(raw[3])
                change = (now_price - yest_close) / yest_close * 100 if yest_close > 0 else 0
                
                # 简单判定
                eval_str = "🌟 涨停" if change > 9.8 else ("❌ 大跌" if change < -5 else "✅ 走弱" if change < 0 else "✅ 走稳")
                print(f"{symbols[i][2:]:<12} {name:<10} {yest_close:<12.2f} {now_price:<12.2f} {change:>+6.2f}%    {eval_str}")
            except Exception as e:
                print(f"Error parsing line {i}: {e}")
                
    except Exception as e:
        print(f"Failed to fetch Sina data: {e}")

if __name__ == "__main__":
    get_sina_data()
