import json
import codecs

def analyze_auction():
    try:
        with codecs.open('auction_20260225.json', 'r', 'utf-8') as f:
            raw = f.read()
            # The raw data might be escaped string or just raw json.
            # Sometimes redis-cli returns string with quotes. Let's handle both.
            if raw.startswith('"') and raw.endswith('"'):
                raw = json.loads(raw)
            data = json.loads(raw)
            
        print(f"Loaded {len(data)} auction records for 2026-02-25.")
        
        # Sort by auction amount
        data.sort(key=lambda x: float(x.get('auction_amount_yuan', x.get('amount', 0))), reverse=True)
        
        print("\n=== 竞价金额前20名 (Top 20 by Auction Amount) ===")
        print(f"{'代码':<8} {'名称':<10} {'竞价涨幅%':<10} {'竞价金额(万)':<12}")
        for item in data[:20]:
            code = item.get('symbol', item.get('code', ''))
            name = item.get('name', '')
            pct = float(item.get('change_pct', 0))
            amt_w = float(item.get('auction_amount_yuan', item.get('amount', 0))) / 10000
            print(f"{code:<8} {name:<10} {pct:>8.2f}% {amt_w:>10.0f}万")
            
        # Top limit-up candidates (pct > 9.0) sorted by amount
        limit_ups = [x for x in data if float(x.get('change_pct', 0)) > 9.0]
        limit_ups.sort(key=lambda x: float(x.get('auction_amount_yuan', x.get('amount', 0))), reverse=True)
        
        print(f"\n=== 竞价涨停/准涨停 (涨幅>9%) Top 15 (共 {len(limit_ups)} 只) ===")
        print(f"{'代码':<8} {'名称':<10} {'竞价涨幅%':<10} {'竞价金额(万)':<12}")
        for item in limit_ups[:15]:
            code = item.get('symbol', item.get('code', ''))
            name = item.get('name', '')
            pct = float(item.get('change_pct', 0))
            amt_w = float(item.get('auction_amount_yuan', item.get('amount', 0))) / 10000
            print(f"{code:<8} {name:<10} {pct:>8.2f}% {amt_w:>10.0f}万")
            
        # Strongest bid strength (bid_amount >= auction_amount * 2, if available)
        # Some items have bid_amount_yuan or bid_amount
        strong_bids = []
        for x in data:
            amt = float(x.get('auction_amount_yuan', x.get('amount', 0)))
            bid_amt = float(x.get('bid_amount_yuan', x.get('bid_amount', 0)))
            if bid_amt > amt * 1.5 and amt > 10000000: # at least 1000w auction
                strong_bids.append(x)
                
        strong_bids.sort(key=lambda x: float(x.get('bid_amount_yuan', x.get('bid_amount', 0))), reverse=True)
        print(f"\n=== 强封单 (封单>竞价额1.5倍 且竞价额>1000万) Top 10 (共 {len(strong_bids)} 只) ===")
        print(f"{'代码':<8} {'名称':<10} {'竞价涨幅%':<10} {'竞价金额(万)':<12} {'封单金额(万)':<12}")
        for item in strong_bids[:10]:
            code = item.get('symbol', item.get('code', ''))
            name = item.get('name', '')
            pct = float(item.get('change_pct', 0))
            amt_w = float(item.get('auction_amount_yuan', item.get('amount', 0))) / 10000
            bid_w = float(item.get('bid_amount_yuan', item.get('bid_amount', 0))) / 10000
            print(f"{code:<8} {name:<10} {pct:>8.2f}% {amt_w:>10.0f}万 {bid_w:>10.0f}万")

    except Exception as e:
        print(f"Error parsing json: {e}")

if __name__ == "__main__":
    analyze_auction()
