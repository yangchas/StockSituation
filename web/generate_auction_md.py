import json
import codecs

data = []
with codecs.open('auction_20260225.json', 'r', 'utf-8') as f:
    raw = f.read()
    if raw.startswith('"') and raw.endswith('"'):
        raw = json.loads(raw)
    data = json.loads(raw)

data.sort(key=lambda x: float(x.get('auction_amount_yuan', x.get('amount', 0))), reverse=True)

with codecs.open('auction_analysis_20260225.md', 'w', 'utf-8') as out:
    out.write('# 2026-02-25 盘后竞价数据分析\n\n')
    out.write(f'共加载 {len(data)} 只股票的竞价记录。\n\n')
    
    out.write('## 🚀 竞价金额前20名 (主力吸筹/抢筹)\n')
    out.write('| 代码 | 名称 | 竞价涨幅% | 竞价金额(万) |\n')
    out.write('|---|---|---|---|\n')
    for item in data[:20]:
        code = item.get('symbol', item.get('code', ''))
        name = item.get('name', '')
        pct = float(item.get('change_pct', 0))
        amt_w = float(item.get('auction_amount_yuan', item.get('amount', 0))) / 10000
        out.write(f'| {code} | {name} | {pct:.2f}% | {amt_w:.0f}万 |\n')
        
    limit_ups = [x for x in data if float(x.get('change_pct', 0)) >= 9.0]
    limit_ups.sort(key=lambda x: float(x.get('auction_amount_yuan', x.get('amount', 0))), reverse=True)
    out.write('\n## 🔥 竞价涨停/准涨停 (涨幅>9%) 前15名\n')
    out.write('| 代码 | 名称 | 竞价涨幅% | 竞价金额(万) |\n')
    out.write('|---|---|---|---|\n')
    for item in limit_ups[:15]:
        code = item.get('symbol', item.get('code', ''))
        name = item.get('name', '')
        pct = float(item.get('change_pct', 0))
        amt_w = float(item.get('auction_amount_yuan', item.get('amount', 0))) / 10000
        out.write(f'| {code} | {name} | {pct:.2f}% | {amt_w:.0f}万 |\n')
        
    strong_bids = []
    for x in data:
        amt = float(x.get('auction_amount_yuan', x.get('amount', 0)))
        bid_amt = float(x.get('bid_amount_yuan', x.get('bid_amount', 0)))
        if bid_amt > amt * 1.5 and amt > 10000000:
            strong_bids.append(x)
    strong_bids.sort(key=lambda x: float(x.get('bid_amount_yuan', x.get('bid_amount', 0))), reverse=True)
    
    out.write('\n## 💪 强封单 (封单>竞价额1.5倍 且 竞价额>1000万) 前15名\n')
    out.write('| 代码 | 名称 | 竞价涨幅% | 竞价金额(万) | 封单金额(万) |\n')
    out.write('|---|---|---|---|---|\n')
    for item in strong_bids[:15]:
        code = item.get('symbol', item.get('code', ''))
        name = item.get('name', '')
        pct = float(item.get('change_pct', 0))
        amt_w = float(item.get('auction_amount_yuan', item.get('amount', 0))) / 10000
        bid_w = float(item.get('bid_amount_yuan', item.get('bid_amount', 0))) / 10000
        out.write(f'| {code} | {name} | {pct:.2f}% | {amt_w:.0f}万 | {bid_w:.0f}万 |\n')

print("Report generated successfully.")
