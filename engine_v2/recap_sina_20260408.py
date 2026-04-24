import urllib.request
import logging
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SinaAudit")

def get_sina_data(code_list):
    """通过新浪接口获取个股实时行情"""
    # 构建请求代码列表
    sina_codes = []
    for code in code_list:
        prefix = "sh" if code.startswith("6") else "sz"
        sina_codes.append(f"{prefix}{code}")
    
    url = "http://hq.sinajs.cn/list=" + ",".join(sina_codes)
    headers = {"Referer": "http://finance.sina.com.cn"}
    
    req = urllib.request.Request(url, headers=headers)
    results = {}
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read().decode('gbk')
            for line in data.strip().split('\n'):
                if '="' in line:
                    # var hq_str_sh600488="name,open,pre_close,price,high,low..."
                    code_part, info = line.split('="')
                    code = code_part[-6:]
                    info = info.strip('";')
                    if info:
                        fields = info.split(',')
                        if len(fields) > 10:
                            results[code] = {
                                "name": fields[0],
                                "open": float(fields[1]),
                                "pre_close": float(fields[2]),
                                "close": float(fields[3]),
                                "high": float(fields[4]),
                                "low": float(fields[5]),
                                "amount": float(fields[9])
                            }
    except Exception as e:
        logger.error(f"Sina API fetch failed: {e}")
        
    return results

async def audit_recap():
    date_str = "最新实时行情"
    targets = {
        "600488": "津药药业", "000586": "汇源通信", "000720": "新能泰山", 
        "603687": "大胜达", "000062": "深圳华强", "002957": "科瑞技术",
        "002980": "华盛昌", "002119": "康强电子", "000890": "法尔胜",
        "000990": "诚志股份", "300927": "江天化学", "002109": "兴化股份",
        "002408": "齐翔腾达", "603968": "醋化股份"
    }
    
    logger.info(f"🚀 启动 Sina 实时网络行情复盘 (Date: {date_str})...")
    raw_data = get_sina_data(list(targets.keys()))
    
    if not raw_data:
        logger.error("❌ 未能获取到 Sina 数据")
        return

    print("\n" + "="*95)
    print(f"{'代码':<8} {'名称':<10} {'昨收价':<8} {'开盘%':<8} {'最高%':<8} {'最低%':<8} {'最新涨幅%':<10} {'总额(亿)':<10}")
    print("-" * 95)
    
    for code, expected_name in targets.items():
        row = raw_data.get(code)
        if row and row["pre_close"] > 0:
            name = row["name"] or expected_name
            pre_close = row["pre_close"]
            open_pct = (row["open"] / pre_close - 1) * 100
            high_pct = (row["high"] / pre_close - 1) * 100
            low_pct = (row["low"] / pre_close - 1) * 100
            close_pct = (row["close"] / pre_close - 1) * 100
            amount_e = row["amount"] / 1e8
            
            # 使用符号和格式化
            print(f"{code:<8} {name:<10} {pre_close:<7.2f} {open_pct:>7.2f}% {high_pct:>7.2f}% {low_pct:>7.2f}% {close_pct:>9.2f}% {amount_e:>10.2f}亿")
        else:
            print(f"{code:<8} {expected_name:<10} 数据未就绪")

    print("="*95)

if __name__ == "__main__":
    asyncio.run(audit_recap())
