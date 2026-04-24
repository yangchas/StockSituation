import redis
import json
import datetime

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=False)
    
    date_str = "20260225"
    date_fmt = "2026-02-25"
    
    print(f"=== VERIFYING DATA FOR {date_str} ===")
    
    # 1. Check Highest Limit Up (连板高度)
    lb_key = f"cache:wencai:limitup_lb:{date_str}".encode()
    lb_raw = r.get(lb_key)
    if not lb_raw:
        # try without wencai
        lb_key = f"limit_up_{date_fmt}".encode()
        lb_raw = r.get(lb_key)
        if not lb_raw:
            lb_key = f"limit_up_{date_str}".encode()
            lb_raw = r.get(lb_key)

    if lb_raw:
        print(f"[OK] Found limit-up board data under key: {lb_key.decode('utf-8')}")
        try:
            lb_list = json.loads(lb_raw.decode('utf-8'))
            print(f"     Total limit-up stocks: {len(lb_list)}")
            max_lb = 0
            for item in lb_list:
                # the key might be '连板天数' or 'lb_days'
                lb = item.get("连板天数", item.get("lb_days", 0))
                try: lb = int(lb)
                except: lb = 1
                max_lb = max(max_lb, lb)
            print(f"     Highest Limit-Up (最高连板): {max_lb}")
        except Exception as e:
            print(f"     [ERROR] Failed to parse: {e}")
    else:
        print("[FAIL] Missing limit-up board data!")

    # 2. Check Market Breadth (红绿盘/涨跌幅)
    # the server might have comprehensive snapshots or we iterate keys
    print("\n[INFO] Scanning stock quotes to assess market breadth (Up/Down ratio)...")
    keys = r.keys(b"stock:quote:*")
    print(f"     Total quote keys found: {len(keys)}")
    
    up_count = 0
    down_count = 0
    flat_count = 0
    total_pct = 0.0
    valid_count = 0
    
    # Sample 100 or do all (since it's only ~5000 it's fast locally on server)
    pipe = r.pipeline()
    for k in keys:
        pipe.hget(k, b'change_pct')
    
    if keys:
        res = pipe.execute()
        for v in res:
            if v:
                try:
                    pct = float(v.decode('utf-8'))
                    if pct > 0: up_count += 1
                    elif pct < 0: down_count += 1
                    else: flat_count += 1
                    total_pct += pct
                    valid_count += 1
                except:
                    pass
        print(f"     [OK] Scanned {valid_count} valid quotes.")
        print(f"     Up: {up_count}, Down: {down_count}, Flat: {flat_count}")
        if valid_count > 0:
            print(f"     Market Average Change (大盘均涨幅): {total_pct/valid_count:.2f}%")
            print(f"     Up/Down Ratio: {up_count/max(1, down_count):.2f}")
    
    # 3. Check existing Sentiment & Comfort scores
    cem_key = f"market:comfort_exit:{date_str}".encode()
    cem_raw = r.hgetall(cem_key)
    if cem_raw:
        cem = {k.decode('utf-8'): v.decode('utf-8') for k, v in cem_raw.items()}
        print(f"\n[OK] Comfort Exit (昨日赚钱效应) Data Found:")
        print(f"     Score: {cem.get('score')}")
    else:
        print("\n[FAIL] missing market:comfort_exit")

if __name__ == '__main__':
    main()
