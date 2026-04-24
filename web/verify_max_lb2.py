import redis
import json

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=False)
    
    lb_raw = r.get(b"cache:limit_up_2026-02-24")
    if not lb_raw:
        print("No limit up data found for cache:limit_up_2026-02-24.")
        return
        
    lb_list = json.loads(lb_raw.decode('utf-8'))
    
    parsed_list = []
    for item in lb_list:
        lb = item.get("连板天数", item.get("lb_days", 0))
        try: lb = int(lb)
        except: lb = 1
        name = item.get("股票简称", item.get("name", "Unknown"))
        code = str(item.get("股票代码", item.get("code", "000000")))[:6]
        parsed_list.append((lb, code, name))
        
    parsed_list.sort(key=lambda x: x[0], reverse=True)
    
    print("=== Top Limit-Up Stocks for 2026-02-24 ===")
    for lb, code, name in parsed_list[:15]:
        print(f"[{lb} 连板] {code} {name}")

if __name__ == '__main__':
    main()
