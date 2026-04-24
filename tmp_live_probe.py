import redis
import json

def probe_live_data():
    r = redis.Redis(host='115.190.156.240', port=6379, db=0, decode_responses=True)
    date_sh = "20260331"
    
    print(f"--- 🛰️ 实时数据链路探测 (Date: {date_sh}) ---")
    
    keys_to_check = [
        f"market:auction:{date_sh}:0925",
        f"market:auction:{date_sh}:latest"
    ]
    
    for k in keys_to_check:
        r_type = r.type(k)
        print(f"\n[Key] {k} ({r_type})")
        if r_type == 'none':
            print("  ❌ 缺失")
        elif r_type == 'hash':
            fields = r.hkeys(k)
            print(f"  ✅ 字段数: {len(fields)}")
            if "top_amount" in fields:
                val = r.hget(k, "top_amount")
                print(f"  ✅ top_amount 长度: {len(val) if val else 0}")
        elif r_type == 'string':
            val = r.get(k)
            print(f"  ✅ 内容长度: {len(val) if val else 0}")

probe_live_data()
