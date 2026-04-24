import redis
import json

r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

def deep_probe(pattern):
    print(f"--- 🔍 正在扫描模式: {pattern} ---")
    keys = r.keys(pattern)
    print(f"找到匹配 Key 个数: {len(keys)}")
    
    for k in keys:
        k_type = r.type(k)
        print(f"\n[Key] {k!r} ({k_type})")
        
        if k_type == 'hash':
            h_keys = r.hkeys(k)
            print(f"   Fields: {h_keys}")
            for field in h_keys:
                val = r.hget(k, field)
                # 只有 top_amount 的数据量可能很大，单独采样
                if field == 'top_amount':
                    try:
                        data = json.loads(val)
                        print(f"   Field 'top_amount': 有效 JSON | 列表长度: {len(data)}")
                        if len(data) > 0:
                            print(f"   Data Sample [0]: {data[0]}")
                    except:
                        print(f"   Field 'top_amount': 🔴 非 JSON 数据 | 长度: {len(val) if val else 0}")
                else:
                    print(f"   Field {field!r}: {val}")
        elif k_type == 'string':
            val = r.get(k)
            print(f"   Value Length: {len(val) if val else 0}")
            print(f"   Value Start: {val[:100]!r}")
        else:
            print(f"   Type {k_type} - Skipping deep scan.")

# 探测 25 分和最新 Key
deep_probe("market:auction:20260331:0925*")
deep_probe("market:auction:20260331:latest*")
