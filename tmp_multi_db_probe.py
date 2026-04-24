import redis

print("--- 🌍 Redis 全局 DB 搜救行动 ---")
found_any = False

for db_id in range(16):
    r = redis.Redis(host='127.0.0.1', port=6379, db=db_id, decode_responses=True)
    try:
        keys = r.keys("*auction*")
        if keys:
            print(f"✅ 在 DB {db_id} 找到 {len(keys)} 个 Key!")
            for k in keys[:5]: # 只打前 5 个
                print(f"   - {k} ({r.type(k)})")
            found_any = True
    except Exception as e:
        pass # 忽略部分 DB 未开启的情况

if not found_any:
    print("❌ 即使遍历了 0-15 号库，依然没有发现任何包含 'auction' 的 Key。")
    print("⚠️ 请检查 Redis 监听端口或容器映射是否正确。")
