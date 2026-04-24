import redis, json, sys

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("=== 1. Auction Data Check ===")
for suffix in ['0920','0924','0925','latest','wencai']:
    key = f"market:auction:20260319:{suffix}"
    exists = r.exists(key)
    if exists:
        data = r.hget(key, 'top_amount')
        count = len(json.loads(data)) if data else 0
        print(f"  {key}: EXISTS, items={count}")
        if data and count > 0:
            items = json.loads(data)
            for it in items[:3]:
                print(f"    sample: {it}")
    else:
        print(f"  {key}: MISSING")

print()
print("=== 2. volatile_pool Check ===")
vp = r.exists('stock:volatile_pool')
print(f"  stock:volatile_pool exists: {vp}")

print()
print("=== 3. Emotion/Phase State ===")
for k in ['market:edge:emotion_phase','market:edge:trading_plan','market:edge:auction_summary','market:edge:strategy_tag']:
    val = r.get(k)
    if val:
        print(f"  {k}: {str(val)[:250]}")
    else:
        hval = r.hgetall(k)
        if hval:
            print(f"  {k} (hash): {str(hval)[:250]}")
        else:
            print(f"  {k}: MISSING")

print()
print("=== 4. Diag Keys ===")
diag_keys = r.keys('diag:*')
for dk in sorted(diag_keys)[:15]:
    val = r.get(dk)
    print(f"  {dk}: {val}")

print()
print("=== 5. Quote Spot Check ===")
for code in ['000601','603687','600821','688106']:
    q = r.hgetall(f'stock:quote:{code}')
    if q:
        print(f"  {code}: price={q.get('price','?')}, chg={q.get('change_pct',q.get('change','?'))}")

print()
print("=== 6. t1.exe Tick Freshness ===")
tk = r.get('market:tick:last_update')
print(f"  market:tick:last_update: {tk}")

print("DONE")
