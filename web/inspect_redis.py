
import redis
import json

def inspect_redis():
    try:
        r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        # Check any 2026-03-23 keys
        keys = r.keys("*2026-03-23*")
        print(f"Found {len(keys)} total keys for 2026-03-23")
        for k in sorted(keys):
            print(f"Key: {k}")
            # val = r.get(k)
            # print(f"Value sample: {str(val)[:100]}...")
            
        # Check other related keys mentioned in logs
        volatile = r.exists("stock:volatile_pool")
        print(f"stock:volatile_pool exists: {volatile}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_redis()
