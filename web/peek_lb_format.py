import redis
import pickle
import json

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=False)
    
    lb_raw = r.get(b"cache:limit_up_2026-02-24")
    if not lb_raw: return
    
    print(f"Data type: {type(lb_raw)}")
    print(f"First 100 bytes: {lb_raw[:100]}")
    
    # Try pickle
    try:
        lb_list = pickle.loads(lb_raw)
        print("Successfully loaded via pickle!")
        print(f"List length: {len(lb_list)}")
        print(f"First item: {lb_list[0]}")
    except Exception as e:
        print(f"Pickle failed: {e}")
        
    # Try json lines or multiple json objects
    try:
        s = lb_raw.decode('utf-8')
        print("Decoded as UTF-8 string.")
        try:
            val = json.loads(s)
            print("Successfully loaded via json (standard)!")
        except Exception as e:
            print(f"JSON failed: {e}")
            # print last 100
            print(f"Last 100 bytes: {s[-100:]}")
    except Exception as e:
        print(f"UTF-8 decode failed: {e}")

if __name__ == '__main__':
    main()
