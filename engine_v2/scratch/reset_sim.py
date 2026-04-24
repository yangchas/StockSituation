
import redis
import os

def reset_redis():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    keys = [
        "market:state:age_days",
        "market:state:last_phase",
        "market:mainline_sector"
    ]
    for k in keys:
        if r.exists(k):
            r.delete(k)
            print(f"CLEANED: {k}")
    print("Simulation hygiene complete. Pulse returned to Day 1.")

if __name__ == "__main__":
    reset_redis()
