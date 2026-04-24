import os
import sys
import json
import redis

CWD = r"d:\work\Go"
os.chdir(CWD)
sys.path.append(CWD)

from ai.API.StockAnalyzer import StockAnalyzer

def final_diagnostic():
    # 1. Redis 物理探测 (Type & Content)
    print("--- 1. Redis 物理侦察 ---")
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    key = "market:auction:20260331:latest"
    try:
        k_type = r.type(key)
        print(f"Key: {key} | Type: {k_type}")
        if k_type == 'zset':
            print(f"ZSet Range Sample: {r.zrange(key, 0, 0, withscores=True)}")
        elif k_type == 'list':
            print(f"List Sample: {r.lindex(key, 0)}")
        elif k_type == 'hash':
            print(f"Hash Keys: {r.hkeys(key)}")
        elif k_type == 'string':
            val = r.get(key)
            print(f"String Peek: {val[:100] if val else 'None'}")
    except Exception as e:
        print(f"Redis Probe Error: {e}")

    # 2. 元数据索引校准探测
    print("\n--- 2. 元数据索引校准 ---")
    analyzer = StockAnalyzer()
    date = "2026-03-30"
    try:
        res = analyzer._call_api('getHisBans', date=date, ban='1', size=10)
        if res and 'info' in res:
            stocks = res['info'][0] if len(res['info']) > 0 else []
            if stocks:
                s0 = stocks[0]
                print(f"Total length of item: {len(s0)}")
                print(f"Index [0] StockID: {s0[0]}")
                print(f"Index [1] Name: {s0[1]}")
                print(f"Index [10] Plate: {s0[10] if len(s0)>10 else 'N/A'}")
                print(f"Index [20] LB_Days: {s0[20] if len(s0)>20 else 'N/A'}")
                print(f"Index [22] ???: {s0[22] if len(s0)>22 else 'N/A'}")
    except Exception as e:
        print(f"Metadata Probe Error: {e}")

final_diagnostic()
