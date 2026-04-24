import os
import sys
import json
import redis
from types import SimpleNamespace

CWD = r"d:\work\Go"
os.chdir(CWD)
sys.path.append(CWD)

from ai.API.StockAnalyzer import StockAnalyzer

def probe_all():
    # 1. Redis 物理探测
    print("--- 1. Redis 物理探测 ---")
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    key = "market:auction:20260331:latest"
    try:
        k_type = r.type(key)
        print(f"Key: {key} | Type: {k_type}")
        if k_type == 'hash':
            h_keys = r.hkeys(key)
            print(f"Hash Keys: {h_keys}")
            if 'top_amount' in h_keys:
                val = r.hget(key, 'top_amount')
                print(f"Sample 'top_amount' length: {len(val) if val else 0}")
        elif k_type == 'string':
            val = r.get(key)
            print(f"String Length: {len(val) if val else 0}")
    except Exception as e:
        print(f"Redis Probe Error: {e}")

    # 2. API 全量涨停探测
    print("\n--- 2. API 全量涨停探测 ---")
    analyzer = StockAnalyzer()
    date = "2026-03-30"
    
    # 探测不同 ban 参数
    for b_val in ['1', '0', None, '']:
        try:
            print(f"Trying ban='{b_val}'...")
            res = analyzer._call_api('getHisBans', date=date, ban=b_val, size=100)
            if res and 'info' in res:
                stocks = res['info'][0] if len(res['info']) > 0 else []
                # 检查 info[0] 是不是列表，如果是，里面的元素是不是也是列表
                if stocks and isinstance(stocks, list) and isinstance(stocks[0], list):
                    print(f"✅ Found {len(stocks)} stocks for ban='{b_val}'")
                    # 打印样例判定索引
                    s0 = stocks[0]
                    # [0] code, [1] name, [10] plate, [25] lb_days
                    print(f"   Sample: Code={s0[0]}, Name={s0[1]}, Plate={s0[10]}, LB={s0[25]}")
                else:
                    print(f"⚠️ Unexpected list depth for ban='{b_val}': {type(stocks)}")
            else:
                print(f"❌ No 'info' field for ban='{b_val}'")
        except Exception as e:
            print(f"API Probe Error (ban={b_val}): {e}")

probe_all()
