"""
v2_api_discover.py
Task: Recursive Scalar Discovery for Kaipan (2026-03-26)
"""
import sys
sys.path.append('/usr/local/lib/python3.9/site-packages/pykaipan')
from pykaipan.pykaipan import getHisBans

def recursive_probe(date_str):
    print(f"--- Recursive Truth Search for {date_str} ---")
    try:
        res = getHisBans(date=date_str, ban='1')
        info = res.get('info', [])
        if info and len(info) > 0:
            print(f"Total Pages: {len(info)}")
            first_group = info[0]
            print(f"Stocks in Page 0: {len(first_group)}")
            
            if len(first_group) > 0:
                stock_record = first_group[0]
                print(f"Stock Record Fields: {len(stock_record)}")
                
                # 1. 深度探测基础信息包 (Index 0)
                if isinstance(stock_record[0], list):
                    print("--- Sub-Item [0] (Basic Info) ---")
                    for j, val in enumerate(stock_record[0]):
                        print(f"  [0][{j}]: {val} (Type: {type(val).__name__})")
                
                # 2. 深度探测其他所有索引的标量
                print("--- Other Index Scalars ---")
                for i, field in enumerate(stock_record):
                    if i == 0: continue
                    if not isinstance(field, (list, dict)):
                        print(f"  [{i}]: {field} (Type: {type(field).__name__})")
                    elif isinstance(field, list) and len(field) < 5:
                         print(f"  [{i}]: {field} (Type: list)")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    recursive_probe("2026-03-26")
