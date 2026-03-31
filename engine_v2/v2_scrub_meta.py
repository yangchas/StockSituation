import sys
import json
import redis
from collections import defaultdict

# 路径对齐
sys.path.append("/usr/local/lib/python3.9/site-packages")

def fix_rotation_logic():
    # 强制修正 PlateResolver 逻辑或直接模拟 3/27 数据
    # 我们直接修改 v2_auction_analyzer.py 进行逻辑增强
    pass

if __name__ == "__main__":
    # 执行一次 3/27 的深度探测，确认美诺华的 Reason
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    # 获取 3/27 的底座（包含 Reason）
    raw_base = r.get("market:base:20260327")
    if raw_base:
        base = json.loads(raw_base)
        for s in base:
             if s['code'] == '603538':
                 print(f"🎯 美诺华 3/27 原始 Reason: {s.get('plate', 'Unknown')}")
