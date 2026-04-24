import redis
import json
import sys
import os

# 强制切换到 engine_v2 目录加载库
sys.path.append("/root/work/engine_v2")
from v2_wrp_final import v2_core_bridge

def server_side_audit():
    print("--- 🖥️  Resonance V5.0 服务器原位审计 ---")
    
    # 1. Redis 核心 Key 核查
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    date_sh = "20260331"
    latest_k = f"market:auction:{date_sh}:latest"
    
    print(f"\n[1/3] Redis 实时源状态 ({latest_k}):")
    t = r.type(latest_k)
    print(f"  - 类型: {t}")
    if t != 'none':
        data = r.hgetall(latest_k)
        print(f"  - 字段数量: {len(data)}")
        if len(data) > 0:
            sample = list(data.items())[0]
            print(f"  - 抽样数据: {sample}")
    else:
        print("  ❌ 警告: Redis 实时推流 Key 缺失！(这是导致所有数据源失效的主因)")

    # 2. Rust 核心引擎快照
    print(f"\n[2/3] Rust 核心 (Engine V2) 快照探测:")
    try:
        snap = v2_core_bridge.get_snapshot()
        if snap:
            print(f"  ✅ Rust 内部镜像已加载 {len(snap)} 只个股实时属性")
            # 抽样提取属性
            sample_code = next(iter(snap.keys()))
            print(f"  - [抽样 {sample_code}]: {snap[sample_code]}")
        else:
            print("  ❌ 错误: Rust 核心内存快照为空！(确认是否有 Tick 被压入)")
    except Exception as e:
        print(f"  ❌ Rust 桥接异常: {e}")

    # 3. 关联进程检测
    print(f"\n[3/3] 关联系统进程巡检:")
    # 通过 os.popen 获取关键进程
    with os.popen("ps -ef | grep -E 'python|tick' | grep -v grep") as f:
        procs = f.read().strip().split('\n')
        for p in procs:
            if 'v2' in p:
                print(f"  -> {p}")

if __name__ == "__main__":
    server_side_audit()
