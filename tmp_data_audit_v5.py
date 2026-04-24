import redis
import json
from v2_wrp_final import v2_core_bridge

def deep_data_audit():
    print("--- 🩺 Resonance V5.0 数据链路深度审计 ---")
    
    # 1. Redis 测位
    r = redis.Redis(host='115.190.156.240', port=6379, db=0, decode_responses=True)
    date_sh = "20260331"
    
    print("\n[Step 1: Redis 探测]")
    keys = [f"market:auction:{date_sh}:latest", f"market:auction:{date_sh}:0925", "market:quote:latest"]
    for k in keys:
        t = r.type(k)
        print(f"  Key: {k} | Type: {t}")
        if t != 'none':
            # 抽样前3个字段
            fields = r.hkeys(k) if t == 'hash' else ["Value"]
            print(f"  -> 字段总数: {len(fields)} | 抽样: {fields[:3]}")
            
    # 2. Rust 内存透视
    print("\n[Step 2: Rust 内存快照探测]")
    try:
        snap = v2_core_bridge.get_snapshot()
        if snap:
            print(f"  ✅ Rust 内存内持有 {len(snap)} 只个股镜像")
            # 抽样万科 A (000002) 或任意一只
            sample_code = next(iter(snap.keys()))
            print(f"  -> 抽样探测 [{sample_code}]: {snap[sample_code]}")
        else:
            print("  ❌ Rust 内存快照为空！(说明没有 Tick 数据被推入 Rust)")
    except Exception as e:
        print(f"  ❌ Rust 探测异常: {e}")

    # 3. 进程活性审计
    print("\n[Step 3: 生产者进程审计]")
    # 这里通过 SSH 检查是否有 tick_ingestion 或类似的推流进程
    print("  (需通过 SSH 进一步核实 tick_ingestion.py 是否在运行)")

if __name__ == "__main__":
    deep_data_audit()
