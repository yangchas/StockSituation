import redis
import json
from collections import defaultdict

def safe_scan():
    # 采用远程物理 IP 连接
    r = redis.Redis(host='115.190.156.240', port=6379, db=0, decode_responses=True)
    try:
        # 1. 基础性能指标
        print("🔍 [Redis] 正在抓取实时连接与内存快照...")
        info = r.info()
        print(f"--- 核心状态 ---")
        print(f"Redis Version: {info['redis_version']}")
        print(f"Used Memory: {info['used_memory_human']} / Peak: {info['used_memory_peak_human']}")
        print(f"Total Clients: {info['connected_clients']}")
        print(f"Ops/Sec: {info['instantaneous_ops_per_sec']}")
        
        # 2. Key 前缀权重统计 (使用 SCAN 避免阻塞)
        print(f"\n🔍 [Scan] 正在通过迭代器扫描数据分布 (前缀聚合)...")
        stats = defaultdict(int)
        type_stats = defaultdict(int)
        
        # 定义核心业务前缀
        prefixes = [
            'stock:quote:', 'market:auction:', 'cache:chip_peaks:', 
            'cache:stock_extra:', 'market:sentiment:', 'tdengine:sync:',
            'kaipanla:'
        ]
        
        count = 0
        for key in r.scan_iter(match='*', count=500):
            count += 1
            matched = False
            for p in prefixes:
                if key.startswith(p):
                    stats[p] += 1
                    matched = True
                    break
            if not matched:
                parts = key.split(':')
                if len(parts) > 1:
                    stats[parts[0]+":"] += 1
                else:
                    stats['other'] += 1
            
            # 抽样前 100 个 Key 的类型分布
            if count <= 100:
                type_stats[str(r.type(key))] += 1
                
        print(f"--- 数据分布 (Total Scan Sample: {count}) ---")
        sorted_stats = sorted(stats.items(), key=lambda x: x[1], reverse=True)
        print(f"{'PREFIX':<25} {'COUNT':<10}")
        print("-" * 35)
        for p, c in sorted_stats[:15]:
            print(f"{p:<25} {c:<10}")

        # 3. 抽样大 Key (针对 Hash 结构)
        print(f"\n🔍 [BigKeys] 正在抽样大标的...")
        # 查找一些典型 Hash 的字段数
        sample_hashes = [
            f"market:auction:{info.get('run_id','0925')}:latest",
            "stock:quote:sh.600000"
        ]
        for h in sample_hashes:
            try:
                if r.exists(h):
                    print(f"[Hash] {h} field_count: {r.hlen(h)}")
            except: pass

    except Exception as e:
        print(f"❌ Diagnostic Failure: {e}")

if __name__ == "__main__":
    safe_scan()
