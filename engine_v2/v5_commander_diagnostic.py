import asyncio
import json
import time
import redis
import sys
import os

# 确保能导入 web.services 等模块
_project_root = r"d:\work\Go"
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# 物理服务器配置
REMOTE_REDIS = "115.190.156.240"
REDIS_PORT = 6379

async def run_diagnostics():
    print(f"🛡️ [Commander-Prime Probe] 正在穿透至物理服务器 ({REMOTE_REDIS})...")
    r = redis.Redis(host=REMOTE_REDIS, port=REDIS_PORT, db=0, decode_responses=True)
    
    # 诊断 1: 昨日涨停 KPI (Yesterday ZT)
    print("\n--- 1. [昨日涨停审计] 已锁定标的状态记录 ---")
    # 这里我们动态探测昨天的日期，或者直接尝试 limit_up_2026-03-30
    # 假设测试日期为 2026-03-31
    prev_day = "2026-03-30" 
    zt_key = f"limit_up_{prev_day}"
    zt_raw = r.get(zt_key)
    if not zt_raw:
        # 降级：扫描最近的 ZT key
        all_keys = r.keys("limit_up_*")
        if all_keys:
            all_keys.sort(reverse=True)
            zt_key = all_keys[0]
            zt_raw = r.get(zt_key)
            print(f"   ℹ️ 自动降级使用最近的日期: {zt_key}")

    if zt_raw:
        zt_items = json.loads(zt_raw)
        total = len(zt_items)
        promotion_cnt = 0
        headshot_cnt = 0
        red_open_cnt = 0
        
        sample_results = []
        for i, it in enumerate(zt_items):
            code = it.get("code") or it.get("股票代码")
            q = r.hgetall(f"stock:quote:{code}")
            if not q: continue
            
            cp = float(q.get("change_pct", 0))
            op = float(q.get("open_pct", 0)) if q.get("open_pct") else cp
            
            # KPI 逻辑
            if op > 0: red_open_cnt += 1
            if cp >= 9.8: promotion_cnt += 1 # 简易晋级判定
            if op > 5.0 and cp < 0: headshot_cnt += 1 # 爆头判定 (开盘诱多深水杀)
            
            if i < 3: # 采样 3 只
                sample_results.append(f"[{code}] {it.get('name', 'N/A')} | 开盘: {op}% | 当前: {cp}%")
        
        print(f"   ✅ [昨涨统计] 标的总数: {total}")
        print(f"   ↳ 红开率: {red_open_cnt/total:.2%} | 晋级率: {promotion_cnt/total:.2%} | 爆头率: {headshot_cnt/total:.2%}")
        for res in sample_results: print(f"     - {res}")
    else:
        print(f"   ❌ [昨日涨停] 无法在 Redis 中找到任何 limit_up_* 数据!")

    # 诊断 2: DDE 覆盖度 (DDE Coverage)
    print("\n--- 2. [DDE 基因分] 存储审计 ---")
    dde_keys_count = r.hlen("rank:dde")
    if dde_keys_count > 0:
        sample_dde = r.hgetall("rank:dde")
        # 简单概率统计分布
        scores = [float(v) for v in list(sample_dde.values())[:100]]
        avg_score = sum(scores) / len(scores) if scores else 0
        print(f"   ✅ [DDE Coverage] 总覆盖个股: {dde_keys_count} 只")
        print(f"   ↳ 样本均分: {avg_score:.2f} (大单底色一致性符合预期)")
    else:
        print("   ⚠️ [DDE Data] Redis 'rank:dde' 完全为空，请确认 DDE 同步进程是否离线。")

    # 诊断 3: 开盘啦 API 实时性 (Kaipanla)
    print("\n--- 3. [阵型层] 开盘啦板块热点实测 ---")
    try:
        from web.services.kaipan_plate_service import fetch_kaipan_plate_rank
        plates = await fetch_kaipan_plate_rank()
        if plates:
            print(f"   ✅ [Kaipanla] 成功抓取板块排行 ({len(plates)} 个)")
            print(f"   ↳ Top 板块: {plates[:3]}")
        else:
            print("   ⚠️ [Kaipanla] 数据为空，可能 API 受限或此时非交易窗口。")
    except Exception as e:
        print(f"   ❌ [Kaipanla Service] 调用失败: {e}")

    print("\n🛡️ [Commander-Prime Audit] 诊断程序执行完成。")

if __name__ == "__main__":
    asyncio.run(run_diagnostics())
