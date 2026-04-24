import asyncio
import json
import time
import redis
import os
import sys

# 路径修复
_project_root = r"d:\work\Go"
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from engine_v2.market_edge_v2_core import MarketEngine

async def commander_prime_probe():
    print("🛡️ [Commander-Prime] 全量数据对撞探针启动中...")
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    engine = MarketEngine()
    
    # 模拟注册几只核心标的 (以 2026-03-31 视角)
    test_symbols = ["603538", "000001", "600519", "300750"]
    engine.register_symbols(test_symbols)
    
    print("\n--- 1. [物理层] Rust 高频指标检测 ---")
    # 模拟推送几笔 tick
    for sym in test_symbols:
        engine.push_tick(sym, 10.55, 5000000, 1000, "14:48:00", 20000000)
        engine.push_tick(sym, 10.60, 8000000, 2000, "14:48:10", 25000000)
    
    snap = engine.get_snapshot()
    for sym in test_symbols:
        s = snap.get(sym, {})
        print(f"   [{sym}] Speed: {s.get('speed', 0):.4f} | BidAmt: {s.get('bid_amt', 0)/1e8:.2f}亿 | MaxP: {s.get('max_p', 0)}")

    print("\n--- 2. [博弈层] 昨日涨停梯队还原检测 ---")
    today_str = "2026-03-31"
    # 尝试获取昨天日期
    prev_day = "2026-03-30" 
    zt_key = f"limit_up_{prev_day}"
    zt_raw = r.get(zt_key)
    if zt_raw:
        zt_list = json.loads(zt_raw)
        print(f"   ✅ [Yesterday ZT] 发现昨日涨停标的: {len(zt_list)} 只")
        # 模拟计算晋级率
        red_cnt = 0
        for it in zt_list[:10]: # 抽样前10
            code = it.get("code") or it.get("股票代码")
            q = r.hgetall(f"stock:quote:{code}")
            if q and float(q.get("change_pct", 0)) > 0: red_cnt += 1
        print(f"   📊 [KPI Preview] 抽样红开/晋级数: {red_cnt}/10")
    else:
        print("   ⚠️ [Yesterday ZT] 未发现昨日涨停数据，请检查同步链路。")

    print("\n--- 3. [资金层] DDE 基因分穿透检测 ---")
    dde_sample = r.hgetall("rank:dde")
    if dde_sample:
        print(f"   ✅ [DDE] 发现昨日资金基因数据 (Sample Count: {len(dde_sample)})")
        print(f"   ↳ 示例评分: {list(dde_sample.items())[0]}")
    else:
        print("   ⚠️ [DDE] Redis 'rank:dde' 缺失，将依赖 V5 物理代理逻辑。")

    print("\n--- 4. [阵型层] 开盘啦板块热点抓取检测 ---")
    try:
        from web.services.kaipan_plate_service import fetch_kaipan_plate_rank
        plates = await fetch_kaipan_plate_rank()
        if plates:
            print(f"   ✅ [Kaipan] 板块排名获取成功: {len(plates)} 个")
            print(f"   ↳ Top1: {plates[0]}")
        else:
            print("   ⚠️ [Kaipan] 获取为空，检查 API 状态。")
    except Exception as e:
        print(f"   ❌ [Kaipan] 导入或抓取失败: {e}")

    print("\n--- 5. [时空层] Rust 筹码分布映射检测 ---")
    for sym in test_symbols:
        # 注入一些 K 线历史
        for _ in range(30): engine.update_daily_k(sym, 10.0, 11.0, 9.0, 1000000)
        conc = engine.calculate_chip_concentration(sym, 50)
        print(f"   [{sym}] 筹码集中度 (0-1): {conc:.4f} (越小越集中)")

    print("\n🛡️ [Audit Result] 数据链路审计完成。")

if __name__ == "__main__":
    asyncio.run(commander_prime_probe())
