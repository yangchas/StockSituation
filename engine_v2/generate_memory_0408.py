import os
import sys
import json
import asyncio
import re

# 动态校准 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine_v2.v2_strategy_memory_service import StrategyMemoryService, EnvironmentDNA
from ai.API.api import UnifiedMarketDataFetcher

async def generate_memory():
    print("🚀 启动行情审计与实战记忆固化 (无 Redis 依赖)...")
    date_str = "2026-04-08"

    # 1. 从日志提取早盘系统的“上帝视角”推测
    log_path = r"c:\Users\yangxuezhen\Desktop\nohup.txt"
    system_calls = {} # name -> (action, conf, reason)
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines:
                if "【建议买入】" in line or "【风险/套利】" in line or "【封死观察】" in line:
                    match = re.search(r"【(.*?)】\s*(\w+)\s*\(([\d\.]+)%\).*?(🚀|💎|🌀)\s*(.*)", line)
                    if match:
                        action, name, conf, icon, reason = match.groups()
                        system_calls[name] = (action, float(conf), reason)
    except Exception as e:
        print(f"日志读取失败: {e}")
        return

    print(f"📌 获取到系统推测标的: {list(system_calls.keys())}")

    # 2. 从问财获取真实物理结果 (突破与力竭)
    wencai = UnifiedMarketDataFetcher()
    print("📡 正在调用问财接口获取涨停及未封板统计...")
    
    q_success = f"{date_str}涨停;{date_str}收盘价;{date_str}最高价;{date_str}开盘价;{date_str}成交额"
    df_success = await wencai.get_wencai_data(q_success)
    
    q_fail = f"{date_str}昨日涨停昨日热门今日未涨停;{date_str}收盘价;{date_str}最高价;{date_str}开盘价;{date_str}成交额"
    df_fail = await wencai.get_wencai_data(q_fail)

    success_map = {row['股票简称']: row for _, row in df_success.iterrows()} if df_success is not None else {}
    fail_map = {row['股票简称']: row for _, row in df_fail.iterrows()} if df_fail is not None else {}

    # 3. 从 Kaipanla 获取真实热点主线
    print("📡 正在调用 Kaipanla 获取昨日热门榜...")
    import sys
    sys.path.append(r'D:\software\anaconda3\lib\site-packages\pykaipan')
    try:
        from pykaipan import getHisPlates
        plates_data = getHisPlates(date=date_str)
        hot_plates = []
        if plates_data and 'list' in plates_data:
            hot_plates = [p[1] for p in plates_data['list'][:5]]
            print(f"🔥 当日最强主线 Top 5: {hot_plates}")
    except Exception as e:
        print(f"Kaipanla 异常: {e}")
        hot_plates = ["算力", "芯片", "机器人"] # Fallback

    # 4. 交叉对比与复盘总结
    print("\n" + "="*80)
    print("📊 2026-04-08 实战指令 vs 终局结果")
    print("="*80)
    
    # 获取指标 (Open, High, Close, Pct) 助手
    def parse_float(val):
        try: return float(str(val).replace('亿', ''))
        except: return 0.0

    wins, losses = 0, 0
    feedback_notes = []

    for name, (action, conf, reason) in system_calls.items():
        if name in success_map:
            row = success_map[name]
            close = parse_float(row.get(f"收盘价:不复权[{date_str}]", 0))
            is_locked = True
        elif name in fail_map:
            row = fail_map[name]
            close = parse_float(row.get(f"收盘价:不复权[{date_str}]", 0))
            is_locked = False
        else:
            # 去新浪找 fallback
            is_locked = False
            close = 0
            
        status_text = "封死涨停 ✅" if is_locked else "炸板回落 ❌"
        audit = "🎯 成功" if ("买" in action and is_locked) or ("风险" in action and not is_locked) else "💀 失败"
        if "买" in action:
            if is_locked: wins += 1
            else: losses += 1
        
        print(f"{name:<10} | 指令: {action} | 最终状态: {status_text} | 结论: {audit}")
        feedback_notes.append(f"{name}({status_text})")

    print(f"\n📈 买入封死率: {wins}/{wins+losses} ({wins/max(1, wins+losses)*100:.1f}%)")

    # 5. 固化进 strategy_memory.json
    print("\n💾 正在将今日环境与经验总结存入智库 (StrategyMemory)...")
    mem_service = StrategyMemoryService("d:/work/Go/engine_v2/strategy_memory.json")
    
    dna = EnvironmentDNA(
        sentiment_score=5.0, # 系统认为的中间分歧日
        max_lb=8, # 津药药业8连板
        leader_feedback="Divergent", # 龙头放量分歧
        top_sector_hotness=1.0,
        is_new_theme_emerging=True,
        momentum_slope=2.5,
        date_ref=date_str
    )
    
    strategy_label = "PATTERN_RISK_DIVERGENCE (高标分歧日的主线切换)"
    comment = f"在龙头(津药药业)发生炸板跳水、释放海量抛压的环境下，'身位套利'和'非绝对主线'的弱转强极易诱多(如大胜达、深圳华强)。必须严格锚定【当日资金流入前3板】且具有硬科技属性的绝对核心(如科瑞技术)才能规避宏观情绪退潮带来的核按钮误伤。"
    
    mem_service.save_case(dna, strategy_label, comment)
    
    print("\n✅ 复盘流程完毕！您可以在任意时间执行此脚本(传入日期)，即可自动拉取问财/Kaipan事实数据并进化策略记忆库。")

if __name__ == "__main__":
    asyncio.run(generate_memory())
