import asyncio
import sys
import os
import json
import logging
from datetime import datetime, date
import redis

# 环境对齐与服务发现
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path: sys.path.append(BASE_DIR)

# [V40.6] 强制控制台输出使用 UTF-8 避免 Windows GBK 报错
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from v2_network_audit_lib import NetworkAuditLib
from web.services.trading_calendar_service import TradingCalendarService

# 日志配置
log_dir = os.path.join(BASE_DIR, "logs")
if not os.path.exists(log_dir): os.makedirs(log_dir)
log_file = os.path.join(log_dir, "recap.log")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("RecapEngine")

class RecapEngine:
    def __init__(self):
        # 增加服务器级 Redis 鲁棒连接
        redis_args = {
            "db": 0, "decode_responses": True,
            "socket_timeout": 5, "socket_connect_timeout": 5
        }
        self.redis = redis.Redis(host='127.0.0.1', port=6379, **redis_args)
        try:
            self.redis.ping()
            logger.info("✅ 成功连接至物理服务器 Redis (127.0.0.1)")
        except:
            logger.warning("⚠️ 远程 Redis 连接失败，重用 127.0.0.1")
            self.redis = redis.Redis(host='127.0.0.1', port=6379, **redis_args)

        self.audit_lib = NetworkAuditLib()
        # [V41.4] 诊断对齐：检查核心方法是否存在 (防止远程同步延迟导致的 AttributeError)
        required_methods = ['get_kaipan_yest_bans', 'get_kaipan_hot_plates', 'get_wencai_limit_truth']
        missing = [m for m in required_methods if not hasattr(self.audit_lib, m)]
        if missing:
            logger.error(f"❌ 审计库版本冲突！缺失方法: {missing}")
            logger.error(f"📂 库文件加载路径: {getattr(sys.modules['v2_network_audit_lib'], '__file__', 'Unknown')}")
            logger.error(f"🛠️ 当前实例可用方法: {[m for m in dir(self.audit_lib) if not m.startswith('_')]}")
            # 尝试通过 __dict__ 路径二次确认是否存在
            raise AttributeError(f"NetworkAuditLib 缺失核心审计方法: {missing}。请检查文件同步状态。")
        
        logger.info("✅ 审计库方法自检通过，版本对位成功")
        self.calendar = TradingCalendarService()

    async def run_audit(self, target_date: str):
        logger.info(f"🏁 开启【物理对冲复盘】任务 | 审计日期: {target_date}")
        
        # 🚀 [V35.0] 对位对齐：V30.0+ 架构使用 ISO 日期作为 Key
        snap_key = f"market:snapshot:{target_date}"
        raw_signals = self.redis.get(snap_key)
        
        # 降级尝试：如果不存在，尝试 tag 格式
        if not raw_signals:
            tag = target_date.replace("-", "")
            snap_key = f"market:snapshot:{tag}:signals"
            raw_signals = self.redis.get(snap_key)

        if not raw_signals:
            logger.warning(f"⚠️ 未找到 {target_date} ({snap_key}) 的早盘指令快照，仅执行全场题材真实对账")
            signals = []
        else:
            raw_data = json.loads(raw_signals)
            signals = raw_data.get('signals', []) if isinstance(raw_data, dict) else raw_data
            
        codes = [s['code'] for s in signals]
        
        # 2. 网络穿透：获取市场真实定音数据
        logger.info(f"📡 穿透网络 API (V40.7 物理定音版) ...")
        
        yest_date = self.calendar.get_previous_trade_day(target_date)
        
        # [KPL] 获取昨日 KPL 历史全量涨停池 (作为基准)
        yest_bans = await self.audit_lib.get_kaipan_yest_bans(yest_date)
        yest_plates = await self.audit_lib.get_kaipan_hot_plates(yest_date)
        
        # [KPL] 获取今日 KPL 历史热门板块 (作为今日主线基准)
        today_plates = await self.audit_lib.get_kaipan_hot_plates(target_date)
        
        # [Wencai] 获取今日涨停真相 (用于计算晋级与封板率)
        success_df, _ = await self.audit_lib.get_wencai_limit_truth(target_date)
        
        # [Sina] 信号对账用实时报价
        perf_map = await self.audit_lib.get_sina_fast_quote(codes)
        
        # 解析今日涨停真相
        today_bans = {}
        if success_df is not None and not success_df.empty:
            # [V40.8] 动态定位连板天数列，解决问财列名后缀 [YYYYMMDD] 导致的解析失败
            lb_col = next((c for c in success_df.columns if '连续' in c and '涨停' in c), None) or \
                     next((c for c in success_df.columns if '连板' in c), None)
            
            for _, row in success_df.iterrows():
                code = str(row.get('code', row.get('symbol', ''))).strip()[-6:]
                lb_val = row.get(lb_col, '1') if lb_col else '1'
                try: lb_days = int("".join(filter(str.isdigit, str(lb_val))) or 1)
                except: lb_days = 1
                today_bans[code] = lb_days
        
        success_codes = set(today_bans.keys())
        
        # 3. 报表构建
        report_lines = [
            f"# 📊 MarketEdge 物理审计复盘 ({target_date})",
            f"\n> [审计时间]: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (V40.7 多源对冲版)",
            f"\n## 1. 核心题材迁移 (Today vs Yesterday)",
        ]
        
        # 板块对撞分析 (KPL vs KPL)
        yest_p_map = {p['name']: p['rank'] for p in yest_plates}
        report_lines.append(f"| 板块名称 | 今日排名 | 昨日排名 | 变动 | 热度指数 |")
        report_lines.append(f"| :--- | :--- | :--- | :--- | :--- |")
        for p in today_plates[:8]:
            name = p['name']
            curr_r = p['rank']
            prev_r = yest_p_map.get(name, "N/A")
            change = f"↑{prev_r - curr_r}" if isinstance(prev_r, int) and prev_r > curr_r else (f"↓{curr_r - prev_r}" if isinstance(prev_r, int) and prev_r < curr_r else "NEW")
            if curr_r == prev_r: change = "—"
            report_lines.append(f"| {name} | {curr_r} | {prev_r} | {change} | {p['hot']:.1f} |")
        
        # 4. 晋级梯队对账 (KPL vs Wencai)
        ladder_stats = {} # height -> [total, success]
        for code, info in yest_bans.items():
            h = info.get('lb_days', 1)
            if h not in ladder_stats: ladder_stats[h] = [0, 0]
            ladder_stats[h][0] += 1
            # 晋级判定：今日还在榜，且天数增加
            if (code in success_codes and today_bans.get(code, 0) > h) or (code in success_codes and h >= 9):
                ladder_stats[h][1] += 1
        
        report_lines.append(f"\n## 2. 晋级梯队对账 (KPL-Yest vs WC-Today)")
        report_lines.append(f"| 阶梯 | 样本基数 | 晋级成功 | 晋级率 |")
        report_lines.append(f"| :--- | :--- | :--- | :--- |")
        for h in sorted(ladder_stats.keys(), reverse=True):
            total_h, succ_h = ladder_stats[h]
            rate = (succ_h / total_h * 100) if total_h > 0 else 0
            report_lines.append(f"| {h}B→{h+1}B | {total_h} | {succ_h} | {rate:.1f}% |")

        report_lines.append(f"\n## 3. Alpha 信号执行对账")
        report_lines.append(f"| 代码 | 名称 | 系统指令 | 最终涨幅 | 物理状态 | 对账结论 |")
        report_lines.append(f"| :--- | :--- | :--- | :--- | :--- | :--- |")
        
        wins, total = 0, 0
        avoided_wins, avoided_total = 0, 0
        
        for sig in signals:
            code = sig['code']
            perf = perf_map.get(code, {})
            close_pct = perf.get('pct_chg', 0.0)
            is_locked = code in success_codes
            action = sig.get('action', '')
            
            # 离散状态判定
            status = "封死涨停 ✅" if is_locked else ("触板回落 ⚠️" if perf.get('high',0) > perf.get('open',0)*1.09 else "回落/吃面 ❌")
            if close_pct <= -9.8: status = "核按钮惩罚 💀"
            
            audit_res = "持平/观望"
            if any(x in action for x in ["买入", "抢筹", "打板"]):
                total += 1
                if is_locked:
                    wins += 1
                    audit_res = "🎯 完美捕捉"
                elif close_pct > 2.0: audit_res = "📈 小额获利"
                else: audit_res = "💊 诱多炸板"
            elif any(x in action for x in ["避雷", "取消", "回避", "不足", "风险"]):
                avoided_total += 1
                if close_pct < -3.0:
                    avoided_wins += 1
                    audit_res = "🛡️ 成功避雷"
                elif close_pct < 0: audit_res = "📉 预判正确"
                else: audit_res = "🔔 踏空/误判"
            
            report_lines.append(f"| {code} | {sig.get('name', 'N/A')} | {action} | {close_pct:+.2f}% | {status} | {audit_res} |")

        win_rate = (wins / total * 100) if total > 0 else 0
        avoid_rate = (avoided_wins / avoided_total * 100) if avoided_total > 0 else 0
        
        # 5. 统计总结
        report_lines.append(f"\n## 4. 统计审计")
        report_lines.append(f"*   **Alpha 买入封板率**: {win_rate:.1f}% (命中: {wins}/{total})")
        report_lines.append(f"*   **RiskSentinel 避雷成功率**: {avoid_rate:.1f}% (避开大跌: {avoided_wins}/{avoided_total})")
        report_lines.append(f"*   **综合决策置信度**: {(win_rate*0.7 + avoid_rate*0.3):.1f}%")
        
        final_report = "\n".join(report_lines)
        tag = target_date.replace("-", "")
        self.redis.set(f"market:recap:{tag}:report", final_report, ex=2592000)
        print("\n" + final_report + "\n")
        logger.info(f"✅ V40.7 审计报表已固化 (market:recap:{tag}:report)")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Audit Date (YYYY-MM-DD)")
    args = parser.parse_args()
    recap = RecapEngine()
    target_day = args.date if args.date else recap.calendar.get_previous_trade_day(date.today().strftime("%Y-%m-%d"))
    asyncio.run(recap.run_audit(target_day))