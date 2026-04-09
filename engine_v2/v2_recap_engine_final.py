
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

from v2_network_audit_lib import NetworkAuditLib
from web.services.trading_calendar_service import TradingCalendarService

# 日志配置 (服务器对齐版)
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
        # 增加服务器级 Redis 鲁棒连接 (含重试与超时控制)
        redis_args = {
            "db": 0, "decode_responses": True,
            "socket_timeout": 5, "socket_connect_timeout": 5,
            "retry_on_timeout": True
        }
        self.redis = redis.Redis(host='127.0.0.1', port=6379, **redis_args)
        try:
            self.redis.ping()
            logger.info("✅ 成功连接至物理服务器 Redis (127.0.0.1)")
        except:
            logger.warning("⚠️ 远程 Redis 连接失败，重试")
            self.redis = redis.Redis(host='127.0.0.1', port=6379, **redis_args)

        self.audit_lib = NetworkAuditLib()
        self.calendar = TradingCalendarService()

    async def run_audit(self, target_date: str):
        logger.info(f"🏁 开启【物理对冲复盘】任务 | 审计日期: {target_date}")
        
        # 1. 指令回拾 (从 Redis 快照加载 09:30 的战略决策)
        tag = target_date.replace("-", "")
        snap_key = f"market:snapshot:{tag}:signals"
        raw_signals = self.redis.get(snap_key)
        
        if not raw_signals:
            logger.warning(f"⚠️ 未找到 {target_date} 的早盘指令快照，无法执行对账。")
            return
        
        snap_data = json.loads(raw_signals)
        # 兼容新旧快照结构
        signals = snap_data.get('signals', snap_data) if isinstance(snap_data, dict) else snap_data
        codes = [s['code'] for s in signals]
        
        # 提取宏观元数据 (如果有)
        meta = snap_data.get('meta', {}) if isinstance(snap_data, dict) else {}
        
        # 2. 网络穿透：获取昨日全场真实主线与封板状态
        logger.info("📡 正在穿透网络 API 获取市场真实定音数据...")
        plates = await self.audit_lib.get_kaipan_hot_plates(target_date)
        perf_map = await self.audit_lib.get_sina_fast_quote(codes)
        
        # 问财真实梯队对撞 (今日晋级 vs 昨日炸板)
        success_df, fail_df = await self.audit_lib.get_wencai_limit_truth(target_date)
        success_codes = set(success_df['code'].tolist()) if success_df is not None else set()
        
        # 3. 对账审计
        report_lines = [
            f"# 📊 MarketEdge 策略审计报告 ({target_date})",
            f"\n> [审计时间]: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (T+1 物理定音版)",
            f"\n## 1. 核心主线对齐",
            "当日最强板块前五：" + " | ".join([p['name'] for p in plates[:5]]) if plates else "未获取到板块数据",
            f"\n## 2. Alpha 信号执行对账 (系统建议 vs. 市场终局)"
        ]
        
        report_lines.append(f"| 代码 | 名称 | 系统指令 | 最终涨幅 | 物理状态 | 对账结论 |")
        report_lines.append(f"| :--- | :--- | :--- | :--- | :--- | :--- |")
        
        wins, losses = 0, 0
        for sig in signals:
            code = sig['code']
            perf = perf_map.get(code, {})
            close_pct = perf.get('pct_chg', 0.0)
            
            # 物理状态判定
            is_locked = code in success_codes
            status = "封死涨停 ✅" if is_locked else ("触板回落 ⚠️" if perf.get('high',0) > perf.get('open',0)*1.09 else "回落/吃面 ❌")
            if close_pct <= -9.8: status = "核按钮惩罚 💀"
            
            # 结论审计
            audit_res = "超预期/持平"
            if "买入" in sig['action']:
                if is_locked: wins += 1; audit_res = "🎯 完美捕捉"
                else: losses += 1; audit_res = "💊 诱多炸板"
            
            report_lines.append(f"| {code} | {sig['name']} | {sig['action']} | {close_pct:+.2f}% | {status} | {audit_res} |")

        # 4. 统计与反馈建议
        accuracy = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
        report_lines.append(f"\n## 3. 统计审计与进化建议")
        report_lines.append(f"*   **Alpha 买入封板率**: {accuracy:.1f}%")
        
        if losses > 0:
            report_lines.append(f"*   **风控警告**: 存在 {losses} 只标的诱多炸板。建议检查其所属板块与 Top 5 主线的匹配度。")
        
        final_report = "\n".join(report_lines)
        print("\n\n" + final_report + "\n\n")
        
        # 保存复盘结果至 Redis
        self.redis.set(f"market:recap:{tag}:report", final_report, ex=2592000) # 保留30天
        logger.info(f"✅ 复盘报表已固化 (market:recap:{tag}:report)")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Audit Date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    recap = RecapEngine()
    target = args.date if args.date else recap.calendar.get_previous_trading_day(date.today().strftime("%Y-%m-%d"))
    asyncio.run(recap.run_audit(target))
