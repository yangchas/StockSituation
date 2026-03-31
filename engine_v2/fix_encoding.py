content = r'''"""
v2_auction_analyzer.py
Professional Auction Analysis (Board Ladder / Expectation Gaps / Precise Themes)
"""
from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict

logger = logging.getLogger("V2Auction")

@dataclass
class ExtremeSignal:
    signal_type: str
    risk_level: str
    description: str
    stocks: List[str] = field(default_factory=list)
    action_hint: str = ""

@dataclass
class AuctionStock:
    code: str
    name: str = ""
    change_pct: float = 0.0
    auction_amount: float = 0.0
    bid_amount: float = 0.0
    lb_days: int = 0
    is_yest_limit: bool = False
    plate: str = ""
    mkt_cap: float = 0.0
    expected_pct: float = 0.0     
    is_super_expected: bool = False

@dataclass
class AuctionReport:
    date_str: str
    source: str
    total_stocks: int = 0
    limit_up_cnt: int = 0
    limit_down_cnt: int = 0
    total_auction_amount: float = 0.0
    highest_board: Optional[AuctionStock] = None
    board_ladder: Dict[int, List[AuctionStock]] = field(default_factory=dict)
    promo_stats: Dict[int, Tuple[int, int]] = field(default_factory=dict)
    extreme_signals: List[ExtremeSignal] = field(default_factory=list)
    money_making_effect: float = 0.0
    sentiment_label: str = ""
    summary_text: str = ""
    all_stocks: List[AuctionStock] = field(default_factory=list)

def build_summary(report: AuctionReport) -> str:
    lines = [
        f"📊 [{report.date_str}] 老股民专业竞价分析报告",
        f"--------------------------------------------------"
    ]
    lines.append(f"🌡️ [情绪脉搏] 赚钱效应: {report.money_making_effect}/10 | {report.sentiment_label}")
    lines.append(f"   [概况] 竞价额:{report.total_auction_amount/1e8:.1f}亿 | 涨停:{report.limit_up_cnt} 只 | 跌停:{report.limit_down_cnt} 只")
    
    hb = report.highest_board
    if hb:
        status = "🌟 超预期" if hb.is_super_expected else "⏳ 弱预期"
        lines.append(f"👑 [市场总龙头] {hb.code}({hb.name}) | {hb.lb_days}板 | 开盘:{hb.change_pct*100:+.2f}% | 状态: {status}")

    lines.append(f"\n🧱 [连板梯队晋级表 (梯队结构)]")
    lines.append(f"   {'梯队等级':<8} {'昨日封板':<10} {'今日高开':<10} {'晋级率'}")
    lines.append(f"   " + "-"*45)
    for level in sorted(report.promo_stats.keys(), reverse=True):
        total, success = report.promo_stats[level]
        rate = f"{success/total*100:.1f}%" if total > 0 else "0%"
        lines.append(f"   {level}进{level+1:<5} {total:<12} {success:<12} {rate}")

    lines.append(f"\n📂 [聚焦题材分析 (去噪分析)]")
    plate_groups = defaultdict(list)
    exclude = {"央企", "银行", "证券", "融资融券", "深股通", "沪股通", "国企"}
    for s in report.all_stocks:
        if s.plate and not any(e in s.plate for e in exclude):
             plate_groups[s.plate].append(s)
             
    top_plates = sorted(plate_groups.items(), key=lambda x: sum(s.auction_amount for s in x[1]), reverse=True)[:5]
    for p_name, p_stocks in top_plates:
        total_amt = sum(s.auction_amount for s in p_stocks)
        ldr = sorted(p_stocks, key=lambda x: x.bid_amount, reverse=True)[0]
        st = "✅强" if ldr.is_super_expected else "⏳弱"
        lines.append(f"   {p_name:<11} 龙头:{ldr.name:<7} | 板块竞价:{total_amt/1e8:5.2f}亿 | 状态:{st}")

    lines.append(f"\n💎 [重点核心个股评估 (预期及格线)]")
    lines.append(f"   {'代码':<6} {'名称':<8} {'及格预期':<7} {'实际涨幅':<7} {'评价'}")
    lines.append(f"   " + "-"*50)
    core = sorted([s for s in report.all_stocks if s.is_yest_limit or s.change_pct > 0.05], 
                  key=lambda x: (x.lb_days, x.auction_amount), reverse=True)[:10]
    for s in core:
        tag = "🌟 超预期" if s.is_super_expected else "❌ 弱于预期"
        lines.append(f"   {s.code:<8} {s.name:<10} {s.expected_pct*100:>+7.1f}% {s.change_pct*100:>+7.1f}% {tag}")

    lines.append(f"\n🎯 [实战预案总结]")
    if report.money_making_effect >= 7:
        strategy = "⚡️ 强势。关注及格线上的强势股回踩介入。"
    elif report.money_making_effect <= 3:
        strategy = "🚫 危险。全线哑火，及格率低，空仓规避。"
    else:
        strategy = "⚖️ 分歧。寻找冰点博弈或核心龙头的吸纳机会。"
    lines.append(f"   - 操作路径: {strategy}")

    return "\n".join(lines)

def detect_all_extremes(stocks: List[AuctionStock], report: AuctionReport) -> List[ExtremeSignal]:
    sigs = []
    lb2_plus = [s for s in stocks if s.lb_days >= 2]
    if lb2_plus:
        crash_ratio = sum(1 for s in lb2_plus if s.change_pct < -0.01) / len(lb2_plus)
        if crash_ratio > 0.7:
             sigs.append(ExtremeSignal("LB_COLLAPSE", "HIGH", f"高位连板梯队大面积跳水({crash_ratio*100:.0f}%)，预示退潮。", "减仓"))
    return sigs

class AuctionAnalyzer:
    async def analyze(
        self,
        raw_items: List[Dict],
        source: str,
        yest_limit_map: Optional[Dict[str, AuctionStock]] = None,
        date_str: Optional[str] = None,
        metadata_provider = None,
        kaipan_analyzer = None,
    ) -> AuctionReport:
        today = date_str or date.today().strftime("%Y-%m-%d")
        report = AuctionReport(date_str=today, source=source)
        yest_limit_map = yest_limit_map or {}
        
        stocks: List[AuctionStock] = []
        for item in raw_items:
            code = str(item.get("symbol", item.get("code", ""))).strip()[-6:]
            meta = await metadata_provider.get_info(code) if metadata_provider else {}
            
            s = AuctionStock(
                code=code,
                name=meta.get("name") or str(item.get("name", "unknown")),
                change_pct=float(item.get("change_pct", 0)),
                auction_amount=float(item.get("auction_amount_yuan", item.get("bid_amount_yuan", 0))),
                bid_amount=float(item.get("bid_amount_yuan", 0)),
                plate=meta.get("plate", ""),
                mkt_cap=meta.get("mkt_cap_a", 0)
            )

            if code in yest_limit_map:
                y = yest_limit_map[code]
                s.lb_days = y.lb_days
                s.is_yest_limit = True
                s.plate = y.plate or s.plate
                s.expected_pct = 0.02 + (s.lb_days - 1) * 0.02 if s.lb_days >= 1 else 0.005
            
            s.is_super_expected = (s.change_pct >= s.expected_pct)
            stocks.append(s)

        report.promo_stats = defaultdict(lambda: [0, 0])
        # 先按板位高度排序，识别真实的最高板
        yest_stocks = sorted([s for s in stocks if s.is_yest_limit], key=lambda x: (x.lb_days, x.auction_amount), reverse=True)
        if yest_stocks:
            report.highest_board = yest_stocks[0]

        for s in stocks:
            if s.is_yest_limit:
                report.promo_stats[s.lb_days][0] += 1
                if s.change_pct >= 0.02:
                    report.promo_stats[s.lb_days][1] += 1

        report.total_auction_amount = sum(s.auction_amount for s in stocks)
        report.limit_up_cnt = sum(1 for s in stocks if s.change_pct >= 0.095)
        report.limit_down_cnt = sum(1 for s in stocks if s.change_pct <= -0.095)
        report.all_stocks = stocks
        
        y_stocks = [s for s in stocks if s.is_yest_limit]
        if y_stocks:
            succ_ratio = sum(1 for s in y_stocks if s.is_super_expected) / len(y_stocks)
            report.money_making_effect = round(succ_ratio * 10, 1)
        else:
            report.money_making_effect = 5.0

        if report.money_making_effect >= 7: report.sentiment_label = "赚钱热 🔥"
        elif report.money_making_effect >= 4: report.sentiment_label = "分歧 🌤"
        else: report.sentiment_label = "退潮 ❄"

        report.extreme_signals = detect_all_extremes(stocks, report)
        report.summary_text = build_summary(report)
        return report

async def load_auction_from_redis(redis_client, date_compact: str) -> Tuple[List[Dict], str]:
    for tag in ("0925", "0924", "wencai"):
        key = f"market:auction:{date_compact}:{tag}"
        raw = await redis_client.hget(key, "top_amount")
        if raw: return json.loads(raw), "rust" if tag != "wencai" else "wencai"
    return [], "missing"
'''

with open(r'd:\work\Go\engine_v2\v2_auction_analyzer.py', 'wb') as f:
    f.write(content.encode('utf-8'))
print("Success Writing Professional Analyzer")
