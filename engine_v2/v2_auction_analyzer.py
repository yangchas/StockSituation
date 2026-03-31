"""
v2_auction_analyzer.py
Professional Auction & Strategic Analysis (V2.15.0 - Resonance Edition)
Logic: 4D COLLISION. Signal Robustness, Volume Profile, and Confidence Scoring.
"""
from __future__ import annotations
import json
import logging
import redis
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from v2_business_logic import V2BusinessLogicService, EmotionPhaseResult, YesterdayStateProfile

# 日志配置
logger = logging.getLogger("V3Analyzer")

@dataclass
class StrategicSignal:
    code: str
    name: str
    action: str # 买入/持有/减仓/止损/观望
    confidence: float # 0-100
    reason: str
    is_fake_signal: bool = False

@dataclass
class AuctionStock:
    code: str
    name: str = ""
    change_pct: float = 0.0
    auction_amount: float = 0.0
    lb_days: int = 0
    is_yest_limit: bool = False
    plate: str = "" 
    expected_pct: float = 0.0     
    is_super_expected: bool = False
    open_pct: float = 0.0 # 竞价开盘涨幅 (用于盘中对照)
    current_pct: float = 0.0 # 当前实时涨幅
    momentum_delta: float = 0.0 # 盘中对冲 (Current - Open)
    volume_intensity: float = 1.0 # 量能强度 (相对倍率)
    speed_1m: float = 0.0 # 1分钟涨速
    amount_2m: float = 0.0 # 2分钟成交额 (新增)
    resonance_factor: float = 1.0 # 板块共振因子 (新增)

@dataclass
class AuctionReport:
    date_str: str
    mode: str = "AUCTION" # AUCTION or INTRA_DAY
    all_stocks: List[AuctionStock] = field(default_factory=list)
    money_making_effect: float = 0.0
    cycle_phase: str = ""
    strategy_advice: str = ""
    total_amount: float = 0.0
    limit_up_cnt: int = 0
    limit_down_cnt: int = 0
    highest_board: Optional[AuctionStock] = None
    amount_king: Optional[AuctionStock] = None
    premium_king: Optional[AuctionStock] = None
    promo_stats: Dict[int, Tuple[int, int, List[AuctionStock], List[AuctionStock]]] = field(default_factory=dict)
    yest_hot_sectors: List[Tuple[str, int, float, str, float, float]] = field(default_factory=list) # Name, Cnt, Delta, Status, Str, Flow
    strategic_signals: List[StrategicSignal] = field(default_factory=list)
    rotation_msg: str = ""
    yest_summary: str = "板块出现分级，资金内斗剧烈，龙头虽在但后排掉队。"
    battle_kpis: Dict = field(default_factory=dict) # 战役 KPI (新增)
    
    # 5级情绪系统
    emotion: Optional[EmotionPhaseResult] = None

def build_summary(report: AuctionReport) -> str:
    h_label = "智库决策支持战报 (Decision Logic V4.0)" if report.mode == "AUCTION" else "智库盘中对撞报告 (Decision Logic V4.0 印证版)"
    lines = [
        f"============================================================",
        f"📊 [{report.date_str}] {h_label}",
        f"--------------------------------------------------",
        f"💡 [情绪周期] 阶段: {report.cycle_phase} (得分: {report.money_making_effect}/10)",
    ]
    
    # Commander-Prime 指挥系统输出
    if report.battle_kpis:
        bk = report.battle_kpis
        lines.append(f"🛡️ [战前指挥] {bk.get('battle_status', 'N/A')}")
        lines.append(f"📈 [标兵体检] 晋级率:{bk['promotion_rate']:.1%} | 爆头率:{bk['headshot_rate']:.1%} | 红开率:{bk['red_open_rate']:.1%}")
        lines.append(f"💰 [封单能级] 昨日均封单: {bk['avg_bid_amt']/1e8:.2f}亿 (能级探测)")
    
    if report.emotion:
        lines.append(f"🧬 [进化状态] {report.emotion.transition_reason} | 置信度: {report.emotion.confidence*100:.0f}%")
        lines.append(f"🎯 [执行建议] 仓位上限: {report.emotion.pos_cap*100:.0f}% | 允许战法: {'、'.join(report.emotion.allowed_setups) or '空仓观望'}")
        if report.emotion.blocked_setups:
            lines.append(f"🚫 [战法回避] {'、'.join([f'<{x}>' for x in report.emotion.blocked_setups])}")
    else:
        lines.append(f"🎯 [执行策略] {report.strategy_advice}")
    
    lines.extend([
        f"📝 [昨日总结] {report.yest_summary}",
        f"--------------------------------------------------"
    ])
    
    # 统计信息
    label_amt = "竞价额" if report.mode == "AUCTION" else "成交额"
    lines.append(f"💰 [统计] {label_amt}:{report.total_amount/1e8:.1f}亿 | 涨停:{report.limit_up_cnt}支 | 跌停:{report.limit_down_cnt}支")
    
    # 极值信息
    if report.amount_king and report.premium_king:
        lines.append(f"🔥 [竞价极值] 成交额之霸: {report.amount_king.name}({report.amount_king.auction_amount/1e8:.2f}亿), 强势溢价王: {report.premium_king.name}({report.premium_king.open_pct*100:+.1f}%)")
    
    # 市场总龙
    hb = report.highest_board
    if hb:
        # 状态判定: 用 open_pct 和 current_pct 中的较大值判断是否封板
        ref_pct = max(hb.open_pct, hb.current_pct)
        if ref_pct >= 0.098:
            status = "[一字坚挺]"
        elif hb.momentum_delta > 0.02:
            status = "[上攻确认]"
        elif hb.open_pct >= hb.expected_pct:
            status = "[超预期]"
        else:
            status = "[不及预期]"
        lines.append(f"👑 [市场总龙] {hb.code}({hb.name}) | {hb.plate} | {hb.lb_days}B | 开盘:{hb.open_pct*100:+.2f}% | 状态:{status}")

    # 阵型图
    lines.append(f"\n🧱 [阵型图 (强弱极值对撞)]")
    lines.append(f"   梯队       昨日       红开       红开率")
    lines.append(f"   ------------------------------------------------------------")
    for level in sorted(report.promo_stats.keys(), reverse=True):
        total, red_open, strongs, weaks = report.promo_stats[level]
        rate = f"{red_open/total*100:.1f}%" if total > 0 else "0%"
        lines.append(f"   {level}B->{level+1}B     {total:<10} {red_open:<10} {rate}")
        
        # 增加标兵、深水详情 (取前2个)
        if strongs:
            s_detail = "、".join([f"{s.name}({s.open_pct*100:+.1f}% | {s.auction_amount/1e8:.2f}亿)" for s in strongs[:2]])
            lines.append(f"    ↳ 💎 [标兵]: {s_detail}")
        if weaks:
            w_detail = "、".join([f"{s.name}({s.open_pct*100:+.1f}% | {s.plate})" for s in weaks[:2]])
            lines.append(f"    ↳ 📉 [深水]: {w_detail}")
        if level > 1: lines.append("") # 梯队间隔

    # 热门板块反馈 (Commander-Prime 增强)
    if report.yest_hot_sectors:
        lines.append(f"\n📂 [资金基因反馈 - 板块热力图]")
        lines.append(f"   板块          涨停  强度(Str)  主力净流(亿)  反馈")
        lines.append(f"   ------------------------------------------------------------")
        for name, count, delta, st, strength, flow in report.yest_hot_sectors:
            feedback = "🔥 极强" if strength > 3000 else ("✅ 走强" if "正常" in st or "走强" in st else "⚠️ 走弱")
            lines.append(f"   {name:<12} {count:<3}  {strength:<8.0f}  {flow/1e8:<10.2f}  {feedback}")

    # 轮动信号
    if report.rotation_msg:
        lines.append(f"\n🚨 [轮动信号告警]")
        lines.append(f"   {report.rotation_msg}")

    # 作战指令 (新增)
    if report.strategic_signals:
        lines.append(f"\n🚀 [作战指令 - 预期差捕捉]")
        for sig in report.strategic_signals[:5]:
            lines.append(f"   {sig.action} | {sig.name} ({sig.confidence}%): {sig.reason}")

    lines.append(f"============================================================")
    return "\n".join(lines)

class AuctionAnalyzer:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.logic = V2BusinessLogicService()

    def _calc_confidence(self, s: AuctionStock, sector_delta: float, sentiment: float) -> float:
        """核心信心度模型 (4D 对撞)"""
        score = 40.0 # 基础起步分
        
        # 1. 个股维度 (40%): 竞价溢价 vs 预期
        if s.is_super_expected: score += 20
        score += max(-10, min(20, (s.open_pct - s.expected_pct) * 100))
        
        # 2. 板块维度 (30%): 板块平均溢价差
        score += max(-15, min(30, sector_delta * 200))
        
        # 3. 情绪维度 (30%): 全场赚钱效应 (0-10)
        score += max(0, min(10, sentiment)) 
        
        return round(min(100.0, max(0.0, score)), 1)

    async def analyze(
        self, current_raw: List[Dict], auction_snapshot: Optional[Dict[str, float]] = None,
        yest_limit_map: Optional[Dict[str, AuctionStock]] = None, yest_hot_plates = None,
        date_str: str = "", battle_kpis: Dict = None
    ) -> AuctionReport:
        mode = "INTRA_DAY" if auction_snapshot else "AUCTION"
        report = AuctionReport(date_str=date_str, mode=mode, battle_kpis=battle_kpis or {})
        yest_limit_map = yest_limit_map or {}
        auction_snapshot = auction_snapshot or {}
        
        stocks: List[AuctionStock] = []
        for item in current_raw:
            code = str(item.get("code", "")).strip()[-6:]
            s = AuctionStock(
                code=code, name=item.get("name", "unknown"),
                current_pct=float(item.get("change_pct", 0)),
                auction_amount=float(item.get("auction_amount_yuan", 0)),
                plate=self.redis.hget("market:stock_plate", code) or "Other"
            )
            
            # 对比竞价
            s.open_pct = auction_snapshot.get(code, s.current_pct)
            s.momentum_delta = s.current_pct - s.open_pct
            
            if code in yest_limit_map:
                y = yest_limit_map[code]
                s.lb_days, s.is_yest_limit, s.plate = y.lb_days, True, y.plate
                s.expected_pct = 0.005 if s.lb_days == 1 else (0.02 + (s.lb_days - 2) * 0.02)
                s.is_super_expected = (s.open_pct >= s.expected_pct)
            
            # 注入实时指标 (Resonance Prime)
            s.speed_1m = float(item.get("speed_1m", 0.0))
            s.volume_intensity = float(item.get("vol_intensity", 1.0))
            s.amount_2m = float(item.get("amount_2m", 0.0))
            s.resonance_factor = float(item.get("resonance_factor", 1.0))
            
            stocks.append(s)

        report.all_stocks = stocks
        y_stocks = [s for s in stocks if s.is_yest_limit]
        if y_stocks:
            # 总龙选择: 最高板天数 > 竞价涨幅 (非动能差)
            report.highest_board = sorted(y_stocks, key=lambda x: (x.lb_days, x.open_pct), reverse=True)[0]
            red_cnt = sum(1 for s in y_stocks if s.current_pct > 0)
            report.money_making_effect = round(red_cnt / len(y_stocks) * 10, 1) if y_stocks else 0
        
        # [高级情绪指标计算]
        total_red = sum(1 for s in stocks if s.current_pct > 0.0)
        total_green = sum(1 for s in stocks if s.current_pct < -0.0)
        red_green_ratio = total_red / max(1, total_green)
        
        # 炸板率 (fade_cnt) 指标近似: 只要是曾经涨停但现在没封板的
        # 这里简化：当前涨幅在 5%~9.5% 之间但是开盘在 9.5% 以上的
        fade_cnt = sum(1 for s in stocks if s.open_pct > 0.095 and s.current_pct < 0.095)
        # 一字破板率
        one_word_stocks = [s for s in stocks if s.open_pct > 0.098]
        one_word_break_rate = sum(1 for s in one_word_stocks if s.current_pct < 0.098) / max(1, len(one_word_stocks))
        
        # 共识度 (consensus)
        consensus_score = sum(1 for s in y_stocks if s.current_pct > 0.095) * 5 # 简易分值
        
        # [情绪周期分类 - 升级为 5 阶段]
        report.emotion = self.logic.predict_market_phase(
            st_score=report.money_making_effect,
            red_green_ratio=red_green_ratio,
            max_lb=report.highest_board.lb_days if report.highest_board else 0,
            consensus_score=consensus_score,
            effectiveness=report.money_making_effect / 10.0,
            fade_count=fade_cnt,
            one_word_break_rate=one_word_break_rate
        )
        report.cycle_phase = report.emotion.phase
        report.strategy_advice = report.emotion.transition_reason # 借用字段
        report.yest_summary = self._generate_yest_summary(yest_limit_map, stocks)

        # [官榜对撞与印证]
        sector_results = {}
        if yest_hot_plates:
            # yest_hot_plates 如果是 Dict，格式为 {name: {strength, net_amount, rank}}
            # 如果是 List，则为原有的 [(name, count), ...]
            is_prime_data = isinstance(yest_hot_plates, dict)
            
            for p_name, count in (yest_hot_plates.items() if is_prime_data else yest_hot_plates):
                # 统计数据
                target_count = count.get('count', 1) if isinstance(count, dict) else count
                p_today = [s for s in stocks if s.is_yest_limit and (p_name in s.plate)]
                
                if p_today:
                    avg_delta = sum(s.current_pct - s.expected_pct for s in p_today) / len(p_today)
                    st = "✅ 印证走强" if avg_delta > 0.01 else ("⚠️ 分歧转弱" if avg_delta < -0.02 else "⚖️ 正常承接")
                    
                    # 提取 Prime 指标
                    strength = 0.0
                    flow = 0.0
                    if is_prime_data:
                        p_meta = yest_hot_plates.get(p_name, {})
                        strength = p_meta.get('strength', 0.0)
                        flow = p_meta.get('net_amount', 0.0)
                    
                    report.yest_hot_sectors.append((p_name, target_count, avg_delta, st, strength, flow))
                    sector_results[p_name] = avg_delta

        # [作战指令生成 - 核心策略引擎 (含预期差)]
        for s in stocks:
            if not s.is_yest_limit: continue
            
            # 1. 构建预期状态模型
            y_profile = YesterdayStateProfile(
                code=s.code,
                state_type="ZT_STRONG" if s.open_pct > 0.095 else "ZT_WEAK",
                change_pct=9.9, # 昨日涨停即为 9.9
                close_strength=0.9
            )
            exp_state = self.logic.evaluate_expectation_state(
                y_profile, s.open_pct * 100, s.current_pct * 100, seal_ratio=1.0
            )
            
            # 2. 股性与建议评价
            quality, cap_type = self.logic.analyze_stock_quality([], 50_000_000_00) # Mock cap
            tips = self.logic.generate_advice(s.code, s.name, exp_state, quality, cap_type)
            
            # 3. 计算综合得分与指令
            p_delta = next((v for k, v in sector_results.items() if k in s.plate), 0.0)
            conf = self._calc_confidence(s, p_delta, report.money_making_effect)
            
            # 判定指令
            action = "观望"
            if exp_state == "weak_to_strong" and conf > 70: action = "弱转强买入"
            elif exp_state == "strong_continue" and conf > 80: action = "强势持筹"
            elif exp_state == "strong_to_weak": action = "强转弱回避"
            
            if action != "观望":
                report.strategic_signals.append(StrategicSignal(
                    code=s.code, name=s.name, action=action, confidence=conf,
                    reason=" | ".join(tips) or f"板块预期差 {p_delta*100:+.1f}%"
                ))

        # [阵型审计]
        report.promo_stats = defaultdict(lambda: [0, 0, [], []])
        for s in stocks:
            if s.is_yest_limit:
                report.promo_stats[s.lb_days][0] += 1
                if s.open_pct > 0: report.promo_stats[s.lb_days][1] += 1
                # 标兵判定: 溢价 > 5% 且成交过千万
                if s.open_pct > 0.05 and s.auction_amount > 10000000:
                    report.promo_stats[s.lb_days][2].append(s)
                # 深水判定: 溢价 < -2%
                if s.open_pct < -0.02:
                    report.promo_stats[s.lb_days][3].append(s)

        # [极值分析]
        # 成交额之霸: 从有名称的股票中选取，避免 unknown
        named_stocks = [s for s in stocks if s.name and s.name != "unknown"]
        if named_stocks:
            report.amount_king = sorted(named_stocks, key=lambda x: x.auction_amount, reverse=True)[0]
        if y_stocks:
            report.premium_king = sorted(y_stocks, key=lambda x: x.open_pct, reverse=True)[0]

        # [轮动信号生成]
        if report.highest_board and report.highest_board.open_pct < report.highest_board.expected_pct:
            # 总龙不及预期，寻找切入板块
            strong_sectors = [name for name, _, _, st in report.yest_hot_sectors if "走强" in st]
            incoming = "、".join(strong_sectors[:2]) if strong_sectors else "暂未发现"
            report.rotation_msg = f"【衰退】: {report.highest_board.plate} | 【切入】: {incoming}\n   💡 [对撞]: 核心总龙 [{report.highest_board.name}] 开盘不及预期，检测到资金可能正通过‘高低切’逻辑切入至 [{incoming}] 板块。"

        report.total_amount = sum(s.auction_amount for s in stocks)
        report.limit_up_cnt = sum(1 for s in stocks if s.current_pct >= 0.095)
        report.limit_down_cnt = sum(1 for s in stocks if s.current_pct <= -0.095)
        report.summary_text = build_summary(report)
        return report

    def _generate_yest_summary(self, yest_map: Dict[str, Any], stocks: List[AuctionStock]) -> str:
        """自动化分析昨日市场状况"""
        y_stocks = [s for s in stocks if s.is_yest_limit]
        if not y_stocks: return "昨日无涨停，存量博弈为主。"
        
        # 1. 获取最高板
        highest = sorted(y_stocks, key=lambda x: x.lb_days, reverse=True)[0]
        # 2. 统计晋级率 (简版: 开盘涨幅 > 0)
        promo_rate = sum(1 for s in y_stocks if s.open_pct > 0) / len(y_stocks)
        
        summary = f"板块出现分级，资金内斗剧烈，龙头{highest.name}({highest.lb_days}B)虽在但后排掉队。"
        if promo_rate > 0.6: summary = f"昨日市场热度极高，龙头{highest.name}引领{highest.lb_days}B高度，全线普涨。"
        elif promo_rate < 0.3: summary = f"昨日亏钱效应极强，龙头{highest.name}孤掌难鸣，多数个股深水开盘。"
        
        return summary
