"""
v2_auction_analyzer.py
Professional Auction & Strategic Analysis (V5.5 - Alpha Command Edition)
Logic: CHIP-RESISTANCE, VOLUME-RATIO, and ARBITRAGE ALIGNMENT.
"""
from __future__ import annotations
import json
import logging
import redis
import os
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict
from v2_business_logic import V2BusinessLogicService, EmotionPhaseResult, YesterdayStateProfile
from v2_strategy_memory_service import StrategyMemoryService, EnvironmentDNA
logger = logging.getLogger("V5Analyzer")

@dataclass
class StrategicSignal:
    code: str
    name: str
    action: str # 买入/持有/减仓/止损/观望
    confidence: float # 0-100
    reason: str
    is_fake_signal: bool = False

class AuctionStock:
    """✅ 固态对象：通过 __slots__ 严格锁定内存分配，消除 Python 3.9 下 dataclass 的隐式 __dict__ 开销"""
    __slots__ = (
        'code', 'name', 'change_pct', 'auction_amount', 'lb_days', 'is_yest_limit', 
        'plate', 'expected_pct', 'is_super_expected', 'open_pct', 'current_pct', 
        'momentum_delta', 'volume_intensity', 'speed_1m', 'amount_2m', 
        'resonance_factor', 'yest_amount', 'resistance_gap', 'vol_ratio', 'tags', 'seal_amount', 'price'
    )
    def __init__(self, code, name="unknown", **kwargs):
        self.code = code
        self.name = name
        self.change_pct = float(kwargs.get('change_pct', 0.0))
        self.auction_amount = float(kwargs.get('auction_amount', 0.0))
        self.lb_days = int(kwargs.get('lb_days', 0))
        self.is_yest_limit = bool(kwargs.get('is_yest_limit', False))
        self.plate = str(kwargs.get('plate', ""))
        self.expected_pct = float(kwargs.get('expected_pct', 0.0))
        self.is_super_expected = bool(kwargs.get('is_super_expected', False))
        self.open_pct = float(kwargs.get('open_pct', 0.0))
        self.current_pct = float(kwargs.get('current_pct', 0.0))
        self.momentum_delta = float(kwargs.get('momentum_delta', 0.0))
        self.volume_intensity = float(kwargs.get('volume_intensity', 1.0))
        self.speed_1m = float(kwargs.get('speed_1m', 0.0))
        self.amount_2m = float(kwargs.get('amount_2m', 0.0))
        self.resonance_factor = float(kwargs.get('resonance_factor', 1.0))
        self.yest_amount = float(kwargs.get('yest_amount', 0.0))
        self.resistance_gap = float(kwargs.get('resistance_gap', -0.1))
        self.vol_ratio = float(kwargs.get('vol_ratio', 0.0))
        self.tags = kwargs.get('tags', [])
        self.seal_amount = float(kwargs.get('seal_amount', 0.0))
        self.price = float(kwargs.get('price', 0.0))

@dataclass
class AuctionReport:
    date_str: str
    mode: str = "AUCTION" 
    all_stocks: List[AuctionStock] = field(default_factory=list)
    money_making_effect: float = 0.0
    cycle_phase: str = ""
    strategy_advice: str = ""
    total_amount: float = 0.0 
    avg_market_pct: float = 0.0 
    limit_up_cnt: int = 0
    limit_down_cnt: int = 0
    highest_board: Optional[AuctionStock] = None
    amount_king: Optional[AuctionStock] = None
    premium_king: Optional[AuctionStock] = None
    promo_stats: Dict = field(default_factory=dict)
    yest_hot_sectors: List = field(default_factory=list) 
    strategic_signals: List = field(default_factory=list)
    negative_stocks: List = field(default_factory=list)
    memory_matches: List = field(default_factory=list)
    rotation_msg: str = ""
    summary_text: str = ""
    battle_kpis: Dict = field(default_factory=dict) 
    red_green_ratio: float = 0.0
    emotion: Optional[Any] = None

def build_summary(report: AuctionReport) -> str:
    """V5.5 短线作战大屏 - 极致紧凑看板"""
    now_hm = report.battle_kpis.get('current_time', '09:25')
    data_src = report.battle_kpis.get('data_source', 'Redis')
    health_icon = "🟢" if data_src == "Redis" else ("🟡" if data_src == "WENCAI" else "🟠")
    
    # 阵型审计告警
    missing_cnt = report.battle_kpis.get('missing_auction_cnt', 0)
    audit_warn = f" | ⚠️ 竞价缺失:{missing_cnt}" if missing_cnt > 0 else ""

    lines = [
        f"================ [{now_hm}] 短线作战分析 (V5.5) ================",
        f"🌡️ [战温] 评分: {report.money_making_effect}/10 | 阶段: {report.cycle_phase} | 数据: {health_icon}{data_src}{audit_warn}",
        f"💰 [量能] {report.total_amount/1e8:.2f}亿 | 核心风向: {report.amount_king.name if report.amount_king else 'N/A'}({report.amount_king.auction_amount/1e8 if report.amount_king else 0:.2f}亿)",
    ]
    
    if report.emotion:
        lines.append(f"🧬 [策略] 仓位上限: {report.emotion.pos_cap*100:.0f}% | 允许战法: {'、'.join(report.emotion.allowed_setups) or '减仓'}")
    
    # 1. 高标生死簿 (3B+)
    lines.append(f"\n👑 [高标生死簿 (3B+)]")
    lines.append(f"   标的(题材)      梯队   溢价(竞)  现价(实)  状态    封单(亿)  特征")
    lines.append(f"   --------------------------------------------------------------------------------")
    leaders = [s for s in report.all_stocks if s.lb_days >= 3]
    for s in sorted(leaders, key=lambda x: x.lb_days, reverse=True):
        status = "封板" if s.current_pct > 0.098 else ("炸板" if s.open_pct > 0.09 and s.current_pct < 0.09 else ("走强" if s.momentum_delta > 0.01 else "分歧"))
        seal_str = f"{s.seal_amount:^9.2f}" if s.current_pct > 0.098 else f"{'-':^11}"
        tag_str = "".join([f"[{t}]" for t in s.tags[:2]])
        name_plate = f"{s.name}({s.plate[:4]})"
        lines.append(f"   {name_plate:<15} {s.lb_days}->{s.lb_days+1}B  {s.open_pct*100:+.1f}%    {s.current_pct*100:+.1f}%    {status:<8} {seal_str} {tag_str}")

    # 2. 板块异动热力
    if report.yest_hot_sectors:
        lines.append(f"\n🔥 [板块异动热力]")
        for name, count, delta, st, strength, flow in report.yest_hot_sectors[:5]:
            lines.append(f"   {name:<12} (Str:{strength:^6.0f}) -> [状态]: {st}")

    # 3. 阵型图 (梯队体检对照)
    lines.append(f"\n🧱 [阵型图 - 梯队晋级对照]")
    for level in sorted(report.promo_stats.keys(), reverse=True):
        total_yest, red_open, strongs, nuclear = report.promo_stats[level]
        if total_yest == 0: continue
        rate = f"{red_open/total_yest*100:.0f}%"
        s_info = ""
        st_label = "[极强]" if red_open/total_yest > 0.8 else ("[分歧]" if red_open/total_yest < 0.4 else "[强势]")
        if strongs:
            s = strongs[0]
            s_info = f" | [标兵]: {s.name}({s.plate[:2]}) {s.current_pct*100:+.1f}% {''.join([f'[{t}]' for t in s.tags[:1]])}"
        lines.append(f"   {level}B->{level+1}B: {red_open} / {total_yest} ({rate}) {st_label}{s_info} | 核:{len(nuclear)}")

    # 4. 物理负反馈 (风险极值)
    if report.negative_stocks:
        lines.append(f"\n📉 [物理负反馈 - 风险极值]")
        lines.append(f"   标的(题材)      开盘     现价     偏离度   风险状态")
        for s in report.negative_stocks[:5]:
            risk_state = "核按钮" if s.open_pct < -0.05 else ("弱杀跌" if s.momentum_delta < -0.02 else "不及预期")
            lines.append(f"   {s.name + '(' + s.plate[:2] + ')':<15} {s.open_pct*100:+.1f}%    {s.current_pct*100:+.1f}%    {(s.open_pct-s.expected_pct)*100:+.1f}%    [{risk_state}]")

    # 5. 记忆匹配 (智库进化)
    if report.memory_matches:
        lines.append(f"\n🧠 [记忆匹配 - 历史策略参考]")
        for match in report.memory_matches[:1]:
            lines.append(f"   💡 相似度 {match['similarity']}%: [历:{match['dna'].get('date_ref', 'N/A')}] {match['strategy']}")
            lines.append(f"   🎯 建议: {match['comment']}")

    # 6. 主线切换预警 (动能雷达)
    if any("切换" in sig.action for sig in report.strategic_signals):
        lines.append(f"\n🚨 [主线切换预报 - 强度迁移中]")
        for sig in report.strategic_signals:
            if "切换" in sig.action:
                lines.append(f"   【🚨】 {sig.name} ({sig.confidence}%): {sig.reason}")

    # 7. 作战指令 (Alpha 信号)
    if report.strategic_signals:
        lines.append(f"\n🎯 [实战指令 - 预期差捕捉]")
        for sig in report.strategic_signals[:8]:
            tag = "【建议买入】" if "买入" in sig.action else ("【封死观察】" if "观察" in sig.action else ("【炸板接力】" if "接力" in sig.action else "【补涨关注】"))
            icon = "🚀" if "买入" in sig.action else ("💎" if "关注" in sig.action or "观察" in sig.action else "🌀")
            lines.append(f"   {tag} {sig.name} ({sig.confidence}%): {icon} {sig.reason}")

    lines.append(f"============================================================")
    return "\n".join(lines)

class AuctionAnalyzer:
    def __init__(self, redis_client=None):
        self.redis = redis_client or redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.logic = V2BusinessLogicService()
        self.memory = StrategyMemoryService()
        self.plate_cache = {} # 🛠️ 外部注入或批量拉取的板块映射
        self.last_plates_strength = {}    
        self.auction_plates_strength = {} 

    def _calc_confidence(self, s: AuctionStock, sector_delta: float, sentiment: float) -> float:
        score = 40.0
        # 🛠️ 优化 A: 低价因子红利 (Price < 6元 且处于热门板块)
        if 0 < s.price < 6.0 and sector_delta > 0.01:
            score += 10.0
            s.tags.append("低价优势")
            
        if s.is_super_expected: score += 20
        # 筹码压制惩罚
        if s.resistance_gap > 0.1: score -= 15
        elif s.resistance_gap < 0.05: score += 10
        # 量能增益
        if s.vol_ratio > 0.15: score += 15
        
        score += max(-10, min(20, (s.open_pct - s.expected_pct) * 100))
        score += max(-15, min(30, sector_delta * 200))
        score += max(0, min(10, sentiment)) 
        return round(min(99.0, max(0.0, score)), 1)

    async def analyze(
        self, current_raw: List[Dict], auction_snapshot: Optional[Dict[str, float]] = None,
        yest_limit_map: Optional[Dict[str, AuctionStock]] = None, yest_hot_plates = None,
        date_str: str = "", battle_kpis: Dict = None, alpha_candidates: Optional[Dict] = None,
        allowed_setups: Optional[List[str]] = None
    ) -> AuctionReport:
        mode = "INTRA_DAY" if auction_snapshot else "AUCTION"
        report = AuctionReport(date_str=date_str, mode=mode, battle_kpis=battle_kpis or {})
        _allowed = allowed_setups or [] # [V9.0] 激活允许战法名单
        # [作战预检] 初始化环境指标，供决策链使用
        yest_limit_map = yest_limit_map or {}
        auction_snapshot = auction_snapshot or {}
        
        # 🛠️ 优化 1: 批量拉去板块信息
        if not self.plate_cache or len(self.plate_cache) < 100:
            self.plate_cache = await self.redis.hgetall("market:stock_plate") or {}

        stocks: List[AuctionStock] = []
        for item in current_raw:
            code = str(item.get("code", "")).strip()[-6:]
            current_pct = float(item.get("change_pct", 0))
            yest_amount = float(item.get("yest_amount", 0.0))
            auction_amount = float(item.get("auction_amount_yuan", 0))
            
            s = AuctionStock(
                code=code, name=item.get("name", "unknown"),
                current_pct=current_pct,
                auction_amount=auction_amount,
                plate=self.plate_cache.get(code, "Other"), # 🛠️ 这里由 O(N) 远程请求变为 O(1) 本地内存查找
                yest_amount=yest_amount,
                resistance_gap=float(item.get("resistance_gap", 0.0)),
                price=float(item.get("price", 0.0)),
                is_yest_limit=False, lb_days=0, tags=[], seal_amount=0.0,
                expected_pct=0.0, is_super_expected=False
            )
            
            # 🛠️ 优化 B: 种子池识别
            if alpha_candidates and code in alpha_candidates:
                s.tags.append("ALPHA种子")
                s.resonance_factor *= 1.2
            s.vol_ratio = s.auction_amount / s.yest_amount if s.yest_amount > 0 else 0.0
            s.open_pct = auction_snapshot.get(code, s.current_pct)
            s.momentum_delta = s.current_pct - s.open_pct
            
            if code in yest_limit_map:
                y = yest_limit_map[code]
                s.lb_days, s.is_yest_limit, s.plate = y.lb_days, True, y.plate
                s.expected_pct = 0.005 if s.lb_days == 1 else (0.02 + (s.lb_days - 2) * 0.02)
                s.is_super_expected = (s.open_pct >= s.expected_pct)
            
            s.speed_1m = float(item.get("speed_1m", 0.0))
            s.volume_intensity = float(item.get("vol_intensity", 1.0))
            s.amount_2m = float(item.get("amount_2m", 0.0))
            s.resonance_factor = float(item.get("resonance_factor", 1.0))
            stocks.append(s)

        report.all_stocks = stocks
        y_stocks = [s for s in stocks if s.is_yest_limit]
        if y_stocks:
            report.highest_board = sorted(y_stocks, key=lambda x: (x.lb_days, x.open_pct), reverse=True)[0]
            red_cnt = sum(1 for s in y_stocks if s.current_pct > 0)
            report.money_making_effect = round(red_cnt / len(y_stocks) * 10, 1)
        
        # 情绪分阶段
        total_red = sum(1 for s in stocks if s.current_pct > 0.0)
        total_green = sum(1 for s in stocks if s.current_pct < -0.0)
        red_green_ratio = total_red / max(1, total_green)
        report.red_green_ratio = red_green_ratio
        report.emotion = self.logic.predict_market_phase(
            st_score=report.money_making_effect,
            red_green_ratio=red_green_ratio,
            max_lb=report.highest_board.lb_days if report.highest_board else 0,
            consensus_score=sum(1 for s in y_stocks if s.current_pct > 0.095) * 5,
            effectiveness=report.money_making_effect / 10.0,
            fade_count=sum(1 for s in stocks if s.open_pct > 0.095 and s.current_pct < 0.095),
            one_word_break_rate=0.0
        )
        report.cycle_phase = report.emotion.phase

        # 板块对撞
        sector_results = {}
        if yest_hot_plates:
            # 🛡️ 稳健解析：支持 dict, list of tuples, 或 list of dicts
            plates_iter = yest_hot_plates.items() if isinstance(yest_hot_plates, dict) else yest_hot_plates
            for item in plates_iter:
                # 兼容性解构
                if isinstance(item, (tuple, list)) and len(item) >= 2:
                    p_name, p_val = item[0], item[1]
                elif isinstance(item, dict):
                    p_name, p_val = item.get('name'), item
                else: 
                    continue

                if not p_name: continue
                # 判定 p_val 是否为详情字典
                target_count = p_val.get('count', 1) if isinstance(p_val, dict) else 1
                
                p_today = [s for s in stocks if s.is_yest_limit and (p_name in s.plate)]
                if p_today:
                    avg_delta = sum(s.current_pct - s.expected_pct for s in p_today) / len(p_today)
                    st = "走强" if avg_delta > 0.01 else ("分歧" if avg_delta < -0.02 else "承接")
                    p_meta = p_val if isinstance(p_val, dict) else {}
                    report.yest_hot_sectors.append((p_name, target_count, avg_delta, st, p_meta.get('strength', 0.0), p_meta.get('net_amount', 0.0)))
                    sector_results[p_name] = avg_delta

        # [作战指令 - AlphaScoring 游资引擎 (V5.5)]
        board_leaders = {s.plate for s in stocks if s.current_pct > 0.098 and s.lb_days > 0}
        for s in stocks:
            action, reason = "观望", "" # [V5.6 Fix] 初始化默认值，防止未命中信号时 Crash
            if s.lb_days == 0 and s.vol_ratio < 0.05: continue
            
            p_delta = next((v for k, v in sector_results.items() if k in s.plate), 0.0)
            conf = self._calc_confidence(s, p_delta, report.money_making_effect)
            # [V6.0 实战纠错] 判定物理封死状态与炸板状态
            is_locked = s.current_pct >= 0.098
            is_breaking = (s.open_pct >= 0.09 and s.current_pct < 0.08)

            # [V7.8] 核按钮全局熔断：严禁捕捞深水劣质基因
            # [V8.0] 动态风险过滤：种子选手在主线爆发日，放宽对低开的容忍度
            is_deteriorated = (s.open_pct < -0.05 or s.current_pct < -0.05)
            if "ALPHA种子" in s.tags and p_delta > 0.02:
                is_deteriorated = (s.open_pct < -0.07 or s.current_pct < -0.07) # 放宽至 -7%
                
            if is_deteriorated:
                action, reason = "高危避雷", f"⚡ 基因恶化: 破位杀跌 (竞价 {s.open_pct*100:+.1f}%)"
                conf = 0.0
                
            # A. 弱转强买入 (硬核门槛: 4%涨幅 + 8%量比 + 筹码无重压)
            elif s.lb_days > 0 and s.open_pct > 0.04 and s.vol_ratio > 0.08:
                if is_locked:
                    action, reason = "封死观察", f"💎 标杆封板: 量比{s.vol_ratio*100:.1f}% | 仅限排单/等待回封"
                elif is_breaking:
                    action, reason = "炸板接力", f"🌀 核心炸板: 动能切换中 | 关注是否有二波封单"
                elif s.resistance_gap < 0.05:
                    action, reason = "弱转强买入", f"🚀 暴力抢筹: 量比{s.vol_ratio*100:.1f}% | 封印解除(无压制)"
                    conf += 15
                else: 
                    action, reason = "弱转强建议", f"量比{s.vol_ratio*100:.1f}% | 筹码压制 {s.resistance_gap*100:.1f}%"
            
            # B. 补涨套利 (龙头封死 + 同板块 + 低位 + 筹码优)
            elif s.lb_days <= 1 and s.plate in board_leaders and s.vol_ratio > 0.05:
                if is_locked:
                    action, reason = "身位补位", f"💎 已封死: 板块补涨先锋"
                elif s.resistance_gap < 0.03:
                    action, reason = "补涨抢筹", f"💎 身位套利: 同板块标杆封死 | 筹码极优"
                    conf += 20
                
            # [V9.0] PatternFactory 战法工厂：覆盖 allowed_setups 触发的高置信模式
            if _allowed and action not in ("高危避雷", "观望"):
                from engine_v2.v2_quantitative_factors import PatternFactory, calc_kelly_position
                pattern = PatternFactory.match(
                    code=s.code, price=s.price,
                    open_pct=s.open_pct, vol_ratio=s.vol_ratio,
                    lb_days=s.lb_days, plate=s.plate,
                    plate_resonance=s.resonance_factor,
                    resistance_gap=s.resistance_gap,
                    is_alpha_seed=("ALPHA种子" in s.tags),
                    allowed_setups=_allowed,
                    sentiment_score=report.money_making_effect,
                )
                if pattern:
                    action = pattern["action"]
                    reason = pattern["reason"]
                    conf += pattern["conf_bonus"]
                    # 凯利仓位建议 (基于 V9.6 历史 DNA 真实胜率对准)
                    history = self.memory.match_dna(code=s.code, setups=[pattern["action"]])
                    real_win_rate = history.get(pattern["action"], {}).get("win_rate", 0.48)
                    if "ALPHA种子" in s.tags: real_win_rate = max(real_win_rate, 0.55)
                    
                    kelly_pos = calc_kelly_position(win_rate=real_win_rate)
                    reason += f" | 建议仓位: {kelly_pos*100:.1f}% (胜率:{real_win_rate:.0%})"

            # C. 强转弱回避 (不及预期 + 动能背离)
            elif s.lb_days >= 3 and s.open_pct < s.expected_pct and s.momentum_delta < -0.01:
                action, reason = "减仓回避", "⚠️ 基因衰退: 竞价不及预期 + 承接无力"
                conf -= 20
            
            if action != "观望":
                report.strategic_signals.append(StrategicSignal(code=s.code, name=s.name, action=action, confidence=conf, reason=reason))

        # [V7.3 动态强度迁移探测]
        if yest_hot_plates:
            # A. 统计当前各板块热力
            current_plates_amt = defaultdict(float)
            
            # 🛡️ 安全提取板块键列表
            if isinstance(yest_hot_plates, dict):
                ref_p_names = list(yest_hot_plates.keys())
            else:
                ref_p_names = []
                for p_it in yest_hot_plates:
                    if isinstance(p_it, (tuple, list)): ref_p_names.append(p_it[0])
                    elif isinstance(p_it, dict): ref_p_names.append(p_it.get('name'))

            for s in stocks:
                for p_name in ref_p_names:
                    if p_name and s.plate and p_name in s.plate:
                        current_plates_amt[p_name] += s.auction_amount

            # B. 计算斜率 (相比竞价锚点)
            max_slope = 1.0
            switch_sector = ""
            for p, amt in current_plates_amt.items():
                ref_amt = self.auction_plates_strength.get(p, 1e6)
                slope = amt / ref_amt
                if slope > max_slope:
                    max_slope = slope
                    switch_sector = p
                
                # 更新历史记录 (用于下一轮平滑)
                self.last_plates_strength[p] = amt
                if mode == "AUCTION" or not self.auction_plates_strength:
                    self.auction_plates_strength[p] = amt

            # C. 触发切换逻辑 (标兵 + 板块斜率 > 200% + 老主题弱)
            old_theme_weak = report.highest_board and "Weak" in report.highest_board.tags # 逻辑简化：假设已打标
            for s in stocks:
                # 寻找[冷启动标兵]：开盘平缓(-1~2%) + 量比极高(>30) + 信心强
                if -0.01 < s.open_pct < 0.02 and s.vol_ratio > 3.0:
                    sector_slope = current_plates_amt.get(s.plate, 0) / max(1e6, self.auction_plates_strength.get(s.plate, 0))
                    if sector_slope > 2.0:
                        conf = self._calc_confidence(s, sector_slope, report.money_making_effect)
                        report.strategic_signals.append(StrategicSignal(
                            code=s.code, name=s.name, action="【主线切换】", 
                            confidence=conf, 
                            reason=f"⚡ 能量换挡: [{s.plate}] 动能斜率 {sector_slope:.1f}x | 标兵 {s.name} 平开放量"
                        ))
        # [系统对焦] 过滤掉当日涨停后的样本，重点分析盘中博弈标的
        # 为了阵型图完整，这里不再过滤 is_yest_limit
        stocks = [s for s in stocks if s.current_pct < 0.11 or s.is_yest_limit]
        
        # [阵型审计] 统计缺失竞价锚点的核心标的
        missing_auction_cnt = 0
        if yest_limit_map:
            for code in yest_limit_map.keys():
                if not auction_snapshot or code not in auction_snapshot:
                    missing_auction_cnt += 1
        report.battle_kpis['missing_auction_cnt'] = missing_auction_cnt

        # [V6.2 职业极值审计]
        if stocks:
            amt_tops = sorted(stocks, key=lambda x: x.auction_amount, reverse=True)[:3]
            for s in amt_tops: s.tags.append("成交额TOP")
            
            p_tops = sorted(stocks, key=lambda x: x.current_pct, reverse=True)[:3]
            for s in p_tops: s.tags.append("涨幅TOP")
            
            locked_stocks = [s for s in stocks if s.current_pct > 0.098]
            if locked_stocks:
                seal_tops = sorted(locked_stocks, key=lambda x: x.auction_amount, reverse=True)[:3]
                for s in seal_tops: s.tags.append("封单王")
                for s in locked_stocks: s.seal_amount = s.auction_amount / 1e8

        # [阵型审计 V6.1 - 上帝视角回归]
        # 1. 统计昨日真实底座 (固定分母)
        yest_counts = defaultdict(int)
        for _, y_s in yest_limit_map.items():
            yest_counts[y_s.lb_days] += 1
            # 记录梯队最高标特征
            max_level = max(yest_counts.keys()) if yest_counts else 0

        report.promo_stats = defaultdict(lambda: [0, 0, [], []]) # [total_yest, red_open, strongs, nuclear]
        for level, count in yest_counts.items():
            report.promo_stats[level][0] = count

        # 2. 对撞映射今日实际
        for s in stocks:
            if s.lb_days == max_level and max_level > 0: s.tags.append("最高标")
            if s.resistance_gap < 0.03: s.tags.append("筹码优")
            
            if s.is_yest_limit:
                level = s.lb_days
                if s.open_pct > 0: report.promo_stats[level][1] += 1
                # 标兵判定 (高溢价 + 高额)
                if s.open_pct > 0.04 and s.auction_amount > 1e7: report.promo_stats[level][2].append(s)
                # 负反馈判定 (核按钮 <-5% 或 严重不及预期)
                if s.open_pct < -0.05 or (s.open_pct - s.expected_pct) < -0.05: 
                    report.promo_stats[level][3].append(s)
                    s.tags.append("负反馈")

        # 3. 提取全场风险极值
        yest_limit_stocks = [s for s in stocks if s.is_yest_limit]
        if yest_limit_stocks:
            report.negative_stocks = sorted(yest_limit_stocks, key=lambda x: x.open_pct)[:5]

        named_stocks = [s for s in stocks if s.name and s.name != "unknown"]
        if named_stocks: report.amount_king = sorted(named_stocks, key=lambda x: x.auction_amount, reverse=True)[0]

        # [V7.2 智库进化 - 环境 DNA 提取与匹配]
        try:
            # 提取龙头反馈 (基于最高标)
            leader_feedback = "Divergent"
            if report.highest_board:
                if report.highest_board.current_pct > 0.098: leader_feedback = "Locked"
                elif report.highest_board.open_pct > 0.04: leader_feedback = "StrongProtection"
                else: leader_feedback = "WeakProtection"
            
            # 检测是否有新题材低位涌现
            new_theme_found = any(s.lb_days == 0 and s.vol_ratio > 4.0 for s in stocks)

            current_dna = EnvironmentDNA(
                sentiment_score=report.money_making_effect,
                max_lb=report.highest_board.lb_days if report.highest_board else 0,
                leader_feedback=leader_feedback,
                top_sector_hotness=sum(s.vol_ratio for s in stocks[:20])/20.0,
                is_new_theme_emerging=new_theme_found,
                momentum_slope=max(1.0, max_slope if 'max_slope' in locals() else 1.0),
                date_ref=date_str
            )
            report.memory_matches = self.memory.find_similar(current_dna)
        except Exception as e:
            logger.warning(f"Memory Recall Failed: {e}")

        report.total_amount = sum(s.auction_amount for s in stocks)
        report.avg_market_pct = sum(s.open_pct for s in stocks) / len(stocks) if stocks else 0.0
        report.limit_up_cnt = sum(1 for s in stocks if s.current_pct >= 0.095)
        report.limit_down_cnt = sum(1 for s in stocks if s.current_pct <= -0.095)
        report.battle_kpis = {
            'current_time': time.strftime("%H:%M"),
            'up_down_ratio': f"{total_red}/{total_green}"
        }
        report.summary_text = build_summary(report)
        return report
