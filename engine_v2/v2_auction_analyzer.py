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
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple, Set, Any
from collections import defaultdict
from v2_business_logic import V2BusinessLogicService, EmotionPhaseResult, YesterdayStateProfile
from v2_strategy_memory_service import StrategyMemoryService, EnvironmentDNA
from v2_quantitative_factors import PatternFactory, calc_kelly_position
logger = logging.getLogger("V5Analyzer")

@dataclass
class StrategicSignal:
    code: str
    name: str
    action: str # 买入/持有/减仓/止损/观望
    confidence: float # 0-100
    reason: str
    current_pct: float = 0.0
    plate: str = ""  # 🚀 [V29.0] 补齐板块属性用于风控映射
    is_fake_signal: bool = False

class AuctionStock:
    """✅ 固态对象：通过 __slots__ 严格锁定内存分配，消除 Python 3.9 下 dataclass 的隐式 __dict__ 开销"""
    __slots__ = (
        'code', 'name', 'change_pct', 'auction_amount', 'lb_days', 'is_yest_limit', 
        'plate', 'expected_pct', 'is_super_expected', 'open_pct', 'current_pct', 
        'momentum_delta', 'volume_intensity', 'speed_1m', 'amount_2m', 
        'resonance_factor', 'yest_amount', 'resistance_gap', 'vol_ratio', 'tags', 'seal_amount', 'price',
        'is_locked',
        # 🚀 [V39.3 Evolution] 技术与资金面扩展插槽
        'macd_hist', 'kdj_j', 'rsi_6', 'bias_20', 'ma5', 'ma10', 'ma20',
        'boll_mid', 'concentration', 'dde_3d_sum',
        # 🚀 [V39.5] 动力与历史扩展
        't2_lb_days', 't2_pct',
        # 🐉 [V41.1] 物理穿透：真实板块(名称)与实战原因
        'real_plate_names', 'sclt', 'ban_reason_text'
    )
    def __init__(self, code, name="unknown", **kwargs):
        self.code = code
        self.name = name
        self.change_pct = float(kwargs.get('change_pct', 0.0))
        self.auction_amount = float(kwargs.get('auction_amount', 0.0))
        self.lb_days = int(kwargs.get('lb_days', 0))
        self.is_yest_limit = bool(kwargs.get('is_yest_limit', False))
        self.plate = str(kwargs.get('plate', ""))
        self.real_plate_names = list(kwargs.get('real_plate_names', []))
        self.ban_reason_text = str(kwargs.get('ban_reason_text', ""))
        self.sclt = str(kwargs.get('sclt', ""))
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
        self.is_locked = bool(kwargs.get('is_locked', False))
        
        # 🚀 [V39.3 Evolution] 因子初始化
        self.macd_hist = float(kwargs.get('macd_hist', 0.0))
        self.kdj_j = float(kwargs.get('kdj_j', 50.0))
        self.rsi_6 = float(kwargs.get('rsi_6', 50.0))
        self.bias_20 = float(kwargs.get('bias_20', 0.0))
        self.ma5 = float(kwargs.get('ma5', 0.0))
        self.ma10 = float(kwargs.get('ma10', 0.0))
        self.ma20 = float(kwargs.get('ma20', 0.0))
        self.boll_mid = float(kwargs.get('boll_mid', 0.0))
        self.concentration = float(kwargs.get('concentration', 0.0))
        self.dde_3d_sum = float(kwargs.get('dde_3d_sum', 0.0))
        
        # 🚀 [V39.5] 历史身位初始化
        self.t2_lb_days = int(kwargs.get('t2_lb_days', 0))
        self.t2_pct = float(kwargs.get('t2_pct', 0.0))

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
    plate_migration: List[Dict] = field(default_factory=list) # [V40.0]
    fade_count: int = 0 # 🚀 [V41.7] 炸板数 (用于识别情绪衰竭)
    one_word_break_rate: float = 0.0 # 🚀 [V41.7] 一字断板率 (用于识别极速分歧)
    mainline_net_inflow: float = 0.0 # 💰 [V42.0] 主线板块主力净额(亿)
    mainline_change_pct: float = 0.0 # 📈 [V42.0] 主线板块涨幅(%)
    resonance_score: float = 0.0
    strategic_signals: List = field(default_factory=list)
    negative_stocks: List = field(default_factory=list)
    memory_matches: List = field(default_factory=list)
    rotation_msg: str = ""
    summary_text: str = ""
    battle_kpis: Dict = field(default_factory=dict) 
    red_green_ratio: float = 0.0
    emotion: Optional[Any] = None

def build_summary(report: AuctionReport) -> str:
    """V39.0 Command-Center Edition - 实战决策指令中心（替代无用情绪循环）"""
    from datetime import datetime
    now_hm = report.battle_kpis.get('current_time', datetime.now().strftime('%H:%M'))
    data_src = report.battle_kpis.get('data_source', 'Redis')
    health_icon = "🟢" if data_src == "Redis" else ("🟡" if data_src == "WENCAI" else "🟠")
    up_down = report.battle_kpis.get('up_down_ratio', '?/?')

    # ─── 【0】 行情态势总判 ────────────────────────────────────
    phase = report.cycle_phase or "unknown"
    emo = report.emotion
    pos_cap = f"{emo.pos_cap*100:.0f}%" if emo else "N/A"
    setups_str = "、".join(emo.allowed_setups) if emo and emo.allowed_setups else "⚠️ 暂停操作"
    
    vol_pred = report.battle_kpis.get('pred_vol', 0) / 1e8
    vol_level = report.battle_kpis.get('vol_level', '平量')
    
    missing_cnt = report.battle_kpis.get('missing_auction_cnt', 0)
    audit_warn = f"⚠️缺失:{missing_cnt} " if missing_cnt > 0 else ""
    
    # 🚀 [V40.0 Fix] 提取当前最强主线板块用于状态栏显示
    top_sector = report.plate_migration[0]['name'] if report.plate_migration else "N/A"
    
    # [V43.3] 计算样本覆盖率 (针对 5000 只标的)
    est_full_market = 5000
    coverage_rate = len(report.all_stocks) / est_full_market * 100
    
    c_factor = report.battle_kpis.get('correction_factor', 1.0)
    audit_tag = f"📊样本:{len(report.all_stocks)}" if c_factor <= 1.0 else f"🧪抽样:{len(report.all_stocks)} (x{c_factor})"
    
    lines = [
        f"",
        f"╔══════ [{now_hm}] V43.3 [Command-Center] ═══ {health_icon}{data_src} {audit_tag} {audit_warn}══╗",
        f"║ 🌡️ 情绪:{report.money_making_effect:.1f}/10  阶段:{phase:<10}  预估全天:{vol_pred:>4.0f}亿 [{vol_level}]",
        f"║ 🛡️ 仓位上限:{pos_cap:<6} 允许战法: {setups_str}",
        f"╚═══════════════════════════════════════════════════════════╝",
    ]

    # ─── 【1】 买入决策排行（核心） ─────────────────────────────
    buy_signals = [s for s in report.strategic_signals
                   if any(k in s.action for k in ("买入", "补涨", "接力", "补位", "身位", "弱转强"))
                   and s.confidence >= 40]
    buy_signals = sorted(buy_signals, key=lambda x: x.confidence, reverse=True)[:6]
    
    if buy_signals:
        lines.append(f"\n🚀 ━━━ 买入候选（按置信度排序） ━━━")
        lines.append(f"   {'标的':<8} {'现价%':>6} {'置信':>5}  {'建议仓位':>7}  {'板块':<8} 原因")
        lines.append(f"   {'─'*72}")
        for sig in buy_signals:
            # 从 reason 里提取凯利仓位，若无则按置信度估算
            kelly_match = ""
            kelly_pos = 0.0
            if "建议仓位:" in sig.reason:
                m = re.search(r'建议仓位:\s*([\d.]+)%', sig.reason)
                if m:
                    kelly_pos = float(m.group(1))
                    kelly_match = f"{kelly_pos:.0f}%"
            if not kelly_match:
                kelly_pos = min(30.0, sig.confidence * 0.3)
                kelly_match = f"{kelly_pos:.0f}%"
            
            action_icon = "🚀" if "买入" in sig.action else ("💎" if "接力" in sig.action else "⚡")
            reason_short = sig.reason[:30].rstrip()
            lines.append(
                f"   {action_icon}{sig.name:<7} {sig.current_pct*100:>+5.1f}%  {sig.confidence:>4.0f}  {kelly_match:>7}  {sig.plate[:6]:<8} {reason_short}"
            )
    else:
        lines.append(f"\n🚀 ━━━ 买入候选 ━━━  ⚠️ 当前无高置信买入信号，建议观望")

    # ─── 【2】 持仓风险警报（高危避雷 + 减仓） ────────────────────
    risk_signals = [s for s in report.strategic_signals
                    if any(k in s.action for k in ("避雷", "减仓", "回避"))]
    if risk_signals:
        lines.append(f"\n🔴 ━━━ 持仓风险警报（建议减仓/止损） ━━━")
        for sig in risk_signals[:5]:
            # [V39.8 Fix] 强行剔除名称中的 (60xxxx) 干扰，只留纯名称 + 板块
            clean_name = re.sub(r'\(?\d{6}\)?', '', sig.name)
            plate_tag = f"[{sig.plate[:4]}]" if sig.plate else ""
            display_name = f"{clean_name}{plate_tag}"
            lines.append(f"   ❌ {display_name:<16} {sig.current_pct*100:>+5.1f}%  [{sig.action}]  {sig.reason[:45]}")

    # ─── 【3】 板块能级全景 [迁徙与共振] ──────────────
    lines.append(f"\n🔥 ━━━ 板块能级全景 [迁徙与共振] ━━━")
    if report.plate_migration:
        # [V40.0] 按强度排序
        sorted_m = sorted(report.plate_migration, key=lambda x: x['strength'], reverse=True)
        lines.append(f"   {'类型':<5} {'板块':<12} {'热度':>5} {'涨幅%':>6} {'净额(亿)':>8} {'风险':<2} {'灵魂标的'}")
        lines.append(f"   {'─'*75}")
        for m in sorted_m[:6]:
            type_icon = "🔥核心" if m['type'] == 'PERSIST' else ("🆕新兴" if m['type'] == 'EMERGING' else "❄️退潮")
            m_net = f"{m['net']:>+7.1f}Y"
            m_chg = f"{m['chg']:>+5.1f}%"
            leader_info = f"{m['leader_name']}({m['leader_lb']}B)" if m['leader_name'] else "---"
            risk_icon = "🟢" if m['risk_level'] == '低' else ("🟡" if m['risk_level'] == '中' else "🔴")
            lines.append(f"   {type_icon:<5} {m['name']:<12} {m['strength']:>5.0f} {m_chg} {m_net} {risk_icon:<2} {leader_info}")
    
    # 板块切换预警
    switch_signals = [s for s in report.strategic_signals if "切换" in s.action]
    if switch_signals:
        lines.append(f"   🚨 主线切换预警: {switch_signals[0].name}  原因: {switch_signals[0].reason[:30]}")

    # ─── 【4】 龙头生死簿 (联动带动效应) ────────────────────────
    leaders = sorted([s for s in report.all_stocks if s.lb_days >= 3],
                     key=lambda x: x.lb_days, reverse=True)[:5]
    if leaders:
        lines.append(f"\n👑 ━━━ 龙头生死簿 [联动效应] ━━━")
        lines.append(f"   {'标的':<16} {'梯队':>5}  {'竞价':>6}  {'现价':>6}  {'状态':<8}  {'封单':>9}")
        lines.append(f"   {'─'*72}")
        for s in leaders:
            # 状态判定
            status = "🟢封板" if s.current_pct > 0.098 else ("💥炸板" if s.open_pct > 0.09 and s.current_pct < 0.09 else ("📈走强" if s.momentum_delta > 0.01 else "⚡分歧"))
            
            # 带动效应说明
            effect = ""
            if s.current_pct > 0.098 and s.lb_days > 4: effect = " [🔥带动回流]"
            elif s.momentum_delta < -0.05: effect = " [💀拖累退潮]"
            
            # 🚨 [P0 - Seal Safety Rating] 封单安全评级
            if s.current_pct > 0.098:
                if s.seal_amount >= 5.0:   seal_str = f"✅{s.seal_amount:.2f}亿"
                elif s.seal_amount >= 1.0: seal_str = f"⚠️{s.seal_amount:.2f}亿"
                else:                      seal_str = f"🚨{s.seal_amount:.2f}亿"
            else:
                seal_str = "   -  "
            
            # [V39.8 Fix] 强行剔除名称中的 (60xxxx) 干扰
            clean_name = re.sub(r'\(?\d{6}\)?', '', s.name)
            plate_tag = f"[{s.plate[:4]}]" if s.plate else ""
            display_name = f"{clean_name}{plate_tag}"
            lines.append(
                f"   {display_name:<16} {s.lb_days}→{s.lb_days+1}B  {s.open_pct*100:>+5.1f}%  {s.current_pct*100:>+5.1f}%  {status:<8} {seal_str:>9}{effect}"
            )

    # ─── 【5】 全场市场脉搏 ─────────────────────────────────────
    amount_king_name = re.sub(r'\(?\d{6}\)?', '', report.amount_king.name) if report.amount_king else "N/A"
    amount_king_str = f"{amount_king_name}({report.amount_king.auction_amount/1e8:.1f}亿)" if report.amount_king else "N/A"
    lines.append(f"\n📊 ━━━ 市场脉搏 ━━━")
    
    # 梯队晋级率（关键情绪评估）
    if report.promo_stats:
        for level in sorted(report.promo_stats.keys(), reverse=True):
            total_yest, promoted, strongs, nuclear, red_open = report.promo_stats[level]
            if total_yest == 0: continue
            rate = promoted / total_yest
            rate_icon = "🟢" if rate > 0.5 else ("🔴" if rate < 0.2 else "🟡")
            # [V39.8 Fix] 强行剔除名称中的 (60xxxx) 干扰
            clean_strong_name = re.sub(r'\(?\d{6}\)?', '', strongs[0].name) if strongs else "N/A"
            s_info = f" 标兵:{clean_strong_name}({strongs[0].current_pct*100:+.1f}%)" if strongs else ""
            lines.append(f"   {rate_icon} {level}B→{level+1}B: {promoted}/{total_yest} (晋级:{rate*100:.0f}%){s_info}")

    lines.append(f"   💰 量能冠军: {amount_king_str}")

    # ─── 【6】 历史记忆匹配（经验参考） ──────────────────────────
    if report.memory_matches:
        m = report.memory_matches[0]
        lines.append(f"\n🧠 ━━━ 历史相似度 {m['similarity']}% → {m['strategy']} ━━━")
        comment_short = m['comment'][:60] if len(m['comment']) > 60 else m['comment']
        lines.append(f"   {comment_short}")
        # 🚨 [P2 - 历史记忆主动警报] 若历史复盘包含风险关键词，升级为主动警告
        risk_keywords = ["严禁追涨", "危险", "回避", "陷阱", "诱多", "炸板", "崩盘"]
        if any(kw in m.get('comment', '') for kw in risk_keywords):
            lines.append(f"   ⚠️ [历史警报] 相似行情教训已触发风险拦截，当前极不建议追涨买入")

    # ─── 【7】 单行心跳（状态栏，供 \r 刷新） ────────────────────
    risk_label = "🟢稳" if report.money_making_effect >= 6 else ("🔴危" if report.money_making_effect < 4.5 else "🟡观")
    clean_hb_name = re.sub(r'\(?\d{6}\)?', '', report.highest_board.name) if report.highest_board else "N/A"
    leader_str = f"{clean_hb_name}({report.highest_board.current_pct*100:+.1f}%)" if report.highest_board else "N/A"
    heartbeat = f"\r[{now_hm}] {risk_label} 情绪:{report.money_making_effect:.1f} | 仓:{pos_cap} | 主线:{top_sector[:4]} | 龙头:{leader_str} | 买入候选:{len(buy_signals)}只   "
    
    lines.append(f"════════════════════════════════════════════════════════════")
    report.rotation_msg = heartbeat
    return "\n".join(lines)




class AuctionAnalyzer:
    def __init__(self, redis_client=None):
        self.redis = redis_client or redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.logic = V2BusinessLogicService()
        self.memory = StrategyMemoryService()
        self.plate_cache = {} 
        self.auction_plates_strength = {} 
        self.last_plates_strength = {}
        self.last_summary_hash = "" # 🚀 V19.2: 用于输出去重

    def _get_vol_ratio(self, time_str: str, mode: str = "AUCTION") -> float:
        """🚀 [V43.1] 针对万亿大市(如4/17)修正的非线性外推系数"""
        if mode == "AUCTION" or time_str <= "09:30":
            return 0.045  # [V43.1] 竞价占比修正为 4.5%，锚定 2.3万亿日成交
        
        # 针对放量主线日的深度时间衰减模型
        ratios = [
            ("09:40", 0.22),  # 前 10 分钟交易极度频发
            ("09:50", 0.28),
            ("10:00", 0.35),
            ("10:30", 0.45),
            ("11:00", 0.55),
            ("11:30", 0.65),
            ("13:30", 0.70),
            ("14:00", 0.82),
            ("14:30", 0.92),
            ("14:50", 1.00),
            ("15:00", 1.00)
        ]
        for t_limit, ratio in ratios:
            if time_str <= t_limit:
                return ratio
        return 1.0

    def _calc_confidence(self, s: AuctionStock, sector_delta: float, sentiment: float, plate_net: float = 0.0) -> float:
        """🚀 [V42.0] 四层架构置信度引擎 (Added Capital Flow Weighting)"""
        # ==========================================
        # Layer 1: 盘口基础得分 (Base Auction Score, 0-60)
        # ==========================================
        base_score = 30.0
        # 1.1 低价/热门板块加成
        if 0 < s.price < 6.0 and sector_delta > 0.01:
            base_score += 10.0; s.tags.append("低价先锋")
            
        # 1.2 超预期得分 (连板股)
        if s.is_super_expected: base_score += 15.0
        
        # 1.3 竞价活跃度 (量能密度)
        if s.vol_ratio > 0.15: base_score += 15.0
        elif s.vol_ratio > 0.08: base_score += 8.0
        
        # 1.4 瞬时偏差 (开盘价与预期价)
        base_score += max(-10, min(15, (s.open_pct - s.expected_pct) * 100))
        
        # 1.5 板块与情绪背景
        base_score += max(-10, min(20, sector_delta * 200))
        base_score += max(0, min(5, sentiment)) 

        # ==========================================
        # Layer 2: 技术趋势乘数 (Trend Multiplier, 0.5x - 1.5x)
        # ==========================================
        multiplier = 1.0
        
        # 2.1 均线对位
        if s.ma5 > s.ma10 > s.ma20 > 0: # 完美多头
            multiplier += 0.2
            s.tags.append("多头排列")
        elif s.price < s.ma20 < s.ma10 and s.ma20 > 0: # 明显下降通道
            multiplier -= 0.3
            
        # 2.2 技术指标背书
        if s.macd_hist > 0: multiplier += 0.1 # MACD 多头
        if s.kdj_j < 20: multiplier += 0.1   # 超跌安全边际
        elif s.kdj_j > 90: multiplier -= 0.1 # 超买预警
        
        # 2.3 筹码结构压制 (物理重压)
        if s.resistance_gap > 0.1: multiplier -= 0.3
        elif s.resistance_gap < 0.05: multiplier += 0.1
        
        # ==========================================
        # Layer 3: 资金面修正 (Money-Flow Overlay, +/- 20)
        # ==========================================
        overlay = 0.0
        
        # 3.1 DDE 持续性检查
        if s.dde_3d_sum > 0: 
            overlay += 10.0
            s.tags.append("机构囤货")
            
        # 3.2 逆向纠偏 (V39.3 核心)
        # 如果昨日大涨但 DDE 净流出 -> 诱多出货
        if s.change_pct > 0.05 and s.dde_3d_sum < -1e6:
            overlay -= 20.0
            s.tags.append("诱多风险")
        # 如果昨日低迷但 DDE 净流入 -> 潜伏低吸
        elif s.change_pct < -0.02 and s.dde_3d_sum > 1e6:
            overlay += 15.0
            s.tags.append("资金潜伏")

        # ==========================================
        # Layer 4: 板块资金共振修正 (Sector Capital Flow, +/- 15) [V42.0]
        # ==========================================
        if plate_net > 50:    # 超强净买 (如通信 +121亿)
            overlay += 15.0
            s.tags.append("板块热钱")
        elif plate_net > 10:  # 强净买
            overlay += 8.0
        elif plate_net < -15: # 净流出 (如算力 -22亿)
            overlay -= 10.0
            s.tags.append("板块失血")

        # 最终得分合成
        final_score = base_score * multiplier + overlay
        return round(min(99.0, max(0.0, final_score)), 1)

    async def analyze(
        self, current_raw: List[Dict], auction_snapshot: Optional[Dict[str, float]] = None,
        yest_limit_map: Optional[Dict[str, AuctionStock]] = None, 
        yest_hot_plates: Optional[Dict[str, Dict]] = None,
        today_hot_plates: Optional[Dict[str, Dict]] = None,
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
            # [V47.4 Hardening] 状态底色初始化：确保每一只票在进入任何业务逻辑前具备确定状态
            action, reason, conf = "观望", "", 0.0
            code = str(item.get("code", "")).strip()[-6:]
            current_pct = float(item.get("change_pct", 0))
            yest_amount = float(item.get("yest_amount", 0.0))
            auction_amount = float(item.get("auction_amount_yuan", 0))
            
            s = AuctionStock(
                code=code, name=item.get("name", "unknown"),
                current_pct=current_pct,
                auction_amount=auction_amount,
                plate=self.plate_cache.get(code, self.plate_cache.get(f"sz{code}", self.plate_cache.get(f"sh{code}", "Other"))), 
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
                s.lb_days, s.is_yest_limit = y.lb_days, True
                # 🐉 [V41.1] 物理穿透：依据优先级加载板块认知 (优先审计发现)
                if getattr(y, 'real_plate_names', []):
                    s.real_plate_names = y.real_plate_names
                    # P1 优先级：将首选审计题材设为核心 plate 属性，确保子系统对准
                    s.plate = s.real_plate_names[0]
                else:
                    s.plate = y.plate # P2 优先级：KPL昨日涨停自带板块
                
                s.ban_reason_text = getattr(y, 'ban_reason_text', "")
                s.sclt = getattr(y, 'sclt', "")
                s.expected_pct = 0.005 if s.lb_days == 1 else (0.02 + (s.lb_days - 2) * 0.02)
                s.is_super_expected = (s.open_pct >= s.expected_pct)
            
            s.speed_1m = float(item.get("speed_1m", 0.0))
            s.volume_intensity = float(item.get("vol_intensity", 1.0))
            s.amount_2m = float(item.get("amount_2m", 0.0))
            s.resonance_factor = float(item.get("resonance_factor", 1.0))
            stocks.append(s)

        report.all_stocks = stocks
        
        # 🚀 [V39.5 Evolution] 市场脉搏实时审计：上移至分析链内，确保看板 Summary 同步对齐
        up_cnt = sum(1 for s in stocks if s.current_pct > 0.0)
        down_cnt = sum(1 for s in stocks if s.current_pct < 0.0)
        report.battle_kpis['up_down_ratio'] = f"{up_cnt}/{down_cnt}"
        
        y_stocks = [s for s in stocks if s.is_yest_limit]
        if y_stocks:
            report.highest_board = sorted(y_stocks, key=lambda x: (x.lb_days, x.open_pct), reverse=True)[0]
            red_cnt = sum(1 for s in y_stocks if s.current_pct > 0)
            base_sentiment = round(red_cnt / len(y_stocks) * 10, 1)

            # 🚨 [P1 - Ladder Health Factor] 梯队健康系数修正
            # 统计低梯队（1B→2B）的晋级率，若过低则压制情绪虚高
            lb1_stocks = [s for s in y_stocks if s.lb_days == 1]
            lb1_promoted = sum(1 for s in lb1_stocks if s.current_pct > 0.098)
            lb1_rate = lb1_promoted / len(lb1_stocks) if lb1_stocks else 1.0
            # 封单安全系数：最高梯队若封单不足 1亿，说明封板质地很差
            top_seal = report.highest_board.seal_amount if report.highest_board else 0
            seal_penalty = 1.0 if top_seal >= 1.0 else max(0.6, top_seal)  # 封单越少，折扣越大
            # 低梯队晋级率 < 20% 时，情绪分下调 1-1.5
            ladder_penalty = 0.0
            if lb1_rate < 0.20: ladder_penalty = 1.5
            elif lb1_rate < 0.40: ladder_penalty = 0.8
            report.money_making_effect = round(max(0.0, base_sentiment * seal_penalty - ladder_penalty), 1)
            report.battle_kpis['ladder_health'] = f"{lb1_rate*100:.0f}%"  # 供看板显示
            report.battle_kpis['top_seal_amt'] = top_seal
        
        # [V47.0 Total Mirror] 非线性预判引擎与其仿真适配
        full_auc_amt = battle_kpis.get('full_market_auc_amt', 0)
        
        # 核心：量能投影逻辑
        cur_time = battle_kpis.get('current_time', '09:25')
        vol_ratio = self._get_vol_ratio(cur_time, mode=report.mode)
        
        if mode == "INTRA_DAY" and len(stocks) < 500 and full_auc_amt > 0:
            # 仿真逻辑：能级映射模式 (Scale-Aware Projection)
            # 计算当前活跃样本相对于其竞价样本的放量倍率
            sample_auc_amt = sum(s.auction_amount for s in stocks if s.source != "SIM_BASE_AURORA")
            sample_live_amt = sum(s.auction_amount for s in stocks if s.source == "SIM_LIVE_PATCH")
            
            # 放量乘数 (保持原有量纲，只应用动态缩放)
            scale_multiplier = (sample_live_amt / sample_auc_amt) if sample_auc_amt > 1e6 else 1.0
            pred_full_day = (full_auc_amt * scale_multiplier) / vol_ratio if vol_ratio > 0 else full_auc_amt / 0.045
            report.total_amount = full_auc_amt * scale_multiplier
            report.battle_kpis['simulation_scaled'] = True
        else:
            # 标准逻辑：覆盖率修正模式
            total_amt = battle_kpis.get('full_market_auc_amt', sum(s.auction_amount for s in stocks))
            report.total_amount = total_amt
            coverage_factor = 1.0
            if 800 <= len(stocks) <= 1500: coverage_factor = 0.65
            pred_full_day = (total_amt / coverage_factor) / vol_ratio if vol_ratio > 0 else total_amt / 0.045
            report.battle_kpis['coverage_factor'] = coverage_factor

        report.battle_kpis['pred_vol'] = pred_full_day
        report.battle_kpis['vol_ratio_used'] = vol_ratio
        
        avg_5d = battle_kpis.get('avg_5d_vol', 1.0e12) 
        vol_level = "放量" if pred_full_day > avg_5d * 1.1 else ("缩量" if pred_full_day < avg_5d * 0.9 else "平量")
        report.battle_kpis['vol_level'] = vol_level
        
        total_red = sum(1 for s in stocks if s.current_pct > 0.0)
        total_green = sum(1 for s in stocks if s.current_pct < -0.0)
        red_green_ratio = total_red / max(1, total_green)
        report.red_green_ratio = red_green_ratio
        # 🐉 [V41.0] 识别超级龙头封死状态
        dragon_locked = False
        dragon_plates = []
        if report.highest_board and report.highest_board.lb_days >= 5:
            # 判定是否封死：涨幅 > 9.8%
            if report.highest_board.current_pct > 0.098:
                dragon_locked = True
                dragon_plates = [report.highest_board.plate] + report.highest_board.real_plate_names

        # [V41.3 Fix] 准确计算炸板率与一字断板率 (用于识别分歧相位)
        one_word_opens = sum(1 for s in stocks if s.open_pct > 0.095)
        fade_count = sum(1 for s in stocks if s.open_pct > 0.095 and s.current_pct < 0.095)
        one_word_rate = fade_count / one_word_opens if one_word_opens > 0 else 0.0
        
        # 🚀 [V41.7] 固化指标，供 Orchestrator 及其它外部引擎直接访问
        report.fade_count = fade_count
        report.one_word_break_rate = one_word_rate

        report.emotion = self.logic.predict_market_phase(
            st_score=report.money_making_effect,
            red_green_ratio=red_green_ratio,
            max_lb=report.highest_board.lb_days if report.highest_board else 0,
            consensus_score=sum(1 for s in y_stocks if s.current_pct > 0.095) * 5,
            effectiveness=report.money_making_effect / 10.0,
            fade_count=fade_count,
            one_word_break_rate=one_word_rate,
            dragon_locked=dragon_locked,
            dragon_real_plates=dragon_plates
        )
        report.cycle_phase = report.emotion.phase

        # [V40.0] Layer 3: 炸板带动性风险评估
        plate_risk = {}
        for s in stocks:
            if s.open_pct >= 0.09 and s.current_pct < 0.08: # 炸板定义
                risk_val = s.lb_days * 0.2 + (s.open_pct - s.current_pct) * 3
                plate_risk[s.plate] = plate_risk.get(s.plate, 0.0) + risk_val

        # [V40.0] 板块迁徙与共振对撞
        if today_hot_plates:
            yest_plates = yest_hot_plates or {}
            for p_name, p_data in today_hot_plates.items():
                if not p_name: continue
                
                # 识别迁移类型
                yest_data = yest_plates.get(p_name)
                m_type = 'EMERGING' # 默认新兴
                if yest_data:
                    if p_data['rank'] <= yest_data['rank']: m_type = 'PERSIST' # 持续/增强
                    elif p_data['net_amount'] < 0: m_type = 'FADING' # 退潮
                
                # 寻找灵魂标的 (板块内 lb_days 最高)
                p_stocks = [s for s in stocks if p_name in s.plate]
                p_leader = sorted(p_stocks, key=lambda x: (x.lb_days, x.current_pct), reverse=True)[0] if p_stocks else None
                
                risk_level = "高" if plate_risk.get(p_name, 0) > 1.5 else ("中" if plate_risk.get(p_name, 0) > 0.5 else "低")
                
                # [V42.0] 提取强化字段
                p_net = float(p_data.get('net_inflow', p_data.get('net_amount', 0)))
                p_chg = float(p_data.get('change_pct', 0))

                report.plate_migration.append({
                    'name': p_name,
                    'type': m_type,
                    'strength': p_data['strength'],
                    'net': p_net,
                    'chg': p_chg,
                    'leader_name': re.sub(r'\(?\d{6}\)?', '', p_leader.name) if p_leader else None,
                    'leader_lb': p_leader.lb_days if p_leader else 0,
                    'risk_level': risk_level
                })
            
            # [V42.0] 固化主线资金指标
            if report.plate_migration:
                top_p = sorted(report.plate_migration, key=lambda x: x['strength'], reverse=True)[0]
                report.mainline_net_inflow = top_p['net']
                report.mainline_change_pct = top_p['chg']

            # [V40.0] 计算市场共振得分 (Top 5 板块平均强度)
            top_strengths = sorted([m['strength'] for m in report.plate_migration], reverse=True)[:5]
            if top_strengths:
                report.resonance_score = sum(top_strengths) / len(top_strengths)

 
        # [V40.0] 恢复真实 sector_results (基于今日板块平均涨跌)
        sector_results = {}
        for p_name in {pm['name'] for pm in report.plate_migration}:
            p_stocks = [s for s in stocks if p_name in s.plate]
            if p_stocks:
                sector_results[p_name] = sum(s.current_pct - s.expected_pct for s in p_stocks) / len(p_stocks)
        
        # [作战指令 - AlphaScoring 游资引擎 (V5.5)]

        # [作战指令 - AlphaScoring 游资引擎 (V5.5)]
        board_leaders = set()
        for s in stocks:
            if s.current_pct > 0.098 and s.lb_days > 0:
                board_leaders.add(s.plate)
                if s.real_plate_names:
                    board_leaders.update(s.real_plate_names)
        for s in stocks:
            action, reason = "观望", "" 
            if s.lb_days == 0 and s.vol_ratio < 0.05: continue
            
            # [V6.0 实战对合] 判定物理状态
            is_locked = s.current_pct >= 0.098
            is_breaking = (s.open_pct >= 0.09 and s.current_pct < 0.08)

            # [V40.0] 预期差逻辑捕捉
            exp_gap = s.open_pct - s.expected_pct
            if exp_gap > 0.03: 
                if is_locked: s.tags.append("强势延续")
                else: s.tags.append("及预期兑现")
            elif exp_gap < -0.03:
                if s.current_pct > s.open_pct + 0.02: s.tags.append("弱转强信号")
                else: s.tags.append("不及预期")
            
            # [V40.0] 预期差逻辑捕捉
            p_res = sector_results.get(s.plate, 0.0)
            # [V42.0] 获取板块净额
            p_data_final = today_hot_plates.get(s.plate, {}) if today_hot_plates else {}
            p_net_final = float(p_data_final.get('net_inflow', p_data_final.get('net_amount', 0)))
            
            confidence = self._calc_confidence(s, p_res, report.money_making_effect, plate_net=p_net_final)

            # [V7.8] 核按钮全局熔断：严禁捕捞深水劣质基因
            # [V8.0] 动态风险过滤：种子选手在主线爆发日，放宽对低开的容忍度
            is_deteriorated = (s.open_pct < -0.05 or s.current_pct < -0.05)
            if "ALPHA种子" in s.tags and p_res > 0.02:
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
                
            # [V9.0] PatternFactory 战法工厂
            history_meta = {"t2_lb_days": s.t2_lb_days, "t2_pct": s.t2_pct}
            # [V47.4 Final Fix] 逻辑闭环：移除中间层的冗余初始化，改由循环顶端统一负责
            pattern = PatternFactory.match(
                code=s.code, price=s.price,
                open_pct=s.open_pct, vol_ratio=s.vol_ratio,
                lb_days=s.lb_days, plate=s.plate,
                plate_resonance=s.resonance_factor,
                resistance_gap=s.resistance_gap,
                is_alpha_seed=("ALPHA种子" in s.tags),
                allowed_setups=_allowed,
                sentiment_score=report.money_making_effect,
                speed_1m=s.speed_1m, 
                amount_2m=s.amount_2m,
                is_intra_day=(mode == "INTRA_DAY"),
                history_meta=history_meta,
                # 🐉 [V41.0] 物理穿透：龙头状态注入
                dragon_locked=dragon_locked,
                dragon_real_plates=dragon_plates
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
                # [V40.0] Layer 5: 盈亏比与最终评分挖掘
                target_pct = 0.10 # 预期涨停
                # 简单止损计算：筹码压制位或前低 (此处简化为 2*gap)
                loss_pct = max(0.02, abs(s.resistance_gap) * 1.5)
                risk_reward = round((target_pct - (s.current_pct or 0)) / loss_pct, 1)
                
                # 确定性加成 (所属板块机会分)
                plate_bonus = 0.0
                p_info = next((pm for pm in report.plate_migration if pm['name'] in s.plate), None)
                if p_info:
                    if p_info['type'] == 'PERSIST': plate_bonus += 10.0
                    elif p_info['type'] == 'EMERGING': plate_bonus += 15.0
                    if p_info['risk_level'] == '低': plate_bonus += 5.0
                
                final_conf = conf + plate_bonus
                
                # 盈亏比门槛过滤: 仅当 R/R >= 2.5 且置信度够高时建议买入
                if "买入" in action and (risk_reward < 2.5 or final_conf < 50):
                    action = "条件不足"
                    reason = f"⚠️ 盈亏比不足({risk_reward}:1) 或 置信度过低({final_conf:.0f})"
                else:
                    reason += f" | 盈亏比 {risk_reward}:1"

                report.strategic_signals.append(StrategicSignal(
                    code=s.code, name=s.name, action=action, 
                    confidence=final_conf, reason=reason, 
                    current_pct=s.current_pct, plate=s.plate
                ))

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
                            confidence=conf, current_pct=s.current_pct,
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
        max_level = 0
        if yest_limit_map:
            for _, y_s in yest_limit_map.items():
                yest_counts[y_s.lb_days] += 1
            max_level = max(yest_counts.keys()) if yest_counts else 0

        report.promo_stats = defaultdict(lambda: [0, 0, [], [], 0]) # [total, promoted, strongs, nuclear, red_open]
        for level, count in yest_counts.items():
            report.promo_stats[level][0] = count

        # 2. 对撞映射今日实际
        for s in stocks:
            if s.lb_days == max_level and max_level > 0: s.tags.append("最高标")
            if s.resistance_gap < 0.03: s.tags.append("筹码优")
            
            if s.is_yest_limit:
                level = s.lb_days
                # [V39.7 Fix] 区分红开率与真实晋级率
                if s.open_pct > 0: report.promo_stats[level][4] += 1
                if s.current_pct > 0.098: report.promo_stats[level][1] += 1
                
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
        
        # 🚀 V19.2: 基于内容的去重打印 + 智能情报行提取
        summary = build_summary(report)
        current_hash = str(hash(summary))
        if current_hash == self.last_summary_hash:
            # 去重期间，只输出精简的战役情报行，不重画大屏
            report.summary_text = report.rotation_msg 
        else:
            self.last_summary_hash = current_hash
            report.summary_text = summary
            
        return report
