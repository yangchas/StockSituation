import time
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Tuple, Set
import numpy as np

logger = logging.getLogger("MarketEdgeV2.Logic")

# --- Data Models ---

@dataclass
class EmotionPhaseResult:
    ts: int
    date: str
    phase: str           # ice_point, start, climax, divergence, retreat
    confidence: float
    transition_reason: str
    pos_cap: float
    allowed_setups: List[str]
    blocked_setups: List[str]
    age_days: int = 0
    age_bars: int = 0

@dataclass
class YesterdayStateProfile:
    code: str
    state_type: str      # ZT_STRONG, ZT_WEAK, BOMB_STRONG, BOMB_WEAK, FLOOR_RESCUED, FLOOR_LOCKED, NORMAL_WEAK, NORMAL_STRONG, NORMAL_NEUTRAL
    change_pct: float
    close_strength: float
    limit_up_type: str = ""
    vol_ratio: float = 1.0
    upper_shadow: float = 0.0

@dataclass
class StockAdvice:
    code: str
    name: str
    score: float
    sentiment_tag: str   # weak_to_strong, strong_to_weak, etc.
    quality_score: float # 0.0 to 1.0
    capital_type: str    # hot_money (游资), institution (机构), neutral
    tips: List[str]      # 中文建议

# --- Core Business Service ---

class V2BusinessLogicService:
    def __init__(self):
        self.today_str = time.strftime("%Y%m%d")
        # Blacklist of "junk" plates
        self.plate_blacklist = {
             "国资改革", "国企改革", "央企改革", "业绩增长", "融资融券", "标普道琼斯", 
             "沪股通", "深股通", "证金持股", "汇金持股", "山东", "广东", "背景"
        }

    # --- 1. Sentiment Cycle (情绪周期) ---

    def predict_market_phase(
        self,
        st_score: float,
        red_green_ratio: float,
        max_lb: int,
        consensus_score: float,
        effectiveness: float = 0.5,
        fade_count: int = 0,
        one_word_break_rate: float = 0.0,
        seal_ratio_front20: float = 1.0,
        last_phase: str = "UNKNOWN"
    ) -> EmotionPhaseResult:
        """
        核心情绪状态机逻辑。
        """
        phase = "start"
        pos_cap = 0.35
        allowed = ["new_theme_first_board"]
        blocked = ["blind_relay", "late_rotation"]
        transition = "DEFAULT_STABLE"
        confidence = 0.5

        # RETREAT (退潮)
        if red_green_ratio < 0.7 or (effectiveness < 0.25 and fade_count > 20):
            phase = "retreat"
            pos_cap = 0.15
            allowed = []
            blocked = ["blind_relay", "high_board_chase", "follower_chase", "late_rotation"]
            transition = "PANIC_RETREAT"
            confidence = 0.85
            if red_green_ratio < 0.4: transition = "BROAD_CRASH"
        
        # ICE_POINT (冰点)
        elif red_green_ratio < 0.6 and max_lb < 4:
            phase = "ice_point"
            pos_cap = 0.2
            allowed = ["ice_rebound_core"]
            blocked = ["standard_relay", "high_board_chase"]
            transition = "ICE_POINT_LIMIT"
            confidence = 0.9

        # CLIMAX (高潮)
        elif (consensus_score > 35 or seal_ratio_front20 > 1.25) and max_lb >= 5 and red_green_ratio > 0.85:
            phase = "climax"
            pos_cap = 0.6
            allowed = ["low_level_relay", "high_board_chase", "new_theme_first_board"]
            blocked = ["late_rotation", "blind_relay"]
            transition = "ACCEL_MAIN_RISE"
            confidence = 0.8

        # DIVERGENCE (分歧)
        elif max_lb > 4 and (red_green_ratio < 1.0 or fade_count > 15 or one_word_break_rate > 0.4):
            phase = "divergence"
            pos_cap = 0.4
            allowed = ["core_dip_buying"]
            blocked = ["follower_chase", "late_rotation", "blind_relay"]
            transition = "HIGH_LEVEL_DIVERGE"
            confidence = 0.7

        # IGNITION (启动/修复)
        elif red_green_ratio >= 1.25:
            phase = "start"
            pos_cap = 0.45
            allowed = ["new_theme_first_board", "low_level_relay"]
            transition = "REPAIR_IGNITION"
            confidence = 0.75

        return EmotionPhaseResult(
            ts=int(time.time() * 1000),
            date=self.today_str,
            phase=phase,
            confidence=confidence,
            transition_reason=transition,
            pos_cap=pos_cap,
            allowed_setups=allowed,
            blocked_setups=blocked
        )

    # --- 2. Expectation Gap (预期差分析) ---

    def evaluate_expectation_state(self, y_profile: YesterdayStateProfile, auc_pct: float, cur_pct: float, seal_ratio: float) -> str:
        """评估预期差结果"""
        is_strong_yesterday = (y_profile.state_type == "ZT_STRONG" or (y_profile.change_pct > 5.0 and y_profile.close_strength > 0.8))
        is_weak_yesterday = (y_profile.state_type in ("BOMB_WEAK", "ZT_WEAK") or y_profile.change_pct < -3.0)

        is_strong_today = (auc_pct >= 5.0 and seal_ratio >= 0.2) or (cur_pct >= auc_pct + 1.0)
        is_weak_today = (auc_pct <= -2.0) or (cur_pct < auc_pct - 2.5)

        if is_weak_yesterday and is_strong_today: return "weak_to_strong"
        if is_strong_yesterday and is_weak_today: return "strong_to_weak"
        if is_strong_yesterday and is_strong_today: return "strong_continue"
        if is_weak_yesterday and is_weak_today: return "weak_continue"
        return "neutral"

    # --- 3. Quality Analysis (股性与建议) ---

    def analyze_stock_quality(self, history_data: List[float], market_cap: float) -> Tuple[float, str]:
        """
        锐评点 5: 提取股性。标准：走势规整度、套牢压力。
        """
        if len(history_data) < 10: return 0.5, "neutral"
        
        # 简单波动率与回撤分析 (作为股性降级)
        std = np.std(history_data) / np.mean(history_data)
        quality = 1.0 - np.clip(std * 5, 0, 0.5) # 波动过大认为股性乱
        
        # 资金偏好 (锐评点 4)
        capital_type = "institution" if market_cap > 100_000_000_00 else ("hot_money" if market_cap < 30_000_000_00 else "neutral")
        
        return round(float(quality), 2), capital_type

    def generate_advice(self, code: str, name: str, state: str, quality: float, cap_type: str) -> List[str]:
        """锐评点 2: 中文操作建议输出"""
        tips = []
        if state == "weak_to_strong":
            tips.append(f"🔄 预期差切换：超预期强修复。")
            if quality > 0.8: tips.append("✅ 股性优良，由于分歧转一致，建议关注二板机会。")
        elif state == "strong_to_weak":
            tips.append(f"⚠️ 预期差切换：一致转分歧，注意回撤。")
            tips.append("🚫 建议回避，谨防天地板。")
        
        if cap_type == "hot_money" and state == "weak_to_strong":
              tips.append("🔥 典型游资票引导，注意换手承接。")
        
        return tips

    # --- Utilities ---
    def filter_plates(self, plates: List[Dict]) -> List[Dict]:
        """锐评点 6: 过滤烂大街板块"""
        return [p for p in plates if p.get('name') not in self.plate_blacklist]

    def _normalize_change_pct(self, val, scale=1.0):
        try:
            v = float(val)
            if abs(v) < 0.2 and scale == 1.0: # 可能是 0.05 这种比例
                 return v * 100.0
            return v
        except: return 0.0
