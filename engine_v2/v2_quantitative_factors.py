"""
v2_quantitative_factors.py
量化因子引擎 (V9.0)

包含:
  - calc_daily_score: 基于 TDengine daily_factors/daily_kline 的日K底分
  - PatternFactory: 将 allowed_setups 转化为物理触发判定
  - calc_kelly_position: 凯利公式仓位建议
"""
import logging
from typing import Optional, List

logger = logging.getLogger("QuantFactors")


# ──────────────────────────────────────────────────────────────
# 1. 日K技术面底分 (盘后批量计算，存入 Redis alpha 种子池)
# ──────────────────────────────────────────────────────────────

def calc_daily_score(
    bias_20: Optional[float],       # TDengine daily_factors.bias_20，如 -0.12 代表偏离均线 -12%
    profit_ratio: Optional[float],  # TDengine daily_factors.profit_ratio，如 0.08 代表8%筹码获利
    vol_ratio: Optional[float],     # TDengine daily_factors.vol_ratio，如 1.8 代表量比1.8倍
    rsi_6: Optional[float],         # TDengine daily_factors.rsi_6，0-100
    concentration: Optional[float], # TDengine daily_factors.concentration，如 0.08 代表8%
) -> int:
    """
    返回 0-100 的整数评分。分数越高表示该股技术面越适合作为 Alpha 种子。
    任何字段为 None 时跳过该分项（不奖不罚）。
    """
    score = 0

    # 超跌修复：偏离20日均线 -10% 以上，视为超跌区
    if bias_20 is not None:
        if bias_20 < -0.10:
            score += 25
        elif bias_20 < -0.05:
            score += 12

    # 低位筹码：获利盘 < 10%，说明大多数人在亏损且短期无大量抛压
    if profit_ratio is not None:
        if profit_ratio < 0.10:
            score += 20
        elif profit_ratio < 0.20:
            score += 10

    # 缩量/放量：盘后量比 > 1.5 意味着前期已有先知资金介入
    if vol_ratio is not None:
        if vol_ratio > 2.0:
            score += 20
        elif vol_ratio > 1.5:
            score += 10

    # 超卖：RSI6 < 25 为技术超卖区
    if rsi_6 is not None:
        if rsi_6 < 25:
            score += 15
        elif rsi_6 < 35:
            score += 7

    # 筹码集中：concentration < 10% 说明筹码高度集中，不易被砸
    if concentration is not None:
        if concentration < 0.10:
            score += 20
        elif concentration < 0.20:
            score += 10

    return min(score, 100)


# ──────────────────────────────────────────────────────────────
# 2. 战法工厂 (Pattern Factory)
# ──────────────────────────────────────────────────────────────

class PatternFactory:
    """
    将 v2_business_logic.py 输出的 allowed_setups 名单
    转化为物理可执行的个股匹配逻辑。

    返回最高置信度的一个战法，或 None（若无战法触发）。
    """

    @staticmethod
    def match(
        code: str,
        price: float,
        open_pct: float,
        vol_ratio: float,
        lb_days: int,
        plate: str,
        plate_resonance: float,
        resistance_gap: float,
        is_alpha_seed: bool,
        allowed_setups: List[str],
        sentiment_score: float,
        red_green_ratio: float = 1.0,
        # 🚀 [V39.5] 动态与历史因子
        speed_1m: float = 0.0,
        amount_2m: float = 0.0,
        is_intra_day: bool = False,
        history_meta: dict = None,
        # 🐉 [V41.0] 物理穿透：龙头状态注入
        dragon_locked: bool = False,
        dragon_real_plates: List[str] = None
    ) -> Optional[dict]:
        """
        返回格式: {"setup_id": str, "action": str, "reason": str, "conf_bonus": int}
        或 None
        """
        candidates = []
        history = history_meta or {}
        t2_lb = history.get('t2_lb_days', 0)
        t2_pct = history.get('t2_pct', 0.0)

        # ── A. 补涨先锋 (follower_chase) ──────────────────────────────────
        if any(s in allowed_setups for s in ["follower_chase", "low_level_relay"]):
            if (plate_resonance > 1.3 and price < 6.5 and -0.01 < open_pct < 0.05 
                and vol_ratio > 2.0 and lb_days <= 1):
                score = 25 + (10 if is_alpha_seed else 0)
                candidates.append({
                    "setup_id": "SETUP_FOLLOW_CHASE",
                    "action": "补涨先锋",
                    "reason": f"💎 龙头锁死·低价协同·量能认同",
                    "conf_bonus": score,
                })

        # ── B. 弱转强接力 (low_level_relay) ───────────────────────────────
        if any(s in allowed_setups for s in ["low_level_relay", "new_theme_first_board"]):
            if (lb_days > 0 and open_pct > 0.04 and vol_ratio > 0.10 and resistance_gap < 0.05):
                score = 20 + (15 if is_alpha_seed else 0)
                candidates.append({
                    "setup_id": "SETUP_WEAK_STRONG",
                    "action": "弱转强买入",
                    "reason": f"🚀 超预期强修复(Weak2Strong)·筹码净区",
                    "conf_bonus": score,
                })

        # ── E. 新题材首板 (new_theme_first_board) ────────────────────────
        if "new_theme_first_board" in allowed_setups:
            # 逻辑：首板 + 板块高共振 (说明是当日涌现的新题材) + 量能倍增
            if (lb_days == 0 and plate_resonance > 1.5 and vol_ratio > 3.0 
                and -0.02 < open_pct < 0.06 and resistance_gap < 0.02):
                score = 25 + (10 if is_alpha_seed else 0)
                candidates.append({
                    "setup_id": "SETUP_NEW_THEME_FIRST",
                    "action": "题材首板",
                    "reason": f"🆕 新题材涌现: 板块强度{plate_resonance:.1f} | 量能异动·首板确认",
                    "conf_bonus": score
                })

        # ── C. 反包先锋 (Counter-Attack / Rebound) 🚀 [V39.5] ──────────────
        # 逻辑：T-2 涨停/新高 + T-1 回调 + T-0 确认
        was_strong_t2 = (t2_lb >= 1 or t2_pct > 0.08)
        is_correction_t1 = (lb_days == 0) # 昨日未涨停
        
        if was_strong_t2 and is_correction_t1:
            # 基础因子：如果已身处盘中模式，优先看脉冲
            if is_intra_day:
                # 动态确认：1分速 > 1.5% 或 2分额大
                is_pulsing = (speed_1m > 0.015)
                if is_pulsing:
                    bonus = 35 if t2_lb >= 3 else 25 # 龙头反包加分更高
                    candidates.append({
                        "setup_id": "SETUP_COUNTER_ATTACK_DYNAMIC",
                        "action": "反包瞬发" if t2_lb < 3 else "妖股反包",
                        "reason": f"⚡ 动能确认: 1分速{speed_1m*100:.1f}% | {'龙头身位' if t2_lb >=3 else '强势反攻'}",
                        "conf_bonus": bonus
                    })
            else:
                # 竞价预判：需要高开或放量
                is_expected = (open_pct > -0.01 and vol_ratio > 2.5)
                if is_expected:
                    candidates.append({
                        "setup_id": "SETUP_COUNTER_ATTACK_PRE",
                        "action": "反包潜伏",
                        "reason": f"📈 基因预唤醒: 前日强势+昨日洗盘 | {'高标回踩' if t2_lb >= 3 else '反包预期'}",
                        "conf_bonus": 20
                    })

        # ── D. 核心低吸 (core_dip_buying) ───────────────────────────────
        if "core_dip_buying" in allowed_setups:
            if (lb_days >= 2 and open_pct < 0.0 and resistance_gap < 0.02 and plate_resonance > 1.0):
                candidates.append({
                    "setup_id": "SETUP_CORE_DIP",
                    "action": "核心低吸",
                    "reason": f"💡 龙头分歧回踩·零压筹码",
                    "conf_bonus": 18,
                })

        # ── F. 龙头补涨 (dragon_follower) ───────────────────────────────
        if dragon_locked and dragon_real_plates and "follower_chase" in allowed_setups:
            # 🐉 [V41.1] 物理穿透匹配：属于超级龙头的实时审计板块(名称) + 低位 + 尚未涨停
            # 兼容处理：可能 plate 是逗号分隔的多个板块名
            stock_plates = plate.replace('、', '+').replace(',', '+').split('+')
            is_in_dragon_sector = any(p in dragon_real_plates for p in stock_plates if p) 
            
            if is_in_dragon_sector and lb_days == 0 and open_pct < 0.05 and vol_ratio > 1.5:
                candidates.append({
                    "setup_id": "SETUP_DRAGON_FOLLOW",
                    "action": "龙头补涨",
                    "reason": f"🐉 跟着大哥肉: 龙头锁死·板块共振({Plate_Name_Match := next((p for p in stock_plates if p in dragon_real_plates), '未知')})",
                    "conf_bonus": 30
                })

        if not candidates:
            return None

        # 冲突仲裁：取置信加分最高的一个
        return max(candidates, key=lambda x: x["conf_bonus"])

        if not candidates:
            return None

        # 冲突仲裁：取置信加分最高的一个
        return max(candidates, key=lambda x: x["conf_bonus"])


# ──────────────────────────────────────────────────────────────
# 3. 凯利公式仓位建议
# ──────────────────────────────────────────────────────────────

def calc_kelly_position(
    win_rate: float,
    avg_win_pct: float = 0.10,   # 止盈目标，默认 10%
    avg_loss_pct: float = 0.03,  # 止损线，默认 3%
    half_kelly: bool = True,     # 实战强制使用半凯利，控制回撤
) -> float:
    """
    返回建议仓位占总资金的比例 (0.0 - 0.20)。
    win_rate: 历史胜率，从 strategy_memory 的相似案例中统计。
              若样本 < 5 条，默认使用 0.45 作为基准胜率。
    """
    if win_rate <= 0 or win_rate >= 1:
        win_rate = 0.45  # 防止极端值

    b = avg_win_pct / avg_loss_pct   # 赔率
    q = 1.0 - win_rate
    f_star = (win_rate * b - q) / b

    if half_kelly:
        f_star *= 0.5

    # 实战安全区间：最低 2%，最高 20%
    return round(max(0.02, min(0.20, f_star)), 4)
