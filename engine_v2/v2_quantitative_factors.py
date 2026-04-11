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
        plate_resonance: float,        # 来自 v2_prime_logic.get_plate_resonance()
        resistance_gap: float,
        is_alpha_seed: bool,           # 是否在 Redis alpha:candidates 中
        allowed_setups: List[str],
        sentiment_score: float,
        red_green_ratio: float = 1.0,
    ) -> Optional[dict]:
        """
        返回格式: {"setup_id": str, "action": str, "reason": str, "conf_bonus": int}
        或 None
        """
        candidates = []

        # ── A. 补涨先锋 (follower_chase / low_level_relay) ──────────────
        # 触发条件：龙头锁死的板块内，低价低开的同板块标的
        if any(s in allowed_setups for s in ["follower_chase", "low_level_relay"]):
            if (plate_resonance > 1.3
                    and price < 6.5
                    and -0.01 < open_pct < 0.05
                    and vol_ratio > 2.0
                    and lb_days <= 1):
                score = 25 + (10 if is_alpha_seed else 0) + (5 if price < 4 else 0)
                candidates.append({
                    "setup_id": "SETUP_FOLLOW_CHASE",
                    "action": "补涨先锋",
                    "reason": f"💎 龙头锁死({plate_resonance:.1f}x)·低价({price:.2f})·量能认同(x{vol_ratio:.1f})",
                    "conf_bonus": score,
                })

        # ── B. 弱转强接力 (low_level_relay / new_theme_first_board) ─────
        if any(s in allowed_setups for s in ["low_level_relay", "new_theme_first_board"]):
            if (lb_days > 0
                    and open_pct > 0.04
                    and vol_ratio > 0.08
                    and resistance_gap < 0.05):
                score = 20 + (15 if is_alpha_seed else 0)
                candidates.append({
                    "setup_id": "SETUP_WEAK_STRONG",
                    "action": "弱转强买入",
                    "reason": f"🚀 超预期强修复·量比{vol_ratio*100:.0f}%·筹码净区",
                    "conf_bonus": score,
                })

        # ── C. 核心低吸 (core_dip_buying) ───────────────────────────────
        if "core_dip_buying" in allowed_setups:
            if (lb_days >= 2
                    and open_pct < 0.0
                    and resistance_gap < 0.02
                    and plate_resonance > 1.0):
                score = 18
                candidates.append({
                    "setup_id": "SETUP_CORE_DIP",
                    "action": "核心低吸",
                    "reason": f"💡 龙头分歧回踩·零压筹码·板块未变盘",
                    "conf_bonus": score,
                })

        # ── D. 冰点破壳 (ice_rebound_core) ──────────────────────────────
        if "ice_rebound_core" in allowed_setups:
            if (sentiment_score < 4.0
                    and lb_days == 1
                    and red_green_ratio > 1.2
                    and vol_ratio > 3.0):
                score = 22
                candidates.append({
                    "setup_id": "SETUP_ICE_BREAK",
                    "action": "冰点破壳",
                    "reason": f"❄️→🔥 冰点首板·红绿比{red_green_ratio:.2f}·放量{vol_ratio:.1f}x",
                    "conf_bonus": score,
                })

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
