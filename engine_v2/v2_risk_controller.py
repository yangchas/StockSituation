"""
v2_risk_controller.py
动态风险控制器 (V9.0)

职责：
  1. 竞价期入场资格审查（题材是否及预期）
  2. 开盘瞬间止损决策（开盘下杀 / 弱势低开）
  3. 盘中动态追踪（冲高回落 / 盘中跳水 / 硬止损）
  4. 主线豁免逻辑（主线日不卖主线标的）
  5. 动态总仓位上限（随情绪/晋级率扩缩）

数据依赖：
  - Redis: stock:quote:{code} (实时行情)
  - Redis: market:mainline_sector (今日主线板块名)
  - v2_prime_logic.ResonancePrimeService.get_plate_resonance()
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from enum import Enum

logger = logging.getLogger("RiskCtrl")


# ──────────────────────────────────────────────────────────────
# 枚举：标准化的操作指令
# ──────────────────────────────────────────────────────────────
class RiskAction(Enum):
    HOLD          = "持仓观察"
    CANCEL_ENTRY  = "取消入场"
    HALF_ENTRY    = "半仓入场"
    FULL_ENTRY    = "正常入场"
    HALF_STOP     = "减仓50%"
    PARTIAL_TP    = "止盈减仓"
    HARD_STOP     = "清仓止损"
    ADD_POSITION  = "加仓"


# ──────────────────────────────────────────────────────────────
# 数据类：单次风险评估结果
# ──────────────────────────────────────────────────────────────
@dataclass
class RiskDecision:
    action: RiskAction
    reason: str
    urgency: str          = "NORMAL"   # NORMAL | HIGH | CRITICAL
    sell_ratio: float     = 0.0        # 需要卖出的仓位比例，0-1.0
    is_mainline_override: bool = False  # 是否触发了主线豁免


# ──────────────────────────────────────────────────────────────
# 1. 竞价期入场审查
# ──────────────────────────────────────────────────────────────
class AuctionEntryChecker:
    """
    在 09:15-09:25 竞价期，对待入场标的进行资格评估。
    通常在 v2_auction_analyzer.py 生成信号后、实际下单前调用。
    """

    @staticmethod
    def evaluate(
        plate_resonance: float,           # 板块共振因子（来自 prime_logic）
        sector_volume_vs_expect: float,   # 板块实际量 / 预期量，1.0 = 符合预期
        plate_locked: bool,               # 板块内是否有龙头锁死
        sentiment_score: float,           # 全场情绪分
        allowed_setups: List[str],        # 当前允许的战法名单
    ) -> RiskDecision:
        """
        返回：是否允许入场，以及以什么仓位进入。
        """
        # 情绪冰点，任何战法都不入场
        if sentiment_score < 3.5:
            return RiskDecision(
                action=RiskAction.CANCEL_ENTRY,
                reason=f"情绪冰点({sentiment_score:.1f})，全场禁止入场",
                urgency="HIGH"
            )

        # 题材崩塌：板块共振极低 + 量能严重不足
        if plate_resonance < 0.8 or sector_volume_vs_expect < 0.4:
            return RiskDecision(
                action=RiskAction.CANCEL_ENTRY,
                reason=f"题材不及预期: 共振={plate_resonance:.2f} / 量能={sector_volume_vs_expect:.0%}",
                urgency="HIGH"
            )

        # 题材偏弱：共振偏低且无龙头
        if plate_resonance < 1.3 and not plate_locked:
            return RiskDecision(
                action=RiskAction.HALF_ENTRY,
                reason=f"板块弱势(共振{plate_resonance:.2f})，建议半仓试探",
                sell_ratio=0.0,
                urgency="NORMAL"
            )

        # 正常入场
        return RiskDecision(
            action=RiskAction.FULL_ENTRY,
            reason=f"题材通过审查: 共振={plate_resonance:.2f} {'龙头锁死' if plate_locked else ''}",
            urgency="NORMAL"
        )


# ──────────────────────────────────────────────────────────────
# 2. 开盘瞬间止损器
# ──────────────────────────────────────────────────────────────
class OpeningStopLoss:
    """
    在 09:30 开盘时，对比实际开盘价与竞价成本价，决定是否止损。
    """

    HARD_STOP_THRESHOLD = -0.05    # 开盘低于竞价 -5%，无条件清仓
    SOFT_STOP_THRESHOLD = -0.03    # 开盘低于竞价 -3%，减半
    TAKE_PROFIT_THRESHOLD = 0.04   # 开盘高于竞价 +4%（超预期），止盈一半

    @staticmethod
    def evaluate(
        open_price: float,             # 实际开盘价
        auction_cost: float,           # 竞价成本价（买入均价）
        is_mainline_stock: bool = False,
        plate_resonance: float = 1.0,
    ) -> RiskDecision:
        """
        open_price: 今日开盘价
        auction_cost: 竞价期买入的平均成本
        """
        if auction_cost <= 0:
            return RiskDecision(action=RiskAction.HOLD, reason="无竞价成本记录，跳过")

        gap = (open_price - auction_cost) / auction_cost

        # 硬止损：开盘下杀
        if gap <= OpeningStopLoss.HARD_STOP_THRESHOLD:
            reason = f"开盘下杀 {gap*100:.1f}%（低于成本 {OpeningStopLoss.HARD_STOP_THRESHOLD*100:.0f}%阈值）"
            # 主线豁免不适用于开盘-5%，这是信号彻底失效
            return RiskDecision(
                action=RiskAction.HARD_STOP,
                reason=reason,
                urgency="CRITICAL",
                sell_ratio=1.0
            )

        # 软止损：弱势低开
        if gap <= OpeningStopLoss.SOFT_STOP_THRESHOLD:
            if is_mainline_stock and plate_resonance > 1.3:
                return RiskDecision(
                    action=RiskAction.HOLD,
                    reason=f"低开 {gap*100:.1f}%，主线豁免保护（共振{plate_resonance:.1f}），观察30s",
                    is_mainline_override=True
                )
            return RiskDecision(
                action=RiskAction.HALF_STOP,
                reason=f"低开 {gap*100:.1f}%，减半仓观察",
                urgency="HIGH",
                sell_ratio=0.5
            )

        # 超预期高开：止盈一半
        if gap >= OpeningStopLoss.TAKE_PROFIT_THRESHOLD:
            return RiskDecision(
                action=RiskAction.PARTIAL_TP,
                reason=f"超预期高开 +{gap*100:.1f}%，止盈 50%，留仓等封板",
                sell_ratio=0.5
            )

        return RiskDecision(action=RiskAction.HOLD, reason=f"开盘正常 {gap*100:+.1f}%，持仓观察")


# ──────────────────────────────────────────────────────────────
# 3. 盘中动态追踪器
# ──────────────────────────────────────────────────────────────
class IntradayTracker:
    """
    在盘中持续监控持仓的价格行为，识别：
    - 硬止损（亏损超阈值）
    - 冲高回落止盈
    - 速度止损（盘中跳水）
    - 炸板未回封
    """

    HARD_STOP_LOSS        = -0.05    # 亏损 -5% 强制止损
    MAINLINE_HARD_STOP    = -0.08    # 主线豁免后，最大容忍 -8%
    SPIKE_REVERSAL_RATIO  = 0.60     # 冲高后回落超过 60% 峰值涨幅
    PLUNGE_SPEED_THRESHOLD = -0.02   # 2 分钟内跌幅 > 2%，触发速度止损
    REOPEN_WAIT_MINUTES   = 30       # 炸板后，等待 30 分钟观察是否回封

    @staticmethod
    def evaluate_hard_stop(
        current_price: float,
        cost_price: float,
        is_mainline_stock: bool,
        mainline_still_valid: bool,   # 主线当前是否仍然有效
    ) -> Optional[RiskDecision]:
        """检查是否触及硬止损线"""
        pnl = (current_price - cost_price) / cost_price
        threshold = (
            IntradayTracker.MAINLINE_HARD_STOP
            if (is_mainline_stock and mainline_still_valid)
            else IntradayTracker.HARD_STOP_LOSS
        )
        if pnl <= threshold:
            override_note = " (主线豁免已到极限)" if is_mainline_stock else ""
            return RiskDecision(
                action=RiskAction.HARD_STOP,
                reason=f"亏损 {pnl*100:.1f}% 触及硬止损线 {threshold*100:.0f}%{override_note}",
                urgency="CRITICAL",
                sell_ratio=1.0,
                is_mainline_override=(is_mainline_stock and mainline_still_valid)
            )
        return None

    @staticmethod
    def evaluate_spike_reversal(
        intraday_high_pct: float,   # 盘中曾经达到的最高涨幅（相对昨收）
        current_pct: float,          # 当前涨幅
        is_mainline_stock: bool,
        mainline_still_valid: bool,
    ) -> Optional[RiskDecision]:
        """检查冲高回落是否触发止盈"""
        if is_mainline_stock and mainline_still_valid:
            return None  # 主线豁免：不因冲高回落止盈

        if intraday_high_pct <= 0.03:
            return None  # 涨幅太小，不判断冲高

        pullback = intraday_high_pct - current_pct
        if pullback >= intraday_high_pct * IntradayTracker.SPIKE_REVERSAL_RATIO:
            return RiskDecision(
                action=RiskAction.PARTIAL_TP,
                reason=f"冲高 {intraday_high_pct*100:.1f}% 后回落 {pullback*100:.1f}%（超 60% 峰值）",
                urgency="HIGH",
                sell_ratio=0.6
            )
        return None

    @staticmethod
    def evaluate_plunge_speed(
        price_2m_ago: float,
        current_price: float,
        is_mainline_stock: bool,
        mainline_still_valid: bool,
    ) -> Optional[RiskDecision]:
        """检查盘中跳水速度止损"""
        speed_chg = (current_price - price_2m_ago) / price_2m_ago
        if speed_chg <= IntradayTracker.PLUNGE_SPEED_THRESHOLD:
            if is_mainline_stock and mainline_still_valid:
                return RiskDecision(
                    action=RiskAction.HALF_STOP,
                    reason=f"主板跳水 {speed_chg*100:.1f}%/2min，主线豁免降级为减仓30%",
                    sell_ratio=0.30,
                    is_mainline_override=True
                )
            return RiskDecision(
                action=RiskAction.HALF_STOP,
                reason=f"盘中跳水 {speed_chg*100:.1f}%/2min，速度止损减仓50%",
                urgency="HIGH",
                sell_ratio=0.5
            )
        return None

    @staticmethod
    def evaluate_limit_open(
        minutes_since_open: int,   # 从炸板开始计时的分钟数
        is_resealed: bool,          # 是否已回封涨停
        is_mainline_stock: bool,
    ) -> Optional[RiskDecision]:
        """炸板后监控：超过 30 分钟未回封，主线豁免也失效"""
        if is_resealed:
            return None  # 已回封，无需操作
        if minutes_since_open >= IntradayTracker.REOPEN_WAIT_MINUTES:
            note = "炸板主线豁免终止" if is_mainline_stock else "炸板止损"
            return RiskDecision(
                action=RiskAction.HARD_STOP,
                reason=f"{note}：炸板已 {minutes_since_open} 分钟未回封，强制清仓",
                urgency="CRITICAL",
                sell_ratio=1.0
            )
        return None


# ──────────────────────────────────────────────────────────────
# 4. 主线有效性判断器
# ──────────────────────────────────────────────────────────────
class MainlineValidator:
    """
    判断当前板块是否仍然是"有效主线"。
    当以下任一条件触发时，主线豁免失效。

    依赖数据（需调用方传入）：
      - sector_strength_rank: 当前板块强度排名（1 = 最强）
      - leader_is_locked: 板块龙头是否仍然封板
      - sector_volume_vs_peak: 当前板块成交量 / 早盘峰值（<0.5 = 萎缩）
      - current_sentiment: 当前情绪分（vs 竞价时情绪分）
      - auction_sentiment: 竞价时的情绪分
    """

    @staticmethod
    def is_mainline_valid(
        sector_strength_rank: int,
        leader_is_locked: bool,
        sector_volume_vs_peak: float,
        current_sentiment: float,
        auction_sentiment: float,
    ) -> Tuple[bool, str]:
        """
        返回 (是否有效, 失效原因)
        """
        # 条件1：板块不再是主线（被其他板块超越）
        if sector_strength_rank > 2:
            return False, f"板块强度排名已跌至 #{sector_strength_rank}，主线地位丢失"

        # 条件2：龙头炸板（结合炸板未回封的时间在 IntradayTracker 中处理）
        if not leader_is_locked:
            return True, "龙头暂时炸板，等待回封..."  # 注意：不立即失效，等 30 分钟

        # 条件3：板块成交量严重萎缩
        if sector_volume_vs_peak < 0.45:
            return False, f"板块量能萎缩至峰值 {sector_volume_vs_peak:.0%}，资金撤退"

        # 条件4：情绪断崖
        sentiment_drop = auction_sentiment - current_sentiment
        if sentiment_drop > 2.5:
            return False, f"情绪断崖：竞价{auction_sentiment:.1f} → 现在{current_sentiment:.1f}，跌幅{sentiment_drop:.1f}"

        return True, "主线有效"


# ──────────────────────────────────────────────────────────────
# 5. 动态总仓位上限计算器
# ──────────────────────────────────────────────────────────────
def get_exposure_cap(
    sentiment_score: float,
    consecutive_strong_days: int,  # 连续强势天数（情绪 > 6 的连续天数）
    promotion_rate: float,          # 当日连板晋级率
) -> float:
    """
    根据市场状态动态确定最大总仓位占比。
    返回 0.0 - 0.80 的浮点数。
    """
    # 情绪冰点，近乎空仓
    if sentiment_score < 3.5:
        return 0.05

    # 基础仓位上限
    if sentiment_score >= 7.0:
        base = 0.70
    elif sentiment_score >= 6.0:
        base = 0.55
    elif sentiment_score >= 4.5:
        base = 0.35
    else:
        base = 0.20

    # 单日爆量打折：不相信单日行情，需要持续性
    if consecutive_strong_days < 2:
        base *= 0.80

    # 晋级率质量折扣/溢价
    if promotion_rate < 0.30:
        base *= 0.50   # 高危，晋级率极低
    elif promotion_rate < 0.40:
        base *= 0.75
    elif promotion_rate > 0.55:
        base = min(base * 1.15, 0.80)  # 封顶 80%

    return round(min(base, 0.80), 2)


# ──────────────────────────────────────────────────────────────
# 使用示例（供调试）
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 场景：04-09 中嘉博创 竞价审查
    entry = AuctionEntryChecker.evaluate(
        plate_resonance=1.85,
        sector_volume_vs_expect=1.3,
        plate_locked=True,
        sentiment_score=7.0,
        allowed_setups=["follower_chase"]
    )
    print(f"[竞价审查] {entry.action.value}: {entry.reason}")

    # 场景：开盘低开 -2%
    opening = OpeningStopLoss.evaluate(
        open_price=3.77,
        auction_cost=3.85,
        is_mainline_stock=True,
        plate_resonance=1.85
    )
    print(f"[开盘决策] {opening.action.value}: {opening.reason}")

    # 场景：动态总仓位
    cap = get_exposure_cap(sentiment_score=7.0, consecutive_strong_days=1, promotion_rate=0.50)
    print(f"[仓位上限] {cap*100:.0f}%")
