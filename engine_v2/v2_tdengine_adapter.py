"""
v2_tdengine_adapter.py
TDengine 5分钟K线适配器

优先级（盘中）：
  Tier 1: TDengine INTERVAL(5m) 查询（快速，无网络消耗）
  Tier 2: 分析 1m 量能 vs 5日均量（TDengine 缺失时降级）
  Tier 3: 网络请求新浪5m K线（仅限核心/推荐个股，严格限流）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict

logger = logging.getLogger("V2TDengine")

# ─────────────────────────────────────────────────────────────────────────────
# 统一输出结构（与 v2_data_service.StandardKLine 保持对齐）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class KLine5m:
    dt: str          # "2026-03-29 09:35:00"
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    resolution: str = "5m"


@dataclass
class VolumePulse:
    """
    Tier 2 降级：当 TDengine 无5m数据时，用 1分钟速度和量比替代。
    逻辑：1分钟成交量 / (5日日均量 / 240分钟) = 当前分钟量比
    """
    symbol: str
    current_1m_vol: int         # 当前1分钟成交量
    avg_daily_vol: int          # 5日日均量
    per_minute_avg: float       # 日均每分钟量 = avg_daily_vol / 240
    vol_ratio: float            # 量比 = current_1m_vol / per_minute_avg
    signal: str                 # "heavy"(放量) / "normal" / "shrink"(缩量)


# ─────────────────────────────────────────────────────────────────────────────
# TDengine 适配器
# ─────────────────────────────────────────────────────────────────────────────

class TDengineKLineAdapter:
    """
    从 TDengine 查询日内5分钟K线。
    使用 INTERVAL(5m) 聚合 stock_data 超级表的 tick 流。
    volume/amount 使用 LAST - FIRST 处理累计值。
    """

    def __init__(self, tdengine_service=None):
        """
        tdengine_service: 传入已有的 TDengineService 实例（单例）
        为 None 时自动尝试导入
        """
        self._svc = tdengine_service
        if self._svc is None:
            try:
                import sys, os
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web'))
                from services.tdengine_service import TDengineService
                self._svc = TDengineService()
                logger.info("[TDengine] 自动连接 TDengineService 成功")
            except Exception as e:
                logger.warning(f"[TDengine] 初始化失败，将使用降级模式: {e}")

    def is_available(self) -> bool:
        return self._svc is not None and getattr(self._svc, 'conn', None) is not None

    def get_5m_klines(
        self,
        symbol: str,
        trade_date: Optional[str] = None,
    ) -> List[KLine5m]:
        """
        查询指定股票当天的5分钟K线。
        trade_date: "2026-03-29"，默认今天
        返回空列表时表示 TDengine 无数据，调用方应触发 Tier 2 降级。
        """
        if not self.is_available():
            return []

        today = trade_date or date.today().strftime("%Y-%m-%d")
        start = f"{today} 09:30:00"
        end   = f"{today} 15:00:00"

        sql = f"""
        SELECT
            _wstart AS ts,
            FIRST(lp)          AS open,
            MAX(lp)            AS high,
            MIN(lp)            AS low,
            LAST(lp)           AS close,
            LAST(v) - FIRST(v) AS volume,
            LAST(a) - FIRST(a) AS amount
        FROM stock_data
        WHERE symbol = '{symbol}'
          AND ts >= '{start}'
          AND ts <= '{end}'
        INTERVAL(5m)
        ORDER BY ts
        """
        try:
            cursor = self._svc.execute_query(sql)
            if not cursor:
                return []
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if row[4] is None or float(row[4]) <= 0:
                    continue  # 跳过价格为0的空行（非交易时段）
                result.append(KLine5m(
                    dt=row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    open=round(float(row[1] or 0), 2),
                    high=round(float(row[2] or 0), 2),
                    low=round(float(row[3] or 0), 2),
                    close=round(float(row[4] or 0), 2),
                    volume=int(row[5] or 0),
                    amount=float(row[6] or 0),
                ))
            logger.debug(f"[TDengine] {symbol} 5m K线: {len(result)} 根")
            return result
        except Exception as e:
            logger.error(f"[TDengine] {symbol} 5m 查询异常: {e}")
            return []

    def get_latest_tick(self, symbol: str) -> Optional[Dict]:
        """获取个股最新一条 tick，用于价格确认（盘中）"""
        if not self.is_available():
            return None
        sql = f"SELECT LAST(lp), LAST(v), LAST(a), LAST(ts) FROM stock_data WHERE symbol = '{symbol}'"
        try:
            cursor = self._svc.execute_query(sql)
            if not cursor:
                return None
            row = cursor.fetchone()
            if row and row[0]:
                return {"price": float(row[0]), "volume": int(row[1] or 0),
                        "amount": float(row[2] or 0), "ts": str(row[3])}
        except Exception as e:
            logger.error(f"[TDengine] {symbol} latest tick: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Tier 2 降级：量能脉冲分析（不依赖 TDengine 5m 数据）
# ─────────────────────────────────────────────────────────────────────────────

def calc_volume_pulse(
    symbol: str,
    current_1m_vol: int,
    avg_daily_vol: int,
    trading_minutes: int = 240,
) -> VolumePulse:
    """
    用 1分钟成交量 vs 5日均量 替代5分钟K线。
    avg_daily_vol: 过去5个交易日日均成交量（股，来自 Rust 多因子矩阵）
    current_1m_vol: 最近1分钟成交量（来自 Redis volatile_pool 或 TDengine 最新 tick 差值）
    """
    per_min_avg = avg_daily_vol / trading_minutes if trading_minutes > 0 else 1
    ratio = current_1m_vol / per_min_avg if per_min_avg > 0 else 0

    if ratio >= 3.0:
        signal = "heavy"   # 放量（≥3倍）
    elif ratio >= 1.5:
        signal = "normal"  # 正常偏大
    else:
        signal = "shrink"  # 缩量

    return VolumePulse(
        symbol=symbol,
        current_1m_vol=current_1m_vol,
        avg_daily_vol=avg_daily_vol,
        per_minute_avg=round(per_min_avg, 1),
        vol_ratio=round(ratio, 2),
        signal=signal,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 统一查询入口（含自动降级）
# ─────────────────────────────────────────────────────────────────────────────

class IntraKLineService:
    """
    盘中K线服务：按 Tier 优先级自动选择数据源。
    - Tier 1: TDengine INTERVAL(5m)
    - Tier 2: VolumePulse（量比替代，需传入 avg_daily_vol）
    - Tier 3: 网络请求（仅对核心股，由调用方决定是否触发）
    """

    def __init__(self, tdengine_service=None):
        self._td = TDengineKLineAdapter(tdengine_service)

    def query(
        self,
        symbol: str,
        avg_daily_vol: int = 0,
        current_1m_vol: int = 0,
        trade_date: Optional[str] = None,
    ) -> Dict:
        """
        返回:
            {
              "tier": 1/2,
              "klines": [KLine5m...],   # Tier 1 有数据时
              "pulse": VolumePulse,     # Tier 2 降级时
              "empty": True/False,
            }
        """
        # Tier 1
        klines = self._td.get_5m_klines(symbol, trade_date)
        if klines:
            return {"tier": 1, "klines": klines, "pulse": None, "empty": False}

        # Tier 2
        if avg_daily_vol > 0 and current_1m_vol > 0:
            pulse = calc_volume_pulse(symbol, current_1m_vol, avg_daily_vol)
            logger.info(f"[IntraK] {symbol} Tier2降级 量比={pulse.vol_ratio} {pulse.signal}")
            return {"tier": 2, "klines": [], "pulse": pulse, "empty": False}

        # 无数据
        return {"tier": 0, "klines": [], "pulse": None, "empty": True}


# ─────────────────────────────────────────────────────────────────────────────
# 测试入口
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    svc = IntraKLineService()

    # Tier 1 测试（TDengine 连接可用时）
    result = svc.query("000001", avg_daily_vol=50_000_000, current_1m_vol=800_000)
    if result["tier"] == 1:
        print(f"Tier 1 TDengine: {len(result['klines'])} 根5m K线")
        if result["klines"]:
            k = result["klines"][-1]
            print(f"  最新: {k.dt} Close={k.close} Vol={k.volume}")
    else:
        print(f"Tier {result['tier']} 降级: {result.get('pulse')}")

    # Tier 2 降级测试（模拟 TDengine 为空）
    pulse = calc_volume_pulse("000001", current_1m_vol=800_000, avg_daily_vol=50_000_000)
    print(f"\nTier 2 量能脉冲: 量比={pulse.vol_ratio} 信号={pulse.signal}")
    print(f"  1分均量={pulse.per_minute_avg:.0f}股  当前1分={pulse.current_1m_vol}股")
