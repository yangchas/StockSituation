from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class BaostockDailyKlineRequest:
    symbol: str
    trade_date: str
    allow_previous_trade_day_fallback: bool = True


@dataclass(frozen=True)
class BaostockAvailabilityResult:
    ready: bool
    formal_trade_date: str
    fresh_after: str
    reason: str
    fallback_trade_date: Optional[str] = None


def check_baostock_daily_kline_availability(
    now: datetime,
    target_date: str,
    previous_trade_date: str,
) -> BaostockAvailabilityResult:
    today_str = now.strftime("%Y-%m-%d")
    hm = now.strftime("%H:%M")
    if target_date == today_str and hm < "17:30":
        return BaostockAvailabilityResult(
            ready=False,
            formal_trade_date=target_date,
            fresh_after=f"{target_date} 17:30",
            reason="Baostock 当日日线在 17:30 前不视为正式可用。",
            fallback_trade_date=previous_trade_date,
        )
    return BaostockAvailabilityResult(
        ready=True,
        formal_trade_date=target_date,
        fresh_after=f"{target_date} 17:30",
        reason="Baostock 当日日线已进入正式可用窗口。",
        fallback_trade_date=None,
    )
