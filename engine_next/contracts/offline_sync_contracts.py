from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass(frozen=True)
class GapFillPlan:
    target_date: str
    missing_symbols: tuple[str, ...]
    reason: str
    lookback_days: int = 0


@dataclass(frozen=True)
class ResumeCheckpointState:
    task_type: str
    date_tag: str
    completed_task_ids: tuple[str, ...]


@dataclass(frozen=True)
class PhysicalValidationResult:
    symbol: str
    date_tag: str
    kline_ready: bool
    factor_ready: bool
    redis_cache_ready: bool
    dde_ready: bool = False
    notes: str = ""


@dataclass(frozen=True)
class WatermarkSnapshot:
    target_date: str
    kline_latest_dates: dict[str, str]
    dde_latest_dates: dict[str, str]
    factor_latest_dates: dict[str, str]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class KlineWindow:
    symbol: str
    target_date: str
    lookback_days: int
    rows: tuple[dict[str, Any], ...]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class FactorResult:
    symbol: str
    trade_date: str
    ready: bool
    payload: dict[str, Any] | None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChipResult:
    symbol: str
    trade_date: str
    ready: bool
    payload: dict[str, Any] | None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class DdeResult:
    symbol: str
    trade_date: str
    ready: bool
    payload: dict[str, Any] | None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class IntegratedSyncResult:
    symbol: str
    target_date: str
    kline_ready: bool
    dde_ready: bool
    factor_ready: bool
    chip_ready: bool
    redis_cache_ready: bool
    wrote_tdengine: tuple[str, ...]
    wrote_redis: tuple[str, ...]
    notes: tuple[str, ...] = ()


def build_gap_fill_plan(
    target_date: str,
    missing_symbols: List[str],
    reason: str,
    lookback_days: int = 0,
) -> Optional[GapFillPlan]:
    if not missing_symbols:
        return None
    return GapFillPlan(
        target_date=target_date,
        missing_symbols=tuple(missing_symbols),
        reason=reason,
        lookback_days=lookback_days,
    )
