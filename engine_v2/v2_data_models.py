from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime

@dataclass
class StandardSnapshot:
    """统一实时行情快照格式"""
    code: str
    price: float
    change_pct: float
    amount: float
    vol: float
    high: float
    low: float
    open: float
    bid_amt: float = 0.0 # 竞价封单/委买额
    ts: int = field(default_factory=lambda: int(datetime.now().timestamp() * 1000))

@dataclass
class StandardKLine:
    """统一K线格式 (支持 日/60m/5m)"""
    code: str
    dt: str              # 2026-03-29 09:35:00
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float
    resolution: str      # day, 60m, 5m

class BaseDataAdapter:
    """数据源抽象适配器"""
    def format_snapshot(self, raw_data: Any) -> StandardSnapshot:
        raise NotImplementedError
        
    def format_kline(self, raw_data: Any, resolution: str) -> List[StandardKLine]:
        raise NotImplementedError
