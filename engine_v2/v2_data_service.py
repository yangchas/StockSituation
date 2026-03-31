from __future__ import annotations
import re
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime

# --- Data Models ---

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
    bid_amt: float = 0.0
    ts: int = field(default_factory=lambda: int(time.time() * 1000))

@dataclass
class StandardKLine:
    """统一K线格式"""
    code: str
    dt: str
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float
    resolution: str  # day, 60m, 5m

# --- Adapters ---

class BaseDataAdapter:
    def format_snapshot(self, raw_data: Any) -> Optional[StandardSnapshot]:
        raise NotImplementedError
    def format_kline(self, raw_data: Any, resolution: str) -> List[StandardKLine]:
        raise NotImplementedError

class SinaDataAdapter(BaseDataAdapter):
    def format_snapshot(self, raw_line: str) -> Optional[StandardSnapshot]:
        try:
            match = re.search(r'="(.+)"', raw_line)
            if not match: return None
            data = match.group(1).split(',')
            if len(data) < 10: return None
            
            # Simple code extraction from string like hq_str_sh600000
            code = "unknown"
            if "str_" in raw_line:
                code = raw_line.split("str_")[1].split("=")[0]
            
            return StandardSnapshot(
                code=code,
                price=float(data[3]),
                change_pct=round((float(data[3])/float(data[2]) - 1)*100, 2) if float(data[2])!=0 else 0,
                amount=float(data[9]),
                vol=float(data[8]),
                high=float(data[4]),
                low=float(data[5]),
                open=float(data[1])
            )
        except: return None

    def format_kline(self, raw_json: List, resolution: str) -> List[StandardKLine]:
        results = []
        for x in raw_json:
            results.append(StandardKLine(
                code=x.get('code', 'unknown'),
                dt=x.get('day') or x.get('dt'),
                open=float(x['open']),
                high=float(x['high']),
                low=float(x['low']),
                close=float(x['close']),
                vol=float(x['volume']),
                amount=float(x.get('amount', 0)),
                resolution=resolution
            ))
        return results

class KaipanlaDataAdapter(BaseDataAdapter):
    def format_snapshot(self, raw_data: Dict) -> StandardSnapshot:
        return StandardSnapshot(
            code=raw_data.get('code', ''),
            price=float(raw_data.get('price', 0)),
            change_pct=float(raw_data.get('px_change_rate', 0)),
            amount=float(raw_data.get('amount', 0)),
            vol=float(raw_data.get('volume', 0)),
            high=float(raw_data.get('high', 0)),
            low=float(raw_data.get('low', 0)),
            open=float(raw_data.get('open', 0)),
            bid_amt=float(raw_data.get('buy_money', 0))
        )
