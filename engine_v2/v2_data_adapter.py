from __future__ import annotations
import re
from typing import List, Dict, Any, Optional
from v2_data_models import StandardSnapshot, StandardKLine, BaseDataAdapter

class SinaDataAdapter(BaseDataAdapter):
    """新浪数据源适配器"""
    
    def format_snapshot(self, raw_line: str) -> Optional[StandardSnapshot]:
        """解析 hq.sinajs.cn 返回的字符串"""
        # format: var hq_str_sh600000="浦发银行,10.15,10.16,10.15,10.18,10.10,10.15,10.16,123456,125000000,..."
        try:
            match = re.search(r'="(.+)"', raw_line)
            if not match: return None
            data = match.group(1).split(',')
            if len(data) < 10: return None
            
            code_match = re.search(r'str_(s[hz]\d{6})', raw_line)
            code = code_match.group(1) if code_match else "unknown"
            
            return StandardSnapshot(
                code=code,
                price=float(data[3]),
                change_pct=round((float(data[3]) / float(data[2]) - 1) * 100, 2) if float(data[2]) != 0 else 0,
                amount=float(data[9]),
                vol=float(data[8]),
                high=float(data[4]),
                low=float(data[5]),
                open=float(data[1])
            )
        except Exception:
            return None

    def format_kline(self, raw_json: List, resolution: str) -> List[StandardKLine]:
        """解析新浪 K 线 JSON"""
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
    """开盘啦数据源适配器"""
    def format_snapshot(self, raw_data: Dict) -> StandardSnapshot:
        # 开盘啦通常返回 JSON
        return StandardSnapshot(
            code=raw_data.get('code', ''),
            price=float(raw_data.get('price', 0)),
            change_pct=float(raw_data.get('px_change_rate', 0)),
            amount=float(raw_data.get('amount', 0)),
            vol=float(raw_data.get('volume', 0)),
            high=float(raw_data.get('high', 0)),
            low=float(raw_data.get('low', 0)),
            open=float(raw_data.get('open', 0)),
            bid_amt=float(raw_data.get('buy_money', 0)) # 封单金额
        )
