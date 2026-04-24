import logging
import io
import contextlib
from typing import Any, Dict, List


logger = logging.getLogger(__name__)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _load_pykaipan_module():
    """
    兼容两种导入形式：
    1) from pykaipan import pykaipan as kp
    2) import pykaipan as kp
    """
    try:
        # pykaipan 在 import 时会 print 路径，这里静默掉，避免污染主日志
        with contextlib.redirect_stdout(io.StringIO()):
            from pykaipan import pykaipan as kp  # type: ignore
        return kp
    except Exception:
        with contextlib.redirect_stdout(io.StringIO()):
            import pykaipan as kp  # type: ignore
        return kp


def fetch_kaipan_plate_rank(date_str: str = "", index: str = "0", size: str = "80") -> Dict[str, Any]:
    """
    拉取开盘啦板块强度榜并规范化输出。支持指定日期。
    """
    try:
        kp = _load_pykaipan_module()
        if not hasattr(kp, "getHisPlates"):
            return {"ok": False, "count": 0, "plates": [], "error": "pykaipan.getHisPlates not found"}

        raw = kp.getHisPlates(str(date_str), str(index), str(size))
        if not isinstance(raw, dict):
            return {"ok": False, "count": 0, "plates": [], "error": f"unexpected response type: {type(raw).__name__}"}

        rows = raw.get("list") or raw.get("List") or []
        if not isinstance(rows, list):
            return {"ok": False, "count": 0, "plates": [], "error": "response list field invalid"}

        parsed: List[Dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            # 兼容 list 结构（你给的示例就是这种）
            if isinstance(row, (list, tuple)) and len(row) >= 13:
                pid = str(row[0]).strip()
                if not pid:
                    continue
                parsed.append(
                    {
                        "id": pid,
                        "name": str(row[1]).strip(),
                        "rank": idx,
                        "strength": _safe_float(row[2]),
                        "change_pct": _safe_float(row[3]),
                        "change_speed": _safe_float(row[4]),
                        "amount": _safe_float(row[5]),
                        "main_net": _safe_float(row[6]),
                        "main_buy": _safe_float(row[7]),
                        "main_sell": _safe_float(row[8]),
                        "vol_ratio": _safe_float(row[9]),
                        "float_mv": _safe_float(row[10]),
                        "big_order_net": _safe_float(row[12]),
                        "total_mv": _safe_float(row[13]) if len(row) > 13 else 0.0,
                    }
                )
            # 兼容 dict 结构（防未来版本变化）
            elif isinstance(row, dict):
                pid = str(row.get("id", row.get("PlateID", ""))).strip()
                if not pid:
                    continue
                parsed.append(
                    {
                        "id": pid,
                        "name": str(row.get("name", row.get("Name", ""))).strip(),
                        "rank": idx,
                        "strength": _safe_float(row.get("强度", row.get("strength", 0.0))),
                        "change_pct": _safe_float(row.get("涨幅", row.get("change_pct", 0.0))),
                        "change_speed": _safe_float(row.get("涨速", row.get("change_speed", 0.0))),
                        "amount": _safe_float(row.get("成交额", row.get("amount", 0.0))),
                        "main_net": _safe_float(row.get("主力净额", row.get("main_net", 0.0))),
                        "main_buy": _safe_float(row.get("主力买", row.get("main_buy", 0.0))),
                        "main_sell": _safe_float(row.get("主力卖", row.get("main_sell", 0.0))),
                        "vol_ratio": _safe_float(row.get("量比", row.get("vol_ratio", 0.0))),
                        "float_mv": _safe_float(row.get("流通值", row.get("float_mv", 0.0))),
                        "big_order_net": _safe_float(row.get("大单净额", row.get("big_order_net", 0.0))),
                        "total_mv": _safe_float(row.get("总市值", row.get("total_mv", 0.0))),
                    }
                )

        return {"ok": True, "count": len(parsed), "plates": parsed}
    except Exception as e:
        logger.warning(f"Kaipan plate fetch failed: {e}")
        return {"ok": False, "count": 0, "plates": [], "error": str(e)}
