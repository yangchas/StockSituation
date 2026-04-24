import argparse
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import baostock as bs

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
AI_API_DIR = ROOT_DIR / "ai" / "API"
if str(AI_API_DIR) not in sys.path:
    sys.path.insert(0, str(AI_API_DIR))

from ai.API.StockAnalyzer import StockAnalyzer
from ai.API.chip_distribution_analyzer import ChipDistributionAnalyzer
from web.redis_storage import RedisStorageManager
from web.tests.kaipan_history_limit_up_fixed import FixedKaipanHistoryLimitClient
from web.services.f10_service import F10DataService
from web.services.stock_kline_service import StockKLineService
from web.tests.kaipan_history_hot_plate_fixed import FixedKaipanHistoryClient
from web.trade_calendar import TradeCalendar


logger = logging.getLogger(__name__)

ARTICLE_ROOT = ROOT_DIR / "Article"
SNAPSHOT_ROOT = ROOT_DIR / "web" / "snapshots"

BROAD_THEMES = {
    "央企",
    "国企改革",
    "并购重组",
    "中字头",
    "业绩增长",
    "机器人",
    "国资改革",
    "资产重组",
}

SAMPLE_STOCK_OVERRIDES = {
    "niepan": {
        "2026-03-08": ["江钨装备", "章源钨业", "翔鹭钨业", "中钨高新"],
        "2026-03-16": ["豫能控股", "华电辽能", "中国电建", "中国能建"],
        "2026-01-31": ["湖南黄金", "四川黄金", "中金黄金", "中国黄金"],
    }
}


@dataclass
class ThemeStat:
    theme: str
    stock_count: int
    high_lb_count: int
    first_board_count: int
    avg_lb_days: float
    score: float
    sample_stocks: List[str] = field(default_factory=list)


@dataclass
class HotPlateProbeRecord:
    interface: str
    request_date: str
    effective_date: str
    date_format: str
    errcode: str
    count: int
    list_len: int
    list_son_len: int
    list_soninfo_len: int
    day_field: List[str] = field(default_factory=list)
    first_row_preview: Any = None
    diagnosis: str = ""


@dataclass
class MarketDaySnapshot:
    date: str
    effective_date: str
    is_fallback_trade_day: bool
    limit_up_count: int
    broken_count: int
    max_lb_days: int
    negative_feedback_ratio: float
    top_themes: List[ThemeStat]
    hot_plates: List[Dict[str, Any]]
    hot_plate_source: str
    market_phase_hint: str
    core_samples: List[str]


@dataclass
class StockAnalysis:
    stock_name: str
    code6: str
    primary_plate: str
    related_themes: List[str]
    belongs_to_mainline: bool
    role: str
    phase: str
    shape_tags: List[str]
    chip_profile: Dict[str, Any]
    amount_profile: Dict[str, Any]
    peer_comparison: Dict[str, Any]
    selection_reason: str


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _json_load_maybe(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


class TeacherAlignmentProbe:
    def __init__(
        self,
        teacher: str = "niepan",
        remote_host: Optional[str] = None,
        snapshots_dir: Optional[Path] = None,
    ) -> None:
        self.teacher = teacher
        self.remote_host = remote_host
        self.snapshots_dir = snapshots_dir or SNAPSHOT_ROOT
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)

        self.calendar = TradeCalendar()
        self.stock_analyzer = StockAnalyzer()
        self.redis_storage = RedisStorageManager()
        self.f10 = F10DataService()
        self.chip_analyzer = ChipDistributionAnalyzer(price_bins=24)
        self.indicator_service = object.__new__(StockKLineService)
        self.fixed_hot_plate_client = FixedKaipanHistoryClient()
        self.fixed_limit_client = FixedKaipanHistoryLimitClient()

        self._baostock_ready = False
        self._f10_rows: Optional[List[Dict[str, str]]] = None

    def ensure_baostock(self) -> None:
        if self._baostock_ready:
            return
        result = bs.login()
        if result.error_code != "0":
            raise RuntimeError(f"baostock login failed: {result.error_msg}")
        self._baostock_ready = True

    def normalize_trade_date(self, date_str: str) -> Tuple[str, bool]:
        date_str = self._normalize_date(date_str)
        if self.calendar.is_trade_day(date_str):
            return date_str, False
        prev_day = self.calendar.get_previous_trade_day(date_str)
        if prev_day:
            return prev_day, True

        current = datetime.strptime(date_str, "%Y-%m-%d")
        while current.weekday() >= 5:
            current -= timedelta(days=1)
        return current.strftime("%Y-%m-%d"), True

    def previous_trade_days(self, date_str: str, count: int) -> List[str]:
        dates = []
        current = date_str
        for _ in range(count):
            dates.append(current)
            prev_day = self.calendar.get_previous_trade_day(current)
            if not prev_day or prev_day in dates:
                prev_dt = datetime.strptime(current, "%Y-%m-%d") - timedelta(days=1)
                while prev_dt.weekday() >= 5:
                    prev_dt -= timedelta(days=1)
                prev_day = prev_dt.strftime("%Y-%m-%d")
            current = prev_day
        return list(reversed(dates))

    def probe_hot_plate_interfaces(self, date_str: str) -> Dict[str, Any]:
        effective_date, rolled = self.normalize_trade_date(date_str)
        request_dates = [date_str, date_str.replace("-", ""), effective_date, effective_date.replace("-", "")]
        records: List[HotPlateProbeRecord] = []

        for raw_date in request_dates:
            for interface in ("get_his_plates", "get_his_plate_rangs", "get_his_plate_ids"):
                payload = getattr(self.stock_analyzer, interface)(raw_date)
                records.append(self._build_probe_record(interface, raw_date, effective_date, payload))

        reliable = any(record.count > 0 and "ignored" not in record.diagnosis for record in records)
        return {
            "requested_date": date_str,
            "effective_date": effective_date,
            "rolled_to_trade_day": rolled,
            "remote_host": self.remote_host,
            "records": [asdict(record) for record in records],
            "is_reliable": reliable,
        }

    def _build_probe_record(
        self, interface: str, request_date: str, effective_date: str, payload: Any
    ) -> HotPlateProbeRecord:
        if not isinstance(payload, dict):
            return HotPlateProbeRecord(
                interface=interface,
                request_date=request_date,
                effective_date=effective_date,
                date_format="compact" if "-" not in request_date else "hyphen",
                errcode="non_dict",
                count=0,
                list_len=0,
                list_son_len=0,
                list_soninfo_len=0,
                diagnosis="invalid_response_type",
            )

        rows = payload.get("list") or payload.get("List") or []
        list_son = payload.get("list_son") or []
        list_soninfo = payload.get("list_soninfo") or []
        day_field = payload.get("Day") or payload.get("day") or []
        if isinstance(day_field, str):
            day_field = [day_field]
        count = _safe_int(payload.get("Count"), len(rows))

        diagnosis = "ok"
        normalized_request = self._normalize_date(request_date)
        if count == 0 and rows == []:
            diagnosis = "empty_shape"
        if day_field and normalized_request not in day_field and effective_date not in day_field:
            diagnosis = "ignored_date_or_current_day_metadata"

        preview = rows[0] if rows else None
        if isinstance(preview, list):
            preview = preview[:10]

        return HotPlateProbeRecord(
            interface=interface,
            request_date=request_date,
            effective_date=effective_date,
            date_format="compact" if "-" not in request_date else "hyphen",
            errcode=str(payload.get("errcode", "")),
            count=count,
            list_len=len(rows) if isinstance(rows, list) else 0,
            list_son_len=len(list_son) if isinstance(list_son, list) else 0,
            list_soninfo_len=len(list_soninfo) if isinstance(list_soninfo, list) else 0,
            day_field=list(day_field),
            first_row_preview=preview,
            diagnosis=diagnosis,
        )

    def build_market_window(self, date_str: str, sample_stocks: Optional[List[str]] = None) -> Dict[str, Any]:
        effective_date, rolled = self.normalize_trade_date(date_str)
        window_dates = self.previous_trade_days(effective_date, 5)
        hot_plate_probe = self.probe_hot_plate_interfaces(date_str)
        days = [self._build_market_day_snapshot(d) for d in window_dates]
        rotation = self._analyze_rotation(days)
        emotion = self._analyze_emotion(days)

        sample_stock_names = sample_stocks or self.extract_article_stock_names(date_str)
        stock_bundles = self._prepare_stock_bundles(sample_stock_names, effective_date)
        stocks = self._analyze_stocks(stock_bundles, days[-1], rotation)

        return {
            "date": date_str,
            "effective_date": effective_date,
            "rolled_to_trade_day": rolled,
            "remote_host": self.remote_host,
            "hot_plate_probe": hot_plate_probe,
            "market_window_5d": [self._day_to_dict(day) for day in days],
            "rotation_analysis": rotation,
            "emotion_cycle": emotion,
            "sample_stocks": [asdict(stock) for stock in stocks],
        }

    def _build_market_day_snapshot(self, date_str: str) -> MarketDaySnapshot:
        effective_date, rolled = self.normalize_trade_date(date_str)
        limit_events, data_date = self._load_limit_up_events(effective_date)
        broken_events = self._load_broken_events(data_date)
        top_themes = self._build_theme_stats(limit_events)
        hot_plates = self._load_hot_plates(data_date, top_themes)
        max_lb_days = max((event["lb_days"] for event in limit_events), default=0)
        broken_count = len(broken_events)
        limit_up_count = len(limit_events)
        negative_feedback_ratio = round(broken_count / max(limit_up_count, 1), 4)
        market_phase_hint = self._infer_daily_phase_hint(top_themes, negative_feedback_ratio, max_lb_days)
        core_samples = [event["stock_name"] for event in sorted(limit_events, key=lambda e: (-e["lb_days"], -e["amount"]))[:5]]
        return MarketDaySnapshot(
            date=date_str,
            effective_date=data_date,
            is_fallback_trade_day=rolled or data_date != effective_date,
            limit_up_count=limit_up_count,
            broken_count=broken_count,
            max_lb_days=max_lb_days,
            negative_feedback_ratio=negative_feedback_ratio,
            top_themes=top_themes[:5],
            hot_plates=hot_plates[:5],
            hot_plate_source=(
                "historical_hot_plate_fixed"
                if hot_plates and hot_plates[0].get("source") == "historical_hot_plate_fixed"
                else "limit_up_theme_fallback"
            ),
            market_phase_hint=market_phase_hint,
            core_samples=core_samples,
        )

    def _load_limit_up_events(self, date_str: str) -> Tuple[List[Dict[str, Any]], str]:
        search_dates = [date_str]
        current = date_str
        for _ in range(10):
            prev = self.calendar.get_previous_trade_day(current)
            if not prev or prev in search_dates:
                break
            search_dates.append(prev)
            current = prev

        for candidate in search_dates:
            payload = self.fixed_limit_client.get_his_bans(candidate)
            rows = self._extract_pykaipan_rows(payload)
            events = [self._parse_limit_up_row(row) for row in rows if isinstance(row, list) and len(row) >= 6]
            events = [event for event in events if event["code6"]]
            if events:
                return events, candidate
        return [], date_str

    def _load_broken_events(self, date_str: str) -> List[Dict[str, Any]]:
        payload = self.fixed_limit_client.get_his_zha(date_str)
        rows = self._extract_pykaipan_rows(payload)
        return [self._parse_broken_row(row) for row in rows if isinstance(row, list) and len(row) >= 6]

    def _extract_pykaipan_rows(self, payload: Any) -> List[List[Any]]:
        if not isinstance(payload, dict):
            return []
        for key in ("list", "List"):
            rows = payload.get(key)
            if isinstance(rows, list) and rows:
                return rows
        info = payload.get("info")
        if isinstance(info, list) and info:
            first = info[0]
            if isinstance(first, list):
                if first and isinstance(first[0], list):
                    return first
                if first and not isinstance(first[0], list):
                    return info
        return []

    def _parse_limit_up_row(self, row: Sequence[Any]) -> Dict[str, Any]:
        if len(row) >= 33:
            reasons = str(row[11]) if len(row) > 11 else ""
            primary_plate = str(row[16]) if len(row) > 16 else ""
            lb_days = _safe_int(row[10], 0) if len(row) > 10 else 0
            amount = _safe_float(row[13], 0.0) if len(row) > 13 else 0.0
            pct_chg = _safe_float(row[29], 0.0) if len(row) > 29 else 0.0
            lb_label = str(row[9]) if len(row) > 9 else ""
            plate_id = str(row[26]) if len(row) > 26 else ""
        else:
            reasons = str(row[12]) if len(row) > 12 else ""
            primary_plate = str(row[5]) if len(row) > 5 else ""
            lb_days = _safe_int(row[15], 0) if len(row) > 15 else 0
            amount = _safe_float(row[7], 0.0) if len(row) > 7 else 0.0
            pct_chg = _safe_float(row[22], 0.0) if len(row) > 22 else 0.0
            lb_label = str(row[18]) if len(row) > 18 else ""
            plate_id = str(row[19]) if len(row) > 19 else ""
        return {
            "code6": str(row[0])[:6],
            "stock_name": str(row[1]),
            "primary_plate": primary_plate,
            "reasons": reasons,
            "lb_days": lb_days,
            "lb_label": lb_label,
            "plate_id": plate_id,
            "amount": amount,
            "pct_chg": pct_chg,
        }

    def _parse_broken_row(self, row: Sequence[Any]) -> Dict[str, Any]:
        return {
            "code6": str(row[0])[:6],
            "stock_name": str(row[1]),
            "lb_label": str(row[9]) if len(row) > 9 else "",
            "lb_days": _safe_int(row[10], 0) if len(row) > 10 else 0,
            "reasons": str(row[11]) if len(row) > 11 else "",
            "amount": _safe_float(row[13], 0.0) if len(row) > 13 else 0.0,
        }

    def _load_hot_plates(self, date_str: str, top_themes: List[ThemeStat]) -> List[Dict[str, Any]]:
        payload = self.fixed_hot_plate_client.get_his_plates(date_str)
        rows = payload.get("list") if isinstance(payload, dict) else []
        if isinstance(rows, list) and rows:
            hot_plates = []
            for idx, row in enumerate(rows[:10], start=1):
                if not isinstance(row, list) or len(row) < 6:
                    continue
                hot_plates.append(
                    {
                        "id": str(row[0]),
                        "name": str(row[1]),
                        "rank": idx,
                        "strength": _safe_float(row[2]),
                        "change_pct": _safe_float(row[3]),
                        "amount": _safe_float(row[5]),
                        "source": "historical_hot_plate_fixed",
                    }
                )
            if hot_plates:
                return hot_plates

        return [
            {
                "id": f"theme_{idx}",
                "name": theme.theme,
                "rank": idx + 1,
                "strength": round(theme.score, 2),
                "change_pct": 0.0,
                "amount": float(theme.stock_count),
                "source": "limit_up_theme_fallback",
            }
            for idx, theme in enumerate(top_themes[:5])
        ]

    def _load_plate_mapping(self, code6: str) -> List[str]:
        raw = self.redis_storage.redis.hget("config:plate_mapping:s2p", code6)
        if not raw:
            return []
        decoded = _json_load_maybe(raw)
        if isinstance(decoded, list):
            return [str(item) for item in decoded if self._is_specific_theme(str(item))]
        return []

    def _candidate_themes(self, event: Dict[str, Any]) -> List[str]:
        themes = []
        code6 = str(event.get("code6", "")).strip()
        if code6:
            themes.extend(self._load_plate_mapping(code6))
        if event.get("primary_plate"):
            themes.append(event["primary_plate"])
        themes.extend(self._split_themes(event.get("reasons", "")))

        deduped = []
        seen = set()
        for theme in themes:
            theme = str(theme).strip()
            if not theme or theme in seen or not self._is_specific_theme(theme):
                continue
            seen.add(theme)
            deduped.append(theme)
        return deduped

    def _split_themes(self, text: str) -> List[str]:
        if not text:
            return []
        parts = re.split(r"[、,+，/\\s]+", text)
        return [part.strip() for part in parts if self._is_specific_theme(part.strip())]

    def _is_specific_theme(self, theme: str) -> bool:
        if not theme or len(theme) <= 1:
            return False
        return all(broad not in theme for broad in BROAD_THEMES)

    def _build_theme_stats(self, events: List[Dict[str, Any]]) -> List[ThemeStat]:
        stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "high_lb": 0, "first_board": 0, "lb_sum": 0.0, "samples": []}
        )
        for event in events:
            lb_days = max(int(event.get("lb_days", 0)), 1)
            for theme in self._candidate_themes(event):
                bucket = stats[theme]
                bucket["count"] += 1
                bucket["lb_sum"] += lb_days
                if lb_days >= 3:
                    bucket["high_lb"] += 1
                if lb_days <= 1:
                    bucket["first_board"] += 1
                if len(bucket["samples"]) < 4:
                    bucket["samples"].append(event["stock_name"])

        results = []
        for theme, data in stats.items():
            avg_lb = data["lb_sum"] / data["count"] if data["count"] else 0.0
            score = data["count"] * 1.0 + data["high_lb"] * 2.0 + data["first_board"] * 0.5 + avg_lb
            results.append(
                ThemeStat(
                    theme=theme,
                    stock_count=data["count"],
                    high_lb_count=data["high_lb"],
                    first_board_count=data["first_board"],
                    avg_lb_days=round(avg_lb, 2),
                    score=round(score, 2),
                    sample_stocks=data["samples"],
                )
            )
        results.sort(key=lambda item: (-item.score, -item.stock_count, item.theme))
        return results

    def _infer_daily_phase_hint(self, top_themes: List[ThemeStat], negative_feedback_ratio: float, max_lb_days: int) -> str:
        if negative_feedback_ratio >= 1.2:
            return "情绪退潮"
        if top_themes and top_themes[0].high_lb_count >= 2 and max_lb_days >= 4:
            return "高位加速"
        if top_themes and top_themes[0].stock_count >= 3:
            return "主线聚焦"
        return "题材轮动"

    def _analyze_rotation(self, days: List[MarketDaySnapshot]) -> Dict[str, Any]:
        top_theme_names = [day.top_themes[0].theme if day.top_themes else "" for day in days]
        filtered = [name for name in top_theme_names if name]
        counts = Counter(filtered)
        dominant_theme, dominant_days = counts.most_common(1)[0] if counts else ("", 0)
        unique_ratio = round(len(set(filtered)) / max(len(filtered), 1), 4)
        avg_negative_feedback = round(
            sum(day.negative_feedback_ratio for day in days[-3:]) / max(len(days[-3:]), 1), 4
        )

        if unique_ratio >= 0.75 and dominant_days <= 2:
            stage = "题材轮动期"
        elif dominant_days >= 3 and avg_negative_feedback < 0.9:
            stage = "主线聚焦期"
        elif avg_negative_feedback >= 1.2:
            stage = "情绪退潮期"
        elif dominant_days >= 2 and avg_negative_feedback < 1.0:
            stage = "修复回流期"
        elif dominant_days >= 3 and max(day.max_lb_days for day in days) >= 4:
            stage = "高位加速期"
        else:
            stage = "题材轮动期"

        return {
            "stage": stage,
            "dominant_theme": dominant_theme,
            "dominant_days": dominant_days,
            "unique_theme_ratio": unique_ratio,
            "daily_top_themes": top_theme_names,
            "reason": self._rotation_reason(stage, dominant_theme, dominant_days, avg_negative_feedback),
        }

    def _rotation_reason(
        self, stage: str, dominant_theme: str, dominant_days: int, avg_negative_feedback: float
    ) -> str:
        if stage == "主线聚焦期":
            return f"近5日 {dominant_theme} 至少 {dominant_days} 日居前，负反馈处于可控区间。"
        if stage == "情绪退潮期":
            return f"近3日负反馈均值 {avg_negative_feedback:.2f} 偏高，炸板/曾涨停压力明显。"
        if stage == "修复回流期":
            return f"{dominant_theme} 在分歧后重新回到前排，负反馈开始收敛。"
        if stage == "高位加速期":
            return f"{dominant_theme} 持续居前且高标晋级，进入高位扩散阶段。"
        return "前排题材切换较快，市场更像轮动博弈而不是单线聚焦。"

    def _analyze_emotion(self, days: List[MarketDaySnapshot]) -> Dict[str, Any]:
        avg_limit = round(sum(day.limit_up_count for day in days) / max(len(days), 1), 2)
        avg_broken = round(sum(day.broken_count for day in days) / max(len(days), 1), 2)
        avg_feedback = round(sum(day.negative_feedback_ratio for day in days) / max(len(days), 1), 4)
        max_height = max((day.max_lb_days for day in days), default=0)

        if avg_feedback >= 1.1:
            cycle = "退潮"
        elif max_height >= 5 and avg_feedback < 0.9:
            cycle = "加速"
        elif avg_limit >= 20 and avg_feedback < 1.0:
            cycle = "修复走强"
        else:
            cycle = "震荡轮动"

        return {
            "cycle": cycle,
            "avg_limit_up_count": avg_limit,
            "avg_broken_count": avg_broken,
            "avg_negative_feedback_ratio": avg_feedback,
            "max_height": max_height,
        }

    def extract_article_stock_names(self, date_str: str) -> List[str]:
        override = SAMPLE_STOCK_OVERRIDES.get(self.teacher, {}).get(date_str)
        if override:
            return list(override)

        article_path = ARTICLE_ROOT / self.teacher / f"{date_str}.md"
        if not article_path.exists():
            return []

        text = article_path.read_text(encoding="utf-8")
        rows = self._get_f10_rows()
        matches = []
        for row in rows:
            name = row["name"]
            if name and name in text:
                matches.append(name)
        matches = sorted(set(matches), key=lambda item: (-len(item), item))
        return matches[:12]

    def _get_f10_rows(self) -> List[Dict[str, str]]:
        if self._f10_rows is not None:
            return self._f10_rows

        self.f10._load_full_data_if_needed()
        rows: List[Dict[str, str]] = []
        if self.f10.f10_data is not None:
            for _, row in self.f10.f10_data.iterrows():
                code = self.f10._normalize_stock_code(row.get("股票代码", ""))
                name = str(row.get("股票简称", "")).strip()
                if code and name and len(name) >= 2:
                    rows.append({"code6": code, "name": name})
        self._f10_rows = rows
        return rows

    def _prepare_stock_bundles(self, stock_names: List[str], effective_date: str) -> List[Dict[str, Any]]:
        bundles = []
        for stock_name in stock_names:
            code6 = self._find_stock_code(stock_name)
            if not code6:
                continue
            related_themes = self._load_plate_mapping(code6) or self.redis_storage.get_stock_related_themes(code6)
            kline_long = self.fetch_baostock_kline(code6, effective_date, 250)
            kline_near = kline_long[-5:] if len(kline_long) >= 5 else kline_long
            indicators = self.indicator_service.calculate_technical_indicators(kline_long)
            chip_profile = self.build_chip_profile(kline_long)
            amount_profile = self.build_amount_profile(kline_long)
            bundles.append(
                {
                    "stock_name": stock_name,
                    "code6": code6,
                    "related_themes": related_themes,
                    "kline_long": kline_long,
                    "kline_near": kline_near,
                    "indicators": indicators,
                    "chip_profile": chip_profile,
                    "amount_profile": amount_profile,
                }
            )
        return bundles

    def _find_stock_code(self, stock_name: str) -> str:
        results = self.f10.search_stocks(stock_name, limit=10)
        for row in results:
            code = self.f10._normalize_stock_code(row.get("code", ""))
            name = str(row.get("name", "")).strip()
            if name == stock_name and code:
                return code
        for row in results:
            code = self.f10._normalize_stock_code(row.get("code", ""))
            if code:
                return code
        return ""

    def fetch_baostock_kline(self, code6: str, end_date: str, trading_days: int) -> List[Dict[str, Any]]:
        self.ensure_baostock()
        start_date = self._trade_day_offset(end_date, trading_days)
        full_code = f"sz.{code6}" if code6.startswith(("00", "30")) else f"sh.{code6}"
        rs = bs.query_history_k_data_plus(
            full_code,
            "date,open,high,low,close,volume,amount,turn,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            rows.append(
                {
                    "time": row[0],
                    "open": _safe_float(row[1]),
                    "high": _safe_float(row[2]),
                    "low": _safe_float(row[3]),
                    "close": _safe_float(row[4]),
                    "volume": _safe_float(row[5]),
                    "amount": _safe_float(row[6]),
                    "turnover": _safe_float(row[7]),
                    "pct_chg": _safe_float(row[8]),
                }
            )
        return rows

    def _trade_day_offset(self, end_date: str, trading_days: int) -> str:
        current = end_date
        for _ in range(trading_days):
            prev_day = self.calendar.get_previous_trade_day(current)
            if not prev_day:
                prev_dt = datetime.strptime(current, "%Y-%m-%d") - timedelta(days=1)
                while prev_dt.weekday() >= 5:
                    prev_dt -= timedelta(days=1)
                prev_day = prev_dt.strftime("%Y-%m-%d")
            current = prev_day
        return current

    def build_chip_profile(self, kline_long: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not kline_long:
            return {}
        current_price = kline_long[-1]["close"]
        chip = self.chip_analyzer.calculate_chip_distribution(kline_long, current_price=current_price)
        analysis = chip.get("analysis", {})
        concentration = analysis.get("concentration", {}).get("price_range", {})
        profit_loss = analysis.get("profit_loss", {})
        dense_areas = analysis.get("dense_areas", [])
        tags = []
        width = _safe_float(concentration.get("range"), 0.0)
        if current_price and width / current_price <= 0.12:
            tags.append("筹码集中")
        if _safe_float(profit_loss.get("profit_ratio"), 0.0) >= 0.6:
            tags.append("获利盘占优")
        if dense_areas:
            top_area = dense_areas[0]
            area_end = _safe_float(top_area.get("price_range", {}).get("end"))
            if current_price and area_end <= current_price:
                tags.append("主筹成本低于现价")

        return {
            "avg_cost": round(_safe_float(analysis.get("avg_cost")), 4),
            "concentration_range": concentration,
            "profit_loss": profit_loss,
            "dense_areas": dense_areas[:3],
            "tags": tags,
        }

    def build_amount_profile(self, kline_long: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not kline_long:
            return {}
        amounts = [item["amount"] for item in kline_long]
        recent5 = amounts[-5:]
        recent20 = amounts[-20:] if len(amounts) >= 20 else amounts
        avg5 = sum(recent5) / max(len(recent5), 1)
        avg20 = sum(recent20) / max(len(recent20), 1)
        ratio = round(avg5 / max(avg20, 1.0), 4)
        last_amount = recent5[-1] if recent5 else 0.0
        tags = []
        if ratio >= 1.5:
            tags.append("近5日放量")
        elif ratio <= 0.8:
            tags.append("近5日缩量")
        if last_amount >= avg20 * 2:
            tags.append("启动日爆量")
        if len(recent5) >= 3 and recent5[-1] < recent5[-2] < recent5[-3]:
            tags.append("尾段量能回落")
        return {
            "avg_amount_5d": round(avg5, 2),
            "avg_amount_20d": round(avg20, 2),
            "amount_ratio_5d_vs_20d": ratio,
            "latest_amount": round(last_amount, 2),
            "tags": tags,
        }

    def _analyze_stocks(
        self,
        bundles: List[Dict[str, Any]],
        latest_day: MarketDaySnapshot,
        rotation: Dict[str, Any],
    ) -> List[StockAnalysis]:
        latest_events, _ = self._load_limit_up_events(latest_day.effective_date)
        latest_event_map = {event["code6"]: event for event in latest_events}
        mainline_themes = {stat.theme for stat in latest_day.top_themes[:3]}
        analyses = []

        for bundle in bundles:
            event = latest_event_map.get(bundle["code6"], {})
            primary_plate = self._pick_primary_plate(bundle["related_themes"], event, latest_day)
            peer_set = [peer for peer in latest_events if primary_plate and primary_plate in self._candidate_themes(peer)]
            role = self._infer_role(bundle["code6"], event, peer_set, latest_day.max_lb_days)
            phase = self._infer_stock_phase(bundle["kline_long"], event, bundle["amount_profile"])
            shape_tags = self._build_shape_tags(bundle["kline_long"], bundle["amount_profile"])
            belongs_to_mainline = primary_plate in mainline_themes or any(theme in mainline_themes for theme in bundle["related_themes"])
            peer_comparison = self._build_peer_comparison(bundle["code6"], event, peer_set)
            selection_reason = self._build_selection_reason(
                stock_name=bundle["stock_name"],
                primary_plate=primary_plate,
                belongs_to_mainline=belongs_to_mainline,
                role=role,
                phase=phase,
                shape_tags=shape_tags,
                chip_tags=bundle["chip_profile"].get("tags", []),
                amount_tags=bundle["amount_profile"].get("tags", []),
                market_stage=rotation["stage"],
            )
            analyses.append(
                StockAnalysis(
                    stock_name=bundle["stock_name"],
                    code6=bundle["code6"],
                    primary_plate=primary_plate,
                    related_themes=bundle["related_themes"],
                    belongs_to_mainline=belongs_to_mainline,
                    role=role,
                    phase=phase,
                    shape_tags=shape_tags,
                    chip_profile=bundle["chip_profile"],
                    amount_profile=bundle["amount_profile"],
                    peer_comparison=peer_comparison,
                    selection_reason=selection_reason,
                )
            )
        return analyses

    def _pick_primary_plate(
        self, related_themes: List[str], event: Dict[str, Any], latest_day: MarketDaySnapshot
    ) -> str:
        theme_order = [stat.theme for stat in latest_day.top_themes]
        for theme in related_themes:
            if theme in theme_order:
                return theme
        for theme in self._candidate_themes(event):
            if theme in theme_order:
                return theme
        if related_themes:
            return related_themes[0]
        if event.get("primary_plate"):
            return event["primary_plate"]
        return ""

    def _infer_role(
        self, code6: str, event: Dict[str, Any], peers: List[Dict[str, Any]], market_max_lb_days: int
    ) -> str:
        lb_days = max(int(event.get("lb_days", 0)), 0)
        if peers:
            sorted_peers = sorted(peers, key=lambda item: (-item["lb_days"], -item["amount"], item["code6"]))
            for idx, peer in enumerate(sorted_peers):
                if peer["code6"] != code6:
                    continue
                if idx == 0 and lb_days >= max(2, market_max_lb_days - 1):
                    return "龙头"
                if idx <= 2:
                    return "前排"
                if lb_days >= 1:
                    return "补涨"
                return "跟风"
        if lb_days >= market_max_lb_days and lb_days >= 2:
            return "龙头"
        if lb_days >= 1:
            return "前排"
        return "观察"

    def _infer_stock_phase(
        self, kline_long: List[Dict[str, Any]], event: Dict[str, Any], amount_profile: Dict[str, Any]
    ) -> str:
        if not kline_long:
            return "数据不足"
        closes = [item["close"] for item in kline_long]
        current = closes[-1]
        max_60 = max(closes[-60:]) if len(closes) >= 60 else max(closes)
        min_60 = min(closes[-60:]) if len(closes) >= 60 else min(closes)
        lb_days = _safe_int(event.get("lb_days"), 0)
        ratio = _safe_float(amount_profile.get("amount_ratio_5d_vs_20d"), 0.0)

        if lb_days >= 4:
            return "高位加速期"
        if lb_days >= 2 or (current >= max_60 * 0.98 and ratio >= 1.3):
            return "启动确认期"
        if current >= min_60 * 1.25 and ratio >= 1.1:
            return "主升中段"
        if ratio <= 0.9:
            return "预热观察期"
        return "分歧整理期"

    def _build_shape_tags(self, kline_long: List[Dict[str, Any]], amount_profile: Dict[str, Any]) -> List[str]:
        if not kline_long:
            return []
        closes = [item["close"] for item in kline_long]
        highs = [item["high"] for item in kline_long]
        lows = [item["low"] for item in kline_long]
        last_close = closes[-1]
        recent20 = closes[-20:] if len(closes) >= 20 else closes
        tags = []

        if recent20:
            range20 = (max(recent20) - min(recent20)) / max(min(recent20), 0.01)
            if range20 <= 0.18:
                tags.append("平台压缩")
        if last_close >= max(highs[-30:]) * 0.98:
            tags.append("临近阶段新高")
        if _safe_float(amount_profile.get("amount_ratio_5d_vs_20d")) >= 1.5:
            tags.append("放量突破")
        if len(closes) >= 3 and closes[-1] > closes[-2] > closes[-3]:
            tags.append("短线加速")
        if len(lows) >= 8 and min(lows[-3:]) > min(lows[-8:-3]):
            tags.append("低点抬高")
        return tags

    def _build_peer_comparison(
        self, code6: str, event: Dict[str, Any], peers: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        if not peers:
            return {"peer_count": 0}
        sorted_peers = sorted(peers, key=lambda item: (-item["lb_days"], -item["amount"], item["code6"]))
        rank = next((idx + 1 for idx, peer in enumerate(sorted_peers) if peer["code6"] == code6), None)
        return {
            "peer_count": len(sorted_peers),
            "rank_within_theme": rank,
            "top_peer": {
                "stock_name": sorted_peers[0]["stock_name"],
                "lb_days": sorted_peers[0]["lb_days"],
                "amount": sorted_peers[0]["amount"],
            },
            "self_lb_days": event.get("lb_days", 0),
            "self_amount": event.get("amount", 0.0),
        }

    def _build_selection_reason(
        self,
        stock_name: str,
        primary_plate: str,
        belongs_to_mainline: bool,
        role: str,
        phase: str,
        shape_tags: List[str],
        chip_tags: List[str],
        amount_tags: List[str],
        market_stage: str,
    ) -> str:
        parts = []
        if belongs_to_mainline and primary_plate:
            parts.append(f"属于当期主线 {primary_plate}")
        if role not in {"观察", ""}:
            parts.append(f"题材位置偏{role}")
        if phase:
            parts.append(f"个股处于{phase}")
        if shape_tags:
            parts.append("形态上出现" + "、".join(shape_tags[:2]))
        if chip_tags:
            parts.append("筹码上具备" + "、".join(chip_tags[:2]))
        if amount_tags:
            parts.append("量能呈现" + "、".join(amount_tags[:2]))
        if market_stage:
            parts.append(f"适配当前{market_stage}")
        return f"{stock_name} 被选中的主要原因是：" + "；".join(parts) + "。"

    def _day_to_dict(self, day: MarketDaySnapshot) -> Dict[str, Any]:
        data = asdict(day)
        data["top_themes"] = [asdict(item) for item in day.top_themes]
        return data

    def build_markdown_report(self, report: Dict[str, Any]) -> str:
        lines = []
        lines.append(f"# Teacher Alignment Probe {report['date']} ({self.teacher})")
        lines.append("")
        lines.append("## 前5日市场节奏")
        for day in report["market_window_5d"]:
            top_theme = day["top_themes"][0]["theme"] if day["top_themes"] else "无"
            lines.append(
                f"- {day['date']} -> 有效日 {day['effective_date']} | 涨停 {day['limit_up_count']} | 炸板 {day['broken_count']} | "
                f"最高连板 {day['max_lb_days']} | 主线候选 {top_theme} | 热门板块来源 {day['hot_plate_source']}"
            )

        lines.append("")
        lines.append("## 当日主线与热门板块")
        latest_day = report["market_window_5d"][-1]
        for plate in latest_day["hot_plates"]:
            lines.append(f"- {plate['rank']}. {plate['name']} ({plate['source']})")
        lines.append(
            f"- 主线阶段: {report['rotation_analysis']['stage']} | 理由: {report['rotation_analysis']['reason']}"
        )

        lines.append("")
        lines.append("## 情绪周期结论")
        emotion = report["emotion_cycle"]
        lines.append(
            f"- 情绪周期: {emotion['cycle']} | 平均涨停 {emotion['avg_limit_up_count']} | 平均炸板 {emotion['avg_broken_count']} | "
            f"平均负反馈 {emotion['avg_negative_feedback_ratio']} | 最高板 {emotion['max_height']}"
        )

        lines.append("")
        lines.append("## 老师个股对照表")
        for stock in report["sample_stocks"]:
            lines.append(
                f"- {stock['stock_name']} {stock['code6']} | 主板块 {stock['primary_plate']} | 角色 {stock['role']} | 阶段 {stock['phase']} | "
                f"形态 {'、'.join(stock['shape_tags']) or '无'}"
            )

        lines.append("")
        lines.append("## 为什么这天选它")
        for stock in report["sample_stocks"]:
            lines.append(f"- {stock['selection_reason']}")

        lines.append("")
        lines.append("## 历史热门板块接口诊断")
        probe = report["hot_plate_probe"]
        lines.append(
            f"- 请求日 {probe['requested_date']} | 有效交易日 {probe['effective_date']} | 接口整体可靠性 {probe['is_reliable']}"
        )
        for record in probe["records"]:
            lines.append(
                f"- {record['interface']}({record['request_date']}) -> count={record['count']} list={record['list_len']} day={record['day_field']} diagnosis={record['diagnosis']}"
            )
        return "\n".join(lines)

    def write_report(self, report: Dict[str, Any]) -> Tuple[Path, Path]:
        json_path = self.snapshots_dir / f"teacher_alignment_probe_{report['date']}_{self.teacher}.json"
        md_path = self.snapshots_dir / f"teacher_alignment_probe_{report['date']}_{self.teacher}.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self.build_markdown_report(report), encoding="utf-8")
        return json_path, md_path

    def run(self, date_str: str, write_snapshot: bool = True) -> Dict[str, Any]:
        sample_stocks = self.extract_article_stock_names(date_str)
        report = self.build_market_window(date_str, sample_stocks=sample_stocks)
        report["teacher"] = self.teacher
        if write_snapshot:
            json_path, md_path = self.write_report(report)
            report["snapshot_json"] = str(json_path)
            report["snapshot_md"] = str(md_path)
        return report

    def _normalize_date(self, date_str: str) -> str:
        date_str = str(date_str).strip()
        if re.fullmatch(r"\d{8}", date_str):
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        return date_str


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Independent teacher alignment probe")
    parser.add_argument("--date", action="append", required=True, help="Target date, e.g. 2026-03-08")
    parser.add_argument("--teacher", default="niepan")
    parser.add_argument("--remote-host", default=None)
    parser.add_argument("--probe-hot-plates", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    probe = TeacherAlignmentProbe(teacher=args.teacher, remote_host=args.remote_host)
    for date_str in args.date:
        report = probe.run(date_str, write_snapshot=not args.no_write)
        if args.probe_hot_plates:
            print(json.dumps(report["hot_plate_probe"], ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "date": report["date"],
                        "effective_date": report["effective_date"],
                        "rotation_stage": report["rotation_analysis"]["stage"],
                        "emotion_cycle": report["emotion_cycle"]["cycle"],
                        "stocks": [stock["stock_name"] for stock in report["sample_stocks"]],
                        "snapshot_json": report.get("snapshot_json"),
                        "snapshot_md": report.get("snapshot_md"),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
