import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class ThemeItem:
    theme: str
    score: float
    first_limit_count: int
    strong_stock_count: int
    stock_count: int
    leaders: List[str]
    total_amount_2min: float


class SimpleThemeNormalizer:
    """简单的题材名称归一化器，去掉多余空格、逗号，保留核心词。"""
    
    def normalize_list(self, themes: List[str]) -> List[str]:
        if not themes:
            return []
        
        normalized = []
        seen = set()
        
        for t in themes:
            if not isinstance(t, str):
                continue
            # 基本清理
            t = t.strip().replace("，", ",").replace(" ", "")
            if not t or len(t) < 2:
                continue
            
            # 分割复合题材 (如 "光伏,储能")
            parts = t.split(",")
            for p in parts:
                p = p.strip()
                if p and len(p) >= 2 and p not in seen:
                    normalized.append(p)
                    seen.add(p)
        
        return normalized


class ThemeRanker:

    """流程二：主线题材榜（事件×情绪×扩散）的最小可用实现。

    设计目标：
    - 只依赖现有事实层（Redis + plate_updater + StockAnalyzer.parse_ban_reasons），避免新增外部请求。
    - 支持多对多：个股 -> 多个题材（开盘啦 reasons）并可选 fallback 到板块名称。
    - 输出可解释字段（leaders、count、amount_2min、primary_theme、themes_top3），便于复盘与调参。

    约定：
    - 计算聚合时对多题材按权重分摊，避免重复计数导致题材“虚胖”。
    - 展示时每只股票用 primary_theme 作为主显示，并保留 themes_top3 作为解释。
    """

    def __init__(
        self,
        theme_normalizer,
        stock_analyzer,
        plate_updater=None,
    ):
        self.theme_normalizer = theme_normalizer
        self.stock_analyzer = stock_analyzer
        self.plate_updater = plate_updater

    def _extract_themes_from_reasons(self, reasons: Any) -> List[str]:
        """兼容 StockAnalyzer.parse_ban_reasons 的输出结构。"""
        themes: List[str] = []
        if not reasons:
            return themes

        # parse_ban_reasons 通常返回 List[Dict]
        if isinstance(reasons, list):
            for r in reasons:
                if isinstance(r, dict):
                    # 常见字段: reason/theme/concept
                    for k in ("theme", "reason", "concept", "name", "plate", "sector"):
                        v = r.get(k)
                        if isinstance(v, str) and v.strip():
                            themes.append(v.strip())
                elif isinstance(r, str) and r.strip():
                    themes.append(r.strip())
        elif isinstance(reasons, dict):
            for k in ("theme", "reason", "concept", "name", "plate", "sector"):
                v = reasons.get(k)
                if isinstance(v, str) and v.strip():
                    themes.append(v.strip())
        elif isinstance(reasons, str) and reasons.strip():
            themes.append(reasons.strip())

        return self.theme_normalizer.normalize_list(themes)

    def _fallback_plate_themes(self, code6: str) -> List[str]:
        if not self.plate_updater:
            return []
        plate_ids = self.plate_updater.stock_to_plates.get(code6, [])
        out = []
        for pid in plate_ids[:3]:
            name = self.plate_updater.all_plates.get(pid, {}).get("name")
            if name:
                out.append(name)
        return self.theme_normalizer.normalize_list(out)

    def build_stock_theme_evidence(
        self,
        code6: str,
        max_themes: int = 3,
    ) -> Dict[str, Any]:
        """为单只股票构建题材证据：primary_theme + themes_top3。

        当前版本：
        - 证据来源优先：开盘啦涨停原因(若能取到) -> 板块映射fallback。
        - 多题材权重：按顺序衰减（1.0, 0.6, 0.4），再归一化到和为1。

        返回：
        {
          "primary_theme": str,
          "themes_top3": [{"theme": str, "w": float, "sources": [str]}]
        }
        """
        sources: List[str] = []

        themes: List[str] = []
        try:
            raw = self.stock_analyzer.get_ban_reasons(code6)
            parsed = self.stock_analyzer.parse_ban_reasons(raw) if raw else []
            themes = self._extract_themes_from_reasons(parsed)
            if themes:
                sources.append("ban_reason")
        except Exception:
            themes = []

        if not themes:
            themes = self._fallback_plate_themes(code6)
            if themes:
                sources.append("plate_map")

        themes = themes[:max_themes]
        if not themes:
            return {"primary_theme": "", "themes_top3": []}

        base = [1.0, 0.6, 0.4]
        ws = base[: len(themes)]
        s = sum(ws)
        ws = [w / s for w in ws]

        themes_top3 = [
            {"theme": t, "w": round(float(w), 4), "sources": sources}
            for t, w in zip(themes, ws)
        ]

        primary_theme = themes_top3[0]["theme"] if themes_top3 else ""

        # 冲突标记：top1 和 top2 过近
        conflict = False
        if len(themes_top3) >= 2 and abs(themes_top3[0]["w"] - themes_top3[1]["w"]) < 0.15:
            conflict = True

        return {
            "primary_theme": primary_theme,
            "themes_top3": themes_top3,
            "theme_conflict": conflict,
        }

    def build(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators_by_stock: Dict[str, Dict[str, Any]],
        first_limit_set: Optional[Set[str]] = None,
        strong_change_pct: float = 1.0,
        strong_amount_2min: float = 10_000_000,
        top_n: int = 50,
    ) -> Tuple[List[ThemeItem], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """返回 (主题榜列表, 主题详情dict, 个股题材证据dict)。

        indicators_by_stock: 必须包含 change_pct / amount_2min 等字段（来自 OptimizedAdvancedTechnicalIndicators）。
        first_limit_set: 严格首板集合（可选，用于加权/统计）。

        stock_theme_evidence：
        - key=code6
        - value={primary_theme,themes_top3,theme_conflict}
        """
        first_limit_set = first_limit_set or set()

        theme_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "stock_count": 0.0,
            "first_limit_count": 0.0,
            "strong_stock_count": 0.0,
            "total_amount_2min": 0.0,
            "leader_score": [],
        })

        stock_theme_evidence: Dict[str, Dict[str, Any]] = {}

        for code6 in candidate_pool:
            ind = indicators_by_stock.get(code6) or {}
            change_pct = float(ind.get("change_pct", 0) or 0)
            amount_2min = float(ind.get("amount_2min", 0) or 0)

            is_strong = (change_pct >= strong_change_pct) and (amount_2min >= strong_amount_2min)
            is_first = code6 in first_limit_set

            evidence = self.build_stock_theme_evidence(code6)
            stock_theme_evidence[code6] = evidence

            themes_top3 = evidence.get("themes_top3") or []
            if not themes_top3:
                continue

            leader_score = (amount_2min ** 0.5) * (1.0 + max(0.0, change_pct) / 10.0)

            for t in themes_top3:
                theme = t.get("theme")
                w = float(t.get("w", 0) or 0)
                if not theme or w <= 0:
                    continue

                s = theme_stats[theme]
                s["stock_count"] += w
                if is_first:
                    s["first_limit_count"] += w
                if is_strong:
                    s["strong_stock_count"] += w
                    s["total_amount_2min"] += amount_2min * w

                s["leader_score"].append((leader_score * w, code6))

        items: List[ThemeItem] = []
        details: Dict[str, Dict[str, Any]] = {}

        for theme, s in theme_stats.items():
            if s["stock_count"] <= 0:
                continue

            leaders_sorted = sorted(s["leader_score"], key=lambda x: x[0], reverse=True)
            leaders = [c for _, c in leaders_sorted[:3]]

            # 最小可用评分：强势股数量 + 首板数量 + 金额深度
            score = (
                s["strong_stock_count"] * 2.0 +
                s["first_limit_count"] * 3.0 +
                (s["total_amount_2min"] / 10_000_000)  # 每1000万=1分
            )
            score = round(float(score), 4)

            item = ThemeItem(
                theme=theme,
                score=score,
                first_limit_count=int(round(s["first_limit_count"])),
                strong_stock_count=int(round(s["strong_stock_count"])),
                stock_count=int(round(s["stock_count"])),
                leaders=leaders,
                total_amount_2min=float(s["total_amount_2min"]),
            )
            items.append(item)

            details[theme] = {
                "ts": int(time.time() * 1000),
                "theme": theme,
                "score": score,
                "stock_count": round(float(s["stock_count"]), 4),
                "first_limit_count": round(float(s["first_limit_count"]), 4),
                "strong_stock_count": round(float(s["strong_stock_count"]), 4),
                "total_amount_2min": float(s["total_amount_2min"]),
                "leaders": leaders,
            }

        items.sort(key=lambda x: x.score, reverse=True)
        items = items[:top_n]

        # 只保留 topN 主题 of details，减少 Redis 压力
        top_themes = {i.theme for i in items}
        details = {k: v for k, v in details.items() if k in top_themes}

        return items, details, stock_theme_evidence
