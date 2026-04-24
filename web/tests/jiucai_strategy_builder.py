import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"
ARTICLE_ROOT = ROOT_DIR / "Article"

from web.tests.teacher_alignment_probe import TeacherAlignmentProbe


JIUCAI_FAMILY_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "jiucai_trend_mainline_family": {
        "strategy_id": "jiucai-trend-mainline-family",
        "strategy_key": "jiucai_trend_mainline_family",
        "strategy_name": "主线趋势与容量承接",
        "setup_type": "主线趋势延续、容量中军、题材主升确认",
        "core_thesis": "更强调主线方向的持续性、容量中军的承接以及趋势主升中的确认与节奏管理。",
        "market_features": {"required": ["市场存在主线或次主线"], "optional": ["指数环境未明显破坏承接"]},
        "theme_features": {"required": ["题材具备持续性"], "optional": ["板块内有容量中军与弹性先锋分层"]},
        "stock_features": {"required": ["个股处于启动确认或主升中段"], "optional": ["优先容量和辨识度兼具的标的"]},
        "amount_features": {"required": ["近5日量能不弱于近20日"], "optional": ["关键确认日放量"]},
        "chip_features": {"required": ["主筹成本低于现价或获利盘占优"], "optional": ["筹码集中度适中"]},
        "entry_conditions": ["题材主线仍在，个股承接稳定，量价结构支持继续主升。"],
        "exit_conditions": ["主线切弱、核心股掉队、放量滞涨或失去承接。"],
        "veto_conditions": ["题材只是日内脉冲，没有持续性。"],
        "risk_flags": ["主线高位加速后回撤", "容量股失速"],
        "execution_hints": ["优先主线内最稳的容量股或最先转强的前排。"],
        "cross_teacher_similarity": ["接近 niepan 的 trend_extension_family 与 midcycle_resonance_family"],
        "cross_teacher_difference": ["比 niepan 更重视主线确认与容量承接，少一些纯家族共振表达。"],
        "suggested_family_reuse": ["trend_extension_family", "midcycle_resonance_family"],
        "suggested_new_family": None,
    },
    "jiucai_rotation_repair_family": {
        "strategy_id": "jiucai-rotation-repair-family",
        "strategy_key": "jiucai_rotation_repair_family",
        "strategy_name": "轮动修复与高低切",
        "setup_type": "分歧修复、轮动回流、高低切纠错",
        "core_thesis": "在轮动和修复环境里，关注错杀后的回流、题材高低切和承接修复，而不是追最高潮的一致板。",
        "market_features": {"required": ["市场处于轮动或修复期"], "optional": ["高位负反馈尚可控"]},
        "theme_features": {"required": ["旧热点存在回流可能或新分支承接增强"], "optional": ["高低切结构明确"]},
        "stock_features": {"required": ["个股处于分歧整理或回流修复"], "optional": ["适合低吸或再确认"]},
        "amount_features": {"required": ["修复日量能回暖"], "optional": ["回踩期缩量不破位"]},
        "chip_features": {"required": ["筹码未完全打乱"], "optional": ["回流前密集区仍有效"]},
        "entry_conditions": ["市场修复、题材回流、个股分歧后再转强。"],
        "exit_conditions": ["修复失败、承接消失、轮动再次切走。"],
        "veto_conditions": ["全面退潮期去硬做修复。"],
        "risk_flags": ["假修复", "高低切失败"],
        "execution_hints": ["优先做修复确认和高低切，不抢首个脉冲。"],
        "cross_teacher_similarity": ["接近 niepan 的 rotation_low_suction_family"],
        "cross_teacher_difference": ["比 niepan 更强调纠错、预判和盘后复盘框架中的高低切。"],
        "suggested_family_reuse": ["rotation_low_suction_family"],
        "suggested_new_family": None,
    },
    "jiucai_icepoint_rebound_family": {
        "strategy_id": "jiucai-icepoint-rebound-family",
        "strategy_key": "jiucai_icepoint_rebound_family",
        "strategy_name": "冰点试错与情绪反抽",
        "setup_type": "冰点判断、试错反弹、错杀纠偏",
        "core_thesis": "更注重冰点识别、错杀后的情绪反抽和次日纠错，而不是持续追高。",
        "market_features": {"required": ["情绪冰点或错杀环境"], "optional": ["指数与情绪出现背离"]},
        "theme_features": {"required": ["存在反弹试错方向"], "optional": ["市场下限品种承担稳定作用"]},
        "stock_features": {"required": ["个股处于退潮反抽或预热观察"], "optional": ["辨识度高于板块平均"]},
        "amount_features": {"required": ["恐慌后量能出现反弹或缩量企稳"], "optional": ["次日高开纠错更强"]},
        "chip_features": {"required": ["筹码结构没有彻底崩坏"], "optional": ["错杀后主筹仍在场"]},
        "entry_conditions": ["情绪冰点、错杀明显、次日存在纠错和反抽窗口。"],
        "exit_conditions": ["纠错不能延续、反抽后无承接、情绪再度回落。"],
        "veto_conditions": ["把弱修复误判成新周期启动。"],
        "risk_flags": ["试错失败", "反抽半日游"],
        "execution_hints": ["更多是试错与反抽，不默认等同新主升。"],
        "cross_teacher_similarity": ["接近 niepan 的 high_level_emotion_family 和 rotation_low_suction_family"],
        "cross_teacher_difference": ["更强调冰点、错杀、纠错这些情绪温度词，而非高位空间票本身。"],
        "suggested_family_reuse": ["high_level_emotion_family", "rotation_low_suction_family"],
        "suggested_new_family": None,
    },
    "jiucai_framework_meta_family": {
        "strategy_id": "jiucai-framework-meta-family",
        "strategy_key": "jiucai_framework_meta_family",
        "strategy_name": "方法论框架与市场认知",
        "setup_type": "市场认知、量化冲击、模式总结",
        "core_thesis": "文章重点在解释市场、模式、量化影响和交易认知，不直接对应单日交易 setup。",
        "market_features": {"required": ["文章以方法论和市场框架为主"], "optional": ["大量讨论量化、交易结构和认知"]},
        "theme_features": {"required": ["不依赖单一题材"], "optional": ["会借多个题材举例说明"]},
        "stock_features": {"required": ["个股只是案例样本"], "optional": ["不强调单一买点"]},
        "amount_features": {"required": ["强调量能、情绪、结构"], "optional": []},
        "chip_features": {"required": [], "optional": ["作为解释层而非信号层"]},
        "entry_conditions": ["不作为直接自动执行策略，更多用于框架解释和规则抽象。"],
        "exit_conditions": ["不适用单次出场规则。"],
        "veto_conditions": ["不要把方法论文直接当作下单策略。"],
        "risk_flags": ["信息过泛", "不可直接执行"],
        "execution_hints": ["只做解释层和跨老师对照层输入。"],
        "cross_teacher_similarity": ["接近 niepan 的 framework_meta_family"],
        "cross_teacher_difference": ["更强调量化、指数、市场下限和主力行为框架。"],
        "suggested_family_reuse": ["framework_meta_family"],
        "suggested_new_family": None,
    },
}


class JiucaiStrategyBuilder:
    def __init__(
        self,
        teacher: str = "jiucai",
        archive_root: Optional[Path] = None,
        article_root: Optional[Path] = None,
        probe: Optional[TeacherAlignmentProbe] = None,
    ) -> None:
        self.teacher = teacher
        self.archive_root = archive_root or ARCHIVE_ROOT
        self.article_root = article_root or ARTICLE_ROOT
        self.archive_root.mkdir(parents=True, exist_ok=True)
        self.probe = probe or TeacherAlignmentProbe(teacher=teacher)

    def article_dates(self) -> List[str]:
        dates = []
        for path in sorted((self.article_root / self.teacher).glob("*.md")):
            match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.md", path.name)
            if match:
                dates.append(match.group(1))
        return dates

    def build_all(self) -> Dict[str, Any]:
        cases = []
        for date_str in self.article_dates():
            archive = self.build_case_archive(date_str)
            self.write_case_archive(archive)
            cases.append(archive)

        families = self._build_family_archives(cases)
        family_paths = self._write_family_archives(families)
        catalog = self._build_catalog(cases, families, family_paths)
        index = self._build_index(cases, families)

        catalog_path = self.archive_root / f"strategy_catalog_{self.teacher}.json"
        index_path = self.archive_root / f"strategy_index_{self.teacher}.json"
        catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
        index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "case_count": len(cases),
            "family_count": len([item for item in families.values() if item["case_count"] > 0]),
            "catalog_path": str(catalog_path),
            "index_path": str(index_path),
        }

    def build_case_archive(self, date_str: str) -> Dict[str, Any]:
        report = self.probe.run(date_str, write_snapshot=False)
        article_text = self._load_article_text(date_str)
        primary_family, secondary_family, confidence, reason, matched, missing = self._classify_case(article_text, report)
        family = JIUCAI_FAMILY_DEFINITIONS[primary_family]
        stock_cases = self._build_stock_cases(report.get("sample_stocks", []))
        market_context = self._build_market_context(report)
        teacher_summary = self._build_teacher_summary(family, report, stock_cases)

        return {
            "strategy_id": family["strategy_id"],
            "strategy_name": family["strategy_name"],
            "setup_type": family["setup_type"],
            "article_date": date_str,
            "effective_trade_date": report["effective_date"],
            "teacher": self.teacher,
            "primary_family": primary_family,
            "secondary_family": secondary_family,
            "family_confidence": confidence,
            "why_this_family": reason,
            "matched_features": matched,
            "missing_but_optional_features": missing,
            "cross_teacher_similarity": family["cross_teacher_similarity"],
            "cross_teacher_difference": family["cross_teacher_difference"],
            "suggested_family_reuse": family["suggested_family_reuse"],
            "suggested_new_family": family["suggested_new_family"],
            "teacher_strategy_summary": teacher_summary,
            "market_context": market_context,
            "entry_playbook": {
                "market_prerequisite": family["market_features"]["required"],
                "confirm_signals": family["entry_conditions"],
                "veto_conditions": family["veto_conditions"],
            },
            "exit_playbook": {
                "take_profit_signals": family["exit_conditions"],
                "risk_flags": family["risk_flags"],
            },
            "entry_conditions": family["entry_conditions"],
            "exit_conditions": family["exit_conditions"],
            "stock_cases": stock_cases,
            "risk_flags": family["risk_flags"],
            "reference_stocks": [
                {"stock_name": item["stock_name"], "code6": item["code6"], "role": item["role"]}
                for item in stock_cases
            ],
            "execution_hints": family["execution_hints"],
            "system_mapping": {
                "market_tags": family["market_features"]["required"],
                "theme_tags": family["theme_features"]["required"],
                "stock_tags": family["stock_features"]["required"],
                "amount_tags": family["amount_features"]["required"],
                "chip_tags": family["chip_features"]["required"],
            },
            "template_relation": "candidate_template",
            "template_parent_strategy_id": None,
        }

    def write_case_archive(self, archive: Dict[str, Any]) -> Tuple[Path, Path]:
        json_path = self.archive_root / f"strategy_case_{archive['article_date']}_{self.teacher}.json"
        md_path = self.archive_root / f"strategy_case_{archive['article_date']}_{self.teacher}.md"
        json_path.write_text(json.dumps(archive, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self._build_case_markdown(archive), encoding="utf-8")
        return json_path, md_path

    def _build_family_archives(self, cases: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for case in cases:
            grouped[case["primary_family"]].append(case)

        families = {}
        for key, definition in JIUCAI_FAMILY_DEFINITIONS.items():
            grouped_cases = grouped.get(key, [])
            families[key] = {
                **definition,
                "example_dates": [case["article_date"] for case in grouped_cases],
                "reference_stocks": self._collect_reference_stocks(grouped_cases),
                "case_count": len(grouped_cases),
            }
        return families

    def _write_family_archives(self, families: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        result = {}
        for key, family in families.items():
            if family["case_count"] == 0:
                continue
            json_path = self.archive_root / f"strategy_family_{key}.json"
            md_path = self.archive_root / f"strategy_family_{key}.md"
            json_path.write_text(json.dumps(family, ensure_ascii=False, indent=2), encoding="utf-8")
            md_path.write_text(self._build_family_markdown(family), encoding="utf-8")
            result[key] = json_path.name
        return result

    def _build_catalog(
        self,
        cases: Sequence[Dict[str, Any]],
        families: Dict[str, Dict[str, Any]],
        family_paths: Dict[str, str],
    ) -> Dict[str, Any]:
        return {
            "teacher": self.teacher,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "families": [
                {
                    "strategy_key": family["strategy_key"],
                    "strategy_id": family["strategy_id"],
                    "strategy_name": family["strategy_name"],
                    "setup_type": family["setup_type"],
                    "example_dates": family["example_dates"],
                    "cross_teacher_similarity": family["cross_teacher_similarity"],
                    "suggested_family_reuse": family["suggested_family_reuse"],
                    "path": family_paths[family["strategy_key"]],
                }
                for family in families.values()
                if family["case_count"] > 0 and family["strategy_key"] in family_paths
            ],
            "cases": [f"strategy_case_{case['article_date']}_{self.teacher}.json" for case in cases],
        }

    def _build_index(self, cases: Sequence[Dict[str, Any]], families: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        date_to_family = {}
        family_to_dates: Dict[str, List[str]] = defaultdict(list)
        for case in cases:
            date_to_family[case["article_date"]] = {
                "primary_family": case["primary_family"],
                "secondary_family": case["secondary_family"],
                "cross_teacher_similarity": case["cross_teacher_similarity"],
                "suggested_family_reuse": case["suggested_family_reuse"],
                "suggested_new_family": case["suggested_new_family"],
            }
            family_to_dates[case["primary_family"]].append(case["article_date"])

        return {
            "teacher": self.teacher,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "cases": [f"strategy_case_{case['article_date']}_{self.teacher}.json" for case in cases],
            "families": [
                {
                    "strategy_key": family["strategy_key"],
                    "strategy_id": family["strategy_id"],
                    "strategy_name": family["strategy_name"],
                    "example_dates": family_to_dates.get(family["strategy_key"], []),
                }
                for family in families.values()
                if family["case_count"] > 0
            ],
            "date_to_family": date_to_family,
            "family_to_dates": dict(family_to_dates),
        }

    def _classify_case(
        self, article_text: str, report: Dict[str, Any]
    ) -> Tuple[str, Optional[str], float, str, List[str], List[str]]:
        stage = report.get("rotation_analysis", {}).get("stage", "")
        scores = defaultdict(float)

        def add_if(patterns: Sequence[str], key: str, weight: float) -> None:
            hit = sum(1 for pattern in patterns if pattern in article_text)
            scores[key] += hit * weight

        add_if(["主线", "趋势", "容量", "中军", "承接", "主升"], "jiucai_trend_mainline_family", 2.0)
        add_if(["轮动", "修复", "高低切", "回流", "分歧", "纠错"], "jiucai_rotation_repair_family", 2.2)
        add_if(["冰点", "反弹", "试错", "错杀", "恐慌", "反抽"], "jiucai_icepoint_rebound_family", 2.4)
        add_if(["量化", "逻辑", "思维", "模式", "框架", "为什么", "市场"], "jiucai_framework_meta_family", 1.8)

        stage_map = {
            "题材轮动期": "jiucai_rotation_repair_family",
            "修复回流期": "jiucai_rotation_repair_family",
            "主线聚焦期": "jiucai_trend_mainline_family",
            "高位加速期": "jiucai_trend_mainline_family",
            "情绪退潮期": "jiucai_icepoint_rebound_family",
        }
        if stage in stage_map:
            scores[stage_map[stage]] += 1.5

        if len(report.get("sample_stocks", [])) <= 2:
            scores["jiucai_framework_meta_family"] += 1.2

        sorted_scores = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
        primary = sorted_scores[0][0] if sorted_scores else "jiucai_framework_meta_family"
        secondary = sorted_scores[1][0] if len(sorted_scores) > 1 and sorted_scores[1][1] > 0 else None
        confidence = min(0.95, 0.55 + max(sorted_scores[0][1], 0.0) / 12.0) if sorted_scores else 0.4

        definition = JIUCAI_FAMILY_DEFINITIONS[primary]
        reason = (
            f"文章语义与市场阶段更接近“{definition['strategy_name']}”："
            f"文章关键词、前后市场节奏和样本股所处阶段整体更匹配这类结构。"
        )
        matched = list(dict.fromkeys(definition["market_features"]["required"] + definition["theme_features"]["required"]))
        missing = definition["market_features"].get("optional", [])[:1] + definition["theme_features"].get("optional", [])[:1]
        return primary, secondary, round(confidence, 2), reason, matched, missing

    def _build_market_context(self, report: Dict[str, Any]) -> Dict[str, Any]:
        latest_day = report.get("market_window_5d", [])[-1] if report.get("market_window_5d") else {}
        return {
            "market_window_5d": report.get("market_window_5d", []),
            "market_stage": report.get("rotation_analysis", {}).get("stage"),
            "market_stage_reason": report.get("rotation_analysis", {}).get("reasoning"),
            "emotion_cycle": report.get("emotion_cycle", {}),
            "hot_plates_today": latest_day.get("hot_plates", []),
            "top_themes_today": latest_day.get("top_themes", []),
            "core_samples_today": latest_day.get("core_samples", []),
        }

    def _build_stock_cases(self, stocks: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "stock_name": stock.get("stock_name"),
                "code6": stock.get("code6"),
                "role": stock.get("role"),
                "phase": stock.get("phase"),
                "primary_plate": stock.get("primary_plate"),
                "shape_tags": stock.get("shape_tags", []),
                "chip_profile": stock.get("chip_profile", {}),
                "amount_profile": stock.get("amount_profile", {}),
                "peer_comparison": stock.get("peer_comparison", {}),
                "selection_reason": stock.get("selection_reason"),
            }
            for stock in stocks
        ]

    def _build_teacher_summary(
        self, family: Dict[str, Any], report: Dict[str, Any], stock_cases: Sequence[Dict[str, Any]]
    ) -> str:
        stage = report.get("rotation_analysis", {}).get("stage", "未知阶段")
        stock_hint = stock_cases[0]["stock_name"] if stock_cases else "样本股"
        return f"在{stage}里，老师更偏向“{family['strategy_name']}”这类处理方式，围绕{stock_hint}等样本做主线、修复或反弹的择时判断。"

    def _collect_reference_stocks(self, cases: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
        seen = set()
        rows = []
        for case in cases:
            for stock in case.get("reference_stocks", []):
                key = (stock.get("stock_name"), stock.get("code6"))
                if key in seen:
                    continue
                seen.add(key)
                rows.append(stock)
        return rows[:12]

    def _build_case_markdown(self, archive: Dict[str, Any]) -> str:
        lines = [f"# Strategy Case {archive['article_date']} ({self.teacher})", ""]
        lines.append("## 策略归类")
        lines.append(f"- 主策略族: {archive['primary_family']}")
        if archive.get("secondary_family"):
            lines.append(f"- 次策略族: {archive['secondary_family']}")
        lines.append(f"- 归类置信度: {archive['family_confidence']}")
        lines.append(f"- 归类原因: {archive['why_this_family']}")
        lines.append("")
        lines.append("## 市场环境")
        lines.append(f"- 有效交易日: {archive['effective_trade_date']}")
        lines.append(f"- 市场阶段: {archive['market_context'].get('market_stage')}")
        for item in archive["market_context"].get("hot_plates_today", [])[:5]:
            lines.append(f"- 热门板块: {item.get('name')} rank={item.get('rank')} source={item.get('source')}")
        lines.append("")
        lines.append("## 个股选择逻辑")
        for stock in archive["stock_cases"][:6]:
            lines.append(
                f"- {stock['stock_name']}({stock['code6']}): {stock['role']} / {stock['phase']} / {stock['primary_plate']} / {stock['selection_reason']}"
            )
        lines.append("")
        lines.append("## 介入与退出")
        for item in archive["entry_conditions"]:
            lines.append(f"- 介入: {item}")
        for item in archive["exit_conditions"]:
            lines.append(f"- 退出: {item}")
        lines.append("")
        lines.append("## 跨老师映射")
        for item in archive["cross_teacher_similarity"]:
            lines.append(f"- 相似点: {item}")
        for item in archive["cross_teacher_difference"]:
            lines.append(f"- 差异点: {item}")
        lines.append(f"- 候选复用族: {', '.join(archive['suggested_family_reuse'])}")
        return "\n".join(lines) + "\n"

    def _build_family_markdown(self, family: Dict[str, Any]) -> str:
        lines = [f"# {family['strategy_name']}", ""]
        lines.append(f"- strategy_key: {family['strategy_key']}")
        lines.append(f"- setup_type: {family['setup_type']}")
        lines.append(f"- core_thesis: {family['core_thesis']}")
        lines.append(f"- example_dates: {', '.join(family.get('example_dates', []))}")
        lines.append("")
        lines.append("## 特征")
        for section in ("market_features", "theme_features", "stock_features", "amount_features", "chip_features"):
            lines.append(f"- {section}: required={family[section].get('required', [])} optional={family[section].get('optional', [])}")
        lines.append("")
        lines.append("## 条件")
        for item in family["entry_conditions"]:
            lines.append(f"- 介入: {item}")
        for item in family["exit_conditions"]:
            lines.append(f"- 退出: {item}")
        for item in family["veto_conditions"]:
            lines.append(f"- 否决: {item}")
        lines.append("")
        lines.append("## 跨老师映射")
        for item in family["cross_teacher_similarity"]:
            lines.append(f"- 相似点: {item}")
        for item in family["cross_teacher_difference"]:
            lines.append(f"- 差异点: {item}")
        lines.append(f"- 建议复用: {', '.join(family['suggested_family_reuse'])}")
        return "\n".join(lines) + "\n"

    def _load_article_text(self, date_str: str) -> str:
        path = self.article_root / self.teacher / f"{date_str}.md"
        return path.read_text(encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build jiucai daily strategy archives.")
    parser.add_argument("--teacher", default="jiucai")
    args = parser.parse_args()
    result = JiucaiStrategyBuilder(teacher=args.teacher).build_all()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
