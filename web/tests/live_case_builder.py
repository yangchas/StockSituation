import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web.tests.live_strategy_extractor import FAMILY_MAPPING, LiveStrategyExtractor
from web.tests.strategy_archive_builder import FAMILY_DEFINITIONS
from web.tests.teacher_alignment_probe import TeacherAlignmentProbe


logger = logging.getLogger(__name__)

DEFAULT_ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"
DEFAULT_ARTICLE_ROOT = ROOT_DIR / "Article"


class LiveCaseBuilder:
    def __init__(
        self,
        teacher: str = "niepan",
        archive_root: Optional[Path] = None,
        article_root: Optional[Path] = None,
        probe: Optional[TeacherAlignmentProbe] = None,
        extractor: Optional[LiveStrategyExtractor] = None,
    ) -> None:
        self.teacher = teacher
        self.archive_root = archive_root or DEFAULT_ARCHIVE_ROOT
        self.archive_root.mkdir(parents=True, exist_ok=True)
        self.article_root = article_root or DEFAULT_ARTICLE_ROOT
        self.probe = probe or TeacherAlignmentProbe(teacher=teacher)
        self.extractor = extractor or LiveStrategyExtractor(
            teacher=teacher,
            article_root=self.article_root,
            archive_root=self.archive_root,
        )
        self._market_cache: Dict[str, Dict[str, Any]] = {}
        self._day_snapshot_cache: Dict[str, Dict[str, Any]] = {}

    def build_all_live_cases(self) -> List[Dict[str, Any]]:
        records = self._load_live_records()
        cases = [self.build_live_case(record, records) for record in records if record.get("publish_date")]
        return cases

    def build_and_write_all_live_cases(self) -> List[Dict[str, Any]]:
        records = self._load_live_records()
        cases = []
        for record in records:
            if not record.get("publish_date"):
                continue
            case = self.build_live_case(record, records)
            self.write_live_case(case)
            cases.append(case)
        self.update_catalog(cases)
        self.update_index(cases)
        return cases

    def build_live_case(self, record: Dict[str, Any], all_records: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        publish_date = record["publish_date"]
        market_report = self._market_report_for_record(record)
        latest_day = market_report["market_window_5d"][-1] if market_report["market_window_5d"] else {}
        selected_stocks = market_report.get("sample_stocks", [])
        linked_families = self._linked_families(record)
        primary_family = linked_families[0] if linked_families else None
        previous_record = self._previous_record(record, all_records or [])

        case = {
            "teacher": self.teacher,
            "live_day": record["live_day"],
            "publish_date": publish_date,
            "effective_trade_date": market_report["effective_date"],
            "title": record["title"],
            "raw_text": record["raw_text"],
            "raw_actions": record["actions"],
            "matched_patterns": record["matched_patterns"],
            "primary_pattern": record["primary_pattern"],
            "market_context": {
                "market_stage": market_report["rotation_analysis"]["stage"],
                "market_reason": market_report["rotation_analysis"]["reason"],
                "mainline_themes": market_report["rotation_analysis"]["daily_top_themes"],
                "effective_trade_date": market_report["effective_date"],
                "rolled_to_trade_day": market_report["rolled_to_trade_day"],
            },
            "hot_plates_today": latest_day.get("hot_plates", []),
            "rotation_analysis": market_report["rotation_analysis"],
            "emotion_cycle": market_report["emotion_cycle"],
            "selected_stocks": selected_stocks,
            "action_interpretation": self._build_action_interpretation(record, market_report, selected_stocks),
            "entry_logic": self._build_entry_logic(record, market_report, selected_stocks),
            "hold_logic": self._build_hold_logic(record, market_report, selected_stocks),
            "exit_logic": self._build_exit_logic(record, market_report, selected_stocks),
            "risk_notes": self._build_risk_notes(record, market_report),
            "linked_strategy_families": linked_families,
            "previous_live_day_change": self._build_previous_day_change(record, previous_record, market_report),
            "snapshot_source": "teacher_alignment_probe_market_chain",
        }
        if primary_family and primary_family in FAMILY_DEFINITIONS:
            family = FAMILY_DEFINITIONS[primary_family]
            case["linked_family_details"] = {
                "primary_family": primary_family,
                "strategy_id": family["strategy_id"],
                "strategy_name": family["strategy_name"],
                "setup_type": family["setup_type"],
            }
        return case

    def write_live_case(self, case: Dict[str, Any]) -> Tuple[Path, Path]:
        json_path = self.archive_root / f"live_case_{case['publish_date']}_{self.teacher}.json"
        md_path = self.archive_root / f"live_case_{case['publish_date']}_{self.teacher}.md"
        json_path.write_text(json.dumps(case, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self.build_markdown(case), encoding="utf-8")
        return json_path, md_path

    def write_all_live_cases(self, cases: List[Dict[str, Any]]) -> List[Tuple[Path, Path]]:
        written = [self.write_live_case(case) for case in cases]
        self.update_catalog(cases)
        self.update_index(cases)
        return written

    def build_markdown(self, case: Dict[str, Any]) -> str:
        lines = [f"# Live Case {case['publish_date']} ({self.teacher})", ""]
        lines.append("## 当天市场环境")
        lines.append(
            f"- 实盘第 {case['live_day']} 天 | 有效交易日 {case['effective_trade_date']} | 市场阶段 {case['market_context']['market_stage']}"
        )
        lines.append(f"- 主线候选: {'、'.join(case['market_context']['mainline_themes']) or '无'}")
        if case["hot_plates_today"]:
            lines.append(
                "- 热门板块: "
                + "；".join(f"{item.get('rank', '?')}. {item.get('name', '')}" for item in case["hot_plates_today"][:5])
            )
        lines.append(f"- 情绪周期: {case['emotion_cycle'].get('cycle', '未知')}")
        lines.append("")

        lines.append("## 老师动作拆解")
        lines.append(f"- 标题: {case['title']}")
        lines.append(f"- 动作: {'、'.join(case['raw_actions']) or '无'}")
        lines.append(f"- 交易意图: {case['action_interpretation']['summary']}")
        for reason in case["action_interpretation"]["reasoning"]:
            lines.append(f"- 理由: {reason}")
        lines.append("")

        lines.append("## 个股选择逻辑")
        if case["selected_stocks"]:
            for stock in case["selected_stocks"]:
                lines.append(
                    f"- {stock['stock_name']} {stock['code6']} | 角色 {stock['role']} | 阶段 {stock['phase']} | "
                    f"主板块 {stock['primary_plate']} | 形态 {'、'.join(stock['shape_tags']) or '无'}"
                )
                lines.append(f"- 选择理由: {stock['selection_reason']}")
        else:
            lines.append("- 当天无明确个股样本，重点在动作和风险管理。")
        lines.append("")

        lines.append("## 介入 / 持仓 / 退出逻辑")
        for item in case["entry_logic"]:
            lines.append(f"- 介入: {item}")
        for item in case["hold_logic"]:
            lines.append(f"- 持仓: {item}")
        for item in case["exit_logic"]:
            lines.append(f"- 退出: {item}")
        lines.append("")

        lines.append("## 和前一实盘日的变化")
        lines.append(f"- {case['previous_live_day_change']}")
        lines.append("")

        lines.append("## 映射到已有策略族")
        lines.append(f"- 关联策略族: {'、'.join(case['linked_strategy_families']) or '无'}")
        if case.get("linked_family_details"):
            details = case["linked_family_details"]
            lines.append(f"- 主映射: {details['strategy_name']} | 类型 {details['setup_type']}")
        for item in case["risk_notes"]:
            lines.append(f"- 风险: {item}")
        return "\n".join(lines)

    def update_catalog(self, cases: List[Dict[str, Any]]) -> Path:
        catalog_path = self.archive_root / f"strategy_catalog_{self.teacher}.json"
        payload = {}
        if catalog_path.exists():
            payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        archives = payload.get("live_case_archives", [])
        for case in cases:
            name = f"live_case_{case['publish_date']}_{self.teacher}.json"
            if name not in archives:
                archives.append(name)
        payload["live_case_archives"] = sorted(archives)
        catalog_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return catalog_path

    def update_index(self, cases: List[Dict[str, Any]]) -> Path:
        index_path = self.archive_root / f"strategy_index_{self.teacher}.json"
        payload = {}
        if index_path.exists():
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        live_cases = []
        publish_date_to_live_case = {}
        live_day_to_publish_date = {}
        for case in cases:
            name = f"live_case_{case['publish_date']}_{self.teacher}.json"
            live_cases.append(
                {
                    "live_day": case["live_day"],
                    "publish_date": case["publish_date"],
                    "effective_trade_date": case["effective_trade_date"],
                    "primary_pattern": case["primary_pattern"],
                    "linked_strategy_families": case["linked_strategy_families"],
                    "archive": name,
                }
            )
            publish_date_to_live_case[case["publish_date"]] = name
            live_day_to_publish_date[str(case["live_day"])] = case["publish_date"]
        payload["live_cases"] = sorted(live_cases, key=lambda item: item["publish_date"])
        payload["publish_date_to_live_case"] = publish_date_to_live_case
        payload["live_day_to_publish_date"] = live_day_to_publish_date
        index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return index_path

    def _load_live_records(self) -> List[Dict[str, Any]]:
        text = (self.article_root / self.teacher / "实盘.md").read_text(encoding="utf-8")
        records = self.extractor.parse_records(text)
        return [self.extractor.classify_record(item) for item in records]

    def _market_report_for_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        publish_date = record["publish_date"]
        if publish_date in self._market_cache:
            return self._market_cache[publish_date]
        report = self._build_market_window_for_live_record(record)
        self._market_cache[publish_date] = report
        return report

    def _build_market_window_for_live_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        date_str = record["publish_date"]
        sample_stocks = record.get("stocks") or []
        if hasattr(self.probe, "normalize_trade_date"):
            effective_date, rolled = self.probe.normalize_trade_date(date_str)
            window_dates = self.probe.previous_trade_days(effective_date, 5)
            hot_plate_probe = self.probe.probe_hot_plate_interfaces(date_str)
            if all(hasattr(self.probe, name) for name in ["_build_market_day_snapshot", "_analyze_rotation", "_analyze_emotion", "_day_to_dict"]):
                days = [self._cached_day_snapshot(d) for d in window_dates]
                rotation = self.probe._analyze_rotation(days)
                emotion = self.probe._analyze_emotion(days)
                stocks = []
                if sample_stocks and all(hasattr(self.probe, name) for name in ["_prepare_stock_bundles", "_analyze_stocks"]):
                    stock_bundles = self.probe._prepare_stock_bundles(sample_stocks, effective_date)
                    stocks = self.probe._analyze_stocks(stock_bundles, days[-1], rotation)
                    stocks = [self._asdict_stock(item) for item in stocks]
                elif hasattr(self.probe, "build_market_window"):
                    upstream = self.probe.build_market_window(date_str, sample_stocks=sample_stocks)
                    stocks = upstream.get("sample_stocks", [])
                return {
                    "date": date_str,
                    "effective_date": effective_date,
                    "rolled_to_trade_day": rolled,
                    "remote_host": getattr(self.probe, "remote_host", None),
                    "hot_plate_probe": hot_plate_probe,
                    "market_window_5d": [self.probe._day_to_dict(day) for day in days],
                    "rotation_analysis": rotation,
                    "emotion_cycle": emotion,
                    "sample_stocks": stocks,
                }
            if hasattr(self.probe, "build_market_window"):
                return self.probe.build_market_window(date_str, sample_stocks=sample_stocks)
        if hasattr(self.probe, "run"):
            return self.probe.run(date_str, write_snapshot=False)
        raise RuntimeError("Probe does not support market window building")

    def _cached_day_snapshot(self, date_str: str) -> Any:
        if date_str in self._day_snapshot_cache:
            return self._day_snapshot_cache[date_str]
        day = self.probe._build_market_day_snapshot(date_str)
        self._day_snapshot_cache[date_str] = day
        return day

    def _asdict_stock(self, stock: Any) -> Dict[str, Any]:
        if isinstance(stock, dict):
            return stock
        return {
            "stock_name": getattr(stock, "stock_name", ""),
            "code6": getattr(stock, "code6", ""),
            "primary_plate": getattr(stock, "primary_plate", ""),
            "related_themes": getattr(stock, "related_themes", []),
            "belongs_to_mainline": getattr(stock, "belongs_to_mainline", False),
            "role": getattr(stock, "role", ""),
            "phase": getattr(stock, "phase", ""),
            "shape_tags": getattr(stock, "shape_tags", []),
            "chip_profile": getattr(stock, "chip_profile", {}),
            "amount_profile": getattr(stock, "amount_profile", {}),
            "peer_comparison": getattr(stock, "peer_comparison", {}),
            "selection_reason": getattr(stock, "selection_reason", ""),
        }

    def _linked_families(self, record: Dict[str, Any]) -> List[str]:
        families = []
        for pattern in record.get("matched_patterns", []):
            for family in FAMILY_MAPPING.get(pattern, []):
                if family not in families:
                    families.append(family)
        return families

    def _build_action_interpretation(
        self, record: Dict[str, Any], market_report: Dict[str, Any], selected_stocks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        actions = record.get("actions", [])
        summary = "动作信号不足，偏观察。"
        if ("止损" in actions or "割肉" in actions) and any(item in actions for item in ["买入", "打板", "扫板", "半路", "加仓", "梭哈"]):
            summary = "这是一次纠错切换，止损旧票后转向更强分支。"
        elif "空仓" in actions:
            summary = "这是一次风险收缩，主动退出并放弃继续博弈。"
        elif "打板" in actions or "扫板" in actions:
            summary = "这是一次板上确认，围绕最强承接做高辨识度进攻。"
        elif "半路" in actions or "买入" in actions:
            summary = "这是一次盘中跟随，目标是先于板上最一致完成介入。"
        elif any(item in actions for item in ["加仓", "锁仓", "持仓", "梭哈"]):
            summary = "这是一次持仓强化，核心在于继续集中在已确认的强样本上。"

        reasoning = [
            f"当天市场处于{market_report['rotation_analysis']['stage']}，主线候选为{'、'.join(market_report['rotation_analysis']['daily_top_themes']) or '无'}。",
        ]
        if selected_stocks:
            top = selected_stocks[0]
            reasoning.append(
                f"主要标的 {top['stock_name']} 处于{top['phase']}，角色偏{top['role']}，形态上有{'、'.join(top['shape_tags'][:2]) or '无明显优势'}。"
            )
        if record.get("matched_patterns"):
            reasoning.append("动作模式命中 " + "、".join(record["matched_patterns"]) + "。")
        return {"summary": summary, "reasoning": reasoning}

    def _build_entry_logic(
        self, record: Dict[str, Any], market_report: Dict[str, Any], selected_stocks: List[Dict[str, Any]]
    ) -> List[str]:
        actions = record.get("actions", [])
        items = []
        if any(item in actions for item in ["打板", "扫板"]):
            items.append("只有在目标股具备最强承接和封板预期时才适合板上确认。")
        if "半路" in actions or ("买入" in actions and "打板" not in actions and "扫板" not in actions):
            items.append("买点应在盘中承接增强阶段，而不是等到最一致的板上位置。")
        if "低吸" in actions:
            items.append("低吸前提是分歧后不破结构，并且回流修复出现明确信号。")
        if ("止损" in actions or "割肉" in actions) and any(item in actions for item in ["买入", "打板", "扫板", "半路"]):
            items.append("先承认旧票错误，再切到更强方向，不做被动死扛。")
        if not items and selected_stocks:
            items.append(f"介入依赖 {selected_stocks[0]['stock_name']} 的角色和阶段确认。")
        return items

    def _build_hold_logic(
        self, record: Dict[str, Any], market_report: Dict[str, Any], selected_stocks: List[Dict[str, Any]]
    ) -> List[str]:
        actions = record.get("actions", [])
        items = []
        if any(item in actions for item in ["持仓", "加仓", "锁仓", "梭哈"]):
            items.append("继续持有的前提是主线未坏、个股承接未丢、情绪没有进入全面退潮。")
        if "加仓" in actions:
            items.append("加仓必须建立在更强确认之上，而不是均价摊平弱票。")
        if "锁仓" in actions:
            items.append("锁仓说明老师认为当前强势样本的趋势优势仍在，短线不需要频繁切换。")
        if "减仓" in actions:
            items.append("减仓通常意味着强度仍在，但需要先兑现一部分利润或降低集中风险。")
        if not items and selected_stocks:
            items.append(f"若继续持有，核心要看 {selected_stocks[0]['stock_name']} 是否还能维持{selected_stocks[0]['phase']}结构。")
        return items

    def _build_exit_logic(
        self, record: Dict[str, Any], market_report: Dict[str, Any], selected_stocks: List[Dict[str, Any]]
    ) -> List[str]:
        actions = record.get("actions", [])
        items = []
        if "空仓" in actions:
            items.append("退出后空仓，说明当天更重视风险规避而不是继续切换。")
        if "止盈" in actions:
            items.append("止盈通常对应冲高兑现或题材强度开始转弱。")
        if "止损" in actions or "割肉" in actions:
            items.append("止损说明旧标的不再符合预期，退出优先级高于继续等待。")
        if "卖出" in actions and "买入" not in actions and "打板" not in actions and "扫板" not in actions:
            items.append("单边卖出更像主动降低风险或阶段性兑现。")
        if not items:
            items.append("若当天没有明确退出动作，默认退出条件是主线转弱、承接丢失或高位失控。")
        return items

    def _build_risk_notes(self, record: Dict[str, Any], market_report: Dict[str, Any]) -> List[str]:
        notes = []
        stage = market_report["rotation_analysis"]["stage"]
        if "退潮" in stage:
            notes.append("市场处于退潮或弱修复阶段，所有追高动作风险都会放大。")
        if "轮动" in stage:
            notes.append("轮动环境下切换速度快，买点和兑现节奏都不能拖。")
        if market_report["emotion_cycle"].get("avg_negative_feedback_ratio", 0) >= 0.8:
            notes.append("近阶段负反馈偏高，炸板和次日低开的概率更大。")
        if "空仓" in record.get("actions", []):
            notes.append("空仓本身就是风险管理动作，不应被误读为错过机会。")
        return notes

    def _build_previous_day_change(
        self,
        record: Dict[str, Any],
        previous_record: Optional[Dict[str, Any]],
        market_report: Dict[str, Any],
    ) -> str:
        if not previous_record:
            return "这是当前实盘档案序列中的第一条可分析记录。"
        prev_actions = "、".join(previous_record.get("actions", [])) or "无明确动作"
        now_actions = "、".join(record.get("actions", [])) or "无明确动作"
        return (
            f"相较上一实盘日 {previous_record['publish_date']}，动作从“{prev_actions}”切到“{now_actions}”，"
            f"当前市场阶段为{market_report['rotation_analysis']['stage']}。"
        )

    def _previous_record(self, record: Dict[str, Any], all_records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not all_records:
            return None
        eligible = [item for item in all_records if item.get("publish_date") and item["publish_date"] < record["publish_date"]]
        if not eligible:
            return None
        return sorted(eligible, key=lambda item: item["publish_date"])[-1]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build daily live trading cases from 实盘.md")
    parser.add_argument("--teacher", default="niepan")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    builder = LiveCaseBuilder(teacher=args.teacher)
    cases = builder.build_and_write_all_live_cases()
    print(
        json.dumps(
            {
                "teacher": args.teacher,
                "case_count": len(cases),
                "first_case": str(builder.archive_root / f"live_case_{cases[0]['publish_date']}_{args.teacher}.json") if cases else None,
                "last_case": str(builder.archive_root / f"live_case_{cases[-1]['publish_date']}_{args.teacher}.json") if cases else None,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
