import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"


class TeacherArchiveSealer:
    def __init__(self, teacher: str = "niepan", archive_root: Optional[Path] = None) -> None:
        self.teacher = teacher
        self.archive_root = archive_root or ARCHIVE_ROOT
        self.archive_root.mkdir(parents=True, exist_ok=True)

    def seal(self) -> Dict[str, Path]:
        catalog = self._load_json(self.archive_root / f"strategy_catalog_{self.teacher}.json")
        index = self._load_json(self.archive_root / f"strategy_index_{self.teacher}.json")
        family_docs = self._load_family_docs()
        misc_doc = self._load_misc_doc()

        rulebook = self._build_rulebook(index, family_docs, misc_doc)
        integration_plan = self._build_integration_plan(family_docs, misc_doc)
        archive_index = self._merge_teacher_archive_index()
        archive_index["teachers"] = sorted(set(archive_index.get("teachers", []) + [self.teacher]))
        archive_index.setdefault("teacher_to_catalog", {})[self.teacher] = f"strategy_catalog_{self.teacher}.json"
        archive_index.setdefault("teacher_to_rulebook", {})[self.teacher] = f"teacher_rulebook_{self.teacher}.json"
        archive_index.setdefault("teacher_to_integration_plan", {})[
            self.teacher
        ] = f"teacher_integration_plan_{self.teacher}.json"

        outputs = {
            "rulebook": self.archive_root / f"teacher_rulebook_{self.teacher}.json",
            "integration_plan": self.archive_root / f"teacher_integration_plan_{self.teacher}.json",
            "archive_index": self.archive_root / "teacher_archive_index.json",
        }
        outputs["rulebook"].write_text(json.dumps(rulebook, ensure_ascii=False, indent=2), encoding="utf-8")
        outputs["integration_plan"].write_text(
            json.dumps(integration_plan, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        outputs["archive_index"].write_text(json.dumps(archive_index, ensure_ascii=False, indent=2), encoding="utf-8")
        return outputs

    def _load_family_docs(self) -> List[Dict[str, Any]]:
        docs = []
        explicit = sorted(self.archive_root.glob(f"strategy_family_*_{self.teacher}.json"))
        if explicit:
            for path in explicit:
                docs.append(self._load_json(path))
            return docs

        for path in sorted(self.archive_root.glob("strategy_family_*.json")):
            if f"_{self.teacher}.json" in path.name:
                continue
            data = self._load_json(path)
            strategy_id = str(data.get("strategy_id", ""))
            strategy_key = str(data.get("strategy_key", ""))
            if self.teacher in strategy_id or not strategy_key.startswith("jiucai_"):
                docs.append(data)
        return docs

    def _load_misc_doc(self) -> Dict[str, Any]:
        matches = sorted(self.archive_root.glob(f"strategy_misc_*_{self.teacher}.json"))
        return self._load_json(matches[0]) if matches else {}

    def _build_rulebook(
        self,
        index: Dict[str, Any],
        family_docs: List[Dict[str, Any]],
        misc_doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        market_rules = [
            {
                "rule_key": "rotation_market",
                "rule_name": "题材轮动期",
                "description": "最热方向频繁切换，优先做分歧回流、弱转强和低位补位，不追一致高潮。",
                "reference_families": ["rotation_low_suction_family", "high_level_emotion_family"],
            },
            {
                "rule_key": "mainline_focus_market",
                "rule_name": "主线聚焦期",
                "description": "主线板块具备持续扩散与辨识度，优先围绕家族联动和核心中军做趋势确认。",
                "reference_families": ["midcycle_resonance_family", "trend_extension_family"],
            },
            {
                "rule_key": "repair_reflow_market",
                "rule_name": "修复回流期",
                "description": "前期强方向经历分歧后回流，确认承接与量能修复优先于单纯抢反弹。",
                "reference_families": ["rotation_low_suction_family", "dragon_second_stage_family"],
            },
            {
                "rule_key": "high_level_emotion_market",
                "rule_name": "高位情绪期",
                "description": "市场由空间票、高位抱团或断板反包主导，交易重点是边界感与承接验证。",
                "reference_families": ["high_level_emotion_family", "dragon_second_stage_family"],
            },
            {
                "rule_key": "retreat_filter_market",
                "rule_name": "退潮甄别期",
                "description": "高位负反馈密集时允许空仓等待，风控优先于进攻。",
                "reference_families": ["high_level_emotion_family"],
            },
        ]

        strategy_rules = []
        family_mapping = {}
        for doc in family_docs:
            strategy_key = str(doc.get("strategy_key", "")).replace("_family", "")
            strategy_rules.append(
                {
                    "rule_key": strategy_key,
                    "rule_name": doc.get("strategy_name", strategy_key),
                    "market_prerequisites": doc.get("market_features", {}),
                    "theme_prerequisites": doc.get("theme_features", {}),
                    "stock_prerequisites": doc.get("stock_features", {}),
                    "amount_prerequisites": doc.get("amount_features", {}),
                    "chip_prerequisites": doc.get("chip_features", {}),
                    "entry_triggers": doc.get("entry_conditions", []),
                    "hold_triggers": doc.get("execution_hints", []),
                    "exit_triggers": doc.get("exit_conditions", []),
                    "veto_conditions": doc.get("veto_conditions", []),
                    "linked_existing_features": self._linked_existing_features(strategy_key),
                    "reference_cases": doc.get("example_dates", []),
                }
            )
            family_mapping[doc.get("strategy_key", strategy_key)] = {
                "strategy_id": doc.get("strategy_id"),
                "example_dates": doc.get("example_dates", []),
                "setup_type": doc.get("setup_type"),
            }

        execution_rules = []
        live_pattern_mapping = misc_doc.get("family_mapping", {})
        for pattern in misc_doc.get("patterns", []):
            execution_rules.append(
                {
                    "rule_key": str(pattern.get("pattern_key", "")).replace("live_", ""),
                    "rule_name": pattern.get("pattern_name"),
                    "action_keywords": pattern.get("action_keywords", []),
                    "market_prerequisites": pattern.get("market_context_features", []),
                    "theme_prerequisites": pattern.get("stock_selection_features", []),
                    "stock_prerequisites": pattern.get("entry_features", []),
                    "amount_prerequisites": [],
                    "chip_prerequisites": [],
                    "entry_triggers": pattern.get("entry_features", []),
                    "hold_triggers": [pattern.get("position_style", "")] if pattern.get("position_style") else [],
                    "exit_triggers": pattern.get("exit_features", []),
                    "veto_conditions": pattern.get("risk_controls", []),
                    "linked_existing_features": live_pattern_mapping.get(pattern.get("pattern_key", ""), []),
                    "reference_cases": [item.get("publish_date") for item in pattern.get("evidence_records", [])[:8]],
                }
            )

        veto_rules = [
            {"rule_key": "do_not_chase_false_mainline", "rule_name": "不追伪主线", "description": "题材没有持续性时默认降级进攻。"},
            {"rule_key": "do_not_hold_failed_repair", "rule_name": "修复失败先退", "description": "分歧修复若失去量能与承接，优先退出。"},
            {"rule_key": "do_not_fight_full_retreat", "rule_name": "全面退潮不硬接", "description": "高位负反馈密集时允许空仓等待。"},
        ]

        return {
            "teacher": self.teacher,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "market_rules": market_rules,
            "strategy_rules": strategy_rules,
            "execution_rules": execution_rules,
            "veto_rules": veto_rules,
            "family_mapping": family_mapping,
            "live_pattern_mapping": live_pattern_mapping,
            "source_case_count": len(index.get("cases", [])),
        }

    def _build_integration_plan(
        self,
        family_docs: List[Dict[str, Any]],
        misc_doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        family_keys = [doc.get("strategy_key") for doc in family_docs]
        execution_keys = [pattern.get("pattern_key") for pattern in misc_doc.get("patterns", [])]
        return {
            "teacher": self.teacher,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "existing_system_inputs": [
                "market:sentiment:{date}",
                "market:emotion_phase:{date}",
                "market:fear_greed:{date}",
                "market:resonance:{date}",
                "rank:plate_spread:{date}",
                "rank:theme:{date}",
                "market:plate_phase_map:{date}",
                "rank:stock:{date}",
                "rank:stock:details:{date}",
                "market:execution_policy:{date}",
                "market:plan:open_verify:{date}",
            ],
            "missing_labels": [
                "family_resonance_tag",
                "dragon_hierarchy_tag",
                "switch_and_cut_signal",
                "concentration_hold_signal",
                "risk_off_signal",
                "high_level_repair_tag",
                "ice_point_reflow_tag",
            ],
            "proposed_teacher_outputs": [
                "market:teacher_rules:{date}",
                "market:teacher_setups:{date}",
                "market:teacher_forbidden:{date}",
                "rank:teacher_stock:{date}",
                "rank:teacher_stock:details:{date}",
            ],
            "redis_key_mapping": {
                "teacher_rules": "market:teacher_rules:{date}",
                "teacher_setups": "market:teacher_setups:{date}",
                "teacher_forbidden": "market:teacher_forbidden:{date}",
                "teacher_stock_rank": "rank:teacher_stock:{date}",
                "teacher_stock_details": "rank:teacher_stock:details:{date}",
            },
            "engine_hook_points": [
                "After calculate_stock_rank() to evaluate teacher setups.",
                "After calculate_execution_policy() to merge teacher veto and risk-off actions.",
                "Before build_open_verify_plan() to enrich plan cards with teacher-style setup interpretation.",
            ],
            "deferred_implementation_notes": [
                "Keep teacher rule evaluation as a sidecar layer; do not overwrite existing execution_policy.",
                "Use long-cycle kline, chip, and amount features only where existing Redis outputs are insufficient.",
                "Treat framework_meta_family as explanation-only; do not emit executable trading signals from it.",
            ],
            "covered_strategy_families": family_keys,
            "covered_execution_patterns": execution_keys,
        }

    def _merge_teacher_archive_index(self) -> Dict[str, Any]:
        path = self.archive_root / "teacher_archive_index.json"
        if path.exists():
            return self._load_json(path)
        return {
            "teachers": [],
            "teacher_to_catalog": {},
            "teacher_to_rulebook": {},
            "teacher_to_integration_plan": {},
        }

    def _linked_existing_features(self, strategy_key: str) -> List[str]:
        mapping = {
            "midcycle_resonance": ["rank:theme", "rank:plate_spread", "rank:stock:details"],
            "dragon_second_stage": ["rank:stock", "rank:stock:details", "market:emotion_phase"],
            "trend_extension": ["rank:plate_profile", "rank:plate_spread", "rank:stock:details"],
            "rotation_low_suction": ["market:strategy_tags", "market:sentiment", "rank:stock:details"],
            "high_level_emotion": ["market:emotion_phase", "market:fear_greed", "market:resonance"],
            "framework_meta": [],
        }
        return mapping.get(strategy_key, [])

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Seal teacher archive assets for future integration.")
    parser.add_argument("--teacher", default="niepan")
    args = parser.parse_args()
    outputs = TeacherArchiveSealer(teacher=args.teacher).seal()
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
