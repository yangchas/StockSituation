import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


class UnifiedTeacherArchiveBuilder:
    def __init__(self, archive_root: Optional[Path] = None) -> None:
        self.archive_root = archive_root or ARCHIVE_ROOT

    def build(self) -> Dict[str, Path]:
        niepan_rulebook = _load_json(self.archive_root / "teacher_rulebook_niepan.json")
        niepan_integration = _load_json(self.archive_root / "teacher_integration_plan_niepan.json")
        teacher_index = _load_json(self.archive_root / "teacher_archive_index.json")
        comparison = _load_json(self.archive_root / "teacher_comparison_niepan_vs_jiucai.json")

        jiucai_families = self._load_jiucai_families()
        misc_doc = self._load_misc_doc("niepan")

        unified_rulebook = self._build_unified_rulebook(niepan_rulebook, jiucai_families, misc_doc)
        unified_mapping = self._build_unified_mapping(niepan_integration, jiucai_families, comparison)
        unified_guide = self._build_unified_guide(unified_mapping, unified_rulebook)
        teacher_index = self._update_teacher_archive_index(teacher_index)

        outputs = {
            "rulebook": self.archive_root / "teacher_rulebook_unified.json",
            "mapping": self.archive_root / "teacher_rule_mapping_unified.json",
            "guide": self.archive_root / "teacher_integration_guide_unified.md",
            "index": self.archive_root / "teacher_archive_index.json",
        }
        outputs["rulebook"].write_text(json.dumps(unified_rulebook, ensure_ascii=False, indent=2), encoding="utf-8")
        outputs["mapping"].write_text(json.dumps(unified_mapping, ensure_ascii=False, indent=2), encoding="utf-8")
        outputs["guide"].write_text(unified_guide, encoding="utf-8")
        outputs["index"].write_text(json.dumps(teacher_index, ensure_ascii=False, indent=2), encoding="utf-8")
        return outputs

    def _load_jiucai_families(self) -> Dict[str, Dict[str, Any]]:
        families = {}
        for path in sorted(self.archive_root.glob("strategy_family_jiucai_*.json")):
            data = _load_json(path)
            families[data["strategy_key"]] = data
        return families

    def _load_misc_doc(self, teacher: str) -> Dict[str, Any]:
        matches = sorted(self.archive_root.glob(f"strategy_misc_*_{teacher}.json"))
        return _load_json(matches[0]) if matches else {}

    def _build_unified_rulebook(
        self,
        niepan_rulebook: Dict[str, Any],
        jiucai_families: Dict[str, Dict[str, Any]],
        misc_doc: Dict[str, Any],
    ) -> Dict[str, Any]:
        market_rules = []
        for item in niepan_rulebook.get("market_rules", []):
            entry = dict(item)
            entry["source_teachers"] = ["niepan", "jiucai"]
            entry["teacher_variants"] = {
                "niepan": "更强调家族联动、总龙层级和高位边界的交易结构。",
                "jiucai": "更强调市场环境、量化影响、轮动修复和冰点纠错的解释框架。",
            }
            market_rules.append(entry)

        strategy_rules = []
        niepan_strategy_map = {item["rule_key"]: item for item in niepan_rulebook.get("strategy_rules", [])}
        jiucai_map = {
            "trend_extension": jiucai_families.get("jiucai_trend_mainline_family"),
            "rotation_repair": jiucai_families.get("jiucai_rotation_repair_family"),
            "icepoint_rebound": jiucai_families.get("jiucai_icepoint_rebound_family"),
            "framework_meta": jiucai_families.get("jiucai_framework_meta_family"),
        }

        strategy_order = [
            "midcycle_resonance",
            "trend_extension",
            "rotation_repair",
            "icepoint_rebound",
            "dragon_second_stage",
            "framework_meta",
        ]
        for key in strategy_order:
            if key in ("rotation_repair", "icepoint_rebound"):
                family = jiucai_map[key]
                strategy_rules.append(self._rule_from_family(key, family, ["jiucai"]))
                continue

            if key == "trend_extension":
                base = dict(niepan_strategy_map.get("trend_extension", {}))
                family = jiucai_map["trend_extension"]
                base["source_teachers"] = ["niepan", "jiucai"]
                base["teacher_variants"] = {
                    "niepan": "偏趋势主升延续、板块突破与中军承接。",
                    "jiucai": "偏主线趋势、容量承接与主线确认。",
                }
                base["reference_cases"] = sorted(
                    set(base.get("reference_cases", []) + family.get("example_dates", []))
                )
                strategy_rules.append(base)
                continue

            if key == "framework_meta":
                base = dict(niepan_strategy_map.get("framework_meta", {}))
                family = jiucai_map["framework_meta"]
                base["source_teachers"] = ["niepan", "jiucai"]
                base["teacher_variants"] = {
                    "niepan": "偏交易框架、跟随最强和模式进化。",
                    "jiucai": "偏市场认知、量化冲击、情绪和指数结构。",
                }
                base["reference_cases"] = sorted(
                    set(base.get("reference_cases", []) + family.get("example_dates", []))
                )
                strategy_rules.append(base)
                continue

            base = dict(niepan_strategy_map.get(key, {}))
            if not base:
                continue
            base["source_teachers"] = ["niepan"]
            base["teacher_variants"] = {
                "niepan": "这是 niepan 体系里的核心可执行策略。",
            }
            strategy_rules.append(base)

        execution_rules = []
        live_pattern_mapping = misc_doc.get("family_mapping", {})
        for pattern in misc_doc.get("patterns", []):
            execution_rules.append(
                {
                    "rule_key": str(pattern.get("pattern_key", "")).replace("live_", ""),
                    "rule_name": pattern.get("pattern_name"),
                    "market_prerequisites": pattern.get("market_context_features", []),
                    "theme_prerequisites": pattern.get("stock_selection_features", []),
                    "stock_prerequisites": pattern.get("entry_features", []),
                    "amount_prerequisites": [],
                    "chip_prerequisites": [],
                    "entry_triggers": pattern.get("entry_features", []),
                    "hold_triggers": [pattern.get("position_style", "")] if pattern.get("position_style") else [],
                    "exit_triggers": pattern.get("exit_features", []),
                    "veto_conditions": pattern.get("risk_controls", []),
                    "source_teachers": ["niepan"],
                    "reference_cases": [item.get("publish_date") for item in pattern.get("evidence_records", [])[:8]],
                    "teacher_variants": {
                        "niepan": "来自实盘动作提取，强调确认、切换、集中和风控。",
                    },
                    "linked_strategy_families": live_pattern_mapping.get(pattern.get("pattern_key", ""), []),
                }
            )

        veto_rules = []
        for item in niepan_rulebook.get("veto_rules", []):
            entry = dict(item)
            entry["source_teachers"] = ["niepan", "jiucai"]
            entry["teacher_variants"] = {
                "niepan": "偏边界和不追伪主线。",
                "jiucai": "偏错杀纠偏、量化干扰和不把弱修复当新周期。",
            }
            veto_rules.append(entry)

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "teachers": ["niepan", "jiucai"],
            "market_rules": market_rules,
            "strategy_rules": strategy_rules,
            "execution_rules": execution_rules,
            "veto_rules": veto_rules,
        }

    def _rule_from_family(self, rule_key: str, family: Optional[Dict[str, Any]], source_teachers: List[str]) -> Dict[str, Any]:
        family = family or {}
        return {
            "rule_key": rule_key,
            "rule_name": family.get("strategy_name", rule_key),
            "market_prerequisites": family.get("market_features", {}),
            "theme_prerequisites": family.get("theme_features", {}),
            "stock_prerequisites": family.get("stock_features", {}),
            "amount_prerequisites": family.get("amount_features", {}),
            "chip_prerequisites": family.get("chip_features", {}),
            "entry_triggers": family.get("entry_conditions", []),
            "hold_triggers": family.get("execution_hints", []),
            "exit_triggers": family.get("exit_conditions", []),
            "veto_conditions": family.get("veto_conditions", []),
            "source_teachers": source_teachers,
            "reference_cases": family.get("example_dates", []),
            "teacher_variants": {
                teacher: family.get("core_thesis", "") for teacher in source_teachers
            },
        }

    def _build_unified_mapping(
        self,
        niepan_integration: Dict[str, Any],
        jiucai_families: Dict[str, Dict[str, Any]],
        comparison: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "existing_system_inputs": niepan_integration.get("existing_system_inputs", []),
            "derived_labels": [
                "family_resonance_tag",
                "dragon_hierarchy_tag",
                "rotation_repair_tag",
                "icepoint_rebound_tag",
                "switch_and_cut_signal",
                "concentration_hold_signal",
                "risk_off_signal",
                "high_level_repair_tag",
            ],
            "teacher_outputs": [
                "market:teacher_rules:{date}",
                "market:teacher_setups:{date}",
                "market:teacher_forbidden:{date}",
                "rank:teacher_stock:{date}",
                "rank:teacher_stock:details:{date}",
            ],
            "redis_key_mapping": niepan_integration.get("redis_key_mapping", {}),
            "engine_hook_points": niepan_integration.get("engine_hook_points", []),
            "non_intrusive_mode": {
                "enabled": True,
                "description": "只作为未来旁路规则层说明，不改主 stock rank、不改 execution_policy、不新增运行时 Redis 写入。",
            },
            "deferred_notes": [
                "统一规则总表作为后续接系统的主入口。",
                "framework_meta 只做解释层，不直接生成交易信号。",
                "jiucai 的主线承接、轮动修复、冰点反抽先以离线规则形式保留，后续再映射到运行时标签。",
                comparison.get("integration_note", ""),
            ],
            "teacher_family_reuse": comparison.get("reuse_mapping", {}),
            "teacher_family_catalog": {
                "jiucai": sorted(jiucai_families.keys()),
                "niepan": niepan_integration.get("covered_strategy_families", []),
            },
        }

    def _build_unified_guide(self, mapping: Dict[str, Any], rulebook: Dict[str, Any]) -> str:
        lines = ["# Teacher Integration Guide Unified", ""]
        lines.append("## 目标")
        lines.append("- 本文件只做后续接入说明，不对应当前系统代码修改。")
        lines.append("- 后续接系统时，统一读取 `teacher_rulebook_unified.json` 和 `teacher_rule_mapping_unified.json`。")
        lines.append("")
        lines.append("## 读取顺序")
        lines.append("- 先读统一规则总表，按市场层、策略层、动作层加载规则。")
        lines.append("- 再读统一映射表，确定现有系统输入、未来派生标签和推荐挂点。")
        lines.append("- 单老师 rulebook / comparison 仅用于追溯和差异解释。")
        lines.append("")
        lines.append("## 推荐未来系统输入")
        for item in mapping.get("existing_system_inputs", []):
            lines.append(f"- {item}")
        lines.append("")
        lines.append("## 推荐未来派生标签")
        for item in mapping.get("derived_labels", []):
            lines.append(f"- {item}")
        lines.append("")
        lines.append("## 推荐未来输出 key")
        for item in mapping.get("teacher_outputs", []):
            lines.append(f"- {item}")
        lines.append("")
        lines.append("## 推荐未来挂点")
        for item in mapping.get("engine_hook_points", []):
            lines.append(f"- {item}")
        lines.append("")
        lines.append("## 第一版明确不要做")
        lines.append("- 不改 `web/market_edge_engine.py` 当前主逻辑。")
        lines.append("- 不覆盖 `rank:stock` 和 `market:execution_policy`。")
        lines.append("- 不新增前端 API。")
        lines.append("- 不新增运行时 Redis 写入。")
        lines.append("")
        lines.append("## 统一规则覆盖")
        lines.append(f"- 市场层规则数: {len(rulebook.get('market_rules', []))}")
        lines.append(f"- 策略层规则数: {len(rulebook.get('strategy_rules', []))}")
        lines.append(f"- 动作层规则数: {len(rulebook.get('execution_rules', []))}")
        return "\n".join(lines) + "\n"

    def _update_teacher_archive_index(self, teacher_index: Dict[str, Any]) -> Dict[str, Any]:
        teacher_index["unified_rulebook"] = "teacher_rulebook_unified.json"
        teacher_index["unified_rule_mapping"] = "teacher_rule_mapping_unified.json"
        teacher_index["unified_integration_guide"] = "teacher_integration_guide_unified.md"
        teacher_index.setdefault("teacher_to_comparison", {})
        teacher_index["teacher_to_comparison"]["niepan_vs_jiucai"] = "teacher_comparison_niepan_vs_jiucai.json"
        return teacher_index


def main() -> None:
    parser = argparse.ArgumentParser(description="Build unified offline teacher rule assets.")
    parser.parse_args()
    outputs = UnifiedTeacherArchiveBuilder().build()
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
