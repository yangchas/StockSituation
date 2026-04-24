import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"


class TeacherComparisonBuilder:
    def __init__(self, archive_root: Optional[Path] = None) -> None:
        self.archive_root = archive_root or ARCHIVE_ROOT

    def build(self, left: str = "niepan", right: str = "jiucai") -> Dict[str, Path]:
        left_catalog = self._load_json(self.archive_root / f"strategy_catalog_{left}.json")
        right_catalog = self._load_json(self.archive_root / f"strategy_catalog_{right}.json")
        archive_index = self._load_json(self.archive_root / "teacher_archive_index.json")

        archive_index["teachers"] = sorted(set(archive_index.get("teachers", []) + [left, right]))
        archive_index.setdefault("teacher_to_catalog", {})[left] = f"strategy_catalog_{left}.json"
        archive_index.setdefault("teacher_to_catalog", {})[right] = f"strategy_catalog_{right}.json"
        archive_index.setdefault("teacher_to_rulebook", {}).setdefault(right, None)
        archive_index.setdefault("teacher_to_integration_plan", {}).setdefault(right, None)

        comparison = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "teachers": [left, right],
            "left_family_count": len(left_catalog.get("families", [])),
            "right_family_count": len(right_catalog.get("families", [])),
            "left_case_count": len(left_catalog.get("cases", [])),
            "right_case_count": len(right_catalog.get("cases", [])),
            "style_summary": {
                left: "更强调家族联动、总龙层级和高位边界，动作上更偏确认、切换和集中持仓。",
                right: "更强调市场环境、量化影响、轮动修复和主线承接，对冰点、纠错和高低切表述更密集。",
            },
            "rule_difference": {
                left: ["midcycle_resonance_family", "dragon_second_stage_family", "high_level_emotion_family"],
                right: ["jiucai_rotation_repair_family", "jiucai_icepoint_rebound_family"],
            },
            "reuse_mapping": {
                "jiucai_trend_mainline_family": ["trend_extension_family", "midcycle_resonance_family"],
                "jiucai_rotation_repair_family": ["rotation_low_suction_family"],
                "jiucai_icepoint_rebound_family": ["high_level_emotion_family", "rotation_low_suction_family"],
                "jiucai_framework_meta_family": ["framework_meta_family"],
            },
            "integration_note": "当前仅完成离线规则、案例和跨老师映射归档，尚未接入系统运行时信号层。",
        }

        outputs = {
            "archive_index": self.archive_root / "teacher_archive_index.json",
            "comparison": self.archive_root / f"teacher_comparison_{left}_vs_{right}.json",
        }
        outputs["archive_index"].write_text(json.dumps(archive_index, ensure_ascii=False, indent=2), encoding="utf-8")
        outputs["comparison"].write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")
        return outputs

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build cross-teacher archive comparison assets.")
    parser.add_argument("--left", default="niepan")
    parser.add_argument("--right", default="jiucai")
    args = parser.parse_args()
    outputs = TeacherComparisonBuilder().build(left=args.left, right=args.right)
    print(json.dumps({key: str(value) for key, value in outputs.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
