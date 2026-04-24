import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web.services.f10_service import F10DataService


logger = logging.getLogger(__name__)

ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"
ARTICLE_ROOT = ROOT_DIR / "Article"

PATTERN_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "live_limit_up_boarding": {
        "pattern_name": "打板与扫板确认",
        "action_keywords": ["打板", "扫板", "板上确认"],
        "core_intent": "在强势样本进入板上确认阶段时，直接参与最强承接。",
        "market_context_features": ["情绪仍有进攻空间", "高辨识度样本具备封板预期"],
        "stock_selection_features": ["强势龙头或高辨识度前排", "封板质量和承接优先"],
        "entry_features": ["板上确认", "扫板跟随", "不做弱板犹豫单"],
        "exit_features": ["炸板不修复离场", "次日不及预期减仓", "冲高不板兑现"],
        "risk_controls": ["避免退潮期乱打板", "避免纯情绪弱跟风板"],
        "position_style": "进攻型",
    },
    "live_halfway_momentum": {
        "pattern_name": "半路与趋势跟随",
        "action_keywords": ["半路", "买入", "点火"],
        "core_intent": "在趋势强化或盘中承接确认时提前跟随，不等到板上最一致。",
        "market_context_features": ["趋势方向仍有延续空间", "盘中承接优于板上拥挤"],
        "stock_selection_features": ["趋势强化样本", "有辨识度的主线或容量股"],
        "entry_features": ["盘中承接增强", "趋势延续而非末端加速", "买点先于最高潮"],
        "exit_features": ["次日不延续减仓", "冲高回落兑现", "失去趋势重心离场"],
        "risk_controls": ["避免半路追末端", "确认承接而不是只看冲高"],
        "position_style": "进攻型",
    },
    "live_low_suction_rotation": {
        "pattern_name": "低吸与回流修复",
        "action_keywords": ["低吸", "回流", "修复", "承接"],
        "core_intent": "在轮动和分歧环境中，等回踩和回流修复后的性价比买点。",
        "market_context_features": ["市场处于轮动试错", "一致高潮后的分歧修复窗口"],
        "stock_selection_features": ["分歧后仍有修复预期的样本", "回踩不破结构的个股"],
        "entry_features": ["低吸承接", "分歧后回流", "调整后再确认"],
        "exit_features": ["修复失败离场", "回流只维持短暂半日兑现", "跌破回踩低点止损"],
        "risk_controls": ["不接全面退潮飞刀", "不把弱修复当新周期"],
        "position_style": "均衡型",
    },
    "live_switch_and_cut": {
        "pattern_name": "止损切换与换强",
        "action_keywords": ["止损", "割肉", "卖出", "切换", "换强"],
        "core_intent": "快速承认错误，从弱票切换到更强方向，减少沉没成本。",
        "market_context_features": ["轮动快于持仓容忍度", "错误持仓需要快速纠偏"],
        "stock_selection_features": ["新切入标的必须强于旧票", "切换目标需有明确承接或辨识度"],
        "entry_features": ["止损旧票后切新票", "弱转强切换", "不恋战旧方向"],
        "exit_features": ["新票不及预期继续止损", "切换后未强化则撤退"],
        "risk_controls": ["防止连续乱切", "先确认新方向质量再切换"],
        "position_style": "纠错型",
    },
    "live_concentration_and_hold": {
        "pattern_name": "集中仓位与持仓管理",
        "action_keywords": ["梭哈", "加仓", "减仓", "锁仓", "持仓"],
        "core_intent": "围绕高确定性样本做集中持仓和仓位管理，而不是频繁切换。",
        "market_context_features": ["已经识别到高确定性样本", "愿意承担更高集中度"],
        "stock_selection_features": ["高辨识度核心股", "已有持仓优势的强样本"],
        "entry_features": ["加仓强化", "锁仓延续", "梭哈押注最强"],
        "exit_features": ["减仓落袋", "锁仓失败转卖出", "集中仓位不再有优势时退出"],
        "risk_controls": ["避免错误梭哈", "加仓必须建立在更强确认之上"],
        "position_style": "集中型",
    },
    "live_risk_off_exit": {
        "pattern_name": "风险规避与离场",
        "action_keywords": ["空仓", "止盈", "卖出", "离场"],
        "core_intent": "当环境不适合继续博弈时，主动收缩风险并兑现利润。",
        "market_context_features": ["高位风险放大", "环境不支持继续硬做"],
        "stock_selection_features": ["已有盈利样本优先兑现", "弱样本直接离场"],
        "entry_features": ["不再开新仓", "以降低风险为主"],
        "exit_features": ["止盈离场", "卖出后空仓", "缩仓等待下个机会"],
        "risk_controls": ["避免在坏环境里强行出手", "把空仓当成有效策略"],
        "position_style": "防守型",
    },
}

FAMILY_MAPPING = {
    "live_limit_up_boarding": ["high_level_emotion_family"],
    "live_halfway_momentum": ["trend_extension_family", "dragon_second_stage_family"],
    "live_low_suction_rotation": ["rotation_low_suction_family"],
    "live_switch_and_cut": ["high_level_emotion_family", "rotation_low_suction_family", "dragon_second_stage_family"],
    "live_concentration_and_hold": ["trend_extension_family", "dragon_second_stage_family"],
    "live_risk_off_exit": ["high_level_emotion_family"],
}


class LiveStrategyExtractor:
    def __init__(
        self,
        teacher: str = "niepan",
        article_root: Optional[Path] = None,
        archive_root: Optional[Path] = None,
        f10_service: Optional[F10DataService] = None,
    ) -> None:
        self.teacher = teacher
        self.article_root = article_root or ARTICLE_ROOT
        self.archive_root = archive_root or ARCHIVE_ROOT
        self.archive_root.mkdir(parents=True, exist_ok=True)
        self.f10 = f10_service or F10DataService()
        self._stock_names: Optional[List[str]] = None

    def extract(self) -> Dict[str, Any]:
        text = self._load_source_text()
        records = self.parse_records(text)
        classified = [self.classify_record(record) for record in records]
        patterns = self.aggregate_patterns(classified)
        archive = {
            "source_file": str((self.article_root / self.teacher / "实盘.md").resolve()),
            "teacher": self.teacher,
            "record_count": len(classified),
            "patterns": patterns,
            "family_mapping": FAMILY_MAPPING,
            "notes": [
                "本轮只做实盘动作模式提取，不嵌入系统执行。",
                "模式按动作语义聚类，不等同于单一交易日复盘。",
            ],
        }
        return archive

    def write_archive(self, archive: Dict[str, Any]) -> List[Path]:
        json_path = self.archive_root / f"strategy_misc_实盘_{self.teacher}.json"
        md_path = self.archive_root / f"strategy_misc_实盘_{self.teacher}.md"
        json_path.write_text(json.dumps(archive, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self.build_markdown(archive), encoding="utf-8")
        self.update_catalog_reference(json_path.name)
        return [json_path, md_path]

    def parse_records(self, text: str) -> List[Dict[str, Any]]:
        lines = [line.strip() for line in text.splitlines()]
        records: List[Dict[str, Any]] = []
        i = 0
        while i < len(lines):
            line = self._clean_html(lines[i])
            title_match = re.search(r"实盘第?\s*(\d+)天[:：,，]?\s*(.+)", line)
            if not title_match:
                bare_match = re.search(r"实盘\s*(\d+)天[:：,，]?\s*(.+)", line)
                title_match = bare_match
            if not title_match:
                i += 1
                continue
            live_day = int(title_match.group(1))
            title = title_match.group(2).strip()
            raw_parts = [line]
            publish_date = ""
            for j in range(i + 1, min(i + 6, len(lines))):
                next_line = self._clean_html(lines[j])
                raw_parts.append(next_line)
                date_match = re.search(r"(20\d{2}-\d{2}-\d{2})$", next_line)
                if date_match:
                    publish_date = date_match.group(1)
                    break
            record = {
                "live_day": live_day,
                "publish_date": publish_date,
                "title": self._normalize_title(title),
                "raw_text": " ".join(part for part in raw_parts if part),
            }
            records.append(record)
            i += 1
        return records

    def classify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        title = record["title"]
        actions = self.extract_actions(title)
        stocks = self.extract_stocks(title)
        matched_patterns = self.match_patterns(actions)
        primary_pattern = self.pick_primary_pattern(actions, matched_patterns)
        result = dict(record)
        result["actions"] = actions
        result["stocks"] = stocks
        result["matched_patterns"] = matched_patterns
        result["primary_pattern"] = primary_pattern
        return result

    def aggregate_patterns(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {key: [] for key in PATTERN_DEFINITIONS}
        for record in records:
            for pattern in record["matched_patterns"]:
                grouped.setdefault(pattern, []).append(record)
        patterns: List[Dict[str, Any]] = []
        for key, definition in PATTERN_DEFINITIONS.items():
            evidence = grouped.get(key, [])
            if not evidence:
                continue
            patterns.append(
                {
                    "pattern_key": key,
                    "pattern_name": definition["pattern_name"],
                    "action_keywords": definition["action_keywords"],
                    "core_intent": definition["core_intent"],
                    "market_context_features": definition["market_context_features"],
                    "stock_selection_features": definition["stock_selection_features"],
                    "entry_features": definition["entry_features"],
                    "exit_features": definition["exit_features"],
                    "risk_controls": definition["risk_controls"],
                    "position_style": definition["position_style"],
                    "record_count": len(evidence),
                    "representative_titles": [item["title"] for item in evidence[:8]],
                    "evidence_records": [
                        {
                            "live_day": item["live_day"],
                            "publish_date": item["publish_date"],
                            "title": item["title"],
                            "actions": item["actions"],
                            "stocks": item["stocks"],
                            "primary_pattern": item["primary_pattern"],
                        }
                        for item in evidence[:25]
                    ],
                }
            )
        patterns.sort(key=lambda item: (-item["record_count"], item["pattern_key"]))
        return patterns

    def build_markdown(self, archive: Dict[str, Any]) -> str:
        lines = [f"# Strategy Misc 实盘 ({archive['teacher']})", ""]
        lines.append("## 概览")
        lines.append(f"- 来源文件: {archive['source_file']}")
        lines.append(f"- 解析记录数: {archive['record_count']}")
        lines.append("")
        lines.append("## 模式清单")
        for pattern in archive["patterns"]:
            lines.append(
                f"- {pattern['pattern_key']} | 次数 {pattern['record_count']} | 代表标题: {'；'.join(pattern['representative_titles'][:3])}"
            )
        lines.append("")
        for pattern in archive["patterns"]:
            lines.append(f"## {pattern['pattern_name']}")
            lines.append(f"- 模式键: {pattern['pattern_key']}")
            lines.append(f"- 核心意图: {pattern['core_intent']}")
            lines.append(f"- 动作词: {'、'.join(pattern['action_keywords'])}")
            lines.append(f"- 市场环境: {'、'.join(pattern['market_context_features'])}")
            lines.append(f"- 选股特征: {'、'.join(pattern['stock_selection_features'])}")
            lines.append(f"- 介入特征: {'、'.join(pattern['entry_features'])}")
            lines.append(f"- 退出特征: {'、'.join(pattern['exit_features'])}")
            lines.append(f"- 风控: {'、'.join(pattern['risk_controls'])}")
            lines.append(f"- 仓位风格: {pattern['position_style']}")
            mapped = archive["family_mapping"].get(pattern["pattern_key"], [])
            lines.append(f"- 对应策略族: {'、'.join(mapped)}")
            for item in pattern["evidence_records"][:5]:
                lines.append(
                    f"- 样本: 第{item['live_day']}天 {item['publish_date']} | {item['title']} | 动作 {'、'.join(item['actions']) or '无'}"
                )
            lines.append("")
        return "\n".join(lines)

    def update_catalog_reference(self, misc_name: str) -> None:
        catalog_path = self.archive_root / f"strategy_catalog_{self.teacher}.json"
        if not catalog_path.exists():
            return
        try:
            payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        except Exception:
            return
        misc_archives = payload.get("misc_archives", [])
        if misc_name not in misc_archives:
            misc_archives.append(misc_name)
        payload["misc_archives"] = misc_archives
        catalog_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def extract_actions(self, title: str) -> List[str]:
        actions = []
        mapping = [
            ("打板", "打板"),
            ("扫板", "扫板"),
            ("半路", "半路"),
            ("低吸", "低吸"),
            ("回流", "回流"),
            ("修复", "修复"),
            ("买入", "买入"),
            ("卖出", "卖出"),
            ("止盈", "止盈"),
            ("止损", "止损"),
            ("割肉", "割肉"),
            ("空仓", "空仓"),
            ("梭哈", "梭哈"),
            ("加仓", "加仓"),
            ("减仓", "减仓"),
            ("锁仓", "锁仓"),
            ("持仓", "持仓"),
        ]
        for needle, label in mapping:
            if needle in title:
                actions.append(label)
        return actions

    def extract_stocks(self, title: str) -> List[str]:
        names = self._load_stock_names()
        matches = []
        for name in names:
            if name in title:
                matches.append(name)
        deduped = []
        seen = set()
        for name in sorted(matches, key=lambda item: (-len(item), item)):
            if any(name in chosen for chosen in deduped):
                continue
            if name not in seen:
                deduped.append(name)
                seen.add(name)
        return deduped[:6]

    def match_patterns(self, actions: List[str]) -> List[str]:
        patterns = []
        if "打板" in actions or "扫板" in actions:
            patterns.append("live_limit_up_boarding")
        if "半路" in actions or ("买入" in actions and "打板" not in actions and "扫板" not in actions):
            patterns.append("live_halfway_momentum")
        if "止损" in actions or "割肉" in actions:
            patterns.append("live_switch_and_cut")
        if "梭哈" in actions or "加仓" in actions or "减仓" in actions or "锁仓" in actions or "持仓" in actions:
            patterns.append("live_concentration_and_hold")
        if "空仓" in actions or "止盈" in actions or ("卖出" in actions and "买入" not in actions and "打板" not in actions):
            patterns.append("live_risk_off_exit")
        if "低吸" in actions or "回流" in actions or "修复" in actions:
            patterns.append("live_low_suction_rotation")
        if not patterns and "买入" in actions:
            patterns.append("live_halfway_momentum")
        return list(dict.fromkeys(patterns))

    def pick_primary_pattern(self, actions: List[str], matched_patterns: List[str]) -> str:
        if "空仓" in actions or ("卖出" in actions and "买入" not in actions and "打板" not in actions):
            return "live_risk_off_exit"
        if ("止损" in actions or "割肉" in actions) and any(item in actions for item in ["买入", "打板", "扫板", "梭哈", "加仓"]):
            return "live_switch_and_cut"
        if "打板" in actions or "扫板" in actions:
            return "live_limit_up_boarding"
        if "半路" in actions:
            return "live_halfway_momentum"
        if "梭哈" in actions or "加仓" in actions or "锁仓" in actions or "持仓" in actions:
            return "live_concentration_and_hold"
        if "买入" in actions:
            return "live_halfway_momentum"
        return matched_patterns[0] if matched_patterns else "live_risk_off_exit"

    def _load_source_text(self) -> str:
        path = self.article_root / self.teacher / "实盘.md"
        return path.read_text(encoding="utf-8")

    def _load_stock_names(self) -> List[str]:
        if self._stock_names is not None:
            return self._stock_names
        self.f10._load_full_data_if_needed()
        names = []
        if self.f10.f10_data is not None:
            for _, row in self.f10.f10_data.iterrows():
                name = str(row.get("股票简称", "")).strip()
                if len(name) >= 2:
                    names.append(name)
        self._stock_names = sorted(set(names), key=lambda item: (-len(item), item))
        return self._stock_names

    def _clean_html(self, line: str) -> str:
        line = line.replace("&#x20;", " ").replace("&#x09;", " ").strip()
        line = re.sub(r"<[^>]+>", " ", line)
        return re.sub(r"\s+", " ", line).strip()

    def _normalize_title(self, title: str) -> str:
        title = re.sub(r"有跟帖亮了.*$", "", title).strip()
        return re.sub(r"\s+", " ", title)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Extract live trading patterns from 实盘.md")
    parser.add_argument("--teacher", default="niepan")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    extractor = LiveStrategyExtractor(teacher=args.teacher)
    archive = extractor.extract()
    paths = extractor.write_archive(archive)
    print(
        json.dumps(
            {
                "record_count": archive["record_count"],
                "patterns": [item["pattern_key"] for item in archive["patterns"]],
                "json_path": str(paths[0]),
                "md_path": str(paths[1]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
