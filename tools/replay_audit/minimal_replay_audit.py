from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable


TIMESTAMP_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*$")
SECTION_RE = re.compile(r"^【(?P<title>[^】]+)】")


@dataclass
class ReplaySection:
    title: str
    lines: list[str] = field(default_factory=list)


@dataclass
class ReplayBlock:
    timestamp: datetime
    raw_lines: list[str]
    sections: dict[str, ReplaySection]


@dataclass
class TimepointAudit:
    target_time: str
    matched_timestamp: str | None
    delta_seconds: int | None
    current_phase: str | None
    system_status: str | None
    market_status: str | None
    mainline: str | None
    secondary_line: str | None
    primary_theme: str | None
    secondary_theme: str | None
    eax_top3: list[dict[str, str]]
    focus_top4: list[dict[str, str]]
    avoid_top3: list[str]
    high_board_top4: list[dict[str, str]]
    raw_excerpt: dict[str, list[str]]


@dataclass
class DailyAuditReport:
    trade_date: str
    log_file: str
    generated_at: str
    timepoints: list[TimepointAudit]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="最小回放审计：抽取关键时间点的竞价/开盘/盘中输出。")
    parser.add_argument("--log", dest="logs", action="append", help="单个日志文件路径，可重复传入。")
    parser.add_argument("--logs-dir", type=str, help="日志目录，递归查找 .out/.log/.txt。")
    parser.add_argument("--date", required=True, help="交易日，格式 YYYY-MM-DD。")
    parser.add_argument(
        "--times",
        nargs="+",
        default=["09:25", "09:31", "09:35", "09:45"],
        help="要抽取的时间点，默认 09:25 09:31 09:35 09:45。",
    )
    parser.add_argument(
        "--output-dir",
        default="reports/replay_audit",
        help="输出目录，默认 reports/replay_audit。",
    )
    parser.add_argument(
        "--max-delta-seconds",
        type=int,
        default=900,
        help="匹配时间点允许的最大偏差秒数，默认 900。",
    )
    return parser.parse_args()


def _discover_logs(args: argparse.Namespace) -> list[Path]:
    results: list[Path] = []
    if args.logs:
        results.extend(Path(item).expanduser().resolve() for item in args.logs)
    if args.logs_dir:
        root = Path(args.logs_dir).expanduser().resolve()
        for pattern in ("*.out", "*.log", "*.txt"):
            results.extend(root.rglob(pattern))
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in results:
        key = str(path)
        if key in seen or not path.is_file():
            continue
        seen.add(key)
        deduped.append(path)
    if not deduped:
        raise SystemExit("未发现可用日志文件。请传 --log 或 --logs-dir。")
    return deduped


def _read_text(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def _split_blocks(text: str, *, trade_date: str) -> list[ReplayBlock]:
    lines = text.splitlines()
    blocks: list[ReplayBlock] = []
    current_ts: datetime | None = None
    current_lines: list[str] = []
    for line in lines:
        match = TIMESTAMP_RE.match(line.lstrip("\ufeff").strip())
        if match:
            if current_ts is not None and current_ts.strftime("%Y-%m-%d") == trade_date:
                blocks.append(
                    ReplayBlock(
                        timestamp=current_ts,
                        raw_lines=current_lines[:],
                        sections=_build_sections(current_lines),
                    )
                )
            current_ts = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
            current_lines = [line]
            continue
        if current_ts is not None:
            current_lines.append(line)
    if current_ts is not None and current_ts.strftime("%Y-%m-%d") == trade_date:
        blocks.append(
            ReplayBlock(
                timestamp=current_ts,
                raw_lines=current_lines[:],
                sections=_build_sections(current_lines),
            )
        )
    return blocks


def _build_sections(lines: list[str]) -> dict[str, ReplaySection]:
    sections: dict[str, ReplaySection] = {}
    current_title = "__root__"
    sections[current_title] = ReplaySection(title=current_title, lines=[])
    for line in lines[1:]:
        match = SECTION_RE.match(line.strip())
        if match:
            current_title = match.group("title")
            sections.setdefault(current_title, ReplaySection(title=current_title, lines=[]))
        sections[current_title].lines.append(line)
    return sections


def _find_line(lines: Iterable[str], needle: str) -> str | None:
    for line in lines:
        if needle in line:
            return line.strip()
    return None


def _extract_after_pipe(line: str | None, label: str) -> str | None:
    if not line or label not in line:
        return None
    parts = [part.strip() for part in line.split("|")]
    if len(parts) < 2:
        return None
    return parts[-1]


def _extract_pair(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    parts = [item.strip() for item in value.split("/") if item.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


def _parse_table_rows(section: ReplaySection | None) -> list[list[str]]:
    if section is None:
        return []
    rows: list[list[str]] = []
    for line in section.lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("【"):
            continue
        if "|" not in stripped:
            continue
        if stripped.startswith("维度 |") or stripped.startswith("个股 |") or stripped.startswith("题材 |") or stripped.startswith("板位 |"):
            continue
        if stripped.startswith("指标 |") or stripped.startswith("定位 |") or stripped.startswith("方向 |"):
            continue
        parts = [part.strip() for part in stripped.split("|")]
        if len(parts) >= 2:
            rows.append(parts)
    return rows


def _extract_eax_top3(block: ReplayBlock) -> list[dict[str, str]]:
    section = block.sections.get("EAX预期差")
    rows = _parse_table_rows(section)[:3]
    result: list[dict[str, str]] = []
    for row in rows:
        result.append(
            {
                "theme": row[0] if len(row) > 0 else "-",
                "eax": row[1] if len(row) > 1 else "-",
                "expectation_gap": row[2] if len(row) > 2 else "-",
                "action": row[3] if len(row) > 3 else "-",
                "evidence": row[4] if len(row) > 4 else "-",
            }
        )
    return result


def _extract_focus_top4(block: ReplayBlock) -> list[dict[str, str]]:
    section = block.sections.get("核心观察池")
    rows = _parse_table_rows(section)[:4]
    result: list[dict[str, str]] = []
    for row in rows:
        result.append(
            {
                "stock": row[0] if len(row) > 0 else "-",
                "action": row[1] if len(row) > 1 else "-",
                "score": row[2] if len(row) > 2 else "-",
                "auction_pct": row[3] if len(row) > 3 else "-",
                "current_pct": row[4] if len(row) > 4 else "-",
                "theme": row[5] if len(row) > 5 else "-",
                "evidence": row[6] if len(row) > 6 else "-",
            }
        )
    return result


def _extract_high_board_top4(block: ReplayBlock) -> list[dict[str, str]]:
    section = block.sections.get("高标生死簿")
    rows = _parse_table_rows(section)[:4]
    result: list[dict[str, str]] = []
    for row in rows:
        result.append(
            {
                "stock": row[0] if len(row) > 0 else "-",
                "ladder": row[1] if len(row) > 1 else "-",
                "auction_pct": row[2] if len(row) > 2 else "-",
                "current_pct": row[3] if len(row) > 3 else "-",
                "status": row[4] if len(row) > 4 else "-",
                "action": row[7] if len(row) > 7 else (row[-1] if row else "-"),
            }
        )
    return result


def _extract_avoid_top3(block: ReplayBlock) -> list[str]:
    section = block.sections.get("风险提示")
    if section is None:
        return []
    line = _find_line(section.lines, "回避 |")
    if not line:
        return []
    tail = line.split("|", 1)[-1].strip()
    if ":" in tail:
        tail = tail.split(":", 1)[-1]
    items = [item.strip() for item in tail.split(",") if item.strip()]
    return items[:3]


def _extract_mainline_info(block: ReplayBlock) -> tuple[str | None, str | None, str | None, str | None]:
    section = block.sections.get("主线脉络")
    if section is None:
        return None, None, None, None
    mainline_line = _find_line(section.lines, "主线/副线 |")
    primary_line, secondary_line = _extract_pair(_extract_after_pipe(mainline_line, "主线/副线"))
    theme_line = _find_line(section.lines, "题材主攻/次强 |")
    primary_theme, secondary_theme = _extract_pair(_extract_after_pipe(theme_line, "题材主攻/次强"))
    return primary_line, secondary_line, primary_theme, secondary_theme


def _extract_basic_status(block: ReplayBlock) -> tuple[str | None, str | None, str | None]:
    root_lines = block.sections.get("__root__", ReplaySection("__root__", [])).lines
    current_phase = _extract_after_pipe(_find_line(root_lines, "当前阶段："), "当前阶段：")
    if current_phase is None:
        phase_line = _find_line(root_lines, "当前阶段：")
        current_phase = phase_line.split("：", 1)[-1].strip() if phase_line and "：" in phase_line else None
    system_line = _find_line(root_lines, "系统状态：")
    system_status = system_line.split("：", 1)[-1].strip() if system_line and "：" in system_line else None
    market_line = _find_line(root_lines, "行情状态：")
    market_status = market_line.split("：", 1)[-1].strip() if market_line and "：" in market_line else None
    return current_phase, system_status, market_status


def _pick_block_for_time(blocks: list[ReplayBlock], *, trade_date: str, target_hm: str, max_delta_seconds: int) -> tuple[ReplayBlock | None, int | None]:
    target_dt = datetime.strptime(f"{trade_date} {target_hm}:00", "%Y-%m-%d %H:%M:%S")
    forward_candidates: list[tuple[int, ReplayBlock]] = []
    fallback_candidates: list[tuple[int, ReplayBlock]] = []
    for block in blocks:
        signed_delta = int((block.timestamp - target_dt).total_seconds())
        abs_delta = abs(signed_delta)
        if abs_delta > max_delta_seconds:
            continue
        if signed_delta >= 0:
            forward_candidates.append((signed_delta, block))
        fallback_candidates.append((abs_delta, block))
    if forward_candidates:
        forward_candidates.sort(key=lambda item: (item[0], item[1].timestamp))
        return forward_candidates[0][1], forward_candidates[0][0]
    if not fallback_candidates:
        return None, None
    fallback_candidates.sort(key=lambda item: (item[0], item[1].timestamp))
    return fallback_candidates[0][1], fallback_candidates[0][0]


def _build_timepoint_audit(blocks: list[ReplayBlock], *, trade_date: str, target_hm: str, max_delta_seconds: int) -> TimepointAudit:
    block, delta = _pick_block_for_time(blocks, trade_date=trade_date, target_hm=target_hm, max_delta_seconds=max_delta_seconds)
    if block is None:
        return TimepointAudit(
            target_time=target_hm,
            matched_timestamp=None,
            delta_seconds=None,
            current_phase=None,
            system_status=None,
            market_status=None,
            mainline=None,
            secondary_line=None,
            primary_theme=None,
            secondary_theme=None,
            eax_top3=[],
            focus_top4=[],
            avoid_top3=[],
            high_board_top4=[],
            raw_excerpt={},
        )
    current_phase, system_status, market_status = _extract_basic_status(block)
    mainline, secondary_line, primary_theme, secondary_theme = _extract_mainline_info(block)
    return TimepointAudit(
        target_time=target_hm,
        matched_timestamp=block.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        delta_seconds=delta,
        current_phase=current_phase,
        system_status=system_status,
        market_status=market_status,
        mainline=mainline,
        secondary_line=secondary_line,
        primary_theme=primary_theme,
        secondary_theme=secondary_theme,
        eax_top3=_extract_eax_top3(block),
        focus_top4=_extract_focus_top4(block),
        avoid_top3=_extract_avoid_top3(block),
        high_board_top4=_extract_high_board_top4(block),
        raw_excerpt={
            "主线脉络": block.sections.get("主线脉络", ReplaySection("主线脉络")).lines[:8],
            "EAX预期差": block.sections.get("EAX预期差", ReplaySection("EAX预期差")).lines[:8],
            "核心观察池": block.sections.get("核心观察池", ReplaySection("核心观察池")).lines[:8],
            "风险提示": block.sections.get("风险提示", ReplaySection("风险提示")).lines[:5],
        },
    )


def _write_report(report: DailyAuditReport, *, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{report.trade_date}_{Path(report.log_file).stem}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    return json_path, md_path


def _render_markdown(report: DailyAuditReport) -> str:
    lines = [
        f"# 最小回放审计 - {report.trade_date}",
        "",
        f"- 日志文件: `{report.log_file}`",
        f"- 生成时间: `{report.generated_at}`",
        "",
    ]
    for item in report.timepoints:
        matched_line = f"- 匹配时间: `{item.matched_timestamp or '-'}`"
        if item.delta_seconds is not None:
            matched_line += f"（偏差 {item.delta_seconds} 秒）"
        lines.extend(
            [
                f"## {item.target_time}",
                "",
                matched_line,
                f"- 阶段: `{item.current_phase or '-'}`",
                f"- 系统状态: `{item.system_status or '-'}`",
                f"- 主线/副线: `{item.mainline or '-'}` / `{item.secondary_line or '-'}`",
                f"- 题材主攻/次强: `{item.primary_theme or '-'}` / `{item.secondary_theme or '-'}`",
                "",
                "### EAX 前三",
            ]
        )
        if item.eax_top3:
            for row in item.eax_top3:
                lines.append(
                    f"- `{row['theme']}` | EAX `{row['eax']}` | 预期差 `{row['expectation_gap']}` | 动作 `{row['action']}`"
                )
        else:
            lines.append("- 无")
        lines.extend(["", "### 观察池前四"] )
        if item.focus_top4:
            for row in item.focus_top4:
                lines.append(
                    f"- `{row['stock']}` | `{row['action']}` | 分数 `{row['score']}` | 题材 `{row['theme']}` | 证据 `{row['evidence']}`"
                )
        else:
            lines.append("- 无")
        lines.extend(["", "### 回避前三"])
        if item.avoid_top3:
            for row in item.avoid_top3:
                lines.append(f"- {row}")
        else:
            lines.append("- 无")
        lines.extend(["", "### 高标前四"])
        if item.high_board_top4:
            for row in item.high_board_top4:
                lines.append(
                    f"- `{row['stock']}` | 梯队 `{row['ladder']}` | 状态 `{row['status']}` | 动作 `{row['action']}`"
                )
        else:
            lines.append("- 无")
        lines.append("")
    return "\n".join(lines).strip() + "\n"

def _build_report(log_path: Path, *, trade_date: str, times: list[str], max_delta_seconds: int) -> DailyAuditReport:
    text = _read_text(log_path)
    blocks = _split_blocks(text, trade_date=trade_date)
    timepoints = [
        _build_timepoint_audit(blocks, trade_date=trade_date, target_hm=target_hm, max_delta_seconds=max_delta_seconds)
        for target_hm in times
    ]
    return DailyAuditReport(
        trade_date=trade_date,
        log_file=str(log_path),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        timepoints=timepoints,
    )


def main() -> int:
    args = _parse_args()
    logs = _discover_logs(args)
    output_dir = Path(args.output_dir).expanduser().resolve()
    for log_path in logs:
        report = _build_report(
            log_path,
            trade_date=args.date,
            times=list(args.times),
            max_delta_seconds=int(args.max_delta_seconds),
        )
        json_path, md_path = _write_report(report, output_dir=output_dir)
        print(f"[ok] {log_path} -> {json_path}")
        print(f"[ok] {log_path} -> {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
