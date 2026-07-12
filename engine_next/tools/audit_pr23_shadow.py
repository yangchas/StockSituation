from __future__ import annotations

import argparse
import re
from pathlib import Path


SHADOW_MARKER = "hot_board.migration_shadow |"
REQUIRED_SHADOW_KEYS = (
    "theme",
    "rank",
    "rank_delta_prev",
    "rank_delta_5m",
    "rank_delta_yday",
    "money_state",
    "money_tags",
    "evidence_axes",
    "validation_state",
    "source_freshness",
)
FORBIDDEN_SHADOW_TERMS = (
    "setup_candidate",
    "trade_candidate",
    "profit_center created",
    "主买",
    "进攻",
    "小仓",
    "确认买入",
)
IMPORTANT_OFFICIAL_PATTERNS = (
    "funnel.summary |",
    "funnel.summary.final |",
    "context pipeline seed |",
    "runtime notification delivered",
    "【核心观察池】",
    "【交易候选】",
    "【强事实候选观察】",
    "【风险关注】",
    "推票诊断 |",
    "主线裁判 |",
    "主叙事 |",
    "时间迁移 |",
    "当前动作 |",
    "机会个股 |",
    "避坑方向 |",
)
TIMESTAMP_PREFIX_RE = re.compile(
    r"^(?:\[\d{4}-\d{2}-\d{2} [^\]]+\]\s*)?"
    r"(?:\d{4}-\d{2}-\d{2} [\d:,]+ - [\w.]+ - \w+ -\s*)?"
)


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def _kv_keys(line: str) -> set[str]:
    keys: set[str] = set()
    for chunk in line.split("|"):
        text = chunk.strip()
        if "=" not in text:
            continue
        key = text.split("=", 1)[0].strip()
        if key:
            keys.add(key)
    return keys


def _shadow_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if SHADOW_MARKER in line]


def _normalize_official_line(line: str) -> str:
    text = TIMESTAMP_PREFIX_RE.sub("", line).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _official_signature(lines: list[str]) -> list[str]:
    signature: list[str] = []
    for line in lines:
        if SHADOW_MARKER in line:
            continue
        normalized = _normalize_official_line(line)
        if not normalized:
            continue
        if any(pattern in normalized for pattern in IMPORTANT_OFFICIAL_PATTERNS):
            signature.append(normalized)
    return signature


def _first_diff(before: list[str], after: list[str]) -> tuple[int, str, str] | None:
    max_len = max(len(before), len(after))
    for index in range(max_len):
        left = before[index] if index < len(before) else "<missing>"
        right = after[index] if index < len(after) else "<missing>"
        if left != right:
            return index + 1, left, right
    return None


def audit_after(lines: list[str]) -> int:
    exit_code = 0
    shadows = _shadow_lines(lines)
    print(f"shadow_count={len(shadows)}")
    if not shadows:
        print("ERROR shadow_missing=1")
        return 1

    missing_samples: list[str] = []
    forbidden_samples: list[str] = []
    weak_axis_samples: list[str] = []
    for line in shadows[:50]:
        keys = _kv_keys(line)
        missing = [key for key in REQUIRED_SHADOW_KEYS if key not in keys]
        if missing:
            missing_samples.append(f"missing={','.join(missing)} | {line[:300]}")
        if any(term in line for term in FORBIDDEN_SHADOW_TERMS):
            forbidden_samples.append(line[:300])
        axis_match = re.search(r"evidence_axes=([^|]+)", line)
        axes = [item.strip() for item in (axis_match.group(1).split(",") if axis_match else []) if item.strip() and item.strip() != "-"]
        if len(axes) == 1 and axes[0] == "hot_axis":
            weak_axis_samples.append(line[:300])

    if missing_samples:
        exit_code = 1
        print(f"ERROR shadow_missing_keys={len(missing_samples)}")
        for sample in missing_samples[:5]:
            print(f"  {sample}")
    if forbidden_samples:
        exit_code = 1
        print(f"ERROR shadow_trade_semantic_leak={len(forbidden_samples)}")
        for sample in forbidden_samples[:5]:
            print(f"  {sample}")
    if weak_axis_samples:
        print(f"WARN shadow_hot_axis_only={len(weak_axis_samples)}")
        for sample in weak_axis_samples[:5]:
            print(f"  {sample}")

    states: dict[str, int] = {}
    freshness: dict[str, int] = {}
    validations: dict[str, int] = {}
    for line in shadows:
        for label, store in (
            ("money_state", states),
            ("source_freshness", freshness),
            ("validation_state", validations),
        ):
            match = re.search(rf"{label}=([^|]+)", line)
            value = match.group(1).strip() if match else "missing"
            store[value] = store.get(value, 0) + 1
    print("money_state_counts=" + ",".join(f"{key}:{value}" for key, value in sorted(states.items())) )
    print("source_freshness_counts=" + ",".join(f"{key}:{value}" for key, value in sorted(freshness.items())) )
    print("validation_state_counts=" + ",".join(f"{key}:{value}" for key, value in sorted(validations.items())) )
    return exit_code


def audit_diff(before_lines: list[str], after_lines: list[str]) -> int:
    before_sig = _official_signature(before_lines)
    after_sig = _official_signature(after_lines)
    print(f"official_signature_before={len(before_sig)}")
    print(f"official_signature_after={len(after_sig)}")
    diff = _first_diff(before_sig, after_sig)
    if diff is None:
        print("golden_diff=pass")
        return 0
    index, before, after = diff
    print("golden_diff=fail")
    print(f"first_diff_line={index}")
    print(f"before={before}")
    print(f"after={after}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit PR-2.3.1 migration shadow logs and golden diff.")
    parser.add_argument("--after", required=True, help="Log after PR-2.3.1.")
    parser.add_argument("--before", help="Optional baseline log before PR-2.3.1 for golden diff.")
    args = parser.parse_args()

    after_lines = _read_lines(Path(args.after))
    exit_code = audit_after(after_lines)
    if args.before:
        before_lines = _read_lines(Path(args.before))
        exit_code = max(exit_code, audit_diff(before_lines, after_lines))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
