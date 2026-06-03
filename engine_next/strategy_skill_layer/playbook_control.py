from __future__ import annotations

from engine_next.domain.decision_models import PlaybookControlMatrix, PlaybookControlRow


PLAYBOOK_LABELS: dict[str, str] = {
    "mainline_attack": "\u4e3b\u7ebf\u8fdb\u653b",
    "dragon_pressure_repair": "\u9ad8\u6807\u4fee\u590d",
    "dragon_head_risk_control": "\u9ad8\u6807\u98ce\u63a7",
    "sector_rotation": "\u9898\u6750\u5207\u6362",
    "weak_to_strong_repair": "\u5f31\u8f6c\u5f3a\u4fee\u590d",
    "yesterday_limit_relay": "\u6628\u65e5\u6da8\u505c\u63a5\u529b",
    "watch": "\u89c2\u5bdf",
}

PLAYBOOK_BY_CANDIDATE_PATH: tuple[tuple[str, str], ...] = (
    ("hot_plate_hard_risk_watch", "dragon_head_risk_control"),
    ("hot_plate_anchor_attack", "sector_rotation"),
    ("hot_plate_anchor_watch", "sector_rotation"),
    ("timeframe_aligned_attack", "sector_rotation"),
    ("timeframe_watch", "sector_rotation"),
    ("local_pack_pressure_repair", "dragon_pressure_repair"),
    ("local_pack_main_attack", "mainline_attack"),
    ("local_pack_aligned", "mainline_attack"),
    ("main_attack", "mainline_attack"),
    ("mainline_follow", "mainline_attack"),
    ("rotation_probe", "sector_rotation"),
    ("risk_theme_watch", "dragon_head_risk_control"),
    ("watch", "watch"),
)


def playbook_label(playbook: str) -> str:
    return PLAYBOOK_LABELS.get(str(playbook or ""), str(playbook or "-"))


def playbook_for_candidate_path(path_type: str) -> str:
    value = str(path_type or "")
    for prefix, playbook in PLAYBOOK_BY_CANDIDATE_PATH:
        if value == prefix or value.startswith(f"{prefix}_"):
            return playbook
    return "watch"


def playbook_row(matrix: PlaybookControlMatrix | None, playbook: str) -> PlaybookControlRow | None:
    if matrix is None:
        return None
    for row in matrix.rows:
        if row.playbook == playbook:
            return row
    return None


def playbook_row_enabled(row: PlaybookControlRow | None) -> bool:
    return bool(row is not None and row.enabled and row.action_hint not in {"avoid", "avoid_chase", "disabled"})


def playbook_matrix_ready(matrix: PlaybookControlMatrix | None) -> bool:
    return bool(matrix is not None and matrix.rows)


def playbook_row_blocks_attack(row: PlaybookControlRow | None) -> bool:
    if row is None:
        return False
    if row.action_hint in {"avoid", "avoid_chase", "disabled"}:
        return True
    return bool(row.cap <= 0.0 and row.risk_tags)


def playbook_decision_text(row: PlaybookControlRow | None, *, fallback_playbook: str = "") -> str:
    if row is None:
        label = playbook_label(fallback_playbook)
        return f"{label}:unknown"
    state = "on" if playbook_row_enabled(row) else "off"
    risks = ",".join(row.risk_tags) if row.risk_tags else "-"
    return f"{playbook_label(row.playbook)}:{state}/{row.action_hint}/cap={row.cap:.0%}/risk={risks}/{row.reason or '-'}"
