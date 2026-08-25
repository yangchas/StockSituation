from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from tools.production_capture.ground_truth_capture import (
    atomic_write_json,
    artifact_record,
    create_capture_marker,
    identity_stable,
    inventory_valid,
    slot_status,
)


def test_slot_status_accepts_only_configured_lateness() -> None:
    scheduled = datetime(2026, 8, 26, 9, 20, 5)
    assert slot_status(scheduled, scheduled)[0] == "ON_TIME"
    assert slot_status(scheduled.replace(second=6), scheduled)[0] == "LATE"
    late = scheduled.replace(second=8, microsecond=1_000)
    assert slot_status(late, scheduled)[0] == "MISSED"


def test_missed_slot_has_no_capture_side_effect_contract() -> None:
    scheduled = datetime(2026, 8, 26, 9, 24, 5)
    status, lateness = slot_status(scheduled.replace(hour=9, minute=32), scheduled)
    assert status == "MISSED"
    assert lateness > 3000


def test_atomic_json_write_and_inventory(tmp_path: Path) -> None:
    path = tmp_path / "rows.json"
    atomic_write_json(path, {"rows": 1})
    assert json.loads(path.read_text(encoding="utf-8")) == {"rows": 1}
    record = artifact_record(path, root=tmp_path)
    manifest = {"artifact_inventory": [record]}
    assert inventory_valid(manifest, tmp_path) == (True, [])


def test_capture_marker_rejects_second_run(tmp_path: Path) -> None:
    marker = create_capture_marker(tmp_path, {"run_id": "one", "trade_date": "2026-08-26"})
    assert marker.is_file()
    try:
        create_capture_marker(tmp_path, {"run_id": "two", "trade_date": "2026-08-26"})
    except FileExistsError:
        pass
    else:
        raise AssertionError("capture marker must be exclusive")


def test_inventory_detects_post_capture_mutation(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_bytes(b'{"symbol":"000001"}\n')
    record = artifact_record(path, root=tmp_path, rows=[{"symbol": "000001", "ts": 1}])
    path.write_bytes(b'{"symbol":"000002"}\n')
    ok, errors = inventory_valid({"artifact_inventory": [record]}, tmp_path)
    assert not ok
    assert any(error.startswith("sha256:") for error in errors)


def test_identity_change_is_not_accepted() -> None:
    base = {
        "main_pid": 10,
        "exe_path": "/bin/t1",
        "binary_sha256": "a",
        "systemd_exec_start": "t1",
        "restart_count": 0,
        "build_info_sha256": "b",
        "release_git_commit": "c",
        "effective_config_sha256": "d",
        "capture_tool_sha256": "e",
    }
    assert identity_stable(base, dict(base))
    changed = dict(base, main_pid=11)
    assert not identity_stable(base, changed)


def test_capture_contract_keeps_logical_timestamp_separate(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_bytes(b'{"symbol":"000001","ts":123}\n')
    record = artifact_record(
        path,
        root=tmp_path,
        rows=[{"symbol": "000001", "ts": 123}],
    )
    assert record["min_logical_timestamp"] == 123
    assert "actual_capture_time" not in record
