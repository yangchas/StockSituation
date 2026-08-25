from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import tools.production_capture.ground_truth_capture as capture_module
from tools.production_capture.ground_truth_capture import (
    CaptureError,
    atomic_write_json,
    artifact_record,
    compact_td,
    capture_tool_frozen,
    create_capture_marker,
    identity_stable,
    inventory_valid,
    normalize_td_value,
    ReadOnlyRedis,
    release_provenance_valid,
    resolved_td_database,
    slot_status,
    td_export,
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
        "declared_binary_sha256": "a",
        "effective_config_sha256": "d",
        "capture_tool_sha256": "e",
        "release_provenance_valid": True,
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


def test_td_normalization_is_explicit_and_timezone_neutral() -> None:
    naive = datetime(2026, 8, 26, 9, 20, 5, 123456)
    aware = datetime(2026, 8, 26, 9, 20, 5, 123456, tzinfo=timezone.utc)
    assert normalize_td_value(naive) == "2026-08-26T09:20:05.123"
    assert normalize_td_value(aware) == "2026-08-26T09:20:05.123+00:00"
    assert normalize_td_value(date(2026, 8, 26)) == "2026-08-26"
    assert normalize_td_value(b"utf8") == "utf8"
    assert compact_td({"v": 1, "s": "x"}) == b'{"s":"x","v":1}'


def test_td_normalization_fails_closed() -> None:
    with pytest.raises(UnicodeDecodeError):
        normalize_td_value(b"\xff")
    with pytest.raises(CaptureError):
        normalize_td_value(object())
    with pytest.raises(CaptureError):
        normalize_td_value(float("nan"))


def test_td_export_is_streaming_canonical_and_uses_resolved_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCursor:
        description = (("ts",), ("symbol",), ("value",))

        def __init__(self) -> None:
            self.queries: list[str] = []
            self.source_batches = [
                [
                    (datetime(2026, 8, 26, 9, 30, 0, 123456), b"000001", 1.5),
                    (datetime(2026, 8, 26, 9, 30, 1, 123456), b"000002", 2.5),
                ],
                [],
            ]
            self.batches = list(self.source_batches)

        def execute(self, query: str) -> None:
            self.queries.append(query)
            self.batches = list(self.source_batches)

        def fetchmany(self, _size: int) -> list[tuple[object, ...]]:
            return self.batches.pop(0)

        def close(self) -> None:
            return None

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_value = FakeCursor()

        def cursor(self) -> FakeCursor:
            return self.cursor_value

        def close(self) -> None:
            return None

    connection = FakeConnection()
    monkeypatch.setenv("TDENGINE_DATABASE", "audit_db")
    monkeypatch.setattr(capture_module, "_taos_connection", lambda: connection)
    records = td_export(tmp_path, "2026-08-26")
    assert len(records) == 6
    assert all("audit_db." in query for query in connection.cursor_value.queries)
    assert all("ORDER BY" in query for query in connection.cursor_value.queries)
    tick = (tmp_path / "td_stock_tick_open.jsonl").read_bytes()
    assert b"2026-08-26T09:30:00.123" in tick
    assert records[-1]["sha256"] == capture_module.sha256_file(tmp_path / "td_stock_tick_open.jsonl")
    assert records[-1]["row_count"] == 2
    assert records[-1]["symbol_count"] == 2
    assert records[-1]["capture_duration_ms"] is not None


def test_resolved_database_rejects_non_identifier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TDENGINE_DATABASE", "market-data1")
    with pytest.raises(CaptureError):
        resolved_td_database()


def test_redis_capture_surface_rejects_writes() -> None:
    class FakePipeline:
        def hmget(self, key: str, *fields: str) -> list[object]:
            return []

        def execute(self) -> list[object]:
            return []

        def set(self, *args: object) -> None:
            return None

    class FakeRedis:
        def pipeline(self, transaction: bool = False) -> FakePipeline:
            return FakePipeline()

    client = ReadOnlyRedis(FakeRedis())
    with pytest.raises(CaptureError):
        client.set("key", "value")
    pipeline = client.pipeline(transaction=False)
    with pytest.raises(CaptureError):
        pipeline.set("key", "value")
    assert client.write_attempts == 2


def test_release_provenance_binds_running_binary_to_build_info(tmp_path: Path) -> None:
    binary = tmp_path / "t1_v2"
    binary.write_bytes(b"binary")
    build_info = tmp_path / "build_info.json"
    binary_hash = capture_module.sha256_file(binary)
    build_info.write_text(json.dumps({"t1_v2_binary_sha256": binary_hash}), encoding="utf-8")
    identity = {
        "binary_sha256": binary_hash,
        "declared_binary_sha256": binary_hash,
        "exe_path": str(binary),
        "build_info_path": str(build_info),
        "build_info_sha256": capture_module.sha256_file(build_info),
    }
    assert release_provenance_valid(identity)
    binary.write_bytes(b"changed")
    assert not release_provenance_valid(identity)


def test_capture_tool_freeze_requires_marker_manifest_and_current_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    create_capture_marker(tmp_path, {"capture_tool_sha256": "tool"})
    manifest = {"capture_tool_sha256": "tool"}
    monkeypatch.setattr(capture_module, "capture_tool_sha256", lambda: "tool")
    assert capture_tool_frozen(manifest, tmp_path)
    monkeypatch.setattr(capture_module, "capture_tool_sha256", lambda: "changed")
    assert not capture_tool_frozen(manifest, tmp_path)


def test_seal_final_recheck_catches_mutation_during_td_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tool_hash = "tool"
    monkeypatch.setattr(capture_module, "capture_tool_sha256", lambda: tool_hash)
    create_capture_marker(tmp_path, {"capture_tool_sha256": tool_hash})

    binary = tmp_path / "t1_v2"
    binary.write_bytes(b"binary")
    build_info = tmp_path / "build_info.json"
    binary_hash = capture_module.sha256_file(binary)
    build_info.write_text(json.dumps({"t1_v2_binary_sha256": binary_hash}), encoding="utf-8")
    identity = {
        "binary_sha256": binary_hash,
        "declared_binary_sha256": binary_hash,
        "exe_path": str(binary),
        "build_info_path": str(build_info),
        "build_info_sha256": capture_module.sha256_file(build_info),
    }

    payload_path = tmp_path / "payload.json"
    atomic_write_json(payload_path, {"value": 1})
    anchor_path = tmp_path / "auction_anchor.json"
    atomic_write_json(anchor_path, {"value": "anchor"})
    inventory = [
        artifact_record(tmp_path / "capture_started.json", root=tmp_path),
        artifact_record(payload_path, root=tmp_path),
        artifact_record(anchor_path, root=tmp_path) | {"anchor_present": True},
    ]
    required_names = ["mapping_security", "auction_0920", "auction_0924", "auction_0925"]
    required_names.extend(f"online_q2_{clock.replace(':', '')}" for clock in capture_module.Q2_SLOTS)
    slots = [{"name": name, "status": "ON_TIME", "artifacts": ["payload.json"]} for name in required_names]
    manifest = {
        "trade_date": "2026-08-26",
        "capture_tool_sha256": tool_hash,
        "runtime_identity_start": identity,
        "runtime_identity_end": identity,
        "runtime_identity_stable": True,
        "release_provenance_valid": True,
        "slots": slots,
        "artifact_inventory": inventory,
        "missing_artifacts": [],
        "partial_evidence": [],
        "write_isolation": {"redis_write_count": 0, "td_write_count": 0, "notification_count": 0, "network_repair_count": 0},
    }
    atomic_write_json(tmp_path / "capture_manifest.partial.json", manifest)

    def mutating_td_export(root: Path, trade_date: str) -> list[dict[str, object]]:
        atomic_write_json(root / "payload.json", {"value": 2})
        return []

    monkeypatch.setattr(capture_module, "td_export", mutating_td_export)
    assert capture_module.seal(argparse.Namespace(trade_date="2026-08-26", output_dir=tmp_path)) == 0
    sealed = json.loads((tmp_path / "capture_manifest.sealed.json").read_text(encoding="utf-8"))
    assert sealed["gate_results"]["artifact_integrity"] == "FAIL"
    assert sealed["seal_status"] == "FAIL"
    assert sealed["integrity_errors_after_seal"]
