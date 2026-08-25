from __future__ import annotations

import argparse
import json
import os
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


def test_inventory_detects_registered_missing_and_size_mismatch(tmp_path: Path) -> None:
    missing = {
        "relative_path": "missing.json",
        "size_bytes": 1,
        "sha256": "missing",
    }
    ok, errors = inventory_valid({"artifact_inventory": [missing]}, tmp_path)
    assert not ok
    assert "missing:missing.json" in errors

    path = tmp_path / "rows.json"
    path.write_bytes(b"one")
    record = artifact_record(path, root=tmp_path)
    record["size_bytes"] += 1
    ok, errors = inventory_valid({"artifact_inventory": [record]}, tmp_path)
    assert not ok
    assert "size:rows.json" in errors


@pytest.mark.parametrize(
    "relative_path",
    ("", "../x", "a/../../x", "/tmp/x", r"C:\\tmp\\x"),
)
def test_inventory_rejects_unsafe_and_absolute_paths(tmp_path: Path, relative_path: str) -> None:
    ok, errors = inventory_valid(
        {"artifact_inventory": [{"relative_path": relative_path}]},
        tmp_path,
    )
    assert not ok
    assert any(error.startswith("invalid_path:") for error in errors)


def test_inventory_rejects_duplicate_paths(tmp_path: Path) -> None:
    path = tmp_path / "rows.json"
    atomic_write_json(path, {"rows": 1})
    record = artifact_record(path, root=tmp_path)
    ok, errors = inventory_valid({"artifact_inventory": [record, record]}, tmp_path)
    assert not ok
    assert "duplicate:rows.json" in errors


@pytest.mark.parametrize("unexpected_name", ("unknown.json", ".orphan.tmp"))
def test_inventory_rejects_unregistered_regular_files(tmp_path: Path, unexpected_name: str) -> None:
    (tmp_path / unexpected_name).write_bytes(b"orphan")
    ok, errors = inventory_valid({"artifact_inventory": []}, tmp_path)
    assert not ok
    assert f"unregistered:{unexpected_name}" in errors


def test_inventory_rejects_unexpected_subdirectory(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    ok, errors = inventory_valid({"artifact_inventory": []}, tmp_path)
    assert not ok
    assert "subdirectory:nested" in errors


def test_inventory_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_bytes(b"target")
    link = tmp_path / "link.json"
    try:
        os.symlink(target, link)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")
    records = [artifact_record(target, root=tmp_path)]
    ok, errors = inventory_valid({"artifact_inventory": records}, tmp_path)
    assert not ok
    assert "symlink:link.json" in errors


def test_inventory_allows_only_control_files_and_requires_registered_marker(tmp_path: Path) -> None:
    marker = create_capture_marker(tmp_path, {"run_id": "one"})
    atomic_write_json(tmp_path / "capture_manifest.partial.json", {"partial": True})
    record = artifact_record(marker, root=tmp_path)
    assert inventory_valid(
        {"artifact_inventory": [record]}, tmp_path, require_capture_started=True
    ) == (True, [])
    ok, errors = inventory_valid(
        {"artifact_inventory": []}, tmp_path, require_capture_started=True
    )
    assert not ok
    assert "missing_registration:capture_started.json" in errors


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


def test_td_export_failure_does_not_publish_partial_official_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeCursor:
        description = (("ts",),)

        def execute(self, _query: str) -> None:
            self.returned = False

        def fetchmany(self, _size: int) -> list[tuple[object, ...]]:
            if self.returned:
                return []
            self.returned = True
            return [(object(),)]

        def close(self) -> None:
            return None

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            return None

    monkeypatch.setattr(capture_module, "_taos_connection", lambda: FakeConnection())
    records = td_export(tmp_path, "2026-08-26")
    assert all(record.get("status") == "FAILED" for record in records)
    assert not list(tmp_path.glob("td_*.jsonl"))
    assert not list(tmp_path.glob("*.tmp"))


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
    assert sealed["capture_performance"]["artifact_count"] == 3
    assert sealed["capture_performance"]["measured_artifact_count"] == 0
    assert sealed["capture_performance"]["status"] == "WARN"


def test_live_capture_manifest_records_production_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = {
        "effective_config_sha256": "config",
        "capture_tool_sha256": "tool",
        "release_provenance_valid": True,
    }
    monkeypatch.setattr(capture_module, "release_identity", lambda: identity)
    monkeypatch.setattr(capture_module, "open_redis", lambda: type("Redis", (), {"write_attempts": 0})())
    monkeypatch.setattr(
        capture_module,
        "wait_slot",
        lambda trade_date, clock: ("MISSED", 10_000, capture_module.scheduled_at(trade_date, clock)),
    )
    assert capture_module.capture(
        argparse.Namespace(trade_date="2026-08-26", output_dir=tmp_path)
    ) == 0
    partial = json.loads((tmp_path / "capture_manifest.partial.json").read_text(encoding="utf-8"))
    assert partial["data_origin"] == "production_capture"
    assert partial["formal_ground_truth"] is True
    assert partial["write_isolation_evidence"]["redis_write_count"] == "runtime_measured"


def test_td_dry_run_uses_formal_export_and_never_creates_sealed_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called = []

    def fake_export(root: Path, trade_date: str) -> list[dict[str, object]]:
        called.append((root, trade_date))
        records = []
        for index in range(6):
            path = root / f"td_export_{index}.jsonl"
            path.write_bytes(b'{"symbol":"000001","ts":1}\n')
            records.append(artifact_record(
                path,
                root=root,
                row_count=1,
                symbol_count=1,
                min_logical_timestamp=1,
                max_logical_timestamp=1,
                capture_duration_ms=1,
            ))
        return records

    monkeypatch.setattr(capture_module, "td_export", fake_export)
    assert capture_module.td_dry_run(
        argparse.Namespace(trade_date="2026-08-26", output_dir=tmp_path)
    ) == 0
    manifest = json.loads((tmp_path / "dry_run_manifest.json").read_text(encoding="utf-8"))
    assert called == [(tmp_path.resolve(), "2026-08-26")]
    assert manifest["data_origin"] == "post_market_td_dry_run"
    assert manifest["formal_ground_truth"] is False
    assert manifest["historical_td_dry_run"] == "PASS"
    assert manifest["write_isolation"] == {
        "redis_write_count": 0,
        "td_write_count": 0,
        "notification_count": 0,
        "network_repair_count": 0,
    }
    assert not (tmp_path / "capture_started.json").exists()
    assert not (tmp_path / "capture_manifest.sealed.json").exists()


def test_td_dry_run_final_recheck_detects_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_export(root: Path, trade_date: str) -> list[dict[str, object]]:
        records = []
        for index in range(6):
            path = root / f"td_export_{index}.jsonl"
            path.write_bytes(b'{"symbol":"000001","ts":1}\n')
            records.append(artifact_record(path, root=root, row_count=1, capture_duration_ms=1))
        (root / "td_export_0.jsonl").write_bytes(b"changed\n")
        return records

    monkeypatch.setattr(capture_module, "td_export", fake_export)
    assert capture_module.td_dry_run(
        argparse.Namespace(trade_date="2026-08-26", output_dir=tmp_path)
    ) == 1
    manifest = json.loads((tmp_path / "dry_run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["historical_td_dry_run"] == "FAIL"
    assert any(error.startswith(("size:", "sha256:")) for error in manifest["final_recheck_errors"])
