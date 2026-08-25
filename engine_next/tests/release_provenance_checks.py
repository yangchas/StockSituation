from __future__ import annotations

from pathlib import Path

from tools.production_capture.release_provenance import build_manifest, validate_manifest


def test_release_manifest_binds_source_and_artifact_hashes(tmp_path: Path) -> None:
    source = tmp_path / "release"
    source.mkdir()
    source_file = source / "engine.py"
    artifact = source / "engine.bin"
    source_file.write_text("stable", encoding="utf-8")
    artifact.write_bytes(b"binary-a")
    manifest = build_manifest(
        source_root=source,
        git_commit="7af7f79c",
        artifacts={"engine": artifact},
        config_hash="config-a",
    )
    ok, errors = validate_manifest(manifest, source_root=source, artifacts={"engine": artifact}, config_hash="config-a")
    assert ok and errors == []
    artifact.write_bytes(b"binary-b")
    ok, errors = validate_manifest(manifest, source_root=source, artifacts={"engine": artifact}, config_hash="config-a")
    assert not ok
    assert errors == ["artifact:engine"]


def test_release_manifest_detects_config_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "release"
    source.mkdir()
    artifact = source / "engine.bin"
    artifact.write_bytes(b"binary")
    manifest = build_manifest(source_root=source, git_commit="commit", artifacts={"engine": artifact}, config_hash="config-a")
    ok, errors = validate_manifest(manifest, source_root=source, artifacts={"engine": artifact}, config_hash="config-b")
    assert not ok
    assert errors == ["config_hash"]
