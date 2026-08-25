"""Small, read-only release provenance hashing helpers.

The manifest is useful only when its recorded hashes are recomputed from the
actual release directory.  It does not publish, deploy, or read secrets.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_tree(
    root: Path,
    *,
    exclude_names: Iterable[str] = ("__pycache__", ".pytest_cache"),
    exclude_paths: Iterable[str] = (),
) -> str:
    root = root.resolve()
    excluded = set(exclude_names)
    excluded_paths = {str(item).replace("\\", "/") for item in exclude_paths}
    digest = hashlib.sha256()
    files = sorted(
        path for path in root.rglob("*")
        if path.is_file()
        and str(path.relative_to(root).as_posix()) not in excluded_paths
        and not any(part in excluded for part in path.relative_to(root).parts)
    )
    for path in files:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content_hash = bytes.fromhex(sha256_file(path))
        digest.update(content_hash)
    return digest.hexdigest()


def build_manifest(
    *,
    source_root: Path,
    git_commit: str,
    artifacts: Mapping[str, Path],
    config_hash: str,
    toolchain: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_paths = [path.resolve().relative_to(source_root.resolve()).as_posix() for path in artifacts.values()]
    return {
        "format": "ReleaseProvenanceV1",
        "git_commit": str(git_commit),
        "source_state_sha256": sha256_tree(source_root, exclude_paths=artifact_paths),
        "artifacts": {name: sha256_file(path) for name, path in sorted(artifacts.items())},
        "config_hash": str(config_hash),
        "toolchain": dict(toolchain or {}),
    }


def validate_manifest(manifest: Mapping[str, Any], *, source_root: Path, artifacts: Mapping[str, Path], config_hash: str) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if manifest.get("format") != "ReleaseProvenanceV1":
        errors.append("format")
    artifact_paths = [path.resolve().relative_to(source_root.resolve()).as_posix() for path in artifacts.values()]
    if manifest.get("source_state_sha256") != sha256_tree(source_root, exclude_paths=artifact_paths):
        errors.append("source_state_sha256")
    recorded_artifacts = manifest.get("artifacts")
    if not isinstance(recorded_artifacts, Mapping):
        errors.append("artifacts")
    else:
        for name, path in sorted(artifacts.items()):
            if recorded_artifacts.get(name) != sha256_file(path):
                errors.append(f"artifact:{name}")
    if manifest.get("config_hash") != str(config_hash):
        errors.append("config_hash")
    return not errors, errors


def load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("release provenance manifest must be a JSON object")
    return dict(payload)
