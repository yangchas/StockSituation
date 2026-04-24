from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
from typing import Iterable

import paramiko


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_sftp_config() -> dict:
    config_path = _repo_root() / ".vscode" / "sftp.json"
    with config_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_local_paths(values: Iterable[str]) -> list[Path]:
    root = _repo_root()
    result: list[Path] = []
    for raw in values:
        path = Path(raw)
        if not path.is_absolute():
            path = (root / path).resolve()
        else:
            path = path.resolve()
        result.append(path)
    return result


def _remote_path_for(local_path: Path, remote_root: str) -> PurePosixPath:
    relative = local_path.resolve().relative_to(_repo_root())
    return PurePosixPath(remote_root, relative.as_posix())


def _ensure_remote_dirs(sftp: paramiko.SFTPClient, remote_file: PurePosixPath) -> None:
    pending: list[str] = []
    current = remote_file.parent
    while str(current) not in ("", "/", "."):
        pending.append(str(current))
        current = current.parent
    for remote_dir in reversed(pending):
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            sftp.mkdir(remote_dir)


def sync_files(files: Iterable[str]) -> list[str]:
    config = _load_sftp_config()
    host = str(config["host"])
    port = int(config.get("port", 22))
    username = str(config["username"])
    password = str(config.get("password", ""))
    remote_root = str(config["remotePath"]).rstrip("/")

    local_paths = _normalize_local_paths(files)
    uploaded: list[str] = []

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=host,
        port=port,
        username=username,
        password=password,
        timeout=15,
    )

    try:
        sftp = ssh.open_sftp()
        try:
            for local_path in local_paths:
                if not local_path.exists():
                    raise FileNotFoundError(f"Local path not found: {local_path}")
                remote_path = _remote_path_for(local_path, remote_root)
                _ensure_remote_dirs(sftp, remote_path)
                sftp.put(str(local_path), str(remote_path))
                uploaded.append(local_path.resolve().relative_to(_repo_root()).as_posix())
                print(f"synced {uploaded[-1]} -> {remote_path}")
        finally:
            sftp.close()
    finally:
        ssh.close()

    return uploaded


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync selected files to the remote server defined by .vscode/sftp.json.")
    parser.add_argument("files", nargs="+", help="Repo-relative or absolute local file paths to upload.")
    args = parser.parse_args()
    sync_files(args.files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
