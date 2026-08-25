"""One-shot, read-only Ground Truth capture and sealing tool.

The tool measures production inputs for equivalence auditing.  It is not a
production runtime dependency and never writes Redis, TDengine, or mail.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


TZ = ZoneInfo("Asia/Shanghai")
MAX_LATENESS_MS = 3_000
Q2_FIELDS = ("px", "pc", "amt", "amt2m", "ls", "ts")
AUCTION_SLOTS = (("0920", "09:20:05"), ("0924", "09:24:05"), ("0925", "09:25:10"))
Q2_SLOTS = (
    "09:30:10", "09:30:20", "09:30:30", "09:30:40", "09:30:50",
    "09:31:00", "09:31:10", "09:31:20", "09:31:30", "09:31:40", "09:31:50",
    "09:32:00", "09:32:10",
)
F10_PATH = Path("/home/exedev/services/engine-next/current/data/f10.csv")


class CaptureError(RuntimeError):
    """A capture or seal contract failure."""


def compact(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def normalize_td_value(value: Any) -> Any:
    """Convert only the scalar types returned by TDengine to JSON values."""
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CaptureError(f"non-finite TD float: {value!r}")
        return value
    raise CaptureError(f"unsupported TD value type: {type(value).__name__}")


def normalize_td_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): normalize_td_value(value) for key, value in row.items()}


def compact_td(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def now_local() -> datetime:
    return datetime.now(TZ)


def parse_trade_date(value: str) -> str:
    normalized = str(value).strip()
    datetime.strptime(normalized, "%Y-%m-%d")
    return normalized


def scheduled_at(trade_date: str, clock: str) -> datetime:
    return datetime.fromisoformat(f"{trade_date}T{clock}").replace(tzinfo=TZ)


def slot_status(actual: datetime, scheduled: datetime, max_lateness_ms: int = MAX_LATENESS_MS) -> tuple[str, int]:
    lateness_ms = max(0, int((actual - scheduled).total_seconds() * 1000))
    if lateness_ms <= max_lateness_ms:
        return ("ON_TIME" if lateness_ms == 0 else "LATE"), lateness_ms
    return "MISSED", lateness_ms


def atomic_write_bytes(path: Path, payload: bytes, *, exclusive: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if exclusive:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(path, flags, 0o644)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            fd = -1
        finally:
            if fd != -1:
                os.close(fd)
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    try:
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        pass


def atomic_write_json(path: Path, value: Any, *, exclusive: bool = False) -> None:
    atomic_write_bytes(path, compact(value) + b"\n", exclusive=exclusive)


def copy_atomic(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        with source.open("rb") as source_handle:
            shutil.copyfileobj(source_handle, handle, length=1024 * 1024)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    try:
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        pass


def decode(value: Any) -> Any:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value


def capture_tool_sha256() -> str:
    return sha256_file(Path(__file__).resolve())


def command_value(*args: str) -> str:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def release_identity() -> dict[str, Any]:
    service = "t1-v2-live.service"
    properties = {}
    for key in ("MainPID", "ExecStart", "NRestarts", "ExecMainStartTimestamp", "ActiveState", "SubState"):
        properties[key] = command_value("systemctl", "show", service, f"-p{key}", "--value")
    pid = properties.get("MainPID", "")
    exe_path = Path(f"/proc/{pid}/exe").resolve() if pid.isdigit() and int(pid) > 0 else None
    build_info_candidates = (
        Path("/home/exedev/services/t1-v2/current/build_info.json"),
        Path("/home/exedev/services/t1-v2/releases/20260825_8969b1f/build_info.json"),
    )
    build_info = next((path.resolve() for path in build_info_candidates if path.is_file()), None)
    payload: dict[str, Any] = {
        "service": service,
        "main_pid": int(pid) if pid.isdigit() else None,
        "exe_path": str(exe_path) if exe_path else None,
        "binary_sha256": sha256_file(exe_path) if exe_path and exe_path.is_file() else None,
        "systemd_exec_start": properties.get("ExecStart") or None,
        "restart_count": int(properties["NRestarts"]) if properties.get("NRestarts", "").isdigit() else None,
        "exec_main_start_timestamp": properties.get("ExecMainStartTimestamp") or None,
        "active_state": properties.get("ActiveState") or None,
        "sub_state": properties.get("SubState") or None,
        "build_info_path": str(build_info) if build_info else None,
        "build_info_sha256": sha256_file(build_info) if build_info and build_info.is_file() else None,
        "capture_tool_sha256": capture_tool_sha256(),
    }
    if build_info and build_info.is_file():
        try:
            info = json.loads(build_info.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            info = {}
        payload.update({
            "release_git_commit": info.get("git_commit"),
            "release_role": info.get("release_role"),
            "declared_binary_sha256": info.get("t1_v2_binary_sha256"),
            "source_bundle_sha256": info.get("source_bundle_sha256"),
            "source_state_sha256": info.get("source_state_sha256"),
            "effective_config_sha256": info.get("effective_config_sha256", info.get("config_hash")),
            "config_provenance": "PASS" if info.get("effective_config_sha256") else "PARTIAL",
        })
    else:
        payload["config_provenance"] = "PARTIAL"
    payload["release_provenance_valid"] = release_provenance_valid(payload)
    return payload


def release_provenance_valid(identity: Mapping[str, Any]) -> bool:
    actual = identity.get("binary_sha256")
    declared = identity.get("declared_binary_sha256")
    exe_path = identity.get("exe_path")
    build_info_path = identity.get("build_info_path")
    build_info_hash = identity.get("build_info_sha256")
    return bool(
        actual
        and declared
        and actual == declared
        and exe_path
        and Path(str(exe_path)).is_file()
        and sha256_file(Path(str(exe_path))) == actual
        and build_info_path
        and build_info_hash
        and Path(str(build_info_path)).is_file()
        and sha256_file(Path(str(build_info_path))) == build_info_hash
    )


def identity_stable(start: Mapping[str, Any], end: Mapping[str, Any]) -> bool:
    fields = (
        "main_pid", "exe_path", "binary_sha256", "systemd_exec_start", "restart_count",
        "build_info_sha256", "release_git_commit", "declared_binary_sha256",
        "effective_config_sha256", "capture_tool_sha256", "release_provenance_valid",
    )
    return all(start.get(field) not in (None, "") and start.get(field) == end.get(field) for field in fields)


def artifact_record(
    path: Path,
    *,
    root: Path,
    rows: Sequence[Mapping[str, Any]] | None = None,
    capture_duration_ms: int | None = None,
    row_count: int | None = None,
    symbol_count: int | None = None,
    min_logical_timestamp: Any = None,
    max_logical_timestamp: Any = None,
) -> dict[str, Any]:
    rows = rows or ()
    symbols = {str(row.get("symbol")) for row in rows if row.get("symbol")}
    timestamps = []
    for row in rows:
        value = row.get("ts", row.get("timestamp", row.get("logical_timestamp")))
        try:
            timestamps.append((0, int(value), value))
        except (TypeError, ValueError):
            if value is not None:
                timestamps.append((1, str(value), value))
    if min_logical_timestamp is None and timestamps:
        min_logical_timestamp = min(timestamps)[2]
    if max_logical_timestamp is None and timestamps:
        max_logical_timestamp = max(timestamps)[2]
    return {
        "relative_path": str(path.relative_to(root).as_posix()),
        "size_bytes": path.stat().st_size,
        "row_count": len(rows) if row_count is None else row_count,
        "symbol_count": len(symbols) if symbol_count is None else symbol_count,
        "min_logical_timestamp": min_logical_timestamp,
        "max_logical_timestamp": max_logical_timestamp,
        "sha256": sha256_file(path),
        "capture_duration_ms": capture_duration_ms,
    }


def inventory_valid(manifest: Mapping[str, Any], root: Path) -> tuple[bool, list[str]]:
    errors = []
    for item in manifest.get("artifact_inventory", []):
        path = root / str(item.get("relative_path", ""))
        if not path.is_file():
            errors.append(f"missing:{item.get('relative_path')}")
            continue
        if path.stat().st_size != item.get("size_bytes"):
            errors.append(f"size:{item.get('relative_path')}")
        if sha256_file(path) != item.get("sha256"):
            errors.append(f"sha256:{item.get('relative_path')}")
    return not errors, errors


def capture_tool_frozen(manifest: Mapping[str, Any], root: Path) -> bool:
    marker_path = root / "capture_started.json"
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    current = capture_tool_sha256()
    values = {
        str(marker.get("capture_tool_sha256") or ""),
        str(manifest.get("capture_tool_sha256") or ""),
        current,
    }
    return len(values) == 1 and "" not in values


class ReadOnlyRedisPipeline:
    """Small read-only surface for the capture tool's Redis pipeline."""

    _WRITE_METHODS = frozenset({"set", "setex", "hset", "hdel", "delete", "expire", "publish", "rpush", "lpush"})

    def __init__(self, pipeline: Any, owner: "ReadOnlyRedis") -> None:
        self._pipeline = pipeline
        self._owner = owner

    def hmget(self, key: str, *fields: str) -> Any:
        return self._pipeline.hmget(key, *fields)

    def execute(self) -> Any:
        return self._pipeline.execute()

    def __getattr__(self, name: str) -> Any:
        if name in self._WRITE_METHODS:
            self._owner.write_attempts += 1
            raise CaptureError(f"Redis write is forbidden during capture: {name}")
        raise AttributeError(name)


class ReadOnlyRedis:
    """Expose only the Redis reads used by this one-shot capture."""

    _WRITE_METHODS = ReadOnlyRedisPipeline._WRITE_METHODS

    def __init__(self, client: Any) -> None:
        self._client = client
        self.write_attempts = 0

    def ping(self) -> Any:
        return self._client.ping()

    def hgetall(self, key: str) -> Any:
        return self._client.hgetall(key)

    def get(self, key: str) -> Any:
        return self._client.get(key)

    def scan_iter(self, **kwargs: Any) -> Any:
        return self._client.scan_iter(**kwargs)

    def pipeline(self, *, transaction: bool = False) -> ReadOnlyRedisPipeline:
        return ReadOnlyRedisPipeline(self._client.pipeline(transaction=transaction), self)

    def __getattr__(self, name: str) -> Any:
        if name in self._WRITE_METHODS:
            self.write_attempts += 1
            raise CaptureError(f"Redis write is forbidden during capture: {name}")
        raise AttributeError(name)


def open_redis() -> ReadOnlyRedis:
    try:
        import redis
    except ImportError as exc:
        raise CaptureError("redis package unavailable") from exc
    client = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=False, socket_timeout=5)
    client.ping()
    return ReadOnlyRedis(client)


def wait_slot(trade_date: str, clock: str) -> tuple[str, int, datetime]:
    scheduled = scheduled_at(trade_date, clock)
    actual = now_local()
    if actual < scheduled:
        time.sleep((scheduled - actual).total_seconds())
        actual = now_local()
    status, lateness_ms = slot_status(actual, scheduled)
    return status, lateness_ms, actual


def q2_snapshot(client: Any, root: Path, slot: str) -> dict[str, Any]:
    started = time.monotonic()
    keys = sorted(str(decode(key)) for key in client.scan_iter(match="q2:??????", count=2000))
    rows: list[dict[str, Any]] = []
    for start in range(0, len(keys), 1000):
        batch = keys[start:start + 1000]
        pipe = client.pipeline(transaction=False)
        for key in batch:
            pipe.hmget(key, *Q2_FIELDS)
        for key, values in zip(batch, pipe.execute()):
            decoded = [decode(value) for value in values]
            if not any(value is not None for value in decoded):
                continue
            rows.append({"symbol": key[3:], **dict(zip(Q2_FIELDS, decoded))})
    if not rows:
        raise CaptureError(f"Q2 snapshot is empty at {slot}")
    path = root / f"q2_{slot.replace(':', '')}.jsonl"
    with tempfile.NamedTemporaryFile(dir=root, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        for row in rows:
            handle.write(compact(row) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    return artifact_record(path, root=root, rows=rows, capture_duration_ms=int((time.monotonic() - started) * 1000)) | {
        "slot": slot,
        "source": "redis:q2:*",
        "actual_capture_time": now_local().isoformat(timespec="milliseconds"),
    }


def capture_mapping_and_metadata(client: Any, root: Path) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    raw = client.hgetall("market:stock_plate") or {}
    mapping = {str(decode(key)): str(decode(value)) for key, value in raw.items()}
    if not mapping:
        raise CaptureError("market:stock_plate is empty")
    mapping_payload = {
        "format": "GroundTruthMappingCaptureV1",
        "source": "redis:market:stock_plate",
        "capture_time": now_local().isoformat(timespec="seconds"),
        "record_count": len(mapping),
        "mapping": dict(sorted(mapping.items())),
    }
    mapping_payload["sha256"] = sha256_bytes(compact(mapping_payload["mapping"]))
    mapping_path = root / "mapping_0910.json"
    atomic_write_json(mapping_path, mapping_payload)
    artifacts.append(artifact_record(mapping_path, root=root))

    f10_source = F10_PATH.resolve() if F10_PATH.is_file() else None
    if f10_source:
        f10_path = root / "f10.csv"
        copy_atomic(f10_source, f10_path)
        meta_path = root / "security_metadata.json"
        atomic_write_json(meta_path, {
            "status": "available",
            "source": str(f10_source),
            "capture_time": now_local().isoformat(timespec="seconds"),
            "source_mtime_ns": f10_source.stat().st_mtime_ns,
            "scope": "security background metadata; no_price_limit input not proven",
            "sha256": sha256_file(f10_path),
        })
        artifacts.extend((artifact_record(f10_path, root=root), artifact_record(meta_path, root=root)))
    else:
        atomic_write_json(root / "security_metadata.json", {"status": "unavailable"})
        artifacts.append(artifact_record(root / "security_metadata.json", root=root))
    return artifacts


def capture_auction(client: Any, root: Path, trade_date: str, tag: str) -> list[dict[str, Any]]:
    date_tag = trade_date.replace("-", "")
    key = f"market:auction:{date_tag}:{tag}"
    fields = {str(decode(k)): decode(v) for k, v in (client.hgetall(key) or {}).items()}
    if not fields:
        raise CaptureError(f"auction key is empty: {key}")
    anchor_key = f"market:auction:anchor:{date_tag}"
    latest_key = f"market:auction:{date_tag}:latest"
    payload = {
        "format": "GroundTruthAuctionRedisCaptureV1",
        "trade_date": trade_date,
        "slot": tag,
        "capture_time": now_local().isoformat(timespec="seconds"),
        "source_key": key,
        "hash_fields": fields,
        "anchor_key": anchor_key,
        "anchor": decode(client.get(anchor_key)),
        "latest_key": latest_key,
        "latest": {str(decode(k)): decode(v) for k, v in (client.hgetall(latest_key) or {}).items()},
    }
    path = root / f"auction_{tag}.json"
    atomic_write_json(path, payload)
    records = [artifact_record(path, root=root)]
    if tag == "0925":
        anchor_path = root / "auction_anchor.json"
        atomic_write_json(anchor_path, {"trade_date": trade_date, "source_key": anchor_key, "value": payload["anchor"]})
        records.append(artifact_record(anchor_path, root=root) | {"anchor_present": payload["anchor"] is not None})
    return records


def write_progress(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_json(path, payload)


def create_capture_marker(root: Path, payload: Mapping[str, Any]) -> Path:
    marker = root / "capture_started.json"
    atomic_write_json(marker, payload, exclusive=True)
    return marker


def capture(args: argparse.Namespace) -> int:
    trade_date = parse_trade_date(args.trade_date)
    root = Path(args.output_dir).resolve()
    if root.exists():
        if (root / "capture_started.json").exists():
            raise CaptureError("capture_started.json already exists; refusing a second run")
        if any(root.iterdir()):
            raise CaptureError("output directory is not empty")
    else:
        root.mkdir(parents=True)
    start_identity = release_identity()
    run_id = f"{trade_date.replace('-', '')}-{start_identity.get('main_pid')}-{int(time.time())}"
    marker = {
        "run_id": run_id,
        "trade_date": trade_date,
        "capture_tool_sha256": capture_tool_sha256(),
        "started_at": now_local().isoformat(timespec="seconds"),
    }
    create_capture_marker(root, marker)
    manifest: dict[str, Any] = {
        "format": "ProductionGroundTruthCaptureV2",
        "trade_date": trade_date,
        "run_id": run_id,
        "capture_tool_sha256": marker["capture_tool_sha256"],
        "runtime_identity_start": start_identity,
        "runtime_identity_end": None,
        "release_provenance_valid": release_provenance_valid(start_identity),
        "slots": [],
        "artifact_inventory": [artifact_record(root / "capture_started.json", root=root)],
        "missing_artifacts": [],
        "partial_evidence": [],
        "write_isolation": {
            "redis_write_count": 0,
            "td_write_count": 0,
            "notification_count": 0,
            "network_repair_count": 0,
        },
        "td_export_status": "pending_post_market",
        "sealed": False,
    }
    progress = root / "capture_manifest.partial.json"
    client = open_redis()

    def run_slot(name: str, clock: str, fn: Any) -> None:
        status, lateness_ms, actual = wait_slot(trade_date, clock)
        slot: dict[str, Any] = {
            "name": name,
            "scheduled_time": scheduled_at(trade_date, clock).isoformat(),
            "actual_capture_time": actual.isoformat(timespec="milliseconds"),
            "lateness_ms": lateness_ms,
            "status": status,
        }
        if status == "MISSED":
            manifest["missing_artifacts"].append(name)
        elif status in {"ON_TIME", "LATE"}:
            capture_started = time.monotonic()
            try:
                records = fn()
                elapsed_ms = int((time.monotonic() - capture_started) * 1000)
                for record in records:
                    if record.get("capture_duration_ms") is None:
                        record["capture_duration_ms"] = elapsed_ms
                slot["artifacts"] = [record["relative_path"] for record in records]
                manifest["artifact_inventory"].extend(records)
            except Exception as exc:  # capture evidence must preserve failure state
                slot["status"] = "FAILED"
                slot["error"] = f"{type(exc).__name__}: {exc}"
                manifest["partial_evidence"].append(name)
        manifest["slots"].append(slot)
        write_progress(progress, manifest)

    run_slot("mapping_security", "09:09:40", lambda: capture_mapping_and_metadata(client, root))
    for tag, clock in AUCTION_SLOTS:
        run_slot(f"auction_{tag}", clock, lambda tag=tag: capture_auction(client, root, trade_date, tag))
    for clock in Q2_SLOTS:
        run_slot(f"online_q2_{clock.replace(':', '')}", clock, lambda clock=clock: [q2_snapshot(client, root, clock)])
    manifest["write_isolation"]["redis_write_count"] = client.write_attempts
    manifest["runtime_identity_end"] = release_identity()
    manifest["runtime_identity_stable"] = identity_stable(manifest["runtime_identity_start"], manifest["runtime_identity_end"])
    manifest["release_provenance_valid"] = (
        release_provenance_valid(manifest["runtime_identity_start"])
        and release_provenance_valid(manifest["runtime_identity_end"])
    )
    if not manifest["runtime_identity_stable"]:
        manifest["partial_evidence"].append("runtime_identity")
    manifest["finished_at"] = now_local().isoformat(timespec="seconds")
    write_progress(progress, manifest)
    return 0


def resolved_td_database() -> str:
    database = os.environ.get("TDENGINE_DATABASE", "market_data1")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise CaptureError("invalid TDENGINE_DATABASE identifier")
    return database


def _taos_connection() -> Any:
    try:
        import taos
    except ImportError as exc:
        raise CaptureError("taos package unavailable") from exc
    return taos.connect(
        host=os.environ.get("TDENGINE_HOST", "127.0.0.1"),
        user=os.environ.get("TDENGINE_USER", "root"),
        password=os.environ.get("TDENGINE_PASSWORD", "taosdata"),
        database=resolved_td_database(),
    )


def td_export(root: Path, trade_date: str) -> list[dict[str, Any]]:
    database = resolved_td_database()
    connection = _taos_connection()
    records: list[dict[str, Any]] = []
    try:
        cursor = connection.cursor()
        exports = (
            ("auction_summary_v2", f"SELECT * FROM {database}.auction_summary_v2 WHERE trade_date='{trade_date.replace('-', '')}' ORDER BY ts", "ts", None),
            ("auction_snapshot_0920", f"SELECT * FROM {database}.auction_snapshot_v2 WHERE trade_date='{trade_date.replace('-', '')}' AND auction_tag='0920' ORDER BY ts, symbol", "ts", "symbol"),
            ("auction_snapshot_0924", f"SELECT * FROM {database}.auction_snapshot_v2 WHERE trade_date='{trade_date.replace('-', '')}' AND auction_tag='0924' ORDER BY ts, symbol", "ts", "symbol"),
            ("auction_snapshot_0925", f"SELECT * FROM {database}.auction_snapshot_v2 WHERE trade_date='{trade_date.replace('-', '')}' AND auction_tag='0925' ORDER BY ts, symbol", "ts", "symbol"),
            ("stock_tick_auction", "SELECT * FROM %s.stock_tick_v2 WHERE ts >= '%s 09:15:00.000' AND ts < '%s 09:26:00.000' ORDER BY ts, symbol" % (database, trade_date, trade_date), "ts", "symbol"),
            ("stock_tick_open", "SELECT * FROM %s.stock_tick_v2 WHERE ts >= '%s 09:30:00.000' AND ts < '%s 09:33:00.000' ORDER BY ts, symbol" % (database, trade_date, trade_date), "ts", "symbol"),
        )
        for name, query, logical_timestamp_field, symbol_field in exports:
            temporary: Path | None = None
            export_started = time.monotonic()
            try:
                if not re.match(r"^\s*SELECT\b", query, re.IGNORECASE):
                    raise CaptureError(f"non-read-only TD query: {name}")
                cursor.execute(query)
                columns = [str(item[0]) for item in (cursor.description or ())]
                path = root / f"td_{name}.jsonl"
                row_count = 0
                symbols: set[str] = set()
                min_timestamp: Any = None
                max_timestamp: Any = None
                stream_digest = hashlib.sha256()
                with tempfile.NamedTemporaryFile(dir=root, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
                    temporary = Path(handle.name)
                    while True:
                        batch = cursor.fetchmany(5000)
                        if not batch:
                            break
                        for values in batch:
                            row = normalize_td_row(dict(zip(columns, values)))
                            line = compact_td(row) + b"\n"
                            handle.write(line)
                            stream_digest.update(line)
                            row_count += 1
                            if symbol_field and row.get(symbol_field) is not None:
                                symbols.add(str(row[symbol_field]))
                            timestamp = row.get(logical_timestamp_field)
                            if timestamp is not None:
                                if min_timestamp is None or str(timestamp) < str(min_timestamp):
                                    min_timestamp = timestamp
                                if max_timestamp is None or str(timestamp) > str(max_timestamp):
                                    max_timestamp = timestamp
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
                record = artifact_record(
                    path,
                    root=root,
                    row_count=row_count,
                    symbol_count=len(symbols),
                    min_logical_timestamp=min_timestamp,
                    max_logical_timestamp=max_timestamp,
                    capture_duration_ms=int((time.monotonic() - export_started) * 1000),
                )
                if record["sha256"] != stream_digest.hexdigest():
                    raise CaptureError(f"stream hash mismatch: {name}")
                records.append(record | {
                    "source": "post_market_td_export",
                    "table": name,
                    "query": query,
                    "schema_columns": columns,
                    "logical_timestamp_field": logical_timestamp_field,
                    "symbol_field": symbol_field,
                    "database": database,
                })
            except Exception as exc:
                records.append({"source": "post_market_td_export", "table": name, "status": "FAILED", "error": f"{type(exc).__name__}: {exc}", "query": query})
                if temporary and temporary.exists():
                    temporary.unlink()
        cursor.close()
    finally:
        connection.close()
    return records


def seal(args: argparse.Namespace) -> int:
    trade_date = parse_trade_date(args.trade_date)
    root = Path(args.output_dir).resolve()
    sealed_path = root / "capture_manifest.sealed.json"
    if sealed_path.exists():
        raise CaptureError("capture_manifest.sealed.json already exists")
    partial_path = root / "capture_manifest.partial.json"
    if not partial_path.is_file():
        raise CaptureError("capture_manifest.partial.json is missing")
    manifest = json.loads(partial_path.read_text(encoding="utf-8"))
    if manifest.get("trade_date") != trade_date:
        raise CaptureError("trade_date mismatch")
    valid_before, errors_before = inventory_valid(manifest, root)
    if errors_before:
        manifest["integrity_errors_before_seal"] = errors_before
    tool_frozen = capture_tool_frozen(manifest, root)
    release_valid = bool(manifest.get("release_provenance_valid")) and all(
        release_provenance_valid(identity)
        for identity in (
            manifest.get("runtime_identity_start", {}),
            manifest.get("runtime_identity_end", {}),
        )
    )
    if tool_frozen and valid_before:
        try:
            td_records = td_export(root, trade_date)
        except Exception as exc:
            td_records = [{"source": "post_market_td_export", "status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}]
            manifest["td_export_error"] = td_records[0]["error"]
    else:
        td_records = []
        if not tool_frozen:
            manifest["partial_evidence"].append("capture_tool_freeze")
        if not release_valid:
            manifest["partial_evidence"].append("release_provenance")
    manifest["artifact_inventory"].extend(record for record in td_records if record.get("relative_path"))
    manifest["td_export_status"] = "PASS" if td_records and all(record.get("relative_path") for record in td_records) else "PARTIAL"
    manifest["runtime_identity_stable"] = bool(manifest.get("runtime_identity_stable"))
    manifest["release_provenance_valid"] = release_valid
    slots = {slot.get("name"): slot for slot in manifest.get("slots", [])}
    timing_ok = bool(slots) and all(slot.get("status") in {"ON_TIME", "LATE"} for slot in slots.values())
    required_names = {
        "mapping_security", "auction_0920", "auction_0924", "auction_0925",
        *(f"online_q2_{clock.replace(':', '')}" for clock in Q2_SLOTS),
    }
    required_slots_ok = all(
        name in slots and slots[name].get("status") in {"ON_TIME", "LATE"}
        and slots[name].get("artifacts")
        for name in required_names
    )
    anchor_ok = any(
        item.get("relative_path") == "auction_anchor.json" and item.get("anchor_present")
        for item in manifest["artifact_inventory"]
    )
    td_ok = bool(td_records) and all(record.get("relative_path") and record.get("row_count", 0) > 0 for record in td_records)
    manifest["td_export_status"] = "PASS" if td_ok else "PARTIAL"
    valid_after, errors_after = inventory_valid(manifest, root)
    if errors_after:
        manifest["integrity_errors_after_seal"] = errors_after
    manifest["gate_results"] = {
        "slot_timing_integrity": "PASS" if timing_ok and required_slots_ok else "FAIL",
        "runtime_identity_stable": "PASS" if manifest.get("runtime_identity_stable") else "FAIL",
        "release_provenance_valid": "PASS" if release_valid else "FAIL",
        "capture_tool_freeze": "PASS" if tool_frozen else "FAIL",
        "mapping_capture": "PASS" if slots.get("mapping_security", {}).get("status") in {"ON_TIME", "LATE"} else "FAIL",
        "auction_0920_capture": "PASS" if slots.get("auction_0920", {}).get("status") in {"ON_TIME", "LATE"} else "FAIL",
        "auction_0924_capture": "PASS" if slots.get("auction_0924", {}).get("status") in {"ON_TIME", "LATE"} else "FAIL",
        "auction_0925_capture": "PASS" if slots.get("auction_0925", {}).get("status") in {"ON_TIME", "LATE"} else "FAIL",
        "auction_anchor_capture": "PASS" if anchor_ok else "FAIL",
        "online_q2_capture": "PASS" if required_slots_ok else "FAIL",
        "td_export": "PASS" if td_ok else "PARTIAL",
        "artifact_integrity": "PASS" if valid_before and valid_after else "FAIL",
        "manifest_sealed": "PASS",
        "capture_write_isolation": "PASS" if not any(manifest.get("write_isolation", {}).values()) else "FAIL",
    }
    complete = all(value == "PASS" for value in manifest["gate_results"].values())
    manifest.update({
        "missing_artifacts": sorted(set(manifest.get("missing_artifacts", []))),
        "sealed": True,
        "sealed_at": now_local().isoformat(timespec="seconds"),
        "seal_status": "PASS" if complete else "FAIL",
        "ground_truth_capture_complete": "PASS" if complete else "FAIL",
        "st_limit_metadata_capture": "PARTIAL",
        "online_ls_equivalence": "NOT_EVALUATED",
    })
    atomic_write_json(sealed_path, manifest)
    for path in root.iterdir():
        if path.is_file():
            path.chmod(0o444)
    root.chmod(0o555)
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="command", required=True)
    for name, function in (("capture", capture), ("seal", seal)):
        command = sub.add_parser(name)
        command.add_argument("--trade-date", required=True)
        command.add_argument("--output-dir", required=True, type=Path)
        command.set_defaults(function=function)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.function(args))
    except (CaptureError, OSError, ValueError) as exc:
        print(f"ground_truth_capture: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
