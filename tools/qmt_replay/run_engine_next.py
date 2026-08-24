"""Run the existing engine_next strategy entry on Q2Frame replay input.

This is a replay frontend only.  It injects the existing IntradayDataHub with
ReplayRedisView, then calls the same AuctionRuntimeController strategy-state
builder used by the live runtime.  It does not create a second strategy path
and it never opens a production Redis/TDengine/RabbitMQ connection.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from engine_next.domain.enums import RunPhase
from engine_next.runtime.controllers.auction_runtime_controller import AuctionRuntimeController
from engine_next.runtime.intraday_context_builder import IntradayContextBuilder, IntradayContextRequest
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.runtime.replay_fixture import ReplayClock, ReplayRedisView, iter_q2frames
from engine_next.strategy_skill_layer import context_pipeline, local_decision_layer


SHANGHAI = ZoneInfo("Asia/Shanghai")
EVIDENCE_CONTRACT_VERSION = "AuctionPhase1A"
EVENT_MINUTES = ((9, 20, "auction_preview_0920"), (9, 24, "auction_preview_0924"), (9, 25, "auction_finalize_0925"), (9, 26, "auction_followup_0926"), (9, 30, "intraday_open_0930"))


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(item) for item in value), key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "value") and not isinstance(value, (str, bytes, int, float, bool)):
        return _jsonable(value.value)
    return value


def _compact_json(value: Any) -> bytes:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _git_commit() -> str:
    try:
        return subprocess.check_output(("git", "rev-parse", "HEAD"), text=True).strip()
    except Exception:
        return "unknown"


def _git_worktree_dirty() -> bool:
    try:
        return bool(subprocess.check_output(("git", "status", "--porcelain"), text=True).strip())
    except Exception:
        return True


def _git_source_state_sha256() -> str:
    """Identify the actual relevant source state, including uncommitted files."""

    scopes = ("C/t1_v2", "engine_next", "tools/qmt_replay")
    try:
        digest = hashlib.sha256()
        digest.update(_git_commit().encode("ascii"))
        digest.update(
            subprocess.check_output(
                ("git", "diff", "--binary", "HEAD", "--", *scopes),
                stderr=subprocess.DEVNULL,
            )
        )
        untracked = subprocess.check_output(
            ("git", "ls-files", "--others", "--exclude-standard", "--", *scopes),
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
        for name in sorted(item.strip() for item in untracked if item.strip()):
            path = Path(name)
            if not path.is_file():
                continue
            digest.update(name.replace("\\", "/").encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return digest.hexdigest()
    except Exception:
        return "unknown"


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _attach_anchor_timestamps(
    evidence: tuple[dict[str, Any], ...],
    rows: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Attach the real Q2 logical timestamp without inventing wall-clock data."""

    timestamps = {
        str(row.get("symbol") or "").strip(): int(row.get("timestamp_ms") or 0)
        for row in rows
        if str(row.get("symbol") or "").strip() and int(row.get("timestamp_ms") or 0) > 0
    }
    return tuple(
        {**row, **({"timestamp_ms": timestamps[str(row.get("symbol") or "").strip()]} if str(row.get("symbol") or "").strip() in timestamps else {})}
        for row in evidence
    )


def _phase_for(now: datetime) -> tuple[RunPhase, str]:
    if now.strftime("%H:%M") < "09:30":
        return RunPhase.AUCTION, "auction"
    return RunPhase.INTRADAY, "intraday"


def _minute_index(now: datetime) -> int:
    return max((now.hour * 60 + now.minute) - (9 * 60 + 15), 0)


def _events_due(now: datetime, fired: set[str]) -> list[dict[str, Any]]:
    due: list[dict[str, Any]] = []
    for hour, minute, name in EVENT_MINUTES:
        if (hour, minute) <= (now.hour, now.minute) and name not in fired:
            fired.add(name)
            due.append({"name": name, "scheduled_local": f"{hour:02d}:{minute:02d}"})
    return due


def _load_fixture(path: Path) -> tuple[dict[str, Any], ReplayRedisView]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("static fixture must be a JSON object")
    if payload.get("format") == "EngineNextStaticFactSnapshotV1":
        facts = payload.get("facts")
        if not isinstance(facts, list):
            raise ValueError("static fact snapshot facts must be a list")
        hashes: dict[str, dict[str, Any]] = {}
        strings: dict[str, Any] = {}
        sets: dict[str, list[Any]] = {}
        for fact in facts:
            if not isinstance(fact, dict):
                raise ValueError("static fact snapshot entry must be an object")
            key = str(fact.get("key") or "").strip()
            redis_type = str(fact.get("redis_type") or "").strip()
            if not key:
                raise ValueError("static fact snapshot entry has empty key")
            if redis_type == "hash":
                value = fact.get("value")
                if not isinstance(value, dict):
                    raise ValueError(f"hash fact must be an object: {key}")
                hashes[key] = value
            elif redis_type == "string":
                strings[key] = fact.get("value")
            elif redis_type == "set":
                value = fact.get("value")
                if not isinstance(value, list):
                    raise ValueError(f"set fact must be a list: {key}")
                sets[key] = value
            else:
                raise ValueError(
                    f"replay static facts support only hash/string/set; got {redis_type!r} for {key}"
                )
        return payload, ReplayRedisView(hashes=hashes, strings=strings, sets=sets)
    hashes = payload.get("hashes", {})
    strings = payload.get("strings", {})
    sets = payload.get("sets", {})
    if not all(isinstance(item, dict) for item in (hashes, strings, sets)):
        raise ValueError("static fixture hashes/strings/sets must be objects")
    return payload, ReplayRedisView(hashes=hashes, strings=strings, sets=sets)


def run_replay(
    *,
    q2_path: Path,
    static_fixture: Path,
    output_path: Path,
    symbols: tuple[str, ...],
    trade_date: str,
    previous_trade_date: str,
    shadow_output_path: Path | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    fixture_payload, view = _load_fixture(static_fixture)
    hot_rank_key = f"cache:hot_rank:{trade_date}"
    hot_rank_meta_key = f"cache:hot_rank_meta:{trade_date}"
    if not view.hlen(hot_rank_key) or not view.get(hot_rank_meta_key):
        raise RuntimeError(
            "replay static fixture must provide hot-rank cache and metadata; "
            "otherwise the production hot-rank fetch path would be external"
        )
    hub = IntradayDataHub(redis_client=view)
    # A replay-only frontend substitution keeps legacy strategy helpers that
    # construct IntradayDataHub() internally on the same read-only view.  The
    # production modules are not changed and the strategy does not branch on
    # replay mode or platform.
    def replay_hub_factory(*_args: Any, **_kwargs: Any) -> IntradayDataHub:
        return hub

    context_pipeline.IntradayDataHub = replay_hub_factory
    local_decision_layer.IntradayDataHub = replay_hub_factory
    builder = IntradayContextBuilder(intraday_hub=hub)
    controller = AuctionRuntimeController(intraday_hub=hub)
    frames = list(iter_q2frames(q2_path))
    if not frames:
        raise ValueError("Q2Frame input is empty")
    first_frame = frames[0]
    first_ts = int(first_frame.get("logical_ts_ms", 0))
    if first_ts <= 0:
        raise ValueError("first Q2Frame has invalid logical_ts_ms")
    q2_sha256 = _file_sha256(q2_path)
    static_fixture_sha256 = _file_sha256(static_fixture)
    git_commit = _git_commit()
    git_worktree_dirty = _git_worktree_dirty()
    source_state_sha256 = _git_source_state_sha256()
    evidence_run_id = str(run_id or f"replay-{trade_date}-{q2_sha256[:12]}")
    clock = ReplayClock(datetime.fromtimestamp(first_ts / 1000.0, SHANGHAI))
    fired_events: set[str] = set()
    records: list[bytes] = []
    total_frames = 0
    first_local = ""
    last_local = ""
    for frame in frames:
        frame_ts = int(frame.get("logical_ts_ms", 0))
        if frame_ts <= 0:
            raise ValueError(f"invalid logical_ts_ms at seq={frame.get('seq_no')}")
        logical_now = datetime.fromtimestamp(frame_ts / 1000.0, SHANGHAI)
        if logical_now.tzinfo != SHANGHAI:
            raise ValueError("replay time must use Asia/Shanghai")

        # Fixed order: validate timestamp -> advance clock -> apply frame.
        if frame_ts < view.last_q2_logical_ts_ms:
            raise ValueError("Q2Frame logical_ts_ms moved backwards")
        clock._set(logical_now)
        view.apply_q2_frame(frame)
        phase, phase_label = _phase_for(clock.now())
        events = _events_due(clock.now(), fired_events)
        request = IntradayContextRequest(
            phase=phase,
            trade_date=trade_date,
            previous_trade_date=previous_trade_date,
            offline_context_date=previous_trade_date,
            symbols=symbols,
            now=clock.now(),
            minute_index=_minute_index(clock.now()),
        )
        primed = builder.prime_runtime_state(request)
        context = builder.build_from_primed(primed)
        # Strategy layer itself stays unchanged; these notes disable only its
        # existing derived-cache writebacks for the read-only replay view.
        context = dataclasses.replace(
            context,
            notes=(*context.notes, "runtime_writeback=0", "temporal_memory_write=0"),
        )
        state = controller._build_console_state(
            context,
            min_confidence=(60 if phase is RunPhase.INTRADAY else 60),
            phase_label=phase_label,
            minute_tag=clock.now().strftime("%H:%M"),
        )
        if state.missing_inputs:
            raise RuntimeError(
                "replay static fixture is missing required strategy inputs "
                f"at seq={frame['seq_no']}: {','.join(state.missing_inputs)}"
            )
        # Exercise the public live runtime wrapper as well.  It delegates to
        # the same state builder; its auction/intraday gating remains the
        # production behavior and its console lines are intentionally not part
        # of the business ledger.
        if phase is RunPhase.AUCTION:
            controller.render_auction_runtime_loop(
                intraday_context=context,
                runtime_readiness_label="trade_ready_runtime",
                symbols=len(symbols),
                quotes=len(primed.quote_rows),
                native=int(primed.native_ingested),
                now=clock.now(),
            )
        else:
            controller.render_intraday_runtime_loop(
                intraday_context=context,
                runtime_readiness_label="trade_ready_runtime",
                symbols=len(symbols),
                quotes=len(primed.quote_rows),
                native=int(primed.native_ingested),
                now=clock.now(),
            )
        bundle = state.bundle
        record = {
            "seq_no": int(frame["seq_no"]),
            "logical_ts_ms": frame_ts,
            "phase": phase.value,
            "events": events,
            "coverage_scope": state.coverage_scope,
            "actual_source": state.actual_source,
            "missing_inputs": state.missing_inputs,
            "focus_symbols": tuple(getattr(bundle, "focus_symbols", ()) or ()) if bundle else (),
            "decisions": tuple(getattr(bundle, "decisions", ()) or ()) if bundle else (),
            "decision_bundle": getattr(bundle, "decision_bundle", None) if bundle else None,
        }
        records.append(_compact_json(record))
        total_frames += 1
        first_local = first_local or clock.now().isoformat()
        last_local = clock.now().isoformat()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"\n".join(records) + b"\n")
    l2 = hashlib.sha256(output_path.read_bytes()).hexdigest()
    shadow_manifest: dict[str, Any] = {}
    if shadow_output_path is not None:
        # Shadow evidence is an optional validation output. Keep its imports
        # lazy so the core Q2Frame -> DecisionLedger replay closure does not
        # depend on validation-only modules.
        from engine_next.runtime.auction_shadow import build_anchor_shadow_evidence
        from tools.qmt_replay.audit_pre_0925 import _q2_anchor_rows

        anchor_rows = _q2_anchor_rows(frames)
        evidence_0920_0924 = build_anchor_shadow_evidence(
            (*anchor_rows["0920"], *anchor_rows["0924"]),
            from_tag="0920",
            to_tag="0924",
        )
        evidence_0924_0925 = build_anchor_shadow_evidence(
            (*anchor_rows["0924"], *anchor_rows["0925"]),
            from_tag="0924",
            to_tag="0925",
        )
        shadow_payload = {
            "format": "AuctionShadowEvidenceV1",
            "source": "q2frame_existing_anchor_fields",
            "data_origin": "replay_generated",
            "contract_version": EVIDENCE_CONTRACT_VERSION,
            "git_commit": git_commit,
            "git_worktree_dirty": git_worktree_dirty,
            "source_state_sha256": source_state_sha256,
            "trade_date": trade_date,
            "run_id": evidence_run_id,
            "input_sha256": {
                "q2frame": q2_sha256,
                "static_fixture": static_fixture_sha256,
            },
            "evidence": {
                "0920_to_0924": _attach_anchor_timestamps(evidence_0920_0924, anchor_rows["0924"]),
                "0924_to_0925": _attach_anchor_timestamps(evidence_0924_0925, anchor_rows["0925"]),
            },
        }
        shadow_output_path.parent.mkdir(parents=True, exist_ok=True)
        shadow_output_path.write_bytes(_compact_json(shadow_payload) + b"\n")
        shadow_manifest = {
            "shadow_output": str(shadow_output_path),
            "shadow_sha256": _file_sha256(shadow_output_path),
            "shadow_format": shadow_payload["format"],
        }
    accessed_keys_path = output_path.with_suffix(output_path.suffix + ".accessed_keys.json")
    accessed_keys_path.write_bytes(_compact_json({"keys": view.accessed_keys}) + b"\n")
    manifest = {
        "git_commit": git_commit,
        "git_worktree_dirty": git_worktree_dirty,
        "source_state_sha256": source_state_sha256,
        "data_origin": "replay_generated",
        "contract_version": EVIDENCE_CONTRACT_VERSION,
        "run_id": evidence_run_id,
        "trade_date": trade_date,
        "previous_trade_date": previous_trade_date,
        "fixture_id": fixture_payload.get("fixture_id", static_fixture.stem),
        "fixture_source": fixture_payload.get("source", "unknown"),
        "fixture_format": fixture_payload.get("format", "contract_fixture_v1"),
        "fixture_provenance": fixture_payload.get("provenance"),
        "q2_sha256": q2_sha256,
        "static_fixture_sha256": static_fixture_sha256,
        "q2_path": str(q2_path),
        "static_fixture": str(static_fixture),
        "record_count": total_frames,
        "first_logical_local": first_local,
        "last_logical_local": last_local,
        "l2_sha256": l2,
        "accessed_keys": str(accessed_keys_path),
        "triggered_events": sorted(fired_events),
    }
    manifest.update(shadow_manifest)
    manifest_path = output_path.with_suffix(output_path.suffix + ".manifest.json")
    manifest_path.write_bytes(_compact_json(manifest) + b"\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Run existing engine_next strategy on Q2Frame replay")
    parser.add_argument("--q2", type=Path, required=True)
    parser.add_argument("--static-fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trade-date", default="2026-08-21")
    parser.add_argument("--previous-trade-date", default="2026-08-20")
    parser.add_argument("--run-id", help="optional provenance id; default is deterministic from trade_date and Q2 hash")
    parser.add_argument("--symbols", nargs="+", default=("000002", "300059", "600519"))
    parser.add_argument(
        "--shadow-output",
        type=Path,
        help="write separate auction Shadow evidence; it is never included in the business ledger",
    )
    args = parser.parse_args()
    manifest = run_replay(
        q2_path=args.q2,
        static_fixture=args.static_fixture,
        output_path=args.output,
        symbols=tuple(args.symbols),
        trade_date=args.trade_date,
        previous_trade_date=args.previous_trade_date,
        shadow_output_path=args.shadow_output,
        run_id=args.run_id,
    )
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
