"""Small replay-only adapters for existing engine_next injection points.

This module deliberately does not introduce a new provider hierarchy. It gives
the existing IntradayDataHub/EngineApp a read-only Redis-shaped view and a
logical clock for deterministic local replay.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


class ReplayClock:
    """Replay-only clock; strategy code can read now(), runner owns _set()."""

    def __init__(self, initial: datetime) -> None:
        self._now = initial

    def now(self) -> datetime:
        return self._now

    def _set(self, value: datetime) -> None:
        if value < self._now:
            raise ValueError("replay clock cannot move backwards")
        self._now = value


class _ReplayPipeline:
    def __init__(self, view: "ReplayRedisView") -> None:
        self._view = view
        self._operations: list[tuple[str, tuple[Any, ...]]] = []

    def hgetall(self, key: str) -> "_ReplayPipeline":
        self._operations.append(("hgetall", (key,)))
        return self

    def hmget(self, key: str, fields: Iterable[str]) -> "_ReplayPipeline":
        self._operations.append(("hmget", (key, tuple(fields))))
        return self

    def execute(self) -> list[Any]:
        result: list[Any] = []
        for name, args in self._operations:
            if name == "hgetall":
                result.append(self._view.hgetall(*args))
            else:
                result.append(self._view.hmget(*args))
        return result


class ReplayRedisView:
    """Read-only Redis-shaped fixture view used only by replay."""

    replay_read_only = True

    def __init__(
        self,
        *,
        hashes: dict[str, dict[str, Any]] | None = None,
        strings: dict[str, Any] | None = None,
        sets: dict[str, Iterable[Any]] | None = None,
    ) -> None:
        self._hashes = {str(key): dict(value) for key, value in (hashes or {}).items()}
        self._strings = {str(key): value for key, value in (strings or {}).items()}
        self._sets = {str(key): set(value) for key, value in (sets or {}).items()}
        self._sorted_sets: dict[str, dict[str, float]] = {}
        self._accessed_keys: set[str] = set()
        self.last_q2_seq_no = 0
        self.last_q2_logical_ts_ms = 0

    @property
    def accessed_keys(self) -> tuple[str, ...]:
        """Exact Redis keys read by the replay path, in deterministic order."""
        return tuple(sorted(self._accessed_keys))

    @property
    def fixture_keys(self) -> tuple[str, ...]:
        """Keys present in the fixture without marking them as runtime reads."""
        return tuple(sorted({*self._hashes, *self._strings, *self._sets, *self._sorted_sets}))

    def _record_read(self, key: str) -> None:
        self._accessed_keys.add(str(key))

    @classmethod
    def from_fixture(cls, path: str | Path) -> "ReplayRedisView":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("replay fixture must be an object")
        hashes = payload.get("hashes", {})
        strings = payload.get("strings", {})
        sets = payload.get("sets", {})
        if not isinstance(hashes, dict) or not isinstance(strings, dict) or not isinstance(sets, dict):
            raise ValueError("replay fixture hashes/strings/sets must be objects")
        return cls(hashes=hashes, strings=strings, sets=sets)

    def apply_q2_frame(self, frame: dict[str, Any], *, prefix: str = "q2:") -> None:
        if frame.get("version") != "Q2FrameV1":
            raise ValueError("unsupported Q2Frame version")
        seq_no = int(frame.get("seq_no", 0))
        logical_ts_ms = int(frame.get("logical_ts_ms", 0))
        if seq_no != self.last_q2_seq_no + 1:
            raise ValueError("Q2Frame seq_no is not continuous")
        if logical_ts_ms < self.last_q2_logical_ts_ms:
            raise ValueError("Q2Frame logical_ts_ms moved backwards")
        updates = frame.get("q2_updates", [])
        if not isinstance(updates, list):
            raise ValueError("Q2Frame q2_updates must be a list")
        for update in updates:
            if not isinstance(update, dict):
                raise ValueError("Q2Frame update must be an object")
            symbol = str(update.get("symbol", "")).strip()
            if len(symbol) != 6 or not symbol.isdigit():
                raise ValueError(f"invalid Q2 symbol: {symbol!r}")
            values = {str(key): value for key, value in update.items() if key != "symbol"}
            self._hashes.setdefault(f"{prefix}{symbol}", {}).update(values)
        self.last_q2_seq_no = seq_no
        self.last_q2_logical_ts_ms = logical_ts_ms

    def hgetall(self, key: str) -> dict[str, Any]:
        self._record_read(key)
        return dict(self._hashes.get(str(key), {}))

    def hget(self, key: str, field: str) -> Any | None:
        self._record_read(key)
        return self._hashes.get(str(key), {}).get(str(field))

    def hmget(self, key: str, fields: Iterable[str]) -> list[Any | None]:
        self._record_read(key)
        bucket = self._hashes.get(str(key), {})
        return [bucket.get(str(field)) for field in fields]

    def hkeys(self, key: str) -> list[str]:
        self._record_read(key)
        return list(self._hashes.get(str(key), {}).keys())

    def hlen(self, key: str) -> int:
        self._record_read(key)
        return len(self._hashes.get(str(key), {}))

    def get(self, key: str) -> Any | None:
        self._record_read(key)
        return self._strings.get(str(key))

    def smembers(self, key: str) -> set[Any]:
        self._record_read(key)
        return set(self._sets.get(str(key), set()))

    def zadd(self, key: str, mapping: dict[Any, float], *args: Any, **kwargs: Any) -> int:
        del args, kwargs
        text = str(key)
        if not text.startswith("cache:sector_flow:"):
            raise RuntimeError(f"replay fixture is read-only: zadd:{text}")
        bucket = self._sorted_sets.setdefault(text, {})
        added = 0
        for member, score in mapping.items():
            member_text = str(member)
            if member_text not in bucket:
                added += 1
            bucket[member_text] = float(score)
        return added

    def zremrangebyscore(self, key: str, minimum: float, maximum: float, *args: Any, **kwargs: Any) -> int:
        del args, kwargs
        text = str(key)
        if not text.startswith("cache:sector_flow:"):
            raise RuntimeError(f"replay fixture is read-only: zremrangebyscore:{text}")
        bucket = self._sorted_sets.setdefault(text, {})
        removed = [member for member, score in bucket.items() if float(minimum) <= score <= float(maximum)]
        for member in removed:
            bucket.pop(member, None)
        return len(removed)

    def zrange(
        self,
        key: str,
        start: int,
        stop: int,
        *,
        withscores: bool = False,
    ) -> list[Any]:
        self._record_read(key)
        bucket = self._sorted_sets.get(str(key), {})
        ordered = sorted(bucket.items(), key=lambda item: (item[1], item[0]))
        if stop == -1:
            selected = ordered[int(start):]
        else:
            selected = ordered[int(start): int(stop) + 1]
        return [(member, score) for member, score in selected] if withscores else [member for member, _ in selected]

    def expire(self, key: str, seconds: int, *args: Any, **kwargs: Any) -> bool:
        del seconds, args, kwargs
        if str(key).startswith("cache:sector_flow:"):
            return True
        raise RuntimeError(f"replay fixture is read-only: expire:{key}")

    def exists(self, key: str) -> bool:
        text = str(key)
        self._record_read(text)
        return text in self._hashes or text in self._strings or text in self._sets

    def scan_iter(self, match: str | None = None, count: int | None = None):
        del count
        keys = sorted({*self._hashes, *self._strings, *self._sets, *self._sorted_sets})
        if match is None or match == "*":
            self._accessed_keys.update(keys)
            yield from keys
            return
        if match.endswith("*"):
            prefix = match[:-1]
            matched = [key for key in keys if key.startswith(prefix)]
            self._accessed_keys.update(matched)
            yield from matched
            return
        matched = [key for key in keys if key == match]
        self._accessed_keys.update(matched)
        yield from matched

    def keys(self, pattern: str = "*") -> list[str]:
        return list(self.scan_iter(match=pattern))

    def pipeline(self) -> _ReplayPipeline:
        return _ReplayPipeline(self)

    def ping(self) -> bool:
        return True

    # Any write attempt is a hard failure: replay must not silently mutate a
    # local fixture or fall through to a production-shaped store.
    def __getattr__(self, name: str):
        if name in {"set", "hset", "hmset", "sadd", "delete", "expire", "hdel"}:
            def forbidden(*args: Any, **kwargs: Any) -> None:
                del args, kwargs
                raise RuntimeError(f"replay fixture is read-only: {name}")
            return forbidden
        raise AttributeError(name)


def iter_q2frames(path: str | Path):
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for line_no, line in enumerate(handle, 1):
            text = line.rstrip("\r\n")
            if not text:
                continue
            try:
                frame = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid Q2Frame JSON at line {line_no}") from exc
            if not isinstance(frame, dict):
                raise ValueError(f"Q2Frame line {line_no} is not an object")
            yield frame
