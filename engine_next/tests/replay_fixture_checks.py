"""Tracked checks for the formal replay dependency closure."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from engine_next.domain.enums import RunPhase
from engine_next.runtime.intraday_context_builder import IntradayContextBuilder
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.runtime.replay_fixture import ReplayClock, ReplayRedisView
from tools.qmt_replay import run_engine_next


SHANGHAI = ZoneInfo("Asia/Shanghai")


def _builder(view: ReplayRedisView) -> IntradayContextBuilder:
    return IntradayContextBuilder(intraday_hub=IntradayDataHub(redis_client=view))


def test_replay_clock_is_monotonic() -> None:
    clock = ReplayClock(datetime(2026, 8, 21, 9, 15, tzinfo=SHANGHAI))
    clock._set(datetime(2026, 8, 21, 9, 16, tzinfo=SHANGHAI))
    with pytest.raises(ValueError, match="cannot move backwards"):
        clock._set(datetime(2026, 8, 21, 9, 15, 1, tzinfo=SHANGHAI))


def test_missing_hot_rank_fixture_fails_before_external_refresh() -> None:
    view = ReplayRedisView()
    builder = _builder(view)
    with patch.object(builder.hub, "fetch_hot_rank", side_effect=AssertionError("network")):
        with pytest.raises(RuntimeError, match="missing or stale"):
            builder._ensure_hot_rank_cache(
                "2026-08-21",
                RunPhase.AUCTION,
                now=datetime(2026, 8, 21, 9, 25, tzinfo=SHANGHAI),
            )


def test_stale_hot_rank_fixture_fails_before_external_refresh() -> None:
    view = ReplayRedisView(
        hashes={"cache:hot_rank:2026-08-21": {"000001": "1"}},
        strings={
            "cache:hot_rank_meta:2026-08-21": json.dumps(
                {"updated_at_ts": 1787000000, "source": "fixture"}
            )
        },
    )
    builder = _builder(view)
    with patch.object(builder.hub, "fetch_hot_rank", side_effect=AssertionError("network")):
        with pytest.raises(RuntimeError, match="missing or stale"):
            builder._ensure_hot_rank_cache(
                "2026-08-21",
                RunPhase.AUCTION,
                now=datetime(2026, 8, 21, 9, 25, tzinfo=SHANGHAI),
            )


def test_fresh_fixture_uses_replay_clock_without_external_refresh() -> None:
    now = datetime(2026, 8, 21, 9, 25, tzinfo=SHANGHAI)
    view = ReplayRedisView(
        hashes={"cache:hot_rank:2026-08-21": {"000001": "1"}},
        strings={
            "cache:hot_rank_meta:2026-08-21": json.dumps(
                {"updated_at_ts": int(now.timestamp()), "source": "fixture"}
            )
        },
    )
    builder = _builder(view)
    with patch.object(builder.hub, "fetch_hot_rank", side_effect=AssertionError("network")):
        builder._ensure_hot_rank_cache("2026-08-21", RunPhase.AUCTION, now=now)


def test_replay_runner_restores_intraday_hub_references_on_failure() -> None:
    original_context = run_engine_next.context_pipeline.IntradayDataHub
    original_local = run_engine_next.local_decision_layer.IntradayDataHub
    with TemporaryDirectory() as directory:
        root = Path(directory)
        fixture = root / "fixture.json"
        fixture.write_text(
            json.dumps(
                {
                    "fixture_id": "closure-test",
                    "trade_date": "2026-08-21",
                    "previous_trade_date": "2026-08-20",
                    "hashes": {"cache:hot_rank:2026-08-21": {"000001": "1"}},
                    "strings": {
                        "cache:hot_rank_meta:2026-08-21": json.dumps(
                            {"updated_at_ts": 1787275501, "source": "fixture"}
                        )
                    },
                    "sets": {},
                }
            ),
            encoding="utf-8",
        )
        q2 = root / "empty.jsonl"
        q2.write_text("", encoding="utf-8")
        with pytest.raises(ValueError, match="Q2Frame input is empty"):
            run_engine_next.run_replay(
                q2_path=q2,
                static_fixture=fixture,
                output_path=root / "ledger.jsonl",
                symbols=("000001",),
                trade_date="2026-08-21",
                previous_trade_date="2026-08-20",
            )
    assert run_engine_next.context_pipeline.IntradayDataHub is original_context
    assert run_engine_next.local_decision_layer.IntradayDataHub is original_local


def test_replay_does_not_report_production_scheduler_events() -> None:
    assert not hasattr(run_engine_next, "_events_due")
    assert run_engine_next.REPLAY_CHECKPOINTS[2] == (9, 25, "0925")
