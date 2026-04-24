from __future__ import annotations

from engine_next.domain.enums import RunPhase
from engine_next.domain.models import RuntimeEventSpec, RuntimeTimelineSummary


ORIGINAL_ENGINE_V2_TIMELINE = RuntimeTimelineSummary(
    events=[
        RuntimeEventSpec(
            time_window="00:00-09:25",
            phase=RunPhase.PREMARKET,
            component="DataLifecycle.on_startup",
            action="sync metadata, sync previous-trade-day offline data, trigger factor pipeline",
            source_refs=("engine_v2/v2_data_lifecycle.py",),
            notes="00:00-15:00 metadata sync; 00:00-09:25 also belongs to offline reconciliation window.",
        ),
        RuntimeEventSpec(
            time_window="01:00-08:30",
            phase=RunPhase.NIGHT,
            component="DataLifecycle.on_startup",
            action="spawn recap audit for previous trade day",
            source_refs=("engine_v2/v2_data_lifecycle.py",),
            notes="Recommended cross-day recap window in the original code.",
        ),
        RuntimeEventSpec(
            time_window="08:30",
            phase=RunPhase.PREMARKET,
            component="Orchestrator.run_guardian",
            action="reset auction state, run startup lifecycle, load yesterday limit-ups",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="09:00",
            phase=RunPhase.PREMARKET,
            component="Orchestrator.run_guardian",
            action="same as 08:30 second startup checkpoint",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="09:15-09:20",
            phase=RunPhase.AUCTION,
            component="t1.cpp",
            action="trial auction accumulation window",
            source_refs=("C/t1.cpp", "C/stock_analysis.h"),
            notes="Auction start and trial end window used by C++ collector.",
        ),
        RuntimeEventSpec(
            time_window="09:20:03-09:23:59",
            phase=RunPhase.AUCTION,
            component="t1.cpp",
            action="emit 09:20 auction summary and persist market:auction:{date}:0920/latest",
            source_refs=("C/t1.cpp",),
        ),
        RuntimeEventSpec(
            time_window="09:24:10-09:24:59",
            phase=RunPhase.AUCTION,
            component="t1.cpp",
            action="emit 09:24 auction summary and persist market:auction:{date}:0924/latest",
            source_refs=("C/t1.cpp",),
        ),
        RuntimeEventSpec(
            time_window="09:25:10-09:29:59",
            phase=RunPhase.AUCTION,
            component="t1.cpp",
            action="emit 09:25 anchor summary and persist market:auction:{date}:0925/latest",
            source_refs=("C/t1.cpp",),
            notes="Contains fallback logic if 09:25 snapshot was not emitted on time.",
        ),
        RuntimeEventSpec(
            time_window="09:25+",
            phase=RunPhase.AUCTION,
            component="Orchestrator.startup repair",
            action="if auction snapshot missing and not too early, force execute_analysis in AUCTION mode",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="09:26",
            phase=RunPhase.AUCTION,
            component="Orchestrator.run_guardian",
            action="refresh yesterday limit pool and run auction analysis",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="09:15-11:50 / 12:55-15:10",
            phase=RunPhase.INTRADAY,
            component="Orchestrator._v2_tick_pump",
            action="poll Redis quotes every ~3s, sync Kaipan hotspots, push ticks into Rust bridge",
            source_refs=("engine_v2/v2_orc_final.py",),
            notes="This is the main high-frequency path; original code already performs controlled intraday network requests here.",
        ),
        RuntimeEventSpec(
            time_window="09:30-15:00",
            phase=RunPhase.INTRADAY,
            component="Orchestrator.run_guardian",
            action="run intraday analysis every 3 minutes near the first 5 seconds of the minute",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="09:30-09:35",
            phase=RunPhase.INTRADAY,
            component="RiskSentinel",
            action="opening stop-loss path",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="09:35-15:00",
            phase=RunPhase.INTRADAY,
            component="RiskSentinel",
            action="intraday tracking path with spike reversal, plunge speed, and limit-open checks every 15s",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="11:25-11:40",
            phase=RunPhase.INTRADAY,
            component="Orchestrator.execute_analysis",
            action="special midday handling branch",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="14:57-15:00",
            phase=RunPhase.INTRADAY,
            component="C++ market utils",
            action="closing auction window",
            source_refs=("C/utils.cpp", "C/stock_analysis.h"),
        ),
        RuntimeEventSpec(
            time_window="15:05",
            phase=RunPhase.POSTMARKET,
            component="Orchestrator.run_guardian",
            action="mark market close and slow down loop",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
        RuntimeEventSpec(
            time_window="17:40",
            phase=RunPhase.POSTMARKET,
            component="Orchestrator + DataLifecycle",
            action="preload watermarks, run EOD lifecycle, spawn final retro script",
            source_refs=("engine_v2/v2_orc_final.py", "engine_v2/v2_data_lifecycle.py"),
            notes="Original system aligns offline settlement to 17:40 to wait for Baostock daily data.",
        ),
        RuntimeEventSpec(
            time_window="17:40+",
            phase=RunPhase.POSTMARKET,
            component="RecapEngine",
            action="load signal snapshot, compare Kaipan yesterday bans/hot plates with Wencai today limit truth, build recap report",
            source_refs=("engine_v2/v2_recap_engine_final.py", "engine_v2/v2_recap_final_auto.py"),
            notes="This is the main postmarket full-collision recap pipeline in the original system.",
        ),
        RuntimeEventSpec(
            time_window="night mode",
            phase=RunPhase.NIGHT,
            component="Orchestrator.run_guardian",
            action="emit rotation message and nightly heartbeat",
            source_refs=("engine_v2/v2_orc_final.py",),
        ),
    ]
)


def iter_events() -> list[RuntimeEventSpec]:
    return list(ORIGINAL_ENGINE_V2_TIMELINE.events)


def iter_phase_events(phase: RunPhase) -> list[RuntimeEventSpec]:
    return [event for event in ORIGINAL_ENGINE_V2_TIMELINE.events if event.phase == phase]
