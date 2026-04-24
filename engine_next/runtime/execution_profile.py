from __future__ import annotations

from dataclasses import dataclass

from engine_next.domain.enums import ExecutionEnvironment


@dataclass(frozen=True)
class ExecutionProfile:
    environment: ExecutionEnvironment
    allow_network_fetch: bool
    allow_redis_access: bool
    allow_tdengine_access: bool
    allow_runtime_jobs: bool
    notes: str = ""


LOCAL_WINDOWS_PROFILE = ExecutionProfile(
    environment=ExecutionEnvironment.LOCAL_WINDOWS,
    allow_network_fetch=False,
    allow_redis_access=False,
    allow_tdengine_access=False,
    allow_runtime_jobs=False,
    notes="Local Windows profile is edit-only. Network, Redis, TDengine, and runtime jobs stay disabled by default.",
)


SERVER_PROFILE = ExecutionProfile(
    environment=ExecutionEnvironment.SERVER,
    allow_network_fetch=True,
    allow_redis_access=True,
    allow_tdengine_access=True,
    allow_runtime_jobs=True,
    notes="Server profile enables real connectors, Redis, TDengine, and runtime jobs for production data flow.",
)


def get_default_execution_profile(environment: ExecutionEnvironment) -> ExecutionProfile:
    if environment == ExecutionEnvironment.SERVER:
        return SERVER_PROFILE
    return LOCAL_WINDOWS_PROFILE
