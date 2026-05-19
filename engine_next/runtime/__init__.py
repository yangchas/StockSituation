"""Runtime specs and timelines.

This package keeps __init__ lightweight to avoid circular imports between
runtime orchestration and offline execution modules.
"""

from importlib import import_module

__all__ = [
    "IntradayDataHub",
    "IntradayFetchResult",
    "IntradayContextBuilder",
    "IntradayContextRequest",
    "TickWindowMetrics",
    "TickWindowTracker",
    "PremarketReadinessService",
    "RuntimeStartupCoordinator",
    "StartupCoordinationPlan",
    "StartupCoordinatorRequest",
    "StartupExecutionBundle",
    "StartupSelfCheckRequest",
    "StartupSelfCheckService",
    "infer_run_phase",
]

_EXPORT_MAP = {
    "IntradayDataHub": ("engine_next.runtime.intraday_data_hub", "IntradayDataHub"),
    "IntradayFetchResult": ("engine_next.runtime.intraday_data_hub", "IntradayFetchResult"),
    "IntradayContextBuilder": ("engine_next.runtime.intraday_context_builder", "IntradayContextBuilder"),
    "IntradayContextRequest": ("engine_next.runtime.intraday_context_builder", "IntradayContextRequest"),
    "TickWindowMetrics": ("engine_next.runtime.tick_window_tracker", "TickWindowMetrics"),
    "TickWindowTracker": ("engine_next.runtime.tick_window_tracker", "TickWindowTracker"),
    "PremarketReadinessService": ("engine_next.runtime.startup_self_check", "PremarketReadinessService"),
    "RuntimeStartupCoordinator": ("engine_next.runtime.startup_runtime_coordinator", "RuntimeStartupCoordinator"),
    "StartupCoordinationPlan": ("engine_next.runtime.startup_runtime_coordinator", "StartupCoordinationPlan"),
    "StartupCoordinatorRequest": ("engine_next.runtime.startup_runtime_coordinator", "StartupCoordinatorRequest"),
    "StartupExecutionBundle": ("engine_next.runtime.startup_runtime_coordinator", "StartupExecutionBundle"),
    "StartupSelfCheckRequest": ("engine_next.runtime.startup_self_check", "StartupSelfCheckRequest"),
    "StartupSelfCheckService": ("engine_next.runtime.startup_self_check", "StartupSelfCheckService"),
    "infer_run_phase": ("engine_next.runtime.startup_self_check", "infer_run_phase"),
}


def __getattr__(name: str):
    if name not in _EXPORT_MAP:
        raise AttributeError(f"module 'engine_next.runtime' has no attribute {name!r}")
    module_name, attr_name = _EXPORT_MAP[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
