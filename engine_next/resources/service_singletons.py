from __future__ import annotations

from typing import Any

from engine_next.adapters.runtime_chip_runner import get_shared_runtime_chip_runner


def get_shared_chip_batch_runner() -> Any:
    return get_shared_runtime_chip_runner()
