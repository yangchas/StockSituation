from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys
from typing import Any

logger = logging.getLogger(__name__)


class EngineNextRustCoreAdapter:
    """
    Placeholder adapter for the future engine_next Rust runtime.

    The real compiled extension is not wired in yet. This object exists so the
    rest of the Python code can target one stable adapter shape while the
    migration is still in progress.
    """

    def __init__(self) -> None:
        self.engine: Any | None = None
        self._load_binary()

    def _load_binary(self) -> None:
        try:
            import engine_next_core

            self.engine = engine_next_core.MarketEngine()
            return
        except Exception as exc:
            logger.debug("engine_next Rust core direct import unavailable: %s", exc)

        for candidate in self._binary_candidates():
            module = self._load_module_from_candidate(candidate)
            if module is not None:
                try:
                    self.engine = module.MarketEngine()
                    logger.info("Loaded engine_next Rust core from %s", candidate)
                    return
                except Exception as exc:
                    logger.debug("engine_next Rust core candidate failed to initialize: %s", exc)

        self.engine = None
        logger.debug("engine_next Rust core is unavailable after probing local binary candidates")

    def _binary_candidates(self) -> list[Path]:
        base_dir = Path(__file__).resolve().parent / "native" / "target" / "release"
        return [
            base_dir / "engine_next_core.so",
            base_dir / "libengine_next_core.so",
            base_dir / "engine_next_core.pyd",
        ]

    def _load_module_from_candidate(self, candidate: Path) -> Any | None:
        if not candidate.exists():
            return None
        try:
            spec = importlib.util.spec_from_file_location("engine_next_core", candidate)
            if spec is None or spec.loader is None:
                return None
            module = importlib.util.module_from_spec(spec)
            sys.modules.setdefault("engine_next_core", module)
            spec.loader.exec_module(module)
            return module
        except Exception as exc:
            logger.debug("Failed loading engine_next Rust core from %s: %s", candidate, exc)
            return None

    def register_symbols(self, symbols: list[str]) -> None:
        if self.engine is None:
            raise NotImplementedError("engine_next Rust core is not available")
        self.engine.register_symbols(symbols)

    def register_plate_mapping(self, plate_id: str, symbols: list[str]) -> None:
        if self.engine is None:
            raise NotImplementedError("engine_next Rust core is not available")
        self.engine.register_plate_mapping(plate_id, symbols)

    def push_tick_raw(
        self,
        symbol: str,
        price: float,
        amount: float,
        volume: float,
        time_str: str = "00:00:00",
        bid_amount: float = 0.0,
    ) -> None:
        if self.engine is None:
            raise NotImplementedError("engine_next Rust core is not available")
        self.engine.push_tick(symbol, price, amount, volume, time_str, bid_amount)

    def get_snapshot(self) -> dict[str, Any]:
        if self.engine is None:
            return {}
        snapshot = self.engine.get_snapshot()
        return snapshot if isinstance(snapshot, dict) else {}


engine_next_core_bridge = EngineNextRustCoreAdapter()
