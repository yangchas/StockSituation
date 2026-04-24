from __future__ import annotations

"""
Composition root for Engine V3.

This file is intentionally minimal for now. V3 should start as an isolated
package with explicit boundaries, not as a copy of the current `web/` runtime.
"""

from dataclasses import dataclass

from engine_v3.core.symbol_table import SymbolTable


@dataclass
class EngineV3Bootstrap:
    symbol_table: SymbolTable

    @classmethod
    def create(cls) -> "EngineV3Bootstrap":
        return cls(symbol_table=SymbolTable())
