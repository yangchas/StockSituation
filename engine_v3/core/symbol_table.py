from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SymbolTable:
    """
    Stable symbol <-> integer mapping for the hot path.

    The table is append-only by default so native code can rely on stable ids.
    """

    capacity: int = 65535
    _symbol_to_id: dict[str, int] = field(default_factory=dict)
    _id_to_symbol: list[str] = field(default_factory=list)

    def register(self, symbol: str) -> int:
        symbol = str(symbol).strip()
        if not symbol:
            raise ValueError("symbol must not be empty")

        existing = self._symbol_to_id.get(symbol)
        if existing is not None:
            return existing

        next_id = len(self._id_to_symbol)
        if next_id >= self.capacity:
            raise OverflowError(f"symbol table capacity exceeded: {self.capacity}")

        self._symbol_to_id[symbol] = next_id
        self._id_to_symbol.append(symbol)
        return next_id

    def register_many(self, symbols: list[str]) -> list[int]:
        return [self.register(symbol) for symbol in symbols]

    def get_id(self, symbol: str) -> int | None:
        return self._symbol_to_id.get(str(symbol).strip())

    def get_symbol(self, symbol_id: int) -> str:
        return self._id_to_symbol[symbol_id]

    def __len__(self) -> int:
        return len(self._id_to_symbol)

    def snapshot(self) -> dict[str, int]:
        return dict(self._symbol_to_id)
