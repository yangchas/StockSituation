from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    choose_primary_plate,
    decode_theme_list,
    encode_theme_list,
    merge_theme_lists,
)


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:].zfill(6) if text else ""


@dataclass(frozen=True)
class StaticDataLoadResult:
    dataset: str
    rows_loaded: int
    source_path: str | None
    redis_keys_written: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


class StartupStaticDataLoader:
    """
    Lightweight startup CSV loader aligned with engine_v2 metadata bootstrap.

    Startup only materializes the runtime-critical plate mapping into Redis.
    Large F10 metadata stays on disk and should not be mirrored into Redis.
    """

    ENCODINGS = ("gbk", "gb18030", "utf-8-sig", "utf-8")
    MIN_EXPECTED_STOCK_PLATE_ROWS = 1000
    PLATE_CSV_NAME = "\u677f\u5757.csv"
    STOCK_PLATE_CSV_NAME = "\u4e2a\u80a1\u677f\u5757.csv"

    def __init__(self, *, redis_client: Any | None = None, base_dir: str | Path | None = None) -> None:
        self._redis = redis_client
        self._base_dir = Path(base_dir) if base_dir is not None else Path(__file__).resolve().parents[2]

    @property
    def redis(self) -> Any:
        if self._redis is None:
            import redis as redis_lib

            self._redis = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        return self._redis

    def discover_plate_csv_pair(self) -> tuple[Path | None, Path | None]:
        root_candidates = []
        cursor = self._base_dir
        for _ in range(4):
            root_candidates.append(cursor)
            cursor = cursor.parent

        for root in root_candidates:
            web_data = root / "web" / "data"
            plate_data = root / "plate" / "data"
            web_plate = web_data / self.PLATE_CSV_NAME
            web_stock_plate = web_data / self.STOCK_PLATE_CSV_NAME
            if web_plate.exists() and web_stock_plate.exists():
                return web_plate, web_stock_plate
            plate_plate = plate_data / self.PLATE_CSV_NAME
            plate_stock_plate = plate_data / self.STOCK_PLATE_CSV_NAME
            if plate_plate.exists() and plate_stock_plate.exists():
                return plate_plate, plate_stock_plate
        return None, None

    def _read_csv_rows(self, path: Path) -> list[list[str]]:
        for encoding in self.ENCODINGS:
            try:
                with path.open("r", encoding=encoding, newline="") as handle:
                    return [row for row in csv.reader(handle) if row]
            except UnicodeDecodeError:
                continue
        raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode {path}")

    def load_stock_plate_mapping(self) -> StaticDataLoadResult:
        plate_path, stock_plate_path = self.discover_plate_csv_pair()
        if plate_path is None or stock_plate_path is None:
            return StaticDataLoadResult(
                dataset="stock_plate_mapping",
                rows_loaded=0,
                source_path=None,
                notes=("plate csv files not found under web/data or plate/data",),
            )

        plate_rows = self._read_csv_rows(plate_path)
        stock_plate_rows = self._read_csv_rows(stock_plate_path)

        plate_id_to_name: dict[str, str] = {}
        for row in plate_rows:
            if len(row) < 2:
                continue
            plate_id = str(row[0]).strip()
            plate_name = str(row[1]).strip()
            if not plate_id or not plate_name:
                continue
            plate_id_to_name[plate_id] = plate_name

        stock_to_themes: dict[str, list[str]] = {}
        for row in stock_plate_rows:
            if len(row) < 2:
                continue
            plate_id = str(row[0]).strip()
            symbol = _normalize_symbol(row[1])
            if not symbol:
                continue
            plate_name = plate_id_to_name.get(plate_id)
            if not plate_name:
                continue
            stock_to_themes[symbol] = merge_theme_lists(stock_to_themes.get(symbol, ()), (plate_name,))

        if not stock_to_themes:
            return StaticDataLoadResult(
                dataset="stock_plate_mapping",
                rows_loaded=0,
                source_path=str(stock_plate_path),
                notes=("csv parse finished but no usable stock-plate rows were produced",),
            )

        existing_theme_raw = self.redis.hgetall(PLATE_MAPPING_S2P_KEY) or {}
        existing_runtime_primary = self.redis.hgetall(RUNTIME_PRIMARY_PLATE_KEY) or {}

        merged_theme_payload: dict[str, str] = {}
        runtime_seed_payload: dict[str, str] = {}
        for symbol, themes in stock_to_themes.items():
            merged_themes = merge_theme_lists(decode_theme_list(existing_theme_raw.get(symbol)), themes)
            merged_theme_payload[symbol] = encode_theme_list(merged_themes)
            if not str(existing_runtime_primary.get(symbol) or "").strip():
                primary_plate = choose_primary_plate(merged_themes, fallback=themes[0] if themes else "")
                if primary_plate:
                    runtime_seed_payload[symbol] = primary_plate

        pipe = self.redis.pipeline()
        if merged_theme_payload:
            pipe.hset(PLATE_MAPPING_S2P_KEY, mapping=merged_theme_payload)
        if runtime_seed_payload:
            pipe.hset(RUNTIME_PRIMARY_PLATE_KEY, mapping=runtime_seed_payload)
        pipe.execute()
        return StaticDataLoadResult(
            dataset="stock_plate_mapping",
            rows_loaded=len(stock_to_themes),
            source_path=str(stock_plate_path),
            redis_keys_written=(PLATE_MAPPING_S2P_KEY, RUNTIME_PRIMARY_PLATE_KEY),
            notes=(
                f"loaded stock->theme mapping from {stock_plate_path.name}",
                f"plate dictionary rows={len(plate_id_to_name)}",
            ),
        )
