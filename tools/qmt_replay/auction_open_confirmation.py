"""CLI/fixture adapter for the canonical opening-facts transformation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from engine_next.runtime.open_confirmation import (
    build_observation,
    build_observation_from_inputs,
    build_open_confirmation_observation,
    render_markdown,
    write_outputs,
)

__all__ = [
    "build_observation",
    "build_observation_from_inputs",
    "build_open_confirmation_observation",
    "render_markdown",
    "write_outputs",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build read-only auction-to-open facts comparison")
    parser.add_argument("--auction-report", type=Path, required=True)
    parser.add_argument("--plate-shadow", type=Path, required=True)
    parser.add_argument("--q2", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-md", type=Path, required=True)
    parser.add_argument("--data-origin", default="replay_fixture_only")
    args = parser.parse_args()
    result = build_observation(
        auction_report=args.auction_report,
        plate_shadow=args.plate_shadow,
        q2=args.q2,
        data_origin=args.data_origin,
    )
    write_outputs(result, json_path=args.output_json, markdown_path=args.output_md)
    print(json.dumps({"format": result["format"], "trade_date": result["trade_date"], "plate_count": len(result["plates"]), "business_sha256": result["business_sha256"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
