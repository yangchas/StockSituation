import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
AI_API_DIR = ROOT_DIR / "ai" / "API"
if str(AI_API_DIR) not in sys.path:
    sys.path.insert(0, str(AI_API_DIR))

import pykaipan.pykaipan as pk

from web.tests.kaipan_history_hot_plate_fixed import FixedKaipanHistoryClient
from web.tests.teacher_alignment_probe import SNAPSHOT_ROOT, TeacherAlignmentProbe


INTERFACES = ("get_his_plates", "get_his_plate_rangs", "get_his_plate_ids")
RAW_INTERFACE_MAP = {
    "get_his_plates": "getHisPlates",
    "get_his_plate_rangs": "getHisPlateRangs",
    "get_his_plate_ids": "getHisPlateIds",
}


def summarize_probe_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "interface": record["interface"],
        "request_date": record["request_date"],
        "effective_date": record["effective_date"],
        "date_format": record["date_format"],
        "errcode": record["errcode"],
        "count": record["count"],
        "list_len": record["list_len"],
        "list_son_len": record["list_son_len"],
        "list_soninfo_len": record["list_soninfo_len"],
        "day_field": record["day_field"],
        "first_row_preview": record["first_row_preview"],
        "diagnosis": record["diagnosis"],
    }


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _normalize_date(date_str: str) -> str:
    date_str = str(date_str).strip()
    if len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str


def _build_raw_record(interface: str, request_date: str, effective_date: str, payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "interface": interface,
            "request_date": request_date,
            "effective_date": effective_date,
            "date_format": "compact" if "-" not in request_date else "hyphen",
            "errcode": "non_dict",
            "count": 0,
            "list_len": 0,
            "list_son_len": 0,
            "list_soninfo_len": 0,
            "day_field": [],
            "first_row_preview": None,
            "diagnosis": "invalid_response_type",
        }

    rows = payload.get("list") or payload.get("List") or []
    list_son = payload.get("list_son") or []
    list_soninfo = payload.get("list_soninfo") or []
    day_field = payload.get("Day") or payload.get("day") or []
    if isinstance(day_field, str):
        day_field = [day_field]
    count = _safe_int(payload.get("Count"), len(rows))

    diagnosis = "ok"
    normalized_request = _normalize_date(request_date)
    if count == 0 and rows == []:
        diagnosis = "empty_shape"
    if day_field and normalized_request not in day_field and effective_date not in day_field:
        diagnosis = "ignored_date_or_current_day_metadata"

    preview = rows[0] if rows else None
    if isinstance(preview, list):
        preview = preview[:10]

    return {
        "interface": interface,
        "request_date": request_date,
        "effective_date": effective_date,
        "date_format": "compact" if "-" not in request_date else "hyphen",
        "errcode": str(payload.get("errcode", "")),
        "count": count,
        "list_len": len(rows) if isinstance(rows, list) else 0,
        "list_son_len": len(list_son) if isinstance(list_son, list) else 0,
        "list_soninfo_len": len(list_soninfo) if isinstance(list_soninfo, list) else 0,
        "day_field": list(day_field),
        "first_row_preview": preview,
        "diagnosis": diagnosis,
    }


def build_raw_pykaipan_probe(probe: TeacherAlignmentProbe, date_str: str) -> Dict[str, List[Dict[str, Any]]]:
    effective_date, _ = probe.normalize_trade_date(date_str)
    request_dates = [date_str, date_str.replace("-", ""), effective_date, effective_date.replace("-", "")]
    grouped_records: Dict[str, List[Dict[str, Any]]] = {name: [] for name in INTERFACES}

    for raw_date in request_dates:
        for interface, raw_name in RAW_INTERFACE_MAP.items():
            payload = getattr(pk, raw_name)(raw_date)
            grouped_records[interface].append(_build_raw_record(interface, raw_date, effective_date, payload))
    return grouped_records


def build_fixed_pykaipan_probe(probe: TeacherAlignmentProbe, date_str: str) -> Dict[str, List[Dict[str, Any]]]:
    effective_date, _ = probe.normalize_trade_date(date_str)
    request_dates = [date_str, date_str.replace("-", ""), effective_date, effective_date.replace("-", "")]
    client = FixedKaipanHistoryClient()
    grouped_records: Dict[str, List[Dict[str, Any]]] = {name: [] for name in INTERFACES}

    for raw_date in request_dates:
        grouped_records["get_his_plates"].append(
            _build_raw_record("get_his_plates", raw_date, effective_date, client.get_his_plates(_normalize_date(raw_date)))
        )
        grouped_records["get_his_plate_rangs"].append(
            _build_raw_record("get_his_plate_rangs", raw_date, effective_date, client.get_his_plate_rangs(_normalize_date(raw_date)))
        )
        grouped_records["get_his_plate_ids"].append(
            _build_raw_record("get_his_plate_ids", raw_date, effective_date, client.get_his_plate_ids(date=_normalize_date(raw_date)))
        )
    return grouped_records


def build_hot_plate_report(probe: TeacherAlignmentProbe, date_str: str) -> Dict[str, Any]:
    probe_result = probe.probe_hot_plate_interfaces(date_str)
    grouped_records: Dict[str, List[Dict[str, Any]]] = {name: [] for name in INTERFACES}
    for record in probe_result["records"]:
        grouped_records.setdefault(record["interface"], []).append(summarize_probe_record(record))

    raw_grouped_records = build_raw_pykaipan_probe(probe, date_str)
    fixed_grouped_records = build_fixed_pykaipan_probe(probe, date_str)

    interface_summary = {}
    for interface, records in grouped_records.items():
        usable = [item for item in records if item["count"] > 0 and "ignored" not in item["diagnosis"]]
        raw_records = raw_grouped_records.get(interface, [])
        raw_usable = [item for item in raw_records if item["count"] > 0 and "ignored" not in item["diagnosis"]]
        fixed_records = fixed_grouped_records.get(interface, [])
        fixed_usable = [item for item in fixed_records if item["count"] > 0 and "ignored" not in item["diagnosis"]]
        interface_summary[interface] = {
            "tested_count": len(records),
            "usable_count": len(usable),
            "is_usable": bool(usable),
            "best_record": usable[0] if usable else (records[0] if records else None),
            "raw_tested_count": len(raw_records),
            "raw_usable_count": len(raw_usable),
            "raw_is_usable": bool(raw_usable),
            "raw_best_record": raw_usable[0] if raw_usable else (raw_records[0] if raw_records else None),
            "fixed_tested_count": len(fixed_records),
            "fixed_usable_count": len(fixed_usable),
            "fixed_is_usable": bool(fixed_usable),
            "fixed_best_record": fixed_usable[0] if fixed_usable else (fixed_records[0] if fixed_records else None),
        }

    return {
        "requested_date": probe_result["requested_date"],
        "effective_date": probe_result["effective_date"],
        "rolled_to_trade_day": probe_result["rolled_to_trade_day"],
        "remote_host": probe_result["remote_host"],
        "is_reliable": probe_result["is_reliable"],
        "stock_analyzer_interfaces": grouped_records,
        "raw_pykaipan_interfaces": raw_grouped_records,
        "fixed_pykaipan_interfaces": fixed_grouped_records,
        "interface_summary": interface_summary,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"# Kaipan Historical Hot Plate Probe {report['requested_date']}")
    lines.append("")
    lines.append("## Summary")
    lines.append(
        f"- requested_date={report['requested_date']} | effective_date={report['effective_date']} | rolled_to_trade_day={report['rolled_to_trade_day']} | is_reliable={report['is_reliable']}"
    )

    lines.append("")
    lines.append("## Interface Summary")
    for interface, summary in report["interface_summary"].items():
        lines.append(
            f"- {interface}: wrapper tested={summary['tested_count']} usable={summary['usable_count']} is_usable={summary['is_usable']} | raw tested={summary['raw_tested_count']} usable={summary['raw_usable_count']} raw_is_usable={summary['raw_is_usable']} | fixed tested={summary['fixed_tested_count']} usable={summary['fixed_usable_count']} fixed_is_usable={summary['fixed_is_usable']}"
        )

    lines.append("")
    lines.append("## Detailed Records Wrapper")
    for interface in INTERFACES:
        lines.append(f"### {interface}")
        records = report["stock_analyzer_interfaces"].get(interface, [])
        if not records:
            lines.append("- no records")
            continue
        for record in records:
            lines.append(
                f"- request={record['request_date']} format={record['date_format']} count={record['count']} list={record['list_len']} day={record['day_field']} diagnosis={record['diagnosis']}"
            )
    lines.append("")
    lines.append("## Detailed Records Raw Pykaipan")
    for interface in INTERFACES:
        lines.append(f"### {interface}")
        records = report["raw_pykaipan_interfaces"].get(interface, [])
        if not records:
            lines.append("- no records")
            continue
        for record in records:
            lines.append(
                f"- request={record['request_date']} format={record['date_format']} count={record['count']} list={record['list_len']} day={record['day_field']} diagnosis={record['diagnosis']}"
            )
    lines.append("")
    lines.append("## Detailed Records Fixed Pykaipan")
    for interface in INTERFACES:
        lines.append(f"### {interface}")
        records = report["fixed_pykaipan_interfaces"].get(interface, [])
        if not records:
            lines.append("- no records")
            continue
        for record in records:
            lines.append(
                f"- request={record['request_date']} format={record['date_format']} count={record['count']} list={record['list_len']} day={record['day_field']} diagnosis={record['diagnosis']}"
            )
    return "\n".join(lines)


def write_report(report: Dict[str, Any], snapshots_dir: Optional[Path] = None) -> Dict[str, str]:
    snapshots_dir = snapshots_dir or SNAPSHOT_ROOT
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    json_path = snapshots_dir / f"hot_plate_probe_raw_{report['requested_date']}.json"
    md_path = snapshots_dir / f"hot_plate_probe_raw_{report['requested_date']}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Raw kaipan historical hot plate probe")
    parser.add_argument("--date", action="append", required=True)
    parser.add_argument("--teacher", default="niepan")
    parser.add_argument("--remote-host", default=None)
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    probe = TeacherAlignmentProbe(teacher=args.teacher, remote_host=args.remote_host)

    for date_str in args.date:
        report = build_hot_plate_report(probe, date_str)
        paths = write_report(report) if not args.no_write else {"json": None, "md": None}
        print(
            json.dumps(
                {
                    "requested_date": report["requested_date"],
                    "effective_date": report["effective_date"],
                    "rolled_to_trade_day": report["rolled_to_trade_day"],
                    "is_reliable": report["is_reliable"],
                    "interface_summary": report["interface_summary"],
                    "snapshot_json": paths["json"],
                    "snapshot_md": paths["md"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
