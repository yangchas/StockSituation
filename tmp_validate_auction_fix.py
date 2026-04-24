import argparse
import json
import sys
from dataclasses import dataclass

import paramiko


HOST = "115.190.156.240"
PORT = 22
USER = "root"
PASSWORD = "Chao123+"


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


class RemoteProbe:
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(host, port, user, password, timeout=15)

    def close(self) -> None:
        self._ssh.close()

    def run(self, command: str, *, timeout: int = 90) -> tuple[str, str]:
        stdin, stdout, stderr = self._ssh.exec_command(command, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="ignore").strip()
        err = stderr.read().decode("utf-8", errors="ignore").strip()
        return out, err


def emit(text: str) -> None:
    encoding = sys.stdout.encoding or "utf-8"
    safe = text.encode(encoding, errors="replace").decode(encoding, errors="replace")
    print(safe)


def build_remote_python(script: str) -> str:
    return "python3 - <<'PY'\n" + script + "\nPY"


def check_process(probe: RemoteProbe) -> CheckResult:
    out, err = probe.run("ps -ef | grep 'python3 -X utf8 -m engine_next.app_main' | grep -v grep || true")
    ok = bool(out.strip())
    return CheckResult(
        name="process",
        ok=ok,
        detail=out or err or "engine_next.app_main is not running",
    )


def check_code_markers(probe: RemoteProbe) -> CheckResult:
    cmd = (
        "grep -n 'auction_finalize_0925\\|auction_followup_0926\\|auction_replay_0926\\|auction_replay_0925' "
        "/root/work/engine_next/app_main.py "
        "/root/work/engine_next/runtime/controllers/auction_runtime_controller.py"
    )
    out, err = probe.run(cmd)
    text = out or err
    has_new = "auction_finalize_0925" in text and "auction_followup_0926" in text
    has_old = "auction_replay_0926" in text or "auction_replay_0925" in text
    ok = has_new and not has_old
    return CheckResult(
        name="code_markers",
        ok=ok,
        detail=text or "no grep output",
    )


def check_auction_0925(probe: RemoteProbe, trade_date: str) -> CheckResult:
    tag = trade_date.replace("-", "")
    remote = build_remote_python(
        f"""
import json
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
key = "market:auction:{tag}:0925"
summary = r.hget(key, "summary")
top_amount = r.hget(key, "top_amount")
payload = json.loads(top_amount) if top_amount else []
sample = payload[:3]
print(json.dumps({{
    "key": key,
    "exists": bool(summary or top_amount),
    "summary": json.loads(summary) if summary else {{}},
    "sample_count": len(payload),
    "sample": sample,
}}, ensure_ascii=False))
"""
    )
    out, err = probe.run(f"cd /root/work && {remote}")
    text = out or err
    try:
        payload = json.loads(out)
    except Exception:
        return CheckResult(name="auction_0925", ok=False, detail=text)
    sample = payload.get("sample") or []
    has_amount = any(float(row.get("auction_amount_yuan", 0.0) or 0.0) > 0 for row in sample)
    has_bid = any(float(row.get("bid_amount_yuan", 0.0) or 0.0) > 0 for row in sample)
    ok = bool(payload.get("exists")) and payload.get("sample_count", 0) > 0 and has_amount and has_bid
    return CheckResult(name="auction_0925", ok=ok, detail=json.dumps(payload, ensure_ascii=False))


def check_anchor_recovery(probe: RemoteProbe, trade_date: str) -> CheckResult:
    remote = build_remote_python(
        f"""
import json
from engine_next.runtime.intraday_data_hub import IntradayDataHub
from engine_next.domain.enums import RunPhase

hub = IntradayDataHub()
res = hub.recover_auction_anchor("{trade_date}", RunPhase.AUCTION)
sample = res.rows[:5]
print(json.dumps({{
    "source": res.source,
    "rows": len(res.rows),
    "sample": sample,
    "amount_positive": sum(1 for row in res.rows if float(row.get("amount", 0.0) or 0.0) > 0),
    "bid_positive": sum(1 for row in res.rows if float(row.get("bid_amount", 0.0) or 0.0) > 0),
}}, ensure_ascii=False))
"""
    )
    out, err = probe.run(f"cd /root/work && {remote}")
    text = out or err
    try:
        payload = json.loads(out)
    except Exception:
        return CheckResult(name="anchor_recovery", ok=False, detail=text)
    ok = (
        payload.get("source") in {"redis_0925", "redis_anchor"}
        and int(payload.get("rows", 0) or 0) > 0
        and int(payload.get("amount_positive", 0) or 0) > 0
        and int(payload.get("bid_positive", 0) or 0) > 0
    )
    return CheckResult(name="anchor_recovery", ok=ok, detail=json.dumps(payload, ensure_ascii=False))


def check_anchor_archive(probe: RemoteProbe, trade_date: str) -> CheckResult:
    tag = trade_date.replace("-", "")
    remote = build_remote_python(
        f"""
import itertools
import json
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
raw = r.get("market:auction:anchor:{tag}")
obj = json.loads(raw) if raw else {{}}
sample = list(itertools.islice(obj.items(), 3))
extended = 0
for _, value in obj.items():
    if isinstance(value, dict) and (float(value.get("amount", 0.0) or 0.0) > 0 or float(value.get("bid_amount", 0.0) or 0.0) > 0):
        extended += 1
print(json.dumps({{
    "entries": len(obj),
    "extended_entries": extended,
    "sample": sample,
}}, ensure_ascii=False))
"""
    )
    out, err = probe.run(f"cd /root/work && {remote}")
    text = out or err
    try:
        payload = json.loads(out)
    except Exception:
        return CheckResult(name="anchor_archive", ok=False, detail=text)
    ok = int(payload.get("entries", 0) or 0) > 0 and int(payload.get("extended_entries", 0) or 0) > 0
    return CheckResult(name="anchor_archive", ok=ok, detail=json.dumps(payload, ensure_ascii=False))


def check_quote_fields(probe: RemoteProbe) -> CheckResult:
    remote = build_remote_python(
        """
import json
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
codes = ["002081", "600736", "300067", "603115"]
sample = {}
for code in codes:
    q = r.hgetall(f"stock:quote:{code}") or {}
    sample[code] = {
        "timestamp": q.get("timestamp"),
        "bid_amount": q.get("bid_amount"),
        "book1_amount_yuan": q.get("book1_amount_yuan"),
        "amount_2m": q.get("amount_2m"),
        "amount_2min": q.get("amount_2min"),
    }
print(json.dumps(sample, ensure_ascii=False))
"""
    )
    out, err = probe.run(f"cd /root/work && {remote}")
    text = out or err
    try:
        payload = json.loads(out)
    except Exception:
        return CheckResult(name="quote_fields", ok=False, detail=text)
    ok = any((row.get("book1_amount_yuan") not in (None, "", "0", "0.0")) for row in payload.values())
    return CheckResult(name="quote_fields", ok=ok, detail=json.dumps(payload, ensure_ascii=False))


def check_latest_log(probe: RemoteProbe) -> CheckResult:
    out, err = probe.run("tail -n 40 /root/work/nohup_engine_next.txt || true")
    text = out or err or "no log output"
    ok = bool(text.strip())
    return CheckResult(name="latest_log", ok=ok, detail=text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate remote auction-fix rollout on chaos.")
    parser.add_argument("--trade-date", default="2026-04-24", help="Trade date in YYYY-MM-DD format.")
    args = parser.parse_args()

    probe = RemoteProbe(HOST, PORT, USER, PASSWORD)
    try:
        results = [
            check_process(probe),
            check_code_markers(probe),
            check_auction_0925(probe, args.trade_date),
            check_anchor_recovery(probe, args.trade_date),
            check_anchor_archive(probe, args.trade_date),
            check_quote_fields(probe),
            check_latest_log(probe),
        ]
    finally:
        probe.close()

    failed = [item for item in results if not item.ok]
    for item in results:
        status = "PASS" if item.ok else "FAIL"
        emit(f"[{status}] {item.name}")
        emit(item.detail)
        emit("-" * 80)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
