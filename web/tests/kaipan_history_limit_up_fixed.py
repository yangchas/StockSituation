import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

AI_API_DIR = ROOT_DIR / "ai" / "API"
if str(AI_API_DIR) not in sys.path:
    sys.path.insert(0, str(AI_API_DIR))

import pykaipan.pykaipan as pk


class FixedKaipanHistoryLimitClient:
    BASE_URL = "https://apphis.longhuvip.com/w1/api/index.php"

    def __init__(self) -> None:
        self.headers = dict(pk.headers)
        self.config = dict(pk.config)

    def _request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(self.BASE_URL, headers=self.headers, params=params, timeout=30)
        response.encoding = response.apparent_encoding
        return response.json()

    def get_his_bans(self, date: str, index: str = "0", size: str = "20") -> Dict[str, Any]:
        params = {
            "Order": "1",
            "a": "HisDaBanList",
            "st": str(size),
            "c": "HisHomeDingPan",
            "PhoneOSNew": "1",
            "DeviceID": self.config["DeviceID"],
            "VerSion": "5.16.0.0",
            "Index": str(index),
            "Is_st": "1",
            "PidType": "1",
            "apiv": "w38",
            "Type": "6",
            "FilterMotherboard": "0",
            "Filter": "0",
            "FilterTIB": "0",
            "Day": date,
            "FilterGem": "0",
        }
        return self._request(params)

    def get_his_zha(self, date: str, index: str = "0", size: str = "20") -> Dict[str, Any]:
        params = {
            "Order": "1",
            "a": "HisDaBanList",
            "st": str(size),
            "c": "HisHomeDingPan",
            "PhoneOSNew": "1",
            "DeviceID": self.config["DeviceID"],
            "VerSion": "5.16.0.0",
            "Index": str(index),
            "Is_st": "1",
            "PidType": "2",
            "apiv": "w38",
            "Type": "4",
            "FilterMotherboard": "0",
            "Filter": "0",
            "FilterTIB": "0",
            "Day": date,
            "FilterGem": "0",
        }
        return self._request(params)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fixed kaipan history limit-up caller")
    parser.add_argument("--date", required=True)
    args = parser.parse_args(argv)

    client = FixedKaipanHistoryLimitClient()
    result = {
        "get_his_bans": client.get_his_bans(args.date),
        "get_his_zha": client.get_his_zha(args.date),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
