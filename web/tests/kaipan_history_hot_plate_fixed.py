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


class FixedKaipanHistoryClient:
    BASE_URL = "https://apphis.longhuvip.com/w1/api/index.php"

    def __init__(self) -> None:
        self.headers = dict(pk.headers)
        self.config = dict(pk.config)

    def _request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(self.BASE_URL, headers=self.headers, params=params, timeout=30)
        response.encoding = response.apparent_encoding
        return response.json()

    def get_his_plates(self, date: str, index: str = "0", size: str = "50") -> Dict[str, Any]:
        params = {
            "Order": "1",
            "a": "RealRankingInfo",
            "st": str(size),
            "c": "ZhiShuRanking",
            "PhoneOSNew": "1",
            "DeviceID": self.config["DeviceID"],
            "VerSion": "5.16.0.0",
            "Index": str(index),
            "Date": date,
            "apiv": "w38",
            "Type": "1",
            "ZSType": "7",
        }
        return self._request(params)

    def get_his_plate_rangs(self, date: str, index: str = "0", size: str = "50") -> Dict[str, Any]:
        params = {
            "Order": "1",
            "a": "GetInterviewsByDateZS",
            "st": str(size),
            "c": "StockLineData",
            "PhoneOSNew": "1",
            "DeviceID": self.config["DeviceID"],
            "VerSion": "5.16.0.0",
            "DEnd": date,
            "Index": str(index),
            "DStart": date,
            "apiv": "w38",
            "Type": "1",
        }
        return self._request(params)

    def get_his_plate_ids(
        self,
        plate_id: str = "801218",
        date: str = "",
        index: str = "0",
        size: str = "30",
    ) -> Dict[str, Any]:
        params = {
            "Order": "1",
            "TSZB": "0",
            "a": "ZhiShuStockList_W8",
            "st": str(size),
            "c": "ZhiShuRanking",
            "PhoneOSNew": "1",
            "old": "1",
            "DeviceID": self.config["DeviceID"],
            "VerSion": "5.16.0.0",
            "IsZZ": "0",
            "Token": self.config["Token"],
            "Index": str(index),
            "Date": date,
            "apiv": "w38",
            "Type": "6",
            "IsKZZType": "0",
            "UserID": self.config["UserID"],
            "PlateID": plate_id,
            "TSZB_Type": "0",
            "filterType": "0",
        }
        return self._request(params)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fixed kaipan history hot plate caller")
    parser.add_argument("--date", required=True)
    parser.add_argument("--plate-id", default="801218")
    args = parser.parse_args(argv)

    client = FixedKaipanHistoryClient()
    result = {
        "get_his_plates": client.get_his_plates(args.date),
        "get_his_plate_rangs": client.get_his_plate_rangs(args.date),
        "get_his_plate_ids": client.get_his_plate_ids(args.plate_id, args.date),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
