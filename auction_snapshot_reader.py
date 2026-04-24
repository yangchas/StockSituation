import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional

import redis
import taos

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TDENGINE_HOST = os.getenv("TDENGINE_HOST", "localhost")
TDENGINE_USER = os.getenv("TDENGINE_USER", "root")
TDENGINE_PASS = os.getenv("TDENGINE_PASS", "taosdata")
TDENGINE_DB = os.getenv("TDENGINE_DB", "quant")

# Redis key patterns (以 C/t1.cpp 写入格式为准)
# latest:  market:auction:{YYYYMMDD}:latest   (hash: tag, ts)
# snapshot: market:auction:{YYYYMMDD}:{tag}  (hash: summary, top_amount, meta)
AUCTION_KEY_LATEST = "market:auction:{date}:latest"
AUCTION_KEY_SNAPSHOT = "market:auction:{date}:{tag}"
EXPECT_GAP_KEY = "signal:auction_expect_gap:{date}"  # string/json

# TDengine table names
TD_TABLE_SUMMARY = "market_auction_summary"
TD_TABLE_TOP_AMOUNT = "market_auction_top_amount"


class AuctionSnapshotReader:
    """读取竞价快照并计算预期差"""

    def __init__(self, redis_url: str = REDIS_URL):
        self.r = redis.from_url(redis_url, decode_responses=True)
        self.conn = taos.connect(host=TDENGINE_HOST, user=TDENGINE_USER, password=TDENGINE_PASS, database=TDENGINE_DB)

    # ---------------------------- Redis 实时读取 ---------------------------
    def _get_today_date_yyyymmdd(self) -> str:
        return datetime.now().strftime("%Y%m%d")

    def read_latest_snapshot(self) -> Optional[Tuple[str, int, Dict[str, Any], Any, Dict[str, Any]]]:
        """按 C/t1.cpp 写入格式读取最新快照。

        返回 (tag, ts_ms, summary_dict, top_amount_obj, meta_dict)
        """
        date = self._get_today_date_yyyymmdd()
        latest_key = AUCTION_KEY_LATEST.format(date=date)

        latest = self.r.hgetall(latest_key)
        tag = latest.get("tag")
        ts_str = latest.get("ts")
        if not tag:
            return None

        snapshot_key = AUCTION_KEY_SNAPSHOT.format(date=date, tag=tag)
        pipe = self.r.pipeline()
        pipe.hget(snapshot_key, "summary")
        pipe.hget(snapshot_key, "top_amount")
        pipe.hget(snapshot_key, "meta")
        summary_json, top_amount_json, meta_json = pipe.execute()

        if not summary_json or not top_amount_json:
            return None

        ts_ms = int(ts_str) if ts_str and ts_str.isdigit() else 0
        summary = json.loads(summary_json)
        top_amount = json.loads(top_amount_json)
        meta = json.loads(meta_json) if meta_json else {}
        return tag, ts_ms, summary, top_amount, meta

    # ------------------------ TDengine 历史数据查询 ------------------------
    def _query_td(self, table: str, date: str) -> Dict[str, Any]:
        sql = f"SELECT * FROM {table} WHERE ts >= '{date} 09:25:00' AND ts < '{date} 09:26:00' LIMIT 1"
        cur = self.conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return {}
        # 将结果行转换为 dict（需要知道列名）
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, row))

    def get_yesterday_0925_snapshot(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        summary = self._query_td(TD_TABLE_SUMMARY, yesterday)
        top_amount = self._query_td(TD_TABLE_TOP_AMOUNT, yesterday)
        return summary, top_amount

    # ----------------------------- 预期差计算 -----------------------------
    @staticmethod
    def _calc_overlap(today: Dict[str, Any], yesterday: Dict[str, Any]) -> float:
        """示例：计算股票代码重叠度"""
        if not today or not yesterday:
            return 0.0
        today_set = set(today.get("codes", []))
        yest_set = set(yesterday.get("codes", []))
        if not today_set:
            return 0.0
        return len(today_set & yest_set) / len(today_set)

    def compute_expect_gap(self, latest: Tuple[str, int, Dict[str, Any], Any, Dict[str, Any]]) -> Dict[str, Any]:
        """计算“预期差”。

        口径选择：A（你已确认）
        - 09:20 / 09:24 / 09:25 都使用一致字段口径做对比
        - 以 C++ summary 中的 *_yuan 字段为准
        """
        yesterday_summary, yesterday_top = self.get_yesterday_0925_snapshot()
        tag, ts_ms, today_summary, today_top, meta = latest

        # 金额字段以 t1.cpp 输出为准
        today_total = float(today_summary.get("total_auction_amount_yuan", 0) or 0)
        yest_total = float(yesterday_summary.get("total_auction_amount_yuan", 0) or 0)

        result = {
            "tag": tag,
            "ts": ts_ms,
            "overlap_ratio": self._calc_overlap(today_summary, yesterday_summary),
            "total_auction_amount_yuan": today_total,
            "yesterday_0925_total_auction_amount_yuan": yest_total,
            "amount_delta_yuan": today_total - yest_total,
        }
        return result

    def save_expect_gap(self, gap: Dict[str, Any]):
        key = EXPECT_GAP_KEY.format(date=datetime.now().strftime("%Y%m%d"))
        self.r.set(key, json.dumps(gap), ex=600)  # 10 分钟过期，可根据需要调整

    # ------------------------------- 主循环 ------------------------------
    def run(self):
        while True:
            latest_snapshot = self.read_latest_snapshot()
            if latest_snapshot:
                gap = self.compute_expect_gap(latest_snapshot)
                self.save_expect_gap(gap)
            time.sleep(1)


if __name__ == "__main__":
    reader = AuctionSnapshotReader()
    reader.run()
