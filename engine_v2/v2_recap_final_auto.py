import os, sys, re, json, redis, logging
from datetime import datetime, date
import baostock as bs
import pandas as pd

# 环境初始化
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("AutoRecap")

class StandardRecap:
    def __init__(self, target_date=None):
        self.date_str = target_date or date.today().strftime("%Y-%m-%d")
        self.tag = self.date_str.replace("-", "")
        self.r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        self.targets = {} # {code: {"name": str, "rating": str, "action": str}}

    def _discover_targets(self):
        """核心进化：多源发现标的"""
        # 1. 尝试 Redis 快照
        snap_key = f"market:snapshot:{self.tag}:signals"
        raw = self.r.get(snap_key)
        if raw:
            try:
                data = json.loads(raw)
                signals = data.get('signals', [])
                for s in signals:
                    self.targets[s['code']] = {"name": s['name'], "rating": s.get('tag', 'unknown'), "action": s['action']}
                logger.info(f"[Snapshot] Recognized {len(self.targets)} targets from Redis")
                return
            except: pass

        # 2. Fallback: 日志扫描 (nohup.txt)
        log_path = "c:/Users/yangxuezhen/Desktop/nohup.txt"
        if os.path.exists(log_path):
            logger.info(f"[Fallback] Snapshot missing, parsing log: {log_path}...")
            
            # 初始化元数据用于名称对代码 (关键：修正物理路径)
            from engine_v2.v2_metadata_provider import MetadataProvider
            data_path = "/root/work/web/data" if os.name != 'nt' else "D:/work/Go/web/data"
            meta = MetadataProvider(data_dir=data_path)
            # 兼容性处理：部分环境下属性可能缺失
            if not hasattr(meta, 'data_dir'): meta.data_dir = data_path
            
            meta._load_f10()
            name_to_code = {v['name']: k for k, v in meta.stock_info.items()}

            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                # 匹配正向：汇源通信(光纤概念)
                matches_pos = re.findall(r"([\u4e00-\u9fa5]{2,8})\(.*?\)\s+\d+->\dB", content)
                for name in set(matches_pos):
                    code = name_to_code.get(name)
                    if code: self.targets[code] = {"name": name, "rating": "LEADER", "action": "HOLD"}
                
                # 匹配风险：【风险/套利】 拉普拉斯
                matches_risk = re.findall(r"【风险/套利】\s+([\u4e00-\u9fa5]{2,8})", content)
                for name in set(matches_risk):
                    code = name_to_code.get(name)
                    if code: self.targets[code] = {"name": name, "rating": "RISK", "action": "AVOID"}
            
            logger.info(f"[LogScan] Recognized {len(self.targets)} targets from logs")

    def _fetch_market_truth(self):
        """穿透 Baostock 获取最终结局"""
        bs.login()
        results = []
        for code, info in self.targets.items():
            name = info['name']
            prefix = 'sh' if code.startswith('6') else 'sz'
            try:
                rs = bs.query_history_k_data_plus(f"{prefix}.{code}", "date,open,high,low,close,preclose,pctChg", self.date_str, self.date_str, "d")
                if rs.next():
                    r = rs.get_row_data()
                    pct, hi, pr = float(r[6]), float(r[2]), float(r[5])
                    lim = round(pr * 1.10, 2)
                    status = "LOCKED" if pct > 9.8 else "DROP" if hi >= lim - 0.01 else "REPAIR" if pct > 0 else "DEEP"
                    results.append({
                        "code": code, "name": name, "rating": info['rating'], "action": info['action'],
                        "pct": pct, "status": status
                    })
            except: continue
        bs.logout()
        return pd.DataFrame(results)

    def run(self):
        print(f"\n{'='*80}\nMarketEdge Automated Recap Center ({self.date_str})\n{'='*80}")
        self._discover_targets()
        if not self.targets:
            print("No targets found for auditing.")
            return
            
        df = self._fetch_market_truth()
        if df.empty:
            print("No market data fetched.")
            return

        print(df[["code", "name", "rating", "action", "pct", "status"]].to_string())
        
        print(f"\n{'='*80}\nAlgorithm Optimization Suggestions\n{'='*80}")
        false_risks = df[(df['action'] == "AVOID") & (df['pct'] > 5)]
        if not false_risks.empty:
            print(f"Warning: Over-sensitive Alert detected for {len(false_risks)} stocks (e.g. {false_risks.iloc[0]['name']}). Suggest relaxing NUCLEAR_THRESHOLD.")
        
        locked_ok = df[df['status'] == "LOCKED"]
        print(f"Stats: Board Success Rate {len(locked_ok)/len(df)*100:.1f}%")

if __name__ == "__main__":
    recap = StandardRecap("2026-04-09")
    recap.run()
