import redis
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Probe")

def audit_remote():
    host = "115.190.156.240"
    port = 6379
    date_sh = "20260407"
    
    try:
        r = redis.StrictRedis(host=host, port=port, db=0, socket_timeout=10, decode_responses=True)
        logger.info(f"🚀 Connecting to {host}:{port}...")
        
        # 1. 审计基准锚点 (09:25)
        auc_key = f"market:auction:{date_sh}:0925"
        auc_raw = r.get(auc_key)
        auc_len = len(json.loads(auc_raw)) if auc_raw else 0
        logger.info(f"📊 [Anchor] {auc_key} -> Count: {auc_len}")
        
        # 2. 审计实时水位 (Latest)
        latest_key = f"market:auction:{date_sh}:latest"
        latest_raw = r.get(latest_key)
        latest_data = json.loads(latest_raw) if latest_raw else []
        latest_len = len(latest_data)
        
        total_vol = sum([float(it.get('amount', 0)) for it in latest_data])
        logger.info(f"💰 [Latest] {latest_key} -> Count: {latest_len} | TotalVolume: {total_vol/1e8:.2f}亿")
        
        # 3. 诊断：是否存在死水 (Check TTL or Last updated time)
        # Note: If latest_len is small, that explains the 2322亿 limit.
        
        print(f"\n--- AUDIT RESULTS ---")
        print(f"Server: {host}")
        print(f"Auction Snapshot: {'OK' if auc_len > 100 else 'EMPTY/MISSING'}")
        print(f"Latest Live Volume: {total_vol/1e8:.2f}亿")
        print(f"Latest Stock Count: {latest_len}")
        
    except Exception as e:
        logger.error(f"❌ Remote Probe Failed: {e}")

if __name__ == "__main__":
    audit_remote()
