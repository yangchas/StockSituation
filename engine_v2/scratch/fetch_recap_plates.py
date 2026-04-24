
import asyncio
import json
import os
import sys

# 路径对齐
sys.path.append(os.getcwd())

async def fetch_recap_data(date_str="2026-04-17"):
    from engine_v2.v2_network_audit_lib import NetworkAuditLib
    audit = NetworkAuditLib()
    
    print(f"--- Fetching Kaipanla Hot Plates for {date_str} ---")
    plates = await audit.get_kaipan_hot_plates(date_str)
    
    print(f"--- Fetching Wencai Hot Sectors for {date_str} ---")
    wencai_plates = await audit.get_wencai_hot_sectors(date_str)
    
    # 打印结果供分析
    result = {
        "kpl_plates": plates,
        "wencai_plates": wencai_plates
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result

if __name__ == "__main__":
    asyncio.run(fetch_recap_data())
