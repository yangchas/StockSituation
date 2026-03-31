import sys
import json
import asyncio
# 环境对齐
sys.path.append("/root/work/engine_v2")

from v2_plate_resolver import PlateResolver
from v2_metadata_provider import MetadataProvider

async def audit():
    meta = MetadataProvider("/root/work/web/data")
    # 模拟底座拿到的 Reason
    r_map = {
        "603538": "医药+减肥药、原料药",
        "000720": "绿色电力",
        "000722": "绿色电力+抽水蓄能",
        "600726": "电力+绿色电力",
        "603601": "商业航天+玻纤"
    }
    
    print(f"{'Code':<8} {'Name':<10} {'Resolved Plate':<15} {'Raw Plates'}")
    print("-" * 60)
    
    for code, reason in r_map.items():
        info = await meta.get_info(code)
        raw = info.get("raw_plates", [])
        res = PlateResolver.resolve_precise_plate(code, raw, reason)
        print(f"{code:<8} {info.get('name', 'unknown'):<10} {res:<15} {raw}")

if __name__ == "__main__":
    asyncio.run(audit())
