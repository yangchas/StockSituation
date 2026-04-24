"""
深度探测 Kaipanla 历史板块 API 的原始数据结构
目的：获取芯片/通信的完整字段（涨幅、主力净额、等）并分析系统盲区
"""
import asyncio
import json
import os
import sys

sys.path.append(os.getcwd())

async def deep_probe_kpl_plates(date_str="2026-04-17"):
    try:
        from ai.API.StockAnalyzer import StockAnalyzer
        api = StockAnalyzer()
        
        print(f"--- 原始 KPL 数据探测 [{date_str}] ---")
        res = await asyncio.get_event_loop().run_in_executor(
            None, api.get_his_plates, date_str
        )
        
        # 打印原始结构（键名探测）
        print(f"  顶层键: {list(res.keys()) if isinstance(res, dict) else type(res)}")
        
        # 找板块列表
        data_list = None
        for key in ['list', 'List', 'data', 'Data', 'plate', 'plates']:
            if isinstance(res, dict) and key in res:
                data_list = res[key]
                print(f"  板块列表键: '{key}', 共 {len(data_list)} 条")
                break
        
        if not data_list:
            print(f"  原始结果: {json.dumps(res, ensure_ascii=False, indent=2)[:2000]}")
            return
        
        # 打印前5条完整原始结构
        print(f"\n--- 原始字段结构（前5条）---")
        for i, item in enumerate(data_list[:5]):
            print(f"  [{i}] type={type(item).__name__}, len={len(item) if hasattr(item,'__len__') else 'N/A'}")
            print(f"      raw: {item}")
        
        # 如果是 dict，打印所有字段名
        if data_list and isinstance(data_list[0], dict):
            print(f"\n  字段列表: {list(data_list[0].keys())}")
        
        print(f"\n--- 全量板块数据（含所有字段）---")
        for i, item in enumerate(data_list[:15]):
            print(f"  [{i+1:02d}] {item}")
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(deep_probe_kpl_plates())
