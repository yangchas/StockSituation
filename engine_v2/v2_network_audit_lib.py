
import sys
import os
import json
import asyncio
import pandas as pd
import aiohttp
from typing import Dict, List, Any, Tuple
from datetime import datetime

# 路径对齐
sys.path.append(os.getcwd())

class NetworkAuditLib:
    """多源网络审计物理库 (V8.5 - 纯API强化版)"""
    
    def __init__(self):
        self._sina_url = "http://hq.sinajs.cn/list="
        self._headers = {"Referer": "http://finance.sina.com.cn"}

    async def get_wencai_limit_truth(self, date_str: str):
        """通过问财网络接口获取当日真实的晋级成功与失败名单 (V8.4 精准版)"""
        from ai.API.api import UnifiedMarketDataFetcher
        wencai = UnifiedMarketDataFetcher()
        
        # A. 晋级池 (今日涨停真相) - 增加连板天数与去ST过滤
        q_success = f"{date_str}涨停，连板天数，去除ST"
        df_success = await wencai.get_wencai_data(q_success)
        
        # B. 失败池降级检测 (昨日涨停今日未涨停)
        q_fail = f"前日涨停;{date_str}未涨停;{date_str}收盘价"
        df_fail = await wencai.get_wencai_data(q_fail)
        
        return df_success, df_fail

    async def get_wencai_hot_sectors(self, date_str: str):
        """[V8.4] 通过问财拉取当日真实热门板块梯队 (增强型模糊匹配)"""
        from ai.API.api import UnifiedMarketDataFetcher
        wencai = UnifiedMarketDataFetcher()
        q = f"{date_str}热门板块，指数涨跌幅"
        try:
            df = await wencai.get_wencai_data(q)
            if df is None or df.empty: return []
            
            # 使用模糊列名定位
            name_col = next((c for c in df.columns if '指数' in c or '板块' in c), None)
            pct_col = next((c for c in df.columns if '涨跌幅' in c or '幅度' in c), None)
            
            results = []
            for i, row in df.head(15).iterrows():
                name = row.get(name_col, 'Unknown') if name_col else 'Unknown'
                change = row.get(pct_col, 0.0) if pct_col else 0.0
                results.append({"name": str(name), "change": float(change or 0), "rank": i+1})
            return results
        except:
            return []

    async def get_sina_fast_quote(self, code_list: list) -> dict:
        """从新浪接口获取最纯粹的现价/成交额 (绕过所有本地缓存)"""
        results = {}
        # 拆分 80 个一组，防止 URL 过长
        for i in range(0, len(code_list), 80):
            chunk = code_list[i:i+80]
            params = ",".join([f"{'sz' if c.startswith(('0','3')) else 'sh'}{c}" for c in chunk])
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self._sina_url}{params}", headers=self._headers) as resp:
                    text = await resp.text()
                    for line in text.split('\n'):
                        if '=' not in line: continue
                        name_part, data_part = line.split('=')
                        code = name_part[-6:]
                        data = data_part.replace('"', '').split(',')
                        if len(data) > 30:
                            results[code] = {
                                "open": float(data[1]),
                                "high": float(data[4]),
                                "low": float(data[5]),
                                "close": float(data[3]),
                                "amount": float(data[9]),
                                "pct_chg": round((float(data[3])/float(data[2]) - 1)*100, 2) if float(data[2])>0 else 0
                            }
        return results

    async def get_kaipan_yest_bans(self, date_str: str) -> Dict[str, Any]:
        """[V8.5] 获取开盘啦当日真实涨停池及其详情"""
        try:
            from ai.API.StockAnalyzer import StockAnalyzer
            api = StockAnalyzer()
            # 获取 1-5 板全量对撞池 (内部已解析索引)
            raw_pool = await asyncio.get_event_loop().run_in_executor(
                None, api.get_history_bans_pool, date_str, 5
            )
            if not raw_pool: return {}
            return {item['code']: item for item in raw_pool}
        except Exception as e:
            print(f"KPL Audit Failed: {e}")
            return {}

    async def get_kaipan_hot_plates(self, date_str: str) -> List[Dict[str, Any]]:
        """[V8.5] 获取开盘啦当日真实主线数据"""
        try:
            from ai.API.StockAnalyzer import StockAnalyzer
            api = StockAnalyzer()
            res = await asyncio.get_event_loop().run_in_executor(
                None, api.get_his_plates, date_str
            )
            # Kaipanla 历史数据结构通常在 'list' 键中
            data_list = res.get('list', res.get('List', []))
            if not data_list: return []
            
            return [{
                        "name": it[1], 
                        "rank": i+1, 
                        "hot": it[2] if len(it)>2 else 0,
                        "change_pct": float(it[3]) if len(it)>3 else 0.0,
                        "net_inflow": float(it[6]) / 1e8 if len(it)>6 else 0.0 # [V42.0] 单位换算为亿
                    } 
                    for i, it in enumerate(data_list[:15])]
        except:
            return []
