
import sys
import os
import json
import asyncio
import pandas as pd
import aiohttp
from datetime import datetime

# 路径对齐
sys.path.append(os.getcwd())

class NetworkAuditLib:
    """多源网络审计物理库 (V8.3 - 纯API版)"""
    
    def __init__(self):
        self._sina_url = "http://hq.sinajs.cn/list="
        self._headers = {"Referer": "http://finance.sina.com.cn"}

    async def get_wencai_limit_truth(self, date_str: str):
        """通过问财网络接口获取当日真实的晋级成功与失败名单"""
        from ai.API.api import UnifiedMarketDataFetcher
        wencai = UnifiedMarketDataFetcher()
        
        # A. 晋级成功池 (今日涨停)
        q_success = f"{date_str}涨停;{date_str}收盘价;{date_str}成交额;{date_str}最高价;{date_str}开盘价"
        df_success = await wencai.get_wencai_data(q_success)
        
        # B. 晋级失败池 (昨日涨停今日未涨停)
        yest_date = date_str # 问财自然语义会将'昨日'解析为date_str的前一天
        q_fail = f"{date_str}昨日涨停;{date_str}未涨停;{date_str}收盘价;{date_str}成交额;{date_str}最高价;{date_str}开盘价"
        df_fail = await wencai.get_wencai_data(q_fail)
        
        return df_success, df_fail

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

    async def get_kaipan_hot_plates(self, date_str: str):
        """获取开盘啦当日真实主线"""
        try:
            from ai.API.StockAnalyzer import StockAnalyzer
            api = StockAnalyzer()
            res = await asyncio.get_event_loop().run_in_executor(None, api._call_api, 'getHisPlates', date_str)
            if not res or 'list' not in res: return []
            return [{"name": it[1], "rank": i+1, "hot": it[2]} for i, it in enumerate(res['list'][:10])]
        except:
            return []
