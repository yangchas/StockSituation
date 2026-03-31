"""
v2_metadata_provider.py
静态元数据提供者 — 从本地 CSV 读取个股名称、板块、市值、ROE等信息。

数据源：
  - d:/work/Go/web/data/f10.csv: 名称、市值、ROE、主营
  - d:/work/Go/web/data/个股板块.csv: 股票与板块ID映射
  - d:/work/Go/web/data/板块.csv: 板块ID与名称映射
"""
import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger("V2Metadata")

class MetadataProvider:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MetadataProvider, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, data_dir: str = "/root/work/web/data"):
        if self._initialized:
            return
        self.data_dir = data_dir
        self.stock_info: Dict[str, Dict[str, Any]] = {}
        self.plate_map: Dict[str, str] = {} # code -> plate_name
        self._load_all()
        self._initialized = True

    def _load_all(self):
        try:
            self._load_f10()
            self._load_plates()
            logger.info(f"[Metadata] 加载完成: {len(self.stock_info)} 只个股, {len(self.plate_map)} 条板块映射")
        except Exception as e:
            logger.error(f"[Metadata] 加载失败: {e}")

    def _load_f10(self):
        path = os.path.join(self.data_dir, "f10.csv")
        if not os.path.exists(path):
            logger.warning(f"文件不存在: {path}")
            return
        
        try:
            df = None
            # 优先尝试 gbk (Windows 系统导出 CSV 的默认编码)
            for enc in ['gbk', 'gb18030', 'utf-8-sig', 'utf-8']:
                try:
                    df = pd.read_csv(path, encoding=enc)
                    if '股票代码' in df.columns or '代码' in df.columns:
                        break
                except: continue
            
            if df is None or df.empty: 
                logger.error(f"无法读取 f10.csv 或内容为空")
                return

            # 清理列名
            df.columns = [str(c).strip() for c in df.columns]
            
            # 定位核心列名 (参考原系统 f10_service.py)
            col_code = '股票代码' if '股票代码' in df.columns else ('代码' if '代码' in df.columns else df.columns[12])
            col_name = '股票简称' if '股票简称' in df.columns else ('名称' if '名称' in df.columns else df.columns[1])
            col_mkt_cap = 'a股市值(不含限售股)' if 'a股市值(不含限售股)' in df.columns else '总市值'
            col_roe = '净资产收益率roe(加权,公布值)' if '净资产收益率roe(加权,公布值)' in df.columns else 'roe'

            for _, row in df.iterrows():
                code_raw = str(row.get(col_code, ""))
                code = code_raw.split(".")[0].zfill(6)
                if not code.isdigit() or len(code) != 6: continue
                
                name = str(row.get(col_name, "unknown")).strip()
                # 额外校验：如果名称太长或者是数字，可能取错列了
                if len(name) > 8 or name.isdigit():
                    # 尝试寻找更短的字符串作为简称
                    for i in range(5):
                        val = str(row.iloc[i]).strip()
                        if 1 < len(val) < 8 and not val.isdigit():
                            name = val
                            break

                self.stock_info[code] = {
                    "name": name,
                    "mkt_cap_a": row.get(col_mkt_cap) or 0,
                    "main_products": str(row.get("主营产品名称") or "").strip(),
                    "roe": row.get(col_roe) or 0,
                }
        except Exception as e:
            logger.error(f"解析 f10.csv 失败: {e}")

    def _load_plates(self):
        # 定义需要过滤的大型宽基或过于空泛的板块
        EXCLUDED_PLATES = {
            "数字经济", "消费电子", "专精特新", "融资融券", "标普大盘", 
            "成分股", "核心资产", "深股通", "沪股通"
        }
        
        # 1. 加载板块 ID -> Name
        plate_names = {}
        p_path = os.path.join(self.data_dir, "板块.csv")
        if os.path.exists(p_path):
            try:
                df_p = pd.read_csv(p_path, encoding='gbk')
                for _, row in df_p.iterrows():
                    pid = str(row.get('id', row.iloc[0]))
                    pname = str(row.get('name', row.iloc[1])).strip()
                    # 跳过黑名单中的空泛板块
                    if pname not in EXCLUDED_PLATES:
                        plate_names[pid] = pname
            except Exception as e:
                logger.debug(f"加载板块.csv失败: {e}")

        # 2. 加载个股 -> 板块 ID 的映射
        sp_path = os.path.join(self.data_dir, "个股板块.csv")
        if os.path.exists(sp_path):
            try:
                df_sp = pd.read_csv(sp_path)
                for _, row in df_sp.iterrows():
                    sid = str(row.get('stockid', row.iloc[0])).zfill(6)
                    pid = str(row.get('plateid', row.iloc[1]))
                    
                    # 仅保留不在黑名单中的第一个匹配板块
                    if sid not in self.plate_map and pid in plate_names:
                        self.plate_map[sid] = plate_names[pid]
            except Exception as e:
                logger.debug(f"加载个股板块.csv失败: {e}")

    def set_redis_client(self, redis_client):
        self.redis_client = redis_client

    async def get_info(self, code: str) -> Dict[str, Any]:
        """获取综合信息 (支持返回原始板块列表)"""
        code = code.zfill(6)
        info = self.stock_info.get(code, {}).copy()
        
        # 核心逻辑：获取 Redis 中存储的全量颗粒度题材数组
        raw_plates = []
        if hasattr(self, 'redis_client') and self.redis_client:
            try:
                res = await self.redis_client.hget("config:plate_mapping:s2p", code)
                if res:
                    if str(res).startswith("["):
                        import json
                        raw_plates = json.loads(res)
                    else:
                        raw_plates = [str(res)]
            except: pass
        
        # 如果 Redis 为空，降级到静态 CSV
        if not raw_plates:
            static_p = self.plate_map.get(code)
            raw_plates = [static_p] if static_p else []
            
        info["raw_plates"] = raw_plates
        # 为了兼容以往代码，仍保留一个默认的 plate
        info["plate"] = raw_plates[0] if raw_plates else "unknown"
        if "name" not in info: info["name"] = "unknown"
        return info

    async def get_name(self, code: str) -> str:
        info = await self.get_info(code)
        return info.get("name", "unknown")

    async def get_plate(self, code: str) -> str:
        info = await self.get_info(code)
        return info.get("plate", "unknown")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    provider = MetadataProvider()
    # 测试 格科微 688728
    print(f"688728: {provider.get_info('688728')}")
    # 测试 随机代码
    print(f"000001: {provider.get_info('000001')}")
