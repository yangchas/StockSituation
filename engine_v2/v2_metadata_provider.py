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

    def __init__(self, data_dir: Optional[str] = None):
        # 🛠️ 允许路径覆盖：如果传入了新路径且与旧路径不同，或者当前为空，则强制重载
        if self._initialized and (data_dir is None or data_dir == self.data_dir):
            if self.stock_info: # 已经加载过且有数据
                return
        
        if data_dir:
            self.data_dir = data_dir
            
        self.stock_info: Dict[str, Dict[str, Any]] = {}
        self.plate_map: Dict[str, str] = {} # code -> plate_name
        self.inverse_plate_map: Dict[str, List[str]] = {} # plate_name -> [codes]
        self._load_all()
        self._initialized = True

    def _load_all(self):
        try:
            self._load_f10()
            self._load_plates()
            if not self.stock_info:
                logger.error(f"❌ [Metadata] 注册失败: 证券字典为空，请检查路径及 CSV 格式")
            else:
                logger.info(f"✅ [Metadata] 加载完成: {len(self.stock_info)} 只个股, {len(self.plate_map)} 条板块映射")
        except Exception as e:
            logger.error(f"❌ [Metadata] 严重初始化失败: {e}")

    def _load_f10(self):
        path = os.path.join(self.data_dir, "f10.csv")
        if not os.path.exists(path):
            logger.error(f"❌ [Critical] 核心文件缺失: {path}")
            return
        
        try:
            # 1. 快速探测列名（不加载数据），确定核心字段映射
            match_df = None
            for enc in ['gbk', 'gb18030', 'utf-8-sig', 'utf-8']:
                try: 
                    match_df = pd.read_csv(path, encoding=enc, nrows=0)
                    break
                except: continue
            
            if match_df is None: return
            
            cols = [str(c).strip() for c in match_df.columns]
            # 🛠️ 截图对齐：根据 Excel 物理位置锁定 (Index 13:代码, 14:简称, 1:市值, 3:ROE)
            col_code = '股票代码' if '股票代码' in cols else cols[13]
            col_name = '股票简称' if '股票简称' in cols else cols[14]
            col_mkt_cap = 'a股市值(不含限售股)' if 'a股市值(不含限售股)' in cols else ('总市值' if '总市值' in cols else cols[1])
            
            # ROE 字符串可能包含非标准字符，优先物理索引 3
            roe_candidates = ['净资产收益率roe(加权,公布值)', 'roe']
            col_roe = next((c for c in roe_candidates if c in cols), cols[3])
            
            # 🛠️ 内存对齐方案：移除不使用的“主营产品”长文本，节省 ~40MB 内存
            use_cols = [col_code, col_name, col_mkt_cap, col_roe]
            
            df = pd.read_csv(path, encoding=match_df.encoding if hasattr(match_df, 'encoding') else 'gbk', usecols=use_cols)
            df.columns = [str(c).strip() for c in df.columns] # 再次清理
            
            # 🛠️ 向量化格式化与去重 (物理剥离后缀)
            df['_std_code'] = df[col_code].astype(str).str.split('.').str[0].str.zfill(6)
            df = df.drop_duplicates('_std_code', keep='last')
            
            # 🛠️ 终极分配优化：使用 numpy 视图层面的 zip 迭代（零临时对象）
            codes = df[col_code].values
            names = df[col_name].values
            assets = df[col_mkt_cap].values
            roes = df[col_roe].values
            
            for code_val, name_val, asset_val, roe_val in zip(codes, names, assets, roes):
                # 对齐处理：剥离后缀
                code = str(code_val).split('.')[0].strip().zfill(6)
                if not code.isdigit() or len(code) != 6: continue
                
                # 纠错逻辑：防止索引偏离
                name = str(name_val).strip()
                if len(name) > 8 or (name.isdigit() and len(name) > 4):
                    name = "unknown"
                
                # 压缩存储：只保留核心数值，不再存储冗长的 main_products
                self.stock_info[code] = {
                    "name": name,
                    "mkt_cap_a": asset_val or 0,
                    "roe": roe_val or 0,
                }
            
            # 🛠️ 内存强制回赎：DataFrame 及 numpy 视图使命完成后立即物理销毁
            del df
            del codes, names, assets, roes
            import gc
            gc.collect()
        except Exception as e:
            logger.error(f"解析 f10.csv 失败: {e}")

    def _load_plates(self):
        # 定义需要过滤的大型宽基或过于空泛的板块
        EXCLUDED_PLATES = {
            "成分股", "核心资产", "深股通", "沪股通", "标普大盘", "标普500"
        }
        
        # 1. 加载板块 ID -> Name
        plate_names = {}
        # 🛡️ 路径纠偏优化：支持 web/data 和 plate/data 等多种工程布局
        p_candidates = [
            os.path.join(self.data_dir, "板块.csv"),
            os.path.join(os.getcwd(), "plate/data/板块.csv")
        ]
        p_path = next((p for p in p_candidates if os.path.exists(p)), p_candidates[0])
        
        if os.path.exists(p_path):
            try:
                # 🛠️ 稳健编码：优先 GBK
                df_p = pd.read_csv(p_path, encoding='gbk', usecols=[0, 1])
                for pid_val, pname_val in zip(df_p.iloc[:, 0].values, df_p.iloc[:, 1].values):
                    pid, pname = str(pid_val), str(pname_val).strip()
                    if pname not in EXCLUDED_PLATES:
                        plate_names[pid] = pname
            except Exception as e:
                logger.debug(f"加载板块.csv失败: {e}")

        # 2. 加载个股 -> 板块 ID 的映射
        sp_candidates = [
            os.path.join(self.data_dir, "个股板块.csv"),
            os.path.join(os.getcwd(), "plate/data/个股板块.csv")
        ]
        sp_path = next((p for p in sp_candidates if os.path.exists(p)), sp_candidates[0])
        
        if os.path.exists(sp_path):
            try:
                # 🛠️ 补丁：增加对 GBK 的支持，并处理个股代码后缀
                df_sp = pd.read_csv(sp_path, encoding='gbk', usecols=[0, 1])
                for pid_val, sid_raw in zip(df_sp.iloc[:, 0].values, df_sp.iloc[:, 1].values):
                    # 🛠️ 核心对齐：剥离 .SH/.SZ 后缀，确保与 stock_info 键值一致
                    sid = str(sid_raw).split('.')[0].strip().zfill(6)
                    pid = str(pid_val)
                    
                    if sid in self.stock_info and pid in plate_names:
                        pname = plate_names[pid]
                        # 更新正向映射
                        if sid not in self.plate_map:
                            self.plate_map[sid] = pname
                        
                        # 构建反向索引 Plate -> List[Code]
                        if pname not in self.inverse_plate_map:
                            self.inverse_plate_map[pname] = []
                        if sid not in self.inverse_plate_map[pname]:
                            self.inverse_plate_map[pname].append(sid)
            except Exception as e:
                logger.warning(f"⚠️ 加载个股板块.csv失败 (尝试UTF-8降载): {e}")
                try: # 降级尝试 UTF-8
                    df_sp = pd.read_csv(sp_path, usecols=[0, 1])
                    for pid_val, sid_raw in zip(df_sp.iloc[:, 0].values, df_sp.iloc[:, 1].values):
                        sid = str(sid_raw).split('.')[0].strip().zfill(6)
                        pid = str(pid_val)
                        if sid in self.stock_info and pid in plate_names:
                            pname = plate_names[pid]
                            self.plate_map[sid] = pname
                            if pname not in self.inverse_plate_map: self.inverse_plate_map[pname] = []
                            if sid not in self.inverse_plate_map[pname]: self.inverse_plate_map[pname].append(sid)
                except: pass

            # 🛠️ 终极内存释放：清理板块加载过程中的所有临时对象
            if 'df_p' in locals(): del df_p
            if 'df_sp' in locals(): del df_sp
            if 'plate_names' in locals(): del plate_names
            import gc
            gc.collect()

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
