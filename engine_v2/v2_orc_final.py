"""
v2_orc_final.py
MarketEdge V7.2.1 Guardian (Strategic Alignment & Decision Hub)
Logic: AUCTION-TO-LIVE ALIGNMENT. Persistent Snapshots & Command Console.
"""
import sys
import os

# 强制将当前目录加入路径，解决 ModuleNotFoundError
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import json
import logging
import numpy as np
import redis
import aiohttp
import asyncio
import subprocess
import re
from types import SimpleNamespace
from datetime import datetime, timedelta, time as dt_time
import time
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd

# 环境对齐与服务发现
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path: sys.path.append(BASE_DIR)

from v2_auction_analyzer import AuctionAnalyzer, AuctionStock
from v2_metadata_provider import MetadataProvider
from v2_wrp_final import v2_core_bridge
from v2_data_lifecycle import DataLifecycleManager
from v2_final_retro import final_retro
from v2_data_fetcher import UnifiedDataFetcher
from web.services.trading_calendar_service import TradingCalendarService
from web.services.tdengine_service import TDengineService
from web.services.stock_kline_service import StockKLineService
from web.services.chip_batch_runner import ChipBatchRunner
from ai.API.api import UnifiedMarketDataFetcher
from ai.API.StockAnalyzer import StockAnalyzer
import aiohttp

# 日志配置
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("Guardian")

STATE_FILE = os.path.join(os.path.dirname(__file__), "data_state.json")

def _load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except: pass
    return {}

def _save_state(state: dict):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"本地状态存证失败: {e}")

class AuctionOrchestrator:
    def __init__(self):
        self.is_running = True
        # Redis 连接 (优先尝试远程 IP 确保跨设备对齐)
        self.redis = redis.Redis(host='115.190.156.240', port=6379, db=0, decode_responses=True)
        # 降级尝试：如果远程打不通，可以备选 127.0.0.1
        try:
            self.redis.ping()
            logger.info("✅ 成功连接至物理服务器 Redis (115.190.156.240)")
        except:
            self.redis = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
            logger.warning("⚠️ 物理 IP 连接失败，降级使用 127.0.0.1")
            
        self.calendar = TradingCalendarService()
        self.analyzer = AuctionAnalyzer()
        # 修正：根据环境动态设置数据路径
        data_dir = os.path.join(BASE_DIR, 'web', 'data')
        self.metadata = MetadataProvider(data_dir=data_dir)
        self.metadata.set_redis_client(self.redis)
        self.calendar = TradingCalendarService()
        self.tdengine = TDengineService()
        self.kline_service = StockKLineService()
        self.api_analyzer = StockAnalyzer()
        self.wencai = UnifiedMarketDataFetcher()
        # 初始化 V2 专用抓取器 (Session 将在启动时维持)
        self.fetcher: Optional[UnifiedDataFetcher] = None 
        self._session: Optional[aiohttp.ClientSession] = None
        self.is_running = True
        self.last_sentiment = 0.0
        self.is_first_session_run = True # 标记盘中首次有效运行
        self.state = _load_state()
        self.auction_snapshot = {} 
        self.bridge = v2_core_bridge
        self.auction_synced_date = None  # 竞价同步状态位
        
        # 定义适配 Lifecycle 的无参异步包装
        async def l_fetch_bans():
            yest = self.calendar.get_previous_trading_day(datetime.now().strftime("%Y-%m-%d"))
            self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest)
            return list(self.yest_limit_map.keys())
            
        async def l_fetch_plates():
            yest = self.calendar.get_previous_trading_day(datetime.now().strftime("%Y-%m-%d"))
            self.hot_plates = await self._fetch_kaipan_hot_plates(yest)
            # 修正：_fetch_kaipan_hot_plates 返回的是 List[Tuple[str, int]]
            return [p[0] for p in self.hot_plates]

        self.lifecycle = DataLifecycleManager(
            symbol_list=[], 
            fetch_yest_bans_fn=l_fetch_bans,
            fetch_yest_plates_fn=l_fetch_plates,
            fetch_daily_kline_fn=self._sync_stock_kline,
            fetch_dde_fn=self._sync_stock_dde,
            trigger_rust_calc_fn=self._trigger_factor_calc
        )

    async def _startup_sync(self, date_str: str):
        """启动时同步与环境预研"""
        # 1. 注入 Symbols
        logger.info(f"🔍 [System] 启动自检中 (基于 Lifecycle 任务)...")
        self.lifecycle.symbols = list(self.metadata.stock_info.keys())
        await self.lifecycle.on_startup()
        
        # 2. 预研情绪阶段
        self.last_sentiment = 0.0 # TODO: 从 Redis 恢复
        yest_str = self.calendar.get_previous_trading_day(date_str)
        self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest_str)
        self.hot_plates = await self._fetch_kaipan_hot_plates(yest_str)
        logger.info(f"[Metadata] Load Success: {len(self.yest_limit_map)} yest-limits, {len(self.hot_plates)} hot-plates")

        # 1.1 注册符号到 Rust Core (若 binary 可用)
        if self.bridge.engine:
            all_symbols = list(self.metadata.stock_info.keys())
            self.bridge.register_symbols(all_symbols)
            for p_name, _ in self.hot_plates:
                p_stocks = [s for s, plate in self.metadata.plate_map.items() if p_name in plate]
                self.bridge.register_plate_mapping(p_name, p_stocks)

        # 2. 补全 09:25 竞价锚点 (需满足：交易日、时间已过 09:25、今日尚未加载)
        if not self.calendar.is_trade_day(date_str):
            logger.info("🌑 [System] 今日非交易日，跳过竞价数据补全")
            return

        now = datetime.now()
        h_m = now.strftime("%H:%M")
        if h_m >= "09:25" and self.auction_synced_date != date_str:
            logger.info("⏳ [Anchor] 检测到已过竞价时间，正在补全开盘锚点...")
            date_sh = date_str.replace("-", "")
            auc_key = f"market:auction:{date_sh}:0925"
            auc_raw = self.redis.hget(auc_key, "top_amount")
            
            if auc_raw:
                items = json.loads(auc_raw)
                self.auction_snapshot = {str(i.get("symbol","")).strip()[-6:]: float(i.get("change_pct",0)) for i in items}
                logger.info(f"✅ [Anchor] Redis 锚点补全成功 ({len(self.auction_snapshot)} 条)")
            else:
                logger.warning(f"⚠️ [Anchor] Redis 缺失 09:25 数据，尝试通过 TDengine/Wencai 补全...")
                items = await self._fetch_tdengine_auction(date_str)
                if not items:
                    items = await self._fetch_wencai_auction(date_str)
                
                if items:
                    for it in items:
                        self.auction_snapshot[it["code"]] = it["change_pct"]
                    self.auction_synced_date = date_str
                    logger.info(f"✅ [Anchor] 降级补全成功 ({len(self.auction_snapshot)} 条)")
                else:
                    logger.error("❌ [Anchor] 补全失败，盘中推演逻辑将受限。")
            
            if self.auction_synced_date == date_str:
                logger.info("✅ [Anchor] 今日竞价锚点已就绪")

    async def _fetch_kaipan_limit_ups(self, date_str: str) -> Dict[str, AuctionStock]:
        kpl_date = date_str.replace("-", "")
        cache_key = f"market:yest_limit_up:{kpl_date}"
        data = self.redis.get(cache_key)
        if data:
            items = json.loads(data)
            return {item['code']: AuctionStock(**item) for item in items}
        try:
            sys.path.append('/usr/local/lib/python3.9/site-packages/pykaipan')
            from pykaipan.pykaipan import getHisBans
            yest_map = {}
            for b_lvl in ['1', '2', '3', '4', '5']:
                res = getHisBans(date=date_str, ban=b_lvl, size=100)
                pages = res.get('info', [])
                if not pages: continue
                for page in pages:
                    for s_rec in page:
                        if len(s_rec) < 20: continue
                        code, name, lb, plate = str(s_rec[0])[-6:], str(s_rec[1]), int(s_rec[15]), str(s_rec[12])
                        yest_map[code] = AuctionStock(code=code, name=name, lb_days=lb, is_yest_limit=True, plate=plate)
            if yest_map:
                cache_items = [{"code": s.code, "name": s.name, "lb_days": s.lb_days, "plate": s.plate} for s in yest_map.values()]
                self.redis.set(cache_key, json.dumps(cache_items), ex=3600*24*7)
                self.state["yest_limit_up_sync"] = datetime.now().strftime("%Y-%m-%d")
                _save_state(self.state)
                return yest_map
        except: pass
        return {}

    async def _fetch_kaipan_hot_plates(self, date_str: str) -> List[Tuple[str, int]]:
        kpl_date = date_str.replace("-", "")
        cache_key = f"market:yest_hot_plates:{kpl_date}"
        data = self.redis.get(cache_key)
        if data: return json.loads(data)
        try:
            sys.path.append('/usr/local/lib/python3.9/site-packages/pykaipan')
            from pykaipan.pykaipan import getHisPlates
            res = getHisPlates(date=date_str)
            p_list = res.get('list', [])
            if p_list:
                hot_plates = [(str(p[1]), int(p[4])) for p in p_list[:5]]
                self.redis.set(cache_key, json.dumps(hot_plates), ex=3600*24*7)
                return hot_plates
        except: pass
        return []

    @staticmethod
    def _parse_cn_number(val) -> float:
        """解析含中文单位的数值，如 '1234.56万' -> 12345600.0, '3.72亿' -> 372000000.0"""
        if val is None:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
        s = str(val).strip().replace(',', '')
        multiplier = 1.0
        if s.endswith('亿'):
            multiplier = 1e8
            s = s[:-1]
        elif s.endswith('万'):
            multiplier = 1e4
            s = s[:-1]
        elif s.endswith('%'):
            s = s[:-1]
            # 返回百分数原值，调用方决定是否除以100
            multiplier = 1.0
        try:
            return float(s) * multiplier
        except (ValueError, TypeError):
            return 0.0

    async def _standardize_item(self, row: Any, source: str, extra: Dict = None) -> Optional[Dict]:
        """
        统一模型数据清洗 (Standardization Pipeline)
        确保: code(6位), name(非unknown), change_pct(小数), amount(元)
        """
        try:
            if source == 'REDIS':
                code = str(row.get("symbol", row.get("code", ""))).strip()[-6:]
                name = row.get("name", "unknown")
                pct = float(row.get("change_pct", 0))
                # 兼容性修正：如果 Redis 存的是百分数(如 5.0)，转为小数(0.05)
                if abs(pct) > 1.0: pct /= 100.0
                amt = float(row.get("auction_amount_yuan", row.get("amount", 0)))
            elif source == 'TDENGINE':
                lp, v, a, symbol = row
                code = str(symbol).strip()[-6:]
                name = "unknown"
                pc = (extra or {}).get(code, 0)
                pct = (float(lp or 0) / pc - 1.0) if pc > 0 else 0.0
                amt = float(a or 0)
            elif source == 'WENCAI':
                code = str(row.get('code', row.get('symbol', ''))).strip()[-6:]
                name = str(row.get('name', 'unknown'))
                pct = self._parse_cn_number(row.get('pct', 0)) / 100.0  # 问财返回百分数
                amt = self._parse_cn_number(row.get('amt', 0))  # 可能带“万”/“亿”单位
            else: return None

            if name == "unknown" or not name:
                name = await self.metadata.get_name(code)
            
            # 健壮性过滤：剔除无法识别的代码和极端的数值噪音
            if not code.isdigit() or len(code) != 6: return None
            if abs(pct) > 0.25: return None 
            
            return {
                "code": code, "name": name, 
                "change_pct": pct, "auction_amount_yuan": amt
            }
        except Exception as e:
            return None

    async def _get_pre_close_map(self, prev_date: str) -> Dict[str, float]:
        """批量获取前一交易日的收盘价作为次日 pre_close"""
        # 由于是 fallback 场景，允许一次性大批量查询，后续可考虑 session 级别的 LRU 缓存
        sql = f"SELECT LAST(close), symbol FROM daily_kline WHERE ts = '{prev_date} 00:00:00' GROUP BY symbol"
        pre_close_map = {}
        try:
            loop = asyncio.get_event_loop()
            cursor = await loop.run_in_executor(None, self.tdengine.execute_query, sql)
            if not cursor: return {}
            rows = cursor.fetchall()
            for row in rows:
                val, symbol = row
                code = str(symbol).strip()[-6:]
                pre_close_map[code] = float(val or 0)
        except Exception as e:
            logger.warning(f"⚠️ [Data] 获取前收盘价失败 ({prev_date}): {e}")
        return pre_close_map

    async def _fetch_tdengine_auction(self, date_str: str) -> List[Dict]:
        """方案 1: TDengine 截面恢复 (09:25-09:30)"""
        # 1. 准备 pre_close 参考系 (修正数值偏移)
        prev_date = self.calendar.get_previous_trade_day(date_str)
        pre_close_map = await self._get_pre_close_map(prev_date)
        
        # 2. 查询 09:25 稳定快照
        start = f"{date_str} 09:25:00"
        end = f"{date_str} 09:30:00"
        sql = f"SELECT LAST(lp), LAST(v), LAST(a), symbol FROM stock_data WHERE ts >= '{start}' AND ts <= '{end}' GROUP BY symbol"
        try:
            loop = asyncio.get_event_loop()
            cursor = await loop.run_in_executor(None, self.tdengine.execute_query, sql)
            if not cursor: return []
            rows = cursor.fetchall()
            items = []
            for row in rows:
                item = await self._standardize_item(row, 'TDENGINE', extra=pre_close_map)
                if item: items.append(item)
            if items: logger.info(f"✅ [TDengine] 成功恢复 {len(items)} 条竞价截面数据 (归一化清洗完毕)。")
            return items
        except Exception as e:
            logger.warning(f"⚠️ [TDengine] 竞价回溯查询失败: {e}")
        return []

    async def _fetch_wencai_auction(self, date_str: str) -> List[Dict]:
        """方案 2: 问财分层抓取 (市值分层 + 竞价>500万)"""
        mc_segments = [
            f"{date_str}市值>1000亿;竞价金额>500万;竞价金额;竞价涨跌幅",
            f"{date_str}市值500亿到1000亿;竞价金额>500万;竞价金额;竞价涨跌幅",
            f"{date_str}市值100亿到500亿;竞价金额>500万;竞价金额;竞价涨跌幅",
            f"{date_str}市值<100亿;竞价涨跌幅>1%;竞价金额>500万;竞价金额",
        ]
        items = []
        seen = set()
        logger.info(f"⏳ [Wencai] 启动分层抓取流水线...")
        for q in mc_segments:
            try:
                df = await self.wencai.get_wencai_data(q)
                if df is None or df.empty: continue
                cols = df.columns.tolist()
                symbol_col = next((c for c in cols if '代码' in str(c)), cols[0])
                name_col = next((c for c in cols if '股票简称' in str(c) or '名称' in str(c)), None)
                amt_col = next((c for c in cols if '竞价金额' in str(c)), None)
                chg_col = next((c for c in cols if '竞价涨跌幅' in str(c)), None)
                for _, row in df.iterrows():
                    code = str(row[symbol_col]).strip()[-6:]
                    if code in seen or not code.isdigit(): continue
                    seen.add(code)
                    
                    # 使用 _parse_cn_number 安全解析
                    raw_pct = row.get(chg_col) if chg_col else 0
                    raw_amt = row.get(amt_col) if amt_col else 0
                    raw_name = str(row.get(name_col, 'unknown')) if name_col else 'unknown'
                    
                    raw_wc = {
                        'code': code,
                        'name': raw_name,
                        'pct': raw_pct,
                        'amt': raw_amt
                    }
                    item = await self._standardize_item(raw_wc, 'WENCAI')
                    if item: items.append(item)
                await asyncio.sleep(0.5) 
            except Exception as e:
                logger.warning(f"⚠️ [Wencai] 分段抓取异常 ({q[:30]}...): {e}")
        if items: logger.info(f"✅ [Wencai] 分层抓取完毕，集成 {len(items)} 条高净值竞价数据。")
        return items

    async def execute_analysis(self, date_str: str, mode: str = "AUCTION"):
        date_sh = date_str.replace("-", "")
        current_data = []
        
        # 1. 尝试从 Rust Core 获取实时快照 (Tier 0)
        rust_snap = {}
        if mode == "INTRA_DAY" and self.bridge.engine:
            rust_snap = self.bridge.get_snapshot()
            if rust_snap:
                logger.debug(f"⚡ [Rust] 成功拉取 {len(rust_snap)} 条实时快照")

        # 1. 动态对齐昨日数据基因
        if not self.yest_limit_map or not self.hot_plates:
            yest_str = self.calendar.get_previous_trade_day(date_str)
            self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest_str)
            self.hot_plates = await self._fetch_kaipan_hot_plates(yest_str)

        # 2. 获取参考坐标系 (Pre-Close) 用于涨幅精算
        prev_date = self.calendar.get_previous_trade_day(date_str)
        pre_close_map = await self._get_pre_close_map(prev_date)

        # 2.1 关键逻辑：如果 Redis 实时源失效，尝试从 Rust 内存镜像直接构建底池 (Resonance V5 穿透逻辑)
        data_sh = date_str.replace("-", "")
        base_key = f"market:auction:{data_sh}:0925" if mode == "AUCTION" else f"market:auction:{data_sh}:latest"
        matched_keys = self.redis.keys(f"{base_key}*")
        data_key = matched_keys[0] if matched_keys else base_key
        
        try:
            r_type = self.redis.type(data_key)
            raw_data = None
            if r_type == 'hash':
                raw_data = self.redis.hget(data_key, "top_amount")
            elif r_type == 'string':
                raw_data = self.redis.get(data_key)
            
            if raw_data:
                raw_items = json.loads(raw_data)
                for raw_it in raw_items:
                    item = await self._standardize_item(raw_it, 'REDIS')
                    if item: current_data.append(item)
                logger.info(f"✅ [Data] Redis 源读取成功: {data_key} ({len(current_data)} 条)")
        except Exception as re_err:
            logger.debug(f"ℹ️ [Data] Redis 探测跳过 (Key: {data_key}): {re_err}")
            
        # 2.1 关键逻辑：如果 Redis 实时源失效，尝试从 Rust 内存镜像直接构建底池 (Resonance V5 穿透逻辑)
        if not current_data and mode == "INTRA_DAY" and rust_snap:
            logger.debug(f"⚡ [Simd] Redis 实时源缺失，正在从 Rust 内存镜像直接穿透采样 ({len(rust_snap)} 条)...")
            rust_idx = {str(k).strip()[-6:]: v for k, v in rust_snap.items() if k != "_EXTREMES_"}
            for code, r_it in rust_idx.items():
                if code == "_EXTREMES_": continue
                # 1. 基础信息
                name = await self.metadata.get_name(code)
                yest_it = self.yest_limit_map.get(code)
                
                # 2. 涨幅精算：(当前价 / 昨收) - 1.0 (修正 10.0 分情绪报错)
                pre_close = pre_close_map.get(code, 0.0)
                curr_price = r_it.get("price", 0.0)
                pct = (curr_price / pre_close - 1.0) if pre_close > 0.1 else 0.0
                
                # 3. 基因补完：板块属性
                stock_info = self.metadata.stock_info.get(code)
                plate = stock_info.get('plate', 'Other') if stock_info else "Other"
                
                current_data.append({
                    "code": code, "name": name,
                    "change_pct": pct,
                    "auction_amount_yuan": r_it.get("amount", 0.0),
                    "speed_1m": r_it.get("speed", 0.0),
                    "vol_intensity": r_it.get("vol_intensity", 1.0),
                    "plate": plate,
                    "lb_days": yest_it.lb_days if yest_it else 0,
                    "is_yest_limit": True if yest_it else False
                })
            if current_data: 
                logger.info(f"✅ [Simd] Rust 镜像底池构建成功: {len(current_data)} 条")
        
        # 2.2 传统三级降级 (TDengine / Wencai)
        if not current_data:
            h_m_now = datetime.now().strftime("%H:%M")
            if h_m_now >= "09:25" and self.auction_synced_date != date_str:
                logger.warning(f"⚠️ [Data] 实时源全线缺失，尝试从 TDengine 恢复竞价基础...")
                td_items = await self._fetch_tdengine_auction(date_str)
                if td_items: 
                    current_data.extend(td_items)
                    self.auction_synced_date = date_str # 成功回溯一次也算完成
                
                # 方案 2: Wencai (如果 TDengine 依然不足)
                if len(current_data) < 50:
                    wc_items = await self._fetch_wencai_auction(date_str)
                    if wc_items: 
                        current_data.extend(wc_items)
                        self.auction_synced_date = date_str
            else:
                if self.auction_synced_date == date_str and mode == "INTRA_DAY":
                    logger.debug(f"ℹ️ [Data] Redis 实时源缺失，但今日竞价已锚定，分析挂载中。")
                else:
                    logger.debug(f"⏳ [Data] 当前时刻 {h_m_now} 尚早，跳过降级查询。")

        # 2.3 合并 Rust 实时指标到 current_data (若 current_data 非空)
        if rust_snap and current_data and not any(it.get("speed_1m") for it in current_data[:10]):
            # 仅当 current_data 中还没有指标时才执行合并逻辑 (防止重复合并导致性能浪费)
            rust_idx = {str(k).strip()[-6:]: v for k, v in rust_snap.items() if k != "_EXTREMES_"}
            for it in current_data:
                code = it["code"]
                if code in rust_idx:
                    r_it = rust_idx[code]
                    it["speed_1m"] = r_it.get("speed", 0.0)
                    it["vol_intensity"] = r_it.get("vol_intensity", 1.0)
                    if it.get("change_pct") is None or it.get("change_pct") == 0:
                        it["change_pct"] = r_it.get("price", 0.0)
        
        if not current_data:
            if datetime.now().second % 60 < 2:
                logger.warning(f"❌ [Data] 所有数据源均失效，跳过本次分析。")
            return

        if mode == "AUCTION":
            for it in current_data:
                self.auction_snapshot[it["code"]] = it["change_pct"]

        # 3. 语义分析
        report = await self.analyzer.analyze(
            current_data, 
            auction_snapshot=self.auction_snapshot if mode == "INTRA_DAY" else None,
            yest_limit_map=self.yest_limit_map, 
            yest_hot_plates=self.hot_plates, date_str=date_str
        )
        
        # 4. 智能输出
        # 增加判断：如果是盘中首次有效运行，强制输出 summary
        is_trigger_change = abs(report.money_making_effect - self.last_sentiment) >= 0.5
        if mode == "AUCTION" or self.is_first_session_run or is_trigger_change:
            print(f"\n{report.summary_text}\n")
            self.last_sentiment = report.money_making_effect
            self.is_first_session_run = False # 首次输出后关闭开关

    async def run_guardian(self):
        logger.info("🛡️ MarketEdge V7.2.1 Strategic Guardian (对准增强版) 物理上线")
        date_str = datetime.now().strftime("%Y-%m-%d")
        self._today = date_str # 绑定全局日期上下文

        # 初始化 Fetcher Session
        self._session = aiohttp.ClientSession()
        self.fetcher = UnifiedDataFetcher(self._session)

        # 0. 启动即同步 (根据 Lifecycle 内部逻辑决策全量或轻量)
        await self._startup_sync(date_str)
        
        # [NEW] 启动异步行情提取泵并预热
        pump_task = asyncio.create_task(self._v2_tick_pump())
        logger.info("⏳ [System] 正在等待行情泵首轮预热 (2s)...")
        await asyncio.sleep(2)
        
        # 1. 尝试初始推演 (仅在 09:15 - 15:31 活跃时段执行)
        now_hm = datetime.now().strftime("%H:%M")
        if "09:15" <= now_hm <= "15:31":
            mode = "AUCTION" if now_hm < "09:30" else "INTRA_DAY"
            logger.info(f"📊 [WarmStart] 正在生成初始推演战报 ({mode})...")
            await self.execute_analysis(date_str, mode=mode)
        else:
            logger.info(f"🌑 [WarmStart] 非交易活跃时段启动，静默进入监听模式。")

        while self.is_running:
            now = datetime.now()
            if now.weekday() >= 5:
                # 修正：周末增加长休眠
                await asyncio.sleep(300)
                continue

            h_m = now.strftime("%H:%M")
            date_str = now.strftime("%Y-%m-%d")
            
            if h_m == "09:26":
                await self.execute_analysis(date_str, mode="AUCTION")
                await asyncio.sleep(60)

            elif h_m == "15:31":
                logger.info("🏁 [EOD] 盘后结算与数据复盘开始...")
                await self.lifecycle.on_eod()
                
                # 盘后报告 (Legacy 迁移)
                try:
                    subprocess.Popen([sys.executable, "v3_final_review.py"])
                except Exception as e:
                    logger.error(f"Failed to start review engine: {e}")
                
                await asyncio.sleep(60)

            elif "09:30" <= h_m <= "15:30":
                if now.minute % 3 == 0 and now.second < 5:
                    await self.execute_analysis(date_str, mode="INTRA_DAY")
                
                # 实时状态心跳
                if now.second % 10 == 0:
                    sys.stdout.write(f"\r[{h_m}] 🩺 战役对准执行中 | 实时情绪:{self.last_sentiment}/10   ")
                    sys.stdout.flush()
                await asyncio.sleep(1)
            else:
                # 非交易时段心跳 (Night Mode)
                if now.second % 60 == 0:
                    logger.info(f"🌑 [NightMode] 引擎基盘在线 | 时刻: {h_m} | 等待下一交易日")
                await asyncio.sleep(1)

    async def _v2_tick_pump(self):
        """[Resonance V5] 极速行情提取泵：将 stock:quote:* 直接按需压入本进程 Rust Core"""
        logger.info("🚀 [Pump] 智库行情提取泵已就位 (Shared-Memory Mode)")
        
        # 预生成 Redis Key 列表以优化性能 (根据 metadata 中的 5000+ 代码)
        all_symbols = list(self.metadata.stock_info.keys())
        key_map = {f"stock:quote:{s}": s for s in all_symbols}
        redis_keys = list(key_map.keys())
        
        while self.is_running:
            try:
                now = datetime.now()
                # 仅在交易活跃时段运行 (09:15 - 11:35, 12:55 - 15:10)
                h_m = now.strftime("%H:%M")
                if not ("09:15" <= h_m <= "11:35" or "12:55" <= h_m <= "15:10"):
                    await asyncio.sleep(60)
                    continue
                
                t_start = time.time()
                # 批量拉取 (Pipeline 避免阻塞)
                pipe = self.redis.pipeline()
                for k in redis_keys:
                    pipe.hgetall(k)
                all_quotes = pipe.execute()
                
                # 压入 Rust 
                push_count = 0
                for k, data in zip(redis_keys, all_quotes):
                    if not data: continue
                    symbol = key_map[k]
                    # 提取核心指标
                    try:
                        price = float(data.get('price', data.get('current', 0)))
                        amount = float(data.get('amount', data.get('turnover', 0)))
                        volume = float(data.get('volume', 0))
                        self.bridge.push_tick_raw(symbol, price, amount, volume)
                        push_count += 1
                    except: continue
                
                t_end = time.time()
                if push_count > 0:
                    logger.debug(f"⚡ [Pump] 成功压入 {push_count} 只 Tick | 耗时: {(t_end-t_start)*1000:.2f}ms")
                
                # 呼吸周期 (3秒抓一次，平衡性能与实时性)
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"❌ [Pump] 运行异常: {e}")
                await asyncio.sleep(5)

    # ─────────────────────────────────────────────────────────────────────────────
    # Kaipanla 接口适配
    # ─────────────────────────────────────────────────────────────────────────────
    
    async def _fetch_kaipan_limit_ups(self, date: str) -> Dict[str, Any]:
        """抓取开盘啦涨停列表 (全量扫描版)"""
        logger.info(f"正在全量扫描昨日涨停梯队 ({date})...")
        full_pool = {}
        try:
            # 探测不同 PidType (1=1板, 2=2板... 10=极限板)
            # 由于 pykaipan 定义冲突，我们尽量拉取更多梯队
            for ban_type in range(1, 11):
                res = await asyncio.get_event_loop().run_in_executor(
                    None, self.api_analyzer._call_api, 'getHisBans', date, str(ban_type), 50
                )
                if not res or 'info' not in res or len(res['info']) == 0: continue
                
                data_list = res['info'][0]
                for it in data_list:
                    # 修正过滤阈值 (实测数组长度为 23)
                    if not isinstance(it, list) or len(it) < 20: continue
                    symbol = it[0]
                    
                    # 语义化解析连板高度 (解析 it[18] 中的 "N天M板" 或 "N连板")
                    desc = str(it[18]) if len(it) > 18 else ""
                    height = ban_type # 默认使用当前扫描的梯队类型
                    
                    if "首板" in desc:
                        height = 1
                    else:
                        match = re.search(r"(\d+)板", desc)
                        if match:
                            height = int(match.group(1))
                    
                    # 构造 Analyzer 预期的属性对象: it[5] 是板块
                    full_pool[symbol] = SimpleNamespace(
                        lb_days=height,
                        plate=it[5] if (len(it) > 5 and isinstance(it[5], str)) else "Other",
                        is_yest_limit=True
                    )
            
            logger.info(f"✅ 全量扫描完成: 捕获 {len(full_pool)} 只昨日涨停股")
            return full_pool
        except Exception as e:
            logger.error(f"❌ Full Ban Scan Error: {e}")
            return {}

    async def _fetch_kaipan_hot_plates(self, date: str) -> List[Tuple[str, int]]:
        """抓取开盘啦热门板块 (兼容新版数组结构)"""
        logger.info(f"获取昨日热门板块 ({date})...")
        try:
            res = await asyncio.get_event_loop().run_in_executor(
                None, self.api_analyzer.get_his_plates, date
            )
            # 新版结构: res['list'] 是数组列表
            if not res or 'list' not in res: return []
            data_list = res['list']
            
            # 索引映射: [1] PlateName
            return [(it[1], i+1) for i, it in enumerate(data_list[:10])]
        except Exception as e:
            logger.error(f"Fetch Plate Error: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────────
    # 数据生命周期适配器 (Lifecycle Adapters)
    # ─────────────────────────────────────────────────────────────────────────────

    async def _sync_stock_kline(self, symbol: str) -> bool:
        """Lifecycle 适配器：同步单只股票日K线 (切到 Baostock)"""
        try:
            # 使用内部带增量同步与 TDengine 持久化的 K 线服务
            # start_date=None / end_date=None 会触发默认 120 交易日增量检查
            data = await asyncio.get_event_loop().run_in_executor(
                None, self.kline_service.fetch_kline_data, symbol, 'd'
            )
            
            # 策略：分级保护核心数据 (上海/深圳主板 vs 北交所)
            if data and len(data) > 0:
                return True
            
            # 如果没抓到数据，判断是否为主板核心板块 (6/0/3 开头)
            if symbol.startswith(('6', '0', '3')):
                # 主板数据缺失 -> 严格模式，返回 False 触发重试/警告
                logger.warning(f"⚠️ [Strict] 主板个股数据缺失 {symbol}, 触发重试...")
                return False
            else:
                # 北交所或其他非核心板块 -> 兼容模式，返回 True (跳过)
                return True
        except Exception as e:
            logger.error(f"Sync KLine Exception ({symbol}): {e}")
            return False

    async def _sync_stock_dde(self, symbol: str) -> bool:
        """Lifecycle 适配器：同步单只股票DDE (20天)"""
        try:
            raw_code = symbol.split(".")[0]
            # 锁定目标日期：DDE 0点更新，盘中结算前（16:30前）始终抓取上一个交易日
            now = datetime.now()
            if now.hour < 16 or (now.hour == 16 and now.minute < 30):
                target_date = self.calendar.get_previous_trading_day(self._today).replace("-", "")
            else:
                target_date = self._today.replace("-", "")
            
            # 获取历史 DDE (从 pykaipan/StockAnalyzer)
            res = await asyncio.get_event_loop().run_in_executor(
                None, self.api_analyzer.get_his_stock_dde, raw_code, target_date
            )
            if not res or res.get('errcode') != '0':
                return False
                
            # 智能提取：过滤标量(errcode等)，保留列表(DDJE, Date等)
            data_dict = {}
            max_len = 0
            for k, v in res.items():
                if isinstance(v, list):
                    data_dict[k] = v
                    max_len = max(max_len, len(v))
            
            if max_len == 0:
                # 没有任何列表数据，视为今日空数据，标记成功并跳过
                return True
                
            # 转为 DataFrame
            df = pd.DataFrame(data_dict)
            
            # 策略：获取近 40 个交易日作为缓冲区，最终保留最近 20 个交易日存入
            # 这能有效处理因停牌或非交易日导致的近 20 天数据不足 20 行的情况
            df = df.head(40).head(20)
            
            # 统一字段名以适配 TDengineService.save_daily_dde
            # 注意：pykaipan 返回的是 'Date', 'DDJE' 等首字母大写的键
            df.rename(columns={
                'DDJE': 'ddje', 'large_net': 'large_net',
                'Date': 'date', 'DDX': 'ddx', 'DDY': 'ddy', 'DDZ': 'ddz'
            }, inplace=True)
            
            # 存入 TDengine
            ok = self.tdengine.save_daily_dde(symbol, df)
            # if ok:
            #     logger.info(f"✅ [DDE] {symbol} 同步成功 ({len(df)} 行)")
            return ok
        except Exception as e:
            logger.error(f"❌ [DDE] Sync Error ({symbol}): {e}")
            return False

    async def _trigger_factor_calc(self) -> bool:
        """Lifecycle 适配器：触发盘后核心计算 (筹码 + 多因子)"""
        logger.info("🚀 [EOD] 启动盘后核心算法计算 (ChipBatchRunner)...")
        try:
            # 锁定目标日期：盘前/盘中始终锁定上一个交易日，结算后(15:31)才算当天
            now = datetime.now()
            if now.hour < 15 or (now.hour == 15 and now.minute < 30):
                target_date = self.calendar.get_previous_trading_day(self._today)
            else:
                target_date = self._today
            
            # 1. 触发筹码、真市值与多因子增量计算 (Redis 缓存化)
            runner = ChipBatchRunner(kline_service=self.kline_service)
            await asyncio.get_event_loop().run_in_executor(
                None, runner.run_batch, target_date
            )
            
            # 2. 注入 Rust 底层进行初始化 (为次日对准做准备)
            symbols = list(self.metadata.stock_info.keys())
            if symbols:
                self.bridge.register_symbols(symbols)
                logger.info(f"✅ [EOD] Rust 底层 symbols 注入完成: {len(symbols)} 个")
            
            logger.info(f"✅ [EOD] 盘后核心计算闭环完成 (目标日期: {target_date})")
            return True
        except Exception as e:
            logger.error(f"❌ [EOD] 因子计算触发异常: {e}")
            return False

if __name__ == "__main__":
    orc = AuctionOrchestrator()
    try:
        asyncio.run(orc.run_guardian())
    except KeyboardInterrupt:
        logger.info("👋 智库已平滑下岗")
    finally:
        if orc._session:
            asyncio.run(orc._session.close())
