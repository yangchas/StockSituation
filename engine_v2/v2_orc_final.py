"""
v2_orc_final.py
MarketEdge V7.2.1 Guardian (Strategic Alignment & Decision Hub)
Logic: AUCTION-TO-LIVE ALIGNMENT. Persistent Snapshots & Command Console.
"""
import sys
import traceback
import os
import json
import logging
import numpy as np
import redis.asyncio as redis
import aiohttp
import asyncio
import subprocess
import re
from types import SimpleNamespace
from datetime import datetime, timedelta, time as dt_time
import time
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd
from collections import deque
# 环境对齐与服务发现
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path: sys.path.append(BASE_DIR)
from v2_auction_analyzer import AuctionAnalyzer, AuctionStock
from v2_infra_provider import get_global_redis, get_global_tdengine # 🚀 新增单例注入
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
from engine_v2.v2_prime_logic import ResonancePrimeService
from v2_risk_controller import AuctionEntryChecker, RiskAction
# 日志配置 (服务器增强版)
log_dir = os.path.join(BASE_DIR, "logs")
if not os.path.exists(log_dir): os.makedirs(log_dir)
log_file = os.path.join(log_dir, "guardian.log")
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
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
        self.redis = redis.from_url("redis://127.0.0.1:6379/0", decode_responses=True)
        try:
            # ping 在 aioredis 中现在是 coroutine
            # asyncio.create_task(self.redis.ping())
            logger.info("✅ 异步 Redis 驱动初始化完成")
        except:
            # self.redis = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
            logger.warning("⚠️ 物理 IP 连接失败，降级使用 127.0.0.1")
        self.calendar = TradingCalendarService()
        self.redis = None # 🚀 [V30.1] 延迟注入单例
        self.analyzer = AuctionAnalyzer(redis_client=None)
        # 🛠️ 暴力路径探测：从当前目录递归向上查找 3 级，直到找到 web/data/f10.csv
        data_dir = ""
        search_root = BASE_DIR
        # 1. 整理特征向量...
        for _ in range(3):
            candidate = os.path.join(search_root, 'web', 'data')
            if os.path.exists(os.path.join(candidate, 'f10.csv')):
                data_dir = candidate
                break
            search_root = os.path.dirname(search_root)
        if not data_dir:
            # 最后的倔强：尝试绝对路径兜底 (d:/work/Go/web/data)
            fallback = "d:/work/Go/web/data"
            data_dir = fallback if os.path.exists(fallback) else os.path.join(BASE_DIR, 'web', 'data')
        # print(f"📍 [] 元数据物理路径确定: {data_dir}")
        logger.info(f"📍 [System] 元数据路径定位: {data_dir}")
        self.metadata = MetadataProvider(data_dir=data_dir)
        self.metadata.set_redis_client(self.redis)
        # 🛠️ 终极降压：将 heavy services 改为延迟加载，避免启动瞬间内存峰值重叠
        self._tdengine = get_global_tdengine() # 🚀 [V30.1] 强制单例注入
        self.kline_service = StockKLineService()
        self.api_analyzer = None # [V31.1] 延迟加载防止泄露
        self.wencai = None # [V31.1] 延迟加载防止泄露
        self._session = None
        self.chip_runner = ChipBatchRunner(kline_service=self.kline_service) 
        self.bridge = v2_core_bridge
        self.lifecycle = DataLifecycleManager(self.bridge, None)
        self.prime = ResonancePrimeService(None)
        # 战时状态管理
        self.last_sentiment = 0.0
        self.yest_limit_map = {}
        self.alpha_candidates = {} # [V38.2] 物理前置
        self.battle_kpis = {} # [V35.9] 物理占位 # [V35.6] 物理防坠机
        self.highest_board = None # V5.5 实时龙头监控位
        self.is_first_session_run = True 
        self.state = _load_state()
        self.tick_history = {} 
        self.risk_stats = {} # [V9.5] 记录各票盘中历史最高价/2min前价格
        self.pre_close_map = {} # [P0 Fix] 缓存昨收价用于风控精算
        # [V15.0] 生命周期状态管理：支持实战不重启捕获与 Redis 归档对位
        self.auction_snapshot = {}
        self.auction_synced_date = ""
        self.last_report = None # 🚀 [V39.1] 持久化最后一份分析报告，防止 NightMode 冲刷
        # 定义适配 Lifecycle 的异步包装
        async def l_fetch_bans():
            yest = self.calendar.get_previous_trading_day(datetime.now().strftime("%Y-%m-%d"))
            self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest)
            return list((self.yest_limit_map or {}).keys())
        async def l_fetch_plates():
            yest = self.calendar.get_previous_trading_day(datetime.now().strftime("%Y-%m-%d"))
            self.hot_plates = await self._fetch_kaipan_hot_plates(yest)
            return [p[0] for p in self.hot_plates]
        self.lifecycle = DataLifecycleManager(
            symbol_list=[], 
            fetch_yest_bans_fn=l_fetch_bans,
            fetch_yest_plates_fn=l_fetch_plates,
            fetch_daily_kline_fn=self._sync_and_calculate_stock,
            fetch_dde_fn=self._sync_stock_dde,
            trigger_rust_calc_fn=self._trigger_factor_calc,
            check_status_fn=self._physical_check_all_dimensions # 🚀 [V39.2] 统一物理校验逻辑
        )
        self.watermarks: Dict[str, Dict[str, str]] = {} # 🚀 [V39.2] 水位内存映射：kline, dde, factor
        self._session = None # 🚀 [V26.2] 初始化预定义，防止清理时报错
    @property
    def tdengine(self):
        """懒加载 TDengine，节省 26MB 启动内存"""
        if self._tdengine is None:
            logger.info("📡 [Service] 正在建立 TDengine 物理连接 (Delayed Logic)...")
            self._tdengine = TDengineService()
        return self._tdengine
    async def _startup_sync(self, date_str: str):
        """启动时同步与环境预研 (内存加固版)平衡"""
        logger.info(f"🔍 [System] 启动自绘自检中 (基于并行加速)...")
        # 补全：获取昨收基准映射
        yest_str = self.calendar.get_previous_trading_day(date_str)
        self.pre_close_map = await self._get_pre_close_map(yest_str)
        logger.info(f"📈 [P0] 已加载 {len(self.pre_close_map)} 只证券的基准价 (Anchor Date: {yest_str})")
        # 🛠️ 优化 1: 仅生成一次全市场代码列表，并跳过北交所 (根据用户指令)
        all_symbols = [
            s for s in self.metadata.stock_info.keys() 
            if not s.startswith(('83', '87', '88', '43', '920'))
        ]
        self.lifecycle.symbols = all_symbols
        await asyncio.get_event_loop().run_in_executor(None, self.kline_service.preload_latest_dates)
        await self.lifecycle.on_startup()
        yest_str = self.calendar.get_previous_trading_day(date_str)
        self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest_str)
        self.hot_plates = await self._fetch_kaipan_hot_plates(yest_str)
        # 🛠️ 优化 3: 异步加载“Alpha种子池”(盘后预计算结果)
        self.alpha_candidates = {}
        raw_candidates = await self.redis.get("market:alpha:candidates")
        if raw_candidates:
            try:
                c_list = json.loads(raw_candidates)
                self.alpha_candidates = {c['code']: c for c in c_list}
                logger.info(f"✅ [Alpha Pool] 成功加载 {len(self.alpha_candidates)} 个低位种子股")
            except: pass
        logger.info(f"[Metadata] Load Success: {len(self.yest_limit_map)} yest-limits, {len(self.hot_plates)} hot-plates")
        if self.bridge.engine:
            self.bridge.register_symbols(all_symbols)
            for p_name, _ in self.hot_plates:
                # 🛠️ 优化 2: 核心收益——改 O(N) 全场扫描为 O(1) 反向索引，秒注册板块
                p_stocks = self.metadata.inverse_plate_map.get(p_name, [])
                if p_stocks:
                    self.bridge.register_plate_mapping(p_name, p_stocks)
        # [V13.5] 移除硬拦截，允许非交易日加载历史锚点用于复盘/模拟
        if not self.calendar.is_trade_day(date_str):
            logger.info("🌑 [System] 检测到非交易日启动 (模拟模式)...")
        now = datetime.now()
        h_m = now.strftime("%H:%M")
        if h_m >= "09:25" and self.auction_synced_date != date_str:
            # [V13.0] 增强型冷启动回赎链路
            date_sh = date_str.replace("-", "")
            # 优先级 1: 专用高性能锚点归档 (New)
            new_auc_key = f"market:auction:anchor:{date_sh}"
            raw_new = await self.redis.get(new_auc_key)
            if raw_new:
                try:
                    self.auction_snapshot = json.loads(raw_new)
                    logger.info(f"✅ [Anchor Recovery] 成功从归档回赎 {len(self.auction_snapshot)} 条竞价锚点 (High Fidelity)")
                    return
                except Exception as e:
                    logger.warning(f"⚠️ [Anchor Recovery] 归档解析失败: {e}")
            # 优先级 2: 原始行情快照数据 (Legacy)
            auc_key = f"market:auction:{date_sh}:0925"
            auc_raw = await self.redis.hget(auc_key, "top_amount")
            items_found = False
            if auc_raw:
                try:
                    items = json.loads(auc_raw)
                    self.auction_snapshot = {}
                    for i in items:
                        if not isinstance(i, dict): continue
                        raw_sym = str(i.get("symbol", i.get("code", ""))).strip()
                        if not raw_sym: continue
                        code = raw_sym.split('.')[0][-6:] if '.' in raw_sym else raw_sym[-6:]
                        pct = float(i.get("change_pct", 0))
                        price = float(i.get("price", 0))
                        if abs(pct) < 1e-6 and price > 0:
                            val = self.pre_close_map.get(code, 0)
                            pc = val[0] if isinstance(val, (tuple, list)) else float(val)
                            if pc > 0: pct = price / pc - 1.0
                        if code: self.auction_snapshot[code] = pct
                    if len(self.auction_snapshot) > 100:
                        self.auction_synced_date = date_str
                        logger.info(f"✅ [Anchor Recovery] 成功从 Redis 归档对位 ({len(self.auction_snapshot)} 条)")
                        items_found = True
                except: pass
            if items_found: return
            # [V16.0] 优先级 3: TDengine 数据库回补 (次级兜底)
            logger.info("⏳ [Resilience] Redis 本地归档缺失，正在切入 TDengine 数据库兜底链路...")
            td_items = await self._fetch_tdengine_auction(date_str)
            if td_items:
                self.auction_snapshot = {
                    str(s.get('code', '')).split('.')[0][-6:]: s.get('change_pct', 0) 
                    for s in td_items if s.get('code')
                }
                if len(self.auction_snapshot) > 100:
                    self.auction_synced_date = date_str
                    logger.info(f"✅ [Anchor Recovery] 成功从 TDengine 补全数据 ({len(self.auction_snapshot)} 条)")
                    return
            # [V16.0] 优先级 4: 问财 API 强制回补 (终极生命线)
            logger.info("📡 [Resilience] 内部数据源全线失守，正在发起问财 API 终极空投指令...")
            wc_items = await self._fetch_wencai_auction(date_str)
            if wc_items:
                self.auction_snapshot = {
                    str(s.get('code', '')).split('.')[0][-6:]: s.get('change_pct', 0) 
                    for s in wc_items if s.get('code')
                }
                if len(self.auction_snapshot) > 50: # 问财源通常较小，50条即可支撑核心池
                    self.auction_synced_date = date_str
                    logger.info(f"✅ [Anchor Recovery] 问财 API 终极补全成功 ({len(self.auction_snapshot)} 条)")
                    return
            logger.error("❌ [Anchor Recovery] 灾难性故障：四级回赎链路全部落空，请人工介入检查数据源。")
        # 🛠️ 终极自检后垃圾回收：清理启动阶段产生的所有临时 Symbols 列表和中转对象
        import gc
        gc.collect()
    # ─────────────────────────────────────────────────────────────────────────────
    # Kaipanla 接口适配 (V3.5 统一网关版)
    # ─────────────────────────────────────────────────────────────────────────────
    async def _fetch_kaipan_limit_ups(self, date: str) -> Dict[str, Any]:
        """抓取开盘啦涨停列表 (V3.5 统一网关版)"""
        logger.info(f"正在通过统一网关请求昨日涨停梯队 ({date})...")
        full_pool = {}
        try:
            # 调用统一网关 (内部已完成 1-5 板扫描、Index [15]/[12] 黄金解析)
            raw_pool = await asyncio.get_event_loop().run_in_executor(
                None, self.api_analyzer.get_history_bans_pool, date, 5
            )
            for item in raw_pool:
                raw_sym = item['code']
                # 🚀 [V39.5] 物理对位：统一强制 6 位数字代码，消除 sh/sz 前缀干扰
                symbol = raw_sym[-6:] if raw_sym else ""
                if not symbol: continue
                full_pool[symbol] = SimpleNamespace(
                    lb_days=item['lb_days'], plate=item['plate'],
                    is_yest_limit=True, close_pct=item['close_pct'],
                    turnover=item['turnover'], name=item['name']
                )
            logger.info(f"✅ 全量扫描完成: 捕获 {len(full_pool)} 只昨日涨停股 (基因对准)")
            return full_pool
        except Exception as e:
            logger.error(f"❌ Unified Ban Scan Error: {e}")
            return {}
    async def _fetch_kaipan_hot_plates(self, date: str) -> List[Tuple[str, int]]:
        """抓取开盘啦热门板块"""
        logger.info(f"获取昨日热门板块 ({date})...")
        try:
            res = await asyncio.get_event_loop().run_in_executor(
                None, self.api_analyzer._call_api, 'getHisPlates', date
            )
            if not res or 'list' not in res: return []
            return [(it[1], i+1) for i, it in enumerate(res['list'][:10])]
        except Exception as e:
            logger.error(f"Fetch Plate Error: {e}")
            return []
    @staticmethod
    def _parse_cn_number(val) -> float:
        if val is None: return 0.0
        try: return float(val)
        except: pass
        s = str(val).strip().replace(',', '')
        multiplier = 1.0
        if s.endswith('亿'): multiplier, s = 1e8, s[:-1]
        elif s.endswith('万'): multiplier, s = 1e4, s[:-1]
        elif s.endswith('%'): s = s[:-1]
        try: return float(s) * multiplier
        except: return 0.0
    async def _standardize_item(self, row: Any, source: str, extra: Dict = None) -> Optional[Dict]:
        try:
            if source == 'REDIS':
                code = str(row.get("symbol", row.get("code", ""))).strip()[-6:]
                name, pct, amt = row.get("name", "unknown"), float(row.get("change_pct", 0)), float(row.get("auction_amount_yuan", row.get("amount", 0)))
                if abs(pct) > 1.0: pct /= 100.0
            elif source == 'TDENGINE':
                lp, v, a, symbol = row
                code, name = str(symbol).strip()[-6:], "unknown"
                pc = (extra or {}).get(code, 0)
                pct, amt = ((float(lp or 0) / pc - 1.0) if pc > 0 else 0.0), float(a or 0)
            elif source == 'WENCAI':
                code, name = str(row.get('code', row.get('symbol', ''))).strip()[-6:], str(row.get('name', 'unknown'))
                pct, amt = self._parse_cn_number(row.get('pct', 0)) / 100.0, self._parse_cn_number(row.get('amt', 0))
            else: return None
            if name == "unknown" or not name: name = await self.metadata.get_name(code)
            if not code.isdigit() or len(code) != 6 or abs(pct) > 0.25: return None 
            return {"code": code, "name": name, "change_pct": pct, "auction_amount_yuan": amt}
        except: return None
    async def _get_pre_close_map(self, prev_date: str) -> Dict[str, Tuple[float, float]]:
        """获取昨日收盘价与全天成交额 (V5.5 游资版)"""
        sql = f"SELECT LAST(close), LAST(amount), symbol FROM daily_kline WHERE ts = '{prev_date} 00:00:00' GROUP BY symbol"
        res_map = {}
        try:
            cursor = await asyncio.get_event_loop().run_in_executor(None, self.tdengine.execute_query, sql)
            if cursor:
                for row in cursor.fetchall():
                    symbol = str(row[2]).strip()[-6:]
                    res_map[symbol] = (float(row[0] or 0), float(row[1] or 0))
        except Exception as e: logger.warning(f"⚠️ [Data] 获取昨日数据失败: {e}")
        return res_map
    async def _fetch_tdengine_auction(self, date_str: str) -> List[Dict]:
        # 确保 prev_date 是相对于传入日期 (可能是历史日期) 的前一交易日
        prev_date = self.calendar.get_previous_trading_day(date_str)
        pc_map = await self._get_pre_close_map(prev_date)
        sql = f"SELECT LAST(lp), LAST(v), LAST(a), symbol FROM stock_data WHERE ts >= '{date_str} 09:25:00' AND ts <= '{date_str} 09:25:01' GROUP BY symbol"
        try:
            cursor = await asyncio.get_event_loop().run_in_executor(None, self.tdengine.execute_query, sql)
            if not cursor: return []
            items = []
            for row in cursor.fetchall():
                item = await self._standardize_item(row, 'TDENGINE', extra=pc_map)
                if item: items.append(item)
            return items
        except: return []
    async def _fetch_wencai_auction(self, date_str: str) -> List[Dict]:
        """问财多源补全 (V5.6 优先级增强)"""
        # 第一优先级：保障昨日涨停池 (即便行情弱，这部分也必须有数据)
        codes_str = ",".join(list((self.yest_limit_map or {}).keys())[:100])
        segments = [
            f"{date_str}竞价涨跌幅;竞价金额;代码 {codes_str}", # 核心池
            f"{date_str}市值>100亿;竞价金额>500万;竞价涨跌幅", 
            f"{date_str}市值<100亿;竞价涨跌幅;竞价金额>500万"
        ]
        items, seen = [], set()
        for q in segments:
            try:
                df = await self.wencai.get_wencai_data(q)
                if df is None or df.empty: continue
                for _, row in df.iterrows():
                    code = str(row.get('code', row.get('symbol', ''))).strip()[-6:]
                    if code in seen or not code.isdigit(): continue
                    seen.add(code)
                    item = await self._standardize_item({'code': code, 'name': row.get('name','unknown'), 'pct': row.get('竞价涨跌幅', 0), 'amt': row.get('竞价金额', 0)}, 'WENCAI')
                    if item: 
                        item['source'] = 'WENCAI'
                        items.append(item)
                await asyncio.sleep(0.3) 
            except: continue
        return items
    async def _reconstruct_from_bars(self, date_str: str) -> List[Dict]:
        """最后防线：从今日首分钟 K 线开盘价还原竞价结果 (V5.6 新增)"""
        logger.info(f"🛡️ [Resilience] 启动最后防线：从 09:30 K线开盘价还原锚点...")
        prev_date = self.calendar.get_previous_trading_day(date_str)
        pc_map = await self._get_pre_close_map(prev_date)
        # 尝试从 minute_kline 表获取当天的第一根 1min Bar 的 open
        sql = f"SELECT FIRST(open), symbol FROM minute_kline WHERE ts = '{date_str} 09:30:00' GROUP BY symbol"
        try:
            cursor = await asyncio.get_event_loop().run_in_executor(None, self.tdengine.execute_query, sql)
            if not cursor: return []
            items = []
            for row in cursor.fetchall():
                code = str(row[1]).strip()[-6:]
                open_p = float(row[0] or 0)
                pc = pc_map.get(code, (0, 0))[0]
                if pc > 0 and open_p > 0:
                    items.append({
                        "code": code, "change_pct": (open_p / pc - 1.0),
                        "auction_amount_yuan": 0.0, "source": "RECONSTRUCT"
                    })
            return items
        except Exception as e:
            logger.warning(f"❌ [Resilience] 开盘价还原失败: {e}")
            return []
    async def execute_analysis(self, date_str: str, mode: str = "AUCTION"):
        # 🚀 [V36.0] 终极物理防御：严禁 NoneType 覆盖
        battle_kpis = getattr(self, "battle_kpis", {}) or {}
        current_data, rust_snap = [], {}
        if mode == "INTRA_DAY" and self.bridge.engine: rust_snap = self.bridge.get_snapshot()
        if not self.yest_limit_map and mode == "INTRA_DAY":
            await self.lifecycle.on_startup()
            self.yest_limit_map = self.analyzer.get_yest_limit_map(date_str)
        prev_date = self.calendar.get_previous_trade_day(date_str)
        pc_data = await self._get_pre_close_map(prev_date)
        data_sh = date_str.replace("-", "")
        # V5.5 预加载筹码压制分布 (从 Redis)
        chip_map = await self.redis.hgetall(f"cache:chip_peaks:{data_sh}") or {}
        # 🚀 [V39.3] 预加载全量技术因子映射 (MACD, KDJ, MA, etc.)
        factor_map_raw = await self.redis.hgetall(f"cache:stock_extra:{data_sh}") or {}
        factor_map = {k: json.loads(v) for k, v in factor_map_raw.items()}
        
        # [V10.0] 彻底解耦：竞价数据仅作为参考锚点 (self.auction_snapshot)，盘中严禁作为行情源
        if mode == "AUCTION" or (not self.auction_snapshot and mode == "INTRA_DAY"):
            base_key = f"market:auction:{data_sh}:0925"
            matched = await self.redis.keys(f"{base_key}*")
            data_key = matched[0] if matched else base_key
            try:
                r_t = await self.redis.type(data_key)
                raw = await self.redis.hget(data_key, "top_amount") if r_t == 'hash' else await self.redis.get(data_key)
                if raw:
                    items = json.loads(raw)
                    if mode == "AUCTION":
                        for raw_it in items:
                            item = await self._standardize_item(raw_it, 'REDIS')
                            if item: current_data.append(item)
                    # 无论什么模式，如果 snapshot 为空，则填充它作为后续分析的参考系
                    if not self.auction_snapshot:
                        self.auction_snapshot = {str(i.get("symbol","")).strip(): float(i.get("change_pct",0)) for i in items}
                        logger.debug(f"✅ [Anchor] 竞价锚点异步补全成功 ({len(self.auction_snapshot)} 条)")
            except Exception as e:
                logger.warning(f"⚠️ [Anchor] 竞价键读取跳过或失败: {e}")
        if mode == "INTRA_DAY" and rust_snap:
            for k, r_it in rust_snap.items():
                if k == "_EXTREMES_": continue
                code = str(k).strip()
                # [V11.1] 鲁棒性对位：6位数字(Rust) -> 带后缀的 Key (Metadata/PC_Data)
                p_c = pc_data.get(code) or pc_data.get(f"{code}.SZ") or pc_data.get(f"{code}.SH") or 0.0
                c_p = r_it.get("price", 0.0)
                if c_p <= 0.1 or p_c <= 0.1: continue
                pct = c_p / p_c - 1.0
                y_amt = 0.0 # 保持变量链完整性
                # 2. 基因补完：板块属性与聚合指标 (V5.5 游资版)
                # 对标 Key 补全
                full_code = code if code in self.metadata.stock_info else (f"{code}.SZ" if f"{code}.SZ" in self.metadata.stock_info else f"{code}.SH")
                stock_info = self.metadata.stock_info.get(full_code, {})
                plate = stock_info.get('plate', 'Other')
                res_factor = self.prime.get_plate_resonance(plate)
                yest_it = (self.yest_limit_map or {}).get(code) or (self.yest_limit_map or {}).get(full_code)
                # 筹码压制解析
                chips = json.loads(chip_map.get(full_code, "[]")) if isinstance(chip_map.get(full_code), str) else []
                top_peak = max([c['price'] for c in chips]) if chips else 0.0
                resistance_gap = (top_peak - c_p) / c_p if top_peak > c_p else -0.1 # -0.1 代表已突破
                # 3. 分钟级指标精算
                bars = self.tick_history.get(code, {})
                now_m = int(time.time() // 60)
                m0, m1, m2 = bars.get(now_m), bars.get(now_m - 1), bars.get(now_m - 2)
                speed_auto, amount_2m = 0.0, 0.0
                if m0:
                    ref_bar = m2 or m1
                    if ref_bar: amount_2m = m0[1] - ref_bar[1] # [1] 为 Amount
                    if m1: speed_auto = (m0[0] - m1[0]) / m1[0] if m1[0] > 0.1 else 0.0 # [0] 为 Price
                # 🚀 [V39.3] 注入增强型因子
                f_data = factor_map.get(full_code, {})
                current_data.append({
                    "code": full_code, 
                    "name": stock_info.get('name', 'Unknown'), 
                    "change_pct": pct,
                    "auction_amount_yuan": r_it.get("amount", 0.0), "plate": plate,
                    "price": float(r_it.get("price", 0.0)),
                    "lb_days": yest_it.lb_days if yest_it else 0, 
                    "is_yest_limit": True if yest_it else False,
                    "resonance_factor": res_factor, 
                    "vol_intensity": r_it.get("vol_intensity", 1.0) * res_factor,
                    "speed_1m": speed_auto, "amount_2m": amount_2m,
                    "yest_amount": y_amt, "resistance_gap": resistance_gap,
                    # 🚀 多因子载入
                    "macd_hist": f_data.get("macd_hist", 0.0),
                    "kdj_j": f_data.get("kdj_j", 50.0),
                    "ma5": f_data.get("ma5", 0.0),
                    "ma10": f_data.get("ma10", 0.0),
                    "ma20": f_data.get("ma20", 0.0),
                    "dde_3d_sum": f_data.get("dde_3d_sum", 0.0) or f_data.get("ddje_3d_sum", 0.0),
                    "concentration": f_data.get("concentration", 0.0),
                    # 🚀 [V39.5] 反包状态注入
                    "t2_lb_days": f_data.get("t2_lb_days", 0),
                    "t2_pct": f_data.get("t2_pct", 0.0),
                    "source": "RUST"
                })
        # 🚀 [V36.0] 这里是最隐蔽的覆盖点，必须强制 or {}
        battle_kpis = await self.prime.calculate_battle_kpis(date_str) or {}
        if not current_data and mode == "AUCTION":
            current_data.extend(await self._fetch_tdengine_auction(date_str))
            if len(current_data) < 50: current_data.extend(await self._fetch_wencai_auction(date_str))
        # [V12.2] 拦截器后移：先执行补丁回捞，再判定是否退出
        if mode == "AUCTION":
            for it in current_data: self.auction_snapshot[it["code"]] = it["change_pct"]
            # [V13.0] 实时固化竞价成果到 Redis，防止盘中重启丢失锚点
            if self.auction_snapshot:
                date_sh = date_str.replace("-", "")
                auc_archive_key = f"market:auction:anchor:{date_sh}"
                await self.redis.set(auc_archive_key, json.dumps(self.auction_snapshot), ex=86400 * 3) # 保留3天
                logger.info(f"💾 [Persistence] 竞价锚点已固化至 Redis ({len(self.auction_snapshot)} 只标的)")
        # [V5.7 阵型强行合拢] 自动查漏补齐昨日涨停 56 只核心标的
        if self.yest_limit_map:
            current_codes = {s['code'] for s in current_data}
            missing_ban_codes = [c for c in (self.yest_limit_map or {}).keys() if c not in current_codes]
            if missing_ban_codes:
                logger.debug(f"🛡️ [Resilience] 阵型合拢补丁开启：正在回捞 {len(missing_ban_codes)} 只掉队的昨日涨停标点...")
                for code in missing_ban_codes:
                    # 单点从 Redis 协议栈抓取实时行情 (stock:quote:xxxx)
                    q = await self.redis.hgetall(f"stock:quote:{code}")
                    if q:
                        p_c = pc_data.get(code, 0.0)
                        c_p = float(q.get("price", 0.0))
                        if p_c > 0 and c_p > 0:
                            current_data.append({
                                "code": code, "name": q.get("name", "unknown"),
                                "price": c_p,
                                "change_pct": (c_p / p_c - 1.0), "auction_amount_yuan": float(q.get("amount", 0)),
                                "plate": self.metadata.stock_info.get(code, {}).get('plate', 'Other'),
                                "yest_amount": 0.0, "resistance_gap": 0.0, "source": "REDIS"
                            })
        if not current_data: 
            logger.warning("⚠️ [Execution] 分析终端无数据输入 (请检查行情泵状态)")
            return
        # [V12.0] 实时感知当前情绪周期 (基于分析器产出的真实截面指标)
        current_allowed_setups = []
        battle_kpis["current_time"] = datetime.now().strftime("%H:%M") # [V38.2] 修正时间锁死
        report = await self.analyzer.analyze(current_data, auction_snapshot=self.auction_snapshot if mode == "INTRA_DAY" else None,
                                           yest_limit_map=self.yest_limit_map, yest_hot_plates=getattr(self.prime, "hot_plates_map", {}) or {},
                                           date_str=date_str, battle_kpis=battle_kpis, alpha_candidates=self.alpha_candidates,
                                           allowed_setups=current_allowed_setups)
        # [V12.0/V12.3] 执行情绪周期同步判定 (必须在产生 report 之后)
        try:
            from engine_v2.v2_business_logic import V2BusinessLogicService
            _logic = V2BusinessLogicService()
            max_lb_val = 0
            if report.highest_board:
                max_lb_val = getattr(report.highest_board, "lb_days", 0)
            _phase = _logic.predict_market_phase(
                st_score=max(report.money_making_effect, (battle_kpis or {}).get("sentiment_score", 0.0)),
                red_green_ratio=max(report.red_green_ratio, (battle_kpis or {}).get("red_green_ratio", 0.0)),
                max_lb=max(max_lb_val, (battle_kpis or {}).get("max_lb", 0)),
                consensus_score=(battle_kpis or {}).get("consensus_score", 20.0),
            )
            current_allowed_setups = _phase.allowed_setups
            report.emotion = _phase
            if "=====" in report.summary_text: sys.stdout.write("\n")
            locked_plates = {s.plate for s in report.all_stocks if getattr(s, 'is_locked', False)}
            for signal in report.strategic_signals:
                check = AuctionEntryChecker.evaluate(
                    plate_resonance=signal.confidence / 60.0,
                    sector_volume_vs_expect=(battle_kpis or {}).get("vol_ratio", 1.0),
                    plate_locked=(signal.plate in locked_plates),
                    sentiment_score=report.money_making_effect,
                    allowed_setups=current_allowed_setups
                )
                if check.action == RiskAction.CANCEL_ENTRY:
                    signal.action = "风险取消"
                    signal.reason = f"🛑 风险控制器拦截: {check.reason}"
                    signal.confidence = 0
                elif check.action == RiskAction.HALF_ENTRY:
                    signal.action = f"试探|{signal.action}"
                    signal.reason += f" (⚠️ 风控建议半仓: {check.reason})"
                    signal.confidence -= 10
        except Exception as e:
            logger.warning(f"⚠️ [Sentiment Sync] 情绪周期同步失败: {e}")

        self.highest_board = report.highest_board # 同步更新龙头状态
        logger.debug(f"📊 [Execution] 全量分析完成 | 样本规模: {len(report.all_stocks)} | 实时评分: {report.money_making_effect}")
        
        # 4\. 智能输出 \(V5\.5 呼吸式优化\)
        now = datetime.now()
        # 紧急异动判定：
        is_urgent = False
        if self.highest_board and self.last_sentiment > 0:
             # A. 龙头炸板或大跌
             if self.highest_board.momentum_delta < -0.04: is_urgent = True
             # B. 情绪剧烈波动
             if abs(report.money_making_effect - self.last_sentiment) >= 0.2: is_urgent = True
        is_period_hit = (now.minute % 9 == 0 and now.second < 10)
        if mode == "AUCTION" or self.is_first_session_run or is_urgent or is_period_hit:
            # [V15.0] 实时捕获模式：在 09:25 分析后，将实时内存数据固化至归档字典并同步 Redis
            if mode == "AUCTION" and not self.auction_synced_date:
                logger.info("💾 [Anchor Persistence] 正在将实时竞价捕获结果固化至内存与 Redis...")
                self.auction_snapshot = {s.code: s.open_pct for s in report.all_stocks if s.code}
                self.auction_synced_date = datetime.now().strftime("%Y-%m-%d")
                # 异步推送到 Redis 供其它系统/重启回赎使用
                date_sh = self.auction_synced_date.replace("-", "")
                await self.redis.set(f"market:auction:anchor:{date_sh}", json.dumps(self.auction_snapshot), ex=172800)
            # 标记战役 KPI 用于看板显示
            report.battle_kpis['current_time'] = now.strftime("%H:%M")
            self.last_report = report # 🚀 [V39.1] 固化以便非交易时间展示
            # 先换行，避免覆盖交互线
            sys.stdout.write("\n")
            if report.summary_text:
                sys.stdout.write(report.summary_text)
                sys.stdout.flush()
                if "=====" in report.summary_text: sys.stdout.write("\n")
            # [V8.2 固化持久化] 将实战指令存入 Redis 快照，供复盘引擎对账
            if mode == "AUCTION" or (now.minute == 30 and now.hour == 9):
                snap_key = f"market:snapshot:{data_sh}:signals"
                signals_data = {
                    "meta": {
                        "total_auction_amt": round(report.total_amount / 1e8, 2), # 亿元
                        "avg_market_pct": round(report.avg_market_pct * 100, 2), # %
                        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
                    },
                    "signals": [
                        {"code": sig.code, "name": sig.name, "action": sig.action, "confidence": sig.confidence, "reason": sig.reason}
                        for sig in report.strategic_signals
                    ]
                }
                await self.redis.set(snap_key, json.dumps(signals_data, ensure_ascii=False), ex=604800)
                logger.info(f"💾 [Snapshot] 宏观镜像与实战指令已固化 ({snap_key})")
            self.last_sentiment, self.is_first_session_run = report.money_making_effect, False

    async def run_guardian(self):
        logger.info("🛡️ MarketEdge V32.5 [War-Room] Stability-Pro 物理对位上线")
        
        # 🚀 [V32.0] 核心稳定性对焦：基础设施单例注入
        from v2_infra_provider import get_global_redis, get_global_session, get_global_tdengine
        
        self.redis = await get_global_redis()
        self._session = await get_global_session()
        self._tdengine = get_global_tdengine()
        
        # 🚀 [V32.0] 延迟实例化：确保所有服务对象在活跃异步 Loop 下生存
        if not self.kline_service: self.kline_service = StockKLineService()
        if not self.wencai: self.wencai = UnifiedMarketDataFetcher()
        if not self.api_analyzer: self.api_analyzer = StockAnalyzer()
        if not self.chip_runner: self.chip_runner = ChipBatchRunner(kline_service=self.kline_service)
        
        # 注入各子系统单例
        self.analyzer.redis = self.redis
        self.metadata.set_redis_client(self.redis)
        self.prime.r = self.redis
        self.prime.redis = self.redis
        self.lifecycle.redis = self.redis
        date_str = datetime.now().strftime("%Y-%m-%d")
        now = datetime.now()
        
        # [V15.0] 智能仿真对焦：仅在周末且未明确指定测试日期时开启
        if now.weekday() >= 5 and self.calendar.is_trade_day(date_str) == False:
             logger.info("🎮 [Simulation] 侦测到非交易日启动，自动切入周末仿真对合 (2026-04-10)...")
             date_str = "2026-04-10"
             yes_str = "2026-04-09"
        else:
             yes_str = self.calendar.get_previous_trading_day(date_str)
        self._today = date_str
        self._session = aiohttp.ClientSession()

        # 🚀 [V39.2] 一次性预审计：加载全场 5000 只标的水位，消除同步中的大量数据库 IO
        await self._preload_all_watermarks(date_str)
        await self._startup_sync(date_str)

        # [V35.3 Official] 物理快照自愈：确保复盘引擎有粮草
        snap_key = f"market:snapshot:{date_str}"
        # 延迟一下，等 Redis 彻底稳固
        await asyncio.sleep(0.5)

        # 🚀 [V39.4 Fix] 预检：如果是当日启动且早于 09:25，严禁执行物理补全，防止数据空洞引发 Crash
        now_str = datetime.now().strftime("%Y-%m-%d")
        now_hm = datetime.now().strftime("%H:%M")
        is_too_early = (date_str == now_str and now_hm < "09:25")

        if not await self.redis.exists(snap_key) and not is_too_early:
             logger.warning(f"🗄️ [Repair] 未发现 {date_str} 的定音快照，正在强制执行物理补全...")
             try:
                 # 🚀 [V35.5] 对位对齐：调用真正的核心分析入口，触发快照固化
                 await self.execute_analysis(date_str, mode="AUCTION")
             except Exception as e:
                 logger.error(f"❌ [Repair] 补全快照失败: {e}")

        asyncio.create_task(self._v2_tick_pump())
        asyncio.create_task(self._risk_monitoring_loop()) # [V9.5] 激活风控哨兵
        
        await asyncio.sleep(2)
        # 初始推演 (盘前重播模式) - 仅在 09:25 以后启动才立即输出，防止盘前刷屏噪音
        now_hm = datetime.now().strftime("%H:%M")
        if now_hm >= "09:25":
            await self.execute_analysis(date_str, mode="AUCTION" if now_hm < "09:30" else "INTRA_DAY")
        while self.is_running:
            now = datetime.now()
            # if now.weekday() >= 5:
            #     await asyncio.sleep(300); continue
            h_m = now.strftime("%H:%M")
            if h_m == "08:30" or h_m == "09:00":
                # [V15.0] 全周期状态重置：清理昨日记忆，准备迎接新一轮竞价捕获
                logger.info("♻️ [Lifecycle] 清除历史竞价记忆，准备进入当日实时分析阶段...")
                self.auction_synced_date = ""
                self.auction_snapshot = {}
                await self.lifecycle.on_startup()
                self.yest_limit_map = await self._fetch_kaipan_limit_ups(yes_str)
                await asyncio.sleep(60)
            elif h_m == "09:26":
                self.yest_limit_map = await self._fetch_kaipan_limit_ups(yes_str)
                await self.execute_analysis(date_str, mode="AUCTION")
                await asyncio.sleep(60)
            elif h_m == "15:05":
                logger.info("🏁 [Market-Close] 停止监听"); await asyncio.sleep(60)
            elif h_m == "17:40":
                await asyncio.get_event_loop().run_in_executor(None, self.kline_service.preload_latest_dates)
                await self.lifecycle.on_eod()
                try: subprocess.Popen([sys.executable, "v2_final_retro.py"])
                except: pass
                await asyncio.sleep(60)
            elif "09:30" <= h_m <= "15:00":
                # [V12.2] 强制首跑点亮：如果是刚启动，且处于交易时间段，立即执行一次全量分析
                is_time_to_analyze = (now.minute % 3 == 0 and now.second < 5)
                if self.is_first_session_run or is_time_to_analyze:
                    await self.execute_analysis(date_str, mode="INTRA_DAY")
                    self.is_first_session_run = False
                # 每 10 秒单行状态栏刷新 (不增行)
                if now.second % 60 == 0:
                    hb_name = self.highest_board.name if self.highest_board else 'N/A'
                    hb_pct = self.highest_board.current_pct * 100 if self.highest_board else 0.0
                    hb_text = f"[{h_m}] 🛡️ 战役对准 | 情绪:{self.last_sentiment:.1f} | 龙头: {hb_name} ({hb_pct:+.1f}%)   "
                    sys.stdout.write(f"\r{hb_text}")
                    sys.stdout.flush()
                await asyncio.sleep(1)
            else:
                if now.second % 60 == 0:
                    if self.last_report and self.last_report.rotation_msg:
                        # 🚀 [V39.1] 优雅心跳：在非交易时段，使用最后一份报告的心跳行刷新，不增行，不刷屏
                        sys.stdout.write(f"{self.last_report.rotation_msg}")
                        sys.stdout.flush()
                    else:
                        logger.info(f"🌑 [NightMode] 引擎基盘在线 | 时刻: {h_m}")
                await asyncio.sleep(1)
    async def _v2_tick_pump(self):
        logger.info("🚀 [Pump] 智库行情提取泵已就位")
        all_symbols = list(self.metadata.stock_info.keys())
        key_map = {f"stock:quote:{s}": s for s in all_symbols}
        redis_keys = list(key_map.keys())
        while self.is_running:
            try:
                now = datetime.now()
                h_m = now.strftime("%H:%M")
                if not ("09:15" <= h_m <= "11:50" or "12:55" <= h_m <= "15:10"):
                    await asyncio.sleep(60); continue
                t_start = time.time()
                # 🛠️ 优化 1: 仅拉取所需字段
                fields = ["price", "current", "amount", "volume", "time", "bid_amount"]
                async with self.redis.pipeline(transaction=False) as pipe:
                    for k in redis_keys: 
                        await pipe.hmget(k, fields)
                    results = await pipe.execute()
                if results is None: continue
                await self.prime.sync_kaipan_hotspots()
                m_idx = int(time.time() // 60)
                for k, vals in zip(redis_keys, results):
                    if not vals or not any(vals): continue
                    # 解包：hmget 返回的是对应 fields 的值列表
                    # price (0), current (1), amount (2), volume (3), time (4), bid_amount (5)
                    code = key_map[k]
                    p = float((vals[0] if vals else 0) or vals[1] or 0)
                    if p <= 0.1: continue 
                    amt = float(vals[2] or 0)
                    vol = float(vals[3] or 0)
                    tm = vals[4] or "00:00:00"
                    bid_amt = float(vals[5] or 0)
                    # 🚀 送往 Rust 核心 (保持高效透传)
                    self.bridge.push_tick_raw(code, p, amt, vol, tm, bid_amt)
                    # ─────────────────────────────────────────────────────────────
                    # 🛠️ 优化 2: 精简 1-Minute Bar 聚合，避免嵌套字典分配
                    # ─────────────────────────────────────────────────────────────
                    if code not in self.tick_history: self.tick_history[code] = {}
                    # 直接存储数值 List 而不是 Dict，减少对象开口开销 (5000+ * 5)
                    self.tick_history[code][m_idx] = (p, amt)
                # 🛠️ 优化 3: 滚动清理逻辑移出内层循环，每分钟仅执行一次全场扫描
                if now.second < 4: # 每分钟开始后的前 4 秒执行一次清理
                    for c in self.tick_history:
                        expired = [tk for tk in self.tick_history[c] if tk < m_idx - 5]
                        for et in expired: self.tick_history[c].pop(et, None)
                # if (time.time() - t_start) > 0.5: logger.warning(f"⚠️ [Pump] 延迟: {(time.time()-t_start)*1000:.2f}ms")
                await asyncio.sleep(3)
            except Exception as e: logger.error(f"❌ [Pump] Error: {e}"); await asyncio.sleep(5)
    async def _risk_monitoring_loop(self):
        """[V9.5] 实时风控哨兵任务"""
        await asyncio.sleep(10) # [V35.9] 启动避让，等待基盘对齐
        logger.info("🛡️ [RiskSentinel] 持仓风险监控哨兵已上线")
        from engine_v2.v2_risk_controller import OpeningStopLoss, IntradayTracker, MainlineValidator, RiskAction
        while self.is_running:
            try:
                now = datetime.now()
                h_m = now.strftime("%H:%M")
                if not ("09:30" <= h_m <= "15:00"):
                    await asyncio.sleep(30); continue
                # 1. 拉取实时持仓 (从 Redis)
                holdings_raw = await self.redis.get("market:account:holdings")
                if not holdings_raw:
                    await asyncio.sleep(15); continue
                holdings = json.loads(holdings_raw) # 预期格式: {"000889": {"cost": 3.85, "is_mainline": True, "entry_time": "09:25"}}
                # 2. 获取快照
                snapshot = self.bridge.get_snapshot()
                if not snapshot:
                    await asyncio.sleep(5); continue
                # 3. 逐一巡检
                for code, info in holdings.items():
                    curr_p = snapshot.get(code, {}).get("price", 0.0)
                    if curr_p <= 0.01: continue
                    cost = info.get("cost", 0.0)
                    is_mainline = info.get("is_mainline", False)
                    decision = None
                    # A. 开盘止损 (09:30 - 09:35)
                    if "09:30" <= h_m <= "09:35":
                        decision = OpeningStopLoss.evaluate(curr_p, cost, is_mainline)
                    # B. 盘中追踪 (09:35 - 15:00)
                    else:
                        # 记录/获取历史最高价
                        if code not in self.risk_stats:
                            self.risk_stats[code] = {"high": curr_p, "p_2m": curr_p, "last_t": time.time()}
                        stats = self.risk_stats[code]
                        stats["high"] = max(stats["high"], curr_p)
                        # 每 2 分钟滚动一次价格锚点
                        if time.time() - stats["last_t"] > 120:
                            stats["p_2m"] = curr_p
                            stats["last_t"] = time.time()
                        # 执行盘中评估器
                        # [P1] 动态验证主线有效性
                        # 获取板块排名与龙头状态 (适配 V3.5 字典结构)
                        plate_stats = (getattr(self.prime, "hot_plates_map", {}) or {}).get(info.get("plate", ""), {}) 
                        is_valid, v_reason = MainlineValidator.is_mainline_valid(
                            sector_strength_rank=plate_stats.get('rank', 99),
                            leader_is_locked=info.get("is_leader_locked", True), # 默认主线龙头为锁定，除非手动干预
                            sector_volume_vs_peak=1.0, # 简版暂定
                            current_sentiment=self.last_sentiment,
                            auction_sentiment=7.0 # 模拟基准
                        )
                        mainline_ready = is_mainline and is_valid
                        decision = IntradayTracker.evaluate_hard_stop(curr_p, cost, is_mainline, mainline_ready)
                        if not decision:
                            # [P0 Fix] 使用真实的昨收价映射
                            prev_close = self.pre_close_map.get(code, cost)
                            high_pct = (stats["high"] / prev_close) - 1.0
                            curr_pct = (curr_p / prev_close) - 1.0
                            decision = IntradayTracker.evaluate_spike_reversal(high_pct, curr_pct, is_mainline, mainline_ready)
                        if not decision:
                            decision = IntradayTracker.evaluate_plunge_speed(stats["p_2m"], curr_p, is_mainline, mainline_ready)
                        # [P1] 炸板监控
                        if not decision:
                            p_close_val = self.pre_close_map.get(code, 0)
                            is_limit = curr_p >= (p_close_val * 1.098) if p_close_val > 0 else False
                            if not is_limit and stats.get("was_limit", False):
                                # 刚刚炸板
                                if "limit_break_t" not in stats:
                                    stats["limit_break_t"] = time.time()
                                    logger.warning(f"🌀 [Risk] {code} 发生炸板，开始 30min 倒计时回封监控")
                                # 评估炸板时长
                                diff_min = (time.time() - stats["limit_break_t"]) / 60
                                decision = IntradayTracker.evaluate_limit_open(int(diff_min), is_limit, is_mainline)
                            elif is_limit:
                                stats["was_limit"] = True
                                stats.pop("limit_break_t", None) # 回封则清除计时
                    # 4. 指令输出
                    if decision and decision.action != RiskAction.HOLD:
                        color = "\033[91m" if "清仓" in decision.reason or "止损" in decision.reason else "\033[93m"
                        reset = "\033[0m"
                        msg = f"{color}🔥 [RISK ALERT] {code} | {decision.action.value} | 原因: {decision.reason}{reset}"
                        sys.stdout.write(f"\n{msg}\n")
                        # 写入报警通道
                        await self.redis.lpush("market:risk:alerts", json.dumps({
                            "time": now.strftime("%H:%M:%S"),
                            "code": code,
                            "action": decision.action.value,
                            "reason": decision.reason
                        }, ensure_ascii=False))
                await asyncio.sleep(15) # 15 秒轮询一次
            except Exception as e:
                logger.error(f"⚠️ [RiskSentinel] Error: {e}")
                await asyncio.sleep(10)
    async def _preload_all_watermarks(self, target_date: str = None):
        """🚀 [V39.2] 性能加速器：一次性聚合全市场水位，消除 5000+ 次循环 IO"""
        try:
            start_t = time.time()
            # 1. 并发拉取 3 大核心维度的全量水位 (K线 / DDE / 因子)
            tasks = [
                asyncio.get_event_loop().run_in_executor(None, self.tdengine.get_all_latest_dates, "daily_kline"),
                asyncio.get_event_loop().run_in_executor(None, self.tdengine.get_all_latest_dates, "daily_dde"),
                asyncio.get_event_loop().run_in_executor(None, self.tdengine.get_all_latest_dates, "daily_factors")
            ]
            k_map, dde_map, factor_map = await asyncio.gather(*tasks)
            
            self.watermarks = {
                'kline': k_map or {},
                'dde': dde_map or {},
                'factor': factor_map or {}
            }
            cost = time.time() - start_t
            logger.info(f"📊 [Bulk-Audit] 水位审计完成! 覆盖: K={len(k_map)}, DDE={len(dde_map)}, Fac={len(factor_map)} | 耗时: {cost:.2f}s")
        except Exception as e:
            logger.error(f"❌ [Bulk-Audit] 水位预载严重异常: {e}")
            self.watermarks = {'kline': {}, 'dde': {}, 'factor': {}}

    async def _sync_and_calculate_stock(self, symbol: str) -> bool:
        """🚀 [V39.2] 智能补全中枢 (Smart-Filler Hub - Memory Cached)
        逻辑：基于预载的内存水位，执行缺谁补谁。
        """
        try:
            # 1. 确定基准日期
            target_day = self._today
            now_t = datetime.now().time()
            if now_t < dt_time(15, 30):
                target_day = self.calendar.get_previous_trade_day(self._today)
            
            # 2. 状态原子审计 (内存级别，无 IO)
            # A. K线 (🚀 [V39.2.2] 安全取值防止 KeyError)
            latest_k = self.watermarks.get('kline', {}).get(symbol)
            k_exists = latest_k >= target_day if latest_k else False
            
            # B. DDE 
            latest_dde = self.watermarks.get('dde', {}).get(symbol)
            dde_exists = latest_dde >= target_day if latest_dde else False
            
            # C. 筹码与因子
            # 因子特别检查：如果内存水位存在但不满足目标，或 Redis 缺失
            latest_fac = self.watermarks.get('factor', {}).get(symbol)
            chip_exists = await self.redis.hexists(f"cache:chip_peaks:{target_day}", symbol)
            # 对于因子，除了 check 进度，还需确保新列（如 ma5）不为 0
            factor_exists = await self.redis.hexists(f"cache:stock_extra:{target_day}", symbol)
            
            # 3. 执行补全
            if not dde_exists:
                await self._sync_stock_dde(symbol, target_day)
            
            current_k_list = None
            if not k_exists:
                current_k_list = await asyncio.get_event_loop().run_in_executor(
                    None, self.kline_service.fetch_kline_data, symbol, 'd', None, target_day
                )
            
            # 算力补全 (筹码/多因子)
            if not chip_exists or not factor_exists or (latest_fac and latest_fac < target_day):
                if not current_k_list:
                    current_k_list = await asyncio.get_event_loop().run_in_executor(
                        None, self.tdengine.get_daily_kline, symbol, 
                        (datetime.strptime(target_day, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d"), 
                        target_day
                    )
                
                if current_k_list and len(current_k_list) >= 35: # V39.2 稳定性门槛
                    res = await asyncio.get_event_loop().run_in_executor(
                        None, self.chip_runner.calculate_for_stock, symbol, current_k_list, target_day
                    )
                    if res and isinstance(res, (list, tuple)) and len(res) == 3:
                        _, peak, factors = res
                        if peak:
                            await self.redis.hset(f"cache:chip_peaks:{target_day}", symbol, json.dumps(peak))
                            await self.redis.hset(f"cache:stock_extra:{target_day}", symbol, json.dumps(factors))
                            await asyncio.get_event_loop().run_in_executor(None, self.tdengine.save_chips, symbol, peak)
                            await asyncio.get_event_loop().run_in_executor(None, self.tdengine.save_factors, symbol, pd.DataFrame([factors]))
            
            return k_exists or (current_k_list is not None)
        except Exception as e:
            logger.error(f"❌ [Hub Error] {symbol} process failed: {e}")
            return False
    async def _get_pre_close_map(self, target_date: str = None) -> Dict[str, float]:
        """[P0 Fix] 获取昨收价映射，使用 Pipeline 优化确保 5000+ 样本秒级对齐"""
        try:
            # 1. 获取所有实时 Key
            keys = await self.redis.keys("stock:quote:*")
            if not keys:
                 if target_date:
                    hist_key = f"market:snapshot:{target_date.replace('-','')}:pre_close"
                    hist_data = await self.redis.get(hist_key)
                    if hist_data: return json.loads(hist_data)
                 # 最后的降级：从元数据预加载
                 return {code: float(info.get('pre_close', 0)) for code, info in self.metadata.stock_info.items()}
            # 2. Pipeline 批量拉取 (O(1) 替代 O(N))
            price_map = {}
            async with self.redis.pipeline(transaction=False) as pipe:
                for k in keys:
                    pipe.hget(k, "pre_close")
                results = await pipe.execute()
                if results is None: return {}
            for k, p_str in zip(keys, results):
                if p_str:
                    code = k.split(":")[-1]
                    if p_str is not None: price_map[code] = float(p_str)
            # 3. 完整性补充：如果 Redis 缺失，用 Metadata 里的昨收补齐
            if len(price_map) < 4000:
                logger.info(f"🛡️ [P0 Refresh] Redis 样本不足 ({len(price_map)}), 正在注入 Metadata 基准...")
                for code, info in self.metadata.stock_info.items():
                    if code not in price_map:
                        p_c = float(info.get('pre_close', 0))
                        if p_c > 0.01: price_map[code] = p_c
            return price_map
        except Exception as e:
            logger.error(f"❌ 昨收基准拉取失败: {e}")
            return {code: float(info.get('pre_close', 0)) for code, info in self.metadata.stock_info.items() if 'pre_close' in info}
    async def _sync_stock_dde(self, symbol: str, override_date: str = None) -> bool:
        """[V39.2 Fix] 修复 DDE 字段不匹配与过时停滞问题"""
        try:
            target_date = (override_date or self._today).replace("-", "")
            # 时间纠偏：如果是盘中且没传 override，则取上一交易日数据
            if not override_date and datetime.now().hour < 16:
                target_date = self.calendar.get_previous_trading_day(self._today).replace("-", "")
            
            res = await asyncio.get_event_loop().run_in_executor(
                None, self.api_analyzer.get_his_stock_dde, symbol.split(".")[0], target_date
            )
            
            if not res or res.get('errcode') != '0': return False
            
            # 1. 动态对齐 API 原始字段 (兼容大写与缺失)
            data = {}
            mapping = {'DDJE': 'ddje', 'Date': 'date', 'DDX': 'ddx', 'DDY': 'ddy', 'DDZ': 'ddz'}
            for api_key, db_key in mapping.items():
                if api_key in res: data[db_key] = res[api_key]
            
            if not data or 'date' not in data: 
                return True # 虽然没拿到当天的，但也算接口通了
                
            df = pd.DataFrame(data).head(20)
            # 补齐缺失字段为 0，防止 TDengine 驱动报错
            for col in ['ddx', 'ddy', 'ddz', 'ddje']:
                if col not in df.columns: df[col] = 0.0
            
            return await asyncio.get_event_loop().run_in_executor(None, self.tdengine.save_daily_dde, symbol, df)
        except Exception as e:
            logger.debug(f"⚠️ [DDE Error] {symbol}: {e}")
            return False
    def _physical_check_all_dimensions(self, symbol: str, tag: str) -> bool:
        """🚀 [V39.2.3] 物理对冲校验优化：优先使用内存审计（Watermarks）"""
        try:
            target_iso = f"{tag[:4]}-{tag[4:6]}-{tag[6:]}"
            
            # 使用内存水位索引，避免 5000+ 次 DB 爆炸查询
            # 1. K线校验
            latest_k = self.watermarks.get('kline', {}).get(symbol)
            if not latest_k or latest_k < target_iso: return False
            
            # 2. DDE 校验
            latest_dde = self.watermarks.get('dde', {}).get(symbol)
            if not latest_dde or latest_dde < target_iso: return False
            
            return True
        except Exception as e:
            return False

    async def _trigger_factor_calc(self) -> bool:
        try:
            if self.bridge.engine: self.bridge.reload_metadata()
            return True
        except: return False
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    orc = AuctionOrchestrator()
    try:
        asyncio.run(orc.run_guardian())
    except KeyboardInterrupt:
        logger.info("🛑 用户手动强行停止。")
    except Exception as e:
        logger.critical(f"💀 [Fatal] Orchestrator Crushed: {e}")
        logger.error(traceback.format_exc())
    finally:
        _s = getattr(orc, "_session", None)
        if _s and not _s.closed:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running(): loop.create_task(_s.close())
                else: loop.run_until_complete(_s.close())
            except: pass
        logger.info("正在释放网络资源...")

