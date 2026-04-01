"""
v2_orc_final.py
MarketEdge V7.2.1 Guardian (Strategic Alignment & Decision Hub)
Logic: AUCTION-TO-LIVE ALIGNMENT. Persistent Snapshots & Command Console.
"""
import sys
import os
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
from collections import deque

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
from engine_v2.v2_prime_logic import ResonancePrimeService

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
        try:
            self.redis.ping()
            logger.info("✅ 成功连接至物理服务器 Redis (115.190.156.240)")
        except:
            self.redis = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
            logger.warning("⚠️ 物理 IP 连接失败，降级使用 127.0.0.1")
            
        self.calendar = TradingCalendarService()
        self.analyzer = AuctionAnalyzer()
        data_dir = os.path.join(BASE_DIR, 'web', 'data')
        self.metadata = MetadataProvider(data_dir=data_dir)
        self.metadata.set_redis_client(self.redis)
        
        self.tdengine = TDengineService()
        self.kline_service = StockKLineService()
        self.api_analyzer = StockAnalyzer()
        self.wencai = UnifiedMarketDataFetcher()
        self.chip_runner = ChipBatchRunner(kline_service=self.kline_service) 
        
        self.last_sentiment = 0.0
        self.is_first_session_run = True 
        self.state = _load_state()
        self.auction_snapshot = {} 
        self.bridge = v2_core_bridge
        self.prime = ResonancePrimeService(self.redis)
        self.tick_history = {} 
        self.auction_synced_date = None  
        
        # 定义适配 Lifecycle 的异步包装
        async def l_fetch_bans():
            yest = self.calendar.get_previous_trading_day(datetime.now().strftime("%Y-%m-%d"))
            self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest)
            return list(self.yest_limit_map.keys())
            
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
            trigger_rust_calc_fn=self._trigger_factor_calc
        )

    async def _startup_sync(self, date_str: str):
        """启动时同步与环境预研"""
        logger.info(f"🔍 [System] 启动自检中 (基于 Lifecycle 任务)...")
        self.lifecycle.symbols = list(self.metadata.stock_info.keys())
        await asyncio.get_event_loop().run_in_executor(None, self.kline_service.preload_latest_dates)
        await self.lifecycle.on_startup()
        
        yest_str = self.calendar.get_previous_trading_day(date_str)
        self.yest_limit_map = await self._fetch_kaipan_limit_ups(yest_str)
        self.hot_plates = await self._fetch_kaipan_hot_plates(yest_str)
        logger.info(f"[Metadata] Load Success: {len(self.yest_limit_map)} yest-limits, {len(self.hot_plates)} hot-plates")

        if self.bridge.engine:
            all_symbols = list(self.metadata.stock_info.keys())
            self.bridge.register_symbols(all_symbols)
            for p_name, _ in self.hot_plates:
                p_stocks = [s for s, plate in self.metadata.plate_map.items() if p_name in plate]
                self.bridge.register_plate_mapping(p_name, p_stocks)

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
                if not items: items = await self._fetch_wencai_auction(date_str)
                if items:
                    for it in items: self.auction_snapshot[it["code"]] = it["change_pct"]
                    self.auction_synced_date = date_str
                else: logger.error("❌ [Anchor] 补全失败。")

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
                symbol = item['code']
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

    async def _get_pre_close_map(self, prev_date: str) -> Dict[str, float]:
        sql = f"SELECT LAST(close), symbol FROM daily_kline WHERE ts = '{prev_date} 00:00:00' GROUP BY symbol"
        pre_close_map = {}
        try:
            cursor = await asyncio.get_event_loop().run_in_executor(None, self.tdengine.execute_query, sql)
            if cursor:
                for row in cursor.fetchall(): pre_close_map[str(row[1]).strip()[-6:]] = float(row[0] or 0)
        except Exception as e: logger.warning(f"⚠️ [Data] 获取前收价失败: {e}")
        return pre_close_map

    async def _fetch_tdengine_auction(self, date_str: str) -> List[Dict]:
        prev_date = self.calendar.get_previous_trade_day(date_str)
        pc_map = await self._get_pre_close_map(prev_date)
        sql = f"SELECT LAST(lp), LAST(v), LAST(a), symbol FROM stock_data WHERE ts >= '{date_str} 09:25:00' AND ts <= '{date_str} 09:30:00' GROUP BY symbol"
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
        mc_segments = [f"{date_str}市值>100亿;竞价金额>500万;竞价涨跌幅", f"{date_str}市值<100亿;竞价涨跌幅>1%;竞价金额>500万"]
        items, seen = [], set()
        for q in mc_segments:
            try:
                df = await self.wencai.get_wencai_data(q)
                if df is None or df.empty: continue
                for _, row in df.iterrows():
                    code = str(row.get('code', row.get('symbol', ''))).strip()[-6:]
                    if code in seen or not code.isdigit(): continue
                    seen.add(code)
                    item = await self._standardize_item({'code': code, 'name': row.get('name','unknown'), 'pct': row.get('竞价涨跌幅', 0), 'amt': row.get('竞价金额', 0)}, 'WENCAI')
                    if item: items.append(item)
                await asyncio.sleep(0.5) 
            except: continue
        return items

    async def execute_analysis(self, date_str: str, mode: str = "AUCTION"):
        current_data, rust_snap = [], {}
        if mode == "INTRA_DAY" and self.bridge.engine: rust_snap = self.bridge.get_snapshot()

        if not self.yest_limit_map and mode == "INTRA_DAY":
            await self.lifecycle.on_startup()
            self.yest_limit_map = self.analyzer.get_yest_limit_map(date_str)

        prev_date = self.calendar.get_previous_trade_day(date_str)
        pc_map = await self._get_pre_close_map(prev_date)

        data_sh = date_str.replace("-", "")
        base_key = f"market:auction:{data_sh}:0925" if mode == "AUCTION" else f"market:auction:{data_sh}:latest"
        matched = self.redis.keys(f"{base_key}*")
        data_key = matched[0] if matched else base_key
        
        try:
            r_t = self.redis.type(data_key)
            raw = self.redis.hget(data_key, "top_amount") if r_t == 'hash' else self.redis.get(data_key)
            if raw:
                for raw_it in json.loads(raw):
                    item = await self._standardize_item(raw_it, 'REDIS')
                    if item: current_data.append(item)
        except: pass
            
        if not current_data and mode == "INTRA_DAY" and rust_snap:
            rust_idx = {str(k).strip()[-6:]: v for k, v in rust_snap.items() if k != "_EXTREMES_"}
            for code, r_it in rust_idx.items():
                p_c = pc_map.get(code, 0.0)
                c_p = r_it.get("price", 0.0)
                if c_p <= 0.1 or p_c <= 0.1: continue
                pct = c_p / p_c - 1.0
                yest_it = self.yest_limit_map.get(code)
                plate = self.metadata.stock_info.get(code, {}).get('plate', 'Other')
                res_factor = self.prime.get_plate_resonance(plate)
                current_data.append({
                    "code": code, "name": await self.metadata.get_name(code), "change_pct": pct,
                    "auction_amount_yuan": r_it.get("amount", 0.0), "plate": plate,
                    "lb_days": yest_it.lb_days if yest_it else 0, "is_yest_limit": True if yest_it else False,
                    "resonance_factor": res_factor, "vol_intensity": r_it.get("vol_intensity", 1.0) * res_factor
                })
        
        battle_kpis = self.prime.calculate_battle_kpis(date_str)
        if not current_data and mode == "AUCTION":
            current_data.extend(await self._fetch_tdengine_auction(date_str))
            if len(current_data) < 50: current_data.extend(await self._fetch_wencai_auction(date_str))

        if not current_data: return

        if mode == "AUCTION":
            for it in current_data: self.auction_snapshot[it["code"]] = it["change_pct"]

        report = await self.analyzer.analyze(current_data, auction_snapshot=self.auction_snapshot if mode == "INTRA_DAY" else None,
                                           yest_limit_map=self.yest_limit_map, yest_hot_plates=self.prime.hot_plates_map,
                                           date_str=date_str, battle_kpis=battle_kpis)
        
        if mode == "AUCTION" or self.is_first_session_run or abs(report.money_making_effect - self.last_sentiment) >= 0.5:
            print(f"\n{report.summary_text}\n")
            self.last_sentiment, self.is_first_session_run = report.money_making_effect, False

    async def run_guardian(self):
        logger.info("🛡️ MarketEdge V7.2.1 Strategic Guardian (对准增强版) 物理上线")
        date_str = datetime.now().strftime("%Y-%m-%d")
        self._today = date_str
        self._session = aiohttp.ClientSession()
        await self._startup_sync(date_str)
        
        asyncio.create_task(self._v2_tick_pump())
        await asyncio.sleep(2)
        
        # 初始推演 (盘前重播模式)
        now_hm = datetime.now().strftime("%H:%M")
        await self.execute_analysis(date_str, mode="AUCTION" if now_hm < "09:30" else "INTRA_DAY")

        while self.is_running:
            now = datetime.now()
            if now.weekday() >= 5:
                await asyncio.sleep(300); continue
            h_m = now.strftime("%H:%M")
            
            if h_m == "08:30":
                await self.lifecycle.on_startup()
                self.yest_limit_map = await self._fetch_kaipan_limit_ups(date_str)
                await asyncio.sleep(60)
            elif h_m == "09:26":
                self.yest_limit_map = await self._fetch_kaipan_limit_ups(date_str)
                await self.execute_analysis(date_str, mode="AUCTION")
                await asyncio.sleep(60)
            elif h_m == "15:05":
                logger.info("🏁 [Market-Close] 停止监听"); await asyncio.sleep(60)
            elif h_m == "16:30":
                await asyncio.get_event_loop().run_in_executor(None, self.kline_service.preload_latest_dates)
                await self.lifecycle.on_eod()
                try: subprocess.Popen([sys.executable, "v2_final_retro.py"])
                except: pass
                await asyncio.sleep(60)
            elif "09:30" <= h_m <= "15:00":
                if now.minute % 3 == 0 and now.second < 5: await self.execute_analysis(date_str, mode="INTRA_DAY")
                if now.second % 10 == 0:
                    sys.stdout.write(f"\r[{h_m}] 🩺 战役对准中 | 实时情绪:{self.last_sentiment}/10   ")
                    sys.stdout.flush()
                await asyncio.sleep(1)
            else:
                if now.second % 60 == 0: logger.info(f"🌑 [NightMode] 引擎基盘在线 | 时刻: {h_m}")
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
                if not ("09:15" <= h_m or "12:55" <= h_m <= "15:10"):
                    await asyncio.sleep(60); continue
                t_start = time.time()
                pipe = self.redis.pipeline()
                for k in redis_keys: pipe.hgetall(k)
                results = pipe.execute()
                await self.prime.sync_kaipan_hotspots()
                for k, q in zip(redis_keys, results):
                    if not q: continue
                    code = key_map[k]
                    p = float(q.get("price") or q.get("current", 0))
                    if p <= 0.1: continue 
                    self.bridge.push_tick_raw(code, p, float(q.get("amount", 0)), float(q.get("volume", 0)), q.get("time", "00:00:00"), float(q.get("bid_amount", 0)))
                    if code not in self.tick_history: self.tick_history[code] = deque(maxlen=45) 
                    self.tick_history[code].append((time.time(), p, float(q.get("amount", 0))))
                # if (time.time() - t_start) > 0.5: logger.warning(f"⚠️ [Pump] 延迟: {(time.time()-t_start)*1000:.2f}ms")
                await asyncio.sleep(3)
            except Exception as e: logger.error(f"❌ [Pump] Error: {e}"); await asyncio.sleep(5)

    async def _sync_and_calculate_stock(self, symbol: str) -> bool:
        try:
            target_day = self._today
            if datetime.now().hour < 15: target_day = self.calendar.get_previous_trading_day(self._today)
            k_list = await asyncio.get_event_loop().run_in_executor(None, self.kline_service.fetch_kline_data, symbol, 'd', None, target_day)
            if k_list and len(k_list) >= 5:
                res = await asyncio.get_event_loop().run_in_executor(None, self.chip_runner.calculate_for_stock, symbol, k_list, target_day)
                code, peak, factors = res
                if peak:
                    self.redis.hset(f"cache:chip_peaks:{target_day}", symbol, json.dumps(peak))
                    self.redis.hset(f"cache:stock_extra:{target_day}", symbol, json.dumps(factors))
                    await asyncio.get_event_loop().run_in_executor(None, self.tdengine.save_chips, symbol, peak)
                return True
            return False
        except: return False

    async def _sync_stock_dde(self, symbol: str) -> bool:
        try:
            target_date = self._today.replace("-", "")
            if datetime.now().hour < 16: target_date = self.calendar.get_previous_trading_day(self._today).replace("-", "")
            res = await asyncio.get_event_loop().run_in_executor(None, self.api_analyzer.get_his_stock_dde, symbol.split(".")[0], target_date)
            if not res or res.get('errcode') != '0': return False
            data = {k: v for k, v in res.items() if isinstance(v, list)}
            if not data: return True
            df = pd.DataFrame(data).head(20)
            df.rename(columns={'DDJE': 'ddje', 'Date': 'date', 'DDX': 'ddx', 'DDY': 'ddy', 'DDZ': 'ddz'}, inplace=True)
            return self.tdengine.save_daily_dde(symbol, df)
        except: return False

    async def _trigger_factor_calc(self) -> bool:
        try:
            if self.bridge.engine: self.bridge.reload_metadata()
            return True
        except: return False

if __name__ == "__main__":
    orc = AuctionOrchestrator()
    try: asyncio.run(orc.run_guardian())
    except KeyboardInterrupt: logger.info("👋 智库已下岗")
    finally:
        if orc._session: asyncio.get_event_loop().run_until_complete(orc._session.close())
