
import asyncio
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

try:
    from ai.API.StockAnalyzer import StockAnalyzer
except ImportError:
    StockAnalyzer = None

logger = logging.getLogger(__name__)

@dataclass
class StockRankItem:
    code: str
    score: float
    primary_theme: str
    top_theme_weight: float
    top_theme_conflict: bool
    auction_rank: int
    auction_bid_amount_yuan: float
    auction_change_pct: float
    amount_2min: float
    change_pct: float
    plate_ids: List[str]
    plate_best: str
    plate_best_score: float
    theme_score: float
    plate_score: float
    dde_score: float
    co_move_score: float
    follow_5m_ratio: float
    follow_30m_ratio: float
    co_move_active_peers: int
    resonance_role: str
    lead_follow_count: int
    lead_follow_ratio: float
    ts: int


@dataclass
class ExpectationSignal:
    code: str
    type: str  # 'price_gap', 'plate_divergence', 'volume_gap', 'weak_to_strong', 'strong_to_weak'
    change: float  # Current/Auction Change (%)
    score: float    # Signal Strength
    details: Dict[str, Any]
    reason: str
    
    def to_dict(self):
        return {
            "code": self.code,
            "type": self.type,
            "change": self.change,
            "score": self.score,
            "details": self.details,
            "reason": self.reason,
            "timestamp": int(time.time() * 1000)
        }


@dataclass
class ExecutionPolicy:
    ts: int
    position_max: float
    mode_allow: List[str]
    candidate_pool_key: str
    ban_conditions: List[str]
    risk_budget: Dict[str, Any]
    explain: Dict[str, Any]


class MarketEdgeEngine:
    """娴佺▼浜岋細璧氶挶鏁堝簲璇嗗埆/鎸栨帢绯荤粺锛堟渶灏忓彲鐢ㄩ棴鐜級銆?
    鐩爣锛氫笉褰卞搷 integrated_server 鐜版湁閫昏緫锛岀嫭绔嬪悗鍙颁换鍔″啓鍏?market/rank key銆?
    渚濊禆娉ㄥ叆锛?    - redis: aioredis client (decode_responses=True)
    - redis_storage: RedisStorageManager (鐢ㄤ簬 get_data/store_data 绛夊悓姝ュ皝瑁?
    - plate_updater: OptimizedEnhancedPlateUpdater
    - calendar: TradeCalendar
    - advanced_indicators: OptimizedAdvancedTechnicalIndicators
    - theme_ranker: ThemeRanker

    璇存槑锛?    - 鏈ā鍧楀疄鐜版祦绋嬩簩鏈€灏忛棴鐜細CandidatePool/PlateSpread/ThemeRank/ComfortExit/Sentiment/OpenScenario/ExecutionPolicy銆?    - 鑻ユ煇浜涗簨瀹炲眰 key 涓嶅瓨鍦紝ExecutionPolicy 浼氶檷绾у苟鏍囪 explain.stale=true銆?    """

    def __init__(
        self,
        *,
        redis,
        redis_storage,
        plate_updater,
        calendar,
        advanced_indicators,
        theme_ranker,
        stock_analyzer: Optional[StockAnalyzer] = None,
        candidate_pool_size: int = 500,
    ):
        self.redis = redis
        self.redis_storage = redis_storage
        self.plate_updater = plate_updater
        self.calendar = calendar
        self.advanced_indicators = advanced_indicators
        self.theme_ranker = theme_ranker
        self.stock_analyzer = stock_analyzer
        self.candidate_pool_size = candidate_pool_size

        self.candidate_pool_cache: Set[str] = set()
        self.last_candidate_pool_update: float = 0.0
        self.opening_verification_done: bool = False

        self.last_stock_rank_update: float = 0.0
        self.last_plate_spread_update: float = 0.0
        self.last_theme_rank_update: float = 0.0
        self.last_comfort_exit_update: float = 0.0
        self.last_sentiment_update: float = 0.0
        self.last_execution_policy_update: float = 0.0
        self.last_intraday_state_update: float = 0.0
        self.last_plate_attitude_update: float = 0.0
        self.last_static_precompute_update: float = 0.0
        self.last_fear_greed_update: float = 0.0
        self.last_herding_update: float = 0.0
        self.last_resonance_update: float = 0.0
        self.last_analysis_universe_update: float = 0.0
        self.last_market_overview_update: float = 0.0
        self.last_stock_profile_update: float = 0.0
        self.last_plate_snapshot_update: float = 0.0
        self.last_plate_profile_update: float = 0.0
        self.last_market_process_profile_update: float = 0.0

        # 璋冭瘯/鍥炴斁妯″紡锛氭墜鍔ㄦ寚瀹氭棩鏈?        self.manual_date: Optional[str] = None
        self.auction_profile_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        self.stock_state_cache: Dict[str, Dict[str, Any]] = {}
        self.plate_weight_cache: Dict[str, List[Tuple[str, float]]] = {}
        self.precomputed_static_date: Optional[str] = None
        self.last_precompute_signature: str = ""
        self.last_precompute_ts: float = 0.0
        self.intraday_transition_seen: Dict[str, int] = {}
        self.return_history: Dict[str, List[float]] = {}
        self.leading_plate_history: List[Tuple[int, str]] = []
        self.log_last_payload: Dict[str, str] = {}
        self.log_last_ts: Dict[str, float] = {}
        self.analysis_universe_cache: Set[str] = set()
        self.profile_transition_seen: Dict[str, int] = {}
        self.code_change_history: Dict[str, List[Tuple[int, float]]] = {}

        # 鍒嗗眰棰戠巼锛堢锛夛細楠岃瘉妯″紡锛堜綆棰戯級
        # 鐩爣锛氬厛楠岃瘉閫昏緫涓庢祦绋嬶紝涓嶈拷姹傞珮棰戝疄鏃舵€?
        self.task_intervals: Dict[str, int] = {
            "candidate_pool": 300,
            "indicators": 60,
            "plate_spread": 300,
            "theme_rank": 300,
            "comfort_exit": 300,
            "sentiment": 300,
            "stock_rank": 300,
            "intraday_state": 60,
            "plate_attitude": 60,
            "execution_policy": 60,
            "static_precompute": 1800,
            "fear_greed": 300,
            "herding": 300,
            "resonance": 300,
            "analysis_universe": 300,
            "market_overview": 300,
            "stock_profile": 300,
            "plate_snapshot": 300,
            "plate_profile": 300,
            "market_process_profile": 300,
        }

    async def build_candidate_pool(self, today_str: str) -> Set[str]:
        candidate_pool: Set[str] = set()

        # 1) 绔炰环 0925 TopN
        try:
            today_yyyymmdd = today_str.replace('-', '')
            auction_key = f"market:auction:{today_yyyymmdd}:0925"
            top_amount_json = await self.redis.hget(auction_key, "top_amount")
            if top_amount_json:
                top_amount_list = json.loads(top_amount_json)
                for item in top_amount_list:
                    code = item.get('symbol')
                    if code and len(code) == 6:
                        candidate_pool.add(code)
        except Exception:
            pass

        # 2) 涓ユ牸棣栨澘姹?stock:first_limit_up
        try:
            first_limit_items = await self.redis.zrange("stock:first_limit_up", 0, -1)
            for item_json in first_limit_items:
                try:
                    item = json.loads(item_json)
                    code = item.get('symbol')
                    if code and len(code) == 6:
                        candidate_pool.add(code)
                except Exception:
                    continue
        except Exception:
            pass

        # 3) 鏄ㄦ棩娑ㄥ仠 limit_up_{prev_day}
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)
            limit_up_key = f"limit_up_{prev_day}"
            prev_limit_up_data = self.redis_storage.get_data(limit_up_key)
            if prev_limit_up_data:
                for item in prev_limit_up_data:
                    if isinstance(item, dict):
                        code = item.get('鑲＄エ浠ｇ爜', '') or item.get('code', '')
                        if code and len(code) == 6:
                            candidate_pool.add(code)
        except Exception:
            pass

        # 鎴柇锛堜繚鎸佺ǔ瀹氾級
        if len(candidate_pool) > self.candidate_pool_size:
            candidate_pool = set(list(candidate_pool)[: self.candidate_pool_size])
            
        # 濡傛灉姹犲瓙杩樻槸绌虹殑锛屽皾璇曚粠鏉垮潡涓偂琛ュ厖锛堥槻姝㈠喎鍚姩瀹屽叏鏃犳硶杩愯锛?        if not candidate_pool and self.manual_date:
            logger.info("鈿狅笍 鍊欓€夋睜涓虹┖锛屽皾璇曚粠娲昏穬鏉垮潡琛ュ厖Top鑲＄エ鐢ㄤ簬鍥炴斁娴嬭瘯...")
            # 绠€鍗曞彇鍑犱釜娲昏穬鏉垮潡鐨勬垚鍒嗚偂
            if hasattr(self.plate_updater, 'main_plates'):
                main_plates = self.plate_updater.main_plates.keys()
            elif hasattr(self.plate_updater, 'main_plates_map'): # fallback for compatibility
                main_plates = self.plate_updater.main_plates_map.keys()
            else:
                main_plates = []

            count = 0
            # Convert keys to list for indexing
            mp_list = list(main_plates)
            if mp_list:
                for pid in mp_list[:5]:
                    stocks = self.plate_updater.plate_to_stocks.get(pid, [])
                    for s in stocks:
                        candidate_pool.add(s)
                        count += 1
                        if count >= 100:
                            break
                    if count >= 100:
                        break

        cache_key = f"cache:candidate_pool:{today_str}"
        await self.redis.set(cache_key, json.dumps(list(candidate_pool), ensure_ascii=False), ex=86400)
        self._log_event("candidate_pool", f"馃強 鍊欓€夋睜鏇存柊: {len(candidate_pool)} 鍙偂绁?, min_interval_sec=300)
        return candidate_pool

    def _safe_pct_from_quote(self, quote: Dict[str, Any], key: str, pre_close: float, fallback: float) -> float:
        v = self._safe_float(quote.get(key), fallback)
        if v and abs(v) < 1e-8:
            return v
        # 閮ㄥ垎琛屾儏婧愭病鏈?high/low 鐨勬定骞咃紝鍙粰缁濆浠锋牸
        price_key = key.replace("_pct", "")
        if pre_close > 0:
            abs_px = self._safe_float(quote.get(price_key), 0.0)
            if abs_px > 0:
                return (abs_px - pre_close) / pre_close * 100.0
        return v

    def _current_phase(self) -> str:
        now = datetime.now().time()
        if now < datetime.strptime("09:15", "%H:%M").time():
            return "pre_open"
        if now < datetime.strptime("09:30", "%H:%M").time():
            return "auction"
        if now <= datetime.strptime("09:40", "%H:%M").time():
            return "opening"
        if now < datetime.strptime("11:30", "%H:%M").time():
            return "intraday_am"
        if now < datetime.strptime("13:00", "%H:%M").time():
            return "lunch_break"
        if now <= datetime.strptime("15:00", "%H:%M").time():
            return "intraday_pm"
        if now <= datetime.strptime("18:10", "%H:%M").time():
            return "post_close"
        return "evening"

    async def _get_auction_profile(self, today_str: str) -> Dict[str, Dict[str, float]]:
        cached = self.auction_profile_cache.get(today_str)
        if cached is not None:
            return cached

        profile: Dict[str, Dict[str, float]] = {}
        try:
            today_yyyymmdd = today_str.replace("-", "")
            auction_key = f"market:auction:{today_yyyymmdd}:0925"
            top_amount_json = await self.redis.hget(auction_key, "top_amount")
            if top_amount_json:
                top_amount_list = json.loads(top_amount_json)
                for idx, it in enumerate(top_amount_list):
                    code = it.get("symbol")
                    if not code or len(code) != 6:
                        continue
                    profile[code] = {
                        "rank": float(idx),
                        "change_pct": self._safe_float(it.get("change_pct", 0), 0.0),
                        "bid_amount_yuan": self._safe_float(it.get("bid_amount_yuan", 0), 0.0),
                    }
        except Exception:
            profile = {}

        self.auction_profile_cache[today_str] = profile
        return profile

    async def _fetch_quotes_batch(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        if not codes:
            return {}
        pipe = self.redis.pipeline()
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        rows = await pipe.execute()
        return {codes[i]: (rows[i] or {}) for i in range(len(codes))}

    async def build_analysis_universe(self, today_str: str, candidate_pool: Set[str], limit: int = 4000) -> Set[str]:
        """Build a broader universe for market/plate/profile analysis."""
        universe: Set[str] = set(candidate_pool or set())

        # Existing mapping already contains stock<->plate relation; use it first.
        try:
            all_codes = list(getattr(self.plate_updater, "stock_to_plates", {}).keys())
            for code in all_codes:
                if code and len(code) == 6:
                    universe.add(code)
        except Exception:
            pass

        # Auction top list
        try:
            today_yyyymmdd = today_str.replace("-", "")
            auction_key = f"market:auction:{today_yyyymmdd}:0925"
            top_amount_json = await self.redis.hget(auction_key, "top_amount")
            if top_amount_json:
                top_amount_list = json.loads(top_amount_json)
                for item in top_amount_list[:1200]:
                    code = item.get("symbol")
                    if code and len(code) == 6:
                        universe.add(code)
        except Exception:
            pass

        # First limit pool
        try:
            items = await self.redis.zrange("stock:first_limit_up", 0, -1)
            for item_json in items:
                try:
                    item = json.loads(item_json)
                    code = item.get("symbol")
                    if code and len(code) == 6:
                        universe.add(code)
                except Exception:
                    continue
        except Exception:
            pass

        # Yesterday limit-up pool
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)
            prev_key = f"limit_up_{prev_day}"
            prev_data = self.redis_storage.get_data(prev_key) or []
            for item in prev_data[:1200]:
                if not isinstance(item, dict):
                    continue
                code = item.get("鑲＄エ浠ｇ爜", "") or item.get("code", "")
                if code and len(code) == 6:
                    universe.add(code)
        except Exception:
            pass

        # Recent volatile pool
        try:
            volatile_items = await self.redis.zrevrange("stock:volatile_pool", 0, 1200)
            for item_json in volatile_items:
                try:
                    item = json.loads(item_json)
                    code = item.get("symbol")
                    if code and len(code) == 6:
                        universe.add(code)
                except Exception:
                    continue
        except Exception:
            pass

        if len(universe) > limit:
            universe = set(list(universe)[:limit])

        await self.redis.set(
            f"cache:analysis_universe:{today_str}",
            json.dumps(list(universe), ensure_ascii=False),
            ex=86400,
        )
        return universe

    def _weighted_plates_for_code(self, code: str) -> List[Tuple[str, float]]:
        weighted = self.plate_weight_cache.get(code)
        if weighted:
            return weighted
        pids = self.plate_updater.stock_to_plates.get(code, []) or []
        if not pids:
            return []
        w = 1.0 / len(pids)
        return [(pid, w) for pid in pids]

    def _build_weighted_plate_map(self, universe: Set[str]) -> Dict[str, List[Tuple[str, float]]]:
        weighted: Dict[str, List[Tuple[str, float]]] = {}
        for code in universe:
            pids = self.plate_updater.stock_to_plates.get(code, []) or []
            if not pids:
                continue
            # 涓绘澘鍧楁潈閲嶆洿楂橈紝閬垮厤澶氬澶氭椂褰掑睘杩囨暎
            w_raw: List[Tuple[str, float]] = []
            total = 0.0
            for pid in pids:
                is_main = False
                if hasattr(self.plate_updater, "main_plates") and pid in self.plate_updater.main_plates:
                    is_main = True
                elif hasattr(self.plate_updater, "main_plates_map") and pid in self.plate_updater.main_plates_map:
                    is_main = True
                w = 1.2 if is_main else 1.0
                total += w
                w_raw.append((pid, w))
            if total <= 0:
                continue
            weighted[code] = [(pid, w / total) for pid, w in w_raw]
        return weighted

    def _theme_match_plate(self, theme: str, plate_name: str) -> bool:
        if not theme or not plate_name:
            return False
        t = str(theme).strip().lower()
        p = str(plate_name).strip().lower()
        if not t or not p:
            return False
        return t in p or p in t

    async def _build_weighted_plate_map_dynamic(
        self, today_str: str, universe: Set[str]
    ) -> Tuple[Dict[str, List[Tuple[str, float]]], Dict[str, List[Dict[str, Any]]]]:
        """鍔ㄦ€佹澘鍧楀綊灞炴潈閲嶏細
        浣跨敤鐜版湁浜嬪疄灞傦紙plate_spread/plate_attitude/plate_metrics/theme_evidence锛夎В鍐充釜鑲″鏉垮潡鍐茬獊銆?        """
        if not universe:
            return {}, {}

        # 1) 棰勮鍙栧叏灞€鏉垮潡寮哄急锛堟墿鏁?鎬佸害锛?        spread_pairs = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 500, withscores=True)
        spread_map: Dict[str, float] = {pid: float(score) for pid, score in spread_pairs}
        max_spread = max(spread_map.values()) if spread_map else 1.0

        att_pairs = await self.redis.zrange(f"rank:plate_attitude:{today_str}", 0, -1, withscores=True)
        attitude_map: Dict[str, float] = {pid: float(score) for pid, score in att_pairs}
        max_att = max(abs(v) for v in attitude_map.values()) if attitude_map else 1.0

        # 2) 鎵归噺璇讳釜鑲￠鏉愯瘉鎹?        evidence_key = f"cache:stock_theme_evidence:{today_str}"
        pipe = self.redis.pipeline()
        for code in universe:
            pipe.hget(evidence_key, code)
        ev_list = await pipe.execute()
        evidence_map: Dict[str, Dict[str, Any]] = {}
        for idx, code in enumerate(list(universe)):
            raw = ev_list[idx]
            if not raw:
                continue
            try:
                evidence_map[code] = json.loads(raw)
            except Exception:
                continue

        # 3) 瀵规瘡鍙偂绁ㄥ仛褰掑睘鎵撳垎
        weighted: Dict[str, List[Tuple[str, float]]] = {}
        explain_map: Dict[str, List[Dict[str, Any]]] = {}
        for code in universe:
            pids = self.plate_updater.stock_to_plates.get(code, []) or []
            if not pids:
                continue

            ev = evidence_map.get(code, {})
            primary_theme = str(ev.get("primary_theme", "") or "")

            score_items: List[Tuple[str, float]] = []
            score_detail_items: List[Dict[str, Any]] = []
            has_sub_plate = any(self.plate_updater.all_plates.get(pid, {}).get("type") == "sub" for pid in pids)
            for pid in pids:
                pinfo = self.plate_updater.all_plates.get(pid, {})
                pname = pinfo.get("name", pid)
                ptype = pinfo.get("type", "unknown")

                # 鍩虹鍒嗭細涓绘澘鍧楄交寰珮浜庡瓙鏉垮潡锛岃嫢鍚岃偂瀛樺湪瀛愭澘鍧楋紝涓绘澘鍧楅€傚害闄嶆潈閬垮厤閲嶅
                base = 1.15 if ptype == "main" else 1.0
                if ptype == "main" and has_sub_plate:
                    base *= 0.75

                # 鎵╂暎鐑害鍒嗭紙0~1锛?                spread_score = spread_map.get(pid, 0.0)
                spread_norm = max(0.0, min(1.0, spread_score / max(1e-6, max_spread)))

                # 鏉垮潡鎬佸害鍒嗭紙-1~1 -> 0~1锛?                att = attitude_map.get(pid, 0.0)
                att_norm = max(0.0, min(1.0, 0.5 + 0.5 * (att / max(1e-6, max_att))))

                # 褰撳墠鏉垮潡娑ㄥ箙鍒嗭紙-2~+2绾挎€у埌0~1锛?                pm = self.plate_updater.get_plate_metrics(pid) or {}
                pchg = self._safe_float(pm.get("change_pct", 0.0), 0.0)
                pchg_norm = max(0.0, min(1.0, (pchg + 2.0) / 4.0))

                # 棰樻潗鍖归厤鍒嗭紙鏈€鍏抽敭锛?                theme_boost = 1.0
                if primary_theme and self._theme_match_plate(primary_theme, pname):
                    theme_boost = 1.8

                # 缁煎悎
                score = (
                    base * 0.45
                    + spread_norm * 0.20
                    + att_norm * 0.15
                    + pchg_norm * 0.20
                ) * theme_boost
                final_score = max(0.0001, float(score))
                score_items.append((pid, final_score))
                score_detail_items.append(
                    {
                        "plate_id": pid,
                        "plate_name": pname,
                        "plate_type": ptype,
                        "base": round(base, 6),
                        "spread_norm": round(spread_norm, 6),
                        "att_norm": round(att_norm, 6),
                        "pchg_norm": round(pchg_norm, 6),
                        "theme_boost": round(theme_boost, 6),
                        "raw_score": round(final_score, 6),
                    }
                )

            total = sum(s for _, s in score_items)
            if total <= 0:
                n = len(score_items)
                weighted[code] = [(pid, 1.0 / n) for pid, _ in score_items]
            else:
                # 鎴柇闀垮熬锛岄伩鍏嶄竴涓偂绁ㄨ澶鏉垮潡鍒嗘暎锛堜繚鐣檛op3锛?                score_items.sort(key=lambda x: x[1], reverse=True)
                score_items = score_items[:3]
                total2 = sum(s for _, s in score_items)
                weighted[code] = [(pid, s / total2) for pid, s in score_items]
            if score_detail_items:
                score_by_pid = dict(weighted.get(code, []))
                for d in score_detail_items:
                    d["weight"] = round(float(score_by_pid.get(d["plate_id"], 0.0)), 6)
                score_detail_items.sort(key=lambda x: x["weight"], reverse=True)
                explain_map[code] = score_detail_items[:5]

        return weighted, explain_map

    async def precompute_static_context(
        self,
        today_str: str,
        universe: Set[str],
        force: bool = False,
        source: str = "unknown",
    ) -> None:
        if not universe:
            return

        now_ts = time.time()
        signature = f"{today_str}:{len(universe)}:{hash(tuple(sorted(universe)))}"

        if (
            not force
            and self.precomputed_static_date == today_str
            and self.plate_weight_cache
            and self.last_precompute_signature == signature
        ):
            return

        if (
            not force
            and self.precomputed_static_date == today_str
            and self.plate_weight_cache
            and (now_ts - self.last_precompute_ts) < 180
        ):
            return

        # 浼樺厛鍔ㄦ€佹槧灏勶紝澶辫触鍥為€€闈欐€佹槧灏?        explain_map: Dict[str, List[Dict[str, Any]]] = {}
        try:
            self.plate_weight_cache, explain_map = await self._build_weighted_plate_map_dynamic(today_str, universe)
            if not self.plate_weight_cache:
                self.plate_weight_cache = self._build_weighted_plate_map(universe)
        except Exception:
            self.plate_weight_cache = self._build_weighted_plate_map(universe)
        self.precomputed_static_date = today_str
        self.last_precompute_signature = signature
        self.last_precompute_ts = now_ts

        # 钀界洏缂撳瓨锛氱洏涓洿鎺ュ鐢紝鍑忓皯澶氬澶氶噸澶嶈绠?        pipe = self.redis.pipeline()
        weight_key = f"cache:stock_plate_weights:{today_str}"
        explain_key = f"cache:stock_plate_weight_explain:{today_str}"
        pipe.delete(weight_key)
        pipe.delete(explain_key)
        for code, items in self.plate_weight_cache.items():
            payload = [{"plate_id": pid, "w": round(w, 6)} for pid, w in items]
            pipe.hset(weight_key, code, json.dumps(payload, ensure_ascii=False))
            if code in explain_map:
                pipe.hset(explain_key, code, json.dumps(explain_map[code], ensure_ascii=False))
        pipe.expire(weight_key, 86400)
        pipe.expire(explain_key, 86400)
        await pipe.execute()

        logger.info(f"🧱 板块归属预计算完成: 日期={today_str}, 股票={len(self.plate_weight_cache)}, 来源={source}")

    async def update_intraday_state_machine(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> List[ExpectationSignal]:
        if not candidate_pool:
            return []

        auction_profile = await self._get_auction_profile(today_str)
        signals: List[ExpectationSignal] = []

        # 鎵归噺璇诲彇琛屾儏
        pipe = self.redis.pipeline()
        codes = list(candidate_pool)
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        quotes = await pipe.execute()

        now_ts = int(time.time() * 1000)
        transitions_for_redis: Dict[str, Dict[str, Any]] = {}

        for idx, code in enumerate(codes):
            q = quotes[idx] or {}
            ind = indicators.get(code, {}) or {}
            auction_change = self._safe_float(auction_profile.get(code, {}).get("change_pct", 0.0), 0.0)
            curr_change = self._safe_float(q.get("change_pct", q.get("change", 0.0)), 0.0)
            pre_close = self._safe_float(q.get("pre_close", q.get("last_close", 0.0)), 0.0)

            high_pct = self._safe_pct_from_quote(q, "high_pct", pre_close, curr_change)
            low_pct = self._safe_pct_from_quote(q, "low_pct", pre_close, curr_change)
            rebound_from_low = curr_change - low_pct
            drawdown_from_high = high_pct - curr_change

            snap = {
                "ts": now_ts,
                "auction_change_pct": auction_change,
                "change_pct": curr_change,
                "high_pct": high_pct,
                "low_pct": low_pct,
                "rebound_from_low": rebound_from_low,
                "drawdown_from_high": drawdown_from_high,
                "amount_2min": self._safe_float(ind.get("amount_2min", 0.0), 0.0),
            }
            prev = self.stock_state_cache.get(code)
            self.stock_state_cache[code] = snap

            if prev is None:
                continue

            # 1) 绔炰环寮?-> 鎷夊崌鐖嗗ご
            if auction_change >= 3.0 and drawdown_from_high >= 4.0 and curr_change <= auction_change - 2.0:
                sig = ExpectationSignal(
                    code=code,
                    type="auction_strong_then_headshot",
                    change=curr_change,
                    score=3.2,
                    details={
                        "auction": round(auction_change, 2),
                        "high_pct": round(high_pct, 2),
                        "current_pct": round(curr_change, 2),
                        "drawdown": round(drawdown_from_high, 2),
                    },
                    reason=f"绔炰环寮簕auction_change:.1f}%鍚庡啿楂樺洖钀斤紝鍥炴挙{drawdown_from_high:.1f}%",
                )
                signals.append(sig)

            # 2) 寮€鐩樹笅鐮稿悗娣盫鎷夊洖
            if low_pct <= -4.0 and rebound_from_low >= 4.0 and curr_change >= -1.0:
                sig = ExpectationSignal(
                    code=code,
                    type="intraday_deep_v_rebound",
                    change=curr_change,
                    score=3.0,
                    details={
                        "low_pct": round(low_pct, 2),
                        "current_pct": round(curr_change, 2),
                        "rebound": round(rebound_from_low, 2),
                    },
                    reason=f"鐩樹腑娣盫淇锛屼綆鐐箋low_pct:.1f}%鍥炴媺鑷硔curr_change:.1f}%",
                )
                signals.append(sig)

            # 3) 绔炰环寮?-> 涓嬬牳鍚庡弽鏍?            if auction_change <= 1.0 and low_pct <= -8.0 and curr_change >= -2.0 and rebound_from_low >= 6.0:
                sig = ExpectationSignal(
                    code=code,
                    type="auction_weak_then_floor_reactor",
                    change=curr_change,
                    score=3.5,
                    details={
                        "auction": round(auction_change, 2),
                        "low_pct": round(low_pct, 2),
                        "current_pct": round(curr_change, 2),
                    },
                    reason=f"绔炰环寮卞悗鐮告繁姘村啀鍙嶆牳锛屼綆鐐箋low_pct:.1f}%",
                )
                signals.append(sig)

            # 4) 绔炰环寮?-> 鐩樹腑杞己
            if auction_change <= -1.0 and curr_change >= auction_change + 3.0 and curr_change > 0:
                sig = ExpectationSignal(
                    code=code,
                    type="auction_weak_to_intraday_strong",
                    change=curr_change,
                    score=2.4,
                    details={"auction": round(auction_change, 2), "current_pct": round(curr_change, 2)},
                    reason=f"绔炰环寮辫浆寮猴紝{auction_change:.1f}% -> {curr_change:.1f}%",
                )
                signals.append(sig)

            # 鍘婚噸锛氬悓鑲＄エ鍚岀被鍨?90 绉掑唴鍙繚鐣欎竴娆?            for sig in signals[-4:]:
                dedup_key = f"{today_str}:{sig.code}:{sig.type}"
                last_ts = self.intraday_transition_seen.get(dedup_key, 0)
                if now_ts - last_ts >= 90_000:
                    self.intraday_transition_seen[dedup_key] = now_ts
                    transitions_for_redis[f"{sig.code}:{sig.type}:{now_ts}"] = sig.to_dict()

        if transitions_for_redis:
            key = f"rank:intraday_transition:{today_str}"
            p = self.redis.pipeline()
            p.zadd(key, {json.dumps(v, ensure_ascii=False): now_ts for v in transitions_for_redis.values()})
            p.expire(key, 86400)
            await p.execute()
            type_count: Dict[str, int] = {}
            for item in transitions_for_redis.values():
                t = item.get("type", "unknown")
                type_count[t] = type_count.get(t, 0) + 1
            summary = ",".join([f"{k}:{v}" for k, v in sorted(type_count.items(), key=lambda x: x[1], reverse=True)])
            self._log_event(
                "intraday_transition",
                f"鈿?鐩樹腑杞姌淇″彿: {len(transitions_for_redis)} 鏉?| {summary}",
                min_interval_sec=120,
            )

        return list(transitions_for_redis.values())

    async def calculate_plate_attitude(self, today_str: str, transitions: List[Dict[str, Any]]) -> None:
        if not transitions:
            return

        if not self.plate_weight_cache:
            await self.precompute_static_context(
                today_str,
                self.candidate_pool_cache or set(),
                force=False,
                source="plate_attitude_bootstrap",
            )

        positive_types = {
            "intraday_deep_v_rebound",
            "auction_weak_then_floor_reactor",
            "auction_weak_to_intraday_strong",
        }
        negative_types = {"auction_strong_then_headshot"}

        plate_scores: Dict[str, float] = {}
        plate_event_counts: Dict[str, int] = {}
        for item in transitions:
            code = item.get("code")
            sig_type = item.get("type", "")
            score = self._safe_float(item.get("score", 0.0), 0.0)
            if not code or score <= 0:
                continue
            direction = 0.0
            if sig_type in positive_types:
                direction = 1.0
            elif sig_type in negative_types:
                direction = -1.0
            if abs(direction) < 1e-8:
                continue

            for pid, w in self.plate_weight_cache.get(code, []):
                plate_scores[pid] = plate_scores.get(pid, 0.0) + direction * score * w
                plate_event_counts[pid] = plate_event_counts.get(pid, 0) + 1

        if not plate_scores:
            return

        zkey = f"rank:plate_attitude:{today_str}"
        dkey = f"rank:plate_attitude:details:{today_str}"
        p = self.redis.pipeline()
        p.delete(zkey)
        p.delete(dkey)
        ts = int(time.time() * 1000)
        for pid, score in plate_scores.items():
            name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
            p.zadd(zkey, {pid: round(score, 4)})
            p.hset(
                dkey,
                pid,
                json.dumps(
                    {
                        "ts": ts,
                        "id": pid,
                        "name": name,
                        "attitude_score": round(score, 4),
                        "events": plate_event_counts.get(pid, 0),
                    },
                    ensure_ascii=False,
                ),
            )
        p.expire(zkey, 86400)
        p.expire(dkey, 86400)
        await p.execute()

    async def calculate_plate_spread(self, today_str: str, candidate_pool: Set[str], stock_indicators: Dict[str, Dict]) -> None:
        """
        璁＄畻鏉垮潡鍒嗘搴?        浼樺寲锛氭帴鍙?stock_indicators 鍙傛暟锛岄伩鍏嶉噸澶嶈幏鍙?        """
        if not candidate_pool:
            return
            
        # 濡傛灉鏈紶鍏ユ寚鏍囷紝鍒欏唴閮ㄨ幏鍙栵紙鍏煎鎬э級
        if stock_indicators is None:
            stock_indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))

        STRONG_CHANGE = 1.0
        STRONG_AMT_2MIN = 10_000_000

        plate_stats: Dict[str, Dict[str, Any]] = {}

        for code6 in candidate_pool:
            ind = stock_indicators.get(code6)
            if not ind:
                continue

            plate_ids = self.plate_updater.stock_to_plates.get(code6, [])
            if not plate_ids:
                continue

            is_strong = (
                float(ind.get('change_pct', 0) or 0) > STRONG_CHANGE
                and float(ind.get('amount_2min', 0) or 0) > STRONG_AMT_2MIN
            )

            for pid in plate_ids:
                s = plate_stats.setdefault(pid, {"N_active": 0, "N_strong": 0, "sum_amount_2min": 0.0})
                s["N_active"] += 1
                if is_strong:
                    s["N_strong"] += 1
                    s["sum_amount_2min"] += float(ind.get('amount_2min', 0) or 0)

        zset_key = f"rank:plate_spread:{today_str}"
        details_key = f"rank:plate_spread:details:{today_str}"
        pipe = self.redis.pipeline()
        pipe.delete(zset_key)
        pipe.delete(details_key)

        log_entries = []

        for pid, s in plate_stats.items():
            if s["N_active"] <= 0:
                continue
            spread_ratio = s["N_strong"] / s["N_active"]
            amount_score = float(np.log1p(s["sum_amount_2min"] / 1_000_000))
            spread_score = round(spread_ratio * 50 + amount_score * 5, 2)
            if spread_score <= 0:
                continue

            name = self.plate_updater.all_plates.get(pid, {}).get('name', pid)
            detail = {
                "ts": int(time.time() * 1000),
                "id": pid,
                "name": name,
                "score": spread_score,
                "spread_ratio": round(spread_ratio, 4),
                "N_active": s["N_active"],
                "N_strong": s["N_strong"],
                "sum_amount_2min": s["sum_amount_2min"],
            }

            pipe.zadd(zset_key, {pid: spread_score})
            pipe.hset(details_key, pid, json.dumps(detail, ensure_ascii=False))
            
            log_entries.append({"name": name, "score": spread_score})

        pipe.expire(zset_key, 86400)
        pipe.expire(details_key, 86400)
        await pipe.execute()
        
        # Log top plates
        if log_entries:
            top_plates = sorted(log_entries, key=lambda x: x["score"], reverse=True)[:3]
            plate_strs = [f"{p['name']}({p['score']})" for p in top_plates]
            self._log_event("plate_top3", f"馃敟 鐑棬鏉垮潡 Top3: {', '.join(plate_strs)}", min_interval_sec=300)

    async def calculate_theme_rank(self, today_str: str, candidate_pool: Set[str], stock_indicators: Dict[str, Dict]) -> None:
        """
        璁＄畻棰樻潗鎺掕
        浼樺寲锛氭帴鍙?stock_indicators 鍙傛暟
        """
        if not candidate_pool:
            return
            
        if stock_indicators is None:
            stock_indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))

        # 涓ユ牸棣栨澘闆嗗悎
        first_limit_set: Set[str] = set()
        try:
            items = await self.redis.zrange("stock:first_limit_up", 0, -1)
            for item_json in items:
                try:
                    it = json.loads(item_json)
                    code = it.get('symbol')
                    if code and len(code) == 6:
                        first_limit_set.add(code)
                except Exception:
                    continue
        except Exception:
            pass

        themes, details, evidence = self.theme_ranker.build(
            today_str=today_str,
            candidate_pool=candidate_pool,
            indicators_by_stock=stock_indicators,
            first_limit_set=first_limit_set,
            top_n=50,
        )

        zset_key = f"rank:theme:{today_str}"
        details_key = f"rank:theme:details:{today_str}"
        evidence_key = f"cache:stock_theme_evidence:{today_str}"

        pipe = self.redis.pipeline()
        pipe.delete(zset_key)
        pipe.delete(details_key)
        pipe.delete(evidence_key)

        for t in themes:
            pipe.zadd(zset_key, {t.theme: t.score})
        for theme, d in details.items():
            pipe.hset(details_key, theme, json.dumps(d, ensure_ascii=False))
        for code6, ev in evidence.items():
            pipe.hset(evidence_key, code6, json.dumps(ev, ensure_ascii=False))

        pipe.expire(zset_key, 86400)
        pipe.expire(details_key, 86400)
        pipe.expire(evidence_key, 86400)
        await pipe.execute()

        if themes:
            top_themes_log = [f"{t.theme}({t.score:.1f})" for t in themes[:3]]
            self._log_event("theme_top3", f"馃幁 鐑棬棰樻潗 Top3: {', '.join(top_themes_log)}", min_interval_sec=300)

    async def calculate_comfort_exit(self, today_str: str) -> None:
        """ComfortExitScore锛氭槰鏃ヨ禋閽辩殑浜轰粖澶╄兘鍚﹁垝鏈嶇鍦恒€?""
        prev_day = self.calendar.get_previous_trade_day(today_str)

        y_key = f"limit_up_{prev_day}"
        y_list = self.redis_storage.get_data(y_key)
        
        # 闄嶇骇锛氬鏋滄壘涓嶅埌鏄ㄦ棩娑ㄥ仠锛屽皾璇曟壘鍓嶆棩鐨勶紙搴斿鍥炴斁鏁版嵁涓嶅叏锛?        if not y_list:
            prev_prev_day = self.calendar.get_previous_trade_day(prev_day)
            y_key = f"limit_up_{prev_prev_day}"
            y_list = self.redis_storage.get_data(y_key)
            
        if not y_list or not isinstance(y_list, list):
            return

        y_stocks: Dict[str, Dict[str, Any]] = {}
        for item in y_list:
            if not isinstance(item, dict):
                continue
            code = item.get('鑲＄エ浠ｇ爜', '') or item.get('code', '')
            if code and len(code) == 6:
                y_stocks[code] = {"lb_days": item.get('杩炴澘澶╂暟', 1)}

        if not y_stocks:
            return

        # 0925绔炰环 top_amount
        top_rank: Dict[str, int] = {}
        top_bid: Dict[str, float] = {}
        try:
            today_yyyymmdd = today_str.replace('-', '')
            auction_key = f"market:auction:{today_yyyymmdd}:0925"
            top_amount_json = await self.redis.hget(auction_key, "top_amount")
            if top_amount_json:
                top_amount_list = json.loads(top_amount_json)
                top_rank = {it.get('symbol'): idx for idx, it in enumerate(top_amount_list) if it.get('symbol')}
                top_bid = {it.get('symbol'): float(it.get('bid_amount_yuan', 0) or 0) for it in top_amount_list if it.get('symbol')}
        except Exception:
            pass

        # 鎵归噺璇诲彇瀹炴椂琛屾儏
        pipe = self.redis.pipeline()
        codes = list(y_stocks.keys())
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        quotes = await pipe.execute()

        scores: List[float] = []
        weights: List[float] = []
        support_count = 0
        dump_count = 0
        high_pos_total = 0
        high_pos_dump_count = 0

        for i, code in enumerate(codes):
            q = quotes[i] or {}

            # change_pct 浼樺厛锛屽吋瀹?change
            change_now = q.get('change_pct', None)
            if change_now is None or change_now == "":
                change_now = q.get('change', 0)
            try:
                change_now = float(change_now or 0)
            except Exception:
                change_now = 0.0

            amount_now = q.get('amount', 0)
            try:
                amount_now = float(amount_now or 0)
            except Exception:
                amount_now = 0.0

            rank = top_rank.get(code, 9999)
            rank_bonus = max(0.0, 20.0 - rank / 50.0)  # Top1000鍐呰秺闈犲墠瓒婇珮

            bid = float(top_bid.get(code, 0.0) or 0.0)
            bid_bonus = min(20.0, (bid / 100_000_000.0) * 5.0)  # 姣?浜垮皝鍗曗増5鍒嗭紝涓婇檺20

            open_bonus = 10.0 if change_now > 0 else -10.0

            dump_penalty = 0.0
            if change_now < -3.0 and amount_now > 50_000_000:
                dump_penalty = 15.0

            score = max(0.0, min(100.0, 50.0 + rank_bonus + bid_bonus + open_bonus - dump_penalty))
            scores.append(score)

            lb_days = y_stocks[code].get('lb_days', 1)
            try:
                lb_days = int(lb_days)
            except Exception:
                lb_days = 1

            weight = 1.0 + 0.3 * (max(1, lb_days) - 1)
            weights.append(weight)

            if score >= 60:
                support_count += 1
            if score <= 40:
                dump_count += 1

            if lb_days >= 3:
                high_pos_total += 1
                if change_now < 0:
                    high_pos_dump_count += 1

        # Log Leaders and Laggards in Yesterday's Limit Up pool
        # Re-construct simple list for logging
        y_pool_performance = []
        for i, code in enumerate(codes):
             q = quotes[i] or {}
             chg = 0.0
             try:
                 # Logic duplications from above, but safe
                 c = q.get('change_pct', q.get('change', 0))
                 chg = float(c) if c else 0.0
             except: pass
             y_pool_performance.append({"code": code, "change": chg})
        
        y_pool_performance.sort(key=lambda x: x['change'], reverse=True)
        leaders = [f"{x['code']}({x['change']:.1f}%)" for x in y_pool_performance if x['change'] > 5.0][:5]
        laggards = [f"{x['code']}({x['change']:.1f}%)" for x in y_pool_performance if x['change'] < -5.0][:5] # Sort reverse? No, need bottom.

        if leaders:
            self._log_event("comfort_leaders", f"馃搱 鏄ㄦ澘-浠婃棩棰嗘定: {', '.join(leaders)}", min_interval_sec=300)
        
        # Get bottom ones
        laggards_list = sorted(y_pool_performance, key=lambda x: x['change'])[:5]
        laggards = [f"{x['code']}({x['change']:.1f}%)" for x in laggards_list if x['change'] < -5.0]
        if laggards:
            self._log_event("comfort_laggards", f"馃搲 鏄ㄦ澘-浠婃棩棰嗚穼: {', '.join(laggards)}", min_interval_sec=300)

        if not scores:
            return

        comfort_exit_score = round(float(np.average(scores, weights=weights)), 2)
        total_stocks = len(codes)

        result = {
            "ts": int(time.time() * 1000),
            "score": comfort_exit_score,
            "support_rate": round(support_count / max(1, total_stocks), 4),
            "dump_rate": round(dump_count / max(1, total_stocks), 4),
            "high_pos_dump_rate": round(high_pos_dump_count / max(1, high_pos_total), 4),
            "sample_size": total_stocks,
        }

        out_key = f"market:comfort_exit:{today_str}"
        await self.redis.hset(out_key, mapping=result)
        await self.redis.expire(out_key, 86400)

    async def calculate_sentiment(self, today_str: str) -> None:
        """鎯呯华鐘舵€佹満锛堢畝鍖栫増锛夛細phase/score銆?""
        ts = int(time.time() * 1000)

        comfort = await self.redis.hgetall(f"market:comfort_exit:{today_str}")
        comfort_score = float(comfort.get('score', 50) or 50)

        top_plates = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 9, withscores=True)
        total_score = sum(score for _, score in top_plates)
        consensus_score = 0.0
        if total_score > 0 and len(top_plates) >= 3:
            top3_score = sum(score for _, score in top_plates[:3])
            consensus_score = (top3_score / total_score) * 100.0

        try:
            first_limit_count = int(await self.redis.zcard("stock:first_limit_up"))
        except Exception:
            first_limit_count = 0

        limitup_lb_key = f"cache:wencai:limitup_lb:{today_str}"
        limitup_lb_data = self.redis_storage.get_data(limitup_lb_key)
        total_limitup_count = len(limitup_lb_data) if limitup_lb_data else 0

        sentiment_score = (
            comfort_score * 0.4
            + min(consensus_score, 100.0) * 0.2
            + min(total_limitup_count * 2.0, 100.0) * 0.2
            + min(first_limit_count * 4.0, 100.0) * 0.2
        )
        sentiment_score = round(float(sentiment_score), 2)

        if sentiment_score > 75:
            phase = 'consistent' if comfort_score > 60 else 'divergent'
        elif sentiment_score > 55:
            phase = 'start'
        elif sentiment_score > 40:
            phase = 'repair'
        else:
            phase = 'retreat'

        out_key = f"market:sentiment:{today_str}"
        await self.redis.hset(out_key, mapping={
            "ts": ts,
            "phase": phase,
            "score": sentiment_score,
            "comfort_exit_score": comfort_score,
            "consensus_score": round(consensus_score, 2),
            "total_limitup_count": total_limitup_count,
            "first_limit_count": first_limit_count,
        })
        await self.redis.expire(out_key, 86400)

    async def calculate_fear_greed(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        """缁煎悎璐┆鎭愭儳鎸囨暟锛堥獙璇佺増锛屽熀浜庣幇鏈夋暟鎹簮锛夈€?""
        if not candidate_pool:
            return

        if indicators is None:
            indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))

        changes = [self._safe_float((ind or {}).get("change_pct", 0.0), 0.0) for ind in indicators.values()]
        up = sum(1 for c in changes if c > 0.0)
        down = sum(1 for c in changes if c < 0.0)
        total = max(1, up + down)
        up_down_ratio = up / max(1, down)
        up_down_ratio_norm = self._clamp01(max(0.3, min(3.0, up_down_ratio)) / 3.0)

        first_limit_count = self._safe_int(await self.redis.zcard("stock:first_limit_up"), 0)
        lb_data = self.redis_storage.get_data(f"cache:wencai:limitup_lb:{today_str}") or []
        total_limitup_count = len(lb_data)
        seal_rate = self._clamp01(first_limit_count / max(1, total_limitup_count))

        max_lb = 0
        for item in lb_data:
            if not isinstance(item, dict):
                continue
            lb = item.get("杩炴澘澶╂暟", item.get("lb_days", 0))
            max_lb = max(max_lb, self._safe_int(lb, 0))

        rolling_key = f"market:lb_rolling_max:{today_str}"
        rolling_max = self._safe_int(await self.redis.get(rolling_key), 0)
        rolling_max = max(rolling_max, max_lb, 1)
        await self.redis.set(rolling_key, rolling_max, ex=86400)
        lb_height_norm = self._clamp01(max_lb / rolling_max)

        profit_breadth = self._clamp01(sum(1 for c in changes if c >= 2.0) / max(1, len(changes)))

        score = round((up_down_ratio_norm + seal_rate + lb_height_norm + profit_breadth) / 4.0, 4)
        state = "neutral"
        if score >= 0.65:
            state = "greed"
        elif score <= 0.35:
            state = "fear"

        extreme_greed = score > 0.85 and total_limitup_count > 100 and max_lb >= 7
        extreme_fear = score < 0.15 and (down / total) > 0.7 and profit_breadth < 0.2

        out_key = f"market:fear_greed:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": int(time.time() * 1000),
                "score": score,
                "state": state,
                "up_down_ratio_norm": round(up_down_ratio_norm, 4),
                "seal_rate": round(seal_rate, 4),
                "lb_height_norm": round(lb_height_norm, 4),
                "profit_breadth": round(profit_breadth, 4),
                "up_count": up,
                "down_count": down,
                "total_limitup_count": total_limitup_count,
                "first_limit_count": first_limit_count,
                "max_lb": max_lb,
                "extreme_greed": int(extreme_greed),
                "extreme_fear": int(extreme_fear),
            },
        )
        await self.redis.expire(out_key, 86400)

    async def calculate_herding(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        """缇婄兢鏁堝簲锛堥獙璇佺増锛夛細闆嗕腑搴︺€佺浉鍏虫€с€佽祫閲戜竴鑷存€с€佽疆鍔ㄩ€熷害銆?""
        if not candidate_pool:
            return

        if indicators is None:
            indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))

        self._update_return_history(indicators)

        amounts = [self._safe_float((ind or {}).get("amount", 0.0), 0.0) for ind in indicators.values()]
        total_amount = sum(a for a in amounts if a > 0.0)
        top_n = max(1, int(len(amounts) * 0.1))
        top10_amount = sum(sorted(amounts, reverse=True)[:top_n]) if amounts else 0.0
        concentration = self._clamp01(top10_amount / max(1.0, total_amount))

        # 鍩轰簬鏈€杩?0涓埅闈㈢殑 change_pct 搴忓垪浼扮畻骞冲潎鐩稿叧鎬?        series = [vals for vals in self.return_history.values() if len(vals) >= 5]
        avg_corr = 0.0
        if len(series) >= 5:
            mat = np.array([s[-10:] for s in series[:80]], dtype=float)
            if mat.shape[0] >= 5 and mat.shape[1] >= 5:
                corr = np.corrcoef(mat)
                upper = corr[np.triu_indices_from(corr, k=1)]
                valid = upper[np.isfinite(upper)]
                if valid.size > 0:
                    avg_corr = float(np.mean(valid))
        corr_norm = self._clamp01((avg_corr + 1.0) / 2.0)

        large_nets = [self._safe_float((ind or {}).get("large_net", 0.0), 0.0) for ind in indicators.values()]
        pos = sum(1 for v in large_nets if v > 0)
        neg = sum(1 for v in large_nets if v < 0)
        flow_consistency = self._clamp01(max(pos, neg) / max(1, pos + neg))

        top_plate = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 0)
        now_ms = int(time.time() * 1000)
        if top_plate:
            self.leading_plate_history.append((now_ms, top_plate[0]))
        # 淇濈暀杩?灏忔椂绐楀彛
        cutoff = now_ms - 2 * 60 * 60 * 1000
        self.leading_plate_history = [(t, p) for t, p in self.leading_plate_history if t >= cutoff]
        distinct_leaders = len(set(p for _, p in self.leading_plate_history))
        samples = max(1, len(self.leading_plate_history))
        rotation_speed = distinct_leaders / samples
        rotation_stability = self._clamp01(1.0 - min(1.0, rotation_speed * 2.0))

        score = round(
            0.35 * concentration + 0.25 * corr_norm + 0.25 * flow_consistency + 0.15 * rotation_stability,
            4,
        )
        state = "neutral"
        if score >= 0.7:
            state = "crowded"
        elif score <= 0.35:
            state = "dispersed"

        out_key = f"market:herding:{today_str}"
        await self.redis.hset(
            out_key,
            mapping={
                "ts": now_ms,
                "score": score,
                "state": state,
                "concentration": round(concentration, 4),
                "avg_corr_norm": round(corr_norm, 4),
                "flow_consistency": round(flow_consistency, 4),
                "rotation_stability": round(rotation_stability, 4),
                "rotation_speed": round(rotation_speed, 4),
                "distinct_leaders": distinct_leaders,
            },
        )
        await self.redis.expire(out_key, 86400)

    async def calculate_resonance(
        self,
        today_str: str,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        """鍏辨尟妯″瀷锛氱偣(涓偂)-绾?棰樻潗)-闈?鏉垮潡)-鐩?甯傚満)"""
        if not candidate_pool:
            return

        if indicators is None:
            indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))

        # 鐐癸細鍊欓€夋睜鍐呭己鍔胯偂鍗犳瘮
        strong_point_count = 0
        for _, ind in indicators.items():
            chg = self._safe_float((ind or {}).get("change_pct", 0.0), 0.0)
            amt2 = self._safe_float((ind or {}).get("amount_2min", 0.0), 0.0)
            if chg >= 2.0 and amt2 >= 10_000_000:
                strong_point_count += 1
        point_score = self._clamp01(strong_point_count / max(1, len(candidate_pool)))

        # 绾匡細棰樻潗鍓?闆嗕腑搴︼紙棰樻潗寰楀垎瓒婇泦涓湪鍓?锛屼紶瀵艰秺鏄庣‘锛?        top_themes = await self.redis.zrevrange(f"rank:theme:{today_str}", 0, 9, withscores=True)
        line_score = 0.0
        if top_themes:
            total = sum(float(s) for _, s in top_themes)
            top3 = sum(float(s) for _, s in top_themes[:3])
            if total > 0:
                line_score = self._clamp01(top3 / total)

        # 闈細鏉垮潡鎵╂暎锛堝墠3鏉垮潡寰楀垎鍗犳瘮锛?        top_plates = await self.redis.zrevrange(f"rank:plate_spread:{today_str}", 0, 9, withscores=True)
        plane_score = 0.0
        if top_plates:
            total_p = sum(float(s) for _, s in top_plates)
            top3_p = sum(float(s) for _, s in top_plates[:3])
            if total_p > 0:
                plane_score = self._clamp01(top3_p / total_p)

        # 鐩橈細甯傚満鎯呯华锛堢敤 fear_greed + sentiment 缁勫悎锛?        fg = await self.redis.hgetall(f"market:fear_greed:{today_str}")
        st = await self.redis.hgetall(f"market:sentiment:{today_str}")
        fg_score = self._safe_float(fg.get("score", 0.5), 0.5)
        st_score = self._safe_float(st.get("score", 50.0), 50.0) / 100.0
        market_score = self._clamp01(0.6 * fg_score + 0.4 * st_score)

        # 鍏辨尟锛氶摼璺渶寮辩幆鑺傚喅瀹氫笂闄?+ 缁煎悎鍔犳潈
        weakest = min(point_score, line_score, plane_score, market_score)
        score = self._clamp01(
            0.20 * point_score + 0.25 * line_score + 0.25 * plane_score + 0.30 * market_score
        )
        resonance_score = round(self._clamp01(0.7 * score + 0.3 * weakest), 4)

        state = "neutral"
        if resonance_score >= 0.72:
            state = "strong_resonance"
        elif resonance_score <= 0.35:
            state = "weak_resonance"

        out_key = f"market:resonance:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "score": resonance_score,
            "state": state,
            "point_score": round(point_score, 4),
            "line_score": round(line_score, 4),
            "plane_score": round(plane_score, 4),
            "market_score": round(market_score, 4),
            "weakest_link": round(weakest, 4),
            "strong_point_count": strong_point_count,
            "candidate_size": len(candidate_pool),
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)
        self._log_event(
            "resonance",
            f"馃摗 鍏辨尟璇勫垎: {resonance_score:.2f} ({state}) | 鐐箋point_score:.2f} 绾縶line_score:.2f} 闈plane_score:.2f} 鐩榹market_score:.2f}",
            min_interval_sec=180,
        )

    async def analyze_expectations(self, core: List[Dict], quotes: List[Dict], prev_limit_up_set: Set[str]) -> Dict[str, Any]:
        """缁熶竴鍒嗘瀽甯傚満缁撴瀯涓庨鏈熷樊 (Market Structure & Expectations)"""
        signals: List[ExpectationSignal] = []
        
        # 甯傚満缁撴瀯缁熻
        struct_stats = {
            "yesterday_strong_promotion": 0, # 鏄ㄦ棩寮?-> 浠婃棩寮?(鏅嬬骇/涓€鑷?
            "yesterday_strong_demotion": 0,  # 鏄ㄦ棩寮?-> 浠婃棩寮?(娣樻卑/鍒嗘)
            "core_repair": 0,                # 鏍稿績淇
            "plate_strength": {},            # 鏉垮潡寮哄害 {plate_name: score}
            "top_bid_ratio": []              # 灏佹垚姣斿墠鎺?(TODO)
        }
        
        plate_scores = {} # pid -> [changes]

        for i, it in enumerate(core):
            code = it.get('symbol')
            if not code: continue
            
            # --- 鍩虹鏁版嵁 ---
            try: auction_change = float(it.get('change_pct', 0) or 0)
            except: auction_change = 0.0
            
            q = quotes[i] or {}
            try: open_change = float(q.get('change_pct') or q.get('change') or 0)
            except: open_change = 0.0
            
            try: amount = float(it.get('amount', 0) or 0)
            except: amount = 0.0

            # --- 鎵╁睍鏁版嵁 (High/Low) ---
            high_pct = open_change 
            low_pct = open_change
            try:
                current_price = float(q.get('price') or 0)
                high_price = float(q.get('high') or current_price)
                low_price = float(q.get('low') or current_price)
                pre_close = float(q.get('last_close') or q.get('pre_close') or 0)
                if pre_close > 0:
                    high_pct = (high_price - pre_close) / pre_close * 100
                    low_pct = (low_price - pre_close) / pre_close * 100
            except: pass
            
            # --- 韬唤璇嗗埆 (Identity) ---
            is_yesterday_strong = code in prev_limit_up_set
            
            # --- 1. 鏃у懆鏈熷弽棣?(Feedback) ---
            if is_yesterday_strong:
                if auction_change > 2.0:
                    struct_stats["yesterday_strong_promotion"] += 1
                elif auction_change < -2.0:
                    struct_stats["yesterday_strong_demotion"] += 1
                    # 寮鸿浆寮变俊鍙?                    signals.append(ExpectationSignal(
                        code=code, type='strong_to_weak', change=open_change, score=2.0,
                        details={'auction': auction_change}, reason=f"鏄ㄦ棩娑ㄥ仠->绔炰环鏍告寜閽?{auction_change:.1f}%)"
                    ))
            
            # --- 2. 鏂板懆鏈?鏍稿績鍗氬紙 ---
            # 寮辫浆寮?(Weak to Strong): 绔炰环寮轰簬棰勬湡 (e.g. 鐖嗛噺楂樺紑)
            if not is_yesterday_strong and auction_change > 3.0 and amount > 50000000: # 5000涓?                 signals.append(ExpectationSignal(
                    code=code, type='weak_to_strong', change=open_change, score=2.5,
                    details={'amount': amount, 'change': auction_change}, reason=f"鎶㈢楂樺紑({auction_change:.1f}%, {amount/10000000:.1f}浜?"
                ))

            # --- 3. 鏉垮潡寮哄害鑱氬悎 (Plate Aggregation) & Divergence ---
            plate_change = 0.0
            plate_name = "鏈煡"
            if hasattr(self.plate_updater, 'stock_to_plates'):
                stock_plates = self.plate_updater.stock_to_plates.get(code, [])
                for pid in stock_plates:
                    is_main = False
                    if hasattr(self.plate_updater, 'main_plates') and pid in self.plate_updater.main_plates: is_main = True
                    elif hasattr(self.plate_updater, 'main_plates_map') and pid in self.plate_updater.main_plates_map: is_main = True
                    
                    if is_main:
                        # Collect scores
                        if pid not in plate_scores: plate_scores[pid] = []
                        plate_scores[pid].append(auction_change)
                        # Get plate metrics for divergence check
                        p_data = self.plate_updater.get_plate_metrics(pid)
                        if p_data:
                            try:
                                plate_change = float(p_data.get('change_pct', 0) or 0)
                                plate_name = p_data.get('name', pid)
                            except: pass

            # --- 4. 鐩樹腑鍗氬紙涓庨鏈熷樊 (Intraday Process & Gap) ---
            
            # A) 杩囩▼鍗氬紙 (V-Shape / A-Shape)
            if low_pct < auction_change - 3.0 and open_change > low_pct + 4.0:
                 signals.append(ExpectationSignal(
                    code=code, type='process_v_shape', change=open_change, score=2.5,
                    details={'auction': auction_change, 'low': low_pct, 'open': open_change},
                    reason=f"娣盫鍙嶆牳(鏈€浣巤low_pct:.1f}% -> 鐜颁环{open_change:.1f}%)"
                ))
            if high_pct > auction_change + 3.0 and open_change < high_pct - 5.0:
                 signals.append(ExpectationSignal(
                    code=code, type='process_a_shape', change=open_change, score=2.5,
                    details={'auction': auction_change, 'high': high_pct, 'open': open_change},
                    reason=f"鍐查珮鍥炶惤(鏈€楂榹high_pct:.1f}% -> 鐜颁环{open_change:.1f}%)"
                ))
            
            # B) 浠锋牸棰勬湡宸?(Price Gap)
            if auction_change > 5.0 and open_change < auction_change - 4.0:
                 signals.append(ExpectationSignal(
                    code=code, type='price_gap_fade', change=open_change, score=2.0,
                    details={'auction': auction_change, 'open': open_change},
                    reason=f"绔炰环寮?{auction_change:.1f}%) ->鐜颁环寮?{open_change:.1f}%)"
                ))
            elif open_change > auction_change + 3.0 and open_change > 0:
                 signals.append(ExpectationSignal(
                    code=code, type='price_gap_rise', change=open_change, score=3.0,
                    details={'auction': auction_change, 'open': open_change},
                    reason=f"绔炰环寮?{auction_change:.1f}%) ->鐜颁环寮?{open_change:.1f}%)"
                ))
                
            # C) 鏉垮潡鑳岀 (Plate Divergence)
            if plate_change > 2.0 and open_change < -1.0:
                signals.append(ExpectationSignal(
                    code=code, type='plate_divergence_lag', change=open_change, score=1.5,
                    details={'plate': plate_change, 'stock': open_change, 'plate_name': plate_name},
                    reason=f"鏉垮潡寮?{plate_name} {plate_change:.1f}%) 涓偂寮?{open_change:.1f}%)"
                ))
            elif plate_change < -1.0 and open_change > 2.0:
                signals.append(ExpectationSignal(
                    code=code, type='plate_divergence_lead', change=open_change, score=2.5,
                    details={'plate': plate_change, 'stock': open_change, 'plate_name': plate_name},
                    reason=f"鏉垮潡寮?{plate_name} {plate_change:.1f}%) 涓偂寮?{open_change:.1f}%)"
                ))

            # D) 閲忚兘棰勬湡宸?(Volume Gap)
            if -1.0 < auction_change < 1.0 and amount > 30000000:
                 signals.append(ExpectationSignal(
                    code=code, type='volume_gap', change=open_change, score=1.8,
                    details={'amount': amount, 'change': auction_change},
                    reason=f"绔炰环骞崇洏({auction_change:.1f}%) 浣嗙垎閲?{amount/10000000:.1f}鍗冧竾)"
                ))
        
        # 璁＄畻鏉垮潡寮哄害 (Top 3 Average)
        for pid, changes in plate_scores.items():
            if len(changes) >= 3: 
                avg_strength = sum(sorted(changes, reverse=True)[:3]) / 3
                if avg_strength > 3.0:
                    p_name = self.plate_updater.get_plate_name(pid)
                    struct_stats["plate_strength"][p_name] = round(avg_strength, 2)

        return {
            "signals": signals,
            "stats": struct_stats
        }

    async def calculate_open_scenario(self, today_str: str) -> None:
        """9:30-9:40 鎵ц涓€娆★細寮€鐩橀獙璇佸叏鏂逛綅棰勬湡宸€?        Unified Logic: Market Structure + Expectations
        """
        if not self.manual_date:
            now_time = datetime.now().time()
            if not (datetime.strptime("09:30", "%H:%M").time() <= now_time <= datetime.strptime("09:40", "%H:%M").time()):
                return
        
        if self.opening_verification_done: return

        # 1. Load Auction Data
        today_yyyymmdd = today_str.replace('-', '')
        auction_key = f"market:auction:{today_yyyymmdd}:0925"
        top_amount_json = await self.redis.hget(auction_key, "top_amount")
        if not top_amount_json:
            if not self.manual_date: self.opening_verification_done = True
            return

        try: top_amount_list = json.loads(top_amount_json)
        except: 
            self.opening_verification_done = True
            return

        if not top_amount_list:
            if not self.manual_date: self.opening_verification_done = True
            return
            
        # 2. Load Yesterday Limit Up (Feedback Anchor)
        prev_limit_up_set = set()
        try:
            prev_day = self.calendar.get_previous_trade_day(today_str)
            limit_up_key = f"limit_up_{prev_day}"
            prev_limit_up_data = self.redis_storage.get_data(limit_up_key)
            if prev_limit_up_data:
                for item in prev_limit_up_data:
                    c = item.get('鑲＄エ浠ｇ爜', '') or item.get('code', '')
                    if c: prev_limit_up_set.add(c)
        except: pass

        # 3. Prepare Core List (Expand to Top 100 for better coverage?)
        # User requested optimize architecture based on "Core Stocks"
        # Let's use Top 80 to catch more plate leaders
        core = top_amount_list[:80]
        codes = [it.get('symbol') for it in core if it.get('symbol')]

        # 4. Fetch Quotes
        pipe = self.redis.pipeline()
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        quotes = await pipe.execute()

        # 5. Run Unified Analysis
        result = await self.analyze_expectations(core, quotes, prev_limit_up_set)
        signals = result["signals"]
        stats = result["stats"]
        
        # 6. Categorize & Log
        danger_list = [s.to_dict() for s in signals if s.type == 'price_gap_fade']
        surprise_list = [s.to_dict() for s in signals if s.type == 'price_gap_rise']
        weak_strong_list = [s.to_dict() for s in signals if s.type == 'weak_to_strong'] # New
        strong_weak_list = [s.to_dict() for s in signals if s.type == 'strong_to_weak'] # New

        if weak_strong_list: # 閲嶇偣鍏虫敞锛氬急杞己/鎶㈢
            logs = [f"{d['code']}({d['reason']})" for d in weak_strong_list[:3]]
            logger.info(f"馃拵 鏍稿績鎶㈢(寮辫浆寮?: {', '.join(logs)}")

        if strong_weak_list: # 閲嶇偣鍏虫敞锛氬己杞急/鏍告寜閽?            logs = [f"{d['code']}({d['reason']})" for d in strong_weak_list[:3]]
            logger.info(f"鈽狅笍 鏍稿績閫€娼?寮鸿浆寮?: {', '.join(logs)}")
            
        # Log Plate Strength
        if stats["plate_strength"]:
            # Sort by strength
            sorted_plates = sorted(stats["plate_strength"].items(), key=lambda x: x[1], reverse=True)
            p_logs = [f"{k}:{v}" for k, v in sorted_plates[:3]]
            logger.info(f"馃敟 鏈€寮烘澘鍧?绔炰环): {', '.join(p_logs)}")

        # Log Gap Signals (Existing)
        if danger_list:
            logs = [f"{d['code']}({d['reason']})" for d in danger_list[:3]]
            logger.info(f"鈿狅笍 绔炰环涓嶅強棰勬湡: {', '.join(logs)}")
        if surprise_list:
            logs = [f"{d['code']}({d['reason']})" for d in surprise_list[:3]]
            logger.info(f"锟?绔炰环瓒呴鏈? {', '.join(logs)}")

        # Update Redis
        async def update_list(key, items):
            if items:
                p = self.redis.pipeline()
                msg_map = {json.dumps(item, ensure_ascii=False): int(time.time()*1000) for item in items}
                p.zadd(key, msg_map)
                p.expire(key, 86400)
                await p.execute()

        await update_list(f"rank:danger:{today_str}", danger_list)
        await update_list(f"rank:surprise:{today_str}", surprise_list)
        await update_list(f"rank:weak_strong:{today_str}", weak_strong_list) # Frontend needs to support this
        await update_list(f"rank:strong_weak:{today_str}", strong_weak_list)

        # Market Structure Conclusion
        structure_msg = "闇囪崱/杞姩"
        if stats["yesterday_strong_promotion"] > stats["yesterday_strong_demotion"] * 2:
            structure_msg = "涓€鑷存€у姞寮?(鍋氬)"
        elif stats["yesterday_strong_demotion"] > stats["yesterday_strong_promotion"] * 1.5:
            structure_msg = "鍒嗘閫€娼?(闃插畧)"
        elif weak_strong_list and stats["plate_strength"]:
            structure_msg = "鏂版棫鍒囨崲 (鍏虫敞鏂版澘鍧?"

        scenario_data: Dict[str, Any] = {
            "ts": int(time.time() * 1000),
            "verification_status": "confirmed" if "鍋氬" in structure_msg else "rejected",
            "confidence": 0.8,
            "reason": f"{structure_msg} - 鏄ㄥ己鏅嬬骇:{stats['yesterday_strong_promotion']} vs 娣樻卑:{stats['yesterday_strong_demotion']} | 棰嗘定鏉垮潡:{list(stats['plate_strength'].keys())[:2]}"
        }

        out_key = f"market:open_scenario:{today_str}"
        await self.redis.hset(out_key, mapping=scenario_data)
        await self.redis.expire(out_key, 86400)

        self.opening_verification_done = True

    def _safe_float(self, v: Any, default: float = 0.0) -> float:
        try:
            if v is None or v == "":
                return default
            return float(v)
        except Exception:
            return default

    def _safe_int(self, v: Any, default: int = 0) -> int:
        try:
            if v is None or v == "":
                return default
            return int(v)
        except Exception:
            return default

    def _normalize_pct(self, v: float, scale: float) -> float:
        return float(v * scale)

    def _infer_change_pct_scale(
        self,
        candidate_pool: Set[str],
        indicators: Dict[str, Dict],
        auction_change: Dict[str, float],
    ) -> float:
        """Infer whether change_pct is ratio (0.2) or percent (20.0)."""
        ratios: List[float] = []
        for code in candidate_pool:
            if code not in auction_change:
                continue
            ind = indicators.get(code) or {}
            c = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            a = self._safe_float(auction_change.get(code, 0.0), 0.0)
            if abs(c) < 1e-6 or abs(a) < 1e-6:
                continue
            ratios.append(abs(a) / abs(c))
        if not ratios:
            return 1.0
        med = float(np.median(ratios))
        if 50.0 <= med <= 150.0:
            return 100.0
        return 1.0

    def _append_change_history(self, code: str, ts_ms: int, change_pct: float) -> None:
        arr = self.code_change_history.setdefault(code, [])
        arr.append((ts_ms, float(change_pct)))
        cutoff = ts_ms - 45 * 60 * 1000
        while arr and arr[0][0] < cutoff:
            arr.pop(0)

    def _window_delta(self, code: str, now_ts_ms: int, window_sec: int) -> float:
        arr = self.code_change_history.get(code) or []
        if len(arr) < 2:
            return 0.0
        start_ts = now_ts_ms - window_sec * 1000
        first_v = None
        last_v = arr[-1][1]
        for ts, v in arr:
            if ts >= start_ts:
                first_v = v
                break
        if first_v is None:
            first_v = arr[0][1]
        return float(last_v - first_v)

    def _sign(self, v: float, th: float) -> int:
        if v >= th:
            return 1
        if v <= -th:
            return -1
        return 0

    def _activation_ts_with_sign(
        self,
        code: str,
        now_ts_ms: int,
        window_sec: int,
        sign: int,
        threshold: float,
    ) -> int:
        if sign == 0:
            return 0
        arr = self.code_change_history.get(code) or []
        if len(arr) < 2:
            return 0
        start_ts = now_ts_ms - window_sec * 1000
        # locate baseline at (or nearest after) window start
        idx = 0
        while idx < len(arr) and arr[idx][0] < start_ts:
            idx += 1
        if idx >= len(arr):
            return 0
        base_v = arr[idx][1]
        for j in range(idx, len(arr)):
            ts, v = arr[j]
            delta = v - base_v
            if sign > 0 and delta >= threshold:
                return ts
            if sign < 0 and delta <= -threshold:
                return ts
        return 0

    def _clamp01(self, v: float) -> float:
        return float(max(0.0, min(1.0, v)))

    def _update_return_history(self, indicators: Dict[str, Dict]) -> None:
        if not indicators:
            return
        for code, ind in indicators.items():
            chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            arr = self.return_history.setdefault(code, [])
            arr.append(chg)
            if len(arr) > 30:
                del arr[:-30]

    def _log_event(
        self,
        key: str,
        msg: str,
        *,
        level: str = "info",
        min_interval_sec: int = 180,
        log_on_change: bool = True,
    ) -> None:
        now = time.time()
        last_msg = self.log_last_payload.get(key, "")
        last_ts = self.log_last_ts.get(key, 0.0)
        changed = (msg != last_msg)
        due = (now - last_ts) >= min_interval_sec
        if (log_on_change and changed) or due:
            if level == "warning":
                logger.warning(msg)
            elif level == "error":
                logger.error(msg)
            else:
                logger.info(msg)
            self.log_last_payload[key] = msg
            self.log_last_ts[key] = now

    def _score_rank(self, rank: int, *, max_rank: int, max_score: float) -> float:
        if rank < 0:
            rank = max_rank
        if rank >= max_rank:
            return 0.0
        # rank=0 -> max_score, rank=max_rank -> 0
        return float(max_score * (1.0 - (rank / max_rank)))

    def _dde_score(self, dde: Optional[Dict[str, Any]]) -> float:
        if not dde or not isinstance(dde, dict):
            return 0.0
        ddx = dde.get('ddx', None)
        if ddx is None:
            ddx = dde.get('DDX', None)
        ddx_v = self._safe_float(ddx, 0.0)
        # 绠€鍖栵細ddx>0 鍔犲垎锛宒dx<0 涓嶅姞鍒嗭紝灏侀《 20
        return float(max(0.0, min(20.0, ddx_v * 10.0)))

    async def calculate_stock_rank(self, today_str: str, candidate_pool: Set[str], indicators: Dict[str, Dict]) -> None:
        """
        璁＄畻涓偂鍏辨尟姒?        浼樺寲锛氭帴鍙?indicators 鍙傛暟
        """
        if not candidate_pool:
            return

        if indicators is None:
            indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(list(candidate_pool))
        quote_map = await self._fetch_quotes_batch(list(candidate_pool))

        # 1) 绔炰环 Top_amount -> rank/灏佸崟/绔炰环娑ㄥ箙
        today_yyyymmdd = today_str.replace('-', '')
        auction_key = f"market:auction:{today_yyyymmdd}:0925"
        top_amount_json = await self.redis.hget(auction_key, "top_amount")
        auction_rank: Dict[str, int] = {}
        auction_bid: Dict[str, float] = {}
        auction_change: Dict[str, float] = {}
        if top_amount_json:
            try:
                top_amount_list = json.loads(top_amount_json)
                for idx, it in enumerate(top_amount_list):
                    code = it.get('symbol')
                    if not code or len(code) != 6:
                        continue
                    auction_rank[code] = idx
                    auction_bid[code] = self._safe_float(it.get('bid_amount_yuan', 0), 0.0)
                    auction_change[code] = self._safe_float(it.get('change_pct', 0), 0.0)
            except Exception:
                pass

        pct_scale = self._infer_change_pct_scale(candidate_pool, indicators, auction_change)

        # 2) 棰樻潗璇佹嵁/棰樻潗姒?        evidence_key = f"cache:stock_theme_evidence:{today_str}"
        evidence_raw = await self.redis.hgetall(evidence_key)
        theme_rank_key = f"rank:theme:{today_str}"

        # 鍙栭鏉愭 topN 褰㈡垚 theme->rank/score 鐨勬槧灏勶紙rank 瓒婂皬瓒婂己锛?        top_themes = await self.redis.zrevrange(theme_rank_key, 0, 49, withscores=True)
        theme_score_map: Dict[str, float] = {t: float(s) for t, s in top_themes}
        theme_rank_map: Dict[str, int] = {t: i for i, (t, _) in enumerate(top_themes)}

        # 3) 鏉垮潡姒滐細杩欓噷浼樺厛鐢?plate_spread锛堟洿璐磋繎鐩樹腑鎵╂暎锛夛紝娌℃湁鐨勮瘽閫€鍖栫敤 plate
        plate_rank_key = f"rank:plate_spread:{today_str}"
        top_plates = await self.redis.zrevrange(plate_rank_key, 0, 199, withscores=True)
        if not top_plates:
            top_plates = await self.redis.zrevrange(f"rank:plate:{today_str}", 0, 199, withscores=True)
        plate_score_map: Dict[str, float] = {pid: float(s) for pid, s in top_plates}
        plate_rank_map: Dict[str, int] = {pid: i for i, (pid, _) in enumerate(top_plates)}

        # 4) DDE锛氬彲閫夛紙濡傛灉娌℃湁 stock_analyzer 灏卞叏 0锛?        dde_map: Dict[str, Dict[str, Any]] = {}
        if self.stock_analyzer is not None:
            # 鍙栨渶杩戜竴涓氦鏄撴棩浣滀负 dde date锛堢敤 today_str-1 澶╁厹搴曪級
            # 娉ㄦ剰锛氬洖鏀炬ā寮忔椂锛宼oday_str 宸茬粡鏄洖鏀炬棩鏈燂紝dde 搴旇鏄墠涓€澶╂暟鎹?            try:
                dde_date = self.calendar.get_previous_trade_day(today_str).replace('-', '')
            except:
                dde_date = (datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m%d")
                
            for code in list(candidate_pool)[:50]:
                try:
                    dde = self.stock_analyzer.get_his_stock_dde(code, dde_date) or self.stock_analyzer.get_stock_dde(code)
                    if dde:
                        dde_map[code] = dde
                except Exception:
                    continue

        # Precompute direction by plate for co-move score
        code_change: Dict[str, float] = {}
        code_amount2: Dict[str, float] = {}
        plate_to_codes: Dict[str, List[str]] = {}
        for code in candidate_pool:
            ind = indicators.get(code) or {}
            q = quote_map.get(code) or {}
            chg = self._safe_float(q.get("change_pct", q.get("change", None)), None)
            if chg is None:
                chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
                chg = self._normalize_pct(chg, pct_scale)
            amt2 = self._safe_float(ind.get("amount_2min", 0.0), 0.0)
            if amt2 <= 0:
                amt2 = self._safe_float(q.get("amount", 0.0), 0.0)
            code_change[code] = chg
            code_amount2[code] = amt2
            for pid in (self.plate_updater.stock_to_plates.get(code, []) or []):
                plate_to_codes.setdefault(pid, []).append(code)

        items: List[StockRankItem] = []
        ts = int(time.time() * 1000)

        # update intraday change history for follow/co-move detection
        for code in candidate_pool:
            self._append_change_history(code, ts, code_change.get(code, 0.0))

        for code in candidate_pool:
            amount_2min = code_amount2.get(code, 0.0)
            change_pct = code_change.get(code, 0.0)

            # 棰樻潗璇佹嵁
            ev = evidence_raw.get(code)
            primary_theme = ""
            top_w = 0.0
            conflict = False
            if ev:
                try:
                    evj = json.loads(ev)
                    primary_theme = str(evj.get('primary_theme', '') or '')
                    themes_top3 = evj.get('themes_top3', []) or []
                    if themes_top3 and isinstance(themes_top3, list):
                        top_w = self._safe_float(themes_top3[0].get('w', 0), 0.0)
                    conflict = bool(evj.get('theme_conflict', False))
                except Exception:
                    pass

            # 棰樻潗寰楀垎锛氶鏉愭鍒嗘暟 * top_w锛堥鏉愯秺纭畾璐＄尞瓒婂ぇ锛?            theme_raw_score = theme_score_map.get(primary_theme, 0.0)
            theme_score = float(theme_raw_score * max(0.2, min(1.0, top_w)))

            # 鏉垮潡褰掑睘 & 鍙栨渶寮烘澘鍧?            plate_ids = self.plate_updater.stock_to_plates.get(code, []) or []
            plate_best = ""
            plate_best_score = 0.0
            for pid in plate_ids:
                s = plate_score_map.get(pid, 0.0)
                if s > plate_best_score:
                    plate_best_score = s
                    plate_best = pid

            # Co-move score: same plate peers moving in same direction
            co_move_score = 0.0
            follow_5m_ratio = 0.0
            follow_30m_ratio = 0.0
            co_move_active_peers = 0
            resonance_role = "neutral"
            lead_follow_count = 0
            lead_follow_ratio = 0.0
            if plate_best and plate_best in plate_to_codes:
                peers = plate_to_codes.get(plate_best, [])
                if len(peers) >= 3:
                    d5 = self._window_delta(code, ts, 5 * 60)
                    d30 = self._window_delta(code, ts, 30 * 60)
                    sign5 = self._sign(d5, 0.4)
                    sign30 = self._sign(d30, 1.0)
                    act5_ts = self._activation_ts_with_sign(code, ts, 5 * 60, sign5, 0.4)
                    act30_ts = self._activation_ts_with_sign(code, ts, 30 * 60, sign30, 1.0)

                    active5 = 0
                    follow5 = 0
                    active30 = 0
                    follow30 = 0
                    for pcode in peers:
                        if pcode == code:
                            continue
                        pd5 = self._window_delta(pcode, ts, 5 * 60)
                        pd30 = self._window_delta(pcode, ts, 30 * 60)
                        ps5 = self._sign(pd5, 0.4)
                        ps30 = self._sign(pd30, 1.0)
                        pact5_ts = self._activation_ts_with_sign(pcode, ts, 5 * 60, sign5, 0.4) if sign5 != 0 else 0
                        pact30_ts = self._activation_ts_with_sign(pcode, ts, 30 * 60, sign30, 1.0) if sign30 != 0 else 0
                        if ps5 != 0:
                            active5 += 1
                            if sign5 != 0 and ps5 == sign5:
                                follow5 += 1
                                if act5_ts > 0 and pact5_ts > act5_ts:
                                    lead_follow_count += 1
                        if ps30 != 0:
                            active30 += 1
                            if sign30 != 0 and ps30 == sign30:
                                follow30 += 1
                                if act30_ts > 0 and pact30_ts > act30_ts:
                                    lead_follow_count += 1

                    if active5 > 0:
                        follow_5m_ratio = follow5 / active5
                    if active30 > 0:
                        follow_30m_ratio = follow30 / active30
                    co_move_active_peers = active5 + active30
                    breadth = min(1.0, (active5 + active30) / max(1, (len(peers) - 1) * 2))
                    co_move_score = min(
                        20.0,
                        max(0.0, follow_5m_ratio * 10.0 + follow_30m_ratio * 8.0 + breadth * 2.0),
                    )
                    if co_move_active_peers > 0:
                        lead_follow_ratio = lead_follow_count / co_move_active_peers
                    if lead_follow_count >= 3 and lead_follow_ratio >= 0.25:
                        resonance_role = "leader"
                    elif (follow_5m_ratio >= 0.5 or follow_30m_ratio >= 0.5):
                        resonance_role = "follower"

            # 鏉垮潡寰楀垎锛氱敤鎺掑悕杞垎锛堥伩鍏嶄笉鍚屾鍗?score 閲忕翰涓嶄竴鑷达級
            best_rank = plate_rank_map.get(plate_best, 9999)
            plate_score = self._score_rank(best_rank, max_rank=200, max_score=20.0)

            # 绔炰环寰楀垎锛氭帓鍚?+ 灏佸崟
            a_rank = auction_rank.get(code, 9999)
            auction_rank_score = self._score_rank(a_rank, max_rank=500, max_score=20.0)
            bid_amt = auction_bid.get(code, 0.0)
            bid_score = min(20.0, (bid_amt / 100_000_000.0) * 5.0)
            a_change = auction_change.get(code, 0.0)

            # 鍒嗘椂寮哄害锛氭定骞?+ 2鍒嗛挓閲?            mom_score = max(0.0, min(20.0, change_pct * 2.0))
            amt_score = max(0.0, min(20.0, np.log1p(amount_2min / 1_000_000.0)))

            # DDE 寰楀垎
            dde_score = self._dde_score(dde_map.get(code))

            # 缁煎悎锛氱珵浠?0.3)+棰樻潗(0.3)+鏉垮潡(0.2)+璧勯噾(0.2)锛屽苟鍔犲皯閲忓垎鏃跺寮?            score = (
                (auction_rank_score + bid_score) * 0.15
                + theme_score * 0.3
                + plate_score * 0.2
                + dde_score * 0.2
                + (mom_score + amt_score) * 0.075
                + co_move_score * 0.075
            )

            # 鍐茬獊鎯╃綒锛氶鏉愪笉娓呮櫚闄嶆潈
            if conflict:
                score *= 0.9

            items.append(StockRankItem(
                code=code,
                score=round(float(score), 4),
                primary_theme=primary_theme,
                top_theme_weight=round(float(top_w), 4),
                top_theme_conflict=conflict,
                auction_rank=int(a_rank),
                auction_bid_amount_yuan=float(bid_amt),
                auction_change_pct=float(a_change),
                amount_2min=float(amount_2min),
                change_pct=float(change_pct),
                plate_ids=list(plate_ids),
                plate_best=plate_best,
                plate_best_score=float(plate_best_score),
                theme_score=float(theme_score),
                plate_score=float(plate_score),
                dde_score=float(dde_score),
                co_move_score=float(co_move_score),
                follow_5m_ratio=float(follow_5m_ratio),
                follow_30m_ratio=float(follow_30m_ratio),
                co_move_active_peers=int(co_move_active_peers),
                resonance_role=resonance_role,
                lead_follow_count=int(lead_follow_count),
                lead_follow_ratio=float(lead_follow_ratio),
                ts=ts,
            ))

        # 鎺掑簭 & 杈撳嚭 topN
        items.sort(key=lambda x: x.score, reverse=True)
        zkey = f"rank:stock:{today_str}"
        dkey = f"rank:stock:details:{today_str}"

        pipe = self.redis.pipeline()
        pipe.delete(zkey)
        pipe.delete(dkey)

        for it in items[:200]:
            pipe.zadd(zkey, {it.code: it.score})
            pipe.hset(dkey, it.code, json.dumps(it.__dict__, ensure_ascii=False))

        pipe.expire(zkey, 86400)
        pipe.expire(dkey, 86400)
        await pipe.execute()
        
        if items:
            # S=缁煎悎鍒? C=褰撳墠娑ㄨ穼骞?%), A2=2鍒嗛挓棰?涓囧厓), F=鍚屽悜璺熼殢鍒?            top_stocks_log = [
                f"{it.code}(S:{it.score:.1f},C:{it.change_pct:.1f}%,A2:{it.amount_2min/10000:.0f}w,F:{it.co_move_score:.1f},F5:{it.follow_5m_ratio:.0%},F30:{it.follow_30m_ratio:.0%},R:{it.resonance_role},L:{it.lead_follow_count})"
                for it in items[:3]
            ]
            self._log_event("stock_top3", f"馃悅 涓偂鍏辨尟 Top3: {', '.join(top_stocks_log)}", min_interval_sec=300)

    async def calculate_market_overview(
        self,
        today_str: str,
        analysis_universe: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        if not analysis_universe:
            return

        codes = list(analysis_universe)
        quote_map = await self._fetch_quotes_batch(codes)
        valid_codes = [c for c in codes if (quote_map.get(c) or {})]
        changes: List[float] = []
        amounts: List[float] = []
        for code in valid_codes:
            q = quote_map.get(code) or {}
            ind = indicators.get(code) or {}
            chg = self._safe_float(q.get("change_pct", q.get("change", None)), None)
            if chg is None:
                chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            amt = self._safe_float(q.get("amount", None), None)
            if amt is None:
                amt = self._safe_float(ind.get("amount", 0.0), 0.0)
            changes.append(chg)
            amounts.append(amt)

        if not valid_codes:
            return

        up_count = sum(1 for v in changes if v > 0)
        down_count = sum(1 for v in changes if v < 0)
        flat_count = len(changes) - up_count - down_count
        limit_up_like = sum(1 for v in changes if v >= 9.8)
        limit_down_like = sum(1 for v in changes if v <= -9.5)
        avg_change = float(np.mean(changes)) if changes else 0.0
        median_change = float(np.median(changes)) if changes else 0.0
        total_amount = sum(a for a in amounts if a > 0)
        top10_amount = sum(sorted(amounts, reverse=True)[: max(1, int(len(amounts) * 0.1))])
        concentration = self._clamp01(top10_amount / max(1.0, total_amount))
        coverage_ratio = len(valid_codes) / max(1, len(codes))

        out_key = f"market:overview:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "universe_size": len(codes),
            "valid_quotes": len(valid_codes),
            "coverage_ratio": round(coverage_ratio, 4),
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "limit_up_like": limit_up_like,
            "limit_down_like": limit_down_like,
            "avg_change_pct": round(avg_change, 4),
            "median_change_pct": round(median_change, 4),
            "total_amount": round(total_amount, 2),
            "top10_amount_concentration": round(concentration, 4),
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)

        if coverage_ratio < 0.4:
            self._log_event(
                "coverage_warn",
                f"鈿狅笍 琛屾儏瑕嗙洊鐜囧亸浣? {len(valid_codes)}/{len(codes)} ({coverage_ratio:.0%})",
                level="warning",
                min_interval_sec=300,
                log_on_change=True,
            )
        else:
            self._log_event(
                "market_overview",
                f"馃尅锔?澶х洏姒傝: 瑕嗙洊{coverage_ratio:.0%} 娑?璺?骞?{up_count}/{down_count}/{flat_count} 鍧囨定{avg_change:.2f}%",
                min_interval_sec=300,
            )

    async def update_stock_day_profiles(
        self,
        today_str: str,
        analysis_universe: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        if not analysis_universe:
            return
        auction_profile = await self._get_auction_profile(today_str)
        quote_map = await self._fetch_quotes_batch(list(analysis_universe))
        now_ts = int(time.time() * 1000)
        hkey = f"profile:stock:day:{today_str}"
        trans_key = f"rank:profile_transition:{today_str}"

        p = self.redis.pipeline()
        transition_count: Dict[str, int] = {}
        for code in analysis_universe:
            q = quote_map.get(code) or {}
            ind = indicators.get(code) or {}
            curr = self._safe_float(q.get("change_pct", q.get("change", 0.0)), 0.0)
            pre_close = self._safe_float(q.get("pre_close", q.get("last_close", 0.0)), 0.0)
            high_pct = self._safe_pct_from_quote(q, "high_pct", pre_close, curr)
            low_pct = self._safe_pct_from_quote(q, "low_pct", pre_close, curr)
            auction = self._safe_float((auction_profile.get(code) or {}).get("change_pct", 0.0), 0.0)
            amount = self._safe_float(ind.get("amount", q.get("amount", 0.0)), 0.0)
            amount_2min = self._safe_float(ind.get("amount_2min", 0.0), 0.0)

            prev = self.stock_state_cache.get(code) or {}
            rebound = curr - low_pct
            drawdown = high_pct - curr
            turning_point = "none"
            if low_pct <= -4.0 and rebound >= 3.0:
                turning_point = "deep_v"
            elif high_pct >= 4.0 and drawdown >= 3.5:
                turning_point = "headshot"
            elif auction <= -1.0 and curr >= 1.5:
                turning_point = "weak_to_strong"

            # transition dedup per stock/type
            if turning_point != "none":
                dedup_key = f"{today_str}:{code}:{turning_point}"
                last = self.profile_transition_seen.get(dedup_key, 0)
                if now_ts - last >= 180_000:
                    self.profile_transition_seen[dedup_key] = now_ts
                    transition = {
                        "ts": now_ts,
                        "code": code,
                        "type": turning_point,
                        "auction_change_pct": round(auction, 3),
                        "change_pct": round(curr, 3),
                        "high_pct": round(high_pct, 3),
                        "low_pct": round(low_pct, 3),
                    }
                    p.zadd(trans_key, {json.dumps(transition, ensure_ascii=False): now_ts})
                    transition_count[turning_point] = transition_count.get(turning_point, 0) + 1

            profile = {
                "ts": now_ts,
                "code": code,
                "auction_change_pct": round(auction, 3),
                "change_pct": round(curr, 3),
                "high_pct": round(high_pct, 3),
                "low_pct": round(low_pct, 3),
                "rebound_from_low": round(rebound, 3),
                "drawdown_from_high": round(drawdown, 3),
                "amount": round(amount, 2),
                "amount_2min": round(amount_2min, 2),
                "turning_point": turning_point,
                "changed": int(abs(curr - self._safe_float(prev.get("change_pct", 0.0), 0.0)) > 0.2),
            }
            self.stock_state_cache[code] = profile
            p.hset(hkey, code, json.dumps(profile, ensure_ascii=False))

        p.expire(hkey, 86400)
        p.expire(trans_key, 86400)
        await p.execute()

        if transition_count:
            summary = ",".join(f"{k}:{v}" for k, v in sorted(transition_count.items(), key=lambda x: x[1], reverse=True))
            self._log_event("profile_transition", f"馃攣 鐢诲儚杞姌: {summary}", min_interval_sec=180, log_on_change=True)

    async def calculate_plate_stock_snapshot(
        self,
        today_str: str,
        analysis_universe: Set[str],
        indicators: Dict[str, Dict],
    ) -> None:
        if not analysis_universe:
            return

        # Reuse existing plate metrics as base truth for plate change.
        plate_metric_map: Dict[str, Dict[str, Any]] = {}
        try:
            all_plate_metrics = self.plate_updater.get_all_plate_metrics_with_integrated_advanced() or []
            for m in all_plate_metrics:
                if not isinstance(m, dict):
                    continue
                pid = m.get("id") or m.get("plate_id") or m.get("code")
                if pid:
                    plate_metric_map[str(pid)] = m
        except Exception:
            plate_metric_map = {}

        quote_map = await self._fetch_quotes_batch(list(analysis_universe))
        plate_agg: Dict[str, Dict[str, Any]] = {}
        for code in analysis_universe:
            q = quote_map.get(code) or {}
            ind = indicators.get(code) or {}
            chg = self._safe_float(q.get("change_pct", q.get("change", None)), None)
            if chg is None:
                chg = self._safe_float(ind.get("change_pct", 0.0), 0.0)
            amt2 = self._safe_float(ind.get("amount_2min", None), None)
            if amt2 is None:
                amt2 = self._safe_float(q.get("amount", 0.0), 0.0)
            if abs(chg) < 1e-9 and amt2 <= 0:
                continue
            for pid, w in self._weighted_plates_for_code(code):
                rec = plate_agg.setdefault(pid, {"sum_w": 0.0, "sum_chg": 0.0, "sum_amt2": 0.0, "stocks": []})
                rec["sum_w"] += w
                rec["sum_chg"] += chg * w
                rec["sum_amt2"] += amt2 * w
                if len(rec["stocks"]) < 6:
                    rec["stocks"].append({"code": code, "change_pct": round(chg, 3), "amount_2min": round(amt2, 2)})

        zkey = f"rank:plate_snapshot:{today_str}"
        dkey = f"rank:plate_snapshot:details:{today_str}"
        p = self.redis.pipeline()
        p.delete(zkey)
        p.delete(dkey)
        for pid, rec in plate_agg.items():
            denom = max(1e-6, rec["sum_w"])
            avg_chg = rec["sum_chg"] / denom
            plate_m = plate_metric_map.get(pid, {})
            base_chg = self._safe_float(plate_m.get("change_pct", avg_chg), avg_chg)
            score = base_chg * 10.0 + np.log1p(max(0.0, rec["sum_amt2"]) / 1_000_000.0)
            detail = {
                "ts": int(time.time() * 1000),
                "id": pid,
                "name": self.plate_updater.all_plates.get(pid, {}).get("name", pid),
                "avg_change_pct": round(avg_chg, 4),
                "base_change_pct": round(base_chg, 4),
                "amount_2min": round(rec["sum_amt2"], 2),
                "score": round(float(score), 4),
                "source": "existing_plate_metrics",
                "sample_stocks": sorted(rec["stocks"], key=lambda x: x["change_pct"], reverse=True)[:6],
            }
            p.zadd(zkey, {pid: float(score)})
            p.hset(dkey, pid, json.dumps(detail, ensure_ascii=False))
        p.expire(zkey, 86400)
        p.expire(dkey, 86400)
        await p.execute()

        top = await self.redis.zrevrange(zkey, 0, 2, withscores=True)
        if top:
            view = []
            for pid, score in top:
                name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
                view.append(f"{name}({float(score):.2f})")
            self._log_event("plate_snapshot", f"馃Л 鏉垮潡蹇収Top3: {', '.join(view)}", min_interval_sec=180)

    async def calculate_plate_profiles(self, today_str: str) -> None:
        """Aggregate stock day profiles into plate-level process profiles."""
        profile_key = f"profile:stock:day:{today_str}"
        profile_map = await self.redis.hgetall(profile_key)
        if not profile_map:
            return

        # Reuse existing plate metrics as baseline for plate trend.
        plate_metric_map: Dict[str, Dict[str, Any]] = {}
        try:
            all_plate_metrics = self.plate_updater.get_all_plate_metrics_with_integrated_advanced() or []
            for m in all_plate_metrics:
                if not isinstance(m, dict):
                    continue
                pid = m.get("id") or m.get("plate_id") or m.get("code")
                if pid:
                    plate_metric_map[str(pid)] = m
        except Exception:
            plate_metric_map = {}

        plate_stats: Dict[str, Dict[str, float]] = {}
        for code, raw in profile_map.items():
            try:
                pf = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(pf, dict):
                continue
            chg = self._safe_float(pf.get("change_pct", 0.0), 0.0)
            rebound = self._safe_float(pf.get("rebound_from_low", 0.0), 0.0)
            drawdown = self._safe_float(pf.get("drawdown_from_high", 0.0), 0.0)
            amt2 = self._safe_float(pf.get("amount_2min", 0.0), 0.0)
            tp = str(pf.get("turning_point", "none") or "none")

            for pid, w in self._weighted_plates_for_code(code):
                rec = plate_stats.setdefault(
                    pid,
                    {
                        "sum_w": 0.0,
                        "sum_chg": 0.0,
                        "sum_rebound": 0.0,
                        "sum_drawdown": 0.0,
                        "sum_amt2": 0.0,
                        "deep_v_w": 0.0,
                        "headshot_w": 0.0,
                        "weak_to_strong_w": 0.0,
                        "up_w": 0.0,
                        "down_w": 0.0,
                    },
                )
                rec["sum_w"] += w
                rec["sum_chg"] += chg * w
                rec["sum_rebound"] += rebound * w
                rec["sum_drawdown"] += drawdown * w
                rec["sum_amt2"] += amt2 * w
                if chg > 0:
                    rec["up_w"] += w
                elif chg < 0:
                    rec["down_w"] += w
                if tp == "deep_v":
                    rec["deep_v_w"] += w
                elif tp == "headshot":
                    rec["headshot_w"] += w
                elif tp == "weak_to_strong":
                    rec["weak_to_strong_w"] += w

        zkey = f"rank:plate_profile:{today_str}"
        dkey = f"rank:plate_profile:details:{today_str}"
        p = self.redis.pipeline()
        p.delete(zkey)
        p.delete(dkey)

        for pid, s in plate_stats.items():
            denom = max(1e-6, s["sum_w"])
            avg_chg = s["sum_chg"] / denom
            avg_rebound = s["sum_rebound"] / denom
            avg_drawdown = s["sum_drawdown"] / denom
            deep_v_rate = s["deep_v_w"] / denom
            headshot_rate = s["headshot_w"] / denom
            weak_to_strong_rate = s["weak_to_strong_w"] / denom
            up_down_ratio = s["up_w"] / max(1e-6, s["down_w"])
            base_chg = self._safe_float((plate_metric_map.get(pid) or {}).get("change_pct", avg_chg), avg_chg)

            repair_strength = 0.6 * deep_v_rate + 0.4 * weak_to_strong_rate
            risk_strength = headshot_rate
            process_score = (
                base_chg * 8.0
                + repair_strength * 20.0
                - risk_strength * 18.0
                + np.log1p(max(0.0, s["sum_amt2"]) / 1_000_000.0)
            )

            detail = {
                "ts": int(time.time() * 1000),
                "id": pid,
                "name": self.plate_updater.all_plates.get(pid, {}).get("name", pid),
                "base_change_pct": round(base_chg, 4),
                "avg_change_pct": round(avg_chg, 4),
                "avg_rebound_from_low": round(avg_rebound, 4),
                "avg_drawdown_from_high": round(avg_drawdown, 4),
                "deep_v_rate": round(deep_v_rate, 4),
                "headshot_rate": round(headshot_rate, 4),
                "weak_to_strong_rate": round(weak_to_strong_rate, 4),
                "up_down_ratio": round(up_down_ratio, 4),
                "amount_2min": round(s["sum_amt2"], 2),
                "process_score": round(float(process_score), 4),
            }
            p.zadd(zkey, {pid: float(process_score)})
            p.hset(dkey, pid, json.dumps(detail, ensure_ascii=False))

        p.expire(zkey, 86400)
        p.expire(dkey, 86400)
        await p.execute()

        top = await self.redis.zrevrange(zkey, 0, 2, withscores=True)
        if top:
            msg = []
            for pid, score in top:
                name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
                msg.append(f"{name}({float(score):.2f})")
            self._log_event("plate_profile", f"馃З 鏉垮潡鐢诲儚Top3: {', '.join(msg)}", min_interval_sec=180)

    async def calculate_market_process_profile(self, today_str: str) -> None:
        """Aggregate plate profiles into market-level process profile."""
        plate_detail_key = f"rank:plate_profile:details:{today_str}"
        details = await self.redis.hgetall(plate_detail_key)
        if not details:
            return

        w_sum = 0.0
        sum_base = 0.0
        sum_deep_v = 0.0
        sum_headshot = 0.0
        sum_w2s = 0.0
        sum_rebound = 0.0
        sum_drawdown = 0.0
        strong_count = 0
        weak_count = 0

        for _, raw in details.items():
            try:
                d = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(d, dict):
                continue
            w = max(1.0, self._safe_float(d.get("amount_2min", 0.0), 0.0) / 10_000_000.0)
            w_sum += w
            base = self._safe_float(d.get("base_change_pct", 0.0), 0.0)
            deep_v = self._safe_float(d.get("deep_v_rate", 0.0), 0.0)
            headshot = self._safe_float(d.get("headshot_rate", 0.0), 0.0)
            w2s = self._safe_float(d.get("weak_to_strong_rate", 0.0), 0.0)
            reb = self._safe_float(d.get("avg_rebound_from_low", 0.0), 0.0)
            dd = self._safe_float(d.get("avg_drawdown_from_high", 0.0), 0.0)
            sum_base += base * w
            sum_deep_v += deep_v * w
            sum_headshot += headshot * w
            sum_w2s += w2s * w
            sum_rebound += reb * w
            sum_drawdown += dd * w
            if base >= 1.0:
                strong_count += 1
            elif base <= -1.0:
                weak_count += 1

        if w_sum <= 0:
            return

        market_base = sum_base / w_sum
        market_deep_v = sum_deep_v / w_sum
        market_headshot = sum_headshot / w_sum
        market_w2s = sum_w2s / w_sum
        market_rebound = sum_rebound / w_sum
        market_drawdown = sum_drawdown / w_sum
        repair_strength = 0.6 * market_deep_v + 0.4 * market_w2s
        risk_strength = market_headshot
        rotation_pressure = max(0.0, risk_strength - repair_strength)

        score = self._clamp01(
            0.35 * (0.5 + max(-5.0, min(5.0, market_base)) / 10.0)
            + 0.30 * max(0.0, min(1.0, repair_strength))
            + 0.20 * (1.0 - max(0.0, min(1.0, risk_strength)))
            + 0.15 * max(0.0, min(1.0, (strong_count + 1) / max(1, strong_count + weak_count + 1))),
        )

        state = "mixed"
        if score >= 0.65 and repair_strength >= risk_strength:
            state = "risk_on"
        elif score <= 0.4 or risk_strength > repair_strength * 1.3:
            state = "risk_off"

        out_key = f"market:process_profile:{today_str}"
        payload = {
            "ts": int(time.time() * 1000),
            "score": round(score, 4),
            "state": state,
            "market_base_change_pct": round(market_base, 4),
            "market_deep_v_rate": round(market_deep_v, 4),
            "market_headshot_rate": round(market_headshot, 4),
            "market_weak_to_strong_rate": round(market_w2s, 4),
            "market_avg_rebound_from_low": round(market_rebound, 4),
            "market_avg_drawdown_from_high": round(market_drawdown, 4),
            "repair_strength": round(repair_strength, 4),
            "risk_strength": round(risk_strength, 4),
            "rotation_pressure": round(rotation_pressure, 4),
            "strong_plate_count": strong_count,
            "weak_plate_count": weak_count,
        }
        await self.redis.hset(out_key, mapping=payload)
        await self.redis.expire(out_key, 86400)

        self._log_event(
            "market_process_profile",
            f"馃 澶х洏杩囩▼鐢诲儚: {state} score={score:.2f} 淇={repair_strength:.2f} 椋庨櫓={risk_strength:.2f}",
            min_interval_sec=180,
            log_on_change=True,
        )

    async def calculate_execution_policy(self, today_str: str) -> None:
        ts = int(time.time() * 1000)

        stale = False

        sentiment: Dict[str, Any] = {}
        open_scenario: Dict[str, Any] = {}
        comfort: Dict[str, Any] = {}
        fear_greed: Dict[str, Any] = {}
        herding: Dict[str, Any] = {}
        resonance: Dict[str, Any] = {}
        process_profile: Dict[str, Any] = {}

        try:
            sentiment = await self.redis.hgetall(f"market:sentiment:{today_str}")
        except Exception:
            stale = True
        try:
            open_scenario = await self.redis.hgetall(f"market:open_scenario:{today_str}")
        except Exception:
            stale = True
        try:
            comfort = await self.redis.hgetall(f"market:comfort_exit:{today_str}")
        except Exception:
            stale = True
        try:
            fear_greed = await self.redis.hgetall(f"market:fear_greed:{today_str}")
        except Exception:
            stale = True
        try:
            herding = await self.redis.hgetall(f"market:herding:{today_str}")
        except Exception:
            stale = True
        try:
            resonance = await self.redis.hgetall(f"market:resonance:{today_str}")
        except Exception:
            stale = True
        try:
            process_profile = await self.redis.hgetall(f"market:process_profile:{today_str}")
        except Exception:
            stale = True

        phase_now = self._current_phase()
        phase = sentiment.get('phase', 'unknown')
        sentiment_score = float(sentiment.get('score', 50) or 50)
        comfort_score = float(comfort.get('score', 50) or 50)
        verification_status = open_scenario.get('verification_status', 'pending')
        if not open_scenario and (self.manual_date or phase_now in ("opening", "intraday_am", "intraday_pm")):
            verification_status = "late_bootstrap"
        fear_greed_score = self._safe_float(fear_greed.get("score", 0.5), 0.5)
        herding_score = self._safe_float(herding.get("score", 0.5), 0.5)
        extreme_greed = self._safe_int(fear_greed.get("extreme_greed", 0), 0)
        extreme_fear = self._safe_int(fear_greed.get("extreme_fear", 0), 0)
        resonance_score = self._safe_float(resonance.get("score", 0.5), 0.5)
        resonance_state = resonance.get("state", "neutral")
        process_state = process_profile.get("state", "mixed")
        process_score = self._safe_float(process_profile.get("score", 0.5), 0.5)
        repair_strength = self._safe_float(process_profile.get("repair_strength", 0.0), 0.0)
        process_risk_strength = self._safe_float(process_profile.get("risk_strength", 0.0), 0.0)

        if not sentiment:
            stale = True
        if not open_scenario and verification_status != "late_bootstrap":
            stale = True
        if not comfort:
            stale = True

        ban_conditions: List[str] = []

        danger_key = f"rank:danger:{today_str}"
        try:
            danger_count = await self.redis.zcard(danger_key)
        except Exception:
            danger_count = 0
            stale = True

        # 鏂板锛氱洏涓澘鍧楁€佸害锛堟潵鑷姸鎬佹満杞姌锛?        plate_attitude_bias = 0.0
        long_plate_suggestions: List[str] = []
        avoid_plate_suggestions: List[str] = []
        try:
            top_att = await self.redis.zrevrange(f"rank:plate_attitude:{today_str}", 0, 0, withscores=True)
            bot_att = await self.redis.zrange(f"rank:plate_attitude:{today_str}", 0, 0, withscores=True)
            top_score = float(top_att[0][1]) if top_att else 0.0
            bot_score = float(bot_att[0][1]) if bot_att else 0.0
            plate_attitude_bias = top_score + bot_score
            top_pids = await self.redis.zrevrange(f"rank:plate_attitude:{today_str}", 0, 2)
            bot_pids = await self.redis.zrange(f"rank:plate_attitude:{today_str}", 0, 2)
            for pid in top_pids:
                name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
                long_plate_suggestions.append(name)
            for pid in bot_pids:
                name = self.plate_updater.all_plates.get(pid, {}).get("name", pid)
                avoid_plate_suggestions.append(name)
        except Exception:
            plate_attitude_bias = 0.0

        position_max = 0.3
        if verification_status == 'rejected':
            position_max = 0.0
            ban_conditions.append('open_scenario=rejected')
        elif comfort_score < 40:
            position_max = 0.1
            ban_conditions.append('comfort_exit_score<40')
        elif phase in ('retreat',):
            position_max = 0.1
            ban_conditions.append('phase=retreat')
        elif phase in ('divergent',):
            position_max = 0.2
        elif phase in ('repair',):
            position_max = 0.2
        elif phase in ('start',):
            position_max = 0.4
        elif phase in ('consistent',):
            position_max = 0.6

        if danger_count >= 20:
            position_max = min(position_max, 0.2)
            ban_conditions.append('danger_count>=20')

        # 鏉垮潡鎬佸害鐨勪繚瀹堜慨姝?        if plate_attitude_bias <= -2.0:
            position_max = min(position_max, 0.2)
            ban_conditions.append('plate_attitude_negative')
        elif plate_attitude_bias >= 2.0 and position_max > 0.2:
            position_max = min(0.7, position_max + 0.05)

        # 鏂板锛氭儏缁笌鎷ユ尋淇
        if extreme_greed and herding_score >= 0.7:
            position_max = min(position_max, 0.2)
            ban_conditions.append("extreme_greed_crowded")
        elif extreme_fear and plate_attitude_bias > 0:
            position_max = min(0.4, position_max + 0.05)

        # 鏂板锛氳繃绋嬬敾鍍忕洿鎺ュ奖鍝嶄粨浣嶄笂闄?        if process_state == "risk_off" or process_risk_strength >= 0.5:
            position_max = min(position_max, 0.2)
            ban_conditions.append("process_risk_off")
        elif process_state == "risk_on" and process_score >= 0.65 and repair_strength > process_risk_strength:
            position_max = min(0.7, position_max + 0.05)

        if position_max <= 0.1:
            mode_allow = ['wait']
        else:
            if phase in ('start', 'repair'):
                mode_allow = ['diffusion', 'trend']
            elif phase in ('consistent',):
                mode_allow = ['relay', 'trend']
            elif phase in ('divergent',):
                mode_allow = ['rotation', 'trend']
            else:
                mode_allow = ['wait']

        # stale 閫昏緫锛氱洏涓厑璁?open_scenario 璧?late_bootstrap锛岄伩鍏嶆湇鍔′腑閫斿惎鍔ㄥ悗闀挎湡 stale
        if sentiment and comfort and (open_scenario or verification_status == "late_bootstrap"):
            stale = False
            
        # 鍥炴斁妯″紡涓嬶紝瀵逛簬 stale 鐨勫垽瀹氬鏉句竴鐐?        if self.manual_date and sentiment and comfort:
            stale = False

        policy = ExecutionPolicy(
            ts=ts,
            position_max=float(position_max),
            mode_allow=mode_allow,
            candidate_pool_key=f"cache:candidate_pool:{today_str}",
            ban_conditions=ban_conditions,
            risk_budget={"max_daily_drawdown": 0.01, "max_attempts": 3},
            explain={
                "stale": stale,
                "phase": phase,
                "sentiment_score": sentiment_score,
                "comfort_exit": comfort_score,
                "open_scenario": verification_status,
                "danger_count": danger_count,
                "plate_attitude_bias": round(plate_attitude_bias, 4),
                "fear_greed_score": round(fear_greed_score, 4),
                "herding_score": round(herding_score, 4),
                "resonance_score": round(resonance_score, 4),
                "resonance_state": resonance_state,
                "extreme_greed": int(extreme_greed),
                "extreme_fear": int(extreme_fear),
                "process_state": process_state,
                "process_score": round(process_score, 4),
                "repair_strength": round(repair_strength, 4),
                "process_risk_strength": round(process_risk_strength, 4),
            },
        )

        out_key = f"market:execution_policy:{today_str}"
        await self.redis.hset(out_key, mapping={
            "ts": policy.ts,
            "position_max": policy.position_max,
            "mode_allow": json.dumps(policy.mode_allow, ensure_ascii=False),
            "candidate_pool_key": policy.candidate_pool_key,
            "ban_conditions": json.dumps(policy.ban_conditions, ensure_ascii=False),
            "risk_budget": json.dumps(policy.risk_budget, ensure_ascii=False),
            "explain": json.dumps(policy.explain, ensure_ascii=False),
        })
        await self.redis.expire(out_key, 86400)
        
        # Log policy summary periodically or on change (logging every time for now as it runs every 15s)
        # But to avoid spam, maybe only log critical info
        if stale:
             self._log_event("policy_status", f"馃洝锔?绛栫暐鏇存柊(STALE): Phase={phase}, Pos={policy.position_max}", min_interval_sec=180)
        else:
             self._log_event("policy_status", f"馃洝锔?绛栫暐鏇存柊: Phase={phase}, Pos={policy.position_max}, Score={sentiment_score}", min_interval_sec=120)

        # 浜ゆ槗寤鸿锛堝紑浠?鍑忎粨/娓呬粨 + 鏉垮潡椋庨櫓鎻愮ず锛?        action = "WAIT"
        risk_level = "MEDIUM"
        reason = []
        if position_max <= 0.05:
            action = "CLOSE"
            risk_level = "HIGH"
            reason.append("绛栫暐浠撲綅鎺ヨ繎0锛屼紭鍏堥槻瀹?)
        elif position_max <= 0.2:
            action = "REDUCE"
            risk_level = "HIGH" if danger_count >= 20 else "MEDIUM"
            reason.append("浠撲綅涓婇檺鍋忎綆锛屾帶鍒跺洖鎾?)
        elif phase in ("start", "consistent") and verification_status != "rejected":
            action = "OPEN"
            risk_level = "MEDIUM"
            reason.append("鎯呯华涓庡紑鐩橀獙璇佸亸姝ｏ紝鍙瘯閿欏紑浠?)
        else:
            reason.append("淇″彿鏈舰鎴愪竴鑷达紝绛夊緟纭")

        if "extreme_greed_crowded" in ban_conditions:
            reason.append("鏋佺璐┆涓旀嫢鎸わ紝闃插啿楂樺洖钀?)
        if "plate_attitude_negative" in ban_conditions:
            reason.append("鏉垮潡鎬佸害鍋忓急锛岃閬垮急鍔挎澘鍧?)
        if "process_risk_off" in ban_conditions:
            reason.append("杩囩▼鐢诲儚椋庨櫓鍋忛珮锛堢垎澶寸巼楂?淇寮憋級锛岄檷浣庝粨浣?)
        if resonance_state == "strong_resonance" and action in ("WAIT", "REDUCE") and verification_status != "rejected":
            action = "OPEN"
            risk_level = "MEDIUM"
            reason.append("鐐圭嚎闈㈢洏鍏辨尟澧炲己锛屽厑璁歌瘯閿欏紑浠?)
            position_max = max(position_max, 0.35)
        elif resonance_state == "weak_resonance" and action == "OPEN":
            action = "REDUCE"
            risk_level = "HIGH"
            reason.append("鍏辨尟鍋忓急锛岄槻杩介珮鍥炴挙")
            position_max = min(position_max, 0.2)
        if not long_plate_suggestions:
            long_plate_suggestions = ["绛夊緟涓荤嚎鏄庣‘"]
        if not avoid_plate_suggestions:
            avoid_plate_suggestions = ["鏃犳槑鏄捐礋鍚戞澘鍧?]

        # 浼樺厛浣跨敤鏉垮潡鐢诲儚杈撳嚭鐨勬澘鍧楀缓璁?        try:
            top_profile = await self.redis.zrevrange(f"rank:plate_profile:{today_str}", 0, 2)
            bot_profile = await self.redis.zrange(f"rank:plate_profile:{today_str}", 0, 2)
            if top_profile:
                long_plate_suggestions = [self.plate_updater.all_plates.get(pid, {}).get("name", pid) for pid in top_profile]
            if bot_profile:
                avoid_plate_suggestions = [self.plate_updater.all_plates.get(pid, {}).get("name", pid) for pid in bot_profile]
        except Exception:
            pass

        advice = {
            "ts": ts,
            "action": action,
            "risk_level": risk_level,
            "position_max": float(position_max),
            "reason": reason,
            "long_plates": long_plate_suggestions,
            "avoid_plates": avoid_plate_suggestions,
            "ban_conditions": ban_conditions,
        }
        advice_key = f"market:operator_advice:{today_str}"
        await self.redis.hset(advice_key, mapping={"ts": ts, "payload": json.dumps(advice, ensure_ascii=False)})
        await self.redis.expire(advice_key, 86400)
        self._log_event(
            "operator_advice",
            f"馃搶 鎿嶄綔寤鸿: {action} | 椋庨櫓:{risk_level} | 寤鸿鏉垮潡:{','.join(long_plate_suggestions[:2])} | 瑙勯伩:{','.join(avoid_plate_suggestions[:2])}",
            min_interval_sec=120,
        )

    async def run(self, interval_seconds: int = 60):
        mode = "replay" if self.manual_date else "live"
        logger.info(f"MarketEdgeEngine started (Mode: {mode}, Manual Date: {self.manual_date})")
        loop_counter = 0
        cached_indicators: Dict[str, Dict] = {}
        while True:
            try:
                loop_counter += 1
                # 1. 纭畾鏃ユ湡锛氫紭鍏堜娇鐢ㄦ墜鍔ㄦ寚瀹氭棩鏈?                today_str = self.manual_date if self.manual_date else datetime.now().strftime('%Y-%m-%d')
                phase = self._current_phase()
                now_ts = time.time()
                is_live_mode = self.manual_date is None
                can_run_intraday = phase in ("opening", "intraday_am", "intraday_pm")

                # Heartbeat log every ~60 seconds (4 * 15s)
                if loop_counter % 4 == 1:
                    self._log_event(
                        "heartbeat",
                        f"馃攧 MarketEdgeEngine 杩愯涓?.. (妯″紡: {mode}, 鏃ユ湡: {today_str}, 闃舵: {phase})",
                        min_interval_sec=240,
                        log_on_change=True,
                    )

                # 瀹炵洏妯″紡涓嬶紝鐩樺墠/鍗堜紤/鐩樺悗涓嶆墽琛岀洏涓绠楋紝閬垮厤鈥滄湭寮€鐩樺厛绠椻€?                if is_live_mode and not can_run_intraday:
                    if phase in ("pre_open", "post_close", "evening"):
                        if now_ts - self.last_static_precompute_update >= self.task_intervals["static_precompute"]:
                            target_universe = self.analysis_universe_cache or self.candidate_pool_cache or set()
                            await self.precompute_static_context(
                                today_str,
                                target_universe,
                                force=False,
                                source="off_session",
                            )
                            self.last_static_precompute_update = now_ts
                    self._log_event(
                        "phase_gate",
                        f"鈴革笍 瀹炵洏闈炰氦鏄撻樁娈碉紝璺宠繃鐩樹腑璁＄畻 (phase={phase})",
                        min_interval_sec=300,
                        log_on_change=True,
                    )
                    await asyncio.sleep(interval_seconds)
                    continue

                # 0) 寮€鐩橀獙璇侊紙9:30-9:40锛屼粎涓€娆★級
                await self.calculate_open_scenario(today_str)

                # 1) 鍊欓€夋睜锛堟瘡 60s 鍒锋柊锛?                if now_ts - self.last_candidate_pool_update >= self.task_intervals["candidate_pool"]:
                    self.candidate_pool_cache = await self.build_candidate_pool(today_str)
                    self.last_candidate_pool_update = now_ts
                    # 鍊欓€夋睜鍙樺寲鍚庨噸寤烘澘鍧楀綊灞炴潈閲嶏紝瑙ｅ喅澶氬澶氬綊灞炵殑瀹炴椂閲嶅璁＄畻
                    await self.precompute_static_context(
                        today_str,
                        self.candidate_pool_cache or set(),
                        force=False,
                        source="candidate_pool",
                    )

                # 1.5) 鍒嗘瀽瀹囧畽锛堝墠鍑犵櫨/鍗冨彧锛屾彁鍗囦俊鎭潰锛?                if now_ts - self.last_analysis_universe_update >= self.task_intervals["analysis_universe"]:
                    self.analysis_universe_cache = await self.build_analysis_universe(today_str, self.candidate_pool_cache)
                    # broaden weight cache for plate/stock mapping in analysis outputs
                    if self.analysis_universe_cache:
                        await self.precompute_static_context(
                            today_str,
                            self.analysis_universe_cache,
                            force=False,
                            source="analysis_universe",
                        )
                    self.last_analysis_universe_update = now_ts

                # 鐩樺墠/鐩樺悗锛氶潤鎬佷笂涓嬫枃棰勮绠楋紙鎱㈤锛?                if phase in ("pre_open", "auction", "post_close", "evening"):
                    if now_ts - self.last_static_precompute_update >= self.task_intervals["static_precompute"]:
                        target_universe = self.analysis_universe_cache or self.candidate_pool_cache or set()
                        await self.precompute_static_context(
                            today_str,
                            target_universe,
                            force=False,
                            source="off_session",
                        )
                        self.last_static_precompute_update = now_ts

                # 2) 鎵归噺琛屾儏锛堝叡浜粰鍚庣画璁＄畻锛?                if self.candidate_pool_cache and (
                    not cached_indicators or now_ts - self.last_intraday_state_update >= self.task_intervals["indicators"]
                ):
                    cached_indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(
                        list(self.candidate_pool_cache)
                    )
                    if cached_indicators:
                        n = len(cached_indicators)
                        zero_amt2 = 0
                        for ind in cached_indicators.values():
                            if self._safe_float((ind or {}).get("amount_2min", 0.0), 0.0) <= 0:
                                zero_amt2 += 1
                        ratio = zero_amt2 / max(1, n)
                        if ratio >= 0.7:
                            self._log_event(
                                "indicator_quality",
                                f"鈿狅笍 鍊欓€夋睜 amount_2min 澶ч噺缂哄け: {zero_amt2}/{n} ({ratio:.0%})",
                                level="warning",
                                min_interval_sec=300,
                                log_on_change=True,
                            )
                # 3) 鐩樹腑杩囩▼鐘舵€佹満锛堝揩棰戯級
                latest_transitions: List[Dict[str, Any]] = []
                if self.candidate_pool_cache and now_ts - self.last_intraday_state_update >= self.task_intervals["intraday_state"]:
                    latest_transitions = await self.update_intraday_state_machine(
                        today_str, self.candidate_pool_cache, cached_indicators
                    )
                    self.last_intraday_state_update = now_ts

                # 4) 鏉垮潡鎬佸害锛堜腑棰戯級
                if latest_transitions and now_ts - self.last_plate_attitude_update >= self.task_intervals["plate_attitude"]:
                    await self.calculate_plate_attitude(today_str, latest_transitions)
                    self.last_plate_attitude_update = now_ts

                # 5) 鎵╂暎锛堟參棰戯級
                if now_ts - self.last_plate_spread_update >= self.task_intervals["plate_spread"]:
                    await self.calculate_plate_spread(today_str, self.candidate_pool_cache, cached_indicators)
                    self.last_plate_spread_update = now_ts

                # 6) 棰樻潗姒滐紙鎱㈤锛?                if now_ts - self.last_theme_rank_update >= self.task_intervals["theme_rank"]:
                    await self.calculate_theme_rank(today_str, self.candidate_pool_cache, cached_indicators)
                    self.last_theme_rank_update = now_ts

                # 7) 鑸掓湇绂诲満锛堟參棰戯級
                if now_ts - self.last_comfort_exit_update >= self.task_intervals["comfort_exit"]:
                    await self.calculate_comfort_exit(today_str)
                    self.last_comfort_exit_update = now_ts

                # 8) 鎯呯华锛堜腑棰戯級
                if now_ts - self.last_sentiment_update >= self.task_intervals["sentiment"]:
                    await self.calculate_sentiment(today_str)
                    self.last_sentiment_update = now_ts

                # 8.5) 璐┆鎭愭儳锛堟參棰戯級
                if now_ts - self.last_fear_greed_update >= self.task_intervals["fear_greed"]:
                    await self.calculate_fear_greed(today_str, self.candidate_pool_cache, cached_indicators)
                    self.last_fear_greed_update = now_ts

                # 8.6) 缇婄兢鏁堝簲锛堟參棰戯級
                if now_ts - self.last_herding_update >= self.task_intervals["herding"]:
                    await self.calculate_herding(today_str, self.candidate_pool_cache, cached_indicators)
                    self.last_herding_update = now_ts

                # 8.7) 鐐圭嚎闈㈢洏鍏辨尟锛堟參棰戯級
                if now_ts - self.last_resonance_update >= self.task_intervals["resonance"]:
                    await self.calculate_resonance(today_str, self.candidate_pool_cache, cached_indicators)
                    self.last_resonance_update = now_ts

                # 8.8) 淇℃伅闈㈠寮猴細澶х洏姒傝
                if self.analysis_universe_cache and now_ts - self.last_market_overview_update >= self.task_intervals["market_overview"]:
                    await self.calculate_market_overview(today_str, self.analysis_universe_cache, {})
                    self.last_market_overview_update = now_ts

                # 8.9) 鍗冭偂鐢诲儚鐘舵€佹満钀界洏
                if self.analysis_universe_cache and now_ts - self.last_stock_profile_update >= self.task_intervals["stock_profile"]:
                    await self.update_stock_day_profiles(today_str, self.analysis_universe_cache, {})
                    self.last_stock_profile_update = now_ts

                # 8.10) 鏉垮潡-涓偂鑱斿姩蹇収
                if self.analysis_universe_cache and now_ts - self.last_plate_snapshot_update >= self.task_intervals["plate_snapshot"]:
                    await self.calculate_plate_stock_snapshot(today_str, self.analysis_universe_cache, {})
                    self.last_plate_snapshot_update = now_ts

                # 8.11) 鏉垮潡鐢诲儚
                if now_ts - self.last_plate_profile_update >= self.task_intervals["plate_profile"]:
                    await self.calculate_plate_profiles(today_str)
                    self.last_plate_profile_update = now_ts

                # 8.12) 澶х洏杩囩▼鐢诲儚
                if now_ts - self.last_market_process_profile_update >= self.task_intervals["market_process_profile"]:
                    await self.calculate_market_process_profile(today_str)
                    self.last_market_process_profile_update = now_ts

                # 9) 涓偂鍏辨尟姒滐紙鎱㈤锛?                if now_ts - self.last_stock_rank_update >= self.task_intervals["stock_rank"]:
                    await self.calculate_stock_rank(today_str, self.candidate_pool_cache, cached_indicators)
                    self.last_stock_rank_update = now_ts

                # 10) 浠撲綅绛栫暐锛堜腑棰戯紝鍏佽闄嶇骇锛?                if now_ts - self.last_execution_policy_update >= self.task_intervals["execution_policy"]:
                    await self.calculate_execution_policy(today_str)
                    self.last_execution_policy_update = now_ts
                
            except Exception as e:
                logger.error(f"鉂?MarketEdgeEngine 杩愯寮傚父: {e}", exc_info=True)
                await asyncio.sleep(5)  # 閿欒鏃舵殏鍋?绉?
            await asyncio.sleep(interval_seconds)

