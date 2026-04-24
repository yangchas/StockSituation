import asyncio
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import logging
import sys
import os

logger = logging.getLogger(__name__)

# Explicitly initialize to None at the module level to prevent NameError
StockAnalyzer = None
HotStockAPI = None

def reinitialize_specialized_apis():
    """Public method to force re-discovery of APIs if paths were changed or failed initially."""
    global StockAnalyzer, HotStockAPI
    
    # Ensure current dir and potential roots are in path
    curr_api_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(curr_api_dir))
    
    for p in [curr_api_dir, project_root]:
        if p not in sys.path:
            sys.path.insert(0, p)

    # 1. Try absolute imports from project root (preferred)
    try:
        # Use importlib for more control if needed, but absolute import is usually fine
        from ai.API.StockAnalyzer import StockAnalyzer as SA_Abs
        from ai.API.HotStockAPI import HotStockAPI as HS_Abs
        StockAnalyzer = SA_Abs
        HotStockAPI = HS_Abs
        logger.debug("[API_INIT] Successfully resolved APIs via absolute imports.")
        return True
    except Exception as e:
        import traceback
        logger.debug(f"[API_INIT] Absolute import failed: {e}")
        pass

    # 2. Try direct imports (if sys.path is already injected)
    try:
        import StockAnalyzer as _SA
        import HotStockAPI as _HS
        # Handle cases where the module contains a class of the same name
        StockAnalyzer = getattr(_SA, 'StockAnalyzer', _SA)
        HotStockAPI = getattr(_HS, 'HotStockAPI', _HS)
        logger.debug("[API_INIT] Resolved APIs via direct imports.")
        return True
    except Exception as e:
        logger.debug(f"[API_INIT] Direct import failed: {e}")
        pass

    # 3. Last resort: relative imports
    try:
        from .StockAnalyzer import StockAnalyzer as SA_Rel
        from .HotStockAPI import HotStockAPI as HS_Rel
        StockAnalyzer = SA_Rel
        HotStockAPI = HS_Rel
        logger.debug("[API_INIT] Resolved APIs via relative imports.")
        return True
    except Exception as e:
        logger.debug(f"[API_INIT] Relative import failed: {e}")
        pass

    return False

# Trigger initialization
reinitialize_specialized_apis()

if StockAnalyzer is None or HotStockAPI is None:
    logger.warning(f"[API_INIT] Partial initialization: StockAnalyzer={StockAnalyzer is not None}, HotStockAPI={HotStockAPI is not None}")


class UnifiedMarketDataFetcher:
    """
    统一市场数据获取接口
    整合：问财（历史/逻辑筛选）、开盘啦（实时状态/板块）、热榜（市场关注度）
    """

    def __init__(
        self,
        wencai_cookie: str = "other_uid=Ths_iwencai_Xuangu_dmb7b3wvaecjds9oa03n98mdyap3qbqo; ta_random_userid=gy637c6f4b; cid=36d9db0444f45066b13c3c4bb223e9de1766389114; v=A_MP_t57nsQTBFKQsduNvmhAgvwYKIPlQb3LHqWRT2BIRx3iLfgXOlGMW3u2",  # 问财接口cookie
        hot_api_source: str = "ths",  # 热榜数据源，默认同花顺
    ):
        """初始化统一数据获取器

        Args:
            wencai_cookie: 问财接口所需的cookie
            hot_api_source: 热榜数据源，'ths'或'eastmoney'
        """
        # 1. 初始化问财接口（同步）
        self.wencai_cookie = wencai_cookie

        # 2. 初始化开盘啦分析器
        if StockAnalyzer:
            try:
                self.kaipan_analyzer = StockAnalyzer()
            except Exception as e:
                logger.warning(f"[API] Failed to instance StockAnalyzer: {e}")
                self.kaipan_analyzer = None
        else:
            self.kaipan_analyzer = None
            logger.warning("[API] StockAnalyzer module is not available in current environment.")

        # 3. 初始化热榜API（异步）
        if HotStockAPI:
            try:
                self.hot_api = HotStockAPI(hot_api_source)
            except Exception as e:
                logger.warning(f"[API] Failed to instance HotStockAPI: {e}")
                self.hot_api = None
        else:
            self.hot_api = None
            logger.warning("[API] HotStockAPI module is not available in current environment.")

    # ==================== 核心方法：股票池获取 ====================

    async def get_core_stock_pool(self, history_lookback: int = 1, hot_top_n: int = 100) -> List[str]:
        """获取核心股票池：昨日异动股 + 今日热榜股"""
        core_pool = set()

        # 1. 从问财获取历史异动股票（昨日涨停、昨日曾涨停）
        yesterday = (datetime.now() - timedelta(days=history_lookback)).strftime("%Y%m%d")

        # 获取昨日涨停股
        zt_stocks = await self._get_wencai_stocks(f"{yesterday}涨停", loop=True)
        # 获取昨日曾涨停（断板）股
        ztc_stocks = await self._get_wencai_stocks(f"{yesterday}曾涨停", loop=True)

        # 合并历史异动股
        history_stocks = set(zt_stocks + ztc_stocks)
        core_pool.update(history_stocks)
        logger.info(f"[数据融合] 从问财获取昨日异动股 {len(history_stocks)} 只")

        # 2. 从热榜获取今日焦点股票
        hot_stocks = await self.hot_api.get_trending_stocks(top_n=hot_top_n)
        hot_codes = [
            self._format_stock_code(stock.get("code") or stock.get("stock_code"))
            for stock in hot_stocks
            if stock
        ]
        core_pool.update(hot_codes)
        logger.info(f"[数据融合] 从热榜获取今日焦点股 {len(hot_codes)} 只")

        # 3. 从开盘啦获取今日涨停/炸板股（实时状态）
        today_bans = self.kaipan_analyzer.get_bans()  # 今日涨停列表
        if today_bans and "data" in today_bans:
            ban_codes = [
                self._format_stock_code(item.get("code"))
                for item in today_bans["data"]
                if item.get("code")
            ]
            core_pool.update(ban_codes)
            logger.info(f"[数据融合] 从开盘啦获取今日涨停/炸板股 {len(ban_codes)} 只")

        return list(core_pool)

    # ==================== 核心方法：单股完整数据获取 ====================

    async def get_stock_status(self, stock_code: str, include_history: bool = False) -> Dict[str, Any]:
        """获取单只股票的完整状态数据（三层数据融合）"""
        formatted_code = self._format_stock_code(stock_code)
        base_code = formatted_code[:6]

        logger.debug(f"[数据获取] 开始整合股票 {formatted_code} 的数据...")

        stock_data: Dict[str, Any] = {
            "code": formatted_code,
            "base_code": base_code,
            "timestamp": datetime.now().isoformat(),
            "data_sources": {},
        }

        # === 第一层：实时状态数据（开盘啦 - 最高优先级）===
        try:
            ban_info = self.kaipan_analyzer.get_stock_gene(base_code)
            if ban_info:
                stock_data["realtime_status"] = self._parse_kaipan_status(ban_info)
                stock_data["data_sources"]["kaipan_status"] = "success"

            ban_reasons = self.kaipan_analyzer.get_ban_reasons(base_code)
            if ban_reasons:
                stock_data["ban_reasons"] = self.kaipan_analyzer.parse_ban_reasons(ban_reasons)

            dde_data = self.kaipan_analyzer.get_stock_dde(base_code)
            if dde_data:
                stock_data["dde"] = dde_data

        except Exception as e:
            stock_data["data_sources"]["kaipan_status"] = f"error: {str(e)}"
            print(f"[警告] 获取开盘啦数据失败: {e}")

        # === 第二层：市场关注度数据（热榜 - 异步）===
        try:
            hot_stocks = await self.hot_api.get_trending_stocks(top_n=200)
            for stock in hot_stocks:
                hot_code = self._format_stock_code(stock.get("code") or stock.get("stock_code"))
                if hot_code.startswith(base_code):
                    stock_data["hot_rank"] = {
                        "rank": stock.get("current_rank", 999),
                        "rate": stock.get("rate", 0),
                        "change": stock.get("change", 0),
                        "rise_and_fall": stock.get("rise_and_fall", 0),
                    }
                    stock_data["data_sources"]["hot_rank"] = "success"
                    break
        except Exception as e:
            stock_data["data_sources"]["hot_rank"] = f"error: {str(e)}"

        # === 第三层：历史与逻辑数据（问财 - 异步）===
        if include_history:
            try:
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                query_str = f"{base_code} {yesterday} 涨停"
                hist_stocks = await self._get_wencai_stocks(query_str, loop=False)

                stock_data["history"] = {
                    "was_limit_up": len(hist_stocks) > 0,
                    "query_date": yesterday,
                }
                stock_data["data_sources"]["history"] = "success"
            except Exception as e:
                stock_data["data_sources"]["history"] = f"error: {str(e)}"

        # === 补充：所属板块/概念（从开盘啦或热榜）===
        try:
            concepts = await self.hot_api.get_top_concepts(top_n=20)
            stock_data["related_concepts"] = list(concepts.keys())[:5]
        except Exception:
            if "ban_reasons" in stock_data:
                reasons = [r.get("reason", "") for r in stock_data["ban_reasons"]]
                stock_data["related_concepts"] = reasons[:3]

        print(f"[数据获取] 股票 {formatted_code} 数据整合完成")
        return stock_data

    # ==================== 批量获取方法 ====================

    async def get_batch_status(self, stock_codes: List[str], max_concurrent: int = 10) -> Dict[str, Dict]:
        """批量获取多只股票的状态数据"""
        results: Dict[str, Dict] = {}

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(code: str):
            async with semaphore:
                try:
                    status = await self.get_stock_status(code)
                    return code, status
                except Exception as e:
                    print(f"[批量获取] 股票 {code} 获取失败: {e}")
                    return code, {"error": str(e)}

        tasks = [fetch_with_semaphore(code) for code in stock_codes]
        fetched_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in fetched_results:
            if isinstance(result, tuple) and len(result) == 2:
                code, data = result
                results[code] = data

        return results

    # ==================== 专用场景方法 ====================

    async def get_today_focus_stocks(self, min_hot_rank: int = 50, need_limit_up: bool = True) -> List[Dict]:
        """获取今日焦点股票（热榜排名靠前 + 涨停状态）"""
        hot_stocks = await self.hot_api.get_trending_stocks(top_n=min_hot_rank)
        hot_codes = []
        for stock in hot_stocks:
            code = self._format_stock_code(stock.get("code") or stock.get("stock_code"))
            if code:
                hot_codes.append({"code": code, "hot_data": stock})

        focus_stocks = []
        tasks = []
        for item in hot_codes[:20]:
            task = self.get_stock_status(item["code"])
            tasks.append((item["code"], task))

        for code, task in tasks:
            try:
                status = await task
                if need_limit_up:
                    is_limit_up = status.get("realtime_status", {}).get("is_limit_up", False)
                    if not is_limit_up:
                        continue

                hot_item = next((item for item in hot_codes if item["code"] == code), None)
                if hot_item:
                    status["hot_data"] = hot_item["hot_data"]

                focus_stocks.append(status)
            except Exception as e:
                print(f"[焦点股票] 获取 {code} 失败: {e}")

        focus_stocks.sort(key=lambda x: x.get("hot_data", {}).get("current_rank", 999))
        return focus_stocks

    # ==================== 问财专用：连板/首板标准化输出 ====================

    def _normalize_stock_code_6(self, code: Any) -> str:
        """统一为 6 位纯数字代码"""
        if not code:
            return ""
        s = str(code).strip().upper()
        if "." in s:
            s = s.split(".")[0]
        s = s.replace("SH", "").replace("SZ", "").replace("BJ", "")
        s = "".join(ch for ch in s if ch.isdigit())
        return s[:6] if len(s) >= 6 else s

    def _is_st_name(self, name: Any) -> bool:
        """基于名称的 ST 兜底过滤（问财已去ST时通常不需要，但可用于二次保险）"""
        if not name:
            return False
        s = str(name).strip().upper()
        return "ST" in s

    def _pick_lb_days_column(self, df: pd.DataFrame) -> Optional[str]:
        """从问财返回中自动识别“连板天数”列名（鲁棒匹配）"""
        if df is None or df.empty:
            return None

        # 强匹配
        candidates = [c for c in df.columns if c == "连板天数"]
        if candidates:
            return candidates[0]

        # 模糊匹配：包含“连板”且包含“天”
        candidates = [c for c in df.columns if isinstance(c, str) and ("连板" in c and "天" in c)]
        if candidates:
            return candidates[0]

        # 进一步兜底：包含“连板”即可
        candidates = [c for c in df.columns if isinstance(c, str) and "连板" in c]
        if candidates:
            return candidates[0]

        return None

    async def get_wencai_data(self, query: str, max_stocks: int = 500) -> pd.DataFrame:
        """通用问财数据获取接口，返回 DataFrame"""
        return await self._get_wencai_stocks(query, loop=True, max_stocks=max_stocks, return_df=True)

    async def get_wencai_limitup_with_lb_days(
        self,
        max_stocks: int = 500,
        loop: bool = True,
        extra_query: str = "",
    ) -> pd.DataFrame:
        """问财：获取“今日涨停 + 连板天数 + 去除ST”的标准化结果

        Returns:
            DataFrame 列：
                - formatted_code: 000001.SZ 格式
                - code6: 6位纯数字
                - lb_days: int（无法解析时为 None）
                - raw: 其余原始列保留（便于后续扩展）
        """
        query = "涨停;连板天数;去除st"
        if extra_query:
            query = f"{query};{extra_query}"

        df = await self._get_wencai_stocks(query, loop=loop, max_stocks=max_stocks, return_df=True)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame(columns=["formatted_code", "code6", "lb_days"])

        df = df.copy()

        # 标准化 code
        if "formatted_code" not in df.columns or df["formatted_code"].isna().all():
            if "代码" in df.columns:
                df["formatted_code"] = df["代码"].apply(self._format_stock_code)
            elif "股票代码" in df.columns:
                df["formatted_code"] = df["股票代码"].apply(self._format_stock_code)
            else:
                df["formatted_code"] = ""

        df["code6"] = df["formatted_code"].apply(self._normalize_stock_code_6)

        # 识别连板天数列
        lb_col = self._pick_lb_days_column(df)
        if lb_col:
            # 尽量转成 int
            df["lb_days"] = pd.to_numeric(df[lb_col], errors="coerce").astype("Int64")
        else:
            df["lb_days"] = pd.Series([pd.NA] * len(df), dtype="Int64")

        # 去重：按 code6
        df = df.drop_duplicates(subset=["code6"], keep="first")

        # 只返回核心列 + 原始列（方便你后续取更多字段）
        core_cols = ["formatted_code", "code6", "lb_days"]
        other_cols = [c for c in df.columns if c not in core_cols]
        return df[core_cols + other_cols]

    async def get_wencai_first_limit(
        self,
        max_stocks: int = 500,
        loop: bool = True,
        extra_query: str = "",
    ) -> pd.DataFrame:
        """问财：获取“今日首板涨停 + 去除ST”的标准化结果

        Returns:
            DataFrame 列：
                - formatted_code
                - code6
        """
        query = "首板涨停;去除st"
        if extra_query:
            query = f"{query};{extra_query}"

        df = await self._get_wencai_stocks(query, loop=loop, max_stocks=max_stocks, return_df=True)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame(columns=["formatted_code", "code6"])

        df = df.copy()

        if "formatted_code" not in df.columns or df["formatted_code"].isna().all():
            if "代码" in df.columns:
                df["formatted_code"] = df["代码"].apply(self._format_stock_code)
            elif "股票代码" in df.columns:
                df["formatted_code"] = df["股票代码"].apply(self._format_stock_code)
            else:
                df["formatted_code"] = ""

        df["code6"] = df["formatted_code"].apply(self._normalize_stock_code_6)
        df = df.drop_duplicates(subset=["code6"], keep="first")

        core_cols = ["formatted_code", "code6"]
        other_cols = [c for c in df.columns if c not in core_cols]
        return df[core_cols + other_cols]

    async def get_wencai_broken_boards(self, max_stocks: int = 100, loop: bool = False) -> List[str]:
        """问财：获取昨日炸板股 (昨日曾涨停且未涨停)"""
        query = "昨日曾涨停;昨日未涨停;去除st"
        return await self._get_wencai_stocks(query, loop=loop, max_stocks=max_stocks)

    async def get_wencai_first_failed(self, max_stocks: int = 100, loop: bool = False) -> List[str]:
        """问财：获取昨日首板失败股 (昨日曾涨停且未涨停且昨日非连板)"""
        query = "昨日曾涨停;昨日未涨停;昨日非连板;去除st"
        return await self._get_wencai_stocks(query, loop=loop, max_stocks=max_stocks)

    async def get_wencai_top_amount(self, top_n: int = 50, loop: bool = False) -> List[str]:
        """问财：获取昨日成交额前N名"""
        query = f"昨日成交额排行前{top_n};去除st"
        return await self._get_wencai_stocks(query, loop=loop, max_stocks=top_n)

    # ==================== 内部工具方法 ====================

    async def _get_wencai_stocks(
        self,
        query: str,
        loop: bool = False,
        max_stocks: int = 200,
        return_df: bool = False,
    ):
        """调用问财接口获取股票列表，支持分页获取更多数据

        Args:
            query: 查询语句
            loop: 是否启用分页（获取超过100只股票）
            max_stocks: 最大股票数量限制
            return_df: True 时返回问财原始 DataFrame（包含连板天数等列），False 时仅返回股票代码列表

        Returns:
            return_df=False: 股票代码列表（000001.SZ 格式）
            return_df=True: pandas.DataFrame（原始返回，额外增加一列 formatted_code）
        """
        try:
            import pywencai

            def sync_query():
                df = None
                codes = []

                def _extract_codes_from_df(_df: pd.DataFrame) -> List[str]:
                    if _df is None or not isinstance(_df, pd.DataFrame) or _df.empty:
                        return []
                    if "代码" in _df.columns:
                        return _df["代码"].tolist()
                    if "股票代码" in _df.columns:
                        return _df["股票代码"].tolist()
                    return []

                try:
                    if loop:
                        df = pywencai.get(query=query, loop=True, cookie=self.wencai_cookie)
                    else:
                        df = pywencai.get(query=query, loop=False, cookie=self.wencai_cookie)

                    if df is None:
                        logger.warning(f"[问财接口] 查询返回None: {query}")
                        return None, []

                    if isinstance(df, pd.DataFrame) and not df.empty:
                        raw_codes = _extract_codes_from_df(df)
                        unique_codes = list(dict.fromkeys(raw_codes))
                        codes = unique_codes[:max_stocks]
                        logger.info(f"[问财接口] 查询 '{query}' 返回 {len(codes)} 只股票")
                    else:
                        logger.warning(f"[问财接口] 查询 '{query}' 返回空结果或非DataFrame类型")
                        return None, []

                except TypeError as e:
                    if "'<' not supported between instances of 'int' and 'ProactorEventLoop'" in str(e):
                        logger.warning(f"[问财接口] 检测到Windows事件循环兼容性问题，禁用loop参数")
                        try:
                            df = pywencai.get(query=query, cookie=self.wencai_cookie)
                            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                                raw_codes = _extract_codes_from_df(df)
                                codes = list(dict.fromkeys(raw_codes))[:max_stocks]
                        except Exception as fallback_error:
                            logger.error(f"[问财接口] 备用方案失败: {fallback_error}")
                            return None, []
                    else:
                        raise e
                except AttributeError as e:
                    if "'NoneType' object has no attribute" in str(e):
                        logger.error(f"[问财接口] 检测到NoneType错误，可能是网络问题或cookie失效: {query} (Detail: {e})")
                    else:
                        logger.error(f"[问财接口] AttributeError: {query}, 错误: {e}")
                    return None, []
                except Exception as e:
                    logger.error(f"[问财接口] 查询失败: {query}, 错误: {e}")
                    return None, []

                return df, codes

            loop_obj = asyncio.get_event_loop()
            df, stocks = await loop_obj.run_in_executor(None, sync_query)

            if return_df:
                if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                    return pd.DataFrame()

                df = df.head(max_stocks).copy()

                if "代码" in df.columns:
                    df["formatted_code"] = df["代码"].apply(self._format_stock_code)
                elif "股票代码" in df.columns:
                    df["formatted_code"] = df["股票代码"].apply(self._format_stock_code)
                else:
                    df["formatted_code"] = ""

                return df

            formatted_stocks = [self._format_stock_code(code) for code in (stocks or []) if code]
            final_stocks = list(dict.fromkeys(formatted_stocks))
            return final_stocks

        except Exception as e:
            print(f"[问财接口] 整体查询失败: {query}, 错误: {e}")
            return []

    def _parse_kaipan_status(self, gene_data: Dict) -> Dict:
        """解析开盘啦的股票基因数据为状态字典"""
        status = {
            "is_limit_up": False,
            "is_breaking": False,
            "limit_order": 0,
            "break_time": None,
            "recover_time": None,
        }

        if isinstance(gene_data, dict):
            if gene_data.get("tag") == "涨停":
                status["is_limit_up"] = True
                status["limit_order"] = gene_data.get("limit_order", 0)
            elif gene_data.get("tag") == "炸板":
                status["is_breaking"] = True
                status["break_time"] = gene_data.get("open_time")
                status["recover_time"] = gene_data.get("last_time")

        return status

    def _format_stock_code(self, code: Any) -> str:
        """统一格式化股票代码为 000001.SZ 格式"""
        if not code:
            return ""

        code_str = str(code).strip()
        code_str = code_str.replace("SH", "").replace("SZ", "").replace(".", "")

        if code_str.isdigit():
            if code_str.startswith(("6", "9", "5")):
                return f"{code_str}.SH"
            elif code_str.startswith(("0", "3", "2")):
                return f"{code_str}.SZ"
            elif code_str.startswith("4"):
                return f"{code_str}.BJ"

        return code_str


async def main():
    fetcher = UnifiedMarketDataFetcher(hot_api_source="ths")

    print("正在获取核心股票池...")
    core_pool = await fetcher.get_core_stock_pool(history_lookback=1, hot_top_n=100)
    print(f"核心股票池数量: {len(core_pool)}")
    print(f"前10只: {core_pool[:10]}")

    print("\n获取单只股票状态示例:")
    stock_status = await fetcher.get_stock_status("000620", include_history=True)

    print(f"股票代码: {stock_status['code']}")
    print(f"实时状态: {stock_status.get('realtime_status', {})}")
    print(f"热榜排名: {stock_status.get('hot_rank', {}).get('rank', '未上榜')}")
    print(f"涨停原因: {[r.get('reason', '') for r in stock_status.get('ban_reasons', [])[:2]]}")
    print(f"数据来源状态: {stock_status['data_sources']}")

    print("\n批量获取示例（前5只）:")
    batch_status = await fetcher.get_batch_status(core_pool[:5], max_concurrent=3)

    for code, status in batch_status.items():
        print(f"{code}: 涨停={status.get('realtime_status', {}).get('is_limit_up', False)}")

    print("\n获取今日焦点股票（热榜前50且涨停）:")
    focus_stocks = await fetcher.get_today_focus_stocks(min_hot_rank=50, need_limit_up=True)

    for i, stock in enumerate(focus_stocks[:5], 1):
        hot_rank = stock.get("hot_data", {}).get("current_rank", "N/A")
        print(f"{i}. {stock['code']} 热榜排名: {hot_rank}")


if __name__ == "__main__":
    asyncio.run(main())