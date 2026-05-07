import logging
import os
import threading
from collections import OrderedDict
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd


class _NoopRedisClient:
    def delete(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _NoopRedisStorageManager:
    def __init__(self) -> None:
        self.redis = _NoopRedisClient()

    def get_data(self, *_args: Any, **_kwargs: Any) -> Any:
        return None

    def store_data(self, *_args: Any, **_kwargs: Any) -> None:
        return None


try:
    _sink = StringIO()
    with redirect_stdout(_sink), redirect_stderr(_sink):
        from web.redis_storage import RedisStorageManager as _RedisStorageManager
except Exception:
    try:
        import sys

        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
        _sink = StringIO()
        with redirect_stdout(_sink), redirect_stderr(_sink):
            from web.redis_storage import RedisStorageManager as _RedisStorageManager
    except Exception:
        _RedisStorageManager = _NoopRedisStorageManager

logger = logging.getLogger(__name__)


class F10DataService:
    """
    F10 CSV metadata provider.

    Design goals:
    - singleton, avoid repeated heavy CSV initialization
    - mimic engine_v2 metadata loading style
    - load code index first, load key columns lazily
    - keep only required columns in memory
    """

    _instance = None
    _instance_lock = threading.Lock()
    _init_lock = threading.Lock()

    ENCODINGS = ("gbk", "gb18030", "utf-8-sig", "utf-8")
    INDEX_COLUMNS = ("股票代码",)
    NAME_COLUMNS = (
        "股票代码",
        "股票简称",
    )
    DATA_COLUMNS = (
        "股票代码",
        "股票简称",
        "所属同花顺行业",
        "城市",
        "新股上市日期",
        "总市值",
        "a股市值(不含限售股)",
        "总股本",
        "流通a股",
        "营业收入",
        "归属于母公司所有者的净利润",
        "净资产收益率roe(加权,公布值)",
        "市净率(pb)",
        "市盈率(pe)",
        "资产负债率",
        "销售毛利率",
        "主营产品名称",
    )

    def __new__(cls, csv_file_path: Optional[str] = None):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, csv_file_path: Optional[str] = None):
        resolved_path = self._resolve_csv_path(csv_file_path)
        with self._init_lock:
            if getattr(self, "_initialized", False):
                if resolved_path == self.csv_file_path:
                    return
                logger.warning(
                    "F10DataService already initialized with %s, ignore new path %s",
                    self.csv_file_path,
                    resolved_path,
                )
                return

            self.csv_file_path = resolved_path
            self._encoding: Optional[str] = None
            self._full_data_lock = threading.Lock()
            self.data_loaded = False
            self.name_map_loaded = False
            self.f10_data: Optional[pd.DataFrame] = None
            self.name_by_code: Dict[str, str] = {}
            self.index_by_code: Dict[str, int] = {}
            self.memory_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
            self.memory_cache_limit = 1024
            self.redis_storage = _RedisStorageManager()

            self._load_index()
            self._initialized = True

    def _resolve_csv_path(self, csv_file_path: Optional[str]) -> str:
        if csv_file_path:
            raw = os.path.expandvars(csv_file_path)
            if os.path.isabs(raw):
                return raw

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        raw_path = os.path.expandvars(csv_file_path or "data/f10.csv")
        candidates = [
            raw_path,
            os.path.join(project_root, raw_path),
            os.path.join(project_root, "web", "data", "f10.csv"),
            os.path.join(os.getcwd(), raw_path),
            os.path.join(os.getcwd(), "web", "data", "f10.csv"),
        ]
        for candidate in candidates:
            if candidate and os.path.exists(candidate):
                return os.path.abspath(candidate)
        return os.path.abspath(candidates[1])

    def _detect_encoding(self) -> Optional[str]:
        if self._encoding:
            return self._encoding
        if not os.path.exists(self.csv_file_path):
            logger.error("F10 csv missing: %s", self.csv_file_path)
            return None

        for encoding in self.ENCODINGS:
            try:
                pd.read_csv(self.csv_file_path, encoding=encoding, nrows=0)
                self._encoding = encoding
                return encoding
            except Exception:
                continue
        logger.error("Unable to detect F10 csv encoding: %s", self.csv_file_path)
        return None

    def _read_csv(self, usecols: Optional[List[str]] = None, nrows: Optional[int] = None) -> pd.DataFrame:
        encoding = self._detect_encoding()
        if not encoding:
            raise FileNotFoundError(self.csv_file_path)
        return pd.read_csv(self.csv_file_path, encoding=encoding, usecols=usecols, nrows=nrows)

    @staticmethod
    def _normalize_stock_code(code: Any) -> str:
        if pd.isna(code):
            return ""
        code_str = str(code).strip().upper()
        if "." in code_str:
            code_str = code_str.split(".", 1)[0]
        code_str = code_str.zfill(6)
        return code_str if len(code_str) == 6 and code_str.isdigit() else ""

    def _load_index(self) -> None:
        if not os.path.exists(self.csv_file_path):
            logger.error("F10 csv missing: %s", self.csv_file_path)
            return

        try:
            df_codes = self._read_csv(usecols=list(self.INDEX_COLUMNS))
            index_map: Dict[str, int] = {}
            for idx, row in df_codes.iterrows():
                code = self._normalize_stock_code(row.get("股票代码"))
                if code:
                    index_map[code] = idx
            self.index_by_code = index_map
            logger.info("F10数据索引加载完成: %s 只股票", len(self.index_by_code))
        except Exception as exc:
            logger.error("加载 F10 索引失败: %s", exc)

    def _load_full_data_if_needed(self) -> None:
        if self.data_loaded:
            return
        with self._full_data_lock:
            if self.data_loaded:
                return
            try:
                try:
                    df = self._read_csv(usecols=list(self.DATA_COLUMNS))
                except ValueError as exc:
                    logger.warning("F10 关键列不完整，回退全量加载: %s", exc)
                    df = self._read_csv()

                df.columns = [str(col).strip() for col in df.columns]
                df["_std_code"] = df["股票代码"].map(self._normalize_stock_code)
                df = df[df["_std_code"] != ""].drop_duplicates("_std_code", keep="last")
                df = df.set_index("_std_code", drop=False)
                self.f10_data = df
                self.data_loaded = True
                logger.info("F10完整数据加载完成: %s 行", len(df))
            except Exception as exc:
                logger.error("加载 F10 完整数据失败: %s", exc)

    def _load_name_map_if_needed(self) -> None:
        if self.name_map_loaded:
            return
        try:
            df = self._read_csv(usecols=list(self.NAME_COLUMNS))
            df.columns = [str(col).strip() for col in df.columns]
            code_col = self.NAME_COLUMNS[0]
            name_col = self.NAME_COLUMNS[1]
            name_map: Dict[str, str] = {}
            for _, row in df.iterrows():
                code = self._normalize_stock_code(row.get(code_col))
                if not code:
                    continue
                name = str(row.get(name_col, "") or "").strip()
                if name:
                    name_map[code] = name
            self.name_by_code = name_map
            self.name_map_loaded = True
        except Exception as exc:
            logger.error("加载 F10 名称映射失败: %s", exc)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if pd.isna(value) or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            text = str(value).strip().replace(",", "")
            try:
                if text.endswith("亿"):
                    return float(text[:-1]) * 100000000
                if text.endswith("万"):
                    return float(text[:-1]) * 10000
                return float(text)
            except (TypeError, ValueError):
                return None

    @staticmethod
    def _parse_main_products(products: Any) -> List[str]:
        if pd.isna(products) or not products:
            return []
        text = str(products)
        if "||" in text:
            return [item.strip() for item in text.split("||") if item.strip()]
        return [item.strip() for item in text.split(";") if item.strip()] or [text.strip()]

    def _extract_product_categories(self, products: Any) -> List[str]:
        categories = set()
        for item in self._parse_main_products(products):
            text = item.lower()
            if "芯片" in item or "半导体" in item:
                categories.add("芯片")
            if "软件" in item or "saas" in text:
                categories.add("软件")
            if "汽车" in item or "新能源车" in item:
                categories.add("汽车")
            if "医药" in item or "医疗" in item:
                categories.add("医药")
            if "机器人" in item or "自动化" in item:
                categories.add("机器人")
            if "军工" in item:
                categories.add("军工")
            if "光伏" in item or "储能" in item:
                categories.add("新能源")
        return sorted(categories)

    def _format_f10_data(self, row_data: pd.Series) -> Dict[str, Any]:
        return {
            "basic": {
                "stock_code": self._normalize_stock_code(row_data.get("股票代码")),
                "stock_name": str(row_data.get("股票简称", "") or ""),
                "industry": str(row_data.get("所属同花顺行业", "") or ""),
                "city": str(row_data.get("城市", "") or ""),
                "listing_date": str(row_data.get("新股上市日期", "") or ""),
            },
            "financial": {
                "total_market_cap": self._safe_float(row_data.get("总市值")),
                "circulating_market_cap": self._safe_float(row_data.get("a股市值(不含限售股)")),
                "total_shares": self._safe_float(row_data.get("总股本")),
                "circulating_shares": self._safe_float(row_data.get("流通a股")),
                "revenue": self._safe_float(row_data.get("营业收入")),
                "net_profit": self._safe_float(row_data.get("归属于母公司所有者的净利润")),
                "roe": self._safe_float(row_data.get("净资产收益率roe(加权,公布值)")),
                "pb": self._safe_float(row_data.get("市净率(pb)")),
                "pe": self._safe_float(row_data.get("市盈率(pe)")),
                "debt_ratio": self._safe_float(row_data.get("资产负债率")),
                "gross_margin": self._safe_float(row_data.get("销售毛利率")),
            },
            "business": {
                "main_products": self._parse_main_products(row_data.get("主营产品名称")),
                "product_categories": self._extract_product_categories(row_data.get("主营产品名称")),
            },
            "timestamp": pd.Timestamp.now().isoformat(),
        }

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        cached = self.memory_cache.get(key)
        if cached is not None:
            self.memory_cache.move_to_end(key)
            return cached
        return None

    def _cache_set(self, key: str, value: Dict[str, Any]) -> None:
        self.memory_cache[key] = value
        self.memory_cache.move_to_end(key)
        while len(self.memory_cache) > self.memory_cache_limit:
            self.memory_cache.popitem(last=False)

    def get_stock_f10(self, stock_code: str) -> Optional[Dict[str, Any]]:
        normalized_code = self._normalize_stock_code(stock_code)
        if not normalized_code:
            return None

        cache_key = f"f10_{normalized_code}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            cached = self.redis_storage.get_data(cache_key)
        except Exception:
            cached = None
        if cached:
            self._cache_set(cache_key, cached)
            return cached

        if normalized_code not in self.index_by_code:
            return None

        self._load_full_data_if_needed()
        if self.f10_data is None or normalized_code not in self.f10_data.index:
            return None

        row_data = self.f10_data.loc[normalized_code]
        if isinstance(row_data, pd.DataFrame):
            row_data = row_data.iloc[-1]
        result = self._format_f10_data(row_data)
        self._cache_set(cache_key, result)
        try:
            self.redis_storage.store_data(cache_key, result, expire_seconds=3600)
        except Exception:
            pass
        return result

    def batch_get_f10(self, stock_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        for code in stock_codes:
            item = self.get_stock_f10(code)
            if item:
                results[code] = item
        return results

    def get_stock_name(self, stock_code: str) -> str:
        normalized_code = self._normalize_stock_code(stock_code)
        if not normalized_code:
            return ""
        if normalized_code in self.name_by_code:
            return str(self.name_by_code.get(normalized_code) or "").strip()
        self._load_name_map_if_needed()
        return str(self.name_by_code.get(normalized_code) or "").strip()

    def batch_get_stock_names(self, stock_codes: List[str]) -> Dict[str, str]:
        normalized_codes = [
            normalized
            for code in stock_codes
            for normalized in (self._normalize_stock_code(code),)
            if normalized
        ]
        if not normalized_codes:
            return {}
        self._load_name_map_if_needed()
        return {
            code: name
            for code in normalized_codes
            for name in (str(self.name_by_code.get(code) or "").strip(),)
            if name
        }

    def search_stocks(self, keyword: str, limit: int = 50) -> List[Dict[str, Any]]:
        self._load_full_data_if_needed()
        if self.f10_data is None:
            return []

        keyword_lower = str(keyword or "").strip().lower()
        if not keyword_lower:
            return []

        results: List[Dict[str, Any]] = []
        for _, row in self.f10_data.iterrows():
            stock_code = self._normalize_stock_code(row.get("股票代码"))
            stock_name = str(row.get("股票简称", "") or "")
            industry = str(row.get("所属同花顺行业", "") or "")
            products = str(row.get("主营产品名称", "") or "")

            haystacks = {
                "code": stock_code.lower(),
                "name": stock_name.lower(),
                "industry": industry.lower(),
                "products": products.lower(),
            }
            match_field = next((field for field, text in haystacks.items() if keyword_lower in text), None)
            if not match_field:
                continue

            results.append(
                {
                    "code": stock_code,
                    "name": stock_name,
                    "industry": industry,
                    "match_field": match_field,
                }
            )
            if len(results) >= limit:
                break
        return results

    def get_cache_stats(self) -> Dict[str, Any]:
        return {
            "memory_cache_size": len(self.memory_cache),
            "name_map_size": len(self.name_by_code),
            "index_size": len(self.index_by_code),
            "data_loaded": self.data_loaded,
            "name_map_loaded": self.name_map_loaded,
            "csv_file_path": self.csv_file_path,
            "encoding": self._encoding,
        }

    def clear_cache(self, stock_code: Optional[str] = None) -> None:
        if stock_code:
            normalized_code = self._normalize_stock_code(stock_code)
            cache_key = f"f10_{normalized_code}"
            self.memory_cache.pop(cache_key, None)
            self.name_by_code.pop(normalized_code, None)
            try:
                self.redis_storage.redis.delete(cache_key)
            except Exception:
                pass
            return

        self.memory_cache.clear()
        self.name_by_code.clear()
        self.name_map_loaded = False
