"""
热榜数据获取封装类
整合同花顺和东方财富两个热榜API，提供统一的接口
"""

import aiohttp
import asyncio
import json
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from enum import Enum


class DataSource(Enum):
    """数据源枚举"""
    THS = "ths"  # 同花顺
    EASTMONEY = "eastmoney"  # 东方财富


class ThsHotListAPI:
    """同花顺热榜API"""
    
    def __init__(self):
        self.base_url = "https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock"
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "dq.10jqka.com.cn",
            "Referer": "https://www.10jqka.com.cn/",
            "Sec-Ch-Ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
        }
    
    async def _get_trending_stocks(
        self, 
        stock_type: str = "a",
        period: str = "day",
        list_type: str = "tech",
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> List[Dict]:
        """获取同花顺热榜股票数据（带重试功能）"""
        params = {
            "stock_type": stock_type,
            "type": period,
            "list_type": list_type
        }
        
        def write_log(message):
            with open('ths_hotlist_log.txt', 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
        
        last_exception = None
        for attempt in range(max_retries):
            try:
                write_log(f"开始获取同花顺热榜数据 (第{attempt + 1}次尝试)")
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.base_url,
                        headers=self.headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        
                        if response.status != 200:
                            raise Exception(f"请求失败，状态码: {response.status}")
                        
                        data = await response.json()
                        
                        if not isinstance(data, dict):
                            raise Exception(f"返回数据格式异常: {data}")
                        
                        if data.get("status_code") != 0:
                            error_msg = data.get("status_msg", "未知错误")
                            raise Exception(f"接口返回错误: {error_msg}")
                        
                        stocks_data = data.get("data", {}).get("stock_list", [])
                        
                        if not isinstance(stocks_data, list):
                            raise Exception("stock_list字段格式不是列表")
                        
                        write_log(f"✅ 第{attempt + 1}次尝试成功获取热榜数据")
                        return stocks_data
                        
            except aiohttp.ClientError as e:
                last_exception = Exception(f"网络请求错误: {str(e)}")
                write_log(f"❌ 第{attempt + 1}次尝试失败: {str(e)}")
                
            except Exception as e:
                last_exception = Exception(f"获取热榜数据失败: {str(e)}")
                write_log(f"❌ 第{attempt + 1}次尝试失败: {str(e)}")
            
            if attempt < max_retries - 1:
                write_log(f"⏳ 等待{retry_delay}秒后重试...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
        
        write_log(f"❌ 所有{max_retries}次尝试都失败，抛出异常")
        raise last_exception if last_exception else Exception("获取热榜数据失败")
    
    def _parse_market_code(self, market_code: int) -> str:
        """解析市场代码"""
        market_map = {
            17: "上海证券交易所",
            33: "深圳证券交易所",
            1: "上海A股",
            2: "上海B股",
            3: "深圳A股",
            4: "深圳B股",
            5: "科创板",
            6: "创业板",
            7: "北交所"
        }
        return market_map.get(market_code, f"未知市场({market_code})")
    
    def _add_market_prefix(self, code: str, market_code: int) -> str:
        """根据市场代码添加股票代码前缀"""
        if market_code == 17:
            return f"SH{code}"
        elif market_code == 33:
            return f"SZ{code}"
        else:
            return code
    
    async def get_formatted_trending_stocks(
        self, 
        top_n: Optional[int] = None,
        include_tags: bool = True,
        include_topic: bool = True,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ) -> List[Dict]:
        """获取格式化后的热榜股票数据"""
        last_exception = None
        for attempt in range(max_retries):
            try:
                stocks = await self._get_trending_stocks(max_retries=1, retry_delay=retry_delay)
                
                if top_n is not None and top_n > 0:
                    stocks = stocks[:top_n]
                
                formatted_stocks = []
                for stock in stocks:
                    formatted_stock = {
                        "code": stock.get("code", ""),
                        "market_code": stock.get("market", 0),
                        "market": self._parse_market_code(stock.get("market", 0)),
                        "full_code": self._add_market_prefix(stock.get("code", ""), stock.get("market", 0)),
                        "name": stock.get("name", ""),
                        "rank": stock.get("display_order", 0),
                        "order": stock.get("order", 0),
                        "rate": float(stock.get("rate")) if stock.get("rate") is not None else 0.0,
                        "rise_and_fall": float(stock.get("rise_and_fall")) if stock.get("rise_and_fall") is not None else 0.0,
                        "hot_rank_chg": stock.get("hot_rank_chg", 0),
                        "search_cnt": stock.get("search_cnt", 0),
                        "update_time": stock.get("update_time", "")
                    }
                    
                    if include_tags:
                        tag_info = stock.get("tag", {})
                        formatted_stock["concept_tags"] = tag_info.get("concept_tag", [])
                        formatted_stock["popularity_tag"] = tag_info.get("popularity_tag")
                    
                    if include_topic:
                        topic_info = stock.get("topic")
                        if topic_info:
                            formatted_stock["topic"] = {
                                "code": topic_info.get("topic_code"),
                                "title": topic_info.get("title"),
                                "ios_url": topic_info.get("ios_jump_url"),
                                "android_url": topic_info.get("android_jump_url")
                            }
                        else:
                            formatted_stock["topic"] = None
                    
                    formatted_stocks.append(formatted_stock)
                
                return formatted_stocks
                
            except Exception as e:
                last_exception = e
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
        
        return []
    
    async def get_stocks_by_concept(
        self, 
        concept: str, 
        top_n: Optional[int] = None
    ) -> List[Dict]:
        """根据概念标签筛选股票"""
        try:
            stocks = await self._get_trending_stocks()
            filtered_stocks = []
            
            for stock in stocks:
                tag_info = stock.get("tag", {})
                concept_tags = tag_info.get("concept_tag", [])
                
                if concept in concept_tags:
                    filtered_stocks.append(stock)
            
            if top_n is not None and top_n > 0:
                filtered_stocks = filtered_stocks[:top_n]
            
            return filtered_stocks
            
        except Exception as e:
            return []
    
    async def get_top_concepts(
        self, 
        top_n: int = 10
    ) -> Dict[str, int]:
        """获取热门概念标签及其出现次数"""
        try:
            stocks = await self._get_trending_stocks()
            concept_counter = {}
            
            for stock in stocks:
                tag_info = stock.get("tag", {})
                concept_tags = tag_info.get("concept_tag", [])
                
                for concept in concept_tags:
                    concept_counter[concept] = concept_counter.get(concept, 0) + 1
            
            sorted_concepts = sorted(
                concept_counter.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            top_concepts = dict(sorted_concepts[:top_n])
            
            return top_concepts
            
        except Exception as e:
            return {}


class EastMoneyAPI:
    """东方财富热榜API"""
    
    def __init__(self):
        self.base_url = "https://emappdata.eastmoney.com"
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Host": "emappdata.eastmoney.com",
            "Origin": "https://vipmoney.eastmoney.com",
            "Referer": "https://vipmoney.eastmoney.com/",
            "Sec-Ch-Ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
        }
    
    async def _get_trending_stocks(self, 
                                   page_no: int = 1, 
                                   page_size: int = 100,
                                   market_type: str = "") -> List[Dict]:
        """获取东方财富热榜股票数据"""
        url = f"{self.base_url}/stockrank/getAllCurrentList"
        
        payload = {
            "appId": "appId01",
            "globalId": "786e4c21-70dc-435a-93bb-38",
            "marketType": market_type,
            "pageNo": page_no,
            "pageSize": page_size
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, 
                    headers=self.headers, 
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    if response.status != 200:
                        raise Exception(f"请求失败，状态码: {response.status}")
                    
                    data = await response.json()
                    
                    if not isinstance(data, dict):
                        raise Exception(f"返回数据格式异常: {data}")
                    
                    if data.get("code") != 0 or data.get("status") != 0:
                        error_msg = data.get("message", "未知错误")
                        raise Exception(f"接口返回错误: {error_msg}")
                    
                    stocks_data = data.get("data", [])
                    
                    if not isinstance(stocks_data, list):
                        raise Exception("data字段格式不是列表")
                    
                    return stocks_data
                    
        except aiohttp.ClientError as e:
            raise Exception(f"网络请求错误: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"JSON解析错误: {str(e)}")
        except Exception as e:
            raise Exception(f"获取热榜数据失败: {str(e)}")
    
    async def get_formatted_trending_stocks(self, 
                                           top_n: Optional[int] = None,
                                           include_ranking: bool = True) -> List[Dict]:
        """获取格式化后的热榜股票数据"""
        try:
            stocks = await self._get_trending_stocks()
            
            if top_n is not None and top_n > 0:
                stocks = stocks[:top_n]
            
            formatted_stocks = []
            for stock in stocks:
                formatted_stock = {
                    "stock_code": stock.get("sc", ""),
                    "current_rank": stock.get("rk", 0),
                    "change": stock.get("rc", 0),
                    "historical_change": stock.get("hisRc", 0)
                }
                
                stock_code = formatted_stock["stock_code"]
                if stock_code.startswith("SH"):
                    formatted_stock["market"] = "上海证券交易所"
                elif stock_code.startswith("SZ"):
                    formatted_stock["market"] = "深圳证券交易所"
                else:
                    formatted_stock["market"] = "未知市场"
                
                formatted_stocks.append(formatted_stock)
            
            return formatted_stocks
            
        except Exception as e:
            return []


class HotStockAPI:
    """热榜数据获取封装类"""
    
    def __init__(self, data_source: Union[DataSource, str] = DataSource.THS):
        """
        初始化热榜API
        
        Args:
            data_source: 数据源，可以是DataSource枚举或字符串（"ths"或"eastmoney"）
        """
        if isinstance(data_source, str):
            data_source = DataSource(data_source.lower())
        
        self.data_source = data_source
        
        if data_source == DataSource.THS:
            self.api = ThsHotListAPI()
        elif data_source == DataSource.EASTMONEY:
            self.api = EastMoneyAPI()
        else:
            raise ValueError(f"不支持的数据源: {data_source}")
    
    async def get_trending_stocks(
        self, 
        top_n: Optional[int] = None,
        **kwargs
    ) -> List[Dict]:
        """
        获取热榜股票数据
        
        Args:
            top_n: 返回前N个股票，None表示返回所有
            **kwargs: 其他参数，根据数据源不同而不同
            
        Returns:
            List[Dict]: 热榜股票数据列表
        """
        try:
            if self.data_source == DataSource.THS:
                include_tags = kwargs.get("include_tags", True)
                include_topic = kwargs.get("include_topic", True)
                
                return await self.api.get_formatted_trending_stocks(
                    top_n=top_n,
                    include_tags=include_tags,
                    include_topic=include_topic
                )
            else:
                return await self.api.get_formatted_trending_stocks(top_n=top_n)
        except Exception as e:
            print(f"获取热榜数据失败: {str(e)}")
            return []
    
    async def get_stocks_by_concept(
        self, 
        concept: str, 
        top_n: Optional[int] = None
    ) -> List[Dict]:
        """
        根据概念标签筛选股票（仅同花顺数据源支持）
        
        Args:
            concept: 概念标签
            top_n: 返回前N个股票
            
        Returns:
            List[Dict]: 符合概念的股票列表
        """
        if self.data_source != DataSource.THS:
            print("警告: 概念筛选功能仅支持同花顺数据源")
            return []
        
        try:
            return await self.api.get_stocks_by_concept(concept, top_n)
        except Exception as e:
            print(f"按概念筛选股票失败: {str(e)}")
            return []
    
    async def get_top_concepts(
        self, 
        top_n: int = 10
    ) -> Dict[str, int]:
        """
        获取热门概念标签（仅同花顺数据源支持）
        
        Args:
            top_n: 返回前N个热门概念
            
        Returns:
            Dict[str, int]: 概念标签及其出现次数
        """
        if self.data_source != DataSource.THS:
            print("警告: 热门概念功能仅支持同花顺数据源")
            return {}
        
        try:
            return await self.api.get_top_concepts(top_n)
        except Exception as e:
            print(f"获取热门概念失败: {str(e)}")
            return {}
    
    def get_data_source_info(self) -> Dict[str, Any]:
        """
        获取数据源信息
        
        Returns:
            Dict[str, Any]: 数据源信息
        """
        return {
            "data_source": self.data_source.value,
            "description": "同花顺热榜" if self.data_source == DataSource.THS else "东方财富热榜",
            "supported_features": {
                "trending_stocks": True,
                "concept_filtering": self.data_source == DataSource.THS,
                "top_concepts": self.data_source == DataSource.THS
            }
        }


# 同步调用包装函数
def sync_get_trending_stocks(
    data_source: Union[DataSource, str] = DataSource.THS,
    top_n: Optional[int] = None,
    **kwargs
) -> List[Dict]:
    """
    同步获取热榜数据的包装函数
    
    Args:
        data_source: 数据源
        top_n: 返回前N个股票
        **kwargs: 其他参数
        
    Returns:
        List[Dict]: 热榜股票数据列表
    """
    api = HotStockAPI(data_source)
    
    async def async_wrapper():
        return await api.get_trending_stocks(top_n, **kwargs)
    
    return asyncio.run(async_wrapper())


def sync_get_stocks_by_concept(
    concept: str,
    top_n: Optional[int] = None
) -> List[Dict]:
    """
    同步根据概念标签筛选股票的包装函数
    
    Args:
        concept: 概念标签
        top_n: 返回前N个股票
        
    Returns:
        List[Dict]: 符合概念的股票列表
    """
    api = HotStockAPI(DataSource.THS)
    
    async def async_wrapper():
        return await api.get_stocks_by_concept(concept, top_n)
    
    return asyncio.run(async_wrapper())


def sync_get_top_concepts(top_n: int = 10) -> Dict[str, int]:
    """
    同步获取热门概念标签的包装函数
    
    Args:
        top_n: 返回前N个热门概念
        
    Returns:
        Dict[str, int]: 概念标签及其出现次数
    """
    api = HotStockAPI(DataSource.THS)
    
    async def async_wrapper():
        return await api.get_top_concepts(top_n)
    
    return asyncio.run(async_wrapper())


if __name__ == "__main__":
    # 测试代码
    async def test_api():
        print("=== 测试热榜API封装类 ===")
        
        # 测试同花顺热榜
        print("\n1. 测试同花顺热榜:")
        ths_api = HotStockAPI(DataSource.THS)
        ths_stocks = await ths_api.get_trending_stocks(top_n=3)
        print(f"获取到 {len(ths_stocks)} 个同花顺热榜股票")
        for stock in ths_stocks:
            print(f"  排名{stock.get('rank', 0)}: {stock.get('full_code', '')} {stock.get('name', '')}")
        
        # 测试东方财富热榜
        print("\n2. 测试东方财富热榜:")
        em_api = HotStockAPI(DataSource.EASTMONEY)
        em_stocks = await em_api.get_trending_stocks(top_n=3)
        print(f"获取到 {len(em_stocks)} 个东方财富热榜股票")
        for stock in em_stocks:
            print(f"  排名{stock.get('current_rank', 0)}: {stock.get('stock_code', '')}")
        
        # 测试数据源信息
        print("\n3. 数据源信息:")
        print(f"同花顺: {ths_api.get_data_source_info()}")
        print(f"东方财富: {em_api.get_data_source_info()}")
    
    asyncio.run(test_api())