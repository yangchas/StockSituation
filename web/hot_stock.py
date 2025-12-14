import aiohttp
import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime


class ThsHotListAPI:
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
        list_type: str = "tech"
    ) -> List[Dict]:
        """
        获取同花顺热榜股票数据
        
        Args:
            stock_type: 股票类型，默认为a（A股）
            period: 时间周期，默认为day（日榜）
            list_type: 榜单类型，默认为tech（技术榜单）
            
        Returns:
            List[Dict]: 股票数据列表
            
        Raises:
            Exception: 当请求失败或返回数据格式异常时抛出
        """
        # 构建请求参数
        params = {
            "stock_type": stock_type,
            "type": period,
            "list_type": list_type
        }
        
        try:
            # 创建异步会话
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    # 检查响应状态
                    if response.status != 200:
                        raise Exception(f"请求失败，状态码: {response.status}")
                    
                    # 解析响应数据
                    data = await response.json()
                    
                    # 检查返回的数据结构
                    if not isinstance(data, dict):
                        raise Exception(f"返回数据格式异常: {data}")
                    
                    if data.get("status_code") != 0:
                        error_msg = data.get("status_msg", "未知错误")
                        raise Exception(f"接口返回错误: {error_msg}")
                    
                    # 提取股票数据
                    stocks_data = data.get("data", {}).get("stock_list", [])
                    
                    if not isinstance(stocks_data, list):
                        raise Exception("stock_list字段格式不是列表")
                    
                    # 返回股票数据
                    return stocks_data
                    
        except aiohttp.ClientError as e:
            raise Exception(f"网络请求错误: {str(e)}")
        except Exception as e:
            raise Exception(f"获取热榜数据失败: {str(e)}")
    
    def _parse_market_code(self, market_code: int) -> str:
        """
        解析市场代码
        
        Args:
            market_code: 市场代码
            
        Returns:
            str: 市场名称
        """
        market_map = {
            17: "上海证券交易所",  # SH开头
            33: "深圳证券交易所",  # SZ开头
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
        """
        根据市场代码添加股票代码前缀
        
        Args:
            code: 股票代码
            market_code: 市场代码
            
        Returns:
            str: 带前缀的股票代码
        """
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
        include_topic: bool = True
    ) -> List[Dict]:
        """
        获取格式化后的热榜股票数据
        
        Args:
            top_n: 返回前N个股票，None表示返回所有
            include_tags: 是否包含标签信息
            include_topic: 是否包含话题信息
            
        Returns:
            List[Dict]: 格式化后的股票数据
        """
        try:
            # 获取原始数据
            stocks = await self._get_trending_stocks()
            
            # 如果需要限制数量
            if top_n is not None and top_n > 0:
                stocks = stocks[:top_n]
            
            # 格式化数据
            formatted_stocks = []
            for stock in stocks:
                # 基础信息
                formatted_stock = {
                    "code": stock.get("code", ""),  # 股票代码
                    "market_code": stock.get("market", 0),  # 市场代码
                    "market": self._parse_market_code(stock.get("market", 0)),  # 市场名称
                    "full_code": self._add_market_prefix(stock.get("code", ""), stock.get("market", 0)),  # 带市场前缀的完整代码
                    "name": stock.get("name", ""),  # 股票名称
                    "rank": stock.get("display_order", 0),  # 显示排名
                    "order": stock.get("order", 0),  # 原始排序
                    "rate": float(stock.get("rate", 0)),  # 热度值
                    "rise_and_fall": float(stock.get("rise_and_fall", 0)),  # 涨跌幅
                    "hot_rank_chg": stock.get("hot_rank_chg", 0),  # 热度排名变化
                    "search_cnt": stock.get("search_cnt", 0),  # 搜索次数
                    "update_time": stock.get("update_time", "")  # 更新时间
                }
                
                # 标签信息
                if include_tags:
                    tag_info = stock.get("tag", {})
                    formatted_stock["concept_tags"] = tag_info.get("concept_tag", [])
                    formatted_stock["popularity_tag"] = tag_info.get("popularity_tag")
                
                # 话题信息
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
            print(f"获取格式化热榜数据失败: {str(e)}")
            return []
    
    async def get_stocks_by_concept(
        self, 
        concept: str, 
        top_n: Optional[int] = None
    ) -> List[Dict]:
        """
        根据概念标签筛选股票
        
        Args:
            concept: 概念标签
            top_n: 返回前N个股票
            
        Returns:
            List[Dict]: 符合概念的股票列表
        """
        try:
            stocks = await self._get_trending_stocks()
            filtered_stocks = []
            
            for stock in stocks:
                tag_info = stock.get("tag", {})
                concept_tags = tag_info.get("concept_tag", [])
                
                if concept in concept_tags:
                    filtered_stocks.append(stock)
            
            # 如果需要限制数量
            if top_n is not None and top_n > 0:
                filtered_stocks = filtered_stocks[:top_n]
            
            return filtered_stocks
            
        except Exception as e:
            print(f"按概念筛选股票失败: {str(e)}")
            return []
    
    async def get_top_concepts(
        self, 
        top_n: int = 10
    ) -> Dict[str, int]:
        """
        获取热门概念标签及其出现次数
        
        Args:
            top_n: 返回前N个热门概念
            
        Returns:
            Dict[str, int]: 概念标签及其出现次数
        """
        try:
            stocks = await self._get_trending_stocks()
            concept_counter = {}
            
            for stock in stocks:
                tag_info = stock.get("tag", {})
                concept_tags = tag_info.get("concept_tag", [])
                
                for concept in concept_tags:
                    concept_counter[concept] = concept_counter.get(concept, 0) + 1
            
            # 按出现次数排序
            sorted_concepts = sorted(
                concept_counter.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            # 取前N个
            top_concepts = dict(sorted_concepts[:top_n])
            
            return top_concepts
            
        except Exception as e:
            print(f"获取热门概念失败: {str(e)}")
            return {}
import aiohttp
import asyncio
from typing import List, Dict, Optional
import json


class EastMoneyAPI:
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
        """
        获取东方财富热榜股票数据
        
        Args:
            page_no: 页码，默认为1
            page_size: 每页数量，默认为100
            market_type: 市场类型，默认为空字符串（全部市场）
        
        Returns:
            List[Dict]: 股票数据列表，每个字典包含股票信息
            
        Raises:
            Exception: 当请求失败或返回数据格式异常时抛出
        """
        url = f"{self.base_url}/stockrank/getAllCurrentList"
        
        # 构建请求参数
        payload = {
            "appId": "appId01",
            "globalId": "786e4c21-70dc-435a-93bb-38",
            "marketType": market_type,
            "pageNo": page_no,
            "pageSize": page_size
        }
        
        try:
            # 创建异步会话
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, 
                    headers=self.headers, 
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    # 检查响应状态
                    if response.status != 200:
                        raise Exception(f"请求失败，状态码: {response.status}")
                    
                    # 解析响应数据
                    data = await response.json()
                    
                    # 检查返回的数据结构
                    if not isinstance(data, dict):
                        raise Exception(f"返回数据格式异常: {data}")
                    
                    if data.get("code") != 0 or data.get("status") != 0:
                        error_msg = data.get("message", "未知错误")
                        raise Exception(f"接口返回错误: {error_msg}")
                    
                    # 提取股票数据
                    stocks_data = data.get("data", [])
                    
                    if not isinstance(stocks_data, list):
                        raise Exception("data字段格式不是列表")
                    
                    # 返回股票数据
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
        """
        获取格式化后的热榜股票数据
        
        Args:
            top_n: 返回前N个股票，None表示返回所有
            include_ranking: 是否包含排名信息
        
        Returns:
            List[Dict]: 格式化后的股票数据
        """
        try:
            # 获取原始数据
            stocks = await self._get_trending_stocks()
            
            # 如果需要限制数量
            if top_n is not None and top_n > 0:
                stocks = stocks[:top_n]
            
            # 格式化数据
            formatted_stocks = []
            for stock in stocks:
                formatted_stock = {
                    "stock_code": stock.get("sc", ""),  # 股票代码
                    "current_rank": stock.get("rk", 0),  # 当前排名
                    "change": stock.get("rc", 0),  # 排名变化
                    "historical_change": stock.get("hisRc", 0)  # 历史变化
                }
                
                # 添加市场信息（从股票代码中解析）
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
            print(f"获取格式化热榜数据失败: {str(e)}")
            return []







# 使用示例
async def main():
    # 创建API实例
    api = EastMoneyAPI()
    
    try:
        # 方法1: 获取原始热榜数据
        print("获取原始热榜数据...")
        raw_stocks = await api._get_trending_stocks()
        print(f"获取到 {len(raw_stocks)} 个股票数据")
        if raw_stocks:
            print("前5个股票:", raw_stocks[:5])
        
        # 方法2: 获取格式化后的热榜数据
        print("\n获取格式化热榜数据...")
        formatted_stocks = await api.get_formatted_trending_stocks(top_n=10)
        print(f"前10个热榜股票:")
        for stock in formatted_stocks:
            print(f"  排名{stock['current_rank']}: {stock['stock_code']} "
                  f"({stock['market']}) - 变化: {stock['change']}")
    
    except Exception as e:
        print(f"错误: {str(e)}")
    api = ThsHotListAPI()
    
    try:
        # 方法1: 获取原始热榜数据
        print("获取原始热榜数据...")
        raw_stocks = await api._get_trending_stocks()
        print(f"获取到 {len(raw_stocks)} 个股票数据")
        if raw_stocks:
            print("前3个股票:", raw_stocks[:3])
        
        # 方法2: 获取格式化后的热榜数据
        print("\n获取格式化热榜数据...")
        formatted_stocks = await api.get_formatted_trending_stocks(top_n=10)
        print(f"前10个热榜股票:")
        for stock in formatted_stocks:
            print(f"  排名{stock['rank']}: {stock['full_code']} {stock['name']} "
                  f"- 涨幅: {stock['rise_and_fall']:.2f}% - 热度: {stock['rate']}")
        
        # 方法3: 按概念筛选股票
        print("\n获取'商业航天'概念股票...")
        commercial_space_stocks = await api.get_stocks_by_concept("商业航天", top_n=5)
        print(f"找到 {len(commercial_space_stocks)} 个商业航天概念股:")
        for stock in commercial_space_stocks:
            print(f"  {stock['code']} {stock['name']}")
        
        # 方法4: 获取热门概念
        print("\n获取热门概念标签...")
        top_concepts = await api.get_top_concepts(top_n=8)
        print("热门概念标签:")
        for concept, count in top_concepts.items():
            print(f"  {concept}: {count}次")
    
    except Exception as e:
        print(f"错误: {str(e)}")


# 如果需要同步调用，可以使用以下包装函数
def sync_get_trending_stocks(
    stock_type: str = "a",
    period: str = "day",
    list_type: str = "tech"
) -> List[Dict]:
    """
    同步获取热榜数据的包装函数
    
    Args:
        stock_type: 股票类型
        period: 时间周期
        list_type: 榜单类型
        
    Returns:
        List[Dict]: 股票数据列表
    """
    api = ThsHotListAPI()
    
    async def async_wrapper():
        return await api._get_trending_stocks(stock_type, period, list_type)
    
    return asyncio.run(async_wrapper())


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())