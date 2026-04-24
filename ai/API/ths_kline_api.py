"""
同花顺个股K线数据获取API封装
支持获取个股的日K、周K、月K等不同周期的K线数据
"""

import requests
import json
import time
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class THSKlineAPI:
    """同花顺K线数据API封装类"""
    
    def __init__(self):
        self.base_url = "https://quota-h.10jqka.com.cn/fuyao/common_hq_aggr/quote/v1/single_kline"
        self.session = requests.Session()
        self._setup_headers()
    
    def _setup_headers(self):
        """设置请求头"""
        self.headers = {
            'authority': 'quota-h.10jqka.com.cn',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'content-type': 'application/json',
            'origin': 'https://www.iwencai.com',
            'platform': 'hxkline',
            'priority': 'u=1, i',
            'referer': 'https://www.iwencai.com/',
            'sec-ch-ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-storage-access': 'active',
            'source-id': 'hxkline-AIME_Component_Library_Component',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
            'x-auth-appname': 'AINVEST',
            'x-auth-progid': '7047',
            'x-auth-type': 'ths',
            'x-auth-version': '1.0',
            'x-fuyao-auth': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdXRob3JpemVyX25hbWVzcGFjZSI6ImNvbW1vbi1ocS1hZ2dyIiwibGljZW5zZWVfdHlwZSI6IkZST05UX0FQUCIsImxpY2Vuc2VlX25hbWVzcGFjZSI6Imh4a2xpbmUtQUlNRV9Db21wb25lbnRfTGlicmFyeV9Db21wb25lbnQifQ.MWqYrKk4Y2_oWTbG3XZjNGoHK_GmIi_KeJKc_mNDqTA'
        }
    
    def _get_market_code(self, stock_code: str) -> str:
        """根据股票代码获取市场代码"""
        if stock_code.startswith(('0', '3')):
            return '33'  # 深市
        elif stock_code.startswith(('6', '9')):
            return '17'  # 沪市
        else:
            return '33'  # 默认深市
    
    def _build_request_data(self, stock_codes: List[str], time_period: str = "day_1", 
                           trade_date: int = -1, begin_time: int = -350, end_time: int = 0,
                           adjust_type: str = "forward", gpid: int = 1) -> Dict:
        """构建请求数据"""
        code_list = []
        for code in stock_codes:
            market = self._get_market_code(code)
            code_list.append({
                "codes": [code],
                "market": market
            })
        
        return {
            "code_list": code_list,
            "trade_class": "intraday",
            "time_period": time_period,
            "trade_date": trade_date,
            "begin_time": begin_time,
            "end_time": end_time,
            "adjust_type": adjust_type,
            "gpid": gpid
        }
    
    def get_kline_data(self, stock_code: str, time_period: str = "day_1", 
                      trade_date: int = -1, begin_time: int = -350, end_time: int = 0,
                      adjust_type: str = "forward", gpid: int = 1) -> Optional[Dict]:
        """
        获取个股K线数据
        
        Args:
            stock_code: 股票代码，如 "002187"
            time_period: 时间周期，可选值：
                - "day_1": 日K
                - "week_1": 周K  
                - "month_1": 月K
                - "minute_1": 1分钟K
                - "minute_5": 5分钟K
                - "minute_15": 15分钟K
                - "minute_30": 30分钟K
                - "minute_60": 60分钟K
            trade_date: 交易日期，-1表示最新
            begin_time: 开始时间偏移量
            end_time: 结束时间偏移量
            adjust_type: 复权类型，"forward"前复权，"backward"后复权，"none"不复权
            gpid: 分组ID
            
        Returns:
            K线数据字典，包含时间、开盘价、最高价、最低价、收盘价、成交量等信息
        """
        try:
            # 构建请求数据
            request_data = self._build_request_data(
                [stock_code], time_period, trade_date, begin_time, end_time, adjust_type, gpid
            )
            
            logger.info(f"获取股票 {stock_code} 的 {time_period} K线数据")
            
            # 发送请求
            response = self.session.post(
                self.base_url,
                headers=self.headers,
                json=request_data,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # 检查响应是否成功（根据实际API响应格式调整）
                if data.get("status_code") == 0:
                    return self._parse_kline_data(data, stock_code)
                else:
                    error_msg = data.get("status_msg", "未知错误")
                    logger.error(f"API返回错误: {error_msg}")
                    return None
            else:
                logger.error(f"HTTP请求失败，状态码: {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("请求超时")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("网络连接错误")
            return None
        except Exception as e:
            logger.error(f"获取K线数据时发生错误: {str(e)}")
            return None
    
    def _parse_kline_data(self, data: Dict, stock_code: str) -> Dict:
        """解析K线数据"""
        try:
            result = {
                "stock_code": stock_code,
                "success": False,
                "data": [],
                "metadata": {}
            }
            
            # 提取数据（根据实际API响应格式调整）
            if "data" in data and data["data"]:
                api_data = data["data"]
                
                # 提取元数据
                result["metadata"] = {
                    "fail_params": api_data.get("fail_params", {}),
                    "time_period": api_data.get("fail_params", {}).get("time_period"),
                    "adjust_type": api_data.get("fail_params", {}).get("adjust_type")
                }
                
                # 解析K线数据（根据实际API响应格式调整）
                kline_data = api_data.get("quote_data", [])
                for kline in kline_data:
                    parsed_kline = {
                        "code": kline.get("code"),
                        "market": kline.get("market"),
                        "time": kline.get("time"),
                        "datetime": self._timestamp_to_datetime(kline.get("time")),
                        "open": kline.get("open"),
                        "high": kline.get("high"),
                        "low": kline.get("low"),
                        "close": kline.get("close"),
                        "volume": kline.get("volume"),
                        "amount": kline.get("amount"),
                        "pre_close": kline.get("pre_close")
                    }
                    result["data"].append(parsed_kline)
                
                result["success"] = True
                logger.info(f"成功获取 {stock_code} 的 {len(result['data'])} 条K线数据")
            
            return result
            
        except Exception as e:
            logger.error(f"解析K线数据时发生错误: {str(e)}")
            return {"stock_code": stock_code, "success": False, "data": [], "metadata": {}}    
    def _timestamp_to_datetime(self, timestamp: int) -> str:
        """将时间戳转换为可读的日期时间格式"""
        try:
            if timestamp > 1000000000000:  # 毫秒级时间戳
                dt = datetime.fromtimestamp(timestamp / 1000)
            else:  # 秒级时间戳
                dt = datetime.fromtimestamp(timestamp)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return str(timestamp)
    
    def get_multiple_kline_data(self, stock_codes: List[str], time_period: str = "day_1", 
                               **kwargs) -> Dict[str, Optional[Dict]]:
        """
        批量获取多个股票的K线数据
        
        Args:
            stock_codes: 股票代码列表
            time_period: 时间周期
            **kwargs: 其他参数
            
        Returns:
            字典，key为股票代码，value为对应的K线数据
        """
        results = {}
        
        for code in stock_codes:
            results[code] = self.get_kline_data(code, time_period, **kwargs)
            # 添加延时避免请求过于频繁
            time.sleep(0.5)
        
        return results


# 便捷函数
def get_stock_kline(stock_code: str, time_period: str = "day_1", **kwargs) -> Optional[Dict]:
    """
    便捷函数：获取个股K线数据
    
    Args:
        stock_code: 股票代码
        time_period: 时间周期
        **kwargs: 其他参数
        
    Returns:
        K线数据
    """
    api = THSKlineAPI()
    return api.get_kline_data(stock_code, time_period, **kwargs)


def get_multiple_stocks_kline(stock_codes: List[str], time_period: str = "day_1", **kwargs) -> Dict[str, Optional[Dict]]:
    """
    便捷函数：批量获取多个股票的K线数据
    
    Args:
        stock_codes: 股票代码列表
        time_period: 时间周期
        **kwargs: 其他参数
        
    Returns:
        多个股票的K线数据字典
    """
    api = THSKlineAPI()
    return api.get_multiple_kline_data(stock_codes, time_period, **kwargs)


if __name__ == "__main__":
    # 测试代码
    api = THSKlineAPI()
    
    # 测试获取单只股票的日K数据
    print("测试获取单只股票的日K数据:")
    result = api.get_kline_data("002187", "day_1")
    if result and result["success"]:
        print(f"股票代码: {result['stock_code']}")
        print(f"股票名称: {result['metadata'].get('stock_name', '未知')}")
        print(f"数据条数: {len(result['data'])}")
        if result['data']:
            print("最新K线数据:")
            latest = result['data'][0]
            for key, value in latest.items():
                print(f"  {key}: {value}")
    else:
        print("获取数据失败")
    
    print("\n" + "="*50 + "\n")
    
    # 测试获取多只股票的数据
    print("测试获取多只股票的数据:")
    stocks = ["002187", "000001", "600036"]
    results = api.get_multiple_kline_data(stocks, "day_1")
    
    for code, data in results.items():
        if data and data["success"]:
            print(f"{code}: 成功获取 {len(data['data'])} 条数据")
        else:
            print(f"{code}: 获取数据失败")