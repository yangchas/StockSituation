"""
东方财富个股K线数据获取API封装
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


class EastMoneyKlineAPI:
    """东方财富K线数据API封装类"""
    
    def __init__(self):
        self.base_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        self.session = requests.Session()
        self._setup_headers()
    
    def _setup_headers(self):
        """设置请求头"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://quote.eastmoney.com/',
            'Sec-Fetch-Dest': 'script',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'same-site'
        }
    
    def _get_secid(self, stock_code: str) -> str:
        """根据股票代码获取secid格式"""
        if stock_code.startswith(('0', '3')):
            return f"0.{stock_code}"  # 深市
        elif stock_code.startswith(('6', '9')):
            return f"1.{stock_code}"  # 沪市
        else:
            return f"0.{stock_code}"  # 默认深市
    
    def _parse_time_period(self, time_period: str) -> Dict:
        """解析时间周期参数"""
        period_mapping = {
            "day_1": "101",      # 日K
            "week_1": "102",     # 周K
            "month_1": "103",    # 月K
            "minute_1": "1",     # 1分钟K
            "minute_5": "5",     # 5分钟K
            "minute_15": "15",   # 15分钟K
            "minute_30": "30",   # 30分钟K
            "minute_60": "60",   # 60分钟K
        }
        
        klt = period_mapping.get(time_period, "101")  # 默认日K
        
        # 设置数据条数限制
        if time_period in ["day_1", "week_1", "month_1"]:
            lmt = "1000"  # 日周月K线获取更多数据
        else:
            lmt = "800"   # 分钟K线数据较多
            
        return {"klt": klt, "lmt": lmt}
    
    def _parse_adjust_type(self, adjust_type: str) -> str:
        """解析复权类型"""
        adjust_mapping = {
            "forward": "1",   # 前复权
            "backward": "2",  # 后复权
            "none": "0"       # 不复权
        }
        return adjust_mapping.get(adjust_type, "1")  # 默认前复权
    
    def _build_request_params(self, stock_code: str, time_period: str = "day_1", 
                             adjust_type: str = "forward", limit: int = 120) -> Dict:
        """构建请求参数"""
        
        # 获取secid
        secid = self._get_secid(stock_code)
        
        # 解析时间周期
        period_info = self._parse_time_period(time_period)
        
        # 解析复权类型
        fqt = self._parse_adjust_type(adjust_type)
        
        # 构建参数
        params = {
            'cb': f'jQuery{int(time.time()*1000)}_{int(time.time()*1000)}',
            'secid': secid,
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
            'klt': period_info["klt"],
            'fqt': fqt,
            'end': '20500101',  # 结束日期设为未来，获取最新数据
            'lmt': str(limit),
            '_': str(int(time.time() * 1000))
        }
        
        return params
    
    def get_kline_data(self, stock_code: str, time_period: str = "day_1", 
                      adjust_type: str = "forward", limit: int = 120) -> Optional[Dict]:
        """
        获取个股K线数据
        
        Args:
            stock_code: 股票代码，如 "300063"
            time_period: 时间周期，可选值：
                - "day_1": 日K
                - "week_1": 周K  
                - "month_1": 月K
                - "minute_1": 1分钟K
                - "minute_5": 5分钟K
                - "minute_15": 15分钟K
                - "minute_30": 30分钟K
                - "minute_60": 60分钟K
            adjust_type: 复权类型，"forward"前复权，"backward"后复权，"none"不复权
            limit: 数据条数限制
            
        Returns:
            K线数据字典，包含时间、开盘价、最高价、最低价、收盘价、成交量等信息
        """
        try:
            # 构建请求参数
            params = self._build_request_params(stock_code, time_period, adjust_type, limit)
            
            logger.info(f"获取股票 {stock_code} 的 {time_period} K线数据，限制 {limit} 条")
            
            # 发送请求
            response = self.session.get(
                self.base_url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                # 解析JSONP响应
                data = self._parse_jsonp_response(response.text)
                
                if data and data.get("rc") == 0:
                    return self._parse_kline_data(data, stock_code)
                else:
                    error_msg = data.get("rt", "未知错误") if data else "响应解析失败"
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
    
    def _parse_jsonp_response(self, response_text: str) -> Optional[Dict]:
        """解析JSONP响应"""
        try:
            # 提取JSON数据（去除JSONP包装）
            start = response_text.find('(')
            end = response_text.rfind(')')
            
            if start != -1 and end != -1:
                json_str = response_text[start + 1:end]
                return json.loads(json_str)
            else:
                logger.error("JSONP响应格式错误")
                return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误: {str(e)}")
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
            
            # 提取股票基本信息
            stock_data = data.get("data", {})
            
            # 提取元数据
            result["metadata"] = {
                "code": stock_data.get("code"),
                "name": stock_data.get("name"),
                "market": stock_data.get("market"),
                "decimal": stock_data.get("decimal"),
                "dktotal": stock_data.get("dktotal"),
                "preKPrice": stock_data.get("preKPrice")
            }
            
            # 解析K线数据
            klines = stock_data.get("klines", [])
            for kline_str in klines:
                kline_parts = kline_str.split(',')
                
                if len(kline_parts) >= 11:
                    parsed_kline = {
                        "date": kline_parts[0],
                        "open": float(kline_parts[1]) if kline_parts[1] else None,
                        "close": float(kline_parts[2]) if kline_parts[2] else None,
                        "high": float(kline_parts[3]) if kline_parts[3] else None,
                        "low": float(kline_parts[4]) if kline_parts[4] else None,
                        "volume": int(kline_parts[5]) if kline_parts[5] else None,
                        "amount": float(kline_parts[6]) if kline_parts[6] else None,
                        "amplitude": float(kline_parts[7]) if kline_parts[7] else None,
                        "change_rate": float(kline_parts[8]) if kline_parts[8] else None,
                        "change_amount": float(kline_parts[9]) if kline_parts[9] else None,
                        "turnover_rate": float(kline_parts[10]) if kline_parts[10] else None
                    }
                    result["data"].append(parsed_kline)
            
            result["success"] = True
            logger.info(f"成功获取 {stock_code} 的 {len(result['data'])} 条K线数据")
            
            return result
            
        except Exception as e:
            logger.error(f"解析K线数据时发生错误: {str(e)}")
            return {"stock_code": stock_code, "success": False, "data": [], "metadata": {}}
    
    def get_multiple_kline_data(self, stock_codes: List[str], time_period: str = "day_1", 
                               adjust_type: str = "forward", limit: int = 120) -> Dict[str, Dict]:
        """
        批量获取多个股票的K线数据
        
        Args:
            stock_codes: 股票代码列表
            time_period: 时间周期
            adjust_type: 复权类型
            limit: 数据条数限制
            
        Returns:
            股票代码到K线数据的映射字典
        """
        results = {}
        
        for stock_code in stock_codes:
            data = self.get_kline_data(stock_code, time_period, adjust_type, limit)
            results[stock_code] = data
            
            # 避免请求过于频繁
            time.sleep(0.1)
        
        return results


def test_eastmoney_api():
    """测试东方财富API"""
    api = EastMoneyKlineAPI()
    
    # 测试股票代码
    test_stocks = ["300063", "000001", "600036"]
    
    print("=== 东方财富K线API测试 ===\n")
    
    for stock_code in test_stocks:
        print(f"\n--- 测试股票 {stock_code} ---")
        
        # 测试日K数据
        result = api.get_kline_data(stock_code, "day_1", "forward", 10)
        
        if result and result["success"]:
            print(f"✅ 成功获取 {len(result['data'])} 条数据")
            print(f"股票名称: {result['metadata'].get('name', '未知')}")
            
            # 显示前3条数据
            for i, kline in enumerate(result["data"][:3]):
                print(f"  第{i+1}条: {kline['date']} - 开:{kline['open']} 高:{kline['high']} 低:{kline['low']} 收:{kline['close']}")
        else:
            print("❌ 获取数据失败")


if __name__ == "__main__":
    test_eastmoney_api()