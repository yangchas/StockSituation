"""
腾讯财经主力资金流向API封装
提供获取股票主力资金流向数据的完整功能
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union


class QQFinanceFundFlowAPI:
    """腾讯财经主力资金流向API封装类"""
    
    def __init__(self):
        """初始化API"""
        self.base_url = "https://proxy.finance.qq.com/cgi/cgi-bin/fundflow/hsfundtab"
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "proxy.finance.qq.com",
            "Origin": "https://gu.qq.com",
            "Referer": "https://gu.qq.com/",
            "Sec-Ch-Ua": '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
        }
    
    def _generate_stock_code(self, symbol: str) -> str:
        """
        生成标准股票代码格式
        
        Args:
            symbol: 股票代码 (如: 600519, 000001, 300063)
            
        Returns:
            str: 标准格式的股票代码 (如: sh600519, sz000001)
        """
        if symbol.startswith(('sh', 'sz')):
            return symbol
        
        # 判断市场
        if symbol.startswith(('6', '9')):
            return f"sh{symbol}"
        elif symbol.startswith(('0', '2', '3')):
            return f"sz{symbol}"
        else:
            return symbol
    
    def _make_request(self, params: Dict) -> Optional[Dict]:
        """
        发送API请求
        
        Args:
            params: 请求参数
            
        Returns:
            Optional[Dict]: API响应数据
        """
        try:
            response = requests.get(
                self.base_url, 
                params=params, 
                headers=self.headers, 
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"API请求失败: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"网络请求异常: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON解析异常: {e}")
            return None
    
    def get_fund_flow_data(self, symbol: str, flow_types: str = None, 
                          kline_days: int = 20) -> Optional[Dict]:
        """
        获取主力资金流向数据
        
        Args:
            symbol: 股票代码
            flow_types: 资金流向类型 (默认: 历史+五日+今日趋势+今日流向)
            kline_days: K线需要天数
            
        Returns:
            Optional[Dict]: 资金流向数据
        """
        if flow_types is None:
            flow_types = "historyFundFlow,fiveDayFundFlow,todayFundTrend,todayFundFlow"
        
        stock_code = self._generate_stock_code(symbol)
        
        params = {
            "code": stock_code,
            "type": flow_types,
            "klineNeedDay": str(kline_days)
        }
        
        return self._make_request(params)
    
    def get_today_fund_flow(self, symbol: str) -> Optional[Dict]:
        """
        获取今日资金流向数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[Dict]: 今日资金流向数据
        """
        data = self.get_fund_flow_data(symbol, "todayFundFlow", 1)
        
        if data and 'data' in data and 'todayFundFlow' in data['data']:
            return data['data']['todayFundFlow']
        
        return None
    
    def get_five_day_fund_flow(self, symbol: str) -> Optional[List[Dict]]:
        """
        获取五日资金流向数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[List[Dict]]: 五日资金流向数据列表
        """
        data = self.get_fund_flow_data(symbol, "fiveDayFundFlow", 5)
        
        if data and 'data' in data and 'fiveDayFundFlow' in data['data']:
            return data['data']['fiveDayFundFlow']
        
        return None
    
    def get_history_fund_flow(self, symbol: str, days: int = 20) -> Optional[List[Dict]]:
        """
        获取历史资金流向数据
        
        Args:
            symbol: 股票代码
            days: 历史天数
            
        Returns:
            Optional[List[Dict]]: 历史资金流向数据列表
        """
        data = self.get_fund_flow_data(symbol, "historyFundFlow", days)
        
        if data and 'data' in data and 'historyFundFlow' in data['data']:
            return data['data']['historyFundFlow']
        
        return None
    
    def get_today_fund_trend(self, symbol: str) -> Optional[Dict]:
        """
        获取今日资金趋势数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[Dict]: 今日资金趋势数据
        """
        data = self.get_fund_flow_data(symbol, "todayFundTrend", 1)
        
        if data and 'data' in data and 'todayFundTrend' in data['data']:
            return data['data']['todayFundTrend']
        
        return None
    
    def get_complete_fund_flow(self, symbol: str) -> Optional[Dict]:
        """
        获取完整的资金流向数据（包含所有类型）
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[Dict]: 完整的资金流向数据
        """
        data = self.get_fund_flow_data(symbol)
        
        if data and 'data' in data:
            return data['data']
        
        return None
    
    def get_multiple_stocks_fund_flow(self, symbols: List[str], 
                                     flow_type: str = "todayFundFlow") -> Dict[str, Optional[Dict]]:
        """
        批量获取多只股票的资金流向数据
        
        Args:
            symbols: 股票代码列表
            flow_type: 资金流向类型
            
        Returns:
            Dict[str, Optional[Dict]]: 各股票的资金流向数据
        """
        results = {}
        
        for symbol in symbols:
            if flow_type == "todayFundFlow":
                results[symbol] = self.get_today_fund_flow(symbol)
            elif flow_type == "fiveDayFundFlow":
                results[symbol] = self.get_five_day_fund_flow(symbol)
            elif flow_type == "historyFundFlow":
                results[symbol] = self.get_history_fund_flow(symbol)
            elif flow_type == "todayFundTrend":
                results[symbol] = self.get_today_fund_trend(symbol)
            else:
                results[symbol] = self.get_fund_flow_data(symbol, flow_type)
        
        return results
    
    def analyze_today_fund_flow(self, symbol: str) -> Optional[Dict]:
        """
        分析今日资金流向
        
        Args:
            symbol: 股票代码
            
        Returns:
            Optional[Dict]: 分析结果
        """
        today_data = self.get_today_fund_flow(symbol)
        
        if not today_data:
            return None
        
        analysis = {
            'symbol': symbol,
            'main_net_in': int(today_data.get('mainNetIn', 0)),  # 主力净流入
            'main_in': int(today_data.get('mainIn', 0)),        # 主力流入
            'main_out': int(today_data.get('mainOut', 0)),      # 主力流出
            'retail_in': int(today_data.get('retailIn', 0)),    # 散户流入
            'retail_out': int(today_data.get('retailOut', 0)),  # 散户流出
            'super_flow': int(today_data.get('superFlow', 0)),  # 超大单流向
            'big_flow': int(today_data.get('bigFlow', 0)),      # 大单流向
            'normal_flow': int(today_data.get('normalFlow', 0)), # 中单流向
            'small_flow': int(today_data.get('smallFlow', 0)),   # 小单流向
            'summary': today_data.get('summary', {}),           # 总结信息
            'rank': today_data.get('rank', ''),                 # 排名
            'desc': today_data.get('desc', '')                  # 描述
        }
        
        # 计算流入流出比例
        total_in = analysis['main_in'] + analysis['retail_in']
        total_out = analysis['main_out'] + analysis['retail_out']
        
        if total_in + total_out > 0:
            analysis['main_in_ratio'] = analysis['main_in'] / (total_in + total_out) * 100
            analysis['main_out_ratio'] = analysis['main_out'] / (total_in + total_out) * 100
        else:
            analysis['main_in_ratio'] = 0
            analysis['main_out_ratio'] = 0
        
        return analysis


def test_api():
    """测试API功能"""
    api = QQFinanceFundFlowAPI()
    
    print("=== 腾讯财经主力资金流向API测试 ===")
    
    # 测试单只股票今日资金流向
    print("\n1. 测试单只股票今日资金流向")
    today_data = api.get_today_fund_flow("600519")
    
    if today_data:
        print(f"获取到今日资金流向数据:")
        print(f"  股票代码: {today_data.get('stockCode', 'N/A')}")
        print(f"  主力净流入: {today_data.get('mainNetIn', 'N/A')}")
        print(f"  主力流入: {today_data.get('mainIn', 'N/A')}")
        print(f"  主力流出: {today_data.get('mainOut', 'N/A')}")
        print(f"  排名: {today_data.get('rank', 'N/A')}")
    else:
        print("获取今日资金流向数据失败")
    
    # 测试分析功能
    print("\n2. 测试资金流向分析")
    analysis = api.analyze_today_fund_flow("600519")
    
    if analysis:
        print(f"分析结果:")
        print(f"  主力净流入: {analysis['main_net_in']:,} 元")
        print(f"  主力流入占比: {analysis['main_in_ratio']:.2f}%")
        print(f"  主力流出占比: {analysis['main_out_ratio']:.2f}%")
        if analysis['summary']:
            print(f"  总结: {analysis['summary'].get('s0', 'N/A')}")
    
    # 测试批量获取
    print("\n3. 测试批量获取")
    stocks = ["600519", "000001", "300063"]
    multi_data = api.get_multiple_stocks_fund_flow(stocks)
    
    for symbol, data in multi_data.items():
        if data:
            print(f"  {symbol}: 主力净流入 {data.get('mainNetIn', 'N/A')}")
        else:
            print(f"  {symbol}: 获取失败")
    
    # 测试完整数据获取
    print("\n4. 测试完整数据获取")
    complete_data = api.get_complete_fund_flow("600519")
    
    if complete_data:
        print("获取到完整资金流向数据，包含以下类型:")
        for key in complete_data.keys():
            data_type = complete_data[key]
            if isinstance(data_type, list):
                print(f"  {key}: {len(data_type)} 条数据")
            else:
                print(f"  {key}: 数据对象")


if __name__ == "__main__":
    test_api()