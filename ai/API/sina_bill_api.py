"""
新浪财经大单明细API封装
提供大单交易数据的获取功能

数据字段说明:
- symbol: 股票代码
- name: 股票名称
- ticktime: 成交时间
- price: 成交价格
- volume: 成交量（股）
- prev_price: 前一笔价格
- kind: 交易类型（D: 大单买入, U: 大单卖出）
"""

import json
import requests
from typing import Dict, List, Optional, Union
from datetime import datetime


class SinaBillAPI:
    """新浪大单明细API封装类"""
    
    def __init__(self):
        """初始化API配置"""
        self.base_url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList"
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'UOR=cn.bing.com,vip.stock.finance.sina.com.cn,; SINAGLOBAL=153.35.17.125_1768112682.290572; Apache=153.35.17.125_1768112682.290573; U_TRS1=0000007d.d39e1787.69634242.53880b89; U_TRS2=0000007d.d3a61787.69634242.b011aa52; FIN_ALL_VISITED=sh600879; ULV=1768112709062:2:2:2:153.35.17.125_1768112682.290573:1768112679074; FINA_V_S_2=sh600879; SR_SEL=1_511; SFA_version9.5.0=2026-01-11%2014%3A21; SFA_version9.5.0_click=1; rotatecount=6',
            'Priority': 'u=1, i',
            'Referer': 'https://vip.stock.finance.sina.com.cn/quotes_service/view/cn_bill.php?symbol=sh600879',
            'Sec-Ch-Ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0'
        }
        
    def _generate_symbol(self, stock_code: str) -> str:
        """
        生成股票代码符号
        
        Args:
            stock_code: 股票代码，如600879
            
        Returns:
            str: 完整的股票符号，如sh600879或sz000001
        """
        if stock_code.startswith(('6', '9')):
            return f"sh{stock_code}"
        elif stock_code.startswith(('0', '3')):
            return f"sz{stock_code}"
        else:
            # 默认处理为沪市
            return f"sh{stock_code}"
    
    def get_bill_list(self, 
                     stock_code: str, 
                     num: int = 60, 
                     page: int = 1, 
                     sort: str = "ticktime", 
                     asc: int = 0, 
                     volume: int = 0, 
                     amount: int = 500000, 
                     bill_type: int = 0, 
                     day: Optional[str] = None) -> List[Dict]:
        """
        获取大单明细数据
        
        Args:
            stock_code: 股票代码，如600879
            num: 每页数据条数，默认60
            page: 页码，默认1
            sort: 排序字段，默认ticktime（成交时间）
            asc: 排序方式，0降序1升序，默认0
            volume: 成交量筛选，默认0（不筛选）
            amount: 成交额筛选（单位：元），默认500000（50万）
            bill_type: 大单类型，0-全部，1-买入，2-卖出，默认0
            day: 查询日期，格式YYYY-MM-DD，默认当天
            
        Returns:
            List[Dict]: 大单明细数据列表
        """
        # 生成股票符号
        symbol = self._generate_symbol(stock_code)
        
        # 处理日期参数
        if day is None:
            day = datetime.now().strftime("%Y-%m-%d")
        
        # 构建请求参数
        params = {
            'symbol': symbol,
            'num': num,
            'page': page,
            'sort': sort,
            'asc': asc,
            'volume': volume,
            'amount': amount,
            'type': bill_type,
            'day': day
        }
        
        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                # 新浪API返回的是JSON格式，但编码为gbk
                response.encoding = 'gbk'
                data = response.json()
                return data
            else:
                print(f"请求失败，状态码: {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"网络请求异常: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"JSON解析异常: {e}")
            return []
    
    def get_today_bill_list(self, stock_code: str, **kwargs) -> List[Dict]:
        """
        获取当天大单明细数据（简化接口）
        
        Args:
            stock_code: 股票代码
            **kwargs: 其他参数，会传递给get_bill_list方法
            
        Returns:
            List[Dict]: 当天大单明细数据
        """
        return self.get_bill_list(stock_code, day=datetime.now().strftime("%Y-%m-%d"), **kwargs)
    
    def get_multiple_stocks_bill(self, stock_codes: List[str], **kwargs) -> Dict[str, List[Dict]]:
        """
        批量获取多只股票的大单明细数据
        
        Args:
            stock_codes: 股票代码列表
            **kwargs: 其他参数
            
        Returns:
            Dict[str, List[Dict]]: 股票代码到大单数据的映射
        """
        result = {}
        for code in stock_codes:
            result[code] = self.get_bill_list(code, **kwargs)
        return result


def test_sina_bill_api():
    """测试新浪大单明细API"""
    api = SinaBillAPI()
    
    # 测试单只股票
    print("=== 测试单只股票大单明细 ===")
    bill_data = api.get_bill_list("600879", num=10, page=1)
    print(f"获取到 {len(bill_data)} 条大单数据")
    if bill_data:
        for i, bill in enumerate(bill_data[:3]):  # 只显示前3条
            print(f"第{i+1}条: {bill}")
    
    # 测试当天数据
    print("\n=== 测试当天大单数据 ===")
    today_data = api.get_today_bill_list("600879", num=5)
    print(f"获取到 {len(today_data)} 条当天大单数据")
    
    # 测试批量获取
    print("\n=== 测试批量获取 ===")
    stocks = ["600879", "000001", "300063"]
    multi_data = api.get_multiple_stocks_bill(stocks, num=3)
    for stock, data in multi_data.items():
        print(f"{stock}: {len(data)} 条数据")


if __name__ == "__main__":
    test_sina_bill_api()