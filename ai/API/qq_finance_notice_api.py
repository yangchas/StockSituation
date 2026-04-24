"""
腾讯财经公司公告API封装
提供上市公司公告数据的获取功能
"""

import json
import requests
import time
from typing import Dict, List, Optional, Union
from datetime import datetime


class QQFinanceNoticeAPI:
    """腾讯财经公司公告API封装类"""
    
    def __init__(self):
        """初始化API配置"""
        self.base_url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/news/info/search"
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cookie': 'qq_domain_video_guid_verify=f3070608966b7700; _qimei_q32=5ab7f1e7287b046f56eb2d400a518309; _qimei_q36=841a5d3df8458964d199c3ca30001e817713; _qimei_h38=19c56ec8fc3f0334d680f79d02000009b1991a; pgv_pvid=1755080170; _qimei_i_2=45e85f8a915b01de9594aa66538076e8fee8a0a513520ad1b7dc795b2693206d346b36923f88e1afacb0; _qimei_i_1=41c153839c0955dd94c4f666088725e0f4eda0a5145303d4b3867c582493206c61633ec73980ebdd8784a4fa; _qimei_fingerprint=634953f821cbb95915b88ac3a1e27b85; _qimei_uuid42=1a10a000d2710068f880f7346e42ad444d351143b3; _qimei_i_3=78ff7a82c40855d2c7c3ad655a8420e6f2baa7f543590386e58a2d0b2f93206d63363f943c89e2ab90a4; tgw_l7_route=d5834d62cea889c783cc9d58b58c332e; RECENT_CODE=600519_1%7C00700_100',
            'Referer': 'https://gu.qq.com/',
            'Sec-Ch-Ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'script',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0'
        }
        
    def _generate_symbol(self, stock_code: str) -> str:
        """
        生成股票代码符号
        
        Args:
            stock_code: 股票代码，如600519
            
        Returns:
            str: 完整的股票符号，如sh600519或sz000001
        """
        if stock_code.startswith(('6', '9')):
            return f"sh{stock_code}"
        elif stock_code.startswith(('0', '3')):
            return f"sz{stock_code}"
        else:
            # 默认处理为沪市
            return f"sh{stock_code}"
    
    def _generate_timestamp(self) -> str:
        """生成时间戳参数"""
        return str(int(time.time() * 1000))
    
    def get_company_notices(self, 
                           stock_code: str, 
                           page: int = 1, 
                           page_size: int = 51, 
                           notice_type: int = 0,
                           var_name: str = "finance_notice") -> Dict:
        """
        获取公司公告数据
        
        Args:
            stock_code: 股票代码，如600519
            page: 页码，默认1
            page_size: 每页数据条数，默认51
            notice_type: 公告类型，0-全部，其他类型待探索
            var_name: JSONP回调函数名，默认finance_notice
            
        Returns:
            Dict: 公告数据，包含公告列表和分页信息
        """
        # 生成股票符号
        symbol = self._generate_symbol(stock_code)
        
        # 生成时间戳
        timestamp = self._generate_timestamp()
        
        # 构建请求参数
        params = {
            'page': page,
            'symbol': symbol,
            'n': page_size,
            '_var': var_name,
            'type': notice_type,
            '_appver': '1.0',
            '_': timestamp
        }
        
        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                # 腾讯API返回的是JSONP格式，需要提取JSON数据
                content = response.text
                
                # 提取JSON数据（去除JSONP包装）
                if content.startswith(f"{var_name}="):
                    json_str = content[len(var_name)+1:]
                    data = json.loads(json_str)
                    
                    if data.get('code') == 0:
                        return data.get('data', {})
                    else:
                        print(f"API返回错误: {data.get('msg', '未知错误')}")
                        return {}
                else:
                    print("响应格式异常，不是JSONP格式")
                    return {}
            else:
                print(f"请求失败，状态码: {response.status_code}")
                return {}
                
        except requests.exceptions.RequestException as e:
            print(f"网络请求异常: {e}")
            return {}
        except json.JSONDecodeError as e:
            print(f"JSON解析异常: {e}")
            return {}
    
    def get_notice_list(self, stock_code: str, page: int = 1, page_size: int = 20) -> List[Dict]:
        """
        获取公告列表（简化接口）
        
        Args:
            stock_code: 股票代码
            page: 页码
            page_size: 每页数量
            
        Returns:
            List[Dict]: 公告列表
        """
        data = self.get_company_notices(stock_code, page, page_size)
        return data.get('data', [])
    
    def get_recent_notices(self, stock_code: str, limit: int = 10) -> List[Dict]:
        """
        获取最近公告
        
        Args:
            stock_code: 股票代码
            limit: 限制数量
            
        Returns:
            List[Dict]: 最近公告列表
        """
        data = self.get_company_notices(stock_code, page_size=limit)
        notices = data.get('data', [])
        return notices[:limit]
    
    def get_multiple_stocks_notices(self, stock_codes: List[str], **kwargs) -> Dict[str, List[Dict]]:
        """
        批量获取多只股票的公告数据
        
        Args:
            stock_codes: 股票代码列表
            **kwargs: 其他参数
            
        Returns:
            Dict[str, List[Dict]]: 股票代码到公告数据的映射
        """
        result = {}
        for code in stock_codes:
            data = self.get_company_notices(code, **kwargs)
            result[code] = data.get('data', [])
        return result
    
    def search_notices_by_keyword(self, stock_code: str, keyword: str, page: int = 1) -> List[Dict]:
        """
        根据关键词搜索公告（需要扩展实现）
        
        Args:
            stock_code: 股票代码
            keyword: 搜索关键词
            page: 页码
            
        Returns:
            List[Dict]: 搜索结果列表
        """
        notices = self.get_notice_list(stock_code, page)
        
        # 简单的本地关键词过滤
        if keyword:
            keyword_lower = keyword.lower()
            filtered_notices = []
            for notice in notices:
                title = notice.get('title', '').lower()
                summary = notice.get('summary', '').lower()
                if keyword_lower in title or keyword_lower in summary:
                    filtered_notices.append(notice)
            return filtered_notices
        
        return notices


def test_qq_finance_notice_api():
    """测试腾讯财经公告API"""
    api = QQFinanceNoticeAPI()
    
    # 测试单只股票公告
    print("=== 测试单只股票公告 ===")
    notice_data = api.get_company_notices("600519", page=1, page_size=10)
    
    if notice_data:
        total_num = notice_data.get('total_num', 0)
        total_page = notice_data.get('total_page', 0)
        notices = notice_data.get('data', [])
        
        print(f"总公告数: {total_num}, 总页数: {total_page}")
        print(f"获取到 {len(notices)} 条公告")
        
        if notices:
            for i, notice in enumerate(notices[:3]):  # 只显示前3条
                print(f"\n第{i+1}条公告:")
                print(f"  标题: {notice.get('title', 'N/A')}")
                print(f"  时间: {notice.get('time', 'N/A')}")
                print(f"  类型: {notice.get('typeStr', 'N/A')}")
                print(f"  来源: {notice.get('src', 'N/A')}")
    else:
        print("获取公告数据失败")
    
    # 测试简化接口
    print("\n=== 测试简化接口 ===")
    notice_list = api.get_notice_list("600519", page_size=5)
    print(f"获取到 {len(notice_list)} 条公告列表")
    
    # 测试最近公告
    print("\n=== 测试最近公告 ===")
    recent_notices = api.get_recent_notices("600519", limit=3)
    print(f"获取到 {len(recent_notices)} 条最近公告")
    
    # 测试批量获取
    print("\n=== 测试批量获取 ===")
    stocks = ["600519", "000001", "300063"]
    multi_data = api.get_multiple_stocks_notices(stocks, page_size=2)
    for stock, data in multi_data.items():
        print(f"{stock}: {len(data)} 条公告")


if __name__ == "__main__":
    test_qq_finance_notice_api()