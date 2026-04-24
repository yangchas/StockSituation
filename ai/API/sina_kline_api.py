"""
新浪财经K线数据API封装
支持获取不同周期的个股历史K线数据

API接口: https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData

参数说明:
- symbol: 股票代码（如sz000001, sh600519）
- scale: K线周期（1, 5, 15, 30, 60, 120, 240, 1440等）
- ma: 移动平均线参数
- datalen: 数据条数

返回数据格式:
[
    {
        "day": "2025-12-09 13:50:00",
        "open": "11.480",
        "high": "11.490", 
        "low": "11.470",
        "close": "11.470",
        "volume": "2042891",
        "ma_price5": 11.486,
        "ma_volume5": 1093752
    },
    ...
]
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union


class SinaKLineAPI:
    """新浪财经K线数据API封装类"""
    
    BASE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    
    # K线周期映射
    SCALE_MAPPING = {
        '1min': 1,      # 1分钟线
        '5min': 5,      # 5分钟线
        '15min': 15,    # 15分钟线
        '30min': 30,    # 30分钟线
        '60min': 60,    # 60分钟线
        '120min': 120,  # 120分钟线
        'day': 240,     # 日线
        'week': 1440,   # 周线
        'month': 4320   # 月线
    }
    
    def __init__(self, timeout: int = 30):
        """
        初始化API
        
        Args:
            timeout: 请求超时时间（秒）
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def _build_symbol(self, stock_code: str) -> str:
        """
        构建完整的股票代码
        
        Args:
            stock_code: 股票代码（如000001, 600519）
            
        Returns:
            str: 完整的股票代码（如sz000001, sh600519）
        """
        if stock_code.startswith(('sh', 'sz')):
            return stock_code
        
        if stock_code.startswith('6'):
            return f"sh{stock_code}"
        elif stock_code.startswith(('0', '3')):
            return f"sz{stock_code}"
        else:
            raise ValueError(f"无法识别的股票代码格式: {stock_code}")
    
    def get_kline_data(self, 
                      stock_code: str, 
                      scale: Union[str, int] = '5min',
                      ma: int = 5,
                      datalen: int = 1023) -> List[Dict]:
        """
        获取K线数据
        
        Args:
            stock_code: 股票代码
            scale: K线周期（支持字符串或数字）
            ma: 移动平均线参数
            datalen: 数据条数
            
        Returns:
            List[Dict]: K线数据列表
        """
        # 处理scale参数
        if isinstance(scale, str):
            if scale not in self.SCALE_MAPPING:
                raise ValueError(f"不支持的K线周期: {scale}，支持的周期: {list(self.SCALE_MAPPING.keys())}")
            scale_value = self.SCALE_MAPPING[scale]
        else:
            scale_value = scale
        
        # 构建完整股票代码
        full_symbol = self._build_symbol(stock_code)
        
        # 构建请求参数
        params = {
            'symbol': full_symbol,
            'scale': scale_value,
            'ma': ma,
            'datalen': datalen
        }
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            # 解析JSON响应
            data = response.json()
            
            # 数据清洗和格式化
            cleaned_data = self._clean_kline_data(data)
            
            return cleaned_data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"API请求失败: {e}")
        except json.JSONDecodeError as e:
            raise Exception(f"JSON解析失败: {e}")
    
    def _clean_kline_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        清洗和格式化K线数据
        
        Args:
            raw_data: 原始数据
            
        Returns:
            List[Dict]: 清洗后的数据
        """
        cleaned_data = []
        
        for item in raw_data:
            cleaned_item = {
                'datetime': item.get('day', ''),
                'open': float(item.get('open', 0)),
                'high': float(item.get('high', 0)),
                'low': float(item.get('low', 0)),
                'close': float(item.get('close', 0)),
                'volume': int(item.get('volume', 0)),
                'ma_price5': float(item.get('ma_price5', 0)),
                'ma_volume5': int(item.get('ma_volume5', 0))
            }
            cleaned_data.append(cleaned_item)
        
        return cleaned_data
    
    def to_dataframe(self, kline_data: List[Dict]) -> pd.DataFrame:
        """
        将K线数据转换为pandas DataFrame
        
        Args:
            kline_data: K线数据列表
            
        Returns:
            pd.DataFrame: 格式化后的DataFrame
        """
        if not kline_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(kline_data)
        
        # 转换时间列
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime').sort_index()
        
        return df
    
    def get_multiple_stocks(self, 
                           stock_codes: List[str], 
                           scale: Union[str, int] = '5min',
                           ma: int = 5,
                           datalen: int = 100) -> Dict[str, List[Dict]]:
        """
        批量获取多只股票的K线数据
        
        Args:
            stock_codes: 股票代码列表
            scale: K线周期
            ma: 移动平均线参数
            datalen: 数据条数
            
        Returns:
            Dict[str, List[Dict]]: 每只股票的K线数据
        """
        results = {}
        
        for stock_code in stock_codes:
            try:
                data = self.get_kline_data(stock_code, scale, ma, datalen)
                results[stock_code] = data
            except Exception as e:
                print(f"获取股票 {stock_code} 数据失败: {e}")
                results[stock_code] = []
        
        return results
    
    def get_recent_data(self, 
                       stock_code: str, 
                       scale: Union[str, int] = 'day',
                       days: int = 30) -> List[Dict]:
        """
        获取最近N天的K线数据
        
        Args:
            stock_code: 股票代码
            scale: K线周期
            days: 天数
            
        Returns:
            List[Dict]: K线数据列表
        """
        # 根据天数估算数据条数
        if scale in ['day', 240]:
            datalen = min(days, 1023)
        elif scale in ['week', 1440]:
            datalen = min(days // 7 + 1, 1023)
        elif scale in ['month', 4320]:
            datalen = min(days // 30 + 1, 1023)
        else:
            # 对于分钟线，估算条数
            datalen = min(days * 240, 1023)  # 最大1023条
        
        # 确保至少获取1条数据
        datalen = max(datalen, 1)
        
        return self.get_kline_data(stock_code, scale, 5, datalen)


def main():
    """使用示例"""
    api = SinaKLineAPI()
    
    # 示例1: 获取平安银行5分钟K线数据
    print("=== 示例1: 获取平安银行5分钟K线数据 ===")
    try:
        data = api.get_kline_data('000001', '5min', 5, 10)
        print(f"获取到 {len(data)} 条数据")
        for i, item in enumerate(data[:3]):  # 显示前3条
            print(f"第{i+1}条: {item}")
    except Exception as e:
        print(f"错误: {e}")
    
    print("\n=== 示例2: 转换为DataFrame ===")
    try:
        data = api.get_kline_data('600519', 'day', 5, 5)  # 贵州茅台日线数据
        df = api.to_dataframe(data)
        print(df.head())
    except Exception as e:
        print(f"错误: {e}")
    
    print("\n=== 示例3: 批量获取多只股票数据 ===")
    try:
        stocks = ['000001', '600519', '300063']
        results = api.get_multiple_stocks(stocks, '5min', 5, 5)
        for stock, data in results.items():
            print(f"{stock}: {len(data)} 条数据")
    except Exception as e:
        print(f"错误: {e}")


if __name__ == "__main__":
    main()