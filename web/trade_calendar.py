import time
import requests
import json
from datetime import datetime, timedelta
import bisect

class TradeCalendar:
    def __init__(self, cache_expire_seconds=300):
        """初始化交易日历
        Args:
            cache_expire_seconds: 缓存过期时间（秒），默认5分钟
        """
        self.cache_expire_seconds = cache_expire_seconds
        self._month_cache = {}  # 缓存格式: {month_str: (timestamp, trade_days_list)}
    
    def _get_cached_month_data(self, month):
        """获取缓存的月份数据，如果过期或不存在则返回None"""
        if month in self._month_cache:
            cache_time, data = self._month_cache[month]
            if time.time() - cache_time < self.cache_expire_seconds:
                return data
        return None
    
    def _set_month_cache(self, month, data):
        """设置月份缓存"""
        self._month_cache[month] = (time.time(), data)
    
    def get_trade_days_of_month(self, month=None):
        """获取指定月份的所有交易日
        Args:
            month: 月份字符串，格式为 'YYYY-MM'，默认为当前月
        Returns:
            交易日列表，格式为 ['YYYY-MM-DD', ...]
        """
        if month is None:
            month = time.strftime("%Y-%m", time.localtime())
        
        # 检查缓存
        cached_data = self._get_cached_month_data(month)
        if cached_data is not None:
            return cached_data.copy()
        
        # 请求数据
        url = f'https://www.szse.cn/api/report/exchange/onepersistenthour/monthList?month={month}&random={time.time()}'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = json.loads(response.text)['data']
            
            # 提取交易日
            trade_days = [item["jyrq"] for item in data if item.get('jybz') == '1']
            
            # 缓存数据
            self._set_month_cache(month, trade_days)
            return trade_days.copy()
        except Exception as e:
            print(f"获取交易日失败: {e}")
            return []
    
    def is_trade_day(self, date_input=None):
        """判断指定日期是否为交易日
        Args:
            date_input: 可以是以下格式之一:
                - None: 默认为今天
                - timestamp: 时间戳
                - string: 'YYYY-MM-DD' 或 'MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
        Returns:
            bool: 是否为交易日
        """
        # 解析日期
        if date_input is None:
            date_obj = datetime.now()
        elif isinstance(date_input, (int, float)):
            date_obj = datetime.fromtimestamp(date_input)
        elif isinstance(date_input, str):
            try:
                # 尝试多种格式
                if ' ' in date_input:
                    date_str = date_input.split(' ')[0]
                else:
                    date_str = date_input
                
                parts = date_str.split('-')
                if len(parts) == 2:  # MM-DD
                    year = datetime.now().year
                    month = parts[0].zfill(2)
                    day = parts[1].zfill(2)
                    date_str = f"{year}-{month}-{day}"
                elif len(parts) == 3:  # YYYY-MM-DD
                    month = parts[1].zfill(2)
                    day = parts[2].zfill(2)
                    date_str = f"{parts[0]}-{month}-{day}"
                else:
                    return False
                
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            except Exception as e:
                print(f"日期格式解析失败: {date_input}, 错误: {e}")
                return False
        else:
            print(f"不支持的日期格式: {date_input}")
            return False
        
        # 获取对应月份的交易日
        month_str = date_obj.strftime('%Y-%m')
        trade_days = self.get_trade_days_of_month(month_str)
        
        # 检查是否为交易日
        date_str = date_obj.strftime('%Y-%m-%d')
        return date_str in trade_days
    
    def get_previous_trade_day(self, date_input=None, max_lookback=2):
        """获取上一个交易日
        Args:
            date_input: 参考日期，格式同is_trade_day，默认为今天
            max_lookback: 最大回溯月份数，默认2个月
        Returns:
            str: 上一个交易日的日期字符串 'YYYY-MM-DD'，如果找不到返回None
        """
        # 解析参考日期
        if date_input is None:
            ref_date = datetime.now()
        elif isinstance(date_input, (int, float)):
            ref_date = datetime.fromtimestamp(date_input)
        elif isinstance(date_input, str):
            # 使用与is_trade_day相同的解析逻辑
            if ' ' in date_input:
                date_str = date_input.split(' ')[0]
            else:
                date_str = date_input
            
            parts = date_str.split('-')
            if len(parts) == 2:  # MM-DD
                year = datetime.now().year
                month = parts[0].zfill(2)
                day = parts[1].zfill(2)
                date_str = f"{year}-{month}-{day}"
            elif len(parts) == 3:  # YYYY-MM-DD
                month = parts[1].zfill(2)
                day = parts[2].zfill(2)
                date_str = f"{parts[0]}-{month}-{day}"
            else:
                return None
            
            ref_date = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            return None
        
        ref_date_str = ref_date.strftime('%Y-%m-%d')
        
        # 收集最多max_lookback个月的交易日数据
        all_trade_days = []
        
        for i in range(max_lookback):
            # 计算要查询的月份
            month_date = ref_date - timedelta(days=30*i)
            month_str = month_date.strftime('%Y-%m')
            
            # 获取该月交易日
            month_trade_days = self.get_trade_days_of_month(month_str)
            
            if month_trade_days:
                all_trade_days.extend(month_trade_days)
            
            # 如果已经收集到足够数据（参考日期之前的交易日），可以提前停止
            if all_trade_days and all_trade_days[0] < ref_date_str:
                # 排序并去重
                all_trade_days = sorted(set(all_trade_days))
                
                # 使用二分查找找到上一个交易日
                pos = bisect.bisect_left(all_trade_days, ref_date_str)
                if pos > 0:
                    return all_trade_days[pos - 1]
        
        # 如果循环结束还没找到，尝试排序后查找
        if all_trade_days:
            all_trade_days = sorted(set(all_trade_days))
            pos = bisect.bisect_left(all_trade_days, ref_date_str)
            if pos > 0:
                return all_trade_days[pos - 1]
        
        return None
    
    def get_next_trade_day(self, date_input=None, max_lookahead=2):
        """获取下一个交易日
        Args:
            date_input: 参考日期，格式同is_trade_day，默认为今天
            max_lookahead: 最大向前查找月份数，默认2个月
        Returns:
            str: 下一个交易日的日期字符串 'YYYY-MM-DD'，如果找不到返回None
        """
        # 解析参考日期（与get_previous_trade_day相同）
        if date_input is None:
            ref_date = datetime.now()
        elif isinstance(date_input, (int, float)):
            ref_date = datetime.fromtimestamp(date_input)
        elif isinstance(date_input, str):
            if ' ' in date_input:
                date_str = date_input.split(' ')[0]
            else:
                date_str = date_input
            
            parts = date_str.split('-')
            if len(parts) == 2:  # MM-DD
                year = datetime.now().year
                month = parts[0].zfill(2)
                day = parts[1].zfill(2)
                date_str = f"{year}-{month}-{day}"
            elif len(parts) == 3:  # YYYY-MM-DD
                month = parts[1].zfill(2)
                day = parts[2].zfill(2)
                date_str = f"{parts[0]}-{month}-{day}"
            else:
                return None
            
            ref_date = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            return None
        
        ref_date_str = ref_date.strftime('%Y-%m-%d')
        
        # 收集最多max_lookahead个月的交易日数据
        all_trade_days = []
        
        for i in range(max_lookahead + 1):  # +1 包括当前月
            # 计算要查询的月份
            month_date = ref_date + timedelta(days=30*i)
            month_str = month_date.strftime('%Y-%m')
            
            # 获取该月交易日
            month_trade_days = self.get_trade_days_of_month(month_str)
            
            if month_trade_days:
                all_trade_days.extend(month_trade_days)
        
        # 排序并去重
        if all_trade_days:
            all_trade_days = sorted(set(all_trade_days))
            
            # 使用二分查找找到下一个交易日
            pos = bisect.bisect_right(all_trade_days, ref_date_str)
            if pos < len(all_trade_days):
                return all_trade_days[pos]
        
        return None
    
    def clear_cache(self):
        """清空缓存"""
        self._month_cache.clear()
    
    def get_trade_days_between(self, start_date, end_date):
        """获取两个日期之间的所有交易日
        Args:
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
        Returns:
            list: 交易日列表
        """
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_trade_days = []
            
            # 获取start到end之间的所有月份的交易日
            current = start.replace(day=1)
            while current <= end:
                month_str = current.strftime('%Y-%m')
                month_trade_days = self.get_trade_days_of_month(month_str)
                
                # 过滤出在[start_date, end_date]范围内的交易日
                for day in month_trade_days:
                    if start_date <= day <= end_date:
                        all_trade_days.append(day)
                
                # 下一个月
                if current.month == 12:
                    current = current.replace(year=current.year+1, month=1)
                else:
                    current = current.replace(month=current.month+1)
            
            return sorted(all_trade_days)
        except Exception as e:
            print(f"获取日期区间交易日失败: {e}")
            return []
