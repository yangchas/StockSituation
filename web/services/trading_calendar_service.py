import logging
import holidays
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger(__name__)

class TradingCalendarService:
    """交易日历服务"""

    """交易日历服务"""
    
    def __init__(self):
        self.cache = {}
        self.cn_holidays = holidays.CN()  # 中国公共假期
        self._init_trading_calendar()
    
    def _init_trading_calendar(self):
        """初始化交易日历"""
        # 中国股市交易时间：周一至周五 9:30-11:30, 13:00-15:00
        # 不交易的时间：周六、周日、法定节假日
        self.trading_hours = {
            'morning_start': '09:30:00',
            'morning_end': '11:30:00',
            'afternoon_start': '13:00:00',
            'afternoon_end': '15:00:00'
        }
    
    def is_trading_day(self, date_str: str) -> bool:
        """判断是否为交易日"""
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            # 检查是否是周末
            if date_obj.weekday() >= 5:  # 5=周六, 6=周日
                return False
            
            # 检查是否是法定节假日
            if date_obj in self.cn_holidays:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 判断交易日失败 {date_str}: {e}")
            return False
            
    def is_trade_day(self, date_str: str) -> bool:
        """Alias for is_trading_day"""
        return self.is_trading_day(date_str)

    def get_previous_trade_day(self, date_str: str) -> str:
        # return "2026-04-09"
        """获取上一个交易日"""
        try:
            current_date = datetime.strptime(date_str, '%Y-%m-%d')
            for _ in range(30): # Safety limit
                current_date -= timedelta(days=1)
                date_s = current_date.strftime('%Y-%m-%d')
                if self.is_trading_day(date_s):
                    return date_s
            return date_str # Fallback
        except Exception as e:
            logger.error(f"❌ 获取上个交易日失败 {date_str}: {e}")
            return date_str
    
    def get_previous_trading_day(self, date_str: str = None) -> str:
        """获取前一个交易日"""
        # return "2026-04-09"
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        for i in range(1, 31):
            prev_date = current_date - timedelta(days=i)
            prev_date_str = prev_date.strftime('%Y-%m-%d')
            
            if self.is_trading_day(prev_date_str):
                return prev_date_str
        
        return (current_date - timedelta(days=30)).strftime('%Y-%m-%d')
    
    def get_next_trading_day(self, date_str: str = None) -> str:
        """获取下一个交易日"""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        current_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        for i in range(1, 31):
            next_date = current_date + timedelta(days=i)
            next_date_str = next_date.strftime('%Y-%m-%d')
            
            if self.is_trading_day(next_date_str):
                return next_date_str
        
        return (current_date + timedelta(days=30)).strftime('%Y-%m-%d')
    
    def get_next_trade_day(self, date_str: str = None) -> str:
        """Alias for get_next_trading_day"""
        return self.get_next_trading_day(date_str)
    
    def get_recent_trading_days(self, days: int = 30) -> List[str]:
        """获取最近N个交易日"""
        end_date = datetime.now()
        trading_days = []
        
        current_date = end_date
        while len(trading_days) < days:
            date_str = current_date.strftime('%Y-%m-%d')
            if self.is_trading_day(date_str):
                trading_days.append(date_str)
            current_date -= timedelta(days=1)
            
            if (end_date - current_date).days > 365:
                break
        
        return sorted(trading_days)
    
    def is_trading_time(self, datetime_str: str = None) -> bool:
        """判断当前是否在交易时间内"""
        if not datetime_str:
            datetime_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            dt_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            date_str = dt_obj.strftime('%Y-%m-%d')
            time_str = dt_obj.strftime('%H:%M:%S')
            
            if not self.is_trading_day(date_str):
                return False
            
            morning_session = (time_str >= self.trading_hours['morning_start'] and 
                             time_str <= self.trading_hours['morning_end'])
            afternoon_session = (time_str >= self.trading_hours['afternoon_start'] and 
                               time_str <= self.trading_hours['afternoon_end'])
            
            return morning_session or afternoon_session
            
        except Exception as e:
            logger.error(f"❌ 判断交易时间失败 {datetime_str}: {e}")
            return False
    
    def get_today_trading_status(self) -> Dict:
        """获取今日交易状态"""
        today = datetime.now().strftime('%Y-%m-%d')
        current_time = datetime.now().strftime('%H:%M:%S')
        
        is_trading_day = self.is_trading_day(today)
        is_trading_time = self.is_trading_time()
        
        status = "非交易日"
        if is_trading_day:
            if is_trading_time:
                status = "交易中"
            else:
                if current_time < self.trading_hours['morning_start']:
                    status = "开盘前"
                elif current_time > self.trading_hours['afternoon_end']:
                    status = "已收盘"
                else:
                    status = "午间休市"
        
        return {
            'date': today,
            'is_trading_day': is_trading_day,
            'is_trading_time': is_trading_time,
            'status': status,
            'trading_hours': self.trading_hours,
            'current_time': current_time
        }

    def get_latest_trade_day(self, date_str: str = None) -> str:
        """获取最近的一个交易日（如果今天是交易日且已收盘，返回今天；否则返回上一个交易日）"""
        if not date_str:
            now = datetime.now()
            today_s = now.strftime('%Y-%m-%d')
            # 如果今天就是交易日
            if self.is_trading_day(today_s):
                # 检查是否已收盘 (15:30)
                if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                    return today_s
            # 否则返回上一个交易日
            return self.get_previous_trading_day(today_s)
        else:
            # 如果指定了日期，逻辑同上，但假设该日已结束（除非是今天）
            now_s = datetime.now().strftime('%Y-%m-%d')
            if date_str == now_s:
                return self.get_latest_trade_day() # 递归调用不带参的逻辑
            
            if self.is_trading_day(date_str):
                return date_str
            return self.get_previous_trading_day(date_str)

# Legacy alias for backward compatibility
class TradeCalendar(TradingCalendarService):
    """Alias for TradingCalendarService to maintain older imports."""
    pass
