#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据分析器 - pykaipan库封装类
封装pykaipan库的所有功能，提供统一的接口和错误处理
"""

import pykaipan.pykaipan as pk
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any


class StockAnalyzer:
    """
    股票数据分析器
    封装pykaipan库的所有功能，提供统一的接口
    """
    
    def __init__(self):
        """初始化分析器"""
        self._pk = pk
        self._last_error = None
    
    @property
    def last_error(self) -> Optional[str]:
        """获取最后一次错误信息"""
        return self._last_error
    
    def _call_api(self, func_name: str, *args, **kwargs) -> Optional[Dict[str, Any]]:
        """
        统一调用API函数
        
        Args:
            func_name: 函数名
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            返回API调用结果，失败返回None
        """
        try:
            func = getattr(self._pk, func_name)
            result = func(*args, **kwargs)
            self._last_error = None
            
            # 🛠️ 递归适配：多层脱壳，确保最终拿到的是数据字典
            while isinstance(result, (tuple, list)) and len(result) > 0:
                if isinstance(result[0], int) and result[0] != 0:
                    # 如果第一个元素是错误码且非0，记录错误
                    self._last_error = f"API Error Code: {result[0]}"
                result = result[1] if len(result) > 1 else result[0]
            
            # 检查API返回的内部错误码
            if isinstance(result, dict) and 'errcode' in result:
                errcode = result.get('errcode', '0')
                if errcode != '0':
                    errmsg = result.get('errmsg', '未知错误')
                    self._last_error = f"API错误 {errcode}: {errmsg}"
                    return None
            
            return result
            
        except Exception as e:
            self._last_error = f"调用{func_name}失败: {str(e)}"
            return None

    def _normalize_date(self, date_str: Optional[str], to_hyphen: bool = True) -> str:
        """
        标准化日期格式
        
        Args:
            date_str: 输入日期字符串 (None, YYYY-MM-DD, YYYYMMDD等)
            to_hyphen: 是否转换为 YYYY-MM-DD 格式。False 则转换为 YYYYMMDD。
            
        Returns:
            标准化后的日期字符串
        """
        if not date_str or not str(date_str).strip():
            return datetime.now().strftime('%Y-%m-%d' if to_hyphen else '%Y%m%d')
        
        date_str = str(date_str).strip().split()[0]
        
        try:
            # 尝试识别各种常见格式并解析
            if '-' in date_str:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            elif '/' in date_str:
                dt = datetime.strptime(date_str, '%Y/%m/%d')
            elif len(date_str) == 8:
                dt = datetime.strptime(date_str, '%Y%m%d')
            else:
                return date_str # 无法识别，原样返回以免破坏
            
            return dt.strftime('%Y-%m-%d' if to_hyphen else '%Y%m%d')
        except Exception:
            return date_str
    
    # ==================== 股票相关功能 ====================
    
    def get_ban_reason(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票涨停原因
        
        Args:
            stock_code: 股票代码
            
        Returns:
            涨停原因信息
        """
        return self._call_api('getBanReason', stock_code)
    
    def get_ban_reasons(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票详细涨停原因列表
        
        Args:
            stock_code: 股票代码
            
        Returns:
            详细涨停原因列表
        """
        return self._call_api('getBanReasons', stock_code)
    
    def get_bans(self, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        获取涨停股票列表
        
        Args:
            date: 日期(YYYYMMDD)，默认今天
            
        Returns:
            涨停股票列表
        """
        # 注意：此处维持原样转换，或根据 pykaipan 文档统一。如果 pykaipan 全局支持 YYYY-MM-DD 则统一。
        date = self._normalize_date(date, to_hyphen=True) 
        return self._call_api('getBans', date)
    
    def get_bans_count(self) -> Optional[Dict[str, Any]]:
        """获取涨停股票数量统计"""
        return self._call_api('getBansCount')
    
    def get_no_bans(self) -> Optional[Dict[str, Any]]:
        """获取非涨停股票列表"""
        return self._call_api('getNoBans')
    
    def get_longhus(self) -> Optional[Dict[str, Any]]:
        """获取龙虎榜股票列表"""
        return self._call_api('getLonghus')
    
    def get_self_stock(self) -> Optional[Dict[str, Any]]:
        """获取自选股列表"""
        return self._call_api('getSelfStock')
    
    def get_stock_dde(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票DDE数据
        
        Args:
            stock_code: 股票代码
            
        Returns:
            DDE数据
        """
        return self._call_api('getStockDDE', stock_code)
    
    def get_stock_gene(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票基因数据
        
        Args:
            stock_code: 股票代码
            
        Returns:
            基因数据
        """
        return self._call_api('getStockGene', stock_code)
    
    # ==================== 板块相关功能 ====================
    
    def get_plate_inner(self) -> Optional[Dict[str, Any]]:
        """获取板块内部数据"""
        return self._call_api('getPlateInner')
    
    def get_plate_min_vols(self, plate_code: str) -> Optional[Dict[str, Any]]:
        """
        获取板块分钟成交量
        
        Args:
            plate_code: 板块代码
            
        Returns:
            分钟成交量数据
        """
        return self._call_api('getPlateMinVols', plate_code)
    
    def get_plate_mins(self, plate_code: str) -> Optional[Dict[str, Any]]:
        """
        获取板块分钟数据
        
        Args:
            plate_code: 板块代码
            
        Returns:
            分钟数据
        """
        return self._call_api('getPlateMins', plate_code)

    def get_history_bans_pool(self, date: str, max_ban: int = 5) -> List[Dict[str, Any]]:
        """
        [V3.5 统一模型] 获取指定日期的全量涨停池 (1-max_ban 梯队)
        采用 retro_today.py 的黄金标准解析索引
        """
        all_bans = []
        # 将日期标准化为 pykaipan 预期的 YYYY-MM-DD
        target_date = self._normalize_date(date, to_hyphen=True)
        
        for ban_lvl in range(1, max_ban + 1):
            try:
                res = self._call_api('getHisBans', date=target_date, ban=str(ban_lvl), size=200)
                if not res: continue
                pages = res.get('info', [])
                if not pages: continue
                
                for page in pages:
                    for rec in page:
                        if not isinstance(rec, (list, tuple)) or len(rec) < 16: continue
                        # [黄金索引对齐]
                        code    = str(rec[0])[-6:].zfill(6)
                        name    = str(rec[1])
                        # rec[15] 是准确的连板高度 (由 Kaipanla 云端计算)
                        lb_days = int(rec[15]) if rec[15] else ban_lvl
                        # rec[12] 是板块名称
                        plate   = str(rec[12]) if len(rec) > 12 else "其他"
                        
                        seal_time = str(rec[3]) if len(rec) > 3 else "?"
                        turnover  = float(rec[9]) if len(rec) > 9 and rec[9] else 0.0
                        close_pct = float(rec[2]) if len(rec) > 2 and rec[2] else 10.0
                        
                        all_bans.append({
                            "code": code, "name": name, "lb_days": lb_days,
                            "plate": plate, "seal_time": seal_time,
                            "turnover": turnover, "close_pct": close_pct
                        })
            except Exception as e:
                # 记录最后一次错误但继续执行
                self._last_error = f"Pool Sync Error at {ban_lvl}B: {e}"
                continue
                
        # 去重 (同一股票可能出现在不同扫描策略的反馈中)
        seen = set()
        result = []
        for b in all_bans:
            if b['code'] not in seen:
                seen.add(b['code'])
                result.append(b)
        return result
    
    # ==================== 历史数据功能 ====================
    
    def get_his_bans(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史涨停数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisBans', date)
    
    def get_his_bans_count(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史涨停数量"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisBansCount', date)
    
    def get_his_longhu(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史龙虎榜数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisLonghu', date)
    
    def get_his_longhu_view(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史龙虎榜视图"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisLonghuView', date)
    
    def get_his_no_bans(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史非涨停数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisNoBans', date)
    
    def get_his_stock(self, stock_code: str, date: str) -> Optional[Dict[str, Any]]:
        """获取股票历史数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisStock', stock_code, date)
    
    def get_his_stock_dde(self, stock_code: str, date: str) -> Optional[Dict[str, Any]]:
        """获取股票历史DDE数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisStockDDE', stock_code, date)
    
    def get_his_plate_as(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史板块A股数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPlateAs', date)
    
    def get_his_plate_bs(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史板块B股数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPlateBs', date)
    
    def get_his_plate_ids(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史板块ID列表"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPlateIds', date)
    
    def get_his_plate_mins(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史板块分钟数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPlateMins', date)
    
    def get_his_plate_rangs(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史板块排名"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPlateRangs', date)
    
    def get_his_plates(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史板块数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPlates', date)
    
    def get_his_floor(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史地板数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisFloor', date)
    
    def get_his_have_floor(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史有地板数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisHaveFloor', date)
    
    def get_his_open(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史开盘数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisOpen', date)
    
    def get_his_open_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史开盘数量统计"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisOpenCounts', date)
    
    def get_his_pan_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史盘面统计"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPanCounts', date)
    
    def get_his_pan_rangs(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史盘面排名"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPanRangs', date)
    
    def get_his_pan_stock_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史盘面股票数量统计"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisPanStockCounts', date)
    
    def get_his_weight_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史权重统计"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisWeightCounts', date)
    
    def get_his_zha(self, date: str) -> Optional[Dict[str, Any]]:
        """获取历史炸板数据"""
        date = self._normalize_date(date, to_hyphen=True)
        return self._call_api('getHisZha', date)
    
    # ==================== 其他功能 ====================
    
    def get_pan_vols(self) -> Optional[Dict[str, Any]]:
        """获取盘面成交量"""
        return self._call_api('getPanVols')
    
    # ==================== 数据解析工具方法 ====================
    
    def parse_ban_reasons(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        解析涨停原因数据
        
        Args:
            result: get_ban_reasons返回的结果
            
        Returns:
            解析后的涨停原因列表
        """
        if not result or 'List' not in result:
            return []
        
        reasons = []
        for item in result['List']:
            reason_data = {
                'stock_code': result.get('StockID', ''),
                'zs_codes': item.get('ZSCode', []),
                'reason': item.get('Reason', ''),
                'date': item.get('Date', ''),
                'scdw': item.get('SCDW', ''),
                'sclt': item.get('SCLT', ''),
                'gnsm': item.get('GNSM', ''),
                'type': item.get('Type', ''),
                'pzs_code': item.get('PZSCode', ''),
                'group_str': item.get('Group_Str', ''),
                'boom_zs': item.get('Boom_ZS', '')
            }
            reasons.append(reason_data)
        
        return reasons
    
    def parse_plate_data(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        解析板块数据
        
        Args:
            result: 板块相关函数返回的结果
            
        Returns:
            解析后的板块数据列表
        """
        if not result or 'List' not in result:
            return []
        
        plates = []
        for item in result['List']:
            if isinstance(item, list) and len(item) >= 3:
                plate_data = {
                    'plate_code': item[0],
                    'plate_name': item[1],
                    'value': item[2]
                }
                plates.append(plate_data)
        
        return plates
    
    def to_dataframe(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> pd.DataFrame:
        """
        将数据转换为DataFrame
        
        Args:
            data: 字典或字典列表
            
        Returns:
            pandas DataFrame
        """
        if isinstance(data, dict):
            # 如果是单个字典，尝试提取主要数据
            if 'List' in data and isinstance(data['List'], list):
                return pd.DataFrame(data['List'])
            elif 'info' in data and isinstance(data['info'], list):
                return pd.DataFrame(data['info'])
            else:
                # 如果是普通字典，转换为单行DataFrame
                return pd.DataFrame([data])
        elif isinstance(data, list):
            return pd.DataFrame(data)
        else:
            return pd.DataFrame()
    
    def save_to_json(self, data: Any, filename: str) -> bool:
        """
        保存数据到JSON文件
        
        Args:
            data: 要保存的数据
            filename: 文件名
            
        Returns:
            是否保存成功
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            self._last_error = f"保存文件失败: {str(e)}"
            return False
    
    def load_from_json(self, filename: str) -> Optional[Any]:
        """
        从JSON文件加载数据
        
        Args:
            filename: 文件名
            
        Returns:
            加载的数据
        """
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self._last_error = f"加载文件失败: {str(e)}"
            return None


# 使用示例
def main():
    """使用示例"""
    # 创建分析器实例
    analyzer = StockAnalyzer()
    
    # 测试股票涨停原因
    stock_code = "300433"
    result = analyzer.get_ban_reasons(stock_code)
    re = pk.getHisPlates("2026-03-17")
    print(re)
    # if 0:
    #     print(f"获取{stock_code}涨停原因成功")
        
    #     # 解析数据
    #     reasons = analyzer.parse_ban_reasons(result)
    #     print(f"解析到{len(reasons)}条涨停原因")
        
    #     # 转换为DataFrame
    #     df = analyzer.to_dataframe(reasons)
    #     print(f"DataFrame形状: {df.shape}")
        
    #     # 保存到文件
    #     analyzer.save_to_json(reasons, f"{stock_code}_ban_reasons.json")
    #     print("数据已保存到文件")
    # else:
    #     print(f"获取失败: {analyzer.last_error}")
    
    # # 测试板块数据
    # plate_result = analyzer.get_plate_inner()
    # print(plate_result)
    # if plate_result:
    #     plates = analyzer.parse_plate_data(plate_result)
    #     print(f"获取到{len(plates)}个板块数据")


if __name__ == "__main__":
    main()