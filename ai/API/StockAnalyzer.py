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
            
            # 检查API返回的错误码
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
        # 如果日期为空或格式不正确，使用默认值
        if date is None or not date.strip():
            date = datetime.now().strftime('%Y%m%d')
        else:
            # 清理日期格式，移除可能的时间部分
            date = date.split()[0]  # 只取日期部分
            # 尝试标准化日期格式
            try:
                # 如果是YYYY-MM-DD格式，转换为YYYYMMDD
                if '-' in date:
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    date = date_obj.strftime('%Y%m%d')
                # 如果是YYYY/MM/DD格式，转换为YYYYMMDD
                elif '/' in date:
                    date_obj = datetime.strptime(date, '%Y/%m/%d')
                    date = date_obj.strftime('%Y%m%d')
            except ValueError:
                # 如果日期格式解析失败，使用今天
                date = datetime.now().strftime('%Y%m%d')
        
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
    
    # ==================== 历史数据功能 ====================
    
    def get_his_bans(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史涨停数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史涨停数据
        """
        return self._call_api('getHisBans', date)
    
    def get_his_bans_count(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史涨停数量
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史涨停数量
        """
        return self._call_api('getHisBansCount', date)
    
    def get_his_longhu(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史龙虎榜数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史龙虎榜数据
        """
        return self._call_api('getHisLonghu', date)
    
    def get_his_longhu_view(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史龙虎榜视图
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史龙虎榜视图
        """
        return self._call_api('getHisLonghuView', date)
    
    def get_his_no_bans(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史非涨停数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史非涨停数据
        """
        return self._call_api('getHisNoBans', date)
    
    def get_his_stock(self, stock_code: str, date: str) -> Optional[Dict[str, Any]]:
        """
        获取股票历史数据
        
        Args:
            stock_code: 股票代码
            date: 日期(YYYYMMDD)
            
        Returns:
            股票历史数据
        """
        return self._call_api('getHisStock', stock_code, date)
    
    def get_his_stock_dde(self, stock_code: str, date: str) -> Optional[Dict[str, Any]]:
        """
        获取股票历史DDE数据
        
        Args:
            stock_code: 股票代码
            date: 日期(YYYYMMDD)
            
        Returns:
            历史DDE数据
        """
        return self._call_api('getHisStockDDE', stock_code, date)
    
    def get_his_plate_as(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史板块A股数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史板块A股数据
        """
        return self._call_api('getHisPlateAs', date)
    
    def get_his_plate_bs(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史板块B股数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史板块B股数据
        """
        return self._call_api('getHisPlateBs', date)
    
    def get_his_plate_ids(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史板块ID列表
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史板块ID列表
        """
        return self._call_api('getHisPlateIds', date)
    
    def get_his_plate_mins(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史板块分钟数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史板块分钟数据
        """
        return self._call_api('getHisPlateMins', date)
    
    def get_his_plate_rangs(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史板块排名
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史板块排名
        """
        return self._call_api('getHisPlateRangs', date)
    
    def get_his_plates(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史板块数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史板块数据
        """
        return self._call_api('getHisPlates', date)
    
    def get_his_floor(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史地板数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史地板数据
        """
        return self._call_api('getHisFloor', date)
    
    def get_his_have_floor(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史有地板数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史有地板数据
        """
        return self._call_api('getHisHaveFloor', date)
    
    def get_his_open(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史开盘数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史开盘数据
        """
        return self._call_api('getHisOpen', date)
    
    def get_his_open_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史开盘数量统计
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史开盘数量统计
        """
        return self._call_api('getHisOpenCounts', date)
    
    def get_his_pan_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史盘面统计
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史盘面统计
        """
        return self._call_api('getHisPanCounts', date)
    
    def get_his_pan_rangs(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史盘面排名
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史盘面排名
        """
        return self._call_api('getHisPanRangs', date)
    
    def get_his_pan_stock_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史盘面股票数量统计
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史盘面股票数量统计
        """
        return self._call_api('getHisPanStockCounts', date)
    
    def get_his_pan_vols(self) -> Optional[Dict[str, Any]]:
        """获取历史盘面成交量"""
        return self._call_api('getHisPanVols')
    
    def get_his_weight_counts(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史权重统计
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史权重统计
        """
        return self._call_api('getHisWeightCounts', date)
    
    def get_his_zha(self, date: str) -> Optional[Dict[str, Any]]:
        """
        获取历史炸板数据
        
        Args:
            date: 日期(YYYYMMDD)
            
        Returns:
            历史炸板数据
        """
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
    
    if result:
        print(f"获取{stock_code}涨停原因成功")
        
        # 解析数据
        reasons = analyzer.parse_ban_reasons(result)
        print(f"解析到{len(reasons)}条涨停原因")
        
        # 转换为DataFrame
        df = analyzer.to_dataframe(reasons)
        print(f"DataFrame形状: {df.shape}")
        
        # 保存到文件
        analyzer.save_to_json(reasons, f"{stock_code}_ban_reasons.json")
        print("数据已保存到文件")
    else:
        print(f"获取失败: {analyzer.last_error}")
    
    # 测试板块数据
    plate_result = analyzer.get_plate_inner()
    if plate_result:
        plates = analyzer.parse_plate_data(plate_result)
        print(f"获取到{len(plates)}个板块数据")


if __name__ == "__main__":
    main()