#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化后的pykaipan DDE接口测试脚本
添加数据量限制和日期范围控制
"""

import pykaipan.pykaipan as pk
from datetime import datetime, timedelta

def format_dde_data(dde_data, max_items=5):
    """格式化DDE数据，限制显示数量"""
    if isinstance(dde_data, dict):
        result = {}
        for key, value in dde_data.items():
            if isinstance(value, list):
                # 限制列表长度
                if len(value) > max_items:
                    result[key] = f"列表[{len(value)}项]，显示前{max_items}项: {value[:max_items]}..."
                else:
                    result[key] = value
            else:
                result[key] = value
        return result
    return dde_data

def test_real_time_dde():
    """测试实时DDE数据"""
    print("=== pykaipan 实时DDE接口测试 ===\n")
    
    # 测试股票代码
    test_stocks = ["600519", "000001", "300063"]
    
    for stock in test_stocks:
        print(f"测试股票 {stock}:")
        
        try:
            # 获取DDE数据
            dde_data = pk.getStockDDE(stock)
            print(f"  ✅ DDE数据获取成功")
            
            # 格式化数据
            formatted_data = format_dde_data(dde_data)
            
            # 打印数据结构
            print(f"    数据类型: {type(formatted_data)}")
            
            if isinstance(formatted_data, dict):
                print(f"    数据字段: {list(formatted_data.keys())}")
                
                # 检查错误码
                if 'errcode' in formatted_data:
                    errcode = formatted_data.get('errcode', '0')
                    errmsg = formatted_data.get('errmsg', '')
                    print(f"    错误码: {errcode}, 错误信息: {errmsg}")
                
                # 打印关键字段
                for key in ['DDJE', 'large_net', 'StockID', 'Date', 'Time']:
                    if key in formatted_data:
                        value = formatted_data[key]
                        print(f"    {key}: {value}")
                        
                # 如果是DDJE字段，显示大单净额
                if 'DDJE' in formatted_data:
                    ddje_value = formatted_data['DDJE']
                    if isinstance(ddje_value, list) and len(ddje_value) > 0:
                        ddje_value = ddje_value[0]
                    print(f"    🎯 大单净额(DDJE): {ddje_value:,} 元")
                    
            else:
                print(f"    数据内容: {formatted_data}")
                
        except Exception as e:
            print(f"  ❌ 获取DDE数据失败: {e}")
        
        print()

def test_historical_dde_with_limit():
    """测试历史DDE数据，带数据量限制"""
    print("=== pykaipan 历史DDE接口测试（带限制） ===\n")
    
    # 使用近期的日期，避免获取太早的数据
    recent_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
    
    # 只测试一只股票
    test_stock = "600519"
    
    print(f"测试股票 {test_stock} 的历史DDE数据 (日期: {recent_date}):")
    
    try:
        his_dde_data = pk.getHisStockDDE(test_stock, recent_date)
        print(f"  ✅ 历史DDE数据获取成功")
        
        # 格式化数据，限制显示数量
        formatted_data = format_dde_data(his_dde_data, max_items=3)
        
        if isinstance(formatted_data, dict):
            print(f"    数据字段: {list(formatted_data.keys())}")
            
            # 只显示关键字段和部分数据
            print("    关键字段预览:")
            for key in ['DDJE', 'large_net', 'StockID', 'Date', 'Time', 'errcode']:
                if key in formatted_data:
                    value = formatted_data[key]
                    print(f"      {key}: {value}")
            
            # 显示其他字段的数量统计
            other_fields = [k for k in formatted_data.keys() if k not in ['DDJE', 'large_net', 'StockID', 'Date', 'Time', 'errcode']]
            if other_fields:
                print(f"    其他字段数量: {len(other_fields)}个")
                print(f"    其他字段示例: {other_fields[:3]}...")
                
        else:
            print(f"    数据内容: {formatted_data}")
            
    except Exception as e:
        print(f"  ❌ 获取历史DDE数据失败: {e}")
    
    print()

def test_dde_summary():
    """测试DDE数据摘要功能"""
    print("=== DDE数据摘要测试 ===\n")
    
    test_stock = "600519"
    
    try:
        # 获取实时DDE数据
        dde_data = pk.getStockDDE(test_stock)
        
        if isinstance(dde_data, dict):
            print(f"股票 {test_stock} DDE数据摘要:")
            
            # 提取关键信息
            ddje = dde_data.get('DDJE', 'N/A')
            stock_id = dde_data.get('StockID', 'N/A')
            date = dde_data.get('Date', 'N/A')
            time = dde_data.get('Time', 'N/A')
            errcode = dde_data.get('errcode', '0')
            
            # 处理DDJE字段（如果是列表，取第一个值）
            if isinstance(ddje, list) and len(ddje) > 0:
                ddje_value = ddje[0]
            else:
                ddje_value = ddje
            
            print(f"  📊 股票代码: {stock_id}")
            print(f"  📅 日期: {date}")
            print(f"  ⏰ 时间: {time}")
            print(f"  💰 大单净额(DDJE): {ddje_value:,} 元")
            print(f"  ✅ 错误码: {errcode}")
            
            # 数据量统计
            total_fields = len(dde_data)
            list_fields = [k for k, v in dde_data.items() if isinstance(v, list)]
            print(f"  📈 数据字段总数: {total_fields}")
            print(f"  📋 列表字段数量: {len(list_fields)}")
            
        else:
            print(f"  ❌ 数据格式异常")
            
    except Exception as e:
        print(f"  ❌ 获取DDE数据失败: {e}")

if __name__ == "__main__":
    print("开始优化后的pykaipan DDE接口测试...\n")
    
    # 测试实时DDE数据
    test_real_time_dde()
    
    # 测试历史DDE数据（带限制）
    test_historical_dde_with_limit()
    
    # 测试数据摘要
    test_dde_summary()
    
    print("测试完成！")