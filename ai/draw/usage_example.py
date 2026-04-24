#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高级抓取工具使用示例
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from advanced_scraper import AdvancedTaskCollScraper
import json
from datetime import datetime

def example_single_week():
    """示例1: 抓取单周数据"""
    print("=== 示例1: 抓取单周数据 ===")
    
    scraper = AdvancedTaskCollScraper()
    
    # 登录信息（请替换为实际凭据）
    username = "your_username"  # 替换为实际用户名
    password = "your_password"  # 替换为实际密码
    
    # 智能登录
    if scraper.smart_login(username, password):
        # 抓取指定周的数据
        week_format = "20250203"  # 年月份第几周
        data = scraper.get_weekly_performance(week_format)
        
        if data:
            # 保存数据
            filename = f"taskcoll_week_{week_format}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"数据已保存到: {filename}")
            print(f"页面标题: {data['parsed_data'].get('title', 'N/A')}")
            print(f"表格数量: {len(data.get('tables', []))}")
        else:
            print("抓取失败")
    else:
        print("登录失败")

def example_multiple_weeks():
    """示例2: 批量抓取多周数据"""
    print("\n=== 示例2: 批量抓取多周数据 ===")
    
    scraper = AdvancedTaskCollScraper()
    
    # 登录信息
    username = "your_username"
    password = "your_password"
    
    if scraper.smart_login(username, password):
        # 要抓取的周列表
        weeks = ["20250201", "20250202", "20250203", "20250204"]
        
        all_data = []
        
        for week in weeks:
            print(f"正在抓取第 {week} 周数据...")
            
            # 使用重试机制抓取
            data = scraper.scrape_with_retry(week)
            
            if data:
                all_data.append(data)
                print(f"第 {week} 周抓取成功")
            else:
                print(f"第 {week} 周抓取失败")
            
            # 添加延迟避免频繁请求
            import time
            time.sleep(1)
        
        # 保存所有数据
        if all_data:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"taskcoll_batch_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            
            print(f"批量数据已保存到: {filename}")
            print(f"成功抓取 {len(all_data)} 周的数据")

def example_with_export():
    """示例3: 抓取并导出为CSV"""
    print("\n=== 示例3: 抓取并导出为CSV ===")
    
    try:
        import pandas as pd
    except ImportError:
        print("pandas未安装，无法导出CSV")
        return
    
    scraper = AdvancedTaskCollScraper()
    
    username = "your_username"
    password = "your_password"
    
    if scraper.smart_login(username, password):
        week_format = "20250203"
        data = scraper.get_weekly_performance(week_format)
        
        if data:
            # 导出为CSV
            scraper.export_to_csv(data)
            
            # 保存JSON
            json_filename = f"taskcoll_week_{week_format}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"JSON数据已保存到: {json_filename}")

def example_form_detection():
    """示例4: 表单探测功能"""
    print("\n=== 示例4: 表单探测功能 ===")
    
    scraper = AdvancedTaskCollScraper()
    
    # 探测登录表单
    form_info = scraper.detect_login_form()
    
    if form_info:
        print("探测到的登录表单信息:")
        print(f"表单动作: {form_info.get('action', 'N/A')}")
        print(f"表单方法: {form_info.get('method', 'N/A')}")
        print(f"输入字段: {form_info.get('fields', {})}")
        print(f"隐藏字段: {form_info.get('hidden_fields', {})}")
    else:
        print("无法探测到登录表单")

def main():
    """主函数 - 运行所有示例"""
    print("高级TaskColl抓取工具使用示例")
    print("=" * 50)
    
    # 运行示例
    example_form_detection()
    example_single_week()
    example_multiple_weeks()
    example_with_export()
    
    print("\n所有示例运行完成!")

if __name__ == "__main__":
    # 检查是否安装了必要的库
    required_libs = ['requests']
    
    missing_libs = []
    for lib in required_libs:
        try:
            __import__(lib)
        except ImportError:
            missing_libs.append(lib)
    
    if missing_libs:
        print(f"缺少必要的库: {', '.join(missing_libs)}")
        print("请使用以下命令安装:")
        print("pip install requests")
        sys.exit(1)
    
    main()