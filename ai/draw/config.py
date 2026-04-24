#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置文件 - 抓取任务配置
"""

import os
from typing import Dict, List

# 网站配置
WEBSITE_CONFIG = {
    'base_url': 'http://taskcoll.simforge.cn',
    'login_url': '/login',  # 可能需要调整
    'performance_url_template': '/performance/myweeklydetl/{}',
    'timeout': 30,
    'retry_times': 3,
    'delay_between_requests': 1,  # 请求间隔（秒）
}

# 请求头配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# 登录表单字段（需要根据实际网站调整）
LOGIN_FORM_FIELDS = {
    'username_field': 'username',  # 用户名输入框name
    'password_field': 'password',  # 密码输入框name
    # 可能需要添加其他字段，如csrf_token等
}

# 输出配置
OUTPUT_CONFIG = {
    'output_dir': 'output',
    'html_save': True,  # 是否保存原始HTML
    'json_save': True,  # 是否保存解析后的JSON
    'csv_save': False,  # 是否保存为CSV
}

# 周格式配置
WEEK_FORMAT_CONFIG = {
    'format': 'YYYYWWDD',  # 年周几格式
    'start_year': 2024,
    'end_year': 2025,
    'weeks_per_year': 52,  # 每年周数
}

def get_credentials() -> Dict[str, str]:
    """
    获取登录凭据
    优先从环境变量读取，如果没有则返回空字典
    """
    credentials = {}
    
    # 从环境变量读取
    username = os.getenv('TASKCOLL_USERNAME')
    password = os.getenv('TASKCOLL_PASSWORD')
    
    if username and password:
        credentials['username'] = username
        credentials['password'] = password
    
    return credentials

def validate_week_format(week_str: str) -> bool:
    """
    验证周格式是否有效
    
    Args:
        week_str: 周格式字符串
        
    Returns:
        是否有效
    """
    if len(week_str) != 8:
        return False
    
    try:
        week_num = int(week_str)
        return 20240000 <= week_num <= 20300000  # 合理范围检查
    except ValueError:
        return False

def generate_week_list(start_week: str, end_week: str) -> List[str]:
    """
    生成周列表
    
    Args:
        start_week: 开始周
        end_week: 结束周
        
    Returns:
        周列表
    """
    if not (validate_week_format(start_week) and validate_week_format(end_week)):
        return []
    
    start_num = int(start_week)
    end_num = int(end_week)
    
    if start_num > end_num:
        return []
    
    weeks = []
    current = start_num
    
    # 简单的数字递增（需要根据实际周逻辑调整）
    while current <= end_num:
        weeks.append(str(current).zfill(8))
        
        # 这里需要根据实际的周递增逻辑调整
        # 示例：每周递增7（如果格式是YYYYMMDD）
        # 或者递增1（如果格式是YYYYWW）
        current += 1  # 需要根据实际格式调整
    
    return weeks