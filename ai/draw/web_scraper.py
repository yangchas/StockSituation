#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
登录抓取工具 - 抓取taskcoll.simforge.cn的绩效页面内容
支持按周格式（如20250203）抓取数据
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os

class TaskCollScraper:
    """TaskColl网站抓取器"""
    
    def __init__(self, base_url: str = "http://taskcoll.simforge.cn"):
        """
        初始化抓取器
        
        Args:
            base_url: 网站基础URL
        """
        self.base_url = base_url
        self.session = requests.Session()
        
        # 设置请求头，模拟浏览器行为
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # 登录状态
        self.is_logged_in = False
        
    def login(self, username: str, password: str) -> bool:
        """
        登录到网站
        
        Args:
            username: 用户名
            password: 密码
            
        Returns:
            登录是否成功
        """
        # 首先访问登录页面获取必要的token和cookie
        try:
            login_page_url = f"{self.base_url}/login"
            response = self.session.get(login_page_url)
            
            if response.status_code != 200:
                print(f"登录页面访问失败: {response.status_code}")
                return False
            
            # 这里需要根据实际登录表单结构调整
            # 通常需要提取csrf token等安全参数
            login_data = {
                'username': username,
                'password': password,
                # 可能需要添加其他参数，如csrf_token等
            }
            
            # 提交登录请求
            login_post_url = f"{self.base_url}/login"  # 可能需要调整为实际的登录接口
            login_response = self.session.post(login_post_url, data=login_data)
            
            if login_response.status_code == 200:
                # 检查登录是否成功
                # 可以根据返回内容或跳转URL判断
                if "登录成功" in login_response.text or "dashboard" in login_response.url:
                    self.is_logged_in = True
                    print("登录成功!")
                    return True
                else:
                    print("登录失败: 用户名或密码错误")
                    return False
            else:
                print(f"登录请求失败: {login_response.status_code}")
                return False
                
        except requests.RequestException as e:
            print(f"登录过程中发生错误: {e}")
            return False
    
    def get_weekly_performance(self, week_format: str) -> Optional[Dict]:
        """
        获取指定周的绩效详情
        
        Args:
            week_format: 周格式，如 "20250203"
            
        Returns:
            页面内容字典，包含HTML和解析后的数据
        """
        if not self.is_logged_in:
            print("请先登录!")
            return None
            
        try:
            # 构建目标URL
            target_url = f"{self.base_url}/performance/myweeklydetl/{week_format}"
            
            print(f"正在抓取: {target_url}")
            
            response = self.session.get(target_url)
            
            if response.status_code == 200:
                # 成功获取页面内容
                content = response.text
                
                # 解析页面内容
                parsed_data = self._parse_performance_page(content, week_format)
                
                return {
                    'url': target_url,
                    'week_format': week_format,
                    'html_content': content,
                    'parsed_data': parsed_data,
                    'timestamp': datetime.now().isoformat()
                }
            elif response.status_code == 403:
                print("访问被拒绝，可能需要重新登录")
                self.is_logged_in = False
                return None
            elif response.status_code == 404:
                print(f"页面不存在: {week_format}")
                return None
            else:
                print(f"请求失败: {response.status_code}")
                return None
                
        except requests.RequestException as e:
            print(f"抓取过程中发生错误: {e}")
            return None
    
    def _parse_performance_page(self, html_content: str, week_format: str) -> Dict:
        """
        解析绩效页面内容
        
        Args:
            html_content: HTML页面内容
            week_format: 周格式
            
        Returns:
            解析后的数据字典
        """
        # 这里需要根据实际的页面结构来解析
        # 以下是一个示例解析逻辑
        
        parsed_data = {
            'week': week_format,
            'tasks': [],
            'summary': {},
            'extracted_info': {}
        }
        
        try:
            # 使用BeautifulSoup进行HTML解析（需要安装）
            # from bs4 import BeautifulSoup
            # soup = BeautifulSoup(html_content, 'html.parser')
            
            # 示例：提取页面标题
            if '<title>' in html_content:
                title_start = html_content.find('<title>') + 7
                title_end = html_content.find('</title>')
                if title_end > title_start:
                    parsed_data['page_title'] = html_content[title_start:title_end]
            
            # 示例：查找关键信息（根据实际页面结构调整）
            if '绩效' in html_content:
                parsed_data['has_performance_data'] = True
            
            # 这里可以添加更多的解析逻辑
            # 比如提取表格数据、任务列表等
            
        except Exception as e:
            print(f"解析页面时发生错误: {e}")
            parsed_data['parse_error'] = str(e)
        
        return parsed_data
    
    def batch_scrape_weeks(self, start_week: str, end_week: str, 
                          username: str, password: str) -> List[Dict]:
        """
        批量抓取多周的数据
        
        Args:
            start_week: 开始周（格式：20250203）
            end_week: 结束周
            username: 用户名
            password: 密码
            
        Returns:
            所有周的数据列表
        """
        # 首先登录
        if not self.login(username, password):
            return []
        
        results = []
        
        # 生成周列表（这里需要根据实际周格式生成逻辑）
        weeks = self._generate_week_range(start_week, end_week)
        
        for week in weeks:
            print(f"正在处理周: {week}")
            
            data = self.get_weekly_performance(week)
            if data:
                results.append(data)
            
            # 添加延迟避免请求过快
            time.sleep(1)
        
        return results
    
    def _generate_week_range(self, start_week: str, end_week: str) -> List[str]:
        """
        生成周范围列表
        
        Args:
            start_week: 开始周
            end_week: 结束周
            
        Returns:
            周列表
        """
        # 这里需要根据实际的周格式来生成
        # 示例：假设周格式为YYYYWWDD（年周几）
        # 您需要根据实际格式调整这个逻辑
        
        weeks = []
        
        try:
            # 简单的递增逻辑（需要根据实际周格式调整）
            start_num = int(start_week)
            end_num = int(end_week)
            
            current = start_num
            while current <= end_num:
                weeks.append(str(current).zfill(8))  # 保持8位格式
                current += 1  # 这里需要根据周递增逻辑调整
                
        except ValueError:
            print("周格式错误，请输入有效的数字格式")
        
        return weeks
    
    def save_data(self, data: Dict, filename: str = None):
        """
        保存抓取的数据
        
        Args:
            data: 要保存的数据
            filename: 文件名，如果为None则自动生成
        """
        if filename is None:
            week = data.get('week_format', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"taskcoll_data_{week}_{timestamp}.json"
        
        # 确保目录存在
        os.makedirs('output', exist_ok=True)
        filepath = os.path.join('output', filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"数据已保存到: {filepath}")

def main():
    """主函数 - 使用示例"""
    
    # 创建抓取器实例
    scraper = TaskCollScraper()
    
    # 用户输入（在实际使用时应该从配置文件或环境变量读取）
    username = input("请输入用户名: ")
    password = input("请输入密码: ")
    week_format = input("请输入周格式（如20250203）: ")
    
    # 登录
    if scraper.login(username, password):
        # 抓取指定周的数据
        data = scraper.get_weekly_performance(week_format)
        
        if data:
            # 保存数据
            scraper.save_data(data)
            
            # 显示摘要信息
            print(f"\n=== 抓取结果摘要 ===")
            print(f"周: {data['week_format']}")
            print(f"页面标题: {data['parsed_data'].get('page_title', 'N/A')}")
            print(f"是否有绩效数据: {data['parsed_data'].get('has_performance_data', False)}")
            print(f"数据已保存到output目录")
        else:
            print("抓取失败")
    else:
        print("登录失败，请检查用户名和密码")

if __name__ == "__main__":
    main()