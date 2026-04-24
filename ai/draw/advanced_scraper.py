#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高级抓取工具 - 支持JavaScript渲染、表单自动填充、错误重试等高级功能
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os
import sys

# 尝试导入可选的高级库
try:
    from bs4 import BeautifulSoup
    BEAUTIFULSOUP_AVAILABLE = True
except ImportError:
    BEAUTIFULSOUP_AVAILABLE = False
    print("警告: BeautifulSoup未安装，HTML解析功能受限")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("警告: pandas未安装，数据导出功能受限")

class AdvancedTaskCollScraper:
    """高级TaskColl网站抓取器"""
    
    def __init__(self, base_url: str = "http://taskcoll.simforge.cn"):
        """初始化高级抓取器"""
        self.base_url = base_url
        self.session = self._create_session()
        self.is_logged_in = False
        self.login_data = {}
        
    def _create_session(self) -> requests.Session:
        """创建带有重试机制的会话"""
        session = requests.Session()
        
        # 设置重试策略
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
            backoff_factor=1
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置请求头
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': f'{self.base_url}/',
        })
        
        return session
    
    def detect_login_form(self) -> Optional[Dict]:
        """探测登录表单结构"""
        try:
            login_url = f"{self.base_url}/login"
            response = self.session.get(login_url)
            
            if response.status_code == 200:
                form_info = self._extract_form_info(response.text)
                return form_info
            
        except Exception as e:
            print(f"探测登录表单失败: {e}")
        
        return None
    
    def _extract_form_info(self, html: str) -> Dict:
        """从HTML中提取表单信息"""
        form_info = {
            'action': '',
            'method': 'post',
            'fields': {},
            'hidden_fields': {}
        }
        
        # 简单的正则表达式提取（可以使用BeautifulSoup增强）
        
        # 提取表单action
        action_match = re.search(r'<form[^>]*action="([^"]*)"', html, re.IGNORECASE)
        if action_match:
            form_info['action'] = action_match.group(1)
        
        # 提取表单method
        method_match = re.search(r'<form[^>]*method="([^"]*)"', html, re.IGNORECASE)
        if method_match:
            form_info['method'] = method_match.group(1).lower()
        
        # 提取输入字段
        input_matches = re.finditer(r'<input[^>]*name="([^"]*)"[^>]*>', html, re.IGNORECASE)
        for match in input_matches:
            field_name = match.group(1)
            
            # 检查字段类型
            type_match = re.search(r'type="([^"]*)"', match.group(0), re.IGNORECASE)
            field_type = type_match.group(1).lower() if type_match else 'text'
            
            # 检查是否隐藏字段
            if field_type == 'hidden':
                # 提取隐藏字段的值
                value_match = re.search(r'value="([^"]*)"', match.group(0), re.IGNORECASE)
                if value_match:
                    form_info['hidden_fields'][field_name] = value_match.group(1)
            else:
                form_info['fields'][field_name] = field_type
        
        return form_info
    
    def smart_login(self, username: str, password: str) -> bool:
        """智能登录 - 自动探测表单并登录"""
        print("开始智能登录...")
        
        # 1. 探测登录表单
        form_info = self.detect_login_form()
        if not form_info:
            print("无法探测到登录表单")
            return False
        
        print(f"探测到登录表单: {form_info}")
        
        # 2. 构建登录数据
        login_data = form_info['hidden_fields'].copy()
        
        # 自动识别用户名和密码字段
        username_field = self._guess_username_field(form_info['fields'])
        password_field = self._guess_password_field(form_info['fields'])
        
        if username_field and password_field:
            login_data[username_field] = username
            login_data[password_field] = password
        else:
            # 如果无法自动识别，使用常见字段名
            login_data['username'] = username
            login_data['password'] = password
        
        # 3. 提交登录
        login_url = f"{self.base_url}{form_info['action']}"
        
        try:
            if form_info['method'] == 'get':
                response = self.session.get(login_url, params=login_data)
            else:
                response = self.session.post(login_url, data=login_data)
            
            # 检查登录是否成功
            if self._check_login_success(response):
                self.is_logged_in = True
                self.login_data = login_data
                print("智能登录成功!")
                return True
            else:
                print("智能登录失败")
                return False
                
        except Exception as e:
            print(f"智能登录过程中发生错误: {e}")
            return False
    
    def _guess_username_field(self, fields: Dict) -> Optional[str]:
        """猜测用户名字段"""
        common_names = ['username', 'user', 'email', 'login', 'account']
        
        for name in common_names:
            if name in fields:
                return name
        
        # 如果没有匹配，返回第一个非隐藏文本字段
        for field_name, field_type in fields.items():
            if field_type in ['text', 'email']:
                return field_name
        
        return None
    
    def _guess_password_field(self, fields: Dict) -> Optional[str]:
        """猜测密码字段"""
        common_names = ['password', 'pass', 'pwd']
        
        for name in common_names:
            if name in fields:
                return name
        
        # 如果没有匹配，返回第一个密码类型字段
        for field_name, field_type in fields.items():
            if field_type == 'password':
                return field_name
        
        return None
    
    def _check_login_success(self, response: requests.Response) -> bool:
        """检查登录是否成功"""
        # 多种方式检查登录状态
        
        # 1. 检查URL是否跳转到非登录页面
        if 'login' not in response.url.lower():
            return True
        
        # 2. 检查响应内容中是否包含成功提示
        success_indicators = ['dashboard', '首页', '欢迎', '登录成功']
        for indicator in success_indicators:
            if indicator in response.text:
                return True
        
        # 3. 检查是否有错误信息
        error_indicators = ['错误', '失败', 'invalid', 'incorrect']
        for indicator in error_indicators:
            if indicator in response.text.lower():
                return False
        
        # 4. 尝试访问需要登录的页面
        test_url = f"{self.base_url}/dashboard"  # 可能需要调整
        test_response = self.session.get(test_url)
        
        if test_response.status_code == 200 and 'login' not in test_response.url.lower():
            return True
        
        return False
    
    def scrape_with_retry(self, week_format: str, max_retries: int = 3) -> Optional[Dict]:
        """带重试机制的抓取"""
        for attempt in range(max_retries):
            try:
                data = self.get_weekly_performance(week_format)
                if data:
                    return data
                
                print(f"第{attempt + 1}次尝试失败，等待重试...")
                time.sleep(2 ** attempt)  # 指数退避
                
            except Exception as e:
                print(f"第{attempt + 1}次尝试异常: {e}")
                time.sleep(2 ** attempt)
        
        print(f"抓取失败: {week_format} (尝试{max_retries}次)")
        return None
    
    def get_weekly_performance(self, week_format: str) -> Optional[Dict]:
        """获取指定周的绩效详情"""
        if not self.is_logged_in:
            print("请先登录!")
            return None
            
        try:
            target_url = f"{self.base_url}/performance/myweeklydetl/{week_format}"
            
            print(f"正在抓取: {target_url}")
            
            response = self.session.get(target_url, timeout=30)
            
            if response.status_code == 200:
                return self._parse_advanced_performance_page(response.text, week_format, target_url)
            else:
                print(f"请求失败: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"抓取过程中发生错误: {e}")
            return None
    
    def _parse_advanced_performance_page(self, html: str, week_format: str, url: str) -> Dict:
        """高级页面解析"""
        data = {
            'url': url,
            'week_format': week_format,
            'timestamp': datetime.now().isoformat(),
            'html_content': html,
            'parsed_data': {},
            'tables': [],
            'links': [],
            'stats': {}
        }
        
        # 基础信息提取
        data['parsed_data']['title'] = self._extract_title(html)
        data['parsed_data']['description'] = self._extract_description(html)
        
        # 使用BeautifulSoup进行高级解析（如果可用）
        if BEAUTIFULSOUP_AVAILABLE:
            soup = BeautifulSoup(html, 'html.parser')
            
            # 提取表格数据
            data['tables'] = self._extract_tables(soup)
            
            # 提取链接
            data['links'] = self._extract_links(soup)
            
            # 提取关键统计信息
            data['stats'] = self._extract_stats(soup)
        
        # 正则表达式提取关键信息
        data['parsed_data']['key_info'] = self._extract_key_info_with_regex(html)
        
        return data
    
    def _extract_title(self, html: str) -> str:
        """提取页面标题"""
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
        return title_match.group(1).strip() if title_match else ""
    
    def _extract_description(self, html: str) -> str:
        """提取页面描述"""
        desc_match = re.search(r'<meta[^>]*name="description"[^>]*content="([^"]*)"', html, re.IGNORECASE)
        return desc_match.group(1) if desc_match else ""
    
    def _extract_tables(self, soup) -> List[Dict]:
        """提取表格数据"""
        tables = []
        
        for i, table in enumerate(soup.find_all('table')):
            table_data = {
                'index': i,
                'headers': [],
                'rows': []
            }
            
            # 提取表头
            headers = table.find_all('th')
            table_data['headers'] = [header.get_text(strip=True) for header in headers]
            
            # 提取表格行
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    table_data['rows'].append(row_data)
            
            tables.append(table_data)
        
        return tables
    
    def _extract_links(self, soup) -> List[Dict]:
        """提取链接"""
        links = []
        
        for link in soup.find_all('a', href=True):
            link_data = {
                'text': link.get_text(strip=True),
                'href': link['href'],
                'title': link.get('title', '')
            }
            links.append(link_data)
        
        return links
    
    def _extract_stats(self, soup) -> Dict:
        """提取统计信息"""
        stats = {}
        
        # 这里可以根据页面结构添加特定的统计信息提取逻辑
        # 例如：任务数量、完成率、评分等
        
        return stats
    
    def _extract_key_info_with_regex(self, html: str) -> Dict:
        """使用正则表达式提取关键信息"""
        key_info = {}
        
        # 提取数字信息（任务数量、评分等）
        number_patterns = {
            'tasks_count': r'(任务|任务数)[^\d]*(\d+)',
            'completion_rate': r'(完成率|完成比例)[^\d]*(\d+(?:\.\d+)?)%',
            'score': r'(评分|得分|分数)[^\d]*(\d+(?:\.\d+)?)',
        }
        
        for key, pattern in number_patterns.items():
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                key_info[key] = match.group(2)
        
        return key_info
    
    def export_to_csv(self, data: Dict, filename: str = None):
        """导出数据到CSV（如果pandas可用）"""
        if not PANDAS_AVAILABLE:
            print("pandas未安装，无法导出CSV")
            return
        
        if filename is None:
            week = data.get('week_format', 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"taskcoll_data_{week}_{timestamp}.csv"
        
        os.makedirs('output', exist_ok=True)
        filepath = os.path.join('output', filename)
        
        # 将表格数据转换为DataFrame并保存
        tables = data.get('tables', [])
        
        if tables:
            # 保存第一个表格
            df = pd.DataFrame(tables[0]['rows'], columns=tables[0]['headers'])
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"表格数据已导出到: {filepath}")
        else:
            # 如果没有表格，保存解析后的数据
            flat_data = self._flatten_data(data)
            df = pd.DataFrame([flat_data])
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"解析数据已导出到: {filepath}")
    
    def _flatten_data(self, data: Dict) -> Dict:
        """扁平化嵌套数据"""
        flat_data = {}
        
        for key, value in data.items():
            if isinstance(value, (str, int, float, bool)):
                flat_data[key] = value
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flat_data[f"{key}_{sub_key}"] = sub_value
            elif isinstance(value, list):
                flat_data[key] = str(value)
        
        return flat_data

def main():
    """主函数 - 高级抓取示例"""
    
    scraper = AdvancedTaskCollScraper()
    
    print("=== 高级TaskColl抓取工具 ===")
    
    # 用户输入
    username = input("请输入用户名: ")
    password = input("请输入密码: ")
    week_format = input("请输入周格式（如20250203）: ")
    
    # 智能登录
    if scraper.smart_login(username, password):
        # 带重试的抓取
        data = scraper.scrape_with_retry(week_format)
        
        if data:
            # 保存JSON
            scraper.save_data(data)
            
            # 导出CSV（如果pandas可用）
            if PANDAS_AVAILABLE:
                scraper.export_to_csv(data)
            
            # 显示详细结果
            print(f"\n=== 抓取结果详情 ===")
            print(f"页面标题: {data['parsed_data'].get('title', 'N/A')}")
            print(f"表格数量: {len(data.get('tables', []))}")
            print(f"链接数量: {len(data.get('links', []))}")
            
            # 显示关键信息
            key_info = data['parsed_data'].get('key_info', {})
            if key_info:
                print("关键信息:")
                for key, value in key_info.items():
                    print(f"  {key}: {value}")
        else:
            print("抓取失败")
    else:
        print("登录失败")

if __name__ == "__main__":
    main()