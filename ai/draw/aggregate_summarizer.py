#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将汇总.csv文件按照分类聚集.csv的格式进行汇总
类似时序图的格式，按日期分组显示各项目任务
"""

import csv
import os
from datetime import datetime
from typing import Dict, List, Tuple


class AggregateSummarizer:
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.data = []
        
    def load_data(self):
        """加载汇总.csv文件数据"""
        try:
            with open(self.input_file, 'r', encoding='utf-8-sig') as f:  # 使用utf-8-sig处理BOM
                reader = csv.DictReader(f)
                for row in reader:
                    # 处理BOM字符，确保列名正确
                    cleaned_row = {}
                    for key, value in row.items():
                        # 去除BOM字符
                        cleaned_key = key.replace('\ufeff', '')
                        cleaned_row[cleaned_key] = value
                    self.data.append(cleaned_row)
            print(f"成功加载 {len(self.data)} 条记录")
            if len(self.data) > 0:
                print(f"数据列名: {list(self.data[0].keys())}")
        except Exception as e:
            print(f"加载数据失败: {e}")
            raise
    
    def parse_date(self, time_code: str) -> str:
        """解析时间代码为日期格式"""
        # 时间代码格式如: 20250101
        if len(time_code) == 8 and time_code.isdigit():
            return time_code
        return time_code
    
    def group_by_date_and_project(self) -> Dict[str, Dict[str, List]]:
        """按日期和项目分组数据"""
        grouped_data = {}
        
        for row in self.data:
            time_code = row['timeCode']
            project_name = row['projectName']
            task_name = row['taskName']
            work_days = row['workDays']
            
            # 合并描述信息
            description1 = row.get('description1', '').strip()
            description2 = row.get('description2', '').strip()
            details = description1
            if description2:
                if details:
                    details += "\n" + description2
                else:
                    details = description2
            
            date_key = self.parse_date(time_code)
            
            if date_key not in grouped_data:
                grouped_data[date_key] = {}
            
            if project_name not in grouped_data[date_key]:
                grouped_data[date_key][project_name] = []
            
            grouped_data[date_key][project_name].append({
                'task_name': task_name,
                'work_days': work_days,
                'details': details,
                'stage_name': row.get('stageName', '')
            })
        
        return grouped_data
    
    def generate_aggregate_format(self, grouped_data: Dict[str, Dict[str, List]]) -> List[List[str]]:
        """生成分类聚集格式的数据（按照用户提供的格式）"""
        # 按日期排序
        sorted_dates = sorted(grouped_data.keys())
        
        # 生成分类聚集格式
        aggregate_data = []
        
        # 添加表头（按照您提供的格式）
        header = ['时间\\项目', '', '', '飞行器MDO纵向课题交付', '', '', '管理协调', '', '临时任务（杨超超）', '', '']
        aggregate_data.append(header)
        
        for date in sorted_dates:
            date_data = grouped_data[date]
            
            # 按项目分组
            project_groups = {}
            for project, tasks in date_data.items():
                project_groups[project] = tasks
            
            # 按照您提供的格式生成数据行
            first_row = True
            for project, project_tasks in project_groups.items():
                for i, task in enumerate(project_tasks):
                    # 根据项目名称确定列位置
                    if project == '飞行器MDO纵向课题交付':
                        if first_row:
                            # 第一行包含日期
                            row_data = [date, task['work_days'], '', task['work_days'], '', task['task_name'], task['details']]
                            first_row = False
                        else:
                            # 后续行不包含日期
                            row_data = ['', task['work_days'], '', task['work_days'], '', task['task_name'], task['details']]
                    elif project == '管理协调（杨超超）':
                        if first_row:
                            row_data = [date, '', '', '', '', '', '', task['work_days'], '', task['task_name'], task['details']]
                            first_row = False
                        else:
                            row_data = ['', '', '', '', '', '', '', task['work_days'], '', task['task_name'], task['details']]
                    elif project == '临时任务（杨超超）':
                        if first_row:
                            row_data = [date, '', '', '', '', '', '', '', '', task['work_days'], task['task_name'], task['details']]
                            first_row = False
                        else:
                            row_data = ['', '', '', '', '', '', '', '', '', task['work_days'], task['task_name'], task['details']]
                    else:
                        # 其他项目（如数字风洞）
                        if first_row:
                            row_data = [date, task['work_days'], '', '', '', '', '', '', '', '', task['task_name'], task['details']]
                            first_row = False
                        else:
                            row_data = ['', task['work_days'], '', '', '', '', '', '', '', '', task['task_name'], task['details']]
                    
                    aggregate_data.append(row_data)
        
        return aggregate_data
    
    def save_aggregate_data(self, aggregate_data: List[List[str]]):
        """保存分类聚集格式的数据"""
        try:
            with open(self.output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(aggregate_data)
            print(f"分类聚集数据已保存到: {self.output_file}")
        except Exception as e:
            print(f"保存数据失败: {e}")
            raise
    
    def generate_summary_report(self, grouped_data: Dict[str, Dict[str, List]]):
        """生成汇总报告"""
        report_file = os.path.join(os.path.dirname(self.output_file), '分类聚集汇总报告.txt')
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("分类聚集汇总报告\n")
            f.write("=" * 50 + "\n\n")
            
            # 统计信息
            total_days = 0
            project_stats = {}
            
            for date, date_data in grouped_data.items():
                for project, tasks in date_data.items():
                    if project not in project_stats:
                        project_stats[project] = 0
                    project_days = sum(float(task['work_days']) for task in tasks)
                    project_stats[project] += project_days
                    total_days += project_days
            
            f.write(f"数据统计:\n")
            f.write(f"- 总记录数: {len(self.data)}\n")
            f.write(f"- 总工作天数: {total_days:.2f}\n")
            f.write(f"- 涉及项目数: {len(project_stats)}\n\n")
            
            f.write("按项目汇总:\n")
            for project, days in sorted(project_stats.items(), key=lambda x: x[1], reverse=True):
                percentage = (days / total_days) * 100 if total_days > 0 else 0
                f.write(f"- {project}: {days:.2f}天 ({percentage:.1f}%)\n")
            f.write("\n")
            
            # 按日期详细汇总
            f.write("按日期详细汇总:\n")
            f.write("-" * 50 + "\n")
            
            sorted_dates = sorted(grouped_data.keys())
            for date in sorted_dates:
                date_data = grouped_data[date]
                f.write(f"\n日期: {date}\n")
                
                for project, tasks in date_data.items():
                    total_project_days = sum(float(task['work_days']) for task in tasks)
                    f.write(f"  {project} (总计: {total_project_days:.2f}天):\n")
                    
                    for task in tasks:
                        f.write(f"    - {task['task_name']}: {task['work_days']}天\n")
                        if task['details']:
                            # 处理多行描述
                            details_lines = task['details'].split('\n')
                            for line in details_lines:
                                if line.strip():
                                    f.write(f"        {line.strip()}\n")
                f.write("-" * 50 + "\n")
        
        print(f"汇总报告已生成: {report_file}")
    
    def process(self):
        """主处理流程"""
        print("开始处理汇总数据...")
        
        # 加载数据
        self.load_data()
        
        # 按日期和项目分组
        grouped_data = self.group_by_date_and_project()
        
        # 生成分类聚集格式
        aggregate_data = self.generate_aggregate_format(grouped_data)
        
        # 保存分类聚集数据
        self.save_aggregate_data(aggregate_data)
        
        # 生成汇总报告
        self.generate_summary_report(grouped_data)
        
        print("处理完成!")


def main():
    input_file = os.path.join(os.path.dirname(__file__), '汇总.csv')
    output_file = os.path.join(os.path.dirname(__file__), '汇总_分类聚集.csv')
    
    summarizer = AggregateSummarizer(input_file, output_file)
    summarizer.process()


if __name__ == '__main__':
    main()