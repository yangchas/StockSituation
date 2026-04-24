"""
时序图格式分类聚集工具
将分类聚集CSV文件转换为类似时序图的格式
"""

import csv
from datetime import datetime
from typing import List, Dict, Any

class TimelineClassifier:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.data = []
        self.projects = []
        
    def load_data(self):
        """加载CSV数据并解析"""
        with open(self.csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        # 解析表头获取项目信息
        header = rows[0]
        self.projects = []
        i = 0
        while i < len(header):
            if header[i] and '项目' not in header[i]:  # 跳过"时间\项目"
                self.projects.append({
                    'name': header[i],
                    'start_col': i,
                    'end_col': i + 3  # 每个项目占4列
                })
            i += 1
            
        # 解析数据行
        current_date = None
        for row in rows[1:]:
            if row[0]:  # 新的日期行
                current_date = row[0]
            
            if current_date:
                # 解析每个项目的任务
                for project in self.projects:
                    start_col = project['start_col']
                    if start_col < len(row) and row[start_col]:  # 有任务数据
                        task_data = {
                            'date': current_date,
                            'project': project['name'],
                            'days': float(row[start_col]) if row[start_col] else 0,
                            'task_name': row[start_col + 2] if start_col + 2 < len(row) else '',
                            'task_details': row[start_col + 3] if start_col + 3 < len(row) else ''
                        }
                        if task_data['task_name']:  # 只添加有任务名称的记录
                            self.data.append(task_data)
    
    def generate_timeline_format(self) -> str:
        """生成时序图格式的输出"""
        if not self.data:
            return "没有数据可处理"
            
        # 按日期和项目分组
        timeline_data = {}
        for item in self.data:
            date = item['date']
            project = item['project']
            
            if date not in timeline_data:
                timeline_data[date] = {}
            
            if project not in timeline_data[date]:
                timeline_data[date][project] = []
                
            timeline_data[date][project].append(item)
        
        # 生成时序图格式
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append("时序图格式 - 任务分类聚集")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        # 按日期排序
        sorted_dates = sorted(timeline_data.keys())
        
        for date in sorted_dates:
            output_lines.append(f"📅 日期: {date}")
            output_lines.append("-" * 40)
            
            for project in self.projects:
                project_name = project['name']
                if project_name in timeline_data[date]:
                    tasks = timeline_data[date][project_name]
                    total_days = sum(task['days'] for task in tasks)
                    
                    output_lines.append(f"🏗️  项目: {project_name} (总计: {total_days}天)")
                    
                    for task in tasks:
                        # 创建时间线标记
                        days_bar = "█" * int(task['days'] * 2)  # 每个0.5天用一个█表示
                        if task['days'] < 0.5:
                            days_bar = "▌"  # 小于0.5天用半格表示
                        
                        output_lines.append(f"   └─ {days_bar} {task['task_name']} ({task['days']}天)")
                        if task['task_details']:
                            # 处理多行详情
                            details = task['task_details'].replace('\n', ' | ')
                            output_lines.append(f"       📝 {details}")
                    
                    output_lines.append("")
            
            output_lines.append("")
        
        # 添加统计信息
        output_lines.append("=" * 80)
        output_lines.append("📊 统计信息")
        output_lines.append("=" * 80)
        
        total_days_by_project = {}
        for item in self.data:
            project = item['project']
            if project not in total_days_by_project:
                total_days_by_project[project] = 0
            total_days_by_project[project] += item['days']
        
        for project, days in total_days_by_project.items():
            output_lines.append(f"{project}: {days} 天")
        
        total_days = sum(total_days_by_project.values())
        output_lines.append(f"总计: {total_days} 天")
        
        return '\n'.join(output_lines)
    
    def generate_mermaid_timeline(self) -> str:
        """生成Mermaid时序图代码"""
        if not self.data:
            return ""
        
        mermaid_lines = []
        mermaid_lines.append("```mermaid")
        mermaid_lines.append("timeline")
        mermaid_lines.append("    title 任务时序图")
        mermaid_lines.append("")
        
        # 按日期分组
        timeline_data = {}
        for item in self.data:
            date = item['date']
            if date not in timeline_data:
                timeline_data[date] = []
            timeline_data[date].append(item)
        
        # 生成Mermaid时序图
        for date in sorted(timeline_data.keys()):
            mermaid_lines.append(f"    section {date}")
            
            for item in timeline_data[date]:
                # 简化任务名称用于显示
                task_name = item['task_name'][:30] + "..." if len(item['task_name']) > 30 else item['task_name']
                mermaid_lines.append(f"        {item['project']} : {task_name} ({item['days']}天)")
        
        mermaid_lines.append("```")
        
        return '\n'.join(mermaid_lines)

def main():
    """主函数"""
    csv_file = "分类聚集.csv"
    
    try:
        classifier = TimelineClassifier(csv_file)
        classifier.load_data()
        
        # 生成文本格式时序图
        timeline_output = classifier.generate_timeline_format()
        
        # 保存到文件
        with open("时序图分类聚集.txt", "w", encoding="utf-8") as f:
            f.write(timeline_output)
        
        print("✅ 时序图格式分类聚集完成！")
        print("📁 输出文件: 时序图分类聚集.txt")
        print("")
        print(timeline_output)
        
        # 生成Mermaid格式
        mermaid_output = classifier.generate_mermaid_timeline()
        with open("mermaid_timeline.md", "w", encoding="utf-8") as f:
            f.write(mermaid_output)
        
        print("\n" + "="*80)
        print("📊 Mermaid时序图代码已生成: mermaid_timeline.md")
        print("="*80)
        print(mermaid_output)
        
    except Exception as e:
        print(f"❌ 处理文件时出错: {e}")

if __name__ == "__main__":
    main()