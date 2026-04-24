import pandas as pd
import re
from collections import defaultdict
import json

class TaskSummarizer:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
        self.task_groups = defaultdict(list)
        
    def load_data(self):
        """加载CSV数据"""
        try:
            self.df = pd.read_csv(self.csv_file_path)
            print(f"成功加载数据，共{len(self.df)}条记录")
            return True
        except Exception as e:
            print(f"加载数据失败: {e}")
            return False
    
    def clean_task_name(self, task_name):
        """清理任务名称，去除周报填写等通用任务"""
        if pd.isna(task_name):
            return ""
        
        # 需要合并的通用任务
        generic_tasks = [
            "周报填写", "问题请教", "其他", "组会", "站会", "会议",
            "知识分享", "培训", "团建", "内部会"
        ]
        
        task_lower = task_name.lower()
        for generic in generic_tasks:
            if generic.lower() in task_lower:
                return "管理协调"
        
        return task_name
    
    def group_tasks(self):
        """按项目阶段和任务名称分组任务"""
        if self.df is None:
            print("请先加载数据")
            return
        
        # 按项目阶段和清理后的任务名称分组
        for _, row in self.df.iterrows():
            project_name = row['projectName'] if pd.notna(row['projectName']) else "其他"
            stage_name = row['stageName'] if pd.notna(row['stageName']) else "其他"
            task_name = self.clean_task_name(row['taskName'])
            work_days = row['workDays'] if pd.notna(row['workDays']) else 0
            
            # 跳过空任务名
            if not task_name:
                continue
                
            # 创建分组键
            group_key = f"{project_name} - {stage_name}"
            
            # 合并description1和description2作为详情
            description1 = row['description1'] if pd.notna(row.get('description1', '')) else ""
            description2 = row['description2'] if pd.notna(row.get('description2', '')) else ""
            details = f"{description1} {description2}".strip()
            
            # 如果任务名已经是"管理协调"，单独处理
            if task_name == "管理协调":
                self.task_groups[group_key].append({
                    'task_name': task_name,
                    'work_days': work_days,
                    'details': details,
                    'original_task': row['taskName']
                })
            else:
                # 检查是否已有类似任务
                found_similar = False
                for task in self.task_groups[group_key]:
                    if task_name in task['task_name'] or task['task_name'] in task_name:
                        # 合并相似任务
                        task['work_days'] += work_days
                        if details:
                            task['details'] += f" | {details}"
                        found_similar = True
                        break
                
                if not found_similar:
                    self.task_groups[group_key].append({
                        'task_name': task_name,
                        'work_days': work_days,
                        'details': details,
                        'original_task': row['taskName']
                    })
    
    def summarize_by_project(self):
        """按项目汇总"""
        project_summary = defaultdict(float)
        
        for group_key, tasks in self.task_groups.items():
            project_name = group_key.split(' - ')[0]
            total_days = sum(task['work_days'] for task in tasks)
            project_summary[project_name] += total_days
        
        return dict(sorted(project_summary.items(), key=lambda x: x[1], reverse=True))
    
    def summarize_by_task_type(self):
        """按任务类型汇总"""
        task_type_summary = defaultdict(float)
        
        for group_key, tasks in self.task_groups.items():
            for task in tasks:
                task_name = task['task_name']
                task_type_summary[task_name] += task['work_days']
        
        return dict(sorted(task_type_summary.items(), key=lambda x: x[1], reverse=True))
    
    def get_detailed_summary(self):
        """获取详细汇总"""
        detailed_summary = []
        
        for group_key, tasks in self.task_groups.items():
            project_name, stage_name = group_key.split(' - ')
            
            # 按任务天数排序
            sorted_tasks = sorted(tasks, key=lambda x: x['work_days'], reverse=True)
            
            for task in sorted_tasks:
                if task['work_days'] >= 0.5:  # 只显示大于等于0.5天的任务
                    detailed_summary.append({
                        'project': project_name,
                        'stage': stage_name,
                        'task': task['task_name'],
                        'work_days': round(task['work_days'], 2),
                        'details': task['details'][:100] + "..." if len(task['details']) > 100 else task['details']
                    })
        
        return detailed_summary
    
    def validate_total_days(self):
        """验证总天数是否为250天"""
        if self.df is None:
            return 0
        
        total_days = self.df['workDays'].sum()
        return total_days
    
    def generate_report(self):
        """生成汇总报告"""
        if self.df is None:
            return "数据未加载"
        
        # 验证总天数
        total_days = self.validate_total_days()
        
        # 按项目汇总
        project_summary = self.summarize_by_project()
        
        # 按任务类型汇总
        task_type_summary = self.summarize_by_task_type()
        
        # 详细汇总
        detailed_summary = self.get_detailed_summary()
        
        # 生成报告
        report = f"""
周报任务汇总报告
================

数据统计:
- 总记录数: {len(self.df)} 条
- 总工作天数: {total_days:.2f} 天
- 目标天数: 250.00 天
- 天数差异: {total_days - 250:.2f} 天

按项目汇总:
"""
        
        for project, days in project_summary.items():
            percentage = (days / total_days) * 100
            report += f"- {project}: {days:.2f} 天 ({percentage:.1f}%)\n"
        
        report += "\n按任务类型汇总:\n"
        for task_type, days in task_type_summary.items():
            if days >= 1:  # 只显示大于等于1天的任务类型
                percentage = (days / total_days) * 100
                report += f"- {task_type}: {days:.2f} 天 ({percentage:.1f}%)\n"
        
        report += "\n详细任务汇总 (按项目阶段):\n"
        for item in detailed_summary:
            report += f"\n项目: {item['project']}\n"
            report += f"阶段: {item['stage']}\n"
            report += f"任务: {item['task']}\n"
            report += f"天数: {item['work_days']} 天\n"
            if item['details']:
                report += f"详情: {item['details']}\n"
            report += "-" * 50 + "\n"
        
        return report

def main():
    """主函数"""
    csv_file = "d:\\work\\Go\\ai\\draw\\汇总.csv"
    
    # 创建汇总器
    summarizer = TaskSummarizer(csv_file)
    
    # 加载数据
    if not summarizer.load_data():
        return
    
    # 分组任务
    summarizer.group_tasks()
    
    # 生成报告
    report = summarizer.generate_report()
    
    # 保存报告到文件
    with open("d:\\work\\Go\\ai\\draw\\任务汇总报告.txt", "w", encoding="utf-8") as f:
        f.write(report)
    
    print("任务汇总报告已生成到: d:\\work\\Go\\ai\\draw\\任务汇总报告.txt")
    print(report)
    
    # 输出总天数验证
    total_days = summarizer.validate_total_days()
    print(f"\n总天数验证: {total_days:.2f} 天")
    print(f"与目标250天的差异: {total_days - 250:.2f} 天")

if __name__ == "__main__":
    main()