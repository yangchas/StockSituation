import requests
import csv
import time
from datetime import datetime

def fetch_weekly_reports(time_code):
    """
    获取指定timeCode的周报数据
    """
    base_url = "http://taskcoll.simforge.cn/prod-api/performance/weeklydetl/list"
    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzUxMiJ9.eyJsb2dpbl91c2VyX2tleSI6IjIzYmVlNjc3LTk5MTgtNDRmMC1hZWUwLWE4NjQ5NDU5MWVhMiJ9.MIi6TlZ25-E8_fH-RJOqOuZti7UrolnBXBx5Wiq2N7pNgC8-KTbf8XHFJgvdyXBxA76EmcSZFYI9EGJNYILdhA",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        "Cookie": "Admin-Token=eyJhbGciOiJIUzUxMiJ9.eyJsb2dpbl91c2VyX2tleSI6IjIzYmVlNjc3LTk5MTgtNDRmMC1hZWUwLWE4NjQ5NDU5MWVhMiJ9.MIi6TlZ25-E8_fH-RJOqOuZti7UrolnBXBx5Wiq2N7pNgC8-KTbf8XHFJgvdyXBxA76EmcSZFYI9EGJNYILdhA; sidebarStatus=0"
    }
    
    params = {
        "pageNum": 1,
        "pageSize": 1000,  # 设置为较大的值，确保获取所有数据
        "timeCode": time_code
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") == 200:
            items = data.get("data", [])
            return items
        else:
            print(f"请求 {time_code} 失败，响应码: {data.get('code')}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"请求 {time_code} 时发生错误: {e}")
        return []
    except Exception as e:
        print(f"处理 {time_code} 数据时发生错误: {e}")
        return []

def generate_all_time_codes(year=2025):
    """
    生成指定年份的所有时间码
    格式: YYYYMMWW (年份4位 + 月份2位 + 周数2位)
    每月最多5周，共12个月，最多60个时间码
    """
    time_codes = []
    for month in range(1, 13):
        for week in range(1, 6):  # 每月最多5周
            time_code = f"{year}{month:02d}{week:02d}"
            time_codes.append(time_code)
    return time_codes

def extract_required_fields(item):
    """
    从原始数据中提取所需字段
    """
    return {
        "name": item.get("name", ""),
        "projectName": item.get("projectName", ""),
        "stageName": item.get("stageName", ""),
        "taskName": item.get("taskName", ""),
        "workDays": item.get("workDays", 0),
        "description1": item.get("description1", ""),
        "description2": item.get("description2", ""),
        "description": item.get("description", ""),
        "timeCode": item.get("timeCode", ""),
        "userName": item.get("userName", ""),  # 额外添加：人员姓名
        "createTime": item.get("createTime", "")  # 额外添加：创建时间
    }

def save_to_csv(data, filename):
    """
    将数据保存为CSV文件
    """
    if not data:
        print("没有数据可保存")
        return
    
    # 定义CSV文件的字段名
    fieldnames = [
        "timeCode", "name", "projectName", "stageName", "taskName", 
        "workDays", "description1", "description2", "description",
        "userName", "createTime"
    ]
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"数据已保存到 {filename}，共 {len(data)} 条记录")
    except Exception as e:
        print(f"保存CSV文件时发生错误: {e}")

def main():
    # 配置要获取的年份
    target_year = 2025
    
    print(f"开始获取 {target_year} 年所有周报数据...")
    
    # 生成所有时间码
    time_codes = generate_all_time_codes(target_year)
    print(f"将尝试 {len(time_codes)} 个时间码: 从 {time_codes[0]} 到 {time_codes[-1]}")
    print("-" * 60)
    
    all_reports = []
    successful_count = 0
    
    # 遍历每个时间码获取数据
    for i, time_code in enumerate(time_codes, 1):
        print(f"正在获取时间码 {time_code} 的数据 ({i}/{len(time_codes)})...")
        
        reports = fetch_weekly_reports(time_code)
        
        if reports:
            # 提取所需字段
            extracted_reports = [extract_required_fields(item) for item in reports]
            all_reports.extend(extracted_reports)
            successful_count += 1
            print(f"  √ 获取到 {len(reports)} 条数据")
        else:
            print(f"  × 没有数据或请求失败")
        
        # 添加延迟，避免请求过快
        time.sleep(0.3)
    
    # 保存到CSV文件
    if all_reports:
        # 按时间码排序
        all_reports.sort(key=lambda x: x.get("timeCode", ""))
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weekly_reports_{target_year}_{timestamp}.csv"
        save_to_csv(all_reports, filename)
        
        # 显示详细统计信息
        print("\n" + "=" * 60)
        print(f"数据获取完成！")
        print(f"成功请求的时间码数: {successful_count}/{len(time_codes)}")
        print(f"总记录数: {len(all_reports)}")
        print(f"文件已保存为: {filename}")
        print("=" * 60)
        
        # 按时间码分组统计
        timecode_stats = {}
        for report in all_reports:
            tc = report["timeCode"]
            timecode_stats[tc] = timecode_stats.get(tc, 0) + 1
        
        print("\n按时间码分布 (仅显示有数据的):")
        for tc, count in sorted(timecode_stats.items()):
            print(f"  {tc}: {count} 条记录")
        
        # 统计项目分布
        project_stats = {}
        for report in all_reports:
            project = report["projectName"]
            if project:
                project_stats[project] = project_stats.get(project, 0) + 1
        
        print(f"\n涉及 {len(project_stats)} 个项目:")
        for project, count in sorted(project_stats.items(), key=lambda x: x[1], reverse=True)[:10]:  # 只显示前10
            print(f"  {project}: {count} 条记录")
        
        # 统计人员分布
        user_stats = {}
        for report in all_reports:
            user = report["userName"]
            if user:
                user_stats[user] = user_stats.get(user, 0) + 1
        
        print(f"\n涉及 {len(user_stats)} 位人员:")
        for user, count in sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:10]:  # 只显示前10
            print(f"  {user}: {count} 条记录")
            
        # 显示数据预览
        print("\n数据预览 (前5条):")
        for i, report in enumerate(all_reports[:5]):
            print(f"{i+1}. {report['timeCode']} - {report['userName']} - {report['projectName']}")
            print(f"   任务: {report['taskName']}")
            print(f"   工作天数: {report['workDays']}")
            print()
            
    else:
        print(f"\n没有获取到任何数据，请检查网络连接或Token是否有效")

if __name__ == "__main__":
    main()