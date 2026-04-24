#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复板块数据文件编码问题
"""

import chardet
import csv

def detect_encoding(file_path):
    """检测文件编码"""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def fix_csv_encoding(file_path, output_path=None):
    """修复CSV文件编码问题"""
    if output_path is None:
        output_path = file_path
    
    # 检测原始编码
    encoding = detect_encoding(file_path)
    print(f"检测到文件 {file_path} 的编码: {encoding}")
    
    # 尝试用不同的编码读取文件
    encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'latin1', 'cp1252']
    
    for enc in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                content = f.read()
                print(f"✓ 使用 {enc} 编码成功读取文件")
                
                # 保存为UTF-8编码
                with open(output_path, 'w', encoding='utf-8') as out_f:
                    out_f.write(content)
                print(f"✓ 文件已转换为UTF-8编码保存到 {output_path}")
                return True
                
        except UnicodeDecodeError as e:
            print(f"✗ 使用 {enc} 编码读取失败: {e}")
        except Exception as e:
            print(f"✗ 使用 {enc} 编码读取时发生错误: {e}")
    
    print("❌ 所有编码尝试都失败了")
    return False

def main():
    """主函数"""
    files_to_fix = [
        "d:\\work\\Go\\web\\data\\板块.csv",
        "d:\\work\\Go\\web\\data\\个股板块.csv"
    ]
    
    for file_path in files_to_fix:
        print(f"\n🔧 处理文件: {file_path}")
        if fix_csv_encoding(file_path):
            print(f"✅ {file_path} 编码修复成功")
        else:
            print(f"❌ {file_path} 编码修复失败")

if __name__ == "__main__":
    main()