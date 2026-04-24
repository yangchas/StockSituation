#!/usr/bin/env python3
"""
复制板块数据文件的脚本
"""
import shutil
import os

def copy_plate_data():
    try:
        # 源文件路径
        plate_csv_src = r"d:\work\Go\plate\data\板块.csv"
        relation_csv_src = r"d:\work\Go\plate\data\个股板块.csv"
        
        # 目标文件路径
        plate_csv_dst = r"d:\work\Go\web\data\板块.csv"
        relation_csv_dst = r"d:\work\Go\web\data\个股板块.csv"
        
        # 确保目标目录存在
        os.makedirs(os.path.dirname(plate_csv_dst), exist_ok=True)
        
        # 复制文件
        print("📋 复制板块数据文件...")
        
        if os.path.exists(plate_csv_src):
            shutil.copy2(plate_csv_src, plate_csv_dst)
            print(f"✅ 已复制: {plate_csv_src} → {plate_csv_dst}")
        else:
            print(f"❌ 源文件不存在: {plate_csv_src}")
            return False
            
        if os.path.exists(relation_csv_src):
            shutil.copy2(relation_csv_src, relation_csv_dst)
            print(f"✅ 已复制: {relation_csv_src} → {relation_csv_dst}")
        else:
            print(f"❌ 源文件不存在: {relation_csv_src}")
            return False
            
        print("✅ 板块数据文件复制完成")
        return True
        
    except Exception as e:
        print(f"❌ 复制文件失败: {e}")
        return False

if __name__ == "__main__":
    copy_plate_data()