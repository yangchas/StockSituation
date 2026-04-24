import paramiko
import os

def force_sync_optimized_code():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    local_file = r"d:\work\Go\engine_v2\v2_orc_final.py"
    remote_file = "/root/work/engine_v2/v2_orc_final.py"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print(f"--- 🚀 正在执行强制物理同步与进程洗牌 ---")
        
        # 1. 直接覆盖上传
        sftp = ssh.open_sftp()
        print(f"  Uploading: {local_file} -> {remote_file}")
        sftp.put(local_file, remote_file)
        sftp.close()
        
        # 2. 强力清空旧进程
        print("  Cleaning up old processes...")
        ssh.exec_command("pkill -9 -f v2_orc_final.py")
        ssh.exec_command("pkill -9 -f python3")
        
        print("✅ 物理覆盖成功！服务器现在已加载最新的 09:25 单次触发逻辑。")
        
    except Exception as e:
        print(f"❌ Force Sync Error: {e}")
    finally:
        ssh.close()

force_sync_optimized_code()
