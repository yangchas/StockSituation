import paramiko
import os

def run_server_audit():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    local_file = r"d:\work\Go\tmp_server_audit.py"
    remote_file = "/root/work/engine_v2/v2_server_audit.py"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print(f"--- 🩺 正在执行服务器原位诊断程序 ---")
        
        # 1. 上传诊断器
        sftp = ssh.open_sftp()
        sftp.put(local_file, remote_file)
        sftp.close()
        
        # 2. 执行审计并捕获输出
        cmd = f"cd /root/work/engine_v2 && python3 v2_server_audit.py"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        print(stdout.read().decode())
        print(stderr.read().decode())
        
    except Exception as e:
        print(f"❌ Server Audit Error: {e}")
    finally:
        ssh.close()

run_server_audit()
