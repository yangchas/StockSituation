import paramiko
import os

def sync_to_server():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    # 获取本地核心文件
    files_to_sync = [
        "engine_v2/v2_wrp_final.py",
        "engine_v2/v2_orc_final.py",
        "engine_v2/rust_core_advanced.rs"
    ]
    
    print(f"--- 🚀 正在同步核心代码到 {host} ---")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        sftp = ssh.open_sftp()
        
        path_prefix = "/root/work/market_edge_v2_core/"
        local_prefix = "d:/work/Go/"
        
        for f in files_to_sync:
            local_path = local_prefix + f
            # 兼容服务器上的路径结构，如果都在根目录下：
            remote_name = os.path.basename(f)
            # 在这种场景下，用户通常是将 rust 文件重命名为项目需要的名称
            if remote_name == "rust_core_advanced.rs":
                remote_name = "src/lib.rs" # 假设在 src 目录下
            
            remote_path = path_prefix + remote_name
            print(f"  Uploading {f} -> {remote_path}")
            sftp.put(local_path, remote_path)
        
        sftp.close()
        print("✅ 同步完成，准备执行回退编译...")
        
        # 触发编译流程
        stdin, stdout, stderr = ssh.exec_command(f"cd {path_prefix} && sh build.sh")
        print(f"Build Result:\n{stdout.read().decode()}")
        print(f"Build Error:\n{stderr.read().decode()}")
        
    except Exception as e:
        print(f"❌ Sync Error: {e}")
    finally:
        ssh.close()

sync_to_server()
