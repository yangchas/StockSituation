import paramiko
import os

def final_deployment_fix():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    local_dir = r"d:\work\Go\engine_v2"
    remote_dir = "/root/work/market_edge_v2_core"
    offending_dir = "/root/work/engine_v2"
    
    print(f"--- 🚀 正在执行终极部署与环境净化: -> {remote_dir} ---")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        
        # 1. 物理删除产生干扰的旧文件夹
        print(f"  Cleanup: Removing {offending_dir}...")
        ssh.exec_command(f"rm -rf {offending_dir}")
        
        sftp = ssh.open_sftp()
        
        # 2. 镜像同步
        for root, dirs, files in os.walk(local_dir):
            rel_path = os.path.relpath(root, local_dir)
            target_dir = remote_dir if rel_path == "." else os.path.join(remote_dir, rel_path).replace("\\", "/")
            
            try:
                sftp.mkdir(target_dir)
            except:
                pass
            
            for f in files:
                if f.endswith('.pyc') or f == '__pycache__': continue
                l_file = os.path.join(root, f)
                r_file = os.path.join(target_dir, f).replace("\\", "/")
                
                print(f"  Pushing: {f}")
                sftp.put(l_file, r_file)
                
                # 特殊处理 Rust 库文件
                if f == "rust_core_advanced.rs":
                    sftp.put(l_file, os.path.join(target_dir, "src/lib.rs").replace("\\", "/"))

        sftp.close()
        
        # 3. 重新构建并清理僵尸进程
        print("✅ 同步完成，准备执行重建...")
        rebuild_cmd = f"pkill -f python ; cd {remote_dir} && sh build.sh"
        stdin, stdout, stderr = ssh.exec_command(rebuild_cmd)
        print(f"Build Result:\n{stdout.read().decode()}")
        print(f"Build Error:\n{stderr.read().decode()}")
        
    except Exception as e:
        print(f"❌ Deployment Error: {e}")
    finally:
        ssh.close()

final_deployment_fix()
