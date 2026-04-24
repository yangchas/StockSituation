import paramiko
import os

def full_sync_to_server():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    local_dir = r"d:\work\Go\engine_v2"
    remote_dir = "/root/work/market_edge_v2_core"
    
    print(f"--- 🚀 正在执行全量代码同步: {local_dir} -> {remote_dir} ---")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        sftp = ssh.open_sftp()
        
        # 1. 递归上传
        for root, dirs, files in os.walk(local_dir):
            # 获取相对路径
            rel_path = os.path.relpath(root, local_dir)
            if rel_path == ".":
                target_dir = remote_dir
            else:
                target_dir = os.path.join(remote_dir, rel_path).replace("\\", "/")
            
            # 确保远程目录存在
            try:
                sftp.mkdir(target_dir)
                print(f"  Created Dir: {target_dir}")
            except:
                pass
            
            for f in files:
                l_file = os.path.join(root, f)
                r_file = os.path.join(target_dir, f).replace("\\", "/")
                
                # 跳过 pyc 和无用文件
                if f.endswith('.pyc') or f == '__pycache__': continue
                
                print(f"  Pushing: {f}")
                sftp.put(l_file, r_file)
                
                # 额外逻辑：如果文件是 rust_core_advanced.rs，同步更新到 src/lib.rs
                if f == "rust_core_advanced.rs":
                    lib_path = os.path.join(target_dir, "src/lib.rs").replace("\\", "/")
                    try:
                        sftp.put(l_file, lib_path)
                        print(f"  Syncing Rust Core -> {lib_path}")
                    except:
                        pass

        sftp.close()
        print("✅ 全量同步完成")
        
        # 2. 编译并清理多余的 old 文件
        build_cmd = f"cd {remote_dir} && sh build.sh"
        print(f"Executing: {build_cmd}")
        stdin, stdout, stderr = ssh.exec_command(build_cmd)
        print(f"Build Result:\n{stdout.read().decode()}")
        print(f"Build Error:\n{stderr.read().decode()}")
        
    except Exception as e:
        print(f"❌ Full Sync Error: {e}")
    finally:
        ssh.close()

full_sync_to_server()
