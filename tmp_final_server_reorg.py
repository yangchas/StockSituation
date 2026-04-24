import paramiko

def final_server_reorg():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    old_dir = "/root/work/market_edge_v2_core"
    new_dir = "/root/work/engine_v2"
    
    print(f"--- 🚀 正在执行服务器架构重组: {old_dir} -> {new_dir} ---")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        
        # 1. 重命名目录 (如果旧目录存在且新目录不存在)
        cmd_rename = f"mv {old_dir} {new_dir}"
        ssh.exec_command(cmd_rename)
        print(f"  Directory Renamed to {new_dir}")
        
        # 2. 更新 build.sh 脚本内容 (路径适配)
        build_script = f"""#!/bin/bash
export RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup
export RUSTUP_UPDATE_ROOT=https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup
export CARGO_BUILD_JOBS=1
export RUSTFLAGS="-C codegen-units=1"
source $HOME/.cargo/env
cd {new_dir}
cargo build --release
cp target/release/libmarket_edge_v2_core.so ./market_edge_v2_core.so
echo "BUILD_SUCCESS"
"""
        # 通过 SSH 写入新的 build.sh
        sftp = ssh.open_sftp()
        with sftp.file(f"{new_dir}/build.sh", "w") as f:
            f.write(build_script)
        
        # 3. 物理确认 src 目录存在 (为用户 SFTP 同步打桩)
        ssh.exec_command(f"mkdir -p {new_dir}/src")
        
        # 4. 最后清理远程垃圾文件
        ssh.exec_command(f"rm -f {new_dir}/test_*.py {new_dir}/debug_*.py {new_dir}/temp_*.py")
        
        sftp.close()
        print("✅ 服务器环境重组完成。")
        
    except Exception as e:
        print(f"❌ Server Reorg Error: {e}")
    finally:
        ssh.close()

final_server_reorg()
