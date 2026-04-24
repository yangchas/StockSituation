import paramiko
import os

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

def recover():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(HOST, 22, USER, PASSWORD, timeout=15)
        
        sftp = ssh.open_sftp()
        remote_path = '/root/work/web/services/chip_batch_runner.py'
        local_path = 'd:/work/Go/web/services/chip_batch_runner_recovered.py'
        
        print(f"Downloading {remote_path}...")
        sftp.get(remote_path, local_path)
        sftp.close()
        ssh.close()
        print(f"✅ Successfully recovered to {local_path}")
        
        with open(local_path, 'r', encoding='utf-8') as f:
            content = f.read()
            print(f"File size: {len(content)} bytes")
            print("First 100 lines preview:")
            print("\n".join(content.splitlines()[:100]))
            
            if 'def calculate_chip_peak' in content:
                print("\n✨ FOUND calculate_chip_peak!")
            else:
                print("\n❌ STILL MISSING calculate_chip_peak in remote file!")
                
    except Exception as e:
        print(f"❌ Recovery failed: {e}")

if __name__ == "__main__":
    recover()
