import paramiko
import os
import sys

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

def deploy():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
    sftp = ssh.open_sftp()
    
    files = [
        ('d:/work/Go/web/services/chip_batch_runner.py', '/root/work/web/services/chip_batch_runner.py'),
    ]
    
    for local, remote in files:
        sftp.put(local, remote)
        print(f"Uploaded {local} to {remote}")
        
    sftp.close()
    
    # Run a quick validation
    print("Running quick validation on server...")
    stdin, stdout, stderr = ssh.exec_command('python3 -c "import sys; sys.path.insert(0, \'/root/work\'); from web.services.chip_batch_runner import ChipBatchRunner; print(\'Import OK\')"')
    print(stdout.read().decode())
    
    ssh.close()

if __name__ == '__main__':
    deploy()
