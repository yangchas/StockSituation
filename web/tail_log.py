import paramiko
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def run_remote():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('115.190.156.240', 22, 'root', 'Chao123+')
    
    commands = [
        'tail -n 50 /root/work/web/nohup.out'
    ]
    
    for cmd in commands:
        print(f"--- RUNNING: {cmd} ---")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        print(stdout.read().decode('utf-8', errors='replace'))
        
    ssh.close()

if __name__ == "__main__":
    run_remote()
