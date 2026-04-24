import paramiko

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

def run_test():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
    
    # Trigger calculation for 2026-03-25
    command = "cd /root/work && PYTHONPATH=/root/work python3 -m web.services.chip_batch_runner 2026-03-25 >> /tmp/chip_batch_manual.log 2>&1 &"
    ssh.exec_command(command)
    print("🚀 Triggered manual run for 2026-02-26 in background.")
    ssh.close()

if __name__ == '__main__':
    run_test()
