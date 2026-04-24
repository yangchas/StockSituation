import paramiko

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

def trigger_v2():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
    
    # Create a wrapper script
    wrapper = """
cd /root/work
export PYTHONPATH=/root/work
python3 -m web.services.chip_batch_runner 2026-02-26 > /tmp/chip_batch_v2.log 2>&1
"""
    sftp = ssh.open_sftp()
    with sftp.open('/root/work/run_chip.sh', 'w') as f:
        f.write(wrapper)
    sftp.chmod('/root/work/run_chip.sh', 0o755)
    sftp.close()
    
    # Run it in background using nohup
    ssh.exec_command('nohup /root/work/run_chip.sh > /dev/null 2>&1 &')
    print("🚀 Triggered via run_chip.sh in background.")
    ssh.close()

if __name__ == '__main__':
    trigger_v2()
