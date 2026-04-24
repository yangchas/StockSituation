import paramiko

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

def list_keys():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
    
    script = """
import redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
keys = r.keys('cache:chip_peaks:*')
print(f'CHIP_KEYS: {keys}')
for k in keys:
    print(f'{k}: {r.hlen(k)}')
"""
    stdin, stdout, stderr = ssh.exec_command(f'python3 -c "{script}"')
    print(stdout.read().decode())
    ssh.close()

if __name__ == '__main__':
    list_keys()
