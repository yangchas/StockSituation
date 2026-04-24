import paramiko

def run():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, user, password)
        sftp = ssh.open_sftp()
        
        test_script = """
import redis

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    res = r.hgetall("market:sentiment:20260225")
    print("REDIS LENGTH:", len(res))
    for k, v in res.items():
        print(f"| {k} => {v}")

if __name__ == '__main__':
    main()
"""
        sftp.open('/root/work/web/test_peek_redis.py', 'w').write(test_script)
        
        stdin, stdout, stderr = ssh.exec_command('cd /root/work/web && python test_peek_redis.py')
        
        print("OUT:\n" + stdout.read().decode('utf-8'))
        print("ERR:\n" + stderr.read().decode('utf-8'))
            
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
