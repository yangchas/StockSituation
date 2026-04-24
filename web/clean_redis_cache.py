import paramiko

host = '115.190.156.240'
port = 22
user = 'root'
password = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, user, password)
    
    # 构造能够清除今天相关脏数据的 Redis 命令
    redis_cmd = """
redis-cli keys "market:sentiment:*" | xargs redis-cli del && \
redis-cli keys "rank:plate_attitude:*" | xargs redis-cli del && \
redis-cli keys "market:execution_policy:*" | xargs redis-cli del
"""
    print("正在清理 Redis 中的旧版缓存...")
    stdin, stdout, stderr = ssh.exec_command(redis_cmd)
    
    out = stdout.read().decode('utf-8').strip()
    err = stderr.read().decode('utf-8').strip()
    
    if out:
        print("清理输出:", out)
    if err:
        print("注意:", err)
        
    print("✅ 缓存数据销毁完毕，新引擎将在下一分钟重新生成干净的数据！")
    
finally:
    ssh.close()
