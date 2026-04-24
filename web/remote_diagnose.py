import paramiko
import time

HOST = '115.190.156.240'
PORT = 22
USER = 'root'
PASSWORD = 'Chao123+'

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(HOST, PORT, USER, PASSWORD, timeout=10)
        print("Connected to remote server: " + HOST)
        
        # 1. Check Redis Keyspace
        _, stdout, _ = ssh.exec_command("redis-cli info keyspace")
        print("\n--- Keyspace Info ---")
        print(stdout.read().decode())
        
        # 2. Scan for recent quote keys
        _, stdout, _ = ssh.exec_command('redis-cli keys "stock:quote:*" | head -n 10')
        keys = stdout.read().decode().strip().split('\n')
        print(f"\n--- Found {len(keys)} quote keys (first 10) ---")
        for k in keys:
            if not k: continue
            _, st_out, _ = ssh.exec_command(f'redis-cli hget "{k}" timestamp')
            ts = st_out.read().decode().strip()
            print(f"Key: {k}, Timestamp: {ts}")
            
        # 3. Check for volatile_pool
        _, stdout, _ = ssh.exec_command('redis-cli type "stock:volatile_pool"')
        vtype = stdout.read().decode().strip()
        print(f"\n--- volatile_pool type: {vtype} ---")
        if vtype == "list":
            _, stdout, _ = ssh.exec_command('redis-cli llen "stock:volatile_pool"')
            print(f"volatile_pool length: {stdout.read().decode().strip()}")
            
        # 4. Check a specific sample code (002310)
        sample_key = "stock:quote:002310"
        _, stdout, _ = ssh.exec_command(f'redis-cli hgetall "{sample_key}"')
        fields = stdout.read().decode().strip().split('\n')
        print(f"\n--- Fields for {sample_key} ---")
        for i in range(0, len(fields), 2):
            if i+1 < len(fields):
                print(f"{fields[i]}: {fields[i+1]}")
                
        # 5. Check if amount_2min exists for any fresh key
        if keys:
            first_key = keys[0]
            _, stdout, _ = ssh.exec_command(f'redis-cli hget "{first_key}" amount_2min')
            amt2 = stdout.read().decode().strip()
            print(f"\n--- amount_2min for {first_key}: {amt2 if amt2 else 'MISSING'} ---")

        ssh.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
