import paramiko, json

host = '115.190.156.240'
port = 22
user = 'root'
password = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, user, password)
    
    def run(cmd):
        stdin, stdout, stderr = ssh.exec_command(cmd)
        return stdout.read().decode('utf-8').strip()
    
    today = "2026-02-26"
    
    print("=" * 60)
    print("[1] market:sentiment (5-phase cycle)")
    print(run(f'redis-cli hgetall "market:sentiment:{today}"'))

    print("\n" + "=" * 60)
    print("[2] market:execution_policy")
    raw = run(f'redis-cli hget "market:execution_policy:{today}" explain')
    if raw:
        try:
            data = json.loads(raw)
            for k, v in data.items():
                print(f"  {k}: {v}")
        except:
            print(raw)
    else:
        print("(empty)")

    print("\n" + "=" * 60)
    print("[3] market:strategy_tags")
    print(run(f'redis-cli hgetall "market:strategy_tags:{today}"'))

    print("\n" + "=" * 60)
    print("[4] diag:expectation_eval")
    print(run(f'redis-cli hgetall "diag:expectation_eval:{today}"'))

    print("\n" + "=" * 60)
    print("[5] market:open_scenario")
    raw2 = run(f'redis-cli hget "market:open_scenario:{today}" verification_status')
    print("verification_status:", raw2)
    
    print("\n" + "=" * 60)
    print("[6] market:comfort_exit")
    print(run(f'redis-cli hgetall "market:comfort_exit:{today}"'))
    
    print("\n" + "=" * 60)
    print("[7] rank:plate_attitude (top 10)")
    print(run(f'redis-cli zrevrange "rank:plate_attitude:{today}" 0 9 withscores'))
    
    print("\n" + "=" * 60)
    print("[8] process_profile intraday")
    print(run(f'redis-cli hgetall "market:process_profile:{today}"'))

finally:
    ssh.close()
