import redis

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    
    keys1 = r.keys('limit_up_*')
    keys2 = r.keys('cache:wencai:limitup_lb*')
    keys3 = r.keys('*limit_up*')
    
    print(f"limit_up_* keys: {keys1}")
    print(f"wencai limitup keys: {keys2}")
    print(f"All *limit_up* keys: {keys3}")

if __name__ == '__main__':
    main()
