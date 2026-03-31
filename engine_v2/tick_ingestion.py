import redis
import time
import sys

# Load the compiled V2 PyO3 engine
try:
    import market_edge_v2_core
except ImportError as e:
    print(f"[FATAL] Failed to import market_edge_v2_core. Make sure you are running in the correct directory. ERROR: {e}")
    sys.exit(1)

def run_ingestion_test():
    print("🚀 启动 V2 极速行情提取泵 (Redis -> Rust SIMD Bridge) ...")
    
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    engine = market_edge_v2_core.MarketEngine(5500)
    
    registered_symbols = set()
    
    while True:
        t_start = time.time()
        
        # 1. 快速抓取全量健值
        keys = r.keys("stock:quote:*")
        
        if not keys:
            sys.stdout.write(f"\r[等待中] Redis 库没有找到 stock:quote:* 键，正在阻塞等待回放数据... (当前检测时间: {time.strftime('%H:%M:%S')})")
            sys.stdout.flush()
            time.sleep(1)
            continue
            
        # 2. Redis 批量拉取 (Pipeline 避免阻塞)
        pipe = r.pipeline()
        for k in keys:
            pipe.hgetall(k)
        
        all_quotes = pipe.execute()
        t_redis = time.time()
        
        # 3. 剥离并塞入 Rust
        processed_count = 0
        for key, data in zip(keys, all_quotes):
            # Key 格式 "stock:quote:600519"
            symbol = key.replace("stock:quote:", "")
            
            if symbol not in registered_symbols:
                engine.register_symbol(symbol)
                registered_symbols.add(symbol)
                
            try:
                # 兼容旧版的各种字段名
                price = float(data.get('price', data.get('current', 0)))
                amount = float(data.get('amount', data.get('turnover', 0)))
                volume = float(data.get('volume', 0))
                
                # 以后为了完美适配回放，这里需要再传 data.get('time') 给 Rust
                engine.push_tick(symbol, price, amount, volume)
                processed_count += 1
            except (ValueError, TypeError):
                continue
                
        t_rust_push = time.time()
        
        # 4. 触发 Rust 极速快照结算
        snapshot = engine.get_snapshot()
        t_rust_calc = time.time()
        
        # 截取样本展示
        sample_key = next(iter(snapshot.keys())) if snapshot and len(snapshot) > 1 else "N/A"
        sample_res = snapshot.get(sample_key, {})
        
        print(f"\n[脉冲截胡] {time.strftime('%H:%M:%S')} - 成功抓取全市场 {processed_count} 票")
        print(f"  -> Redis 管道拉取耗时: {(t_redis - t_start)*1000:.2f} ms")
        print(f"  -> 丢入 Rust 底座耗时: {(t_rust_push - t_redis)*1000:.2f} ms")
        print(f"  -> SIMD 计算提取耗时: {(t_rust_calc - t_rust_push)*1000:.2f} ms")
        
        if sample_key != "_SECTORS_" and sample_key != "N/A":
             print(f"  -> 样本抽取 [{sample_key}]: {sample_res}")
        else:
             second_key = list(snapshot.keys())[1] if len(snapshot) > 1 else "N/A"
             print(f"  -> 样本抽取 [{second_key}]: {snapshot.get(second_key, {})}")
             
        # 等待 3 秒呼吸周期，防止死循环刷屏
        time.sleep(3)

if __name__ == '__main__':
    run_ingestion_test()
