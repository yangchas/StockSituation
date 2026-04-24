import redis
import json

def simulate_arbitrage_scoring():
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    day = '2026-03-06'
    
    extra_data = r.hgetall(f'cache:stock_extra:{day}')
    if not extra_data:
        print("No stock_extra data found.")
        return

    print(f"--- Simulating Arbitrage Score (Market Cap Component) for {day} ---")
    
    results = []
    for code, val_json in list(extra_data.items())[:20]:
        val = json.loads(val_json)
        
        # 模拟 indicators 对象的合并过程
        # 在 market_edge_engine.py 中:
        # ind = dict(cached_indicators.get(code, {}))
        # extra = self.stock_extra.get(code, {})
        # ind.update(extra)
        
        real_market_cap = val.get('real_market_cap', 0)
        market_cap_cpp = val.get('market_cap', 0)
        
        # 逻辑复刻:
        real_cap = real_market_cap  # 假设 indicators 里已经从 extra 合并了
        cap_score = 0
        if real_cap <= 0:
            raw_cap = market_cap_cpp
            if raw_cap > 1000000:
                real_cap = raw_cap / 1e8
            elif raw_cap > 0:
                real_cap = raw_cap
        
        if real_cap >= 50:
            cap_score = 15
        elif real_cap >= 20:
            cap_score = 10
        elif real_cap >= 5:
            cap_score = 5
            
        results.append({
            "code": code,
            "real_cap": real_cap,
            "cap_score": cap_score
        })

    for res in results:
        print(f"Stock: {res['code']} | Final Cap: {res['real_cap']:.2f}亿 | Cap Score Gain: +{res['cap_score']}")

if __name__ == "__main__":
    simulate_arbitrage_scoring()
