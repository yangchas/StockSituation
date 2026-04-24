        codes = [it.get('symbol') for it in core if it.get('symbol')]

        # 批量获取实时行情
        pipe = self.redis.pipeline()
        for code in codes:
            pipe.hgetall(f"stock:quote:{code}")
        quotes = await pipe.execute()

        confirmation_score = 0
        rejection_score = 0
        danger_list: List[Dict[str, Any]] = []
        surprise_list: List[Dict[str, Any]] = []

        for i, it in enumerate(core):
            code = it.get('symbol')
            if not code:
                continue

            # 竞价预期涨幅
            try:
                auction_change_pct = float(it.get('change_pct', 0) or 0)
            except Exception:
                auction_change_pct = 0.0

            q = quotes[i] or {}
            live_change = q.get('change_pct', None)
            if live_change is None or live_change == "":
                live_change = q.get('change', 0)
            try:
                live_change_pct = float(live_change or 0)
            except Exception:
                live_change_pct = 0.0

            if auction_change_pct > 5.0:
                # 强预期票：看开盘是否维持
                if live_change_pct >= auction_change_pct - 2.0:
                    confirmation_score += 1
                elif live_change_pct < auction_change_pct - 4.0:
                    rejection_score += 1
                    danger_list.append({
                        "ts": int(time.time() * 1000),
                        "type": "high_open_fade",
                        "target": code,
                        "reason": f"竞价 {auction_change_pct:.1f}% -> 开盘 {live_change_pct:.1f}%",
                    })
            
            # Logic for Surprise (Exceed Expectation)
            if live_change_pct > auction_change_pct + 3.0:
                 surprise_list.append({
                    "ts": int(time.time() * 1000),
                    "type": "low_open_rise",
                    "target": code,
                    "reason": f"竞价 {auction_change_pct:.1f}% -> 开盘 {live_change_pct:.1f}%",
                })

        # Log details about expectation difference
        if danger_list:
            # reason string format: "竞价 9.0% -> 开盘 2.0%" -> split(' ') -> [0:竞价, 1:9.0%, 2:->, 3:开盘, 4:2.0%]
            danger_logs = [f"{d['target']}({d['reason'].split(' ')[1]}->{d['reason'].split(' ')[4]})" for d in danger_list[:5]]
            logger.info(f"⚠️ 竞价不及预期(高开低走): {', '.join(danger_logs)}")
            
        if surprise_list:
            surprise_logs = [f"{d['target']}({d['reason'].split(' ')[1]}->{d['reason'].split(' ')[4]})" for d in surprise_list[:5]]
            logger.info(f"🚀 竞价超预期(低开高走): {', '.join(surprise_logs)}")

        scenario_data: Dict[str, Any] = {
            "ts": int(time.time() * 1000),
            "verification_status": "uncertain",
            "confidence": 0.5,
            "reason": "竞价核心票强度不足或表现平稳",
        }
