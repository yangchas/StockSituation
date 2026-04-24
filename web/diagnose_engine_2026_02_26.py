"""
2026-02-26 引擎指标诊断脚本
======================================
通过 SSH 连接远程服务器 Redis，检查竞价数据完整性并模拟各阶段指标计算。
"""
import paramiko
import json
import sys
import time

HOST = '115.190.156.240'
PORT = 22
USER = 'root'
PASSWORD = 'Chao123+'
DATE = '2026-02-26'
DATE_COMPACT = DATE.replace('-', '')


def ssh_run(ssh, cmd, timeout=15):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    return out


def rcmd(ssh, cmd):
    return ssh_run(ssh, f"redis-cli -n 0 {cmd}")


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def safe_float(v, default=0.0):
    try:
        if v is None or v == '' or v == 'None' or v == '(nil)':
            return default
        return float(v)
    except:
        return default


def norm_pct(v):
    x = safe_float(v)
    return x * 100.0 if abs(x) <= 1.0 else x


def parse_hgetall(raw):
    """把 redis-cli hgetall 的输出解析为 dict。"""
    if not raw or raw in ('(empty array)', '(empty list or set)'):
        return {}
    lines = raw.split('\n')
    d = {}
    for i in range(0, len(lines) - 1, 2):
        k = lines[i].strip().lstrip('"').rstrip('"')
        v = lines[i + 1].strip().lstrip('"').rstrip('"')
        # redis-cli 带序号时去掉序号  e.g. "1) "
        for prefix in range(1, 100):
            p = f"{prefix}) "
            if k.startswith(p):
                k = k[len(p):].strip('"')
            if v.startswith(p):
                v = v[len(p):].strip('"')
        d[k] = v
    return d


# ═══════════════════════════════════════════════════════════
# 1. 竞价数据完整性
# ═══════════════════════════════════════════════════════════

def check_auction_data(ssh):
    section("1. 竞价快照数据完整性")

    # 扫 key
    k1 = rcmd(ssh, f'keys "market:auction:{DATE_COMPACT}:*"')
    k2 = rcmd(ssh, f'keys "market:auction:{DATE}:*"')
    all_keys = sorted(set(
        k.strip() for k in (k1 + '\n' + k2).split('\n') if k.strip() and k.strip() != '(empty array)'
    ))
    print(f"  检测到 auction key {len(all_keys)} 个:")
    for k in all_keys:
        print(f"    {k}")

    if not all_keys:
        print("  ❌ 未找到竞价数据！所有依赖竞价的指标将无法计算。")
        return None

    # 读取 top_amount
    auction_data = None
    for key in sorted(all_keys, key=lambda x: '0925' not in x):
        raw = rcmd(ssh, f'hget "{key}" top_amount')
        if raw and raw != '(nil)':
            try:
                auction_data = json.loads(raw)
                print(f"\n  ✅ 从 {key} 读取 top_amount: {len(auction_data)} 条")
                break
            except:
                pass
        raw2 = rcmd(ssh, f'get "{key}"')
        if raw2 and raw2 != '(nil)':
            try:
                auction_data = json.loads(raw2)
                print(f"\n  ✅ 从 {key}(string) 读取: {len(auction_data)} 条")
                break
            except:
                pass

    if not auction_data:
        print("  ❌ 所有 key 均无法解析竞价数据")
        return None

    # 字段抽样
    sample = auction_data[:20]
    required = ['symbol', 'change_pct', 'bid_amount_yuan', 'auction_amount_yuan']
    print(f"\n  字段完整性（前{len(sample)}条抽样）:")
    for f in required:
        cnt = sum(1 for it in sample if it.get(f) not in (None, '', 0))
        ok = "✅" if cnt >= len(sample) * 0.8 else "⚠️"
        print(f"    {ok} {f:25s} {cnt}/{len(sample)}")

    # 涨跌分布
    all_auc = [norm_pct(it.get('change_pct', 0)) for it in auction_data]
    n = len(all_auc)
    high_open = sum(1 for x in all_auc if x >= 5.0)
    deep_low  = sum(1 for x in all_auc if x <= -8.0)
    limit_up  = sum(1 for x in all_auc if x >= 9.8)
    bid_ok    = sum(1 for it in auction_data if safe_float(it.get('bid_amount_yuan', 0)) > 0)

    print(f"\n  竞价分布（共{n}条）:")
    print(f"    高开(>=5%)   {high_open:4d}  ({high_open/max(1,n)*100:.1f}%)")
    print(f"    深跌(<=-8%)  {deep_low:4d}  ({deep_low/max(1,n)*100:.1f}%)")
    print(f"    涨停(>=9.8%) {limit_up:4d}  ({limit_up/max(1,n)*100:.1f}%)")
    print(f"    封单有效     {bid_ok:4d}  ({bid_ok/max(1,n)*100:.1f}%)")

    # 计算 regime_a 预期值
    hor = high_open / max(1, n)
    dlr = deep_low / max(1, n)
    if hor >= 0.20 and dlr <= 0.05:
        expected_regime_a = "high_open_consensus"
    elif dlr >= 0.15:
        expected_regime_a = "panic_low_open"
    elif hor >= 0.10 and dlr >= 0.10:
        expected_regime_a = "divergence_auction"
    else:
        expected_regime_a = "neutral_auction"
    print(f"\n  ➡️ 预计 regime_a = {expected_regime_a}")
    print(f"     (high_open_ratio={hor:.3f}, deep_low_ratio={dlr:.3f})")

    return auction_data


# ═══════════════════════════════════════════════════════════
# 2. 引擎核心 Key
# ═══════════════════════════════════════════════════════════

def check_core_keys(ssh):
    section("2. 引擎核心 Redis Key")

    checks = [
        (f"market:sentiment:{DATE}",                 "hgetall", "情绪评分",    ['phase','score']),
        (f"market:strategy_tags:{DATE}",             "hgetall", "策略标签",    ['primary_tag','regimes']),
        (f"diag:expectation_eval:{DATE}",            "hgetall", "预期差评估",  ['effectiveness','fade_count','rise_count','sample_size']),
        (f"market:execution_policy:{DATE}",          "hgetall", "执行策略",    ['position_max','mode_allow','ban_conditions']),
        (f"market:open_scenario:{DATE}",             "hgetall", "开盘验证",    ['verification_status','reason']),
        (f"market:comfort_exit:{DATE}",              "hgetall", "舒服离场",    ['score']),
        (f"market:process_profile:{DATE}",           "hgetall", "过程画像",    ['state','score','repair_strength','risk_strength']),
        (f"market:resonance:{DATE}",                 "hgetall", "共振评分",    ['score','state']),
        (f"market:fear_greed:{DATE}",                "hgetall", "贪婪恐惧",    ['score']),
        (f"market:herding:{DATE}",                   "hgetall", "羊群效应",    ['score']),
        (f"market:operator_advice:{DATE}",           "hgetall", "操作建议",    ['payload']),
        (f"market:strategy_tags:{DATE}:regime_a",    "get",     "regime_a冻结", []),
        (f"market:strategy_tags:{DATE}:plate_comp",  "get",     "plate_comp冻结", []),
    ]

    results = {}
    for key, cmd_type, label, show_fields in checks:
        if cmd_type == "hgetall":
            raw = rcmd(ssh, f'hgetall "{key}"')
            data = parse_hgetall(raw)
            if data:
                print(f"  ✅ {label:14s} {len(data)} 字段")
                for sf in show_fields:
                    val = data.get(sf, '—')
                    if len(str(val)) > 80:
                        val = str(val)[:80] + '...'
                    print(f"       {sf} = {val}")
                results[label] = data
            else:
                print(f"  ❌ {label:14s} 无数据")
                results[label] = {}
        else:
            raw = rcmd(ssh, f'get "{key}"')
            if raw and raw != '(nil)':
                print(f"  ✅ {label:14s} = {raw}")
                results[label] = raw
            else:
                print(f"  ⚠️ {label:14s} 未写入")
                results[label] = None

    return results


# ═══════════════════════════════════════════════════════════
# 3. 模拟 expectation_eval 核心计算
# ═══════════════════════════════════════════════════════════

def simulate_expectation_eval(ssh, auction_data):
    section("3. 模拟 expectation_eval 计算")

    if not auction_data:
        print("  ⚠️ 无竞价数据，跳过")
        return

    rows = []
    for it in auction_data[:200]:
        code = str(it.get('symbol') or '').strip()
        if len(code) != 6:
            continue
        auc = norm_pct(it.get('change_pct', 0))
        bid = safe_float(it.get('bid_amount_yuan', 0))
        aamt = safe_float(it.get('auction_amount_yuan', 0))
        rows.append({'code': code, 'auc': auc, 'bid': bid, 'aamt': aamt})

    print(f"  竞价样本: {len(rows)} 条, 抽取前50只读行情...")

    # 用管道批量读取行情（通过 SSH 逐个 redis-cli）
    evaluated = []
    for x in rows[:50]:
        code = x['code']
        raw = rcmd(ssh, f'hget "stock:quote:{code}" change_pct')
        if raw and raw != '(nil)':
            cur = norm_pct(raw)
            gap = cur - x['auc']
            evaluated.append({**x, 'cur': cur, 'gap': gap})

    if not evaluated:
        print("  ❌ 行情全部缺失, 无法模拟")
        return

    print(f"  成功匹配行情: {len(evaluated)}/{min(50, len(rows))}")

    # 指标计算
    fade = [x for x in evaluated if x['auc'] >= 5.0 and x['gap'] <= -4.0]
    rise = [x for x in evaluated if x['auc'] <= 1.0 and x['gap'] >= 3.0 and x['cur'] > 0]
    high_auc = [x for x in evaluated if x['auc'] >= 5.0]
    low_auc  = [x for x in evaluated if x['auc'] <= 1.0]

    def _avg(arr, key):
        return sum(x[key] for x in arr) / max(1, len(arr)) if arr else 0.0

    high_non_fade = [x for x in high_auc if x not in fade]
    low_non_rise  = [x for x in low_auc if x not in rise]
    fade_adv = _avg(high_non_fade, 'cur') - _avg(fade, 'cur')
    rise_adv = _avg(rise, 'cur') - _avg(low_non_rise, 'cur')
    eff = max(0.0, min(1.0, (fade_adv + rise_adv) / 20.0))

    extreme_low = [x for x in evaluated if x['auc'] <= -8.0]
    extreme_low_rebound = [x for x in extreme_low if x['gap'] >= 5.0 and x['cur'] > -2.0]
    strong_high = [x for x in evaluated if x['auc'] >= 5.0]
    strong_high_hold = [x for x in strong_high if x['cur'] >= x['auc'] - 2.0]

    print(f"\n  模拟结果（sample={len(evaluated)}）:")
    print(f"    effectiveness  = {eff:.4f}")
    print(f"    fade_count     = {len(fade)}")
    print(f"    rise_count     = {len(rise)}")
    print(f"    extreme_low    = {len(extreme_low)}, rebound={len(extreme_low_rebound)}")
    print(f"    strong_high    = {len(strong_high)}, hold={len(strong_high_hold)}")
    print(f"    fade_adv={fade_adv:.2f}  rise_adv={rise_adv:.2f}")

    # 与 Redis 中的值对比
    stored = rcmd(ssh, f'hgetall "diag:expectation_eval:{DATE}"')
    stored_data = parse_hgetall(stored)
    if stored_data:
        s_eff = safe_float(stored_data.get('effectiveness'))
        s_fade = stored_data.get('fade_count', '?')
        s_rise = stored_data.get('rise_count', '?')
        s_sample = stored_data.get('sample_size', '?')
        print(f"\n  Redis 已存值:")
        print(f"    effectiveness  = {s_eff}")
        print(f"    fade_count     = {s_fade}")
        print(f"    rise_count     = {s_rise}")
        print(f"    sample_size    = {s_sample}")
        if str(s_fade) == str(len(fade)) and str(s_rise) == str(len(rise)):
            print(f"  ✅ 模拟值与存储值一致（当前是盘后，值应该相同）")
        else:
            print(f"  ℹ️ 模拟值与存储值不同（可能是样本范围不同: 模拟50 vs 存储{s_sample}）")
    else:
        print(f"\n  ⚠️ Redis 中无 expectation_eval 数据")


# ═══════════════════════════════════════════════════════════
# 4. 策略标签模拟
# ═══════════════════════════════════════════════════════════

def check_strategy_tags(ssh, auction_data, core_results):
    section("4. 策略标签 regime 逻辑验证")

    # 昨日情绪
    prev_date = '2026-02-25'
    prev_raw = rcmd(ssh, f'hgetall "market:sentiment:{prev_date}"')
    prev_data = parse_hgetall(prev_raw)
    prev_phase = prev_data.get('phase', 'unknown')
    prev_proc_raw = rcmd(ssh, f'hgetall "market:process_profile:{prev_date}"')
    prev_proc = parse_hgetall(prev_proc_raw)
    prev_process_state = prev_proc.get('state', 'unknown')

    print(f"  昨日(02-25) phase={prev_phase}, process_state={prev_process_state}")

    if prev_phase == 'consistent' and prev_process_state == 'risk_on':
        regime_y = 'consensus_high'
    elif prev_phase == 'retreat' or prev_process_state == 'risk_off':
        regime_y = 'retreat'
    elif prev_phase in ('repair', 'ice_point'):
        regime_y = 'repair'
    else:
        regime_y = 'mixed'
    print(f"  ➡️ regime_y = {regime_y}")

    # regime_a 冻结检查
    frozen_a = core_results.get('regime_a冻结')
    if frozen_a:
        print(f"  ✅ regime_a 已冻结: {frozen_a}")
    else:
        print(f"  ⚠️ regime_a 未冻结（新代码尚未运行或当天未触发 strategy_tags）")

    # plate_comp 冻结检查
    frozen_pc = core_results.get('plate_comp冻结')
    if frozen_pc:
        print(f"  ✅ plate_comp 已冻结: {frozen_pc}")
    else:
        print(f"  ⚠️ plate_comp 未冻结（同上）")

    # 当前策略标签
    tags_data = core_results.get('策略标签', {})
    if tags_data:
        p_tag = tags_data.get('primary_tag', '?')
        regimes_raw = tags_data.get('regimes', '{}')
        try:
            regimes = json.loads(regimes_raw)
        except:
            regimes = {}
        print(f"\n  当前策略标签: {p_tag}")
        print(f"  regimes: {json.dumps(regimes, ensure_ascii=False, indent=4)}")
    else:
        print(f"\n  ⚠️ 无策略标签数据")


# ═══════════════════════════════════════════════════════════
# 5. 执行策略诊断
# ═══════════════════════════════════════════════════════════

def check_execution_policy(ssh, core_results):
    section("5. 执行策略诊断")

    policy = core_results.get('执行策略', {})
    if not policy:
        print("  ❌ 无执行策略数据")
        return

    pos = safe_float(policy.get('position_max'))
    mode_raw = policy.get('mode_allow', '[]')
    ban_raw = policy.get('ban_conditions', '[]')
    explain_raw = policy.get('explain', '{}')

    try:
        modes = json.loads(mode_raw)
    except:
        modes = [mode_raw]
    try:
        bans = json.loads(ban_raw)
    except:
        bans = [ban_raw]
    try:
        explain = json.loads(explain_raw)
    except:
        explain = {}

    phase = explain.get('phase', '?')
    stale = explain.get('stale', '?')

    print(f"  position_max = {pos}")
    print(f"  mode_allow   = {modes}")
    print(f"  phase        = {phase}")
    print(f"  stale        = {stale}")
    print(f"  ban_conditions = {bans}")

    # 验证 repair->start 修复效果
    if phase == 'repair':
        if pos >= 0.35:
            print(f"\n  ✅ phase=repair, pos={pos} >= 0.35 — 修复已生效(等同start)")
        else:
            print(f"\n  ⚠️ phase=repair, pos={pos} < 0.35 — 修复可能未生效或被其他因素压低")
    elif phase == 'start':
        print(f"\n  ℹ️ phase=start, pos={pos}")

    # 操作建议
    advice_data = core_results.get('操作建议', {})
    if advice_data:
        payload_raw = advice_data.get('payload', '{}')
        try:
            advice = json.loads(payload_raw)
            print(f"\n  操作建议:")
            print(f"    action     = {advice.get('action', '?')}")
            print(f"    risk_level = {advice.get('risk_level', '?')}")
            print(f"    long板块   = {advice.get('long_plates', [])}")
            print(f"    avoid板块  = {advice.get('avoid_plates', [])}")
            reasons = advice.get('reason', [])
            for r_msg in reasons[:5]:
                print(f"    理由: {r_msg}")
        except:
            pass


# ═══════════════════════════════════════════════════════════
# 6. 综合诊断报告
# ═══════════════════════════════════════════════════════════

def print_summary(auction_data, core_results):
    section("6. 综合诊断总结")

    issues = []
    ok_items = []

    # 竞价数据
    if auction_data and len(auction_data) >= 100:
        ok_items.append(f"竞价快照 {len(auction_data)} 条")
    elif auction_data:
        issues.append(f"竞价快照偏少: {len(auction_data)} 条")
    else:
        issues.append("竞价快照完全缺失")

    # 核心key
    essential = ['情绪评分', '策略标签', '预期差评估', '执行策略', '舒服离场']
    for label in essential:
        data = core_results.get(label, {})
        if data:
            ok_items.append(label)
        else:
            issues.append(f"{label} 无数据")

    # 冻结key
    if core_results.get('regime_a冻结'):
        ok_items.append("regime_a冻结")
    else:
        issues.append("regime_a未冻结（新代码尚未运行）")
    if core_results.get('plate_comp冻结'):
        ok_items.append("plate_comp冻结")
    else:
        issues.append("plate_comp未冻结（新代码尚未运行）")

    # phase=repair 检查
    policy = core_results.get('执行策略', {})
    if policy:
        try:
            explain = json.loads(policy.get('explain', '{}'))
            if explain.get('phase') == 'repair':
                pos = safe_float(policy.get('position_max'))
                if pos >= 0.35:
                    ok_items.append(f"repair兼容修复生效(pos={pos})")
                else:
                    issues.append(f"repair仍映射低仓位(pos={pos})")
        except:
            pass

    print("  ✅ 正常项:")
    for item in ok_items:
        print(f"     {item}")
    print()
    if issues:
        print("  ⚠️ 需关注:")
        for item in issues:
            print(f"     {item}")
    else:
        print("  🎉 全部指标正常!")


# ═══════════════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════════════

def main():
    print(f"引擎诊断: {DATE} | 服务器: {HOST}")
    print(f"{'─'*60}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(HOST, PORT, USER, PASSWORD, timeout=10)
        print("✅ SSH连接成功")
    except Exception as e:
        print(f"❌ SSH连接失败: {e}")
        sys.exit(1)

    try:
        # 1. 竞价数据
        auction_data = check_auction_data(ssh)

        # 2. 核心 key
        core_results = check_core_keys(ssh)

        # 3. 模拟 expectation_eval
        simulate_expectation_eval(ssh, auction_data)

        # 4. 策略标签逻辑
        check_strategy_tags(ssh, auction_data, core_results)

        # 5. 执行策略
        check_execution_policy(ssh, core_results)

        # 6. 总结
        print_summary(auction_data, core_results)

    except Exception as e:
        print(f"\n❌ 诊断过程异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        ssh.close()
        print(f"\n{'─'*60}")
        print("SSH连接已关闭")


if __name__ == "__main__":
    main()
