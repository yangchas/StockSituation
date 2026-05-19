from __future__ import annotations


def classify_theme_trade_conclusion(
    *,
    theme_trade_label: str,
    open_confirm_state: str,
    fakeout_level: str,
    high_open_fail_count: int = 0,
    low_open_repair_count: int = 0,
    expansion_count: int = 0,
    leader_count: int = 0,
    yest_limit_count: int = 0,
) -> str:
    if theme_trade_label == "high_event":
        if open_confirm_state == "falsified" or fakeout_level == "high":
            return "high_event_self_excited"
        if leader_count >= 1:
            return "leader_only_alive"
        return "high_event_self_excited"

    if theme_trade_label == "old_mainline":
        if open_confirm_state == "falsified" or high_open_fail_count >= max(1, yest_limit_count):
            return "old_mainline_distribution"
        if expansion_count >= 2 and open_confirm_state == "strengthened":
            return "old_mainline_strong_continue"
        if open_confirm_state == "strengthened" or low_open_repair_count >= 1:
            return "old_mainline_weak_continue"
        if leader_count >= 1:
            return "leader_only_alive"
        return "old_mainline_weak_continue"

    if theme_trade_label == "switch_candidate":
        if open_confirm_state == "strengthened" and expansion_count >= 2:
            return "switch_expansion_confirmed"
        if open_confirm_state == "falsified" or fakeout_level == "high":
            return "switch_failed"
        if open_confirm_state == "strengthened" or low_open_repair_count >= 1:
            return "switch_partially_confirmed"
        return "switch_wait_confirm"

    if theme_trade_label == "independent_hug":
        if open_confirm_state == "falsified" or fakeout_level == "high":
            return "independent_hug_failed"
        return "leader_only_alive"

    if open_confirm_state == "falsified":
        return "rotation_noise"
    if open_confirm_state == "strengthened" and expansion_count >= 2:
        return "switch_partially_confirmed"
    return "rotation_noise"
