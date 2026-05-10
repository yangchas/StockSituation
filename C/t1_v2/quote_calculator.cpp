#include "quote_calculator.h"

#include <algorithm>
#include <cstdlib>
#include <cstdint>

namespace t1_v2 {

void QuoteCalculator::apply_base_tick(QuoteState& state, const RawTick& tick, MarketPhase phase) const {
    state.ts_ms = tick.ts_ms;
    state.phase = phase;
    state.px_milli = tick.px_milli;
    state.pc_milli = tick.pc_milli;
    state.amt_yuan = tick.amt_yuan;
    state.vol_units = tick.vol_units;

    if (state.mx_milli <= 0 || tick.px_milli > state.mx_milli) {
        state.mx_milli = tick.px_milli;
    }
    if (state.mn_milli <= 0 || (tick.px_milli > 0 && tick.px_milli < state.mn_milli)) {
        state.mn_milli = tick.px_milli;
    }

    const int limit_up_milli = tick.limit_up_milli > 0
        ? tick.limit_up_milli
        : calc_limit_price_milli(tick, true);
    const int limit_down_milli = tick.limit_down_milli > 0
        ? tick.limit_down_milli
        : calc_limit_price_milli(tick, false);

    if (limit_up_milli > 0 && near_price(tick.px_milli, limit_up_milli)) {
        state.limit_state = LimitState::Up;
    } else if (limit_down_milli > 0 && near_price(tick.px_milli, limit_down_milli)) {
        state.limit_state = LimitState::Down;
    } else {
        state.limit_state = LimitState::Normal;
    }
}

void QuoteCalculator::recompute_fast_metrics(QuoteState& state) const {
    state.spd1m_bp = calc_speed_bp(state, 1);
    state.vec3m_bp = calc_speed_bp(state, 3);
    state.vec5m_bp = calc_speed_bp(state, 5);
    state.amt2m_yuan = calc_amount_delta_yuan(state, 2);
    state.amt5m_yuan = calc_amount_delta_yuan(state, 5);
}

int QuoteCalculator::calc_speed_bp(const QuoteState& state, int lookback_minutes) const {
    if (state.px_milli <= 0 || state.minute_ring.latest_minute < 0) {
        return 0;
    }
    const int target_minute = state.minute_ring.latest_minute - lookback_minutes;
    const MinuteSlot* slot = ring_reader_.get_slot(state.minute_ring, target_minute);
    if (!slot || slot->px_milli <= 0) {
        return 0;
    }
    const int64_t delta = static_cast<int64_t>(state.px_milli) - slot->px_milli;
    return static_cast<int>((delta * 10000) / slot->px_milli);
}

int64_t QuoteCalculator::calc_amount_delta_yuan(const QuoteState& state, int lookback_minutes) const {
    if (state.amt_yuan <= 0 || state.minute_ring.latest_minute < 0) {
        return 0;
    }
    const MinuteSlot* slot = ring_reader_.find_rolling_amount_reference(
        state.minute_ring,
        state.minute_ring.latest_minute,
        lookback_minutes
    );
    if (!slot) {
        return 0;
    }
    return std::max<int64_t>(0, state.amt_yuan - slot->amt_yuan);
}

int QuoteCalculator::calc_limit_price_milli(const RawTick& tick, bool upper) {
    if (tick.pc_milli <= 0) {
        return 0;
    }
    const bool pct20 = is_20pct_symbol(tick.symbol);
    const int ratio_x10 = upper ? (pct20 ? 12 : 11) : (pct20 ? 8 : 9);
    const int target_cent = static_cast<int>((static_cast<int64_t>(tick.pc_milli) * ratio_x10 + 50) / 100);
    return target_cent * 10;
}

bool QuoteCalculator::is_20pct_symbol(const char* symbol) {
    if (!symbol || symbol[0] == '\0' || symbol[1] == '\0') {
        return false;
    }
    return (symbol[0] == '3' && symbol[1] == '0') ||
           (symbol[0] == '6' && symbol[1] == '8');
}

bool QuoteCalculator::near_price(int lhs_milli, int rhs_milli) {
    if (lhs_milli <= 0 || rhs_milli <= 0) {
        return false;
    }
    return std::abs(lhs_milli - rhs_milli) <= 10;
}

}  // namespace t1_v2
