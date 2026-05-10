#include "quote_calculator.h"

#include <algorithm>
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

    if (tick.limit_up_milli > 0 && tick.px_milli >= tick.limit_up_milli) {
        state.limit_state = LimitState::Up;
    } else if (tick.limit_down_milli > 0 && tick.px_milli <= tick.limit_down_milli) {
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

}  // namespace t1_v2
