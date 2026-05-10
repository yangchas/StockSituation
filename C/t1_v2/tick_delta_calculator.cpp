#include "tick_delta_calculator.h"

#include <cstdlib>

namespace t1_v2 {

void TickDeltaCalculator::apply_tick(
    QuoteState& state,
    const RawTick& tick,
    MarketPhase phase,
    int64_t large_order_threshold_yuan
) const {
    if (has_source_delta(tick)) {
        state.inst_vol = tick.inst_vol;
        state.inst_amt_yuan = tick.inst_amt_yuan;
        state.large_net_yuan = tick.large_net_yuan;
        state.cumulative_large_net_yuan += tick.large_net_yuan;
        return;
    }

    if (state.ts_ms <= 0 || tick.ts_ms <= state.ts_ms) {
        state.inst_vol = 0;
        state.inst_amt_yuan = 0;
        state.large_net_yuan = 0;
        return;
    }

    state.inst_vol = positive_delta(tick.vol_units, state.vol_units);
    state.inst_amt_yuan = positive_delta(tick.amt_yuan, state.amt_yuan);
    state.large_net_yuan = signed_large_net(
        state.inst_amt_yuan,
        tick.px_milli,
        state.px_milli,
        phase,
        large_order_threshold_yuan
    );
    state.cumulative_large_net_yuan += state.large_net_yuan;
}

bool TickDeltaCalculator::has_source_delta(const RawTick& tick) {
    return tick.inst_vol != 0 || tick.inst_amt_yuan != 0 || tick.large_net_yuan != 0;
}

int64_t TickDeltaCalculator::positive_delta(int64_t current, int64_t previous) {
    if (current <= previous || previous <= 0) {
        return 0;
    }
    return current - previous;
}

int64_t TickDeltaCalculator::signed_large_net(
    int64_t inst_amt_yuan,
    int current_px_milli,
    int previous_px_milli,
    MarketPhase phase,
    int64_t large_order_threshold_yuan
) {
    if (phase == MarketPhase::Auction || inst_amt_yuan <= 0 || inst_amt_yuan <= large_order_threshold_yuan) {
        return 0;
    }
    return current_px_milli > previous_px_milli ? inst_amt_yuan : -inst_amt_yuan;
}

}  // namespace t1_v2
