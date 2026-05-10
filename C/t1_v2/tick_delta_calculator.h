#pragma once

#include <cstdint>

#include "quote_state.h"
#include "raw_tick.h"
#include "runtime_mode.h"

namespace t1_v2 {

class TickDeltaCalculator {
public:
    void apply_tick(QuoteState& state, const RawTick& tick, MarketPhase phase, int64_t large_order_threshold_yuan) const;

private:
    static bool has_source_delta(const RawTick& tick);
    static int64_t positive_delta(int64_t current, int64_t previous);
    static int64_t signed_large_net(
        int64_t inst_amt_yuan,
        int current_px_milli,
        int previous_px_milli,
        MarketPhase phase,
        int64_t large_order_threshold_yuan
    );
};

}  // namespace t1_v2
