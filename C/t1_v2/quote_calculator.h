#pragma once

#include "minute_ring_updater.h"
#include "quote_state.h"
#include "raw_tick.h"
#include "runtime_mode.h"

namespace t1_v2 {

class QuoteCalculator {
public:
    void apply_base_tick(QuoteState& state, const RawTick& tick, MarketPhase phase) const;
    void recompute_fast_metrics(QuoteState& state) const;

private:
    int calc_speed_bp(const QuoteState& state, int lookback_minutes) const;
    int64_t calc_amount_delta_yuan(const QuoteState& state, int lookback_minutes) const;

private:
    MinuteRingUpdater ring_reader_;
};

}  // namespace t1_v2
