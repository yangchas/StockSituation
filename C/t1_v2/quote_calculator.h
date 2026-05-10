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
    static int calc_limit_price_milli(const RawTick& tick, bool upper);
    static bool is_20pct_symbol(const char* symbol);
    static bool near_price(int lhs_milli, int rhs_milli);

private:
    MinuteRingUpdater ring_reader_;
};

}  // namespace t1_v2
