#pragma once

#include "quote_state.h"
#include "raw_tick.h"
#include "runtime_mode.h"

namespace t1_v2 {

class AuctionCalculator {
public:
    bool apply_tick(QuoteState& state, const RawTick& tick, MarketPhase phase) const;

private:
    bool capture_anchor_prices(AuctionState& auction, const RawTick& tick) const;
    bool update_match_and_rest(AuctionState& auction, const RawTick& tick, MarketPhase phase) const;
    int64_t calc_match_amt_yuan(const RawTick& tick, MarketPhase phase) const;
    int64_t calc_rest_bid_amt_yuan(const RawTick& tick) const;
    int64_t calc_rest_ask_amt_yuan(const RawTick& tick) const;
    int hms_from_timestamp_ms(int64_t ts_ms) const;
};

}  // namespace t1_v2
