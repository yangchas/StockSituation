#pragma once

#include <cstdint>

#include "runtime_mode.h"

namespace t1_v2 {

struct AuctionState {
    int a20_px_milli = 0;
    int a24_px_milli = 0;
    int a25_px_milli = 0;

    int64_t match_amt_yuan = 0;
    int64_t rest_bid_amt_yuan = 0;
    int64_t rest_ask_amt_yuan = 0;

    int64_t ts_ms = 0;
    LimitState limit_state = LimitState::Normal;
};

}  // namespace t1_v2
