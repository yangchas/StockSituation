#pragma once

#include <cstdint>

#include "auction_state.h"
#include "dirty_mask.h"
#include "minute_ring.h"
#include "runtime_mode.h"

namespace t1_v2 {

struct QuoteState {
    char symbol[7] = {0};
    char market[8] = {0};

    int64_t ts_ms = 0;
    MarketPhase phase = MarketPhase::Premarket;
    LimitState limit_state = LimitState::Normal;

    int px_milli = 0;
    int pc_milli = 0;
    int mx_milli = 0;
    int mn_milli = 0;

    int64_t amt_yuan = 0;
    // Shares, not lots. Must match RawTick::vol_units.
    int64_t vol_units = 0;
    int64_t inst_vol = 0;
    int64_t inst_amt_yuan = 0;
    int64_t large_net_yuan = 0;
    int64_t cumulative_large_net_yuan = 0;

    int spd1m_bp = 0;
    int vec3m_bp = 0;
    int vec5m_bp = 0;
    int64_t amt2m_yuan = 0;
    int64_t amt5m_yuan = 0;

    MinuteRingState minute_ring;
    AuctionState auction;

    int last_written_px_milli = 0;
    int64_t last_written_amt_yuan = 0;
    int64_t last_written_ts_ms = 0;
    uint32_t dirty_mask = DIRTY_NONE;
};

}  // namespace t1_v2
