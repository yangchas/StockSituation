#pragma once

#include <cstdint>

namespace t1_v2 {

struct MinuteSlot {
    int64_t minute_index = -1;
    int px_milli = 0;
    int64_t amt_yuan = 0;
    // Shares, not lots. Must match RawTick::vol_units.
    int64_t vol_units = 0;
};

struct MinuteRingState {
    static constexpr int KEEP = 8;
    MinuteSlot slots[KEEP];
    int64_t latest_minute = -1;
};

}  // namespace t1_v2
