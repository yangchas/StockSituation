#pragma once

#include <cstdint>

#include "runtime_mode.h"

namespace t1_v2 {

class PhaseResolver {
public:
    MarketPhase resolve(int64_t logical_ts_ms) const;
    bool is_auction(int64_t logical_ts_ms) const;
    bool is_intraday(int64_t logical_ts_ms) const;

private:
    static int hms_from_timestamp_ms(int64_t ts_ms);
};

}  // namespace t1_v2
