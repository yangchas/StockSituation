#pragma once

#include <cstdint>
#include <vector>

#include "raw_tick.h"
#include "runtime_mode.h"

namespace t1_v2 {

struct TickBatch {
    RuntimeMode mode = RuntimeMode::Live;
    int64_t logical_ts_ms = 0;
    int64_t wall_ts_ms = 0;
    uint32_t seq_no = 0;
    std::vector<RawTick> ticks;
};

}  // namespace t1_v2
