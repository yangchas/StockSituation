#pragma once

#include <cstdint>

#include "runtime_mode.h"

namespace t1_v2 {

struct LogicalClock {
    RuntimeMode mode = RuntimeMode::Live;
    int64_t logical_ts_ms = 0;
    int64_t wall_ts_ms = 0;
    int replay_speed = 1;
};

}  // namespace t1_v2
