#pragma once

#include <cstdint>

namespace t1_v2 {

enum DirtyMask : uint32_t {
    DIRTY_NONE = 0,
    DIRTY_QUOTE_CORE = 1u << 0,
    DIRTY_MINUTE_METR = 1u << 1,
    DIRTY_AUCTION = 1u << 2,
    DIRTY_A20 = 1u << 3,
    DIRTY_A24 = 1u << 4,
    DIRTY_A25 = 1u << 5,
    DIRTY_RUNTIME = 1u << 6,
    DIRTY_FLOW = 1u << 7,
};

}  // namespace t1_v2
