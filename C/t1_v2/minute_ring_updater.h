#pragma once

#include "minute_ring.h"
#include "raw_tick.h"

namespace t1_v2 {

class MinuteRingUpdater {
public:
    void apply_tick(MinuteRingState& ring, const RawTick& tick) const;
    const MinuteSlot* get_slot(const MinuteRingState& ring, int64_t minute_index) const;
    const MinuteSlot* find_rolling_amount_reference(
        const MinuteRingState& ring,
        int64_t latest_minute,
        int window_minutes
    ) const;

private:
    int64_t minute_index_of(int64_t ts_ms) const;
    MinuteSlot& ensure_slot(MinuteRingState& ring, int64_t minute_index) const;
};

}  // namespace t1_v2
