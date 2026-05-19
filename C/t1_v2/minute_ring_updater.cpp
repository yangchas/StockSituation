#include "minute_ring_updater.h"

#include <ctime>

namespace t1_v2 {

void MinuteRingUpdater::apply_tick(MinuteRingState& ring, const RawTick& tick) const {
    const int64_t minute_index = minute_index_of(tick.ts_ms);
    if (minute_index < 0) {
        return;
    }
    if (ring.latest_minute >= 0 && minute_index < ring.latest_minute - MinuteRingState::KEEP) {
        return;
    }
    MinuteSlot& slot = ensure_slot(ring, minute_index);
    slot.minute_index = minute_index;
    slot.px_milli = tick.px_milli;
    if (tick.amt_yuan >= slot.amt_yuan) {
        slot.amt_yuan = tick.amt_yuan;
    }
    slot.vol_units = tick.vol_units;
    if (ring.latest_minute < 0 || minute_index > ring.latest_minute) {
        ring.latest_minute = minute_index;
    }
}

const MinuteSlot* MinuteRingUpdater::get_slot(const MinuteRingState& ring, int64_t minute_index) const {
    if (minute_index < 0) {
        return nullptr;
    }
    const std::size_t index = static_cast<std::size_t>(minute_index % MinuteRingState::KEEP);
    const MinuteSlot& slot = ring.slots[index];
    if (slot.minute_index == minute_index) {
        return &slot;
    }
    return nullptr;
}

const MinuteSlot* MinuteRingUpdater::find_rolling_amount_reference(
    const MinuteRingState& ring,
    int64_t latest_minute,
    int window_minutes
) const {
    if (latest_minute < 0 || window_minutes <= 0) {
        return nullptr;
    }
    for (int offset = window_minutes; offset >= 1; --offset) {
        if (const MinuteSlot* slot = get_slot(ring, latest_minute - offset)) {
            return slot;
        }
    }
    return nullptr;
}

int64_t MinuteRingUpdater::minute_index_of(int64_t ts_ms) const {
    if (ts_ms <= 0) {
        return -1;
    }
    return ts_ms / 60000;
}

MinuteSlot& MinuteRingUpdater::ensure_slot(MinuteRingState& ring, int64_t minute_index) const {
    const std::size_t index = static_cast<std::size_t>(minute_index % MinuteRingState::KEEP);
    MinuteSlot& slot = ring.slots[index];
    if (slot.minute_index != minute_index) {
        slot = MinuteSlot{};
        slot.minute_index = minute_index;
    }
    return slot;
}

}  // namespace t1_v2
