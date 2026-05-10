#pragma once

#include <cstdint>

#include "config_v2.h"
#include "runtime_mode.h"

namespace t1_v2 {

struct SnapshotTriggerState {
    bool emit_a20 = false;
    bool emit_a24 = false;
    bool emit_a25 = false;
    bool emit_latest_auction = false;
    bool emit_runtime = false;
};

class SnapshotTrigger {
public:
    explicit SnapshotTrigger(const ConfigV2& config);

    SnapshotTriggerState update(int64_t logical_ts_ms, MarketPhase phase);

    bool a20_done() const { return emitted_a20_; }
    bool a24_done() const { return emitted_a24_; }
    bool a25_done() const { return emitted_a25_; }

private:
    int hms_from_timestamp_ms(int64_t ts_ms) const;

private:
    ConfigV2 config_;
    bool emitted_a20_ = false;
    bool emitted_a24_ = false;
    bool emitted_a25_ = false;
    int64_t last_latest_auction_ts_ms_ = 0;
    int64_t last_runtime_ts_ms_ = 0;
};

}  // namespace t1_v2
