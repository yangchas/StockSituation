#include "snapshot_trigger.h"

#include <ctime>

namespace t1_v2 {

SnapshotTrigger::SnapshotTrigger(const ConfigV2& config) : config_(config) {}

SnapshotTriggerState SnapshotTrigger::update(int64_t logical_ts_ms, MarketPhase phase) {
    SnapshotTriggerState state;
    const int trade_date = trade_date_from_timestamp_ms(logical_ts_ms);
    if (trade_date <= 0) {
        return state;
    }
    if (current_trade_date_ > 0 && trade_date < current_trade_date_) {
        // A late/out-of-order tick from an older session must not rewind
        // day-local state or advance any interval marker.
        return state;
    }
    if (trade_date != current_trade_date_) {
        current_trade_date_ = trade_date;
        emitted_a20_ = false;
        emitted_a24_ = false;
        emitted_a25_ = false;
        last_latest_auction_ts_ms_ = 0;
        last_runtime_ts_ms_ = 0;
    }
    const int hms = hms_from_timestamp_ms(logical_ts_ms);

    if (!emitted_a20_ && hms >= 92003 && hms < 92400) {
        emitted_a20_ = true;
        state.emit_a20 = true;
    }
    if (!emitted_a24_ && hms >= 92410 && hms < 92500) {
        emitted_a24_ = true;
        state.emit_a24 = true;
    }
    if (!emitted_a25_ && hms >= 92500 && hms < 93000) {
        emitted_a25_ = true;
        state.emit_a25 = true;
    }

    if (phase == MarketPhase::Auction &&
        (last_latest_auction_ts_ms_ <= 0 ||
         logical_ts_ms - last_latest_auction_ts_ms_ >= config_.processing.a2_latest_interval_ms)) {
        last_latest_auction_ts_ms_ = logical_ts_ms;
        state.emit_latest_auction = true;
    }

    if (last_runtime_ts_ms_ <= 0 ||
        logical_ts_ms - last_runtime_ts_ms_ >= config_.processing.runtime_interval_ms) {
        last_runtime_ts_ms_ = logical_ts_ms;
        state.emit_runtime = true;
    }

    return state;
}

int SnapshotTrigger::hms_from_timestamp_ms(int64_t ts_ms) const {
    if (ts_ms <= 0) {
        return 0;
    }
    const std::time_t seconds = static_cast<std::time_t>(ts_ms / 1000);
    std::tm local_tm{};
#if defined(_WIN32)
    localtime_s(&local_tm, &seconds);
#else
    localtime_r(&seconds, &local_tm);
#endif
    return local_tm.tm_hour * 10000 + local_tm.tm_min * 100 + local_tm.tm_sec;
}

int SnapshotTrigger::trade_date_from_timestamp_ms(int64_t ts_ms) const {
    if (ts_ms <= 0) {
        return 0;
    }
    const std::time_t seconds = static_cast<std::time_t>(ts_ms / 1000);
    std::tm local_tm{};
#if defined(_WIN32)
    localtime_s(&local_tm, &seconds);
#else
    localtime_r(&seconds, &local_tm);
#endif
    return (local_tm.tm_year + 1900) * 10000 + (local_tm.tm_mon + 1) * 100 + local_tm.tm_mday;
}

}  // namespace t1_v2
