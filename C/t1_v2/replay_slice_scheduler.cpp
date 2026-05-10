#include "replay_slice_scheduler.h"

#include <ctime>
#include <iomanip>
#include <sstream>

namespace t1_v2 {

ReplaySliceScheduler::ReplaySliceScheduler(const ConfigV2& config) : config_(config) {}

bool ReplaySliceScheduler::start() {
    start_ms_ = parse_local_time_ms(config_.replay.start_time);
    end_ms_ = parse_local_time_ms(config_.replay.end_time);
    const int interval = config_.replay.tick_interval_ms > 0 ? config_.replay.tick_interval_ms : 3000;
    if (start_ms_ <= 0 || end_ms_ <= start_ms_ || interval <= 0) {
        finished_ = true;
        return false;
    }
    current_ms_ = start_ms_;
    finished_ = false;
    return true;
}

ReplaySlice ReplaySliceScheduler::next() {
    if (finished_) {
        return {};
    }
    const int64_t interval = config_.replay.tick_interval_ms > 0 ? config_.replay.tick_interval_ms : 3000;
    if (current_ms_ >= end_ms_) {
        if (config_.replay.loop) {
            current_ms_ = start_ms_;
        } else {
            finished_ = true;
            return {};
        }
    }

    ReplaySlice slice;
    slice.start_ms = current_ms_;
    slice.end_ms = current_ms_ + interval;
    if (slice.end_ms > end_ms_) {
        slice.end_ms = end_ms_;
    }
    slice.valid = slice.start_ms < slice.end_ms;
    current_ms_ += interval;
    return slice;
}

int64_t ReplaySliceScheduler::parse_local_time_ms(const std::string& text) {
    std::tm tm{};
    std::istringstream iss(text);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (iss.fail()) {
        return 0;
    }
    tm.tm_isdst = -1;
    const std::time_t seconds = std::mktime(&tm);
    if (seconds <= 0) {
        return 0;
    }
    return static_cast<int64_t>(seconds) * 1000;
}

}  // namespace t1_v2
