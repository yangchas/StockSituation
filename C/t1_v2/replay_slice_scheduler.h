#pragma once

#include <cstdint>
#include <string>

#include "config_v2.h"

namespace t1_v2 {

struct ReplaySlice {
    int64_t start_ms = 0;
    int64_t end_ms = 0;
    bool valid = false;
};

class ReplaySliceScheduler {
public:
    explicit ReplaySliceScheduler(const ConfigV2& config);

    bool start();
    ReplaySlice next();
    bool finished() const { return finished_; }
    int64_t current_ms() const { return current_ms_; }

    static int64_t parse_local_time_ms(const std::string& text);

private:
    ConfigV2 config_;
    int64_t start_ms_ = 0;
    int64_t end_ms_ = 0;
    int64_t current_ms_ = 0;
    bool finished_ = true;
};

}  // namespace t1_v2
