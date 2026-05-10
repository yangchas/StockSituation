#pragma once

#include <cstdint>
#include <string>

#include "config_v2.h"

namespace t1_v2 {

class TdReplayQueryBuilder {
public:
    explicit TdReplayQueryBuilder(const ConfigV2& config);

    int64_t align_to_tick_interval(int64_t ts_ms) const;
    std::string build_slice_query(int64_t slice_start_ms) const;

private:
    static bool is_safe_table_name(const std::string& table);

private:
    ConfigV2 config_;
};

}  // namespace t1_v2
