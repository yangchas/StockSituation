#pragma once

#include <cstdint>

namespace t1_v2 {

struct RawTick {
    char symbol[7] = {0};

    int64_t ts_ms = 0;
    int px_milli = 0;
    int pc_milli = 0;
    int o_milli = 0;
    int h_milli = 0;
    int l_milli = 0;

    int64_t amt_yuan = 0;
    // Shares, not lots. Keep this unit stable across live and replay.
    int64_t vol_units = 0;

    int ap_milli[5] = {0};
    int bp_milli[5] = {0};
    int64_t av[5] = {0};
    int64_t bv[5] = {0};

    int64_t inst_vol = 0;
    int64_t inst_amt_yuan = 0;
    int64_t large_net_yuan = 0;

    int limit_up_milli = 0;
    int limit_down_milli = 0;
};

}  // namespace t1_v2
