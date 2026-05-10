#pragma once

#include "quote_state.h"

namespace t1_v2 {

class DirtyMarker {
public:
    uint32_t mark_after_tick(
        QuoteState& state,
        int previous_px_milli,
        int64_t previous_amt_yuan,
        int64_t previous_vol_units,
        int previous_spd1m_bp,
        int64_t previous_amt2m_yuan,
        int64_t previous_amt5m_yuan,
        int previous_vec3m_bp,
        int previous_vec5m_bp,
        int64_t previous_inst_vol,
        int64_t previous_inst_amt_yuan,
        int64_t previous_large_net_yuan,
        bool auction_changed
    ) const;
};

}  // namespace t1_v2
