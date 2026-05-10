#include "dirty_marker.h"

namespace t1_v2 {

uint32_t DirtyMarker::mark_after_tick(
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
) const {
    uint32_t mask = DIRTY_NONE;
    if (state.px_milli != previous_px_milli ||
        state.amt_yuan != previous_amt_yuan ||
        state.vol_units != previous_vol_units) {
        mask |= DIRTY_QUOTE_CORE;
    }
    if (state.spd1m_bp != previous_spd1m_bp ||
        state.amt2m_yuan != previous_amt2m_yuan ||
        state.amt5m_yuan != previous_amt5m_yuan ||
        state.vec3m_bp != previous_vec3m_bp ||
        state.vec5m_bp != previous_vec5m_bp) {
        mask |= DIRTY_MINUTE_METR;
    }
    if (state.inst_vol != previous_inst_vol ||
        state.inst_amt_yuan != previous_inst_amt_yuan ||
        state.large_net_yuan != previous_large_net_yuan) {
        mask |= DIRTY_FLOW;
    }
    if (auction_changed) {
        mask |= DIRTY_AUCTION;
    }
    if (state.auction.a20_px_milli > 0) {
        mask |= DIRTY_A20;
    }
    if (state.auction.a24_px_milli > 0) {
        mask |= DIRTY_A24;
    }
    if (state.auction.a25_px_milli > 0) {
        mask |= DIRTY_A25;
    }
    state.dirty_mask |= mask;
    return mask;
}

}  // namespace t1_v2
