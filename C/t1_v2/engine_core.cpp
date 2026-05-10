#include "engine_core.h"

namespace t1_v2 {

EngineCore::EngineCore(const ConfigV2& config) : config_(config) {}

EngineCore::~EngineCore() {
    shutdown();
}

bool EngineCore::initialize() {
    quote_store_ = std::make_unique<QuoteStateStore>();
    snapshot_trigger_ = std::make_unique<SnapshotTrigger>(config_);
    initialized_ = true;
    return true;
}

void EngineCore::shutdown() {
    quote_store_.reset();
    snapshot_trigger_.reset();
    initialized_ = false;
}

EngineProcessStats EngineCore::on_batch(const TickBatch& batch) {
    EngineProcessStats stats;
    stats.logical_ts_ms = batch.logical_ts_ms;
    stats.tick_count = static_cast<uint32_t>(batch.ticks.size());
    stats.phase = phase_resolver_.resolve(batch.logical_ts_ms);
    if (snapshot_trigger_) {
        stats.snapshot_trigger = snapshot_trigger_->update(batch.logical_ts_ms, stats.phase);
    }

    if (!initialized_ || !quote_store_) {
        return stats;
    }

    quote_store_->begin_batch();
    for (const RawTick& tick : batch.ticks) {
        QuoteState& state = quote_store_->get_or_create(tick.symbol);
        const int previous_px = state.px_milli;
        const int64_t previous_amt = state.amt_yuan;
        const int64_t previous_vol = state.vol_units;
        const int previous_spd1m = state.spd1m_bp;
        const int64_t previous_amt2m = state.amt2m_yuan;
        const int64_t previous_amt5m = state.amt5m_yuan;
        const int previous_vec3m = state.vec3m_bp;
        const int previous_vec5m = state.vec5m_bp;
        const int64_t previous_inst_vol = state.inst_vol;
        const int64_t previous_inst_amt_yuan = state.inst_amt_yuan;
        const int64_t previous_large_net_yuan = state.large_net_yuan;

        tick_delta_calculator_.apply_tick(
            state,
            tick,
            stats.phase,
            static_cast<int64_t>(config_.thresholds.large_order_threshold)
        );
        quote_calculator_.apply_base_tick(state, tick, stats.phase);
        const bool auction_changed = auction_calculator_.apply_tick(state, tick, stats.phase);
        minute_ring_updater_.apply_tick(state.minute_ring, tick);
        quote_calculator_.recompute_fast_metrics(state);
        dirty_marker_.mark_after_tick(
            state,
            previous_px,
            previous_amt,
            previous_vol,
            previous_spd1m,
            previous_amt2m,
            previous_amt5m,
            previous_vec3m,
            previous_vec5m,
            previous_inst_vol,
            previous_inst_amt_yuan,
            previous_large_net_yuan,
            auction_changed
        );
    }
    stats.new_symbol_count = quote_store_->last_batch_symbol_count();
    return stats;
}

const QuoteStateStore& EngineCore::quote_store() const {
    return *quote_store_;
}

QuoteStateStore& EngineCore::mutable_quote_store() {
    return *quote_store_;
}

}  // namespace t1_v2
