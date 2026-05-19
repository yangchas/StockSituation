#pragma once

#include <cstdint>
#include <memory>

#include "auction_calculator.h"
#include "config_v2.h"
#include "dirty_marker.h"
#include "minute_ring_updater.h"
#include "phase_resolver.h"
#include "quote_calculator.h"
#include "quote_state_store.h"
#include "snapshot_trigger.h"
#include "tick_batch.h"
#include "tick_delta_calculator.h"

namespace t1_v2 {

struct EngineProcessStats {
    uint32_t tick_count = 0;
    uint32_t new_symbol_count = 0;
    int64_t logical_ts_ms = 0;
    MarketPhase phase = MarketPhase::Premarket;
    SnapshotTriggerState snapshot_trigger;
};

class EngineCore {
public:
    explicit EngineCore(const ConfigV2& config);
    ~EngineCore();

    bool initialize();
    void shutdown();
    EngineProcessStats on_batch(const TickBatch& batch);

    const QuoteStateStore& quote_store() const;
    QuoteStateStore& mutable_quote_store();

private:
    void refresh_phase_for_active_quotes(MarketPhase phase);

    ConfigV2 config_;
    PhaseResolver phase_resolver_;
    MinuteRingUpdater minute_ring_updater_;
    QuoteCalculator quote_calculator_;
    AuctionCalculator auction_calculator_;
    TickDeltaCalculator tick_delta_calculator_;
    DirtyMarker dirty_marker_;
    std::unique_ptr<SnapshotTrigger> snapshot_trigger_;
    std::unique_ptr<QuoteStateStore> quote_store_;
    bool initialized_ = false;
    MarketPhase last_batch_phase_ = MarketPhase::Premarket;
    bool has_last_batch_phase_ = false;
};

}  // namespace t1_v2
