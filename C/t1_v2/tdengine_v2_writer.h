#pragma once

#include <string>
#include <vector>

#include "config_v2.h"
#include "quote_state_store.h"
#include "snapshot_trigger.h"
#include "tick_batch.h"

namespace t1_v2 {

class TDengineV2Writer {
public:
    explicit TDengineV2Writer(const ConfigV2& config);

    bool initialize();
    void shutdown();

    bool should_write_stock_ticks(const TickBatch& batch) const;
    std::string stock_tick_stable_ddl() const;
    std::string auction_snapshot_stable_ddl() const;
    std::string auction_summary_table_ddl() const;

    std::string build_stock_tick_insert_sql(const TickBatch& batch) const;
    std::string build_auction_summary_insert_sql(
        const QuoteStateStore& store,
        const SnapshotTriggerState& trigger,
        int64_t logical_ts_ms
    ) const;
    std::string build_auction_snapshot_insert_sql(
        const QuoteStateStore& store,
        const SnapshotTriggerState& trigger,
        int64_t logical_ts_ms
    ) const;
    std::vector<std::string> build_schema_statements() const;
    std::vector<std::string> build_batch_statements(
        const TickBatch& batch,
        const QuoteStateStore& store,
        const SnapshotTriggerState& trigger,
        int64_t logical_ts_ms
    );

private:
    static std::string trade_date_yyyymmdd(int64_t ts_ms);
    static std::string auction_tag_from_trigger(const SnapshotTriggerState& trigger);
    static int change_bp(const QuoteState& state);
    static bool is_valid_symbol(const char* symbol);

private:
    ConfigV2 config_;
    bool initialized_ = false;
    bool schema_emitted_ = false;
};

}  // namespace t1_v2
