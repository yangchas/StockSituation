#include "source_tick_batch_builder.h"

#include <algorithm>

namespace t1_v2 {

SourceTickBatchBuildStats SourceTickBatchBuilder::build(
    const std::vector<SourceTickRecord>& source_records,
    RuntimeMode mode,
    int64_t logical_ts_ms,
    uint32_t seq_no,
    TickBatch& out
) {
    SourceTickBatchBuildStats stats;
    stats.input_count = static_cast<uint32_t>(source_records.size());

    TickBatch batch;
    batch.mode = mode;
    batch.logical_ts_ms = logical_ts_ms;
    batch.seq_no = seq_no;
    batch.ticks.reserve(source_records.size());

    for (const SourceTickRecord& source : source_records) {
        RawTick tick;
        if (!RawTickConverter::from_source_record(source, tick)) {
            ++stats.rejected_count;
            continue;
        }
        stats.max_tick_ts_ms = std::max(stats.max_tick_ts_ms, tick.ts_ms);
        batch.ticks.push_back(tick);
    }

    stats.accepted_count = static_cast<uint32_t>(batch.ticks.size());
    if (batch.logical_ts_ms <= 0) {
        batch.logical_ts_ms = stats.max_tick_ts_ms;
    }
    batch.wall_ts_ms = batch.logical_ts_ms;
    out = std::move(batch);
    return stats;
}

}  // namespace t1_v2
