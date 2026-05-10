#pragma once

#include <cstdint>
#include <vector>

#include "raw_tick_converter.h"
#include "runtime_mode.h"
#include "tick_batch.h"

namespace t1_v2 {

struct SourceTickBatchBuildStats {
    uint32_t input_count = 0;
    uint32_t accepted_count = 0;
    uint32_t rejected_count = 0;
    int64_t max_tick_ts_ms = 0;
};

class SourceTickBatchBuilder {
public:
    static SourceTickBatchBuildStats build(
        const std::vector<SourceTickRecord>& source_records,
        RuntimeMode mode,
        int64_t logical_ts_ms,
        uint32_t seq_no,
        TickBatch& out
    );
};

}  // namespace t1_v2
