#pragma once

#include <cstdint>

#include "engine_core.h"
#include "redis_command_formatter.h"
#include "source_tick_batch_builder.h"

namespace t1_v2 {

struct RuntimeBatchStats {
    uint32_t seq_no = 0;
    int64_t logical_ts_ms = 0;
    uint32_t source_input = 0;
    uint32_t source_accepted = 0;
    uint32_t source_rejected = 0;
    int64_t source_max_tick_ts_ms = 0;
    int64_t wall_ts_ms = 0;
    int64_t source_delay_ms = 0;
    uint32_t engine_ticks = 0;
    uint32_t new_symbols = 0;
    uint32_t redis_commands = 0;
    uint32_t redis_fields = 0;
    uint32_t redis_argv = 0;
    uint32_t redis_payload_bytes = 0;
};

class RuntimeBatchStatsBuilder {
public:
    static RuntimeBatchStats build(
        uint32_t seq_no,
        int64_t logical_ts_ms,
        int64_t wall_ts_ms,
        const SourceTickBatchBuildStats& source,
        const EngineProcessStats& engine,
        const RedisCommandFormatStats& redis
    ) {
        RuntimeBatchStats stats;
        stats.seq_no = seq_no;
        stats.logical_ts_ms = logical_ts_ms;
        stats.source_input = source.input_count;
        stats.source_accepted = source.accepted_count;
        stats.source_rejected = source.rejected_count;
        stats.source_max_tick_ts_ms = source.max_tick_ts_ms;
        stats.wall_ts_ms = wall_ts_ms;
        if (wall_ts_ms > 0 && source.max_tick_ts_ms > 0) {
            stats.source_delay_ms = wall_ts_ms - source.max_tick_ts_ms;
        }
        stats.engine_ticks = engine.tick_count;
        stats.new_symbols = engine.new_symbol_count;
        stats.redis_commands = static_cast<uint32_t>(redis.command_count);
        stats.redis_fields = static_cast<uint32_t>(redis.field_count);
        stats.redis_argv = static_cast<uint32_t>(redis.argv_count);
        stats.redis_payload_bytes = static_cast<uint32_t>(redis.payload_bytes);
        return stats;
    }
};

}  // namespace t1_v2
