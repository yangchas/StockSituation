#include "runtime_pipeline.h"

#include <iterator>

namespace t1_v2 {

RuntimePipeline::RuntimePipeline(const ConfigV2& config)
    : config_(config),
      engine_(config),
      redis_writer_(config),
      tdengine_writer_(config) {}

bool RuntimePipeline::initialize() {
    if (!engine_.initialize()) {
        return false;
    }
    if (!redis_writer_.initialize()) {
        engine_.shutdown();
        return false;
    }
    if (!tdengine_writer_.initialize()) {
        redis_writer_.shutdown();
        engine_.shutdown();
        return false;
    }
    initialized_ = true;
    return true;
}

void RuntimePipeline::shutdown() {
    tdengine_writer_.shutdown();
    redis_writer_.shutdown();
    engine_.shutdown();
    initialized_ = false;
}

RuntimePipelineResult RuntimePipeline::process_batch(
    const TickBatch& batch,
    const SourceTickBatchBuildStats& source_stats
) {
    RuntimePipelineResult result;
    if (!initialized_) {
        return result;
    }

    result.engine_stats = engine_.on_batch(batch);
    const bool write_redis = !config_.processing.dry_run &&
        (batch.mode != RuntimeMode::Replay || config_.replay.write_redis);
    if (write_redis) {
        result.redis_commands = redis_writer_.build_q2_commands(engine_.quote_store(), batch.logical_ts_ms);
        result.has_q2_commands = !result.redis_commands.empty();
        std::vector<RedisCommand> auction_commands = redis_writer_.build_a2_commands(
            engine_.quote_store(),
            result.engine_stats.snapshot_trigger,
            batch.logical_ts_ms
        );
        result.redis_commands.insert(
            result.redis_commands.end(),
            std::make_move_iterator(auction_commands.begin()),
            std::make_move_iterator(auction_commands.end())
        );
    }
    result.redis_format_stats = RedisCommandFormatter::estimate(result.redis_commands);
    const bool write_tdengine = !config_.processing.dry_run &&
        (batch.mode != RuntimeMode::Replay || config_.replay.write_tdengine);
    if (write_tdengine) {
        result.tdengine_statements = tdengine_writer_.build_batch_statements(
            batch,
            engine_.quote_store(),
            result.engine_stats.snapshot_trigger,
            batch.logical_ts_ms
        );
    }
    result.runtime_stats = RuntimeBatchStatsBuilder::build(
        batch.seq_no,
        batch.logical_ts_ms,
        batch.wall_ts_ms,
        source_stats,
        result.engine_stats,
        result.redis_format_stats
    );
    if (write_redis) {
        const int interval_ms = config_.processing.runtime_interval_ms > 0
            ? config_.processing.runtime_interval_ms
            : 2000;
        if (last_runtime_redis_ts_ms_ <= 0 ||
            batch.logical_ts_ms - last_runtime_redis_ts_ms_ >= interval_ms) {
            std::vector<RedisCommand> runtime_commands = redis_writer_.build_runtime_commands(
                result.runtime_stats,
                batch.mode
            );
            result.redis_commands.insert(
                result.redis_commands.end(),
                std::make_move_iterator(runtime_commands.begin()),
                std::make_move_iterator(runtime_commands.end())
            );
            if (!runtime_commands.empty()) {
                last_runtime_redis_ts_ms_ = batch.logical_ts_ms;
            }
        }
    }
    return result;
}

int RuntimePipeline::commit_redis_success(int64_t logical_ts_ms) {
    if (!initialized_) {
        return 0;
    }
    return redis_writer_.commit_q2_writes(engine_.mutable_quote_store(), logical_ts_ms);
}

}  // namespace t1_v2
