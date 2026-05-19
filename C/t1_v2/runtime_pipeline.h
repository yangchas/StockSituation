#pragma once

#include <cstdint>
#include <vector>

#include "engine_core.h"
#include "redis_command_formatter.h"
#include "redis_v2_writer.h"
#include "runtime_batch_stats.h"
#include "tdengine_v2_writer.h"

namespace t1_v2 {

struct RuntimePipelineResult {
    EngineProcessStats engine_stats;
    RedisCommandFormatStats redis_format_stats;
    RuntimeBatchStats runtime_stats;
    std::vector<RedisCommand> redis_commands;
    std::vector<std::string> tdengine_statements;
    bool has_q2_commands = false;
};

class RuntimePipeline {
public:
    explicit RuntimePipeline(const ConfigV2& config);

    bool initialize();
    void shutdown();
    RuntimePipelineResult process_batch(const TickBatch& batch, const SourceTickBatchBuildStats& source_stats);
    int commit_redis_success(int64_t logical_ts_ms);

    const EngineCore& engine() const { return engine_; }

private:
    ConfigV2 config_;
    EngineCore engine_;
    RedisV2Writer redis_writer_;
    TDengineV2Writer tdengine_writer_;
    bool initialized_ = false;
    int64_t last_runtime_redis_ts_ms_ = 0;
};

}  // namespace t1_v2
