#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "runtime_execution_coordinator.h"
#include "runtime_pipeline.h"
#include "tick_source.h"

namespace t1_v2 {

struct RuntimeLoopOptions {
    // 0 means unlimited. Tests can set this to prevent an accidental busy loop.
    uint32_t max_empty_polls = 0;
    uint32_t max_batches = 0;
};

struct RuntimeLoopStats {
    bool ok = true;
    std::string error;
    uint32_t batches = 0;
    uint32_t empty_polls = 0;
    uint32_t source_errors = 0;
    uint32_t source_acks = 0;
    uint32_t source_rejects = 0;
    uint32_t source_ack_failures = 0;
    uint32_t source_reject_failures = 0;
    uint32_t source_ack_skipped = 0;
    uint64_t source_input = 0;
    uint64_t source_rejected = 0;
    uint64_t ticks = 0;
    uint64_t redis_commands = 0;
    uint64_t tdengine_statements = 0;
    uint64_t redis_committed_quotes = 0;
};

class RuntimeLoop {
public:
    RuntimeLoop(
        const ConfigV2& config,
        std::unique_ptr<ITickSource> source,
        IRedisCommandExecutor& redis_executor,
        ITDengineCommandExecutor& tdengine_executor,
        RuntimeLoopOptions options = {}
    );

    RuntimeLoopStats run();

private:
    bool should_preflight_redis() const;
    bool should_preflight_tdengine() const;
    bool should_stop_after_empty(RuntimeLoopStats& stats) const;
    bool should_ack_source() const;
    bool has_runtime_bound() const;
    void sleep_after_empty() const;

private:
    ConfigV2 config_;
    std::unique_ptr<ITickSource> source_;
    RuntimePipeline pipeline_;
    RuntimeExecutionCoordinator coordinator_;
    RuntimeLoopOptions options_;
};

}  // namespace t1_v2
