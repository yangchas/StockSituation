#pragma once

#include "redis_command_executor.h"
#include "runtime_pipeline.h"
#include "tdengine_command_executor.h"

namespace t1_v2 {

struct RuntimeExecutionResult {
    bool ok = true;
    bool redis_executed = false;
    bool tdengine_executed = false;
    int redis_committed_quotes = 0;
    RedisExecutionResult redis;
    TDengineExecutionResult tdengine;
};

struct RuntimePreflightResult {
    bool ok = true;
    bool redis_checked = false;
    bool tdengine_checked = false;
    RedisExecutionResult redis;
    TDengineExecutionResult tdengine;
    std::string error;
};

class RuntimeExecutionCoordinator {
public:
    RuntimeExecutionCoordinator(IRedisCommandExecutor& redis_executor, ITDengineCommandExecutor& tdengine_executor);

    RuntimePreflightResult preflight(bool check_redis, bool check_tdengine);
    RuntimeExecutionResult execute_and_commit(RuntimePipeline& pipeline, const RuntimePipelineResult& batch_result);

private:
    IRedisCommandExecutor& redis_executor_;
    ITDengineCommandExecutor& tdengine_executor_;
};

}  // namespace t1_v2
