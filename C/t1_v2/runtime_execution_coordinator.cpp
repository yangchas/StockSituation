#include "runtime_execution_coordinator.h"

namespace t1_v2 {

RuntimeExecutionCoordinator::RuntimeExecutionCoordinator(
    IRedisCommandExecutor& redis_executor,
    ITDengineCommandExecutor& tdengine_executor
) : redis_executor_(redis_executor), tdengine_executor_(tdengine_executor) {}

RuntimeExecutionResult RuntimeExecutionCoordinator::execute_and_commit(
    RuntimePipeline& pipeline,
    const RuntimePipelineResult& batch_result
) {
    RuntimeExecutionResult result;

    result.tdengine = tdengine_executor_.execute(batch_result.tdengine_statements);
    result.tdengine_executed = !batch_result.tdengine_statements.empty();
    if (!result.tdengine.ok) {
        result.ok = false;
        return result;
    }

    result.redis = redis_executor_.execute(batch_result.redis_commands);
    result.redis_executed = !batch_result.redis_commands.empty();
    if (!result.redis.ok) {
        result.ok = false;
        return result;
    }

    if (result.redis_executed && batch_result.has_q2_commands) {
        result.redis_committed_quotes = pipeline.commit_redis_success(batch_result.runtime_stats.logical_ts_ms);
    }
    result.ok = true;
    return result;
}

}  // namespace t1_v2
