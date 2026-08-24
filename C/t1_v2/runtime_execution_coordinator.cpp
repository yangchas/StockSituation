#include "runtime_execution_coordinator.h"

namespace t1_v2 {

RuntimeExecutionCoordinator::RuntimeExecutionCoordinator(
    IRedisCommandExecutor& redis_executor,
    ITDengineCommandExecutor& tdengine_executor
) : redis_executor_(redis_executor), tdengine_executor_(tdengine_executor) {}

RuntimePreflightResult RuntimeExecutionCoordinator::preflight(bool check_redis, bool check_tdengine) {
    RuntimePreflightResult result;
    if (check_redis) {
        result.redis_checked = true;
        result.redis = redis_executor_.preflight();
        if (!result.redis.ok) {
            result.ok = false;
            result.error = "redis preflight failed: " + result.redis.error;
            return result;
        }
    }
    if (check_tdengine) {
        result.tdengine_checked = true;
        result.tdengine = tdengine_executor_.preflight();
        if (!result.tdengine.ok) {
            result.ok = false;
            result.error = "tdengine preflight failed: " + result.tdengine.error;
            return result;
        }
    }
    result.ok = true;
    return result;
}

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

RuntimePreflightResult RuntimeExecutionCoordinator::recheck_after_reset(bool check_redis, bool check_tdengine) {
    reset_all_connections();
    return preflight(check_redis, check_tdengine);
}

void RuntimeExecutionCoordinator::reset_all_connections() {
    redis_executor_.reset_connection();
    tdengine_executor_.reset_connection();
}

}  // namespace t1_v2
