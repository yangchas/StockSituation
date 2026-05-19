#include <iostream>
#include <memory>

#include "config_v2.h"
#include "redis_command_executor.h"
#include "runtime_loop.h"
#include "self_test.h"
#include "tdengine_command_executor.h"
#include "tick_source_factory.h"

int main(int argc, char* argv[]) {
    using namespace t1_v2;

    ConfigManagerV2& manager = ConfigManagerV2::instance();
    CommandLineParseResult parse_result = manager.apply_command_line(argc, argv);
    if (parse_result.show_help) {
        std::cout << ConfigManagerV2::usage_text(argc > 0 ? argv[0] : "t1_v2");
        return 0;
    }
    if (parse_result.run_self_test) {
        return run_self_test() ? 0 : 1;
    }
    if (!parse_result.ok) {
        std::cerr << parse_result.error << std::endl;
        std::cerr << ConfigManagerV2::usage_text(argc > 0 ? argv[0] : "t1_v2");
        return 2;
    }

    const ConfigV2& config = manager.get();
    std::unique_ptr<ITickSource> source = TickSourceFactory::create(config);

#if defined(T1_V2_ENABLE_REDIS)
    HiredisRedisCommandExecutor redis_executor(config);
#else
    NullRedisCommandExecutor redis_executor;
#endif

#if defined(T1_V2_ENABLE_TDENGINE)
    TaosTDengineCommandExecutor tdengine_executor(config);
#else
    NullTDengineCommandExecutor tdengine_executor;
#endif

    RuntimeLoopOptions options;
    options.max_batches = static_cast<uint32_t>(config.processing.max_batches > 0 ? config.processing.max_batches : 0);
    options.max_empty_polls = static_cast<uint32_t>(config.processing.max_empty_polls > 0 ? config.processing.max_empty_polls : 0);
    RuntimeLoop loop(config, std::move(source), redis_executor, tdengine_executor, options);
    const RuntimeLoopStats stats = loop.run();
    if (!stats.ok) {
        std::cerr << "t1_v2 fatal"
                  << " | stage=" << (stats.failure_stage.empty() ? "-" : stats.failure_stage)
                  << " | error=" << (stats.error.empty() ? "-" : stats.error)
                  << " | source_error=" << (stats.source_error.empty() ? "-" : stats.source_error)
                  << " | batches=" << stats.batches
                  << " | empty=" << stats.empty_polls
                  << " | source_errs=" << stats.source_errors
                  << " | ack=" << stats.source_acks
                  << " | ack_fail=" << stats.source_ack_failures
                  << " | reject=" << stats.source_rejects
                  << " | reject_fail=" << stats.source_reject_failures
                  << " | ticks=" << stats.ticks
                  << " | redis_cmds=" << stats.redis_commands
                  << " | td_sql=" << stats.tdengine_statements
                  << " | redis_committed=" << stats.redis_committed_quotes
                  << " | last_in=" << stats.last_batch_source_input
                  << " | last_reject=" << stats.last_batch_source_rejected
                  << " | last_ticks=" << stats.last_batch_ticks
                  << " | last_redis=" << stats.last_batch_redis_commands
                  << " | last_td=" << stats.last_batch_tdengine_statements
                  << " | last_ts_ms=" << stats.last_batch_logical_ts_ms
                  << std::endl;
        return 3;
    }
    if (config.logging.verbose) {
        std::cout << "t1_v2 summary | batches=" << stats.batches
                  << " | source_in=" << stats.source_input
                  << " | source_reject=" << stats.source_rejected
                  << " | ack=" << stats.source_acks
                  << " | ack_fail=" << stats.source_ack_failures
                  << " | ack_skip=" << stats.source_ack_skipped
                  << " | reject=" << stats.source_rejects
                  << " | reject_fail=" << stats.source_reject_failures
                  << " | ticks=" << stats.ticks
                  << " | empty=" << stats.empty_polls
                  << " | redis_cmds=" << stats.redis_commands
                  << " | td_sql=" << stats.tdengine_statements
                  << " | redis_committed=" << stats.redis_committed_quotes
                  << std::endl;
    }

    return 0;
}
