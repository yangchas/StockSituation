#include "runtime_loop.h"

#include <chrono>
#include <iostream>
#include <thread>

namespace t1_v2 {

RuntimeLoop::RuntimeLoop(
    const ConfigV2& config,
    std::unique_ptr<ITickSource> source,
    IRedisCommandExecutor& redis_executor,
    ITDengineCommandExecutor& tdengine_executor,
    RuntimeLoopOptions options
) : config_(config),
    source_(std::move(source)),
    pipeline_(config),
    coordinator_(redis_executor, tdengine_executor),
    options_(options) {}

RuntimeLoopStats RuntimeLoop::run() {
    RuntimeLoopStats stats;
    if (!source_) {
        stats.ok = false;
        stats.error = "tick source is null";
        return stats;
    }
    if (config_.runtime_mode == RuntimeMode::Live && config_.processing.dry_run && !has_runtime_bound()) {
        stats.ok = false;
        stats.error = "live dry-run requires --max-batches or --max-empty-polls";
        return stats;
    }
    if (!pipeline_.initialize()) {
        stats.ok = false;
        stats.error = "runtime pipeline initialize failed";
        return stats;
    }
    const RuntimePreflightResult preflight = coordinator_.preflight(
        should_preflight_redis(),
        should_preflight_tdengine()
    );
    if (!preflight.ok) {
        pipeline_.shutdown();
        stats.ok = false;
        stats.error = preflight.error.empty() ? "runtime preflight failed" : preflight.error;
        return stats;
    }
    if (!source_->start()) {
        pipeline_.shutdown();
        stats.ok = false;
        const std::string source_error = source_->error_message();
        stats.error = source_error.empty() ? "tick source is not ready" : source_error;
        return stats;
    }
    if (config_.logging.verbose) {
        std::cout << "t1_v2 preflight | mode="
                  << (config_.runtime_mode == RuntimeMode::Replay ? "replay" : "live")
                  << " | redis=" << (preflight.redis_checked ? "ok" : "skip")
                  << " | tdengine=" << (preflight.tdengine_checked ? "ok" : "skip")
                  << " | source=ok"
                  << std::endl;
    }

    while (true) {
        TickSourceResult source_result = source_->next_batch();
        if (source_result.status == TickSourceStatus::EndOfStream) {
            break;
        }
        if (source_result.status == TickSourceStatus::Empty) {
            ++stats.empty_polls;
            if (should_stop_after_empty(stats)) {
                break;
            }
            sleep_after_empty();
            continue;
        }
        if (source_result.status == TickSourceStatus::Error) {
            ++stats.source_errors;
            if (source_result.requires_ack) {
                if (!should_ack_source()) {
                    ++stats.source_ack_skipped;
                } else if (source_->reject(source_result, source_result.requeue_on_error)) {
                    ++stats.source_rejects;
                } else {
                    ++stats.source_reject_failures;
                }
            }
            stats.ok = false;
            stats.error = source_result.error_msg ? source_result.error_msg : "tick source error";
            break;
        }

        RuntimePipelineResult batch_result = pipeline_.process_batch(
            source_result.batch,
            source_result.source_stats
        );
        RuntimeExecutionResult execution_result = coordinator_.execute_and_commit(pipeline_, batch_result);
        if (!execution_result.ok) {
            if (source_result.requires_ack) {
                if (!should_ack_source()) {
                    ++stats.source_ack_skipped;
                } else if (source_->reject(source_result, source_result.requeue_on_error)) {
                    ++stats.source_rejects;
                } else {
                    ++stats.source_reject_failures;
                }
            }
            stats.ok = false;
            stats.error = execution_result.tdengine.ok ? execution_result.redis.error : execution_result.tdengine.error;
            if (stats.error.empty()) {
                stats.error = "runtime execution failed";
            }
            break;
        }
        if (source_result.requires_ack) {
            if (!should_ack_source()) {
                ++stats.source_ack_skipped;
            } else if (source_->ack(source_result)) {
                ++stats.source_acks;
            } else {
                ++stats.source_ack_failures;
                stats.ok = false;
                stats.error = "source ack failed";
                break;
            }
        }

        ++stats.batches;
        stats.source_input += batch_result.runtime_stats.source_input;
        stats.source_rejected += batch_result.runtime_stats.source_rejected;
        stats.ticks += batch_result.engine_stats.tick_count;
        stats.redis_commands += batch_result.redis_commands.size();
        stats.tdengine_statements += batch_result.tdengine_statements.size();
        stats.redis_committed_quotes += static_cast<uint64_t>(execution_result.redis_committed_quotes);
        if (options_.max_batches > 0 && stats.batches >= options_.max_batches) {
            break;
        }
    }

    source_->stop();
    pipeline_.shutdown();
    return stats;
}

bool RuntimeLoop::should_preflight_redis() const {
    if (config_.processing.dry_run) {
        return false;
    }
    return config_.runtime_mode != RuntimeMode::Replay || config_.replay.write_redis;
}

bool RuntimeLoop::should_preflight_tdengine() const {
    if (config_.processing.dry_run) {
        return false;
    }
    return config_.runtime_mode != RuntimeMode::Replay || config_.replay.write_tdengine;
}

bool RuntimeLoop::should_stop_after_empty(RuntimeLoopStats& stats) const {
    return options_.max_empty_polls > 0 && stats.empty_polls >= options_.max_empty_polls;
}

bool RuntimeLoop::should_ack_source() const {
    return !config_.processing.dry_run || config_.processing.ack_in_dry_run;
}

bool RuntimeLoop::has_runtime_bound() const {
    return options_.max_batches > 0 || options_.max_empty_polls > 0 ||
        config_.processing.max_batches > 0 || config_.processing.max_empty_polls > 0;
}

void RuntimeLoop::sleep_after_empty() const {
    if (!config_.processing.enable_rate_limiting || config_.processing.processing_delay_ms <= 0) {
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing.processing_delay_ms));
}

}  // namespace t1_v2
