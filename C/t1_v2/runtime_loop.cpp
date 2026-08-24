#include "runtime_loop.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

#include "runtime_log.h"

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
    uint32_t consecutive_failures = 0;
    bool boot_preflight_ok = false;
    bool boot_source_ok = false;
    if (!source_) {
        stats.ok = false;
        stats.failure_stage = "bootstrap.null_source";
        stats.error = "tick source is null";
        return stats;
    }
    if (config_.runtime_mode == RuntimeMode::Live && config_.processing.dry_run && !has_runtime_bound()) {
        stats.ok = false;
        stats.failure_stage = "bootstrap.invalid_live_dry_run";
        stats.error = "live dry-run requires --max-batches or --max-empty-polls";
        return stats;
    }
    if (!pipeline_.initialize()) {
        stats.ok = false;
        stats.failure_stage = "bootstrap.pipeline_initialize";
        stats.error = "runtime pipeline initialize failed";
        return stats;
    }
    const RuntimePreflightResult preflight = coordinator_.preflight(
        should_preflight_redis(),
        should_preflight_tdengine()
    );
    boot_preflight_ok = preflight.ok;
    if (!preflight.ok) {
        if (!should_tolerate_runtime_failure()) {
            pipeline_.shutdown();
            stats.ok = false;
            stats.failure_stage = "bootstrap.preflight";
            stats.error = preflight.error.empty() ? "runtime preflight failed" : preflight.error;
            return stats;
        }
        log_transient_failure(
            "bootstrap.preflight",
            preflight.error.empty() ? "runtime preflight failed" : preflight.error,
            ++consecutive_failures
        );
    }
    if (!source_->start()) {
        const std::string source_error = source_->error_message();
        if (!should_tolerate_runtime_failure()) {
            pipeline_.shutdown();
            stats.ok = false;
            stats.failure_stage = "bootstrap.source_start";
            stats.source_error = source_error;
            stats.error = source_error.empty() ? "tick source is not ready" : source_error;
            return stats;
        }
        log_transient_failure(
            "bootstrap.source_start",
            source_error.empty() ? "tick source is not ready" : source_error,
            ++consecutive_failures
        );
    } else {
        boot_source_ok = true;
    }
    if (config_.logging.verbose) {
        const bool local_q2frame_replay = config_.runtime_mode == RuntimeMode::Replay &&
            !config_.replay.q2frame_path.empty();
        std::cout << runtime_log_ts() << " | t1_v2 preflight | mode="
                  << (config_.runtime_mode == RuntimeMode::Replay ? "replay" : "live")
                  << " | redis=" << (local_q2frame_replay ? "skip" : (
                        preflight.redis_checked
                            ? (boot_preflight_ok || preflight.redis.ok ? "ok" : "degraded")
                            : "skip"
                     ))
                  << " | q2frame=" << (local_q2frame_replay
                        ? (boot_preflight_ok ? "ok" : "degraded")
                        : "skip")
                  << " | tdengine=" << (
                        preflight.tdengine_checked
                            ? (boot_preflight_ok || preflight.tdengine.ok ? "ok" : "degraded")
                            : "skip"
                     )
                  << " | source=" << (boot_source_ok ? "ok" : "degraded")
                  << std::endl;
    }

    while (true) {
        TickSourceResult source_result = source_->next_batch();
        if (source_result.status == TickSourceStatus::EndOfStream) {
            break;
        }
        if (source_result.status == TickSourceStatus::Skipped) {
            stats.source_input += source_result.source_stats.input_count;
            stats.source_rejected += source_result.source_stats.rejected_count;
            if (source_result.requires_ack) {
                if (!should_ack_source()) {
                    ++stats.source_ack_skipped;
                } else if (source_->reject(source_result, false)) {
                    ++stats.source_rejects;
                } else {
                    ++stats.source_reject_failures;
                    if (!should_tolerate_runtime_failure()) {
                        stats.ok = false;
                        stats.failure_stage = "source.reject_after_skip";
                        stats.source_error = source_->error_message();
                        stats.error = "source reject failed";
                        break;
                    }
                    log_transient_failure(
                        "source.reject_after_skip",
                        source_->error_message().empty() ? "source reject failed" : source_->error_message(),
                        ++consecutive_failures
                    );
                    sleep_after_failure(consecutive_failures);
                    continue;
                }
            }
            consecutive_failures = 0;
            continue;
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
            if (!should_tolerate_runtime_failure()) {
                stats.ok = false;
                stats.failure_stage = "source.batch_error";
                stats.source_error = source_->error_message();
                stats.error = source_result.error_msg ? source_result.error_msg : "tick source error";
                break;
            }
            log_transient_failure(
                "source.batch_error",
                source_result.error_msg ? source_result.error_msg : (source_->error_message().empty() ? "tick source error" : source_->error_message()),
                ++consecutive_failures
            );
            try_full_recovery(consecutive_failures);
            sleep_after_failure(consecutive_failures);
            continue;
        }

        RuntimePipelineResult batch_result = pipeline_.process_batch(
            source_result.batch,
            source_result.source_stats
        );
        stats.last_batch_source_input = batch_result.runtime_stats.source_input;
        stats.last_batch_source_rejected = batch_result.runtime_stats.source_rejected;
        stats.last_batch_ticks = batch_result.engine_stats.tick_count;
        stats.last_batch_redis_commands = batch_result.redis_commands.size();
        stats.last_batch_tdengine_statements = batch_result.tdengine_statements.size();
        stats.last_batch_logical_ts_ms = batch_result.runtime_stats.logical_ts_ms;
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
            const std::string execution_error = execution_result.tdengine.ok
                ? execution_result.redis.error
                : execution_result.tdengine.error;
            if (!should_tolerate_runtime_failure()) {
                stats.ok = false;
                stats.failure_stage = execution_result.tdengine.ok ? "commit.redis" : "commit.tdengine";
                stats.source_error = source_->error_message();
                stats.error = execution_error;
                if (stats.error.empty()) {
                    stats.error = "runtime execution failed";
                }
                break;
            }
            log_transient_failure(
                execution_result.tdengine.ok ? "commit.redis" : "commit.tdengine",
                execution_error.empty() ? "runtime execution failed" : execution_error,
                ++consecutive_failures
            );
            try_full_recovery(consecutive_failures);
            sleep_after_failure(consecutive_failures);
            continue;
        }
        if (source_result.requires_ack) {
            if (!should_ack_source()) {
                ++stats.source_ack_skipped;
            } else if (source_->ack(source_result)) {
                ++stats.source_acks;
            } else {
                ++stats.source_ack_failures;
                if (!should_tolerate_runtime_failure()) {
                    stats.ok = false;
                    stats.failure_stage = "source.ack";
                    stats.source_error = source_->error_message();
                    stats.error = "source ack failed";
                    break;
                }
                log_transient_failure(
                    "source.ack",
                    source_->error_message().empty() ? "source ack failed" : source_->error_message(),
                    ++consecutive_failures
                );
                try_full_recovery(consecutive_failures);
                sleep_after_failure(consecutive_failures);
                continue;
            }
        }

        if (consecutive_failures > 0 && config_.logging.verbose) {
            std::cout << runtime_log_ts()
                      << " | t1_v2 recovered"
                      << " | after_failures=" << consecutive_failures
                      << " | batches=" << stats.batches + 1
                      << std::endl;
        }
        consecutive_failures = 0;
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
    // A Q2Frame replay must preflight its local file executor, while keeping
    // external Redis writes disabled.
    return config_.runtime_mode != RuntimeMode::Replay ||
        config_.replay.write_redis || !config_.replay.q2frame_path.empty();
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

bool RuntimeLoop::should_tolerate_runtime_failure() const {
    return config_.runtime_mode == RuntimeMode::Live;
}

void RuntimeLoop::sleep_after_empty() const {
    if (!config_.processing.enable_rate_limiting || config_.processing.processing_delay_ms <= 0) {
        return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing.processing_delay_ms));
}

void RuntimeLoop::sleep_after_failure(uint32_t consecutive_failures) const {
    int base_delay_ms = config_.processing.retry_delay_ms > 0 ? config_.processing.retry_delay_ms : 1000;
    const uint32_t multiplier = std::min<uint32_t>(consecutive_failures == 0 ? 1 : consecutive_failures, 5);
    std::this_thread::sleep_for(std::chrono::milliseconds(base_delay_ms * static_cast<int>(multiplier)));
}

void RuntimeLoop::log_transient_failure(const char* stage, const std::string& error, uint32_t consecutive_failures) const {
    std::cerr << runtime_log_ts()
              << " | t1_v2 transient"
              << " | stage=" << (stage ? stage : "-")
              << " | consecutive=" << consecutive_failures
              << " | error=" << (error.empty() ? "-" : error)
              << std::endl;
}

uint32_t RuntimeLoop::transient_reset_threshold() const {
    return static_cast<uint32_t>(
        config_.processing.transient_failures_before_reset > 0
            ? config_.processing.transient_failures_before_reset
            : 3
    );
}

void RuntimeLoop::try_full_recovery(uint32_t consecutive_failures) {
    if (consecutive_failures < transient_reset_threshold()) {
        return;
    }
    const RuntimePreflightResult recheck = coordinator_.recheck_after_reset(
        should_preflight_redis(),
        should_preflight_tdengine()
    );
    std::cerr << runtime_log_ts()
              << " | t1_v2 reset"
              << " | threshold=" << transient_reset_threshold()
              << " | consecutive=" << consecutive_failures
              << " | redis=" << (recheck.redis_checked ? (recheck.redis.ok ? "ok" : "fail") : "skip")
              << " | tdengine=" << (recheck.tdengine_checked ? (recheck.tdengine.ok ? "ok" : "fail") : "skip")
              << " | error=" << (recheck.error.empty() ? "-" : recheck.error)
              << std::endl;
    source_->stop();
    const bool restarted = source_->start();
    std::cerr << runtime_log_ts()
              << " | t1_v2 source_restart"
              << " | ok=" << (restarted ? "yes" : "no")
              << " | error=" << (source_->error_message().empty() ? "-" : source_->error_message())
              << std::endl;
}

}  // namespace t1_v2
