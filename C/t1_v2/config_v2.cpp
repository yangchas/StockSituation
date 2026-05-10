#include "config_v2.h"

#include <cstdlib>
#include <sstream>

namespace t1_v2 {

ConfigManagerV2& ConfigManagerV2::instance() {
    static ConfigManagerV2 manager;
    return manager;
}

ConfigManagerV2::ConfigManagerV2() {
    reload_from_environment();
}

void ConfigManagerV2::reload_from_environment() {
    const std::string data_source = read_env_string("DATA_SOURCE", "");
    if (data_source == "tdengine_replay" || data_source == "replay") {
        config_.runtime_mode = RuntimeMode::Replay;
    } else if (data_source == "rabbitmq" || data_source == "live") {
        config_.runtime_mode = RuntimeMode::Live;
    }

    config_.rabbitmq.host = read_env_string("RABBITMQ_HOST", config_.rabbitmq.host);
    config_.rabbitmq.port = read_env_int("RABBITMQ_PORT", config_.rabbitmq.port);
    config_.rabbitmq.user = read_env_string("RABBITMQ_USER", config_.rabbitmq.user);
    config_.rabbitmq.password = read_env_string("RABBITMQ_PASSWORD", config_.rabbitmq.password);
    config_.rabbitmq.vhost = read_env_string("RABBITMQ_VHOST", config_.rabbitmq.vhost);
    config_.rabbitmq.queue_name = read_env_string("QUEUE_NAME", config_.rabbitmq.queue_name);
    config_.rabbitmq.connect_timeout_ms = read_env_int("RABBITMQ_CONNECT_TIMEOUT_MS", config_.rabbitmq.connect_timeout_ms);
    config_.rabbitmq.consume_timeout_ms = read_env_int("RABBITMQ_CONSUME_TIMEOUT_MS", config_.rabbitmq.consume_timeout_ms);
    config_.rabbitmq.heartbeat_seconds = read_env_int("RABBITMQ_HEARTBEAT_SECONDS", config_.rabbitmq.heartbeat_seconds);

    config_.redis.host = read_env_string("REDIS_HOST", config_.redis.host);
    config_.redis.port = read_env_int("REDIS_PORT", config_.redis.port);
    config_.redis.db = read_env_int("REDIS_DB", config_.redis.db);
    config_.redis.q2_prefix = read_env_string("REDIS_Q2_PREFIX", config_.redis.q2_prefix);
    config_.redis.a2_prefix = read_env_string("REDIS_A2_PREFIX", config_.redis.a2_prefix);
    config_.redis.m2_prefix = read_env_string("REDIS_M2_PREFIX", config_.redis.m2_prefix);
    config_.redis.legacy_auction_prefix =
        read_env_string("REDIS_LEGACY_AUCTION_PREFIX", config_.redis.legacy_auction_prefix);
    config_.redis.legacy_anchor_prefix =
        read_env_string("REDIS_LEGACY_ANCHOR_PREFIX", config_.redis.legacy_anchor_prefix);
    config_.redis.quote_ttl_seconds = read_env_int("REDIS_QUOTE_TTL_SECONDS", config_.redis.quote_ttl_seconds);
    config_.redis.auction_ttl_seconds = read_env_int("REDIS_AUCTION_TTL_SECONDS", config_.redis.auction_ttl_seconds);
    config_.redis.auction_top_n = read_env_int("REDIS_AUCTION_TOP_N", config_.redis.auction_top_n);

    config_.tdengine.host = read_env_string("TDENGINE_HOST", config_.tdengine.host);
    config_.tdengine.port = read_env_int("TDENGINE_PORT", config_.tdengine.port);
    config_.tdengine.user = read_env_string("TDENGINE_USER", config_.tdengine.user);
    config_.tdengine.password = read_env_string("TDENGINE_PASSWORD", config_.tdengine.password);
    config_.tdengine.database = read_env_string("TDENGINE_DATABASE", config_.tdengine.database);
    config_.tdengine.replay_table = read_env_string("TDENGINE_REPLAY_TABLE", config_.tdengine.replay_table);

    config_.replay.start_time = read_env_string("REPLAY_START_TIME", config_.replay.start_time);
    config_.replay.end_time = read_env_string("REPLAY_END_TIME", config_.replay.end_time);
    config_.replay.speed = read_env_int("REPLAY_SPEED", config_.replay.speed);
    config_.replay.loop = read_env_bool("REPLAY_LOOP", config_.replay.loop);
    config_.replay.tick_interval_ms = read_env_int("REPLAY_TICK_INTERVAL_MS", config_.replay.tick_interval_ms);
    config_.replay.batch_size = read_env_int("REPLAY_BATCH_SIZE", config_.replay.batch_size);
    config_.replay.write_redis = read_env_bool("REPLAY_WRITE_REDIS", config_.replay.write_redis);
    config_.replay.write_tdengine = read_env_bool("REPLAY_WRITE_TDENGINE", config_.replay.write_tdengine);

    config_.processing.messages_per_batch = read_env_int("MESSAGES_PER_BATCH", config_.processing.messages_per_batch);
    config_.processing.max_retry_count = read_env_int("MAX_RETRY_COUNT", config_.processing.max_retry_count);
    config_.processing.retry_delay_ms = read_env_int("RETRY_DELAY_MS", config_.processing.retry_delay_ms);
    config_.processing.processing_delay_ms = read_env_int("PROCESSING_DELAY_MS", config_.processing.processing_delay_ms);
    config_.processing.enable_rate_limiting = read_env_bool("ENABLE_RATE_LIMITING", config_.processing.enable_rate_limiting);
    config_.processing.report_interval_seconds =
        read_env_int("REPORT_INTERVAL_SECONDS", config_.processing.report_interval_seconds);
    config_.processing.q2_min_write_interval_ms =
        read_env_int("Q2_MIN_WRITE_INTERVAL_MS", config_.processing.q2_min_write_interval_ms);
    config_.processing.a2_latest_interval_ms =
        read_env_int("A2_LATEST_INTERVAL_MS", config_.processing.a2_latest_interval_ms);
    config_.processing.runtime_interval_ms =
        read_env_int("RUNTIME_INTERVAL_MS", config_.processing.runtime_interval_ms);
    config_.processing.dry_run = read_env_bool("DRY_RUN", config_.processing.dry_run);
    config_.processing.ack_in_dry_run = read_env_bool("ACK_IN_DRY_RUN", config_.processing.ack_in_dry_run);
    config_.processing.max_batches = read_env_int("MAX_BATCHES", config_.processing.max_batches);
    config_.processing.max_empty_polls = read_env_int("MAX_EMPTY_POLLS", config_.processing.max_empty_polls);

    config_.logging.file_path = read_env_string("LOG_FILE_PATH", config_.logging.file_path);
    config_.logging.level = read_env_int("LOG_LEVEL", config_.logging.level);
    config_.logging.enable_file_log = read_env_bool("ENABLE_FILE_LOG", config_.logging.enable_file_log);
    config_.logging.verbose = read_env_bool("VERBOSE", config_.logging.verbose);
}

CommandLineParseResult ConfigManagerV2::apply_command_line(int argc, char* argv[]) {
    CommandLineParseResult result = apply_command_line_to_config(argc, argv, config_);
    return result;
}

CommandLineParseResult ConfigManagerV2::apply_command_line_to_config(int argc, char* argv[], ConfigV2& config) {
    CommandLineParseResult result;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i] ? std::string(argv[i]) : std::string();

        auto require_value = [&](const char* option) -> const char* {
            if (i + 1 >= argc || argv[i + 1] == nullptr) {
                result.ok = false;
                result.error = std::string("missing value for ") + option;
                return nullptr;
            }
            return argv[++i];
        };

        if (arg == "--help" || arg == "-h") {
            result.show_help = true;
            return result;
        }
        if (arg == "--self-test") {
            result.run_self_test = true;
            return result;
        }
        if (arg == "--replay" || arg == "-r") {
            config.runtime_mode = RuntimeMode::Replay;
            continue;
        }
        if (arg == "--live" || arg == "-l") {
            config.runtime_mode = RuntimeMode::Live;
            continue;
        }
        if (arg == "--start" || arg == "-s") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.replay.start_time = value;
            continue;
        }
        if (arg == "--end" || arg == "-e") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.replay.end_time = value;
            continue;
        }
        if (arg == "--speed" || arg == "-sp") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.replay.speed = std::atoi(value);
            if (config.replay.speed <= 0) {
                config.replay.speed = 1;
            }
            continue;
        }
        if (arg == "--queue") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.rabbitmq.queue_name = value;
            continue;
        }
        if (arg == "--replay-table") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.tdengine.replay_table = value;
            continue;
        }
        if (arg == "--replay-write-redis") {
            config.replay.write_redis = true;
            continue;
        }
        if (arg == "--no-replay-write-redis") {
            config.replay.write_redis = false;
            continue;
        }
        if (arg == "--replay-write-tdengine") {
            config.replay.write_tdengine = true;
            continue;
        }
        if (arg == "--no-replay-write-tdengine") {
            config.replay.write_tdengine = false;
            continue;
        }
        if (arg == "--dry-run") {
            config.processing.dry_run = true;
            continue;
        }
        if (arg == "--no-dry-run") {
            config.processing.dry_run = false;
            continue;
        }
        if (arg == "--ack-in-dry-run") {
            config.processing.ack_in_dry_run = true;
            continue;
        }
        if (arg == "--max-batches") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.processing.max_batches = std::atoi(value);
            if (config.processing.max_batches < 0) {
                config.processing.max_batches = 0;
            }
            continue;
        }
        if (arg == "--max-empty-polls") {
            const char* value = require_value(arg.c_str());
            if (!result.ok) return result;
            config.processing.max_empty_polls = std::atoi(value);
            if (config.processing.max_empty_polls < 0) {
                config.processing.max_empty_polls = 0;
            }
            continue;
        }

        result.ok = false;
        result.error = "unknown option: " + arg;
        return result;
    }
    return result;
}

std::string ConfigManagerV2::usage_text(const char* program_name) {
    std::ostringstream oss;
    const char* name = (program_name && *program_name) ? program_name : "t1_v2";
    oss << "Usage: " << name << " [options]\n"
        << "\nNo-argument default: live RabbitMQ mode with Redis/TDengine writes enabled by compiled features.\n"
        << "Use --replay for TDengine replay and --dry-run for validation.\n"
        << "Options:\n"
        << "  -r, --replay                 Run TDengine replay mode\n"
        << "  -s, --start <time>           Replay start time, YYYY-MM-DD HH:MM:SS\n"
        << "  -e, --end <time>             Replay end time, YYYY-MM-DD HH:MM:SS\n"
        << "  -sp, --speed <n>             Replay speed multiplier\n"
        << "      --queue <name>           RabbitMQ queue name\n"
        << "      --replay-table <name>    TDengine replay table\n"
        << "      --replay-write-redis     Write Redis while replaying\n"
        << "      --replay-write-tdengine  Write TDengine while replaying\n"
        << "      --dry-run                Decode and compute but suppress Redis/TD writes\n"
        << "      --max-batches <n>        Stop after n processed batches, 0 means unlimited\n"
        << "      --max-empty-polls <n>    Stop after n empty polls, 0 means unlimited\n"
        << "      --self-test              Run built-in semantic checks\n"
        << "  -h, --help                   Show this help\n";
    return oss.str();
}

int ConfigManagerV2::read_env_int(const char* key, int fallback) {
    const char* value = std::getenv(key);
    return value ? std::atoi(value) : fallback;
}

bool ConfigManagerV2::read_env_bool(const char* key, bool fallback) {
    const char* value = std::getenv(key);
    if (!value) {
        return fallback;
    }
    const std::string text(value);
    if (text == "1" || text == "true" || text == "TRUE" || text == "yes" || text == "YES") {
        return true;
    }
    if (text == "0" || text == "false" || text == "FALSE" || text == "no" || text == "NO") {
        return false;
    }
    return fallback;
}

std::string ConfigManagerV2::read_env_string(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    return value ? std::string(value) : fallback;
}

}  // namespace t1_v2
