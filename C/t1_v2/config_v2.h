#pragma once

#include <cstdint>
#include <string>

#include "runtime_mode.h"

namespace t1_v2 {

struct RabbitMqConfig {
    std::string host = "localhost";
    int port = 5672;
    std::string user = "admin";
    std::string password = "admin";
    std::string vhost = "/";
    std::string queue_name = "stream2";
    int connect_timeout_ms = 1000;
    int consume_timeout_ms = 1000;
    int heartbeat_seconds = 30;
};

struct RedisConfig {
    std::string host = "localhost";
    int port = 6379;
    int db = 0;

    std::string q2_prefix = "q2:";
    std::string a2_prefix = "a2:";
    std::string m2_prefix = "m2:";
    std::string legacy_auction_prefix = "market:auction:";
    std::string legacy_anchor_prefix = "market:auction:anchor:";
    int quote_ttl_seconds = 86400;
    int auction_ttl_seconds = 172800;
    int auction_top_n = 200;
};

struct TdengineConfig {
    std::string host = "chaos";
    int port = 6030;
    std::string user = "root";
    std::string password = "taosdata";
    std::string database = "market_data1";
    std::string replay_table = "stock_tick_v2";
};

struct ReplayConfig {
    std::string start_time = "2026-04-29 09:25:00";
    std::string end_time = "2026-04-29 09:25:03";
    std::string tickpack_path;
    std::string q2frame_path;
    std::string auction_command_path;
    int speed = 1;
    bool loop = false;
    int tick_interval_ms = 3000;
    int batch_size = 5000;
    bool write_redis = true;
    bool write_tdengine = false;
};

struct ProcessingConfig {
    int messages_per_batch = 1;
    int max_retry_count = 3;
    int retry_delay_ms = 1000;
    int processing_delay_ms = 10;
    bool enable_rate_limiting = true;
    int report_interval_seconds = 10;

    int minute1_window_ms = 60000;
    int minute5_window_ms = 300000;
    int max_history_ticks = 120;
    int volatility_threshold_ms = 10000;

    int q2_min_write_interval_ms = 400;
    int a2_latest_interval_ms = 1000;
    int runtime_interval_ms = 2000;
    bool dry_run = false;
    bool ack_in_dry_run = false;
    int max_batches = 0;
    int max_empty_polls = 0;
    int transient_failures_before_reset = 3;
};

struct LoggingConfig {
    std::string file_path = "stock_analysis_v2.log";
    int level = 2;
    bool enable_file_log = true;
    bool verbose = true;
};

struct ThresholdConfig {
    double price_change_threshold = 0.02;
    double volume_ratio_threshold = 3.0;
    double min_amount_threshold = 1000000.0;
    double large_order_threshold = 500000.0;
};

struct RuntimeKeysConfig {
    std::string volatile_pool_key = "stock:volatile_pool";
    std::string first_limit_up_key = "stock:first_limit_up";
    int volatile_expire_seconds = 3600;
};

struct ConfigV2 {
    RuntimeMode runtime_mode = RuntimeMode::Live;
    RabbitMqConfig rabbitmq;
    RedisConfig redis;
    TdengineConfig tdengine;
    ReplayConfig replay;
    ProcessingConfig processing;
    LoggingConfig logging;
    ThresholdConfig thresholds;
    RuntimeKeysConfig keys;
};

struct CommandLineParseResult {
    bool ok = true;
    bool show_help = false;
    bool run_self_test = false;
    std::string error;
};

class ConfigManagerV2 {
public:
    static ConfigManagerV2& instance();

    ConfigManagerV2(const ConfigManagerV2&) = delete;
    ConfigManagerV2& operator=(const ConfigManagerV2&) = delete;

    const ConfigV2& get() const { return config_; }
    void update(const ConfigV2& config) { config_ = config; }
    void reload_from_environment();
    CommandLineParseResult apply_command_line(int argc, char* argv[]);

    static CommandLineParseResult apply_command_line_to_config(int argc, char* argv[], ConfigV2& config);
    static std::string usage_text(const char* program_name);

private:
    ConfigManagerV2();

    static int read_env_int(const char* key, int fallback);
    static bool read_env_bool(const char* key, bool fallback);
    static std::string read_env_string(const char* key, const std::string& fallback);

private:
    ConfigV2 config_;
};

}  // namespace t1_v2
