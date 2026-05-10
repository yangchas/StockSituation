#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "config_v2.h"
#include "quote_state.h"
#include "quote_state_store.h"
#include "snapshot_trigger.h"

namespace t1_v2 {

struct RuntimeBatchStats;

using RedisFieldList = std::vector<std::pair<std::string, std::string>>;

enum class RedisCommandType {
    HSetMulti,
    SetString,
    SAdd,
    Expire,
};

struct RedisCommand {
    RedisCommandType type = RedisCommandType::HSetMulti;
    std::string key;
    std::string value;
    std::vector<std::string> members;
    RedisFieldList fields;
    int ttl_seconds = 0;
};

class RedisV2Writer {
public:
    explicit RedisV2Writer(const ConfigV2& config);

    bool initialize();
    void shutdown();

    bool should_write_q2(const QuoteState& state, int64_t logical_ts_ms) const;
    std::string quote_key(const QuoteState& state) const;
    std::string active_symbol_key(int64_t logical_ts_ms) const;
    RedisFieldList build_q2_fields(const QuoteState& state) const;
    void mark_q2_written(QuoteState& state, int64_t logical_ts_ms) const;
    int commit_q2_writes(QuoteStateStore& store, int64_t logical_ts_ms) const;
    std::vector<RedisCommand> build_q2_commands(const QuoteStateStore& store, int64_t logical_ts_ms) const;
    std::vector<RedisCommand> build_a2_commands(
        const QuoteStateStore& store,
        const SnapshotTriggerState& trigger,
        int64_t logical_ts_ms
    ) const;
    std::vector<RedisCommand> build_runtime_commands(const RuntimeBatchStats& stats, RuntimeMode mode) const;

private:
    static std::string to_string_i64(int64_t value);
    static std::string to_string_i32(int value);
    static std::string trade_date_yyyymmdd(int64_t ts_ms);
    static std::string auction_tag_from_trigger(const SnapshotTriggerState& trigger);
    static int change_bp(const QuoteState& state);
    static bool is_equity_alias_state(const QuoteState& state);
    static std::string change_pct_string(const QuoteState& state);
    std::string build_a2_meta_json(const std::string& tag, int64_t logical_ts_ms, int row_count) const;
    std::string build_top_json(const std::vector<const QuoteState*>& rows) const;
    std::string build_legacy_summary_json(
        const std::string& tag,
        int64_t logical_ts_ms,
        const std::vector<const QuoteState*>& rows
    ) const;
    std::string build_legacy_top_amount_json(const std::vector<const QuoteState*>& rows) const;
    std::string build_anchor_archive_json(const std::string& tag, const std::vector<const QuoteState*>& rows) const;

private:
    ConfigV2 config_;
    bool initialized_ = false;
};

}  // namespace t1_v2
