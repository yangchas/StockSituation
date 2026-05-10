#include "redis_v2_writer.h"

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <sstream>

#include "runtime_batch_stats.h"

namespace t1_v2 {

RedisV2Writer::RedisV2Writer(const ConfigV2& config) : config_(config) {}

bool RedisV2Writer::initialize() {
    initialized_ = true;
    return true;
}

void RedisV2Writer::shutdown() {
    initialized_ = false;
}

bool RedisV2Writer::should_write_q2(const QuoteState& state, int64_t logical_ts_ms) const {
    if (!initialized_ || state.symbol[0] == '\0' || state.dirty_mask == DIRTY_NONE) {
        return false;
    }

    const bool has_anchor_dirty = (state.dirty_mask & (DIRTY_A20 | DIRTY_A24 | DIRTY_A25)) != 0;
    if (has_anchor_dirty) {
        return true;
    }

    if (state.last_written_ts_ms <= 0) {
        return true;
    }
    return logical_ts_ms - state.last_written_ts_ms >= config_.processing.q2_min_write_interval_ms;
}

std::string RedisV2Writer::quote_key(const QuoteState& state) const {
    return config_.redis.q2_prefix + std::string(state.symbol);
}

std::string RedisV2Writer::active_symbol_key(int64_t logical_ts_ms) const {
    const std::string date = trade_date_yyyymmdd(logical_ts_ms);
    if (date.empty()) {
        return "";
    }
    return config_.redis.q2_prefix + "active:" + date;
}

RedisFieldList RedisV2Writer::build_q2_fields(const QuoteState& state) const {
    RedisFieldList fields;
    fields.reserve(23);
    fields.emplace_back("px", to_string_i32(state.px_milli));
    fields.emplace_back("pc", to_string_i32(state.pc_milli));
    fields.emplace_back("amt", to_string_i64(state.amt_yuan));
    fields.emplace_back("vol", to_string_i64(state.vol_units));
    fields.emplace_back("iv", to_string_i64(state.inst_vol));
    fields.emplace_back("ia", to_string_i64(state.inst_amt_yuan));
    fields.emplace_back("ln", to_string_i64(state.large_net_yuan));
    fields.emplace_back("ts", to_string_i64(state.ts_ms));
    fields.emplace_back("ph", to_string_i32(static_cast<int>(state.phase)));
    fields.emplace_back("ls", to_string_i32(static_cast<int>(state.limit_state)));
    fields.emplace_back("mx", to_string_i32(state.mx_milli));
    fields.emplace_back("mn", to_string_i32(state.mn_milli));
    fields.emplace_back("spd1m", to_string_i32(state.spd1m_bp));
    fields.emplace_back("amt2m", to_string_i64(state.amt2m_yuan));
    fields.emplace_back("amt5m", to_string_i64(state.amt5m_yuan));
    fields.emplace_back("vec3m", to_string_i32(state.vec3m_bp));
    fields.emplace_back("vec5m", to_string_i32(state.vec5m_bp));
    fields.emplace_back("a20", to_string_i32(state.auction.a20_px_milli));
    fields.emplace_back("a24", to_string_i32(state.auction.a24_px_milli));
    fields.emplace_back("a25", to_string_i32(state.auction.a25_px_milli));
    fields.emplace_back("am", to_string_i64(state.auction.match_amt_yuan));
    fields.emplace_back("br", to_string_i64(state.auction.rest_bid_amt_yuan));
    fields.emplace_back("ar", to_string_i64(state.auction.rest_ask_amt_yuan));
    return fields;
}

void RedisV2Writer::mark_q2_written(QuoteState& state, int64_t logical_ts_ms) const {
    state.last_written_px_milli = state.px_milli;
    state.last_written_amt_yuan = state.amt_yuan;
    state.last_written_ts_ms = logical_ts_ms;
    state.dirty_mask = DIRTY_NONE;
}

int RedisV2Writer::commit_q2_writes(QuoteStateStore& store, int64_t logical_ts_ms) const {
    int committed = 0;
    store.for_each_active([&](QuoteState& state) {
        if (!should_write_q2(state, logical_ts_ms)) {
            return;
        }
        mark_q2_written(state, logical_ts_ms);
        ++committed;
    });
    return committed;
}

std::vector<RedisCommand> RedisV2Writer::build_q2_commands(const QuoteStateStore& store, int64_t logical_ts_ms) const {
    std::vector<RedisCommand> commands;
    commands.reserve(store.size() * 2 + 2);
    std::vector<std::string> active_symbols;
    active_symbols.reserve(std::min<std::size_t>(store.size(), 512));
    store.for_each_active([&](const QuoteState& state) {
        if (!should_write_q2(state, logical_ts_ms)) {
            return;
        }

        std::string key = quote_key(state);
        RedisCommand hset;
        hset.type = RedisCommandType::HSetMulti;
        hset.key = key;
        hset.fields = build_q2_fields(state);
        commands.emplace_back(std::move(hset));

        RedisCommand expire;
        expire.type = RedisCommandType::Expire;
        expire.key = std::move(key);
        expire.ttl_seconds = config_.redis.quote_ttl_seconds;
        commands.emplace_back(std::move(expire));

        active_symbols.emplace_back(state.symbol);
    });

    const std::string active_key = active_symbol_key(logical_ts_ms);
    if (!active_symbols.empty() && !active_key.empty()) {
        RedisCommand active_sadd;
        active_sadd.type = RedisCommandType::SAdd;
        active_sadd.key = active_key;
        active_sadd.members = std::move(active_symbols);
        commands.emplace_back(std::move(active_sadd));

        RedisCommand active_expire;
        active_expire.type = RedisCommandType::Expire;
        active_expire.key = active_key;
        active_expire.ttl_seconds = config_.redis.quote_ttl_seconds;
        commands.emplace_back(std::move(active_expire));
    }
    return commands;
}

std::vector<RedisCommand> RedisV2Writer::build_a2_commands(
    const QuoteStateStore& store,
    const SnapshotTriggerState& trigger,
    int64_t logical_ts_ms
) const {
    std::vector<RedisCommand> commands;
    const std::string tag = auction_tag_from_trigger(trigger);
    if (!initialized_ || tag.empty() || logical_ts_ms <= 0) {
        return commands;
    }

    std::vector<const QuoteState*> candidates;
    candidates.reserve(store.size());
    store.for_each_active([&](const QuoteState& state) {
        if (state.symbol[0] == '\0' || state.auction.ts_ms <= 0) {
            return;
        }
        if (state.auction.match_amt_yuan <= 0 &&
            state.auction.rest_bid_amt_yuan <= 0 &&
            state.auction.rest_ask_amt_yuan <= 0) {
            return;
        }
        candidates.push_back(&state);
    });

    const std::size_t top_n = static_cast<std::size_t>(std::max(1, config_.redis.auction_top_n));
    auto top_by = [&](auto score_fn) {
        std::vector<const QuoteState*> rows = candidates;
        const std::size_t keep = std::min(top_n, rows.size());
        if (rows.size() > keep) {
            std::partial_sort(rows.begin(), rows.begin() + keep, rows.end(), [&](const QuoteState* lhs, const QuoteState* rhs) {
                return score_fn(*lhs) > score_fn(*rhs);
            });
            rows.resize(keep);
        } else {
            std::sort(rows.begin(), rows.end(), [&](const QuoteState* lhs, const QuoteState* rhs) {
                return score_fn(*lhs) > score_fn(*rhs);
            });
        }
        return rows;
    };

    const std::vector<const QuoteState*> top_amount_rows = top_by([](const QuoteState& state) {
        return state.auction.match_amt_yuan;
    });

    RedisFieldList fields;
    fields.reserve(4);
    fields.emplace_back("meta", build_a2_meta_json(tag, logical_ts_ms, static_cast<int>(candidates.size())));
    fields.emplace_back("top_amt", build_top_json(top_amount_rows));
    fields.emplace_back("top_br", build_top_json(top_by([](const QuoteState& state) {
        return state.auction.rest_bid_amt_yuan;
    })));
    fields.emplace_back("top_chg", build_top_json(top_by([](const QuoteState& state) {
        return static_cast<int64_t>(change_bp(state));
    })));

    const std::string key = config_.redis.a2_prefix + trade_date_yyyymmdd(logical_ts_ms) + ":" + tag;
    RedisCommand hset;
    hset.type = RedisCommandType::HSetMulti;
    hset.key = key;
    hset.fields = std::move(fields);
    commands.emplace_back(std::move(hset));

    RedisCommand expire;
    expire.type = RedisCommandType::Expire;
    expire.key = key;
    expire.ttl_seconds = config_.redis.auction_ttl_seconds;
    commands.emplace_back(std::move(expire));

    const std::string date = trade_date_yyyymmdd(logical_ts_ms);
    const std::string legacy_key = config_.redis.legacy_auction_prefix + date + ":" + tag;
    RedisCommand legacy_hset;
    legacy_hset.type = RedisCommandType::HSetMulti;
    legacy_hset.key = legacy_key;
    legacy_hset.fields.reserve(3);
    legacy_hset.fields.emplace_back("summary", build_legacy_summary_json(tag, logical_ts_ms, candidates));
    legacy_hset.fields.emplace_back("top_amount", build_legacy_top_amount_json(top_amount_rows));
    legacy_hset.fields.emplace_back("meta", build_a2_meta_json(tag, logical_ts_ms, static_cast<int>(candidates.size())));
    commands.emplace_back(std::move(legacy_hset));

    RedisCommand legacy_expire;
    legacy_expire.type = RedisCommandType::Expire;
    legacy_expire.key = legacy_key;
    legacy_expire.ttl_seconds = config_.redis.auction_ttl_seconds;
    commands.emplace_back(std::move(legacy_expire));

    if (tag == "0920" || tag == "0924" || tag == "0925") {
        const std::string latest_key = config_.redis.legacy_auction_prefix + date + ":latest";
        RedisCommand latest_hset;
        latest_hset.type = RedisCommandType::HSetMulti;
        latest_hset.key = latest_key;
        latest_hset.fields.emplace_back("tag", tag);
        latest_hset.fields.emplace_back("ts", to_string_i64(logical_ts_ms));
        commands.emplace_back(std::move(latest_hset));

        RedisCommand latest_expire;
        latest_expire.type = RedisCommandType::Expire;
        latest_expire.key = latest_key;
        latest_expire.ttl_seconds = config_.redis.auction_ttl_seconds;
        commands.emplace_back(std::move(latest_expire));
    }
    if (tag == "0925") {
        const std::string anchor_key = config_.redis.legacy_anchor_prefix + date;
        RedisCommand anchor_set;
        anchor_set.type = RedisCommandType::SetString;
        anchor_set.key = anchor_key;
        anchor_set.value = build_anchor_archive_json(tag, top_amount_rows);
        commands.emplace_back(std::move(anchor_set));

        RedisCommand anchor_expire;
        anchor_expire.type = RedisCommandType::Expire;
        anchor_expire.key = anchor_key;
        anchor_expire.ttl_seconds = 3 * 24 * 60 * 60;
        commands.emplace_back(std::move(anchor_expire));
    }
    return commands;
}

std::vector<RedisCommand> RedisV2Writer::build_runtime_commands(
    const RuntimeBatchStats& stats,
    RuntimeMode mode
) const {
    std::vector<RedisCommand> commands;
    if (!initialized_ || stats.logical_ts_ms <= 0) {
        return commands;
    }
    const std::string date = trade_date_yyyymmdd(stats.logical_ts_ms);
    if (date.empty()) {
        return commands;
    }

    RedisFieldList fields;
    fields.reserve(16);
    fields.emplace_back("ts", to_string_i64(stats.logical_ts_ms));
    fields.emplace_back("seq", to_string_i32(static_cast<int>(stats.seq_no)));
    fields.emplace_back("mode", mode == RuntimeMode::Replay ? "replay" : "live");
    fields.emplace_back("source_ts", to_string_i64(stats.source_max_tick_ts_ms));
    fields.emplace_back("wall_ts", to_string_i64(stats.wall_ts_ms));
    fields.emplace_back("delay_ms", to_string_i64(stats.source_delay_ms));
    fields.emplace_back("source_in", to_string_i32(static_cast<int>(stats.source_input)));
    fields.emplace_back("source_ok", to_string_i32(static_cast<int>(stats.source_accepted)));
    fields.emplace_back("source_rej", to_string_i32(static_cast<int>(stats.source_rejected)));
    fields.emplace_back("ticks", to_string_i32(static_cast<int>(stats.engine_ticks)));
    fields.emplace_back("new", to_string_i32(static_cast<int>(stats.new_symbols)));
    fields.emplace_back("redis_cmd", to_string_i32(static_cast<int>(stats.redis_commands)));
    fields.emplace_back("redis_fields", to_string_i32(static_cast<int>(stats.redis_fields)));
    fields.emplace_back("redis_argv", to_string_i32(static_cast<int>(stats.redis_argv)));
    fields.emplace_back("redis_bytes", to_string_i32(static_cast<int>(stats.redis_payload_bytes)));

    const std::string key = config_.redis.m2_prefix + "runtime:" + date;
    RedisCommand hset;
    hset.type = RedisCommandType::HSetMulti;
    hset.key = key;
    hset.fields = std::move(fields);
    commands.emplace_back(std::move(hset));

    RedisCommand expire;
    expire.type = RedisCommandType::Expire;
    expire.key = key;
    expire.ttl_seconds = config_.redis.quote_ttl_seconds;
    commands.emplace_back(std::move(expire));
    return commands;
}

std::string RedisV2Writer::to_string_i64(int64_t value) {
    return std::to_string(value);
}

std::string RedisV2Writer::to_string_i32(int value) {
    return std::to_string(value);
}

std::string RedisV2Writer::trade_date_yyyymmdd(int64_t ts_ms) {
    if (ts_ms <= 0) {
        return "";
    }
    const std::time_t seconds = static_cast<std::time_t>(ts_ms / 1000);
    std::tm local_tm{};
#if defined(_WIN32)
    localtime_s(&local_tm, &seconds);
#else
    localtime_r(&seconds, &local_tm);
#endif
    char buffer[9] = {0};
    std::strftime(buffer, sizeof(buffer), "%Y%m%d", &local_tm);
    return std::string(buffer);
}

std::string RedisV2Writer::auction_tag_from_trigger(const SnapshotTriggerState& trigger) {
    if (trigger.emit_a25) {
        return "0925";
    }
    if (trigger.emit_a24) {
        return "0924";
    }
    if (trigger.emit_a20) {
        return "0920";
    }
    if (trigger.emit_latest_auction) {
        return "latest";
    }
    return "";
}

int RedisV2Writer::change_bp(const QuoteState& state) {
    if (state.px_milli <= 0 || state.pc_milli <= 0) {
        return 0;
    }
    return static_cast<int>(((static_cast<int64_t>(state.px_milli) - state.pc_milli) * 10000) / state.pc_milli);
}

std::string RedisV2Writer::change_pct_string(const QuoteState& state) {
    const int bp = change_bp(state);
    const int scaled = bp;
    const int whole = scaled / 100;
    int frac = scaled % 100;
    if (frac < 0) {
        frac = -frac;
    }
    std::ostringstream oss;
    oss << whole << ".";
    if (frac < 10) {
        oss << "0";
    }
    oss << frac;
    return oss.str();
}

std::string RedisV2Writer::build_a2_meta_json(const std::string& tag, int64_t logical_ts_ms, int row_count) const {
    std::ostringstream oss;
    oss << "{\"tag\":\"" << tag << "\",\"ts\":" << logical_ts_ms
        << ",\"n\":" << row_count << "}";
    return oss.str();
}

std::string RedisV2Writer::build_top_json(const std::vector<const QuoteState*>& rows) const {
    std::ostringstream oss;
    oss << "[";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const QuoteState& state = *rows[i];
        if (i > 0) {
            oss << ",";
        }
        oss << "{\"s\":\"" << state.symbol << "\","
            << "\"px\":" << state.px_milli << ","
            << "\"chg\":" << change_bp(state) << ","
            << "\"am\":" << state.auction.match_amt_yuan << ","
            << "\"br\":" << state.auction.rest_bid_amt_yuan << ","
            << "\"ar\":" << state.auction.rest_ask_amt_yuan << ","
            << "\"ls\":" << static_cast<int>(state.limit_state)
            << "}";
    }
    oss << "]";
    return oss.str();
}

std::string RedisV2Writer::build_legacy_summary_json(
    const std::string& tag,
    int64_t logical_ts_ms,
    const std::vector<const QuoteState*>& rows
) const {
    int high_open = 0;
    int low_open = 0;
    int flat_open = 0;
    int limit_up = 0;
    int limit_down = 0;
    int64_t total_match = 0;
    int64_t total_limit_up_bid = 0;
    for (const QuoteState* row : rows) {
        if (!row) {
            continue;
        }
        const int chg = change_bp(*row);
        if (chg > 0) {
            ++high_open;
        } else if (chg < 0) {
            ++low_open;
        } else {
            ++flat_open;
        }
        if (row->limit_state == LimitState::Up) {
            ++limit_up;
            total_limit_up_bid += row->auction.rest_bid_amt_yuan;
        } else if (row->limit_state == LimitState::Down) {
            ++limit_down;
        }
        total_match += row->auction.match_amt_yuan;
    }

    std::ostringstream oss;
    oss << "{\"ts\":" << logical_ts_ms
        << ",\"tag\":\"" << tag << "\""
        << ",\"total_stocks\":" << rows.size()
        << ",\"high_open_count\":" << high_open
        << ",\"low_open_count\":" << low_open
        << ",\"flat_open_count\":" << flat_open
        << ",\"limit_up_count\":" << limit_up
        << ",\"limit_down_count\":" << limit_down
        << ",\"total_auction_amount_yuan\":" << total_match
        << ",\"total_limit_up_bid_amount_yuan\":" << total_limit_up_bid
        << "}";
    return oss.str();
}

std::string RedisV2Writer::build_legacy_top_amount_json(const std::vector<const QuoteState*>& rows) const {
    std::ostringstream oss;
    oss << "[";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const QuoteState& state = *rows[i];
        if (i > 0) {
            oss << ",";
        }
        oss << "{\"symbol\":\"" << state.symbol << "\""
            << ",\"price\":" << (state.px_milli / 1000.0)
            << ",\"change_pct\":" << change_pct_string(state)
            << ",\"auction_amount_yuan\":" << state.auction.match_amt_yuan
            << ",\"bid_amount_yuan\":" << state.auction.rest_bid_amt_yuan
            << "}";
    }
    oss << "]";
    return oss.str();
}

std::string RedisV2Writer::build_anchor_archive_json(
    const std::string& tag,
    const std::vector<const QuoteState*>& rows
) const {
    std::ostringstream oss;
    oss << "{";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const QuoteState& state = *rows[i];
        if (i > 0) {
            oss << ",";
        }
        oss << "\"" << state.symbol << "\":{"
            << "\"change_pct\":" << change_pct_string(state)
            << ",\"amount\":" << state.auction.match_amt_yuan
            << ",\"bid_amount\":" << state.auction.rest_bid_amt_yuan
            << ",\"tag\":\"" << tag << "\""
            << ",\"source\":\"redis_0925\""
            << "}";
    }
    oss << "}";
    return oss.str();
}

}  // namespace t1_v2
