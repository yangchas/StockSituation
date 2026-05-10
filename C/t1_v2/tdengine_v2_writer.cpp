#include "tdengine_v2_writer.h"

#include <ctime>
#include <sstream>

namespace t1_v2 {

TDengineV2Writer::TDengineV2Writer(const ConfigV2& config) : config_(config) {}

bool TDengineV2Writer::initialize() {
    initialized_ = true;
    return true;
}

void TDengineV2Writer::shutdown() {
    initialized_ = false;
}

bool TDengineV2Writer::should_write_stock_ticks(const TickBatch& batch) const {
    if (!initialized_ || batch.ticks.empty()) {
        return false;
    }
    if (batch.mode == RuntimeMode::Replay) {
        return config_.replay.write_tdengine;
    }
    return true;
}

std::string TDengineV2Writer::stock_tick_stable_ddl() const {
    return "CREATE STABLE IF NOT EXISTS stock_tick_v2 ("
           "ts TIMESTAMP,"
           "px_milli INT,pc_milli INT,o_milli INT,h_milli INT,l_milli INT,"
           "amt_yuan BIGINT,vol_units BIGINT,"
           "ap1_milli INT,ap2_milli INT,ap3_milli INT,ap4_milli INT,ap5_milli INT,"
           "bp1_milli INT,bp2_milli INT,bp3_milli INT,bp4_milli INT,bp5_milli INT,"
           "av1 BIGINT,av2 BIGINT,av3 BIGINT,av4 BIGINT,av5 BIGINT,"
           "bv1 BIGINT,bv2 BIGINT,bv3 BIGINT,bv4 BIGINT,bv5 BIGINT,"
           "inst_vol BIGINT,inst_amt_yuan BIGINT,large_net_yuan BIGINT"
           ") TAGS (symbol BINARY(20))";
}

std::string TDengineV2Writer::auction_snapshot_stable_ddl() const {
    return "CREATE STABLE IF NOT EXISTS auction_snapshot_v2 ("
           "ts TIMESTAMP,"
           "px_milli INT,chg_bp INT,"
           "match_amt_yuan BIGINT,rest_bid_amt_yuan BIGINT,rest_ask_amt_yuan BIGINT,"
           "limit_state TINYINT"
           ") TAGS (symbol BINARY(20),trade_date BINARY(8),auction_tag BINARY(4))";
}

std::string TDengineV2Writer::auction_summary_table_ddl() const {
    return "CREATE TABLE IF NOT EXISTS auction_summary_v2 ("
           "ts TIMESTAMP,"
           "trade_date BINARY(8),auction_tag BINARY(4),"
           "total_stocks INT,high_open_count INT,low_open_count INT,flat_open_count INT,"
           "limit_up_count INT,limit_down_count INT,"
           "total_match_amt_yuan BIGINT,total_rest_bid_amt_yuan BIGINT,total_rest_ask_amt_yuan BIGINT"
           ")";
}

std::string TDengineV2Writer::build_stock_tick_insert_sql(const TickBatch& batch) const {
    if (!should_write_stock_ticks(batch)) {
        return "";
    }
    std::ostringstream sql;
    sql << "INSERT INTO ";
    bool wrote = false;
    for (const RawTick& tick : batch.ticks) {
        if (tick.symbol[0] == '\0' || tick.ts_ms <= 0) {
            continue;
        }
        const std::string symbol(tick.symbol);
        sql << "t2_s_" << symbol << " USING stock_tick_v2 TAGS ('" << symbol << "') VALUES ("
            << tick.ts_ms << ","
            << tick.px_milli << "," << tick.pc_milli << "," << tick.o_milli << ","
            << tick.h_milli << "," << tick.l_milli << ","
            << tick.amt_yuan << "," << tick.vol_units << ","
            << tick.ap_milli[0] << "," << tick.ap_milli[1] << "," << tick.ap_milli[2] << ","
            << tick.ap_milli[3] << "," << tick.ap_milli[4] << ","
            << tick.bp_milli[0] << "," << tick.bp_milli[1] << "," << tick.bp_milli[2] << ","
            << tick.bp_milli[3] << "," << tick.bp_milli[4] << ","
            << tick.av[0] << "," << tick.av[1] << "," << tick.av[2] << "," << tick.av[3] << "," << tick.av[4] << ","
            << tick.bv[0] << "," << tick.bv[1] << "," << tick.bv[2] << "," << tick.bv[3] << "," << tick.bv[4] << ","
            << tick.inst_vol << "," << tick.inst_amt_yuan << "," << tick.large_net_yuan
            << ") ";
        wrote = true;
    }
    return wrote ? sql.str() : "";
}

std::string TDengineV2Writer::build_auction_summary_insert_sql(
    const QuoteStateStore& store,
    const SnapshotTriggerState& trigger,
    int64_t logical_ts_ms
) const {
    const std::string tag = auction_tag_from_trigger(trigger);
    if (tag.empty() || logical_ts_ms <= 0) {
        return "";
    }

    int total = 0;
    int high_open = 0;
    int low_open = 0;
    int flat_open = 0;
    int limit_up = 0;
    int limit_down = 0;
    int64_t total_match = 0;
    int64_t total_rest_bid = 0;
    int64_t total_rest_ask = 0;

    store.for_each_active([&](const QuoteState& state) {
        if (state.symbol[0] == '\0' || state.auction.ts_ms <= 0) {
            return;
        }
        ++total;
        const int chg = change_bp(state);
        if (chg > 0) {
            ++high_open;
        } else if (chg < 0) {
            ++low_open;
        } else {
            ++flat_open;
        }
        if (state.limit_state == LimitState::Up) {
            ++limit_up;
        } else if (state.limit_state == LimitState::Down) {
            ++limit_down;
        }
        total_match += state.auction.match_amt_yuan;
        total_rest_bid += state.auction.rest_bid_amt_yuan;
        total_rest_ask += state.auction.rest_ask_amt_yuan;
    });

    std::ostringstream sql;
    sql << "INSERT INTO auction_summary_v2 VALUES ("
        << logical_ts_ms << ",'"
        << trade_date_yyyymmdd(logical_ts_ms) << "','"
        << tag << "',"
        << total << "," << high_open << "," << low_open << "," << flat_open << ","
        << limit_up << "," << limit_down << ","
        << total_match << "," << total_rest_bid << "," << total_rest_ask
        << ")";
    return sql.str();
}

std::string TDengineV2Writer::build_auction_snapshot_insert_sql(
    const QuoteStateStore& store,
    const SnapshotTriggerState& trigger,
    int64_t logical_ts_ms
) const {
    const std::string tag = auction_tag_from_trigger(trigger);
    const std::string trade_date = trade_date_yyyymmdd(logical_ts_ms);
    if (tag.empty() || trade_date.empty() || logical_ts_ms <= 0) {
        return "";
    }

    std::ostringstream sql;
    sql << "INSERT INTO ";
    bool wrote = false;
    store.for_each_active([&](const QuoteState& state) {
        if (!is_valid_symbol(state.symbol) || state.auction.ts_ms <= 0) {
            return;
        }
        const std::string symbol(state.symbol);
        sql << "a2_" << trade_date << "_" << tag << "_" << symbol
            << " USING auction_snapshot_v2 TAGS ('" << symbol << "','"
            << trade_date << "','" << tag << "') VALUES ("
            << logical_ts_ms << ","
            << state.px_milli << ","
            << change_bp(state) << ","
            << state.auction.match_amt_yuan << ","
            << state.auction.rest_bid_amt_yuan << ","
            << state.auction.rest_ask_amt_yuan << ","
            << static_cast<int>(state.limit_state)
            << ") ";
        wrote = true;
    });
    return wrote ? sql.str() : "";
}

std::vector<std::string> TDengineV2Writer::build_schema_statements() const {
    return {
        stock_tick_stable_ddl(),
        auction_snapshot_stable_ddl(),
        auction_summary_table_ddl(),
    };
}

std::vector<std::string> TDengineV2Writer::build_batch_statements(
    const TickBatch& batch,
    const QuoteStateStore& store,
    const SnapshotTriggerState& trigger,
    int64_t logical_ts_ms
) const {
    std::vector<std::string> statements;
    if (std::string sql = build_stock_tick_insert_sql(batch); !sql.empty()) {
        statements.emplace_back(std::move(sql));
    }
    if (std::string sql = build_auction_snapshot_insert_sql(store, trigger, logical_ts_ms); !sql.empty()) {
        statements.emplace_back(std::move(sql));
    }
    if (std::string sql = build_auction_summary_insert_sql(store, trigger, logical_ts_ms); !sql.empty()) {
        statements.emplace_back(std::move(sql));
    }
    return statements;
}

std::string TDengineV2Writer::trade_date_yyyymmdd(int64_t ts_ms) {
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

std::string TDengineV2Writer::auction_tag_from_trigger(const SnapshotTriggerState& trigger) {
    if (trigger.emit_a25) {
        return "0925";
    }
    if (trigger.emit_a24) {
        return "0924";
    }
    if (trigger.emit_a20) {
        return "0920";
    }
    return "";
}

int TDengineV2Writer::change_bp(const QuoteState& state) {
    if (state.px_milli <= 0 || state.pc_milli <= 0) {
        return 0;
    }
    return static_cast<int>(((static_cast<int64_t>(state.px_milli) - state.pc_milli) * 10000) / state.pc_milli);
}

bool TDengineV2Writer::is_valid_symbol(const char* symbol) {
    if (!symbol) {
        return false;
    }
    int count = 0;
    for (const char* p = symbol; *p; ++p) {
        if (*p < '0' || *p > '9') {
            return false;
        }
        ++count;
    }
    return count == 6;
}

}  // namespace t1_v2
