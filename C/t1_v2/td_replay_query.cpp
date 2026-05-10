#include "td_replay_query.h"

#include <sstream>

namespace t1_v2 {
namespace {

bool is_v2_tick_table(const std::string& table) {
    return table == "stock_tick_v2";
}

}  // namespace

TdReplayQueryBuilder::TdReplayQueryBuilder(const ConfigV2& config) : config_(config) {}

int64_t TdReplayQueryBuilder::align_to_tick_interval(int64_t ts_ms) const {
    const int64_t interval = config_.replay.tick_interval_ms > 0 ? config_.replay.tick_interval_ms : 3000;
    if (ts_ms <= 0) {
        return 0;
    }
    return (ts_ms / interval) * interval;
}

std::string TdReplayQueryBuilder::build_slice_query(int64_t slice_start_ms) const {
    const std::string table = is_safe_table_name(config_.tdengine.replay_table)
        ? config_.tdengine.replay_table
        : "stock_data";
    const int64_t interval = config_.replay.tick_interval_ms > 0 ? config_.replay.tick_interval_ms : 3000;
    const int64_t slice_end_ms = slice_start_ms + interval;

    std::ostringstream sql;
    if (is_v2_tick_table(table)) {
        sql << "SELECT ts, symbol, "
            << "px_milli/1000.0 AS lp, o_milli/1000.0 AS o, h_milli/1000.0 AS h, "
            << "l_milli/1000.0 AS l, pc_milli/1000.0 AS lc, amt_yuan AS a, vol_units AS v, "
            << "ap1_milli/1000.0 AS ap1, ap2_milli/1000.0 AS ap2, ap3_milli/1000.0 AS ap3, "
            << "ap4_milli/1000.0 AS ap4, ap5_milli/1000.0 AS ap5, "
            << "bp1_milli/1000.0 AS bp1, bp2_milli/1000.0 AS bp2, bp3_milli/1000.0 AS bp3, "
            << "bp4_milli/1000.0 AS bp4, bp5_milli/1000.0 AS bp5, "
            << "av1, av2, av3, av4, av5, "
            << "bv1, bv2, bv3, bv4, bv5, "
            << "inst_vol, inst_amt_yuan AS inst_amt, large_net_yuan AS large_net "
            << "FROM " << table << " ";
    } else {
        sql << "SELECT ts, symbol, lp, o, h, l, lc, a, v, "
            << "ap1, ap2, ap3, ap4, ap5, "
            << "bp1, bp2, bp3, bp4, bp5, "
            << "av1, av2, av3, av4, av5, "
            << "bv1, bv2, bv3, bv4, bv5, "
            << "inst_vol, inst_amt, large_net "
            << "FROM " << table << " ";
    }
    sql << "WHERE ts >= " << slice_start_ms << " "
        << "AND ts < " << slice_end_ms << " "
        << "ORDER BY ts ASC, symbol ASC";
    return sql.str();
}

bool TdReplayQueryBuilder::is_safe_table_name(const std::string& table) {
    if (table.empty()) {
        return false;
    }
    for (char ch : table) {
        const bool ok = (ch >= 'a' && ch <= 'z') ||
                        (ch >= 'A' && ch <= 'Z') ||
                        (ch >= '0' && ch <= '9') ||
                        ch == '_';
        if (!ok) {
            return false;
        }
    }
    return true;
}

}  // namespace t1_v2
