#pragma once

#include <cstdint>
#include <string>

#include "raw_tick.h"

namespace t1_v2 {

// SourceTickRecord mirrors the decoded DataRecord / old stock_data row fields.
// It deliberately avoids protobuf and TDengine headers so conversion can be
// tested without linking live IO libraries.
struct SourceTickRecord {
    int64_t tss = 0;
    double lp = 0.0;
    double o = 0.0;
    double h = 0.0;
    double l = 0.0;
    double lc = 0.0;
    double a = 0.0;
    int64_t v = 0;
    int64_t p = 0;

    double ap[5] = {0.0};
    double bp[5] = {0.0};
    int64_t av[5] = {0};
    int64_t bv[5] = {0};

    std::string symbol;
    std::string exchange;
    std::string market;

    int64_t inst_vol = 0;
    int64_t inst_amt = 0;
    int64_t large_net = 0;

    double limit_up = 0.0;
    double limit_down = 0.0;
    int16_t limit_band_bp = 0;
    bool no_price_limit = false;
    bool is_st = false;
};

class RawTickConverter {
public:
    static bool from_source_record(const SourceTickRecord& source, RawTick& out);

    static int price_to_milli(double price);
    static int64_t amount_to_yuan(double amount);
    static bool copy_symbol(const std::string& symbol, char (&out)[7]);
    static bool copy_market(const std::string& market, char (&out)[8]);
};

}  // namespace t1_v2
