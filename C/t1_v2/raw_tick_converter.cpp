#include "raw_tick_converter.h"

#include <cmath>
#include <cstring>

namespace t1_v2 {

bool RawTickConverter::from_source_record(const SourceTickRecord& source, RawTick& out) {
    RawTick tick;
    if (source.tss <= 0 || !copy_symbol(source.symbol, tick.symbol)) {
        return false;
    }

    tick.ts_ms = source.tss;
    tick.px_milli = price_to_milli(source.lp);
    tick.pc_milli = price_to_milli(source.lc);
    tick.o_milli = price_to_milli(source.o);
    tick.h_milli = price_to_milli(source.h);
    tick.l_milli = price_to_milli(source.l);
    tick.amt_yuan = amount_to_yuan(source.a);
    tick.vol_units = source.v;

    for (int i = 0; i < 5; ++i) {
        tick.ap_milli[i] = price_to_milli(source.ap[i]);
        tick.bp_milli[i] = price_to_milli(source.bp[i]);
        tick.av[i] = source.av[i];
        tick.bv[i] = source.bv[i];
    }

    tick.inst_vol = source.inst_vol;
    tick.inst_amt_yuan = source.inst_amt;
    tick.large_net_yuan = source.large_net;

    out = tick;
    return true;
}

int RawTickConverter::price_to_milli(double price) {
    if (!std::isfinite(price) || price <= 0.0) {
        return 0;
    }
    return static_cast<int>(std::llround(price * 1000.0));
}

int64_t RawTickConverter::amount_to_yuan(double amount) {
    if (!std::isfinite(amount) || amount <= 0.0) {
        return 0;
    }
    return static_cast<int64_t>(std::llround(amount));
}

bool RawTickConverter::copy_symbol(const std::string& symbol, char (&out)[7]) {
    if (symbol.size() != 6) {
        return false;
    }
    for (char ch : symbol) {
        if (ch < '0' || ch > '9') {
            return false;
        }
    }
    std::memset(out, 0, 7);
    std::memcpy(out, symbol.data(), 6);
    return true;
}

}  // namespace t1_v2
