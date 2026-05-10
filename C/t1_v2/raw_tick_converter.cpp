#include "raw_tick_converter.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>

namespace t1_v2 {

namespace {

std::string effective_market(const SourceTickRecord& source) {
    if (!source.market.empty()) {
        return source.market;
    }
    std::string exchange = source.exchange;
    std::transform(exchange.begin(), exchange.end(), exchange.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (exchange == "sz") {
        return "sz";
    }
    if (source.symbol.rfind("68", 0) == 0) {
        return "kc";
    }
    if (exchange == "sh") {
        return "sh";
    }
    return "";
}

bool is_equity_market_symbol(const char* market, const char* symbol) {
    if (!symbol || symbol[0] == '\0') {
        return false;
    }
    const std::string market_text(market ? market : "");
    if (market_text == "sz") {
        return (symbol[0] == '0' || symbol[0] == '3') &&
               !(symbol[0] == '3' && symbol[1] == '9');
    }
    if (market_text == "sh" || market_text == "kc") {
        return (symbol[0] == '6');
    }
    if (market_text == "bj" || market_text == "bs" || market_text == "nq") {
        return symbol[0] == '8' || symbol[0] == '4';
    }
    return market_text.empty();
}

bool looks_like_index_price_for_sz_equity(const char* market, const char* symbol, double price) {
    if (!market || !symbol) {
        return false;
    }
    const std::string market_text(market);
    if (market_text != "sz") {
        return false;
    }
    if (!(symbol[0] == '0' || symbol[0] == '3')) {
        return false;
    }
    return price >= 1000.0;
}

}  // namespace

bool RawTickConverter::from_source_record(const SourceTickRecord& source, RawTick& out) {
    RawTick tick;
    if (source.tss <= 0 || !copy_symbol(source.symbol, tick.symbol)) {
        return false;
    }
    copy_market(effective_market(source), tick.market);
    if (!is_equity_market_symbol(tick.market, tick.symbol)) {
        return false;
    }
    if (looks_like_index_price_for_sz_equity(tick.market, tick.symbol, source.lp)) {
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
    tick.limit_up_milli = price_to_milli(source.limit_up);
    tick.limit_down_milli = price_to_milli(source.limit_down);
    tick.limit_band_bp = source.limit_band_bp;
    tick.no_price_limit = source.no_price_limit || source.limit_band_bp < 0;
    tick.is_st = source.is_st;

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

bool RawTickConverter::copy_market(const std::string& market, char (&out)[8]) {
    std::memset(out, 0, 8);
    if (market.empty()) {
        return false;
    }
    const std::size_t n = std::min<std::size_t>(market.size(), 7);
    for (std::size_t i = 0; i < n; ++i) {
        out[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(market[i])));
    }
    return true;
}

}  // namespace t1_v2
