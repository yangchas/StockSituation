#include "auction_calculator.h"

#include <algorithm>
#include <ctime>

namespace t1_v2 {

bool AuctionCalculator::apply_tick(QuoteState& state, const RawTick& tick, MarketPhase phase) const {
    if (phase != MarketPhase::Auction) {
        return false;
    }
    AuctionState& auction = state.auction;
    auction.ts_ms = tick.ts_ms;
    auction.limit_state = state.limit_state;
    bool changed = capture_anchor_prices(auction, tick);
    changed = update_match_and_rest(auction, tick, phase) || changed;
    return changed;
}

bool AuctionCalculator::capture_anchor_prices(AuctionState& auction, const RawTick& tick) const {
    bool changed = false;
    const int hms = hms_from_timestamp_ms(tick.ts_ms);
    if (hms >= 92000 && hms <= 92020) {
        changed = changed || auction.a20_px_milli != tick.px_milli;
        auction.a20_px_milli = tick.px_milli;
    }
    if (hms >= 92400 && hms <= 92420) {
        changed = changed || auction.a24_px_milli != tick.px_milli;
        auction.a24_px_milli = tick.px_milli;
    }
    if (hms >= 92500 && hms <= 92520) {
        changed = changed || auction.a25_px_milli != tick.px_milli;
        auction.a25_px_milli = tick.px_milli;
    }
    return changed;
}

bool AuctionCalculator::update_match_and_rest(AuctionState& auction, const RawTick& tick, MarketPhase phase) const {
    const int64_t match_amt = calc_match_amt_yuan(tick, phase);
    const int64_t rest_bid = calc_rest_bid_amt_yuan(tick);
    const int64_t rest_ask = calc_rest_ask_amt_yuan(tick);
    const bool changed = auction.match_amt_yuan != match_amt ||
                         auction.rest_bid_amt_yuan != rest_bid ||
                         auction.rest_ask_amt_yuan != rest_ask;
    auction.match_amt_yuan = match_amt;
    auction.rest_bid_amt_yuan = rest_bid;
    auction.rest_ask_amt_yuan = rest_ask;
    return changed;
}

int64_t AuctionCalculator::calc_match_amt_yuan(const RawTick& tick, MarketPhase phase) const {
    if (phase != MarketPhase::Auction || tick.px_milli <= 0) {
        return 0;
    }

    const int hms = hms_from_timestamp_ms(tick.ts_ms);
    if (hms >= 92500 && tick.vol_units > 0) {
        return (static_cast<int64_t>(tick.px_milli) * tick.vol_units) / 1000;
    }

    const int64_t bid1_amt = (static_cast<int64_t>(tick.bp_milli[0]) * tick.bv[0]) / 1000;
    const int64_t ask1_amt = (static_cast<int64_t>(tick.ap_milli[0]) * tick.av[0]) / 1000;
    return std::max<int64_t>(0, std::min(bid1_amt, ask1_amt));
}

int64_t AuctionCalculator::calc_rest_bid_amt_yuan(const RawTick& tick) const {
    const int64_t bid2_amt = (static_cast<int64_t>(tick.bp_milli[1]) * tick.bv[1]) / 1000;
    return std::max<int64_t>(0, bid2_amt);
}

int64_t AuctionCalculator::calc_rest_ask_amt_yuan(const RawTick& tick) const {
    const int64_t ask2_amt = (static_cast<int64_t>(tick.ap_milli[1]) * tick.av[1]) / 1000;
    return std::max<int64_t>(0, ask2_amt);
}

int AuctionCalculator::hms_from_timestamp_ms(int64_t ts_ms) const {
    if (ts_ms <= 0) {
        return 0;
    }
    const std::time_t seconds = static_cast<std::time_t>(ts_ms / 1000);
    std::tm local_tm{};
#if defined(_WIN32)
    localtime_s(&local_tm, &seconds);
#else
    localtime_r(&seconds, &local_tm);
#endif
    return local_tm.tm_hour * 10000 + local_tm.tm_min * 100 + local_tm.tm_sec;
}

}  // namespace t1_v2
