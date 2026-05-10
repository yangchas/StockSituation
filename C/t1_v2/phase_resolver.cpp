#include "phase_resolver.h"

#include <ctime>

namespace t1_v2 {

MarketPhase PhaseResolver::resolve(int64_t logical_ts_ms) const {
    if (is_auction(logical_ts_ms)) {
        return MarketPhase::Auction;
    }
    if (is_intraday(logical_ts_ms)) {
        return MarketPhase::Intraday;
    }
    const int hms = hms_from_timestamp_ms(logical_ts_ms);
    if (hms > 150000) {
        return MarketPhase::Postmarket;
    }
    return MarketPhase::Premarket;
}

bool PhaseResolver::is_auction(int64_t logical_ts_ms) const {
    const int hms = hms_from_timestamp_ms(logical_ts_ms);
    return hms >= 91500 && hms <= 92600;
}

bool PhaseResolver::is_intraday(int64_t logical_ts_ms) const {
    const int hms = hms_from_timestamp_ms(logical_ts_ms);
    return (hms >= 93000 && hms <= 113000) || (hms >= 130000 && hms <= 150000);
}

int PhaseResolver::hms_from_timestamp_ms(int64_t ts_ms) {
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
