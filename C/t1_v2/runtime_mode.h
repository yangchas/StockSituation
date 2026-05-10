#pragma once

#include <cstdint>

namespace t1_v2 {

enum class RuntimeMode : uint8_t {
    Live = 0,
    Replay = 1,
};

enum class MarketPhase : uint8_t {
    Premarket = 0,
    Auction = 1,
    Intraday = 2,
    Postmarket = 3,
};

enum class LimitState : int8_t {
    Down = -1,
    Normal = 0,
    Up = 1,
};

}  // namespace t1_v2
