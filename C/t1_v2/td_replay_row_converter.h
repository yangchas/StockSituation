#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "raw_tick_converter.h"

namespace t1_v2 {

enum class TdReplayCellKind : uint8_t {
    Null = 0,
    String = 1,
    I64 = 2,
    Double = 3,
};

struct TdReplayCell {
    TdReplayCellKind kind = TdReplayCellKind::Null;
    std::string string_value;
    int64_t i64_value = 0;
    double double_value = 0.0;

    static TdReplayCell null();
    static TdReplayCell string(std::string value);
    static TdReplayCell i64(int64_t value);
    static TdReplayCell f64(double value);
};

enum class TdReplayTargetField : uint8_t {
    Ignore = 0,
    Ts,
    Symbol,
    Lp,
    Open,
    High,
    Low,
    PrevClose,
    Amount,
    Volume,
    AskPrice1,
    AskPrice2,
    AskPrice3,
    AskPrice4,
    AskPrice5,
    BidPrice1,
    BidPrice2,
    BidPrice3,
    BidPrice4,
    BidPrice5,
    AskVolume1,
    AskVolume2,
    AskVolume3,
    AskVolume4,
    AskVolume5,
    BidVolume1,
    BidVolume2,
    BidVolume3,
    BidVolume4,
    BidVolume5,
    InstVolume,
    InstAmount,
    LargeNet,
};

struct TdReplayRowPlan {
    std::vector<TdReplayTargetField> targets;
};

class TdReplayRowConverter {
public:
    static TdReplayRowPlan build_plan(const std::vector<std::string>& field_names);
    static bool convert(const TdReplayRowPlan& plan, const std::vector<TdReplayCell>& cells, SourceTickRecord& out);

private:
    static TdReplayTargetField target_for_name(const std::string& name);
    static int64_t as_i64(const TdReplayCell& cell);
    static double as_double(const TdReplayCell& cell);
    static std::string normalize_symbol(const std::string& raw);
    static void apply_exchange_defaults(SourceTickRecord& record);
};

}  // namespace t1_v2
