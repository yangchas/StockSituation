#include "td_replay_row_converter.h"

#include <algorithm>
#include <cctype>
#include <utility>

namespace t1_v2 {

TdReplayCell TdReplayCell::null() {
    return {};
}

TdReplayCell TdReplayCell::string(std::string value) {
    TdReplayCell cell;
    cell.kind = TdReplayCellKind::String;
    cell.string_value = std::move(value);
    return cell;
}

TdReplayCell TdReplayCell::i64(int64_t value) {
    TdReplayCell cell;
    cell.kind = TdReplayCellKind::I64;
    cell.i64_value = value;
    cell.double_value = static_cast<double>(value);
    return cell;
}

TdReplayCell TdReplayCell::f64(double value) {
    TdReplayCell cell;
    cell.kind = TdReplayCellKind::Double;
    cell.double_value = value;
    cell.i64_value = static_cast<int64_t>(value);
    return cell;
}

TdReplayRowPlan TdReplayRowConverter::build_plan(const std::vector<std::string>& field_names) {
    TdReplayRowPlan plan;
    plan.targets.reserve(field_names.size());
    for (const std::string& name : field_names) {
        plan.targets.push_back(target_for_name(name));
    }
    return plan;
}

bool TdReplayRowConverter::convert(
    const TdReplayRowPlan& plan,
    const std::vector<TdReplayCell>& cells,
    SourceTickRecord& out
) {
    SourceTickRecord record;
    const std::size_t n = std::min(plan.targets.size(), cells.size());
    for (std::size_t i = 0; i < n; ++i) {
        const TdReplayCell& cell = cells[i];
        if (cell.kind == TdReplayCellKind::Null) {
            continue;
        }
        switch (plan.targets[i]) {
            case TdReplayTargetField::Ts: record.tss = as_i64(cell); break;
            case TdReplayTargetField::Symbol:
                record.symbol = normalize_symbol(cell.string_value);
                record.market = normalize_market(cell.string_value);
                break;
            case TdReplayTargetField::Lp: record.lp = as_double(cell); break;
            case TdReplayTargetField::Open: record.o = as_double(cell); break;
            case TdReplayTargetField::High: record.h = as_double(cell); break;
            case TdReplayTargetField::Low: record.l = as_double(cell); break;
            case TdReplayTargetField::PrevClose: record.lc = as_double(cell); break;
            case TdReplayTargetField::Amount: record.a = as_double(cell); break;
            case TdReplayTargetField::Volume: record.v = as_i64(cell); break;
            case TdReplayTargetField::AskPrice1: record.ap[0] = as_double(cell); break;
            case TdReplayTargetField::AskPrice2: record.ap[1] = as_double(cell); break;
            case TdReplayTargetField::AskPrice3: record.ap[2] = as_double(cell); break;
            case TdReplayTargetField::AskPrice4: record.ap[3] = as_double(cell); break;
            case TdReplayTargetField::AskPrice5: record.ap[4] = as_double(cell); break;
            case TdReplayTargetField::BidPrice1: record.bp[0] = as_double(cell); break;
            case TdReplayTargetField::BidPrice2: record.bp[1] = as_double(cell); break;
            case TdReplayTargetField::BidPrice3: record.bp[2] = as_double(cell); break;
            case TdReplayTargetField::BidPrice4: record.bp[3] = as_double(cell); break;
            case TdReplayTargetField::BidPrice5: record.bp[4] = as_double(cell); break;
            case TdReplayTargetField::AskVolume1: record.av[0] = as_i64(cell); break;
            case TdReplayTargetField::AskVolume2: record.av[1] = as_i64(cell); break;
            case TdReplayTargetField::AskVolume3: record.av[2] = as_i64(cell); break;
            case TdReplayTargetField::AskVolume4: record.av[3] = as_i64(cell); break;
            case TdReplayTargetField::AskVolume5: record.av[4] = as_i64(cell); break;
            case TdReplayTargetField::BidVolume1: record.bv[0] = as_i64(cell); break;
            case TdReplayTargetField::BidVolume2: record.bv[1] = as_i64(cell); break;
            case TdReplayTargetField::BidVolume3: record.bv[2] = as_i64(cell); break;
            case TdReplayTargetField::BidVolume4: record.bv[3] = as_i64(cell); break;
            case TdReplayTargetField::BidVolume5: record.bv[4] = as_i64(cell); break;
            case TdReplayTargetField::InstVolume: record.inst_vol = as_i64(cell); break;
            case TdReplayTargetField::InstAmount: record.inst_amt = as_i64(cell); break;
            case TdReplayTargetField::LargeNet: record.large_net = as_i64(cell); break;
            case TdReplayTargetField::LimitUp: record.limit_up = as_double(cell); break;
            case TdReplayTargetField::LimitDown: record.limit_down = as_double(cell); break;
            case TdReplayTargetField::LimitBandBp: record.limit_band_bp = static_cast<int16_t>(as_i64(cell)); break;
            case TdReplayTargetField::NoPriceLimit: record.no_price_limit = as_i64(cell) != 0; break;
            case TdReplayTargetField::IsSt: record.is_st = as_i64(cell) != 0; break;
            case TdReplayTargetField::Ignore: break;
        }
    }
    apply_exchange_defaults(record);
    out = std::move(record);
    return out.tss > 0 && !out.symbol.empty();
}

TdReplayTargetField TdReplayRowConverter::target_for_name(const std::string& name) {
    if (name == "ts") return TdReplayTargetField::Ts;
    if (name == "symbol") return TdReplayTargetField::Symbol;
    if (name == "lp") return TdReplayTargetField::Lp;
    if (name == "o") return TdReplayTargetField::Open;
    if (name == "h") return TdReplayTargetField::High;
    if (name == "l") return TdReplayTargetField::Low;
    if (name == "lc") return TdReplayTargetField::PrevClose;
    if (name == "a") return TdReplayTargetField::Amount;
    if (name == "v") return TdReplayTargetField::Volume;
    if (name == "ap1") return TdReplayTargetField::AskPrice1;
    if (name == "ap2") return TdReplayTargetField::AskPrice2;
    if (name == "ap3") return TdReplayTargetField::AskPrice3;
    if (name == "ap4") return TdReplayTargetField::AskPrice4;
    if (name == "ap5") return TdReplayTargetField::AskPrice5;
    if (name == "bp1") return TdReplayTargetField::BidPrice1;
    if (name == "bp2") return TdReplayTargetField::BidPrice2;
    if (name == "bp3") return TdReplayTargetField::BidPrice3;
    if (name == "bp4") return TdReplayTargetField::BidPrice4;
    if (name == "bp5") return TdReplayTargetField::BidPrice5;
    if (name == "av1") return TdReplayTargetField::AskVolume1;
    if (name == "av2") return TdReplayTargetField::AskVolume2;
    if (name == "av3") return TdReplayTargetField::AskVolume3;
    if (name == "av4") return TdReplayTargetField::AskVolume4;
    if (name == "av5") return TdReplayTargetField::AskVolume5;
    if (name == "bv1") return TdReplayTargetField::BidVolume1;
    if (name == "bv2") return TdReplayTargetField::BidVolume2;
    if (name == "bv3") return TdReplayTargetField::BidVolume3;
    if (name == "bv4") return TdReplayTargetField::BidVolume4;
    if (name == "bv5") return TdReplayTargetField::BidVolume5;
    if (name == "inst_vol") return TdReplayTargetField::InstVolume;
    if (name == "inst_amt") return TdReplayTargetField::InstAmount;
    if (name == "large_net") return TdReplayTargetField::LargeNet;
    if (name == "limit_up" || name == "limit_up_price") return TdReplayTargetField::LimitUp;
    if (name == "limit_down" || name == "limit_down_price") return TdReplayTargetField::LimitDown;
    if (name == "limit_band_bp" || name == "limit_ratio_bp") return TdReplayTargetField::LimitBandBp;
    if (name == "no_price_limit") return TdReplayTargetField::NoPriceLimit;
    if (name == "is_st" || name == "st_flag") return TdReplayTargetField::IsSt;
    return TdReplayTargetField::Ignore;
}

int64_t TdReplayRowConverter::as_i64(const TdReplayCell& cell) {
    if (cell.kind == TdReplayCellKind::I64) {
        return cell.i64_value;
    }
    if (cell.kind == TdReplayCellKind::Double) {
        return static_cast<int64_t>(cell.double_value);
    }
    return 0;
}

double TdReplayRowConverter::as_double(const TdReplayCell& cell) {
    if (cell.kind == TdReplayCellKind::Double) {
        return cell.double_value;
    }
    if (cell.kind == TdReplayCellKind::I64) {
        return static_cast<double>(cell.i64_value);
    }
    return 0.0;
}

std::string TdReplayRowConverter::normalize_symbol(const std::string& raw) {
    std::string text = raw;
    const std::size_t colon = text.find(':');
    if (colon != std::string::npos) {
        text = text.substr(colon + 1);
    }
    const std::size_t dot = text.find('.');
    if (dot != std::string::npos) {
        text = text.substr(0, dot);
    }
    if (text.size() <= 6) {
        return text;
    }
    return text.substr(text.size() - 6);
}

std::string TdReplayRowConverter::normalize_market(const std::string& raw) {
    const std::size_t colon = raw.find(':');
    if (colon != std::string::npos && colon > 0) {
        std::string market = raw.substr(0, colon);
        std::transform(market.begin(), market.end(), market.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return market;
    }
    const std::size_t dot = raw.find('.');
    if (dot != std::string::npos && dot + 1 < raw.size()) {
        std::string market = raw.substr(dot + 1);
        std::transform(market.begin(), market.end(), market.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return market;
    }
    return "";
}

void TdReplayRowConverter::apply_exchange_defaults(SourceTickRecord& record) {
    if (record.symbol.empty()) {
        return;
    }
    if (!record.market.empty()) {
        if (record.exchange.empty()) {
            if (record.market == "sz") {
                record.exchange = "SZ";
            } else if (record.market == "bj" || record.market == "bs" || record.market == "nq") {
                record.exchange = "BJ";
            } else {
                record.exchange = "SH";
            }
        }
        return;
    }
    if (!record.exchange.empty()) {
        return;
    }
    if (record.symbol[0] == '6') {
        record.exchange = "SH";
        record.market = record.symbol.rfind("68", 0) == 0 ? "kc" : "sh";
    } else if (record.symbol[0] == '0' || record.symbol[0] == '3') {
        record.exchange = "SZ";
        record.market = "sz";
    } else if (record.symbol[0] == '8' || record.symbol[0] == '4') {
        record.exchange = "BJ";
        record.market = "bj";
    } else {
        record.exchange = "Unknown";
        record.market = "Unknown";
    }
}

}  // namespace t1_v2
