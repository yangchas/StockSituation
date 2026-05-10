#include "quote_state_store.h"

#include <cstring>

namespace t1_v2 {

QuoteStateStore::QuoteStateStore(std::size_t reserve_symbols) {
    states_.reserve(reserve_symbols);
}

QuoteState& QuoteStateStore::get_or_create(const char symbol[7]) {
    const std::string key(symbol ? symbol : "");
    auto [it, inserted] = states_.try_emplace(key);
    if (inserted) {
        std::strncpy(it->second.symbol, key.c_str(), sizeof(it->second.symbol) - 1);
    }
    if (inserted) {
        ++last_batch_symbol_count_;
    }
    return it->second;
}

void QuoteStateStore::for_each_active(const std::function<void(QuoteState&)>& fn) {
    for (auto& item : states_) {
        fn(item.second);
    }
}

void QuoteStateStore::for_each_active(const std::function<void(const QuoteState&)>& fn) const {
    for (const auto& item : states_) {
        fn(item.second);
    }
}

void QuoteStateStore::begin_batch() {
    last_batch_symbol_count_ = 0;
}

}  // namespace t1_v2
