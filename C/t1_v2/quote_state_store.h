#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>

#include "quote_state.h"

namespace t1_v2 {

class QuoteStateStore {
public:
    explicit QuoteStateStore(std::size_t reserve_symbols = 6000);

    QuoteState& get_or_create(const char symbol[7]);
    void for_each_active(const std::function<void(QuoteState&)>& fn);
    void for_each_active(const std::function<void(const QuoteState&)>& fn) const;
    std::size_t size() const { return states_.size(); }

    void begin_batch();
    uint32_t last_batch_symbol_count() const { return last_batch_symbol_count_; }

private:
    std::unordered_map<std::string, QuoteState> states_;
    uint32_t last_batch_symbol_count_ = 0;
};

}  // namespace t1_v2
