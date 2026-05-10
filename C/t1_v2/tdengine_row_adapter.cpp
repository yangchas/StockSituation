#include "tdengine_row_adapter.h"

#include <cstdint>

namespace t1_v2 {

bool TdengineRowAdapter::enabled() {
#if defined(T1_V2_ENABLE_TDENGINE)
    return true;
#else
    return false;
#endif
}

#if defined(T1_V2_ENABLE_TDENGINE)

namespace {

using VarDataLenT = int16_t;
constexpr int kVarstrHeaderSize = sizeof(VarDataLenT);

std::string tdengine_var_string(void* value) {
    if (!value) {
        return "";
    }
    const auto* raw = static_cast<const char*>(value);
    const auto len = *reinterpret_cast<const VarDataLenT*>(raw - kVarstrHeaderSize);
    if (len <= 0) {
        return "";
    }
    return std::string(raw, static_cast<std::size_t>(len));
}

TdReplayCell cell_from_value(void* value, int type) {
    if (!value) {
        return TdReplayCell::null();
    }
    switch (type) {
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
            return TdReplayCell::string(tdengine_var_string(value));
        case TSDB_DATA_TYPE_TIMESTAMP:
            return TdReplayCell::i64(*static_cast<int64_t*>(value));
        case TSDB_DATA_TYPE_FLOAT:
            return TdReplayCell::f64(*static_cast<float*>(value));
        case TSDB_DATA_TYPE_DOUBLE:
            return TdReplayCell::f64(*static_cast<double*>(value));
        case TSDB_DATA_TYPE_INT:
            return TdReplayCell::i64(*static_cast<int32_t*>(value));
        case TSDB_DATA_TYPE_BIGINT:
            return TdReplayCell::i64(*static_cast<int64_t*>(value));
        default:
            return TdReplayCell::null();
    }
}

}  // namespace

std::vector<std::string> TdengineRowAdapter::field_names(TAOS_FIELD* fields, int field_count) {
    std::vector<std::string> names;
    if (!fields || field_count <= 0) {
        return names;
    }
    names.reserve(static_cast<std::size_t>(field_count));
    for (int i = 0; i < field_count; ++i) {
        names.emplace_back(fields[i].name ? fields[i].name : "");
    }
    return names;
}

TdengineRowAdapterResult TdengineRowAdapter::cells_from_row(
    TAOS_ROW row,
    TAOS_FIELD* fields,
    int field_count,
    std::vector<TdReplayCell>& out
) {
    out.clear();
    if (!row || !fields || field_count <= 0) {
        return {false, false, "invalid tdengine row input"};
    }
    out.reserve(static_cast<std::size_t>(field_count));
    for (int i = 0; i < field_count; ++i) {
        out.push_back(cell_from_value(row[i], fields[i].type));
    }
    return {true, false, ""};
}

#endif

}  // namespace t1_v2
