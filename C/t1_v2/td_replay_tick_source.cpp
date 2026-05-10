#include "td_replay_tick_source.h"

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include "source_tick_batch_builder.h"
#include "tdengine_row_adapter.h"

namespace t1_v2 {

TdReplayTickSource::TdReplayTickSource(const ConfigV2& config)
    : config_(config),
      scheduler_(config),
      query_builder_(config) {}

bool TdReplayTickSource::start() {
    last_error_.clear();
    seq_no_ = 0;
    if (!scheduler_.start()) {
        last_error_ = "invalid TDengine replay time range";
        started_ = false;
        return false;
    }
    if (!connect()) {
        started_ = false;
        return false;
    }
    started_ = true;
    return true;
}

void TdReplayTickSource::stop() {
    disconnect();
    started_ = false;
}

TickSourceResult TdReplayTickSource::next_batch() {
    if (!started_) {
        TickSourceResult result;
        result.status = TickSourceStatus::Error;
        result.error_msg = last_error_.empty() ? "TdReplayTickSource is not started" : last_error_.c_str();
        return result;
    }
    if (scheduler_.finished()) {
        TickSourceResult result;
        result.status = TickSourceStatus::EndOfStream;
        return result;
    }
    if (!pending_records_.empty()) {
        return emit_pending_batch();
    }
    return query_next_slice();
}

bool TdReplayTickSource::connect() {
#if defined(T1_V2_ENABLE_TDENGINE)
    conn_ = taos_connect(
        config_.tdengine.host.c_str(),
        config_.tdengine.user.c_str(),
        config_.tdengine.password.c_str(),
        config_.tdengine.database.c_str(),
        config_.tdengine.port
    );
    if (!conn_) {
        last_error_ = "TDengine replay connect failed";
        return false;
    }
    return true;
#else
    last_error_ = "TdReplayTickSource requires --with-tdengine";
    return false;
#endif
}

void TdReplayTickSource::disconnect() {
#if defined(T1_V2_ENABLE_TDENGINE)
    if (conn_) {
        taos_close(conn_);
        conn_ = nullptr;
    }
#endif
}

TickSourceResult TdReplayTickSource::query_next_slice() {
#if defined(T1_V2_ENABLE_TDENGINE)
    const ReplaySlice slice = scheduler_.next();
    if (!slice.valid) {
        TickSourceResult result;
        result.status = TickSourceStatus::EndOfStream;
        return result;
    }

    const std::string sql = query_builder_.build_slice_query(slice.start_ms);
    TAOS_RES* res = taos_query(conn_, sql.c_str());
    if (!res) {
        TickSourceResult result;
        last_error_ = "TDengine replay query failed";
        result.status = TickSourceStatus::Error;
        result.error_msg = last_error_.c_str();
        return result;
    }
    const int code = taos_errno(res);
    if (code != 0) {
        TickSourceResult result;
        last_error_ = taos_errstr(res);
        taos_free_result(res);
        result.status = TickSourceStatus::Error;
        result.error_msg = last_error_.c_str();
        return result;
    }

    const int field_count = taos_field_count(res);
    TAOS_FIELD* fields = taos_fetch_fields(res);
    const std::vector<std::string> field_names = TdengineRowAdapter::field_names(fields, field_count);
    const TdReplayRowPlan plan = TdReplayRowConverter::build_plan(field_names);
    std::vector<TdReplayCell> cells;
    std::vector<SourceTickRecord> source_records;
    source_records.reserve(static_cast<std::size_t>(config_.replay.batch_size > 0 ? config_.replay.batch_size : 5000));

    TAOS_ROW row = nullptr;
    uint32_t raw_row_count = 0;
    while ((row = taos_fetch_row(res)) != nullptr) {
        ++raw_row_count;
        TdengineRowAdapterResult adapter_result = TdengineRowAdapter::cells_from_row(row, fields, field_count, cells);
        if (!adapter_result.ok) {
            continue;
        }
        SourceTickRecord record;
        if (TdReplayRowConverter::convert(plan, cells, record)) {
            source_records.push_back(std::move(record));
        }
    }
    taos_free_result(res);

    if (source_records.empty()) {
        TickSourceResult result;
        result.status = TickSourceStatus::Empty;
        return result;
    }
    pending_records_ = std::move(source_records);
    pending_logical_ts_ms_ = slice.start_ms;
    pending_raw_input_count_ = raw_row_count;
    pending_total_accepted_count_ = static_cast<uint32_t>(pending_records_.size());
    return emit_pending_batch();
#else
    TickSourceResult result;
    result.status = TickSourceStatus::Error;
    result.error_msg = "TdReplayTickSource requires --with-tdengine";
    return result;
#endif
}

TickSourceResult TdReplayTickSource::emit_pending_batch() {
    TickSourceResult result;
    if (pending_records_.empty()) {
        result.status = TickSourceStatus::Empty;
        return result;
    }
    const std::size_t max_batch = static_cast<std::size_t>(
        config_.replay.batch_size > 0 ? config_.replay.batch_size : 5000
    );
    const std::size_t take = std::min(max_batch, pending_records_.size());
    std::vector<SourceTickRecord> batch_records(
        std::make_move_iterator(pending_records_.begin()),
        std::make_move_iterator(pending_records_.begin() + static_cast<std::ptrdiff_t>(take))
    );
    pending_records_.erase(pending_records_.begin(), pending_records_.begin() + static_cast<std::ptrdiff_t>(take));

    result.status = TickSourceStatus::Ok;
    result.source_stats = SourceTickBatchBuilder::build(
        batch_records,
        RuntimeMode::Replay,
        pending_logical_ts_ms_,
        ++seq_no_,
        result.batch
    );
    result.source_stats.input_count = static_cast<uint32_t>(take);
    if (pending_raw_input_count_ > 0 && pending_records_.empty()) {
        const uint32_t rejected = pending_raw_input_count_ > pending_total_accepted_count_
            ? pending_raw_input_count_ - pending_total_accepted_count_
            : 0;
        result.source_stats.input_count = static_cast<uint32_t>(take) + rejected;
        result.source_stats.rejected_count = rejected;
        pending_raw_input_count_ = 0;
        pending_total_accepted_count_ = 0;
    }
    return result;
}

}  // namespace t1_v2
