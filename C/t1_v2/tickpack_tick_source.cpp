#include "tickpack_tick_source.h"

#include <cctype>
#include <fstream>
#include <sstream>

#include "source_tick_batch_builder.h"

namespace t1_v2 {
namespace {

const char* kHeader =
    "source_ordinal\ttss\tlp\to\th\tl\tlc\ta\tv\tp\t"
    "ap1\tap2\tap3\tap4\tap5\tbp1\tbp2\tbp3\tbp4\tbp5\t"
    "av1\tav2\tav3\tav4\tav5\tbv1\tbv2\tbv3\tbv4\tbv5\t"
    "symbol\texchange\tmarket\tinst_vol\tinst_amt\tlarge_net\t"
    "limit_up\tlimit_down\tlimit_band_bp\tno_price_limit\tis_st";

constexpr std::size_t kColumnCount = 41;

}  // namespace

TickPackTickSource::TickPackTickSource(const ConfigV2& config) : config_(config) {}

bool TickPackTickSource::start() {
    input_.close();
    has_pending_ = false;
    has_last_record_ = false;
    eof_ = false;
    line_no_ = 0;
    seq_no_ = 0;
    last_error_.clear();
    if (config_.replay.tickpack_path.empty()) {
        last_error_ = "replay tickpack path is empty";
        return false;
    }
    if (!load_file()) {
        return false;
    }
    started_ = true;
    return true;
}

void TickPackTickSource::stop() {
    started_ = false;
    input_.close();
    has_pending_ = false;
    has_last_record_ = false;
    eof_ = false;
    line_no_ = 0;
}

TickSourceResult TickPackTickSource::next_batch() {
    TickSourceResult result;
    if (!started_) {
        result.status = TickSourceStatus::Error;
        result.error_msg = last_error_.empty() ? "TickPackTickSource is not started" : last_error_.c_str();
        return result;
    }
    if (!has_pending_) {
        StoredRecord first;
        if (!read_next_record(first)) {
            if (!last_error_.empty()) {
                result.status = TickSourceStatus::Error;
                result.error_msg = last_error_.c_str();
            } else {
                result.status = TickSourceStatus::EndOfStream;
            }
            return result;
        }
        pending_ = std::move(first);
        has_pending_ = true;
    }

    const int64_t interval = config_.replay.tick_interval_ms > 0
        ? config_.replay.tick_interval_ms
        : 3000;
    const int64_t first_ts = pending_.record.tss;
    const int64_t logical_ts = (first_ts / interval) * interval;
    const int64_t slice_end = logical_ts + interval;
    std::vector<SourceTickRecord> source_records;
    while (has_pending_) {
        if (pending_.record.tss >= slice_end) {
            break;
        }
        source_records.push_back(std::move(pending_.record));
        has_pending_ = false;
        StoredRecord next;
        if (read_next_record(next)) {
            pending_ = std::move(next);
            has_pending_ = true;
        } else if (!last_error_.empty()) {
            result.status = TickSourceStatus::Error;
            result.error_msg = last_error_.c_str();
            return result;
        }
    }
    result.status = TickSourceStatus::Ok;
    result.source_stats = SourceTickBatchBuilder::build(
        source_records,
        RuntimeMode::Replay,
        logical_ts,
        ++seq_no_,
        result.batch
    );
    return result;
}

bool TickPackTickSource::load_file() {
    input_.open(config_.replay.tickpack_path, std::ios::binary);
    if (!input_) {
        last_error_ = "cannot open replay tickpack: " + config_.replay.tickpack_path;
        return false;
    }

    std::string line;
    if (!std::getline(input_, line)) {
        last_error_ = "replay tickpack is empty";
        return false;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line != kHeader) {
        last_error_ = "unsupported or invalid TickPackV1 header";
        return false;
    }

    line_no_ = 1;
    StoredRecord first;
    if (!read_next_record(first)) {
        if (last_error_.empty()) {
            last_error_ = "replay tickpack has no records";
        }
        return false;
    }
    pending_ = std::move(first);
    has_pending_ = true;
    return true;
}

bool TickPackTickSource::read_next_record(StoredRecord& out) {
    std::string line;
    while (std::getline(input_, line)) {
        ++line_no_;
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        const std::vector<std::string> columns = split_tsv(line);
        StoredRecord stored;
        if (!parse_line(columns, stored)) {
            last_error_ = "invalid TickPackV1 row at line " + std::to_string(line_no_);
            return false;
        }
        if (has_last_record_) {
            const bool ordered =
                stored.record.tss > last_record_.record.tss ||
                (stored.record.tss == last_record_.record.tss &&
                 (stored.record.symbol > last_record_.record.symbol ||
                  (stored.record.symbol == last_record_.record.symbol &&
                   stored.source_ordinal >= last_record_.source_ordinal)));
            if (!ordered) {
                last_error_ = "TickPackV1 rows are not sorted at line " + std::to_string(line_no_);
                return false;
            }
        }
        last_record_ = stored;
        has_last_record_ = true;
        out = std::move(stored);
        return true;
    }
    eof_ = true;
    return false;
}

bool TickPackTickSource::parse_line(const std::vector<std::string>& columns, StoredRecord& out) {
    if (columns.size() != kColumnCount) {
        return false;
    }
    SourceTickRecord record;
    int64_t ordinal = 0;
    if (!parse_i64(columns[0], ordinal) || ordinal < 0) return false;
    out.source_ordinal = static_cast<uint64_t>(ordinal);
    if (!parse_i64(columns[1], record.tss)) return false;
    if (!parse_double(columns[2], record.lp)) return false;
    if (!parse_double(columns[3], record.o)) return false;
    if (!parse_double(columns[4], record.h)) return false;
    if (!parse_double(columns[5], record.l)) return false;
    if (!parse_double(columns[6], record.lc)) return false;
    if (!parse_double(columns[7], record.a)) return false;
    if (!parse_i64(columns[8], record.v)) return false;
    if (!parse_i64(columns[9], record.p)) return false;
    for (int i = 0; i < 5; ++i) if (!parse_double(columns[10 + i], record.ap[i])) return false;
    for (int i = 0; i < 5; ++i) if (!parse_double(columns[15 + i], record.bp[i])) return false;
    for (int i = 0; i < 5; ++i) if (!parse_i64(columns[20 + i], record.av[i])) return false;
    for (int i = 0; i < 5; ++i) if (!parse_i64(columns[25 + i], record.bv[i])) return false;
    record.symbol = columns[30];
    record.exchange = columns[31];
    record.market = columns[32];
    if (!parse_i64(columns[33], record.inst_vol)) return false;
    if (!parse_i64(columns[34], record.inst_amt)) return false;
    if (!parse_i64(columns[35], record.large_net)) return false;
    if (!parse_double(columns[36], record.limit_up)) return false;
    if (!parse_double(columns[37], record.limit_down)) return false;
    if (!parse_i16(columns[38], record.limit_band_bp)) return false;
    if (!parse_bool(columns[39], record.no_price_limit)) return false;
    if (!parse_bool(columns[40], record.is_st)) return false;
    out.record = std::move(record);
    return true;
}

std::vector<std::string> TickPackTickSource::split_tsv(const std::string& line) {
    std::vector<std::string> result;
    std::string current;
    std::stringstream stream(line);
    while (std::getline(stream, current, '\t')) {
        result.push_back(current);
    }
    if (!line.empty() && line.back() == '\t') {
        result.emplace_back();
    }
    return result;
}

bool TickPackTickSource::parse_i64(const std::string& text, int64_t& out) {
    try {
        std::size_t used = 0;
        const long long value = std::stoll(text, &used);
        if (used != text.size()) return false;
        out = static_cast<int64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool TickPackTickSource::parse_i16(const std::string& text, int16_t& out) {
    int64_t value = 0;
    if (!parse_i64(text, value) || value < -32768 || value > 32767) return false;
    out = static_cast<int16_t>(value);
    return true;
}

bool TickPackTickSource::parse_double(const std::string& text, double& out) {
    try {
        std::size_t used = 0;
        out = std::stod(text, &used);
        return used == text.size();
    } catch (...) {
        return false;
    }
}

bool TickPackTickSource::parse_bool(const std::string& text, bool& out) {
    if (text == "1" || text == "true") {
        out = true;
        return true;
    }
    if (text == "0" || text == "false") {
        out = false;
        return true;
    }
    return false;
}

}  // namespace t1_v2
