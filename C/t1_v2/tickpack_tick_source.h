#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "config_v2.h"
#include "raw_tick_converter.h"
#include "tick_source.h"

namespace t1_v2 {

class TickPackTickSource final : public ITickSource {
public:
    explicit TickPackTickSource(const ConfigV2& config);
    ~TickPackTickSource() override = default;

    bool start() override;
    void stop() override;
    TickSourceResult next_batch() override;
    RuntimeMode mode() const override { return RuntimeMode::Replay; }
    std::string error_message() const override { return last_error_; }

private:
    struct StoredRecord {
        SourceTickRecord record;
        uint64_t source_ordinal = 0;
    };

    bool load_file();
    bool read_next_record(StoredRecord& out);
    bool parse_line(const std::vector<std::string>& columns, StoredRecord& out);
    static std::vector<std::string> split_tsv(const std::string& line);
    static bool parse_i64(const std::string& text, int64_t& out);
    static bool parse_i16(const std::string& text, int16_t& out);
    static bool parse_double(const std::string& text, double& out);
    static bool parse_bool(const std::string& text, bool& out);

    ConfigV2 config_;
    std::ifstream input_;
    StoredRecord pending_;
    StoredRecord last_record_;
    bool has_pending_ = false;
    bool has_last_record_ = false;
    bool eof_ = false;
    std::size_t line_no_ = 0;
    uint32_t seq_no_ = 0;
    bool started_ = false;
    std::string last_error_;
};

}  // namespace t1_v2
