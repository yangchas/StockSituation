#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "redis_command_executor.h"

namespace t1_v2 {

// A concrete replay-only executor. It consumes the existing RedisCommand
// representation and writes Q2Frame JSONL without opening Redis.
class Q2FrameCommandExecutor final : public IRedisCommandExecutor {
public:
    explicit Q2FrameCommandExecutor(const ConfigV2& config);
    ~Q2FrameCommandExecutor() override;

    RedisExecutionResult preflight() override;
    RedisExecutionResult execute(const std::vector<RedisCommand>& commands) override;
    void reset_connection() override;

private:
    static std::string json_escape(const std::string& value);
    static bool is_q2_key(const RedisCommand& command, const std::string& prefix);
    static bool is_auction_fact_command(const RedisCommand& command, const ConfigV2& config);
    static std::string symbol_from_key(const std::string& key, const std::string& prefix);
    static bool is_string_field(const std::string& field);
    static bool parse_i64_field(const RedisFieldList& fields, const std::string& name, int64_t& out);
    static bool parse_i32_field(const RedisFieldList& fields, const std::string& name, int& out);
    std::string build_frame(const std::vector<const RedisCommand*>& updates, uint32_t seq_no) const;
    static std::string build_auction_command_record(const RedisCommand& command);

    ConfigV2 config_;
    std::ofstream output_;
    std::ofstream auction_output_;
    uint32_t seq_no_ = 0;
};

}  // namespace t1_v2
