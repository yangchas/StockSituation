#include "q2frame_command_executor.h"

#include <algorithm>
#include <sstream>

namespace t1_v2 {

Q2FrameCommandExecutor::Q2FrameCommandExecutor(const ConfigV2& config) : config_(config) {}

Q2FrameCommandExecutor::~Q2FrameCommandExecutor() {
    reset_connection();
}

RedisExecutionResult Q2FrameCommandExecutor::preflight() {
    RedisExecutionResult result;
    if (config_.replay.q2frame_path.empty()) {
        result.ok = false;
        result.error = "Q2Frame output path is empty";
        return result;
    }
    reset_connection();
    output_.open(config_.replay.q2frame_path, std::ios::out | std::ios::trunc);
    if (!output_) {
        result.ok = false;
        result.error = "cannot open Q2Frame output: " + config_.replay.q2frame_path;
        return result;
    }
    if (!config_.replay.auction_command_path.empty()) {
        auction_output_.open(config_.replay.auction_command_path, std::ios::out | std::ios::trunc);
        if (!auction_output_) {
            output_.close();
            result.ok = false;
            result.error = "cannot open replay auction command output: " + config_.replay.auction_command_path;
            return result;
        }
    }
    seq_no_ = 0;
    return result;
}

RedisExecutionResult Q2FrameCommandExecutor::execute(const std::vector<RedisCommand>& commands) {
    RedisExecutionResult result;
    result.command_count = static_cast<int>(commands.size());
    if (commands.empty()) {
        return result;
    }
    if (!output_.is_open()) {
        result.ok = false;
        result.error = "Q2Frame output is not open";
        return result;
    }

    std::vector<const RedisCommand*> updates;
    for (const RedisCommand& command : commands) {
        if (auction_output_.is_open() && is_auction_fact_command(command, config_)) {
            auction_output_ << build_auction_command_record(command) << '\n';
        }
        if (is_q2_key(command, config_.redis.q2_prefix)) {
            updates.push_back(&command);
        }
    }
    if (auction_output_.is_open()) {
        auction_output_.flush();
        if (!auction_output_) {
            result.ok = false;
            result.error = "failed to write replay auction command output";
            return result;
        }
    }
    if (updates.empty()) {
        return result;
    }

    // QuoteStateStore currently iterates an unordered_map.  Keep the replay
    // envelope cross-platform deterministic without changing the production
    // store or Redis command path.
    std::stable_sort(updates.begin(), updates.end(), [](const RedisCommand* lhs, const RedisCommand* rhs) {
        return lhs->key < rhs->key;
    });

    output_ << build_frame(updates, ++seq_no_) << '\n';
    output_.flush();
    if (!output_) {
        result.ok = false;
        result.error = "failed to write Q2Frame";
    }
    return result;
}

void Q2FrameCommandExecutor::reset_connection() {
    if (output_.is_open()) {
        output_.flush();
        output_.close();
    }
    if (auction_output_.is_open()) {
        auction_output_.flush();
        auction_output_.close();
    }
}

std::string Q2FrameCommandExecutor::json_escape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 2);
    for (const char ch : value) {
        if (ch == '\\' || ch == '"') {
            escaped.push_back('\\');
        }
        escaped.push_back(ch);
    }
    return escaped;
}

bool Q2FrameCommandExecutor::is_q2_key(const RedisCommand& command, const std::string& prefix) {
    return command.type == RedisCommandType::HSetMulti &&
        !prefix.empty() && command.key.rfind(prefix, 0) == 0 &&
        command.key.compare(prefix.size(), 7, "active:") != 0;
}

bool Q2FrameCommandExecutor::is_auction_fact_command(const RedisCommand& command, const ConfigV2& config) {
    if (command.type != RedisCommandType::HSetMulti && command.type != RedisCommandType::SetString) {
        return false;
    }
    const bool legacy_snapshot = !config.redis.legacy_auction_prefix.empty() &&
        command.key.rfind(config.redis.legacy_auction_prefix, 0) == 0;
    const bool anchor = !config.redis.legacy_anchor_prefix.empty() &&
        command.key.rfind(config.redis.legacy_anchor_prefix, 0) == 0;
    return legacy_snapshot || anchor;
}

std::string Q2FrameCommandExecutor::build_auction_command_record(const RedisCommand& command) {
    std::ostringstream json;
    json << "{\"version\":\"RedisAuctionCommandV1\",\"key\":\""
         << json_escape(command.key) << "\",\"type\":\"";
    if (command.type == RedisCommandType::SetString) {
        json << "set\",\"value\":\"" << json_escape(command.value) << "\"}";
        return json.str();
    }
    json << "hset\",\"fields\":{";
    for (std::size_t i = 0; i < command.fields.size(); ++i) {
        if (i > 0) json << ',';
        json << "\"" << json_escape(command.fields[i].first) << "\":\""
             << json_escape(command.fields[i].second) << "\"";
    }
    json << "}}";
    return json.str();
}

std::string Q2FrameCommandExecutor::symbol_from_key(const std::string& key, const std::string& prefix) {
    return key.substr(prefix.size());
}

bool Q2FrameCommandExecutor::is_string_field(const std::string& field) {
    return field == "mk";
}

bool Q2FrameCommandExecutor::parse_i64_field(const RedisFieldList& fields, const std::string& name, int64_t& out) {
    for (const auto& field : fields) {
        if (field.first == name) {
            try {
                std::size_t used = 0;
                out = std::stoll(field.second, &used);
                return used == field.second.size();
            } catch (...) {
                return false;
            }
        }
    }
    return false;
}

bool Q2FrameCommandExecutor::parse_i32_field(const RedisFieldList& fields, const std::string& name, int& out) {
    int64_t value = 0;
    if (!parse_i64_field(fields, name, value)) return false;
    out = static_cast<int>(value);
    return true;
}

std::string Q2FrameCommandExecutor::build_frame(
    const std::vector<const RedisCommand*>& updates,
    uint32_t seq_no
) const {
    int64_t logical_ts_ms = 0;
    int phase = 0;
    std::ostringstream json;
    json << "{\"version\":\"Q2FrameV1\",\"seq_no\":" << seq_no
         << ",\"logical_ts_ms\":";
    for (const RedisCommand* command : updates) {
        int64_t ts = 0;
        if (parse_i64_field(command->fields, "ts", ts)) {
            logical_ts_ms = std::max(logical_ts_ms, ts);
        }
        int current_phase = 0;
        if (phase == 0 && parse_i32_field(command->fields, "ph", current_phase)) {
            phase = current_phase;
        }
    }
    json << logical_ts_ms << ",\"phase\":" << phase << ",\"q2_updates\":[";
    for (std::size_t i = 0; i < updates.size(); ++i) {
        if (i > 0) json << ',';
        const RedisCommand& command = *updates[i];
        json << "{\"symbol\":\""
             << json_escape(symbol_from_key(command.key, config_.redis.q2_prefix)) << "\"";
        for (const auto& field : command.fields) {
            json << ",\"" << json_escape(field.first) << "\":";
            if (is_string_field(field.first)) {
                json << "\"" << json_escape(field.second) << "\"";
            } else {
                json << field.second;
            }
        }
        json << '}';
    }
    json << "]}";
    return json.str();
}

}  // namespace t1_v2
