#include "redis_command_formatter.h"

#include <utility>

namespace t1_v2 {

bool RedisCommandFormatter::format(const RedisCommand& command, RedisArgvCommand& out, std::string* error) {
    out.argv.clear();
    switch (command.type) {
        case RedisCommandType::HSetMulti:
            return format_hset_multi(command, out, error);
        case RedisCommandType::SetString:
            return format_set_string(command, out, error);
        case RedisCommandType::SAdd:
            return format_sadd(command, out, error);
        case RedisCommandType::Expire:
            return format_expire(command, out, error);
    }
    if (error) {
        *error = "unknown redis command type";
    }
    return false;
}

RedisCommandFormatStats RedisCommandFormatter::estimate(const std::vector<RedisCommand>& commands) {
    RedisCommandFormatStats stats;
    stats.command_count = commands.size();
    for (const RedisCommand& command : commands) {
        RedisArgvCommand argv;
        std::string error;
        if (!format(command, argv, &error)) {
            stats.ok = false;
            stats.error = error;
            return stats;
        }
        stats.argv_count += argv.argv.size();
        if (command.type == RedisCommandType::HSetMulti) {
            stats.field_count += command.fields.size();
        }
        stats.payload_bytes += command_payload_bytes(argv);
    }
    return stats;
}

bool RedisCommandFormatter::format_hset_multi(const RedisCommand& command, RedisArgvCommand& out, std::string* error) {
    if (command.key.empty()) {
        if (error) *error = "HSET key is empty";
        return false;
    }
    if (command.fields.empty()) {
        if (error) *error = "HSET fields are empty";
        return false;
    }
    out.argv.reserve(2 + command.fields.size() * 2);
    out.argv.emplace_back("HSET");
    out.argv.emplace_back(command.key);
    for (const auto& field : command.fields) {
        if (field.first.empty()) {
            if (error) *error = "HSET field name is empty";
            out.argv.clear();
            return false;
        }
        out.argv.emplace_back(field.first);
        out.argv.emplace_back(field.second);
    }
    return true;
}

bool RedisCommandFormatter::format_set_string(const RedisCommand& command, RedisArgvCommand& out, std::string* error) {
    if (command.key.empty()) {
        if (error) *error = "SET key is empty";
        return false;
    }
    out.argv.reserve(3);
    out.argv.emplace_back("SET");
    out.argv.emplace_back(command.key);
    out.argv.emplace_back(command.value);
    return true;
}

bool RedisCommandFormatter::format_sadd(const RedisCommand& command, RedisArgvCommand& out, std::string* error) {
    if (command.key.empty()) {
        if (error) *error = "SADD key is empty";
        return false;
    }
    if (command.members.empty()) {
        if (error) *error = "SADD members are empty";
        return false;
    }
    out.argv.reserve(2 + command.members.size());
    out.argv.emplace_back("SADD");
    out.argv.emplace_back(command.key);
    for (const std::string& member : command.members) {
        if (member.empty()) {
            if (error) *error = "SADD member is empty";
            out.argv.clear();
            return false;
        }
        out.argv.emplace_back(member);
    }
    return true;
}

bool RedisCommandFormatter::format_expire(const RedisCommand& command, RedisArgvCommand& out, std::string* error) {
    if (command.key.empty()) {
        if (error) *error = "EXPIRE key is empty";
        return false;
    }
    if (command.ttl_seconds <= 0) {
        if (error) *error = "EXPIRE ttl must be positive";
        return false;
    }
    out.argv.reserve(3);
    out.argv.emplace_back("EXPIRE");
    out.argv.emplace_back(command.key);
    out.argv.emplace_back(std::to_string(command.ttl_seconds));
    return true;
}

std::size_t RedisCommandFormatter::command_payload_bytes(const RedisArgvCommand& command) {
    std::size_t total = 0;
    for (const std::string& arg : command.argv) {
        total += arg.size();
    }
    return total;
}

}  // namespace t1_v2
