#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "redis_v2_writer.h"

namespace t1_v2 {

struct RedisArgvCommand {
    std::vector<std::string> argv;
};

struct RedisCommandFormatStats {
    bool ok = true;
    std::string error;
    std::size_t command_count = 0;
    std::size_t argv_count = 0;
    std::size_t field_count = 0;
    std::size_t payload_bytes = 0;
};

class RedisCommandFormatter {
public:
    static bool format(const RedisCommand& command, RedisArgvCommand& out, std::string* error = nullptr);
    static RedisCommandFormatStats estimate(const std::vector<RedisCommand>& commands);

private:
    static bool format_hset_multi(const RedisCommand& command, RedisArgvCommand& out, std::string* error);
    static bool format_set_string(const RedisCommand& command, RedisArgvCommand& out, std::string* error);
    static bool format_sadd(const RedisCommand& command, RedisArgvCommand& out, std::string* error);
    static bool format_expire(const RedisCommand& command, RedisArgvCommand& out, std::string* error);
    static std::size_t command_payload_bytes(const RedisArgvCommand& command);
};

}  // namespace t1_v2
