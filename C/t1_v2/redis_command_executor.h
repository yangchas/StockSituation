#pragma once

#include <string>
#include <vector>

#include "config_v2.h"
#include "redis_command_formatter.h"
#include "redis_v2_writer.h"

#if defined(T1_V2_ENABLE_REDIS)
#include <hiredis/hiredis.h>
#endif

namespace t1_v2 {

struct RedisExecutionResult {
    bool ok = true;
    std::string error;
    int command_count = 0;
};

class IRedisCommandExecutor {
public:
    virtual ~IRedisCommandExecutor() = default;
    virtual RedisExecutionResult execute(const std::vector<RedisCommand>& commands) = 0;
};

class NullRedisCommandExecutor final : public IRedisCommandExecutor {
public:
    RedisExecutionResult execute(const std::vector<RedisCommand>& commands) override {
        RedisExecutionResult result;
        result.ok = true;
        result.command_count = static_cast<int>(commands.size());
        return result;
    }
};

#if defined(T1_V2_ENABLE_REDIS)
class HiredisRedisCommandExecutor final : public IRedisCommandExecutor {
public:
    explicit HiredisRedisCommandExecutor(const ConfigV2& config);
    ~HiredisRedisCommandExecutor() override;

    RedisExecutionResult execute(const std::vector<RedisCommand>& commands) override;
    void disconnect();
    bool is_connected() const { return context_ != nullptr; }

private:
    bool connect(RedisExecutionResult& result);
    bool append(const RedisCommand& command, RedisExecutionResult& result);
    bool append(const RedisArgvCommand& command, RedisExecutionResult& result);
    bool drain(int expected_replies, RedisExecutionResult& result);

private:
    ConfigV2 config_;
    redisContext* context_ = nullptr;
};
#endif

}  // namespace t1_v2
