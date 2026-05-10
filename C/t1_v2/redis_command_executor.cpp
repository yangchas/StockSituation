#include "redis_command_executor.h"

#if defined(T1_V2_ENABLE_REDIS)

#include <sys/time.h>

#include <vector>

namespace t1_v2 {

namespace {

timeval ms_to_timeval(int ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    return tv;
}

}  // namespace

HiredisRedisCommandExecutor::HiredisRedisCommandExecutor(const ConfigV2& config) : config_(config) {}

HiredisRedisCommandExecutor::~HiredisRedisCommandExecutor() {
    disconnect();
}

RedisExecutionResult HiredisRedisCommandExecutor::execute(const std::vector<RedisCommand>& commands) {
    RedisExecutionResult result;
    if (commands.empty()) {
        result.ok = true;
        result.command_count = 0;
        return result;
    }
    if (!connect(result)) {
        return result;
    }

    int appended = 0;
    for (const RedisCommand& command : commands) {
        if (!append(command, result)) {
            disconnect();
            return result;
        }
        ++appended;
    }

    if (!drain(appended, result)) {
        disconnect();
        return result;
    }
    result.ok = true;
    result.command_count = appended;
    return result;
}

void HiredisRedisCommandExecutor::disconnect() {
    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }
}

bool HiredisRedisCommandExecutor::connect(RedisExecutionResult& result) {
    if (context_) {
        return true;
    }
    timeval connect_timeout = ms_to_timeval(1000);
    context_ = redisConnectWithTimeout(config_.redis.host.c_str(), config_.redis.port, connect_timeout);
    if (!context_ || context_->err) {
        result.ok = false;
        result.error = context_ && context_->errstr ? context_->errstr : "redis connect failed";
        disconnect();
        return false;
    }

    timeval io_timeout = ms_to_timeval(1500);
    if (redisSetTimeout(context_, io_timeout) != REDIS_OK) {
        result.ok = false;
        result.error = "redis set timeout failed";
        disconnect();
        return false;
    }

    if (config_.redis.db > 0) {
        redisReply* reply = static_cast<redisReply*>(redisCommand(context_, "SELECT %d", config_.redis.db));
        if (!reply) {
            result.ok = false;
            result.error = "redis select failed";
            disconnect();
            return false;
        }
        const bool ok = reply->type != REDIS_REPLY_ERROR;
        if (!ok && reply->str) {
            result.error = reply->str;
        }
        freeReplyObject(reply);
        if (!ok) {
            result.ok = false;
            disconnect();
            return false;
        }
    }
    return true;
}

bool HiredisRedisCommandExecutor::append(const RedisCommand& command, RedisExecutionResult& result) {
    std::vector<const char*> argv;
    std::vector<std::size_t> argvlen;
    std::string ttl_buffer;

    auto push = [&](const std::string& value) {
        argv.push_back(value.c_str());
        argvlen.push_back(value.size());
    };
    auto push_literal = [&](const char* value) {
        argv.push_back(value);
        argvlen.push_back(std::char_traits<char>::length(value));
    };
    auto fail = [&](const char* error) {
        result.ok = false;
        result.error = error ? error : "redis command format failed";
        return false;
    };

    switch (command.type) {
        case RedisCommandType::HSetMulti:
            if (command.key.empty()) return fail("HSET key is empty");
            if (command.fields.empty()) return fail("HSET fields are empty");
            argv.reserve(2 + command.fields.size() * 2);
            argvlen.reserve(2 + command.fields.size() * 2);
            push_literal("HSET");
            push(command.key);
            for (const auto& field : command.fields) {
                if (field.first.empty()) return fail("HSET field name is empty");
                push(field.first);
                push(field.second);
            }
            break;
        case RedisCommandType::SetString:
            if (command.key.empty()) return fail("SET key is empty");
            argv.reserve(3);
            argvlen.reserve(3);
            push_literal("SET");
            push(command.key);
            push(command.value);
            break;
        case RedisCommandType::SAdd:
            if (command.key.empty()) return fail("SADD key is empty");
            if (command.members.empty()) return fail("SADD members are empty");
            argv.reserve(2 + command.members.size());
            argvlen.reserve(2 + command.members.size());
            push_literal("SADD");
            push(command.key);
            for (const std::string& member : command.members) {
                if (member.empty()) return fail("SADD member is empty");
                push(member);
            }
            break;
        case RedisCommandType::Expire:
            if (command.key.empty()) return fail("EXPIRE key is empty");
            if (command.ttl_seconds <= 0) return fail("EXPIRE ttl must be positive");
            ttl_buffer = std::to_string(command.ttl_seconds);
            argv.reserve(3);
            argvlen.reserve(3);
            push_literal("EXPIRE");
            push(command.key);
            push(ttl_buffer);
            break;
        default:
            return fail("unknown redis command type");
    }

    if (redisAppendCommandArgv(context_, static_cast<int>(argv.size()), argv.data(), argvlen.data()) != REDIS_OK) {
        result.ok = false;
        result.error = context_ && context_->errstr ? context_->errstr : "redis append failed";
        return false;
    }
    return true;
}

bool HiredisRedisCommandExecutor::append(const RedisArgvCommand& command, RedisExecutionResult& result) {
    std::vector<const char*> argv;
    std::vector<std::size_t> argvlen;
    argv.reserve(command.argv.size());
    argvlen.reserve(command.argv.size());
    for (const std::string& arg : command.argv) {
        argv.push_back(arg.c_str());
        argvlen.push_back(arg.size());
    }
    if (redisAppendCommandArgv(context_, static_cast<int>(argv.size()), argv.data(), argvlen.data()) != REDIS_OK) {
        result.ok = false;
        result.error = context_ && context_->errstr ? context_->errstr : "redis append failed";
        return false;
    }
    return true;
}

bool HiredisRedisCommandExecutor::drain(int expected_replies, RedisExecutionResult& result) {
    for (int i = 0; i < expected_replies; ++i) {
        void* reply_void = nullptr;
        if (redisGetReply(context_, &reply_void) != REDIS_OK) {
            if (reply_void) {
                freeReplyObject(reply_void);
            }
            result.ok = false;
            result.error = context_ && context_->errstr ? context_->errstr : "redis get reply failed";
            return false;
        }
        auto* reply = static_cast<redisReply*>(reply_void);
        const bool ok = reply && reply->type != REDIS_REPLY_ERROR;
        if (!ok && reply && reply->str) {
            result.error = reply->str;
        }
        if (reply) {
            freeReplyObject(reply);
        }
        if (!ok) {
            result.ok = false;
            return false;
        }
    }
    return true;
}

}  // namespace t1_v2

#endif
