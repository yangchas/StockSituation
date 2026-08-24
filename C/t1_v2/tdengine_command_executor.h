#pragma once

#include <string>
#include <vector>

#include "config_v2.h"

#if defined(T1_V2_ENABLE_TDENGINE)
#include <taos.h>
#endif

namespace t1_v2 {

struct TDengineExecutionResult {
    bool ok = true;
    std::string error;
    int statement_count = 0;
};

class ITDengineCommandExecutor {
public:
    virtual ~ITDengineCommandExecutor() = default;
    virtual TDengineExecutionResult preflight() = 0;
    virtual TDengineExecutionResult execute(const std::vector<std::string>& statements) = 0;
    virtual void reset_connection() {}
};

class NullTDengineCommandExecutor final : public ITDengineCommandExecutor {
public:
    TDengineExecutionResult preflight() override {
        return {};
    }

    TDengineExecutionResult execute(const std::vector<std::string>& statements) override {
        TDengineExecutionResult result;
        result.ok = true;
        result.statement_count = static_cast<int>(statements.size());
        return result;
    }
};

#if defined(T1_V2_ENABLE_TDENGINE)
class TaosTDengineCommandExecutor final : public ITDengineCommandExecutor {
public:
    explicit TaosTDengineCommandExecutor(const ConfigV2& config);
    ~TaosTDengineCommandExecutor() override;

    TDengineExecutionResult preflight() override;
    TDengineExecutionResult execute(const std::vector<std::string>& statements) override;
    void disconnect();
    bool is_connected() const { return conn_ != nullptr; }

private:
    bool connect(TDengineExecutionResult& result);
    bool execute_one(const std::string& statement, TDengineExecutionResult& result);

private:
    ConfigV2 config_;
    TAOS* conn_ = nullptr;
};
#endif

}  // namespace t1_v2
