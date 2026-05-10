#include "tdengine_command_executor.h"

#if defined(T1_V2_ENABLE_TDENGINE)

namespace t1_v2 {

TaosTDengineCommandExecutor::TaosTDengineCommandExecutor(const ConfigV2& config) : config_(config) {}

TaosTDengineCommandExecutor::~TaosTDengineCommandExecutor() {
    disconnect();
}

TDengineExecutionResult TaosTDengineCommandExecutor::preflight() {
    TDengineExecutionResult result;
    if (!connect(result)) {
        return result;
    }
    result.ok = true;
    return result;
}

TDengineExecutionResult TaosTDengineCommandExecutor::execute(const std::vector<std::string>& statements) {
    TDengineExecutionResult result;
    if (statements.empty()) {
        result.ok = true;
        result.statement_count = 0;
        return result;
    }
    if (!connect(result)) {
        return result;
    }

    int executed = 0;
    for (const std::string& statement : statements) {
        if (statement.empty()) {
            continue;
        }
        if (!execute_one(statement, result)) {
            disconnect();
            return result;
        }
        ++executed;
    }
    result.ok = true;
    result.statement_count = executed;
    return result;
}

void TaosTDengineCommandExecutor::disconnect() {
    if (conn_) {
        taos_close(conn_);
        conn_ = nullptr;
    }
}

bool TaosTDengineCommandExecutor::connect(TDengineExecutionResult& result) {
    if (conn_) {
        return true;
    }
    conn_ = taos_connect(
        config_.tdengine.host.c_str(),
        config_.tdengine.user.c_str(),
        config_.tdengine.password.c_str(),
        config_.tdengine.database.c_str(),
        static_cast<uint16_t>(config_.tdengine.port)
    );
    if (!conn_) {
        result.ok = false;
        const char* err = taos_errstr(nullptr);
        result.error = err ? err : "tdengine connect failed";
        return false;
    }
    return true;
}

bool TaosTDengineCommandExecutor::execute_one(const std::string& statement, TDengineExecutionResult& result) {
    TAOS_RES* res = taos_query(conn_, statement.c_str());
    const int code = taos_errno(res);
    if (code != 0) {
        result.ok = false;
        const char* err = taos_errstr(res);
        result.error = err ? err : "tdengine sql failed";
        if (res) {
            taos_free_result(res);
        }
        return false;
    }
    if (res) {
        taos_free_result(res);
    }
    return true;
}

}  // namespace t1_v2

#endif
