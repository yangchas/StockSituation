#pragma once

#include "config_v2.h"
#include "tick_source.h"

#if defined(T1_V2_ENABLE_RABBITMQ)
#include <rabbitmq-c/amqp.h>
#endif

namespace t1_v2 {

class RabbitMqTickSource final : public ITickSource {
public:
    explicit RabbitMqTickSource(const ConfigV2& config);
    ~RabbitMqTickSource() override = default;

    bool start() override;
    void stop() override;
    TickSourceResult next_batch() override;
    RuntimeMode mode() const override { return RuntimeMode::Live; }
    std::string error_message() const override { return last_error_; }
    bool ack(const TickSourceResult& result) override;
    bool reject(const TickSourceResult& result, bool requeue) override;

private:
    bool connect();
    void disconnect();
    bool start_consumer();
    void arm_connect_backoff();
    bool connect_backoff_active() const;
    static int64_t monotonic_ms();
    void set_error(const char* prefix, const char* detail = nullptr);

    ConfigV2 config_;
    bool started_ = false;
    uint32_t seq_no_ = 0;
    std::string last_error_;
    int64_t next_connect_retry_ms_ = 0;

#if defined(T1_V2_ENABLE_RABBITMQ)
    amqp_connection_state_t conn_ = nullptr;
    bool consumer_started_ = false;
#endif
};

}  // namespace t1_v2
