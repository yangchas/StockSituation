#include "rabbitmq_tick_source.h"

#include <chrono>
#include <string>
#include <vector>

#include "rabbitmq_batch_decoder.h"

#if defined(T1_V2_ENABLE_RABBITMQ)
#include <rabbitmq-c/tcp_socket.h>
#include <sys/time.h>
#endif

namespace t1_v2 {

namespace {

#if defined(T1_V2_ENABLE_RABBITMQ)
timeval timeout_from_ms(int timeout_ms) {
    if (timeout_ms <= 0) {
        timeout_ms = 1000;
    }
    timeval timeout{};
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    return timeout;
}
#endif

}  // namespace

RabbitMqTickSource::RabbitMqTickSource(const ConfigV2& config) : config_(config) {}

bool RabbitMqTickSource::start() {
    last_error_.clear();
    seq_no_ = 0;
#if defined(T1_V2_ENABLE_RABBITMQ) && (!defined(T1_V2_ENABLE_PROTOBUF) || !defined(T1_V2_ENABLE_ZLIB))
    last_error_ = "RabbitMqTickSource requires --with-protobuf and --with-zlib before consuming live messages";
    started_ = false;
    return false;
#elif defined(T1_V2_ENABLE_RABBITMQ)
    started_ = connect();
    return started_;
#else
    last_error_ = "RabbitMqTickSource requires --with-rabbitmq";
    started_ = false;
    return false;
#endif
}

void RabbitMqTickSource::stop() {
    disconnect();
    started_ = false;
}

TickSourceResult RabbitMqTickSource::next_batch() {
    if (!started_) {
        TickSourceResult result;
        result.status = TickSourceStatus::Error;
        result.error_msg = last_error_.empty() ? "RabbitMqTickSource is not started" : last_error_.c_str();
        return result;
    }

#if defined(T1_V2_ENABLE_RABBITMQ)
    if ((!conn_ || !consumer_started_) && !connect()) {
        TickSourceResult result;
        result.status = TickSourceStatus::Empty;
        return result;
    }

    amqp_envelope_t envelope;
    struct timeval timeout = timeout_from_ms(config_.rabbitmq.consume_timeout_ms);
    amqp_rpc_reply_t reply = amqp_consume_message(conn_, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        std::vector<char> body(
            static_cast<char*>(envelope.message.body.bytes),
            static_cast<char*>(envelope.message.body.bytes) + envelope.message.body.len
        );
        const uint64_t delivery_tag = envelope.delivery_tag;
        const bool redelivered = envelope.redelivered != 0;
        amqp_destroy_envelope(&envelope);
        amqp_maybe_release_buffers_on_channel(conn_, 1);

        TickBatch batch;
        RabbitMqBatchDecodeResult decoded = RabbitMqBatchDecoder::decode_body(body, ++seq_no_, batch);
        TickSourceResult result;
        result.requires_ack = true;
        result.delivery_tag = delivery_tag;
        if (!decoded.ok) {
            last_error_ = decoded.error.empty() ? "RabbitMQ body decode failed" : decoded.error;
            result.status = TickSourceStatus::Error;
            result.requeue_on_error = !redelivered;
            result.error_msg = last_error_.c_str();
            return result;
        }
        result.status = TickSourceStatus::Ok;
        result.batch = std::move(batch);
        result.source_stats = decoded.build_stats;
        return result;
    }

    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
        TickSourceResult result;
        result.status = TickSourceStatus::Empty;
        return result;
    }

    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        set_error("RabbitMQ consume failed: ", amqp_error_string2(reply.library_error));
    } else {
        last_error_ = "RabbitMQ consume failed";
    }
    disconnect();
    arm_connect_backoff();
    TickSourceResult empty_after_disconnect;
    empty_after_disconnect.status = TickSourceStatus::Empty;
    return empty_after_disconnect;
#endif

    TickSourceResult result;
    result.status = TickSourceStatus::Error;
    result.error_msg = last_error_.empty() ? "RabbitMqTickSource requires --with-rabbitmq" : last_error_.c_str();
    return result;
}

bool RabbitMqTickSource::ack(const TickSourceResult& result) {
#if defined(T1_V2_ENABLE_RABBITMQ)
    if (conn_ && result.requires_ack && result.delivery_tag > 0) {
        const int status = amqp_basic_ack(conn_, 1, result.delivery_tag, 0);
        if (status == AMQP_STATUS_OK) {
            return true;
        }
        set_error("RabbitMQ ack failed: ", amqp_error_string2(status));
        disconnect();
        return false;
    }
#else
    (void)result;
#endif
    return true;
}

bool RabbitMqTickSource::reject(const TickSourceResult& result, bool requeue) {
#if defined(T1_V2_ENABLE_RABBITMQ)
    if (conn_ && result.requires_ack && result.delivery_tag > 0) {
        const int status = amqp_basic_reject(conn_, 1, result.delivery_tag, requeue ? 1 : 0);
        if (status == AMQP_STATUS_OK) {
            return true;
        }
        set_error("RabbitMQ reject failed: ", amqp_error_string2(status));
        disconnect();
        return false;
    }
#else
    (void)result;
    (void)requeue;
#endif
    return true;
}

bool RabbitMqTickSource::connect() {
#if defined(T1_V2_ENABLE_RABBITMQ)
#if !defined(T1_V2_ENABLE_PROTOBUF) || !defined(T1_V2_ENABLE_ZLIB)
    last_error_ = "RabbitMqTickSource requires --with-protobuf and --with-zlib before consuming live messages";
    return false;
#endif
    if (conn_) {
        return consumer_started_ || start_consumer();
    }
    if (connect_backoff_active()) {
        return false;
    }
    conn_ = amqp_new_connection();
    if (!conn_) {
        last_error_ = "RabbitMQ connect failed: create connection";
        arm_connect_backoff();
        return false;
    }
    amqp_socket_t* socket = amqp_tcp_socket_new(conn_);
    if (!socket) {
        last_error_ = "RabbitMQ connect failed: create socket";
        disconnect();
        arm_connect_backoff();
        return false;
    }
    timeval connect_timeout = timeout_from_ms(config_.rabbitmq.connect_timeout_ms);
    int status = amqp_socket_open_noblock(
        socket,
        config_.rabbitmq.host.c_str(),
        config_.rabbitmq.port,
        &connect_timeout
    );
    if (status != AMQP_STATUS_OK) {
        set_error("RabbitMQ connect failed: open socket: ", amqp_error_string2(status));
        disconnect();
        arm_connect_backoff();
        return false;
    }
    amqp_rpc_reply_t login_reply = amqp_login(
        conn_,
        config_.rabbitmq.vhost.c_str(),
        0,
        131072,
        config_.rabbitmq.heartbeat_seconds > 0 ? config_.rabbitmq.heartbeat_seconds : 0,
        AMQP_SASL_METHOD_PLAIN,
        config_.rabbitmq.user.c_str(),
        config_.rabbitmq.password.c_str()
    );
    if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        last_error_ = "RabbitMQ connect failed: login";
        disconnect();
        arm_connect_backoff();
        return false;
    }
    amqp_channel_open(conn_, 1);
    amqp_rpc_reply_t channel_reply = amqp_get_rpc_reply(conn_);
    if (channel_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        last_error_ = "RabbitMQ connect failed: open channel";
        disconnect();
        arm_connect_backoff();
        return false;
    }
    if (!start_consumer()) {
        disconnect();
        arm_connect_backoff();
        return false;
    }
    next_connect_retry_ms_ = 0;
    return true;
#else
    last_error_ = "RabbitMqTickSource requires --with-rabbitmq";
    return false;
#endif
}

void RabbitMqTickSource::disconnect() {
#if defined(T1_V2_ENABLE_RABBITMQ)
    consumer_started_ = false;
    if (conn_) {
        amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn_);
        conn_ = nullptr;
    }
#endif
}

bool RabbitMqTickSource::start_consumer() {
#if defined(T1_V2_ENABLE_RABBITMQ)
    if (!conn_) {
        last_error_ = "RabbitMQ consumer start failed: no connection";
        return false;
    }
    const uint16_t prefetch_count = config_.processing.messages_per_batch > 0
        ? static_cast<uint16_t>(config_.processing.messages_per_batch > 65535 ? 65535 : config_.processing.messages_per_batch)
        : 1;
    amqp_basic_qos(conn_, 1, 0, prefetch_count, 0);
    amqp_rpc_reply_t qos_reply = amqp_get_rpc_reply(conn_);
    if (qos_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        last_error_ = "RabbitMQ consumer start failed: qos";
        return false;
    }
    amqp_basic_consume(
        conn_,
        1,
        amqp_cstring_bytes(config_.rabbitmq.queue_name.c_str()),
        amqp_empty_bytes,
        0,
        0,
        0,
        amqp_empty_table
    );
    amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(conn_);
    if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        last_error_ = "RabbitMQ consumer start failed: basic_consume";
        return false;
    }
    consumer_started_ = true;
    return true;
#else
    last_error_ = "RabbitMqTickSource requires --with-rabbitmq";
    return false;
#endif
}

void RabbitMqTickSource::arm_connect_backoff() {
    next_connect_retry_ms_ = monotonic_ms() + 5000;
}

bool RabbitMqTickSource::connect_backoff_active() const {
    return next_connect_retry_ms_ > 0 && monotonic_ms() < next_connect_retry_ms_;
}

int64_t RabbitMqTickSource::monotonic_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
}

void RabbitMqTickSource::set_error(const char* prefix, const char* detail) {
    last_error_ = prefix ? std::string(prefix) : std::string();
    if (detail) {
        last_error_ += detail;
    }
}

}  // namespace t1_v2
