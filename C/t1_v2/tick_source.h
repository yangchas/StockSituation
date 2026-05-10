#pragma once

#include <cstdint>

#include "source_tick_batch_builder.h"
#include "tick_batch.h"

namespace t1_v2 {

enum class TickSourceStatus : uint8_t {
    Ok = 0,
    Empty = 1,
    EndOfStream = 2,
    Error = 3,
};

struct TickSourceResult {
    TickSourceStatus status = TickSourceStatus::Empty;
    TickBatch batch;
    SourceTickBatchBuildStats source_stats;
    bool requires_ack = false;
    bool requeue_on_error = true;
    uint64_t delivery_tag = 0;
    const char* error_msg = nullptr;
};

class ITickSource {
public:
    virtual ~ITickSource() = default;

    virtual bool start() = 0;
    virtual void stop() = 0;
    virtual TickSourceResult next_batch() = 0;
    virtual RuntimeMode mode() const = 0;
    virtual bool ack(const TickSourceResult& result) {
        (void)result;
        return true;
    }
    virtual bool reject(const TickSourceResult& result, bool requeue) {
        (void)result;
        (void)requeue;
        return true;
    }
};

}  // namespace t1_v2
