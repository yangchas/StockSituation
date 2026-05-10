#pragma once

#include <string>
#include <vector>

#include "rabbitmq_wire_message.h"
#include "source_tick_batch_builder.h"

namespace t1_v2 {

struct RabbitMqBatchDecodeResult {
    bool ok = false;
    bool unsupported = false;
    std::string error;
    RabbitMqWireHeader header;
    SourceTickBatchBuildStats build_stats;
};

class RabbitMqBatchDecoder {
public:
    static RabbitMqBatchDecodeResult decode_body(
        const std::vector<char>& body,
        uint32_t seq_no,
        TickBatch& out
    );
};

}  // namespace t1_v2
