#pragma once

#include <string>
#include <vector>

#include "raw_tick_converter.h"

namespace t1_v2 {

struct ProtobufDecodeResult {
    bool ok = false;
    bool unsupported = false;
    uint32_t record_count = 0;
    std::string error;
};

class ProtobufTickDecoder {
public:
    static ProtobufDecodeResult decode_data_request_payload(
        const char* data,
        std::size_t size,
        std::vector<SourceTickRecord>& out
    );
};

}  // namespace t1_v2
