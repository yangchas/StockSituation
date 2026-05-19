#include "rabbitmq_batch_decoder.h"

#include <sstream>

#include "protobuf_tick_decoder.h"

namespace t1_v2 {

RabbitMqBatchDecodeResult RabbitMqBatchDecoder::decode_body(
    const std::vector<char>& body,
    uint32_t seq_no,
    TickBatch& out
) {
    RabbitMqBatchDecodeResult result;
    RabbitMqWireMessageView view;
    std::string error;
    if (!RabbitMqWireMessageParser::parse(body, view, &error)) {
        result.error = error;
        return result;
    }
    result.header = view.header;

    std::vector<SourceTickRecord> source_records;
    const ProtobufDecodeResult decoded = ProtobufTickDecoder::decode_data_request_payload(
        view.payload,
        view.payload_size,
        source_records
    );
    if (!decoded.ok) {
        result.unsupported = decoded.unsupported;
        result.error = decoded.error;
        return result;
    }

    result.build_stats = SourceTickBatchBuilder::build(source_records, RuntimeMode::Live, 0, seq_no, out);
    out.wall_ts_ms = view.header.timestamp;
    result.ok = result.build_stats.accepted_count > 0;
    if (!result.ok) {
        std::ostringstream oss;
        oss << "no valid source records"
            << " | header_records=" << view.header.record_count
            << " | decoded=" << decoded.record_count
            << " | input=" << result.build_stats.input_count
            << " | rejected=" << result.build_stats.rejected_count;
        result.error = oss.str();
    }
    return result;
}

}  // namespace t1_v2
