#include "rabbitmq_batch_decoder.h"

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
        result.error = "no valid source records";
    }
    return result;
}

}  // namespace t1_v2
