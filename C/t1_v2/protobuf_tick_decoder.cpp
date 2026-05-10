#include "protobuf_tick_decoder.h"

#include <cstddef>

#include "payload_decompressor.h"

#if defined(T1_V2_ENABLE_PROTOBUF)
#include "schema.pb.h"
#endif

namespace t1_v2 {

namespace {

#if defined(T1_V2_ENABLE_PROTOBUF)
PayloadCompression from_proto_compression(dataservice::CompressionType compression) {
    switch (compression) {
        case dataservice::NONE:
            return PayloadCompression::None;
        case dataservice::GZIP:
            return PayloadCompression::Gzip;
        case dataservice::DEFLATE:
            return PayloadCompression::Deflate;
        default:
            return PayloadCompression::Unknown;
    }
}

SourceTickRecord from_proto_record(const dataservice::DataRecord& record) {
    SourceTickRecord source;
    source.tss = record.tss();
    source.lp = record.lp();
    source.o = record.o();
    source.h = record.h();
    source.l = record.l();
    source.lc = record.lc();
    source.a = record.a();
    source.v = record.v();
    source.p = record.p();
    source.ap[0] = record.ap1();
    source.ap[1] = record.ap2();
    source.ap[2] = record.ap3();
    source.ap[3] = record.ap4();
    source.ap[4] = record.ap5();
    source.bp[0] = record.bp1();
    source.bp[1] = record.bp2();
    source.bp[2] = record.bp3();
    source.bp[3] = record.bp4();
    source.bp[4] = record.bp5();
    source.av[0] = record.av1();
    source.av[1] = record.av2();
    source.av[2] = record.av3();
    source.av[3] = record.av4();
    source.av[4] = record.av5();
    source.bv[0] = record.bv1();
    source.bv[1] = record.bv2();
    source.bv[2] = record.bv3();
    source.bv[3] = record.bv4();
    source.bv[4] = record.bv5();
    source.symbol = record.symbol();
    source.exchange = record.exchange();
    source.market = record.market();
    return source;
}
#endif

}  // namespace

ProtobufDecodeResult ProtobufTickDecoder::decode_data_request_payload(
    const char* data,
    std::size_t size,
    std::vector<SourceTickRecord>& out
) {
    out.clear();
    if (!data || size == 0) {
        return {false, false, 0, "empty protobuf payload"};
    }

#if !defined(T1_V2_ENABLE_PROTOBUF)
    return {false, true, 0, "protobuf support not enabled in this build"};
#else
    dataservice::DataRequest request;
    if (!request.ParseFromArray(data, static_cast<int>(size))) {
        return {false, false, 0, "DataRequest parse failed"};
    }

    std::vector<char> batch_bytes;
    const std::string& compressed = request.compressed_data();
    const PayloadDecompressResult decompressed = PayloadDecompressor::decompress(
        from_proto_compression(request.compression()),
        compressed.data(),
        compressed.size(),
        batch_bytes
    );
    if (!decompressed.ok) {
        return {false, decompressed.unsupported, 0, decompressed.error};
    }

    dataservice::DataBatch batch;
    if (!batch.ParseFromArray(batch_bytes.data(), static_cast<int>(batch_bytes.size()))) {
        return {false, false, 0, "DataBatch parse failed"};
    }
    if (batch.records_size() <= 0) {
        return {false, false, 0, "DataBatch has no records"};
    }

    out.reserve(static_cast<std::size_t>(batch.records_size()));
    for (int i = 0; i < batch.records_size(); ++i) {
        out.push_back(from_proto_record(batch.records(i)));
    }
    return {true, false, static_cast<uint32_t>(out.size()), ""};
#endif
}

}  // namespace t1_v2
