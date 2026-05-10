#include "payload_decompressor.h"

#include <cstddef>

#if defined(T1_V2_ENABLE_ZLIB)
#include <zlib.h>
#endif

namespace t1_v2 {

PayloadCompression PayloadDecompressor::parse_compression(const std::string& text) {
    if (text == "NONE" || text == "none" || text == "0") {
        return PayloadCompression::None;
    }
    if (text == "GZIP" || text == "gzip" || text == "1") {
        return PayloadCompression::Gzip;
    }
    if (text == "DEFLATE" || text == "deflate" || text == "2") {
        return PayloadCompression::Deflate;
    }
    return PayloadCompression::Unknown;
}

PayloadDecompressResult PayloadDecompressor::decompress(
    PayloadCompression compression,
    const char* data,
    std::size_t size,
    std::vector<char>& out
) {
    out.clear();
    if (!data || size == 0) {
        return {false, false, "empty payload"};
    }

    if (compression == PayloadCompression::None) {
        return copy_none(data, size, out);
    }

#if defined(T1_V2_ENABLE_ZLIB)
    if (compression != PayloadCompression::Gzip && compression != PayloadCompression::Deflate) {
        return {false, false, "unknown compression"};
    }

    z_stream stream{};
    stream.avail_in = static_cast<uInt>(size);
    stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data));
    const int window_bits = compression == PayloadCompression::Gzip ? (MAX_WBITS | 32) : MAX_WBITS;
    int ret = inflateInit2(&stream, window_bits);
    if (ret != Z_OK) {
        return {false, false, "inflate init failed"};
    }

    struct InflateGuard {
        z_stream* stream = nullptr;
        explicit InflateGuard(z_stream* value) : stream(value) {}
        ~InflateGuard() { inflateEnd(stream); }
    } guard(&stream);

    constexpr std::size_t kChunkSize = 64 * 1024;
    std::vector<char> buffer(kChunkSize);
    do {
        stream.avail_out = static_cast<uInt>(buffer.size());
        stream.next_out = reinterpret_cast<Bytef*>(buffer.data());
        ret = inflate(&stream, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
            out.clear();
            return {false, false, "inflate failed"};
        }
        const std::size_t have = buffer.size() - stream.avail_out;
        out.insert(out.end(), buffer.data(), buffer.data() + have);
    } while (ret != Z_STREAM_END);

    return {!out.empty(), false, out.empty() ? "empty inflated payload" : ""};
#else
    return {false, true, "zlib support not enabled in this build"};
#endif
}

PayloadDecompressResult PayloadDecompressor::copy_none(const char* data, std::size_t size, std::vector<char>& out) {
    out.assign(data, data + size);
    return {!out.empty(), false, out.empty() ? "empty payload" : ""};
}

}  // namespace t1_v2
