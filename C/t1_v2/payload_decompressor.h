#pragma once

#include <string>
#include <vector>

namespace t1_v2 {

enum class PayloadCompression {
    None = 0,
    Gzip = 1,
    Deflate = 2,
    Unknown = 3,
};

struct PayloadDecompressResult {
    bool ok = false;
    bool unsupported = false;
    std::string error;
};

class PayloadDecompressor {
public:
    static PayloadCompression parse_compression(const std::string& text);
    static PayloadDecompressResult decompress(
        PayloadCompression compression,
        const char* data,
        std::size_t size,
        std::vector<char>& out
    );

private:
    static PayloadDecompressResult copy_none(const char* data, std::size_t size, std::vector<char>& out);
};

}  // namespace t1_v2
