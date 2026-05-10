#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace t1_v2 {

struct RabbitMqWireHeader {
    std::string proto_version;
    std::string compression;
    std::string batch_id;
    int32_t record_count = 0;
    int32_t original_size = 0;
    int32_t compressed_size = 0;
    int64_t timestamp = 0;
};

struct RabbitMqWireMessageView {
    RabbitMqWireHeader header;
    const char* payload = nullptr;
    std::size_t payload_size = 0;
};

class RabbitMqWireMessageParser {
public:
    static bool parse(const std::vector<char>& body, RabbitMqWireMessageView& out, std::string* error = nullptr);

private:
    static uint32_t read_u32_be(const char* data);
    static bool parse_header_json(const std::string& json, RabbitMqWireHeader& out);
    static std::string extract_string(const std::string& json, const char* key);
    static int32_t extract_i32(const std::string& json, const char* key);
    static int64_t extract_i64(const std::string& json, const char* key);
};

}  // namespace t1_v2
