#include "rabbitmq_wire_message.h"

#include <cctype>

namespace t1_v2 {

bool RabbitMqWireMessageParser::parse(
    const std::vector<char>& body,
    RabbitMqWireMessageView& out,
    std::string* error
) {
    out = RabbitMqWireMessageView{};
    if (body.size() < 4) {
        if (error) *error = "wire body too short";
        return false;
    }

    const uint32_t header_len = read_u32_be(body.data());
    if (header_len == 0 || body.size() < 4u + header_len) {
        if (error) *error = "invalid wire header length";
        return false;
    }

    RabbitMqWireHeader header;
    const std::string header_json(body.data() + 4, body.data() + 4 + header_len);
    if (!parse_header_json(header_json, header)) {
        if (error) *error = "invalid wire header json";
        return false;
    }

    const std::size_t payload_offset = 4u + header_len;
    if (payload_offset >= body.size()) {
        if (error) *error = "wire payload empty";
        return false;
    }

    out.header = std::move(header);
    out.payload = body.data() + payload_offset;
    out.payload_size = body.size() - payload_offset;
    return true;
}

uint32_t RabbitMqWireMessageParser::read_u32_be(const char* data) {
    const auto b0 = static_cast<uint32_t>(static_cast<unsigned char>(data[0]));
    const auto b1 = static_cast<uint32_t>(static_cast<unsigned char>(data[1]));
    const auto b2 = static_cast<uint32_t>(static_cast<unsigned char>(data[2]));
    const auto b3 = static_cast<uint32_t>(static_cast<unsigned char>(data[3]));
    return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
}

bool RabbitMqWireMessageParser::parse_header_json(const std::string& json, RabbitMqWireHeader& out) {
    out.record_count = extract_i32(json, "record_count");
    out.compression = extract_string(json, "compression");
    out.original_size = extract_i32(json, "original_size");
    out.compressed_size = extract_i32(json, "compressed_size");
    out.timestamp = extract_i64(json, "timestamp");
    out.batch_id = extract_string(json, "batch_id");
    out.proto_version = extract_string(json, "proto_version");

    return out.record_count > 0 &&
           out.record_count <= 100000 &&
           !out.compression.empty();
}

std::string RabbitMqWireMessageParser::extract_string(const std::string& json, const char* key) {
    const std::string pattern = std::string("\"") + key + "\"";
    std::size_t pos = json.find(pattern);
    if (pos == std::string::npos) {
        return "";
    }
    pos = json.find(':', pos + pattern.size());
    if (pos == std::string::npos) {
        return "";
    }
    ++pos;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '"') {
        return "";
    }
    const std::size_t start = ++pos;
    while (pos < json.size() && json[pos] != '"') {
        ++pos;
    }
    if (pos >= json.size()) {
        return "";
    }
    return json.substr(start, pos - start);
}

int32_t RabbitMqWireMessageParser::extract_i32(const std::string& json, const char* key) {
    const int64_t value = extract_i64(json, key);
    if (value > 2147483647LL || value < -2147483648LL) {
        return 0;
    }
    return static_cast<int32_t>(value);
}

int64_t RabbitMqWireMessageParser::extract_i64(const std::string& json, const char* key) {
    const std::string pattern = std::string("\"") + key + "\"";
    std::size_t pos = json.find(pattern);
    if (pos == std::string::npos) {
        return 0;
    }
    pos = json.find(':', pos + pattern.size());
    if (pos == std::string::npos) {
        return 0;
    }
    ++pos;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) {
        ++pos;
    }
    bool negative = false;
    if (pos < json.size() && json[pos] == '-') {
        negative = true;
        ++pos;
    }
    int64_t value = 0;
    bool has_digit = false;
    while (pos < json.size() && std::isdigit(static_cast<unsigned char>(json[pos]))) {
        has_digit = true;
        value = value * 10 + (json[pos] - '0');
        ++pos;
    }
    if (!has_digit) {
        return 0;
    }
    return negative ? -value : value;
}

}  // namespace t1_v2
