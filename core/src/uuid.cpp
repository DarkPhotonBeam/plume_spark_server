#include <plume_spark/uuid.hpp>
#include <array>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <string_view>

std::string plume_spark::generate_uuid_v4() {
    thread_local std::random_device rd;          // on Linux: getrandom(), so the bits are strong
    std::array<uint8_t, 16> b;
    for (size_t i = 0; i < b.size(); i += 4) {
        uint32_t r = rd();
        std::memcpy(&b[i], &r, 4);
    }
    b[6] = (b[6] & 0x0F) | 0x40;                 // version 4
    b[8] = (b[8] & 0x3F) | 0x80;                 // variant 10xx (RFC 9562)

    static constexpr char kHex[] = "0123456789abcdef";
    std::string s;
    s.reserve(36);
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) s += '-';
        s += kHex[b[i] >> 4];
        s += kHex[b[i] & 0x0F];
    }
    return s;
}

bool plume_spark::is_valid_uuid(std::string_view s) {
    if (s.size() != 36) return false;
    for (size_t i = 0; i < s.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s[i] != '-') return false;
        } else if (!std::isxdigit(static_cast<unsigned char>(s[i]))) {
            return false;
        }
    }
    return true;
}
