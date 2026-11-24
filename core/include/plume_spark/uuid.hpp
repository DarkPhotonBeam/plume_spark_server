#pragma once
#include <string>
#include <string_view>

namespace plume_spark {
    std::string generate_uuid_v4();
    bool is_valid_uuid(std::string_view s);
}