#pragma once

#include <string>
#include <string_view>

namespace plume_spark {
    const std::string *spark_config_default(std::string_view key);
}