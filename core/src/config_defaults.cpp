#include <plume_spark/config_defaults.hpp>

#include <map>

namespace plume_spark {
    namespace {
        const std::map<std::string, std::string, std::less<>> &defaults() {
            static const std::map<std::string, std::string, std::less<>> m = {
                {"spark.connect.session.planCompression.threshold", "-1"},
                {"spark.connect.session.planCompression.defaultAlgorithm", "NONE"},
                {"spark.sql.session.timeZone", "UTC"},
            };
            return m;
        }
    }

    const std::string *spark_config_default(const std::string_view key) {
        const auto it = defaults().find(key);
        return it == defaults().end() ? nullptr : &it->second;
    }
}
