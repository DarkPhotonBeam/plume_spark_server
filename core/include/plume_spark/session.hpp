#pragma once
#include <map>
#include <mutex>
#include <string>

#include "config_defaults.hpp"

namespace plume_spark {
    class Session {
    public:
        Session(std::string user_id, std::string session_id, std::string server_session_id)
            :   user_id(std::move(user_id)),
                session_id(std::move(session_id)),
                server_session_id(std::move(server_session_id)) {}

        const std::string user_id;
        const std::string session_id;
        const std::string server_session_id;

        std::optional<std::string> get_config(std::string_view key) const {
            std::lock_guard lock(mu_);
            if (auto it = config_.find(key); it != config_.end()) return it->second;
            if (auto *d = spark_config_default(key)) return *d;
            return std::nullopt;
        }
        void set_configs(std::span<const std::pair<std::string, std::string>> pairs) {
            std::lock_guard lock(mu_);
            for (const auto &[k, v] : pairs) config_.insert_or_assign(k, v);
        }
    private:
        mutable std::mutex mu_;
        std::map<std::string, std::string, std::less<>> config_;
    };
}
