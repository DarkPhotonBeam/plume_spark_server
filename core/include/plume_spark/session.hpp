#pragma once
#include <map>
#include <mutex>
#include <string>

namespace plume_spark {
    struct Session {
        std::string user_id;
        std::string session_id;
        std::string server_session_id;

        std::mutex mu;
        std::map<std::string, std::string> config;
    };
}