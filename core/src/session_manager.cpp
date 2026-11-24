#include <plume_spark/session_manager.hpp>
#include <../include/plume_spark/util/uuid.hpp>

namespace {

}


namespace plume_spark {

    std::shared_ptr<Session> SessionManager::get_or_create(const std::string &user_id,
                                                                                 const std::string &session_id) {
        std::lock_guard lock(mu_);
        auto &res = sessions_[key(user_id, session_id)];
        if (res) {
            return res;
        }
        res = std::make_shared<Session>(user_id, session_id, util::generate_uuid_v4());
        return res;
    }

    std::shared_ptr<Session> SessionManager::find(const std::string &user_id,
        const std::string &session_id) {
        std::lock_guard lock(mu_);
        return sessions_[key(user_id, session_id)];
    }

    void SessionManager::release(const std::string &user_id, const std::string &session_id) {
        std::lock_guard lock(mu_);
        const auto k = key(user_id, session_id);
        if (!sessions_.contains(k)) {
            return;
        }
        sessions_.erase(k);
    }

}

