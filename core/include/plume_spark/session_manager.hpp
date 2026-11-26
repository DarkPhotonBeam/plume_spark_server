#pragma once
#include <memory>
#include <unordered_map>
#include <absl/status/statusor.h>
#include "plume_spark/session.hpp"
#include "plume_spark/util/uuid.hpp"

namespace plume_spark {
    template<typename R>
    concept SessionRequest = requires(const R &r)
    {
        { r.session_id() } -> std::convertible_to<const std::string &>;
        { r.user_context().user_id() } -> std::convertible_to<const std::string &>;
    };

    class SessionManager {
    public:
        std::shared_ptr<Session> get_or_create(const std::string &user_id, const std::string &session_id);
        std::shared_ptr<Session> find(const std::string &user_id, const std::string &session_id);
        void release(const std::string &user_id, const std::string &session_id);

        template <SessionRequest Req>
        absl::StatusOr<std::shared_ptr<Session>> resolve(const Req &req) {
            if (!util::is_valid_uuid(req.session_id())) {
                return absl::InvalidArgumentError("INVALID_HANDLE.FORMAT");
            }
            auto s = get_or_create(req.user_context().user_id(), req.session_id());
            if constexpr (requires { req.has_client_observed_server_side_session_id(); }) {
                if (req.has_client_observed_server_side_session_id() && req.client_observed_server_side_session_id() != s->server_session_id) {
                    return absl::FailedPreconditionError("INVALID_HANDLE.SESSION_CHANGED");
                }
            }
            return s;
        }

    private:
        // a session is uniquely identified by the tuple (user_id, session_id)
        static std::string key(const std::string &u, const std::string &s) { return u + '\0' + s; }
        std::mutex mu_;
        std::unordered_map<std::string, std::shared_ptr<Session>> sessions_;
    };
}
