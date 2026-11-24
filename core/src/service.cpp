#include <plume_spark/service.hpp>
#include <google/protobuf/text_format.h>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>

#include "plume_spark/config_defaults.hpp"
#include "plume_spark/uuid.hpp"
#include "plume_spark/util/casts.hpp"

namespace plume_spark {
    namespace sc = ::spark::connect;

    grpc::Status SparkConnectServiceImpl::ExecutePlan(grpc::ServerContext *ctx,
                                                      const sc::ExecutePlanRequest *req,
                                                      grpc::ServerWriter<sc::ExecutePlanResponse> *res) {
        auto s = session_manager_.resolve(*req);
        if (!s.ok()) {
            return util::to_grpc(s.status());
        }
        auto &sess = **s;

        spdlog::debug("ExecutePlanRequest:\n{}", util::msg_to_text(*req));
        return {grpc::StatusCode::UNIMPLEMENTED, "ExecutePlan not implemented yet"};
    }

    grpc::Status SparkConnectServiceImpl::AnalyzePlan(grpc::ServerContext *ctx,
                                                      const sc::AnalyzePlanRequest *req,
                                                      sc::AnalyzePlanResponse *res) {
        auto s = session_manager_.resolve(*req);
        if (!s.ok()) {
            return util::to_grpc(s.status());
        }
        auto &sess = **s;

        spdlog::debug("AnalyzePlanRequest:\n{}", util::msg_to_text(*req));
        return {grpc::StatusCode::UNIMPLEMENTED, "AnalyzePlan not implemented yet"};
    }

    grpc::Status SparkConnectServiceImpl::Config(grpc::ServerContext *ctx,
                                                 const sc::ConfigRequest *req,
                                                 sc::ConfigResponse *res) {
        auto s = session_manager_.resolve(*req);
        if (!s.ok()) {
            return util::to_grpc(s.status());
        }
        auto &sess = **s;

        std::lock_guard lock(sess.mu);

        res->set_session_id(req->session_id());
        res->set_server_side_session_id(sess.server_session_id);

        spdlog::debug("ConfigRequest:\n{}", util::msg_to_text(*req));

        auto lookup = [&](const std::string& key) -> const std::string* {
            const auto it = sess.config.find(key);
            return it == sess.config.end() ? spark_config_default(key) : &it->second;
        };

        auto add_pair = [&](const std::string &key, const std::string* value) {
            auto *p = res->add_pairs();
            p->set_key(key);
            if (value) p->set_value(*value);
        };

        auto &op = req->operation();
        switch (op.op_type_case()) {
            case spark::connect::ConfigRequest_Operation::kSet:
                spdlog::debug("ConfigOperation SET");
                break;
            case spark::connect::ConfigRequest_Operation::kGet:
                spdlog::debug("ConfigOperation GET");
                for (const auto &k : op.get().keys()) {
                    add_pair(k, lookup(k));
                }
                return grpc::Status::OK;
            case spark::connect::ConfigRequest_Operation::kGetWithDefault:
                spdlog::debug("ConfigOperation GET_WITH_DEFAULT");
                break;
            case spark::connect::ConfigRequest_Operation::kGetOption:
                spdlog::debug("ConfigOperation GET_OPTION");
                break;
            case spark::connect::ConfigRequest_Operation::kGetAll:
                spdlog::debug("ConfigOperation GET_ALL");
                break;
            case spark::connect::ConfigRequest_Operation::kUnset:
                spdlog::debug("ConfigOperation UNSET");
                break;
            case spark::connect::ConfigRequest_Operation::kIsModifiable:
                spdlog::debug("ConfigOperation IS_MODIFIABLE");
                break;
            case spark::connect::ConfigRequest_Operation::OP_TYPE_NOT_SET:
                spdlog::debug("ConfigOperation OP_TYPE_NOT_SET");
                break;
        }

        return {grpc::StatusCode::UNIMPLEMENTED, "Config not implemented yet"};
    }

    grpc::Status SparkConnectServiceImpl::ReleaseExecute(grpc::ServerContext *ctx,
                                                         const sc::ReleaseExecuteRequest *req,
                                                         sc::ReleaseExecuteResponse *res) {
        auto s = session_manager_.resolve(*req);
        if (!s.ok()) {
            return util::to_grpc(s.status());
        }
        auto &sess = **s;

        spdlog::debug("ReleaseExecuteRequest:\n{}", util::msg_to_text(*req));
        res->set_session_id(req->session_id());
        res->set_server_side_session_id(sess.server_session_id);
        return grpc::Status::OK;
    }
} // namespace plume_spark
