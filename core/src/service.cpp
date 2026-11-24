#include <plume_spark/service.hpp>
#include <google/protobuf/text_format.h>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>

#include "plume_spark/config_defaults.hpp"
#include "../include/plume_spark/util/uuid.hpp"
#include "plume_spark/util/casts.hpp"

namespace plume_spark {
    namespace sc = ::spark::connect;

    grpc::Status SparkConnectServiceImpl::ExecutePlan(grpc::ServerContext *ctx,
                                                      const sc::ExecutePlanRequest *req,
                                                      grpc::ServerWriter<sc::ExecutePlanResponse> *writer) {
        auto s = session_manager_.resolve(*req);
        if (!s.ok()) {
            return util::to_grpc(s.status());
        }
        auto &sess = **s;

        const std::string op_id = req->has_operation_id() ? req->operation_id() : util::generate_uuid_v4();

        spdlog::debug("ExecutePlanRequest:\n{}", util::msg_to_text(*req));

        auto make_response = [&] {
            sc::ExecutePlanResponse r;
            r.set_session_id(sess.session_id);
            r.set_server_side_session_id(sess.server_session_id);
            r.set_operation_id(op_id);
            r.set_response_id(util::generate_uuid_v4());
            return r;
        };

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        auto dummy_batch = util::make_range_batch(10);
        if (!dummy_batch.ok()) return {grpc::StatusCode::INTERNAL, dummy_batch.status().ToString()};
        batches.push_back(*dummy_batch);

        for (const auto &b : batches) {
            auto ipc = util::to_ipc_stream(*b);
            if (!ipc.ok()) return {grpc::StatusCode::INTERNAL, ipc.status().ToString()};

            auto res = make_response();
            auto *ab = res.mutable_arrow_batch();
            ab->set_row_count(b->num_rows());
            ab->set_data(std::move(*ipc));
            if (!writer->Write(res)) {
                return {grpc::StatusCode::CANCELLED, "client closed the stream"};
            }
        }

        auto done = make_response();
        done.mutable_result_complete();
        writer->Write(done);

        return grpc::Status::OK;
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


        res->set_session_id(req->session_id());
        res->set_server_side_session_id(sess.server_session_id);

        spdlog::debug("ConfigRequest:\n{}", util::msg_to_text(*req));

        auto add_pair = [&](const std::string &key, const std::optional<std::string> &value) {
            auto *p = res->add_pairs();
            p->set_key(key);
            if (value) p->set_value(*value);
        };

        auto &op = req->operation();
        switch (op.op_type_case()) {
            case sc::ConfigRequest_Operation::kSet: {
                spdlog::debug("ConfigOperation SET");
                std::vector<std::pair<std::string, std::string>> pairs;
                pairs.reserve(op.set().pairs_size());
                for (const auto &kv : op.set().pairs()) {
                    if (kv.has_value()) pairs.emplace_back(kv.key(), kv.value());
                }
                sess.set_configs(pairs);
                return grpc::Status::OK;
            }
            case sc::ConfigRequest_Operation::kGet: {
                spdlog::debug("ConfigOperation GET {}", util::msg_to_text(op.get()));
                for (const auto &k : op.get().keys()) {
                    add_pair(k, sess.get_config(k));
                }
                return grpc::Status::OK;
            }
            case sc::ConfigRequest_Operation::kGetWithDefault: {
                spdlog::debug("ConfigOperation GET_WITH_DEFAULT");
                for (const auto &kv : op.get_with_default().pairs()) {
                    auto v = sess.get_config(kv.key());
                    if (!v && kv.has_value()) v = kv.value();
                    add_pair(kv.key(), v);
                }
                return grpc::Status::OK;
            }
            case sc::ConfigRequest_Operation::kGetOption: {
                spdlog::debug("ConfigOperation GET_OPTION");
                for (const auto &k : op.get_option().keys()) {
                    add_pair(k, sess.get_config(k));
                }
                return grpc::Status::OK;
                break;
            }
            case sc::ConfigRequest_Operation::kGetAll:
                spdlog::debug("ConfigOperation GET_ALL");
                break;
            case sc::ConfigRequest_Operation::kUnset:
                spdlog::debug("ConfigOperation UNSET");
                break;
            case sc::ConfigRequest_Operation::kIsModifiable:
                spdlog::debug("ConfigOperation IS_MODIFIABLE");
                break;
            case sc::ConfigRequest_Operation::OP_TYPE_NOT_SET:
                spdlog::debug("ConfigOperation OP_TYPE_NOT_SET");
                return {grpc::StatusCode::INVALID_ARGUMENT, "ConfigRequest without operation"};
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
