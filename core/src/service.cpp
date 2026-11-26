#include "plume_spark/service.hpp"
#include <google/protobuf/text_format.h>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <absl/status/statusor.h>

#include "plume_spark/config_defaults.hpp"
#include "plume_spark/util/uuid.hpp"
#include "plume_spark/ast_builder.hpp"
#include "plume_spark/ast/expression.hpp"
#include "plume_spark/util/casts.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "plume/parser/compile.hpp"
#include "plume/parser/converter.hpp"
#include "plume_spark/plume_builder.hpp"
#include <cpr/cpr.h>

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "plume/dandelion/api.hpp"
#include "plume/memory/adapter.hpp"
#include "plume_spark/util/plan_format.hpp"

namespace plume_spark {
    namespace sc = ::spark::connect;

    absl::StatusOr<plume::dandelion::BinaryData> run_on_dandelion(const plume::dandelion::BinaryData &body,
                                                              const std::string &url,       // e.g. "http://localhost:8080/hot/matmul"
                                                              std::chrono::milliseconds timeout) {
        cpr::Response resp = cpr::Post(
            cpr::Url{url},
            cpr::Body{reinterpret_cast<const char *>(body.data()), body.size()},   // binary body, no copy into a std::string first
            cpr::Header{{"Content-Type", "application/octet-stream"}},
            cpr::Timeout{timeout});

        if (resp.error)                                   // transport error: connection refused, timeout, ...
            return absl::UnavailableError("dandelion request failed: " + resp.error.message);
        if (resp.status_code < 200 || resp.status_code >= 300)
            return absl::InternalError("dandelion returned HTTP " + std::to_string(resp.status_code) + ": " + resp.text);

        return plume::dandelion::BinaryData(resp.text.begin(), resp.text.end());   // resp.text holds raw bytes
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
to_record_batches(duckdb::Connection &con, plume::dandelion::DataSetVec &sets, const plume::Schema &out_schema) {
        // Arrow schema from the plan, so an empty result still has one.
        duckdb::vector<duckdb::LogicalType> types;
        duckdb::vector<std::string> names;
        for (const auto &c : out_schema.columns) {
            types.push_back(plume::ToLogicalType(c.type));
            names.push_back(c.name);
        }
        duckdb::ClientProperties props = con.context->GetClientProperties();   // defaults: UTC, 32-bit offsets (utf8, not large_utf8), no string views
        ArrowSchema c_schema;
        duckdb::ArrowConverter::ToArrowSchema(&c_schema, types, names, props);
        ARROW_ASSIGN_OR_RAISE(auto schema, arrow::ImportSchema(&c_schema));

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        if (sets.empty()) return batches;
        for (auto &item : sets[0]) {
            plume::Schema block_schema;
            // Chunks borrow item.data; VARCHAR offsets are swizzled into pointers in place.
            auto chunks = plume::memory::ImportBlockChunks(item.data.data(), item.data.size(), block_schema);
            if (chunks.is_error()) return arrow::Status::Invalid(chunks.error_msg());
            for (auto &chunk : std::move(chunks).unwrap()) {
                if (chunk->GetTypes() != types)
                    return arrow::Status::Invalid("result block types differ from the plan's output schema");
                ArrowArray c_array;
                duckdb::ArrowConverter::ToArrowArray(*chunk, &c_array, props, {});   // copies into Arrow-owned buffers
                ARROW_ASSIGN_OR_RAISE(auto rb, arrow::ImportRecordBatch(&c_array, schema));
                batches.push_back(std::move(rb));
            }
        }
        return batches;
    }

    grpc::Status SparkConnectServiceImpl::ExecutePlan(grpc::ServerContext *ctx,
                                                      const sc::ExecutePlanRequest *req,
                                                      grpc::ServerWriter<sc::ExecutePlanResponse> *writer) {
        try {
            return execute_plan_impl(ctx, req, writer);
        } catch (const std::exception &e) {
            spdlog::error("ExecutePlan threw: {}", e.what());
            return {grpc::StatusCode::INTERNAL, e.what()};
        }
    }

    grpc::Status SparkConnectServiceImpl::execute_plan_impl(grpc::ServerContext *ctx,
                                                      const sc::ExecutePlanRequest *req,
                                                      grpc::ServerWriter<sc::ExecutePlanResponse> *writer) {
        auto s = session_manager_.resolve(*req);
        if (!s.ok()) {
            return util::to_grpc(s.status());
        }
        auto &sess = **s;

        const std::string op_id = req->has_operation_id() ? req->operation_id() : util::generate_uuid_v4();

        spdlog::debug("ExecutePlanRequest:\n{}", util::format_plan(req->plan()));

        if (!req->has_plan() || !req->plan().has_root()) {
            return grpc::Status::OK;
        }

        auto &root = req->plan().root();

        AstBuilder ast_builder;

        auto ast = ast_builder.build(root);;
        if (!ast.ok()) {
            spdlog::error("building ast failed: {}", ast.status().ToString());
            return util::to_grpc(ast.status());
        }
        spdlog::debug("built ast");


        /// CONVERSION STEP BEGIN

        //
        // plume::parser::ConverterConfig converter_config{};
        //
        // PlumeBuilder builder(std::move(ast), ast_builder.get_resolved_data_sources(), converter_config);
        // auto plan_res = builder.build();
        // if (!plan_res.ok()) {
        //     spdlog::error("Building plume physical plan failed: {}", plan_res.status().ToString());
        //     return util::to_grpc(plan_res.status());
        // }
        //
        //
        // plume::parser::CompiledComposition cc;
        // cc.plan = *std::move(plan_res);
        //
        // spdlog::debug("Plume Physical Plan:\n{}", cc.plan->ToString());
        //
        // // ?
        //
        // duckdb::DuckDB db(nullptr);
        // duckdb::Connection con(db);
        //
        //
        // auto comp_res = plume::dandelion::BuildDandelionComposition(con, *cc.plan, "my_comp", converter_config);
        // if (comp_res.is_error()) {
        //     spdlog::error("Building Dandelion Composition failed: {}", comp_res.error_msg());
        //     return {grpc::StatusCode::INTERNAL, comp_res.error_msg()};
        // }
        //
        // spdlog::debug("Composition:\n{}", comp_res.unwrap().dsl);
        //
        // auto body = plume::dandelion::InvocationBody(comp_res.unwrap(), false);
        //
        // if (body.is_error()) {
        //     spdlog::error("Building Body failed: {}", body.error_msg());
        //     return {grpc::StatusCode::INTERNAL, body.error_msg()};
        // }
        //
        // auto resp = run_on_dandelion(body.unwrap(), "http://localhost:9090/hot/matmul", std::chrono::seconds(60));
        // if (!resp.ok()) {
        //     spdlog::error("run on dandelion failed: {}", resp.status().ToString());
        //     return util::to_grpc(resp.status());
        // }
        //
        // auto text = plume::dandelion::ParseAndRenderResponseBody(*resp, cc.plan->output_schema);
        // //spdlog::info("result:\n{}", text.is_ok() ? text.unwrap() : text.error_msg());


        /// CONVERSION STEP END


        // auto sets = plume::dandelion::ParseResponseBody(*resp);
        // if (sets.is_error()) return {grpc::StatusCode::INTERNAL, sets.error_msg()};
        // auto owned_sets = std::move(sets).unwrap();                      // must be mutable: ImportBlockChunks swizzles
        // auto batches_res = to_record_batches(con, owned_sets, cc.plan->output_schema);
        // if (!batches_res.ok()) return {grpc::StatusCode::INTERNAL, batches_res.status().ToString()};
        // auto batches = *batches_res;



        auto make_response = [&] {
            sc::ExecutePlanResponse r;
            r.set_session_id(sess.session_id);
            r.set_server_side_session_id(sess.server_session_id);
            r.set_operation_id(op_id);
            r.set_response_id(util::generate_uuid_v4());
            return r;
        };


        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        auto dummy_batch = util::make_range_batch(20);
        if (!dummy_batch.ok()) {
            spdlog::error("make_range_batch failed");
            return {grpc::StatusCode::INTERNAL, dummy_batch.status().ToString()};
        }
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
                spdlog::debug("ConfigOperation GET, keys:\n{}", util::msg_to_text(op.get()));
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
