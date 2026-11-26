#pragma once
#include <spark/connect/base.grpc.pb.h>

#include "plume_spark/session_manager.hpp"

namespace plume_spark {
    class SparkConnectServiceImpl final : public ::spark::connect::SparkConnectService::Service {
    public:
        grpc::Status ExecutePlan(grpc::ServerContext *ctx,
                                 const ::spark::connect::ExecutePlanRequest *req,
                                 grpc::ServerWriter<::spark::connect::ExecutePlanResponse> *writer) override;

        grpc::Status AnalyzePlan(grpc::ServerContext *ctx,
                                 const ::spark::connect::AnalyzePlanRequest *req,
                                 ::spark::connect::AnalyzePlanResponse *resp) override;

        grpc::Status Config(grpc::ServerContext *ctx,
                            const ::spark::connect::ConfigRequest *req,
                            ::spark::connect::ConfigResponse *resp) override;

        grpc::Status ReleaseExecute(grpc::ServerContext *ctx,
                                    const ::spark::connect::ReleaseExecuteRequest *req,
                                    ::spark::connect::ReleaseExecuteResponse *resp) override;

    private:
        grpc::Status execute_plan_impl(grpc::ServerContext *ctx,
                                                      const ::spark::connect::ExecutePlanRequest *req,
                                                      grpc::ServerWriter<::spark::connect::ExecutePlanResponse> *writer);

        SessionManager session_manager_;
    };
} // namespace plume_spark
