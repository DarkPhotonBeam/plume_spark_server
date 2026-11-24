#include <plume_spark/service.hpp>
#include <grpcpp/grpcpp.h>

namespace plume_spark {
    namespace sc = ::spark::connect;

    grpc::Status SparkConnectServiceImpl::ExecutePlan(grpc::ServerContext *,
                                                      const sc::ExecutePlanRequest *,
                                                      grpc::ServerWriter<sc::ExecutePlanResponse> *) {
        return {grpc::StatusCode::UNIMPLEMENTED, "ExecutePlan not implemented yet"};
    }

    grpc::Status SparkConnectServiceImpl::AnalyzePlan(grpc::ServerContext *,
                                                      const sc::AnalyzePlanRequest *,
                                                      sc::AnalyzePlanResponse *) {
        return {grpc::StatusCode::UNIMPLEMENTED, "AnalyzePlan not implemented yet"};
    }

    grpc::Status SparkConnectServiceImpl::Config(grpc::ServerContext *,
                                                 const sc::ConfigRequest *,
                                                 sc::ConfigResponse *) {
        return {grpc::StatusCode::UNIMPLEMENTED, "Config not implemented yet"};
    }

    grpc::Status SparkConnectServiceImpl::ReleaseExecute(grpc::ServerContext *,
                                                         const sc::ReleaseExecuteRequest *,
                                                         sc::ReleaseExecuteResponse *) {
        return grpc::Status::OK;
    }
} // namespace plume_spark
