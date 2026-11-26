#include <iostream>
#include <grpcpp/server_builder.h>
#include <spdlog/cfg/env.h>
#include <spdlog/spdlog.h>
#include "plume_spark/service.hpp"


int main(int argc, char *argv[]) {
    // logger setup
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
    spdlog::cfg::load_env_levels();

    const std::string address = "0.0.0.0:15002";

    plume_spark::SparkConnectServiceImpl service;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.SetMaxReceiveMessageSize(128 * 1024 * 1024);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        spdlog::error("failed to bind {}", address);
        return 1;
    }
    spdlog::info("server listening on {}", address);

    server->Wait();

    return 0;
}
