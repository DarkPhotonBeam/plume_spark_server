cmake_minimum_required(VERSION 3.25)
project(plume_spark_server C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

add_subdirectory(proto)
add_subdirectory(core)
add_subdirectory(server)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
