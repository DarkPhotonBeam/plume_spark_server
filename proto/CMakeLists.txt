set(PROTO_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(GLOB SPARK_PROTO_FILES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/spark/connect/*.proto")

add_library(plume_spark_proto STATIC ${SPARK_PROTO_FILES})
add_library(plume_spark::proto ALIAS plume_spark_proto)
set_target_properties(plume_spark_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(plume_spark_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(plume_spark_proto PUBLIC "$<BUILD_INTERFACE:${PROTO_OUT_DIR}>")

protobuf_generate(
        TARGET plume_spark_proto
        LANGUAGE cpp
        IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
        PROTOC_OUT_DIR ${PROTO_OUT_DIR}
)

protobuf_generate(
        TARGET plume_spark_proto
        LANGUAGE grpc
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
        PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
        IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
        PROTOC_OUT_DIR ${PROTO_OUT_DIR}
)