add_executable(plume_spark_server src/main.cpp)
target_link_libraries(plume_spark_server PRIVATE plume_spark::core spdlog::spdlog)