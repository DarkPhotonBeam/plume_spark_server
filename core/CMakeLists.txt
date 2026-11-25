find_package(Parquet CONFIG REQUIRED)

include(cmake/PlumeDeps.cmake)

add_library(plume_spark_core
        src/service.cpp
        src/session_manager.cpp
        src/config_defaults.cpp
        src/ast_builder.cpp
        src/plume_builder.cpp
        src/util/uuid.cpp
        src/ast/format.cpp
        src/catalog/parquet.cpp
)
add_library(plume_spark::core ALIAS plume_spark_core)

target_include_directories(plume_spark_core
        PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        PRIVATE src
)

target_link_libraries(plume_spark_core PUBLIC plume_spark::proto absl::status absl::statusor PRIVATE Arrow::arrow_static spdlog::spdlog Parquet::parquet_static plume cpr::cpr)