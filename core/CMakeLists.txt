find_package(Parquet CONFIG REQUIRED)

include(cmake/PlumeDeps.cmake)

add_library(plume_spark_core
        src/service.cpp
        src/session_manager.cpp
        src/config_defaults.cpp
        src/ast_builder.cpp
        src/parser/builder.cpp
        src/parser/translations.cpp
        src/util/uuid.cpp
        src/util/plan_format.cpp
        src/ast/relation.cpp
        src/catalog/parquet.cpp
        src/catalog/csv.cpp
        src/catalog/data_source.cpp
)
add_library(plume_spark::core ALIAS plume_spark_core)

target_include_directories(plume_spark_core
        PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)

target_link_libraries(plume_spark_core PUBLIC plume_spark::proto absl::status absl::statusor PRIVATE Arrow::arrow_static spdlog::spdlog Parquet::parquet_static plume cpr::cpr)