add_library(plume_spark_core src/service.cpp)
add_library(plume_spark::core ALIAS plume_spark_core)

target_include_directories(plume_spark_core
        PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        PRIVATE src
)

target_link_libraries(plume_spark_core PUBLIC plume_spark::proto PRIVATE Arrow::arrow_static spdlog::spdlog)