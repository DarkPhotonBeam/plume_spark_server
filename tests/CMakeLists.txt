find_package(GTest CONFIG REQUIRED)

add_executable(plume_spark_tests src/service_test.cpp)
target_link_libraries(plume_spark_tests PRIVATE plume_spark::core GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(plume_spark_tests)