# Builds DuckDB (pinned + patched like plume expects) and plume's core library
# (with the compile layer) as part of this CMake project. Provides target `plume`.
include(FetchContent)
set(PLUME_DIR ${CMAKE_SOURCE_DIR}/third_party/plume)   # git submodule, branch duckdb_extension

# --- DuckDB: same tag and patches as plume's scripts/setup_duckdb.sh -------------------------
file(GLOB PLUME_DUCKDB_PATCHES ${PLUME_DIR}/cmake/duckdb_patches/*.patch)
FetchContent_Declare(duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG        v2.0.0
        GIT_SHALLOW    TRUE
        PATCH_COMMAND  git apply ${PLUME_DUCKDB_PATCHES}
        UPDATE_DISCONNECTED TRUE)          # don't re-fetch / re-patch on every configure
set(BUILD_SHELL      OFF CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS  OFF CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ENABLE_SANITIZER OFF CACHE BOOL "" FORCE)
set(ENABLE_UBSAN     OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(duckdb)  # -> targets duckdb_static, core_functions_extension, ...

# Reuse plume's DuckDB.cmake for its include-dir list (DuckDB third_party dirs such as fmt are
# needed), but link the DuckDB targets built above instead of prebuilt archives.
set(DUCKDB_DIR ${duckdb_SOURCE_DIR} CACHE PATH "" FORCE)
set(DUCKDB_STATIC_LIB duckdb_static CACHE STRING "" FORCE)
set(DUCKDB_CORE_FUNCTIONS_LIB core_functions_extension CACHE STRING "" FORCE)
include(${PLUME_DIR}/cmake/DuckDB.cmake)   # defines INTERFACE target plume_duckdb

# --- plume's other dependencies (vcpkg: cpr, nlohmann-json, protobuf) ------------------------
find_package(cpr CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
include(${PLUME_DIR}/cmake/Protobuf.cmake)

# --- plume core ------------------------------------------------------------------------------
set(PLUME_INCLUDES OFF)             # we provide the dependencies (plume_duckdb, cpr, json) above
set(PLUME_WITH_COMPILE_LAYER ON)    # catalog, physical plan, composition, Dandelion API
set(PLUME_BUILD_FUNCTIONS OFF)
set(PLUME_BUILD_CLIENT OFF)
set(PLUME_BUILD_BENCHMARKS OFF)
set(PLUME_WITH_TESTS OFF)
add_subdirectory(${PLUME_DIR} ${CMAKE_BINARY_DIR}/plume EXCLUDE_FROM_ALL)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/plume/core/proto)   # same workaround as the DuckDB extension