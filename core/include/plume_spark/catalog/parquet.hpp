#pragma once
#include <absl/status/statusor.h>
#include <parquet/arrow/schema.h>
#include <plume/catalog/parquet.hpp>
#include "plume_spark/types/data_type.hpp"

namespace plume_spark::catalog {

    arrow::Result<std::shared_ptr<arrow::Schema>> read_parquet_schema(const std::shared_ptr<arrow::io::RandomAccessFile> &file);

    struct ResolvedParquet {
        std::shared_ptr<plume::catalog::RemoteParquetDataSource> source; // keep for building the plume plan
        std::shared_ptr<arrow::Schema> schema; // logical schema (from the first file)
    };

    absl::StatusOr<types::DataType> to_ast_type(const arrow::DataType &t);

    absl::StatusOr<ResolvedParquet> resolve_parquet(std::string name, std::vector<std::string> urls);
}
