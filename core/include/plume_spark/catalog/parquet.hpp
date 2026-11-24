#pragma once
#include <parquet/arrow/schema.h>

namespace plume_spark::catalog {

    arrow::Result<std::shared_ptr<arrow::Schema>> read_parquet_schema(const std::shared_ptr<arrow::io::RandomAccessFile> &file);

}
