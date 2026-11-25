#include <plume_spark/catalog/parquet.hpp>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <arrow/type.h>
#include <parquet/arrow/schema.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>

#include "plume/catalog/remote_resolver.hpp"
#include "plume_spark/ast/data_type.hpp"

namespace plume_spark::catalog {
    namespace pc = plume::catalog;
    namespace ast = plume_spark::ast;

    namespace {
        absl::StatusOr<std::shared_ptr<pc::RemoteParquetDataSource> >
    resolve_with_plume(std::string name, std::vector<std::string> urls, size_t threads = 8) {
            pc::RemoteResolver resolver(pc::AutoFetcher(), threads); // 1
            auto src = std::static_pointer_cast<pc::RemoteParquetDataSource>(
                pc::CreateRemoteParquetSource(std::move(name), std::move(urls))); // 2

            if (auto r = src->Resolve(resolver); r.is_error()) // 3: queue jobs
                return absl::InvalidArgumentError(r.error_msg());
            if (auto r = resolver.Execute(); r.is_error()) // 4: run them
                return absl::InvalidArgumentError(r.error_msg());
            return src;
        }

        absl::StatusOr<std::shared_ptr<arrow::Schema> > arrow_schema_from_footer(const std::vector<uint8_t> &bytes) {
            if (bytes.size() < 8) return absl::InvalidArgumentError("footer too short");
            std::shared_ptr<parquet::FileMetaData> md;
            try {
                md = parquet::FileMetaData::Make(bytes.data(), static_cast<int64_t>(bytes.size() - 8)); // 5
            } catch (const parquet::ParquetException &e) {
                // the low-level parquet:: API throws
                return absl::InvalidArgumentError(e.what());
            }
            std::shared_ptr<arrow::Schema> schema;
            auto st = parquet::arrow::FromParquetSchema(md->schema(), parquet::default_arrow_reader_properties(),
                                                        md->key_value_metadata(), &schema); // 6
            if (!st.ok()) return absl::InvalidArgumentError(st.ToString());
            return schema;
        }
    }

    absl::StatusOr<ast::DataType> to_ast_type(const arrow::DataType &t) {
        switch (t.id()) {
            case arrow::Type::DICTIONARY: // pyarrow-written strings: use the value type
                return to_ast_type(*static_cast<const arrow::DictionaryType &>(t).value_type());
            case arrow::Type::NA: return ast::DataType::null();
            case arrow::Type::BOOL: return ast::DataType::boolean();
            case arrow::Type::INT8: return ast::DataType::int8();
            case arrow::Type::INT32: return ast::DataType::int32();
            case arrow::Type::INT64: return ast::DataType::int64();
            case arrow::Type::FLOAT: return ast::DataType::float32();
            case arrow::Type::DOUBLE: return ast::DataType::float64();
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING: return ast::DataType::string();
            case arrow::Type::DECIMAL128: {
                const auto &d = static_cast<const arrow::Decimal128Type &>(t);
                return ast::DataType::decimal(static_cast<uint8_t>(d.precision()), static_cast<uint8_t>(d.scale()));
            }
            case arrow::Type::TIMESTAMP: return ast::DataType::int64(); // todo: replace with actual timestamp type
            // TIMESTAMP: tz set -> Spark TimestampType, no tz -> TimestampNTZType (needs DataType factories)
            default:
                return absl::UnimplementedError(absl::StrCat("unsupported column type ", t.ToString()));
        }
    }

    absl::StatusOr<ResolvedParquet> resolve_parquet(std::string name, std::vector<std::string> urls) {
        auto src = resolve_with_plume(std::move(name), std::move(urls));
        if (!src.ok()) return src.status();

        auto schema = arrow_schema_from_footer((*src)->footer_bytes.at(0));
        if (!schema.ok()) return schema.status();

        return ResolvedParquet{*std::move(src), *std::move(schema)}; // 8: keep source
    }
}
