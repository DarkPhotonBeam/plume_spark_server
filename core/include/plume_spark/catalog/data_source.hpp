#pragma once
#include <arrow/type.h>
#include <vector>

#include "plume/catalog/catalog.hpp"

namespace plume_spark::catalog {
    enum class Format { Parquet, Csv, LocalTable };
    using Schema = std::vector<ast::Field>;

    class DataSource {
        struct Key { explicit Key() = default; };
    public:
        DataSource(Key, Format f, Schema s, std::shared_ptr<plume::catalog::DataSource> p)
            : format_(f), schema_(std::move(s)), plume_src_(std::move(p)) {}

        static absl::StatusOr<std::shared_ptr<const DataSource>> parquet(std::vector<std::string> urls);
        static absl::StatusOr<std::shared_ptr<const DataSource>> csv(std::vector<std::string> urls);

        [[nodiscard]] Format format() const { return format_; }
        [[nodiscard]] const Schema &schema() const { return schema_; }

        [[nodiscard]] const std::shared_ptr<plume::catalog::DataSource> &plume_src() const { return plume_src_; }

    private:
        Format format_;
        Schema schema_;
        std::shared_ptr<plume::catalog::DataSource> plume_src_;
    };
}
