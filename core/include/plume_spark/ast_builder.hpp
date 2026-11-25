#pragma once
#include <plume_spark/ast/base.hpp>

#include "spark/connect/relations.pb.h"

namespace plume::catalog {
    struct RemoteParquetDataSource;
}

namespace plume_spark {
    struct ResolvedDataSource {
        std::string name;
        std::string format;
        std::vector<ast::Attribute> columns;
        std::shared_ptr<plume::catalog::RemoteParquetDataSource> plume_src;
        std::shared_ptr<arrow::Schema> arrow_schema;
    };

    class AstBuilder {
    public:
        [[nodiscard]] ast::RelPtr build(const ::spark::connect::Relation &root);
        const std::vector<ResolvedDataSource> &get_resolved_data_sources();

    private:
        ast::RelPtr build_rel(const ::spark::connect::Relation &rel);
        ast::ExprPtr build_expr(const ::spark::connect::Expression &exp);
        ast::Literal build_lit(const ::spark::connect::Expression_Literal &lit);

        absl::StatusOr<const ResolvedDataSource&> add_datasource_(const std::string &name, const std::string &format, std::shared_ptr<plume::catalog::RemoteParquetDataSource> plume_src, const arrow::Schema& schema);
        std::optional<ast::Attribute> find_attribute(const std::string &name);

        std::vector<ResolvedDataSource> resolved_data_sources_;
        std::unordered_map<std::string, std::pair<size_t, size_t>> name_map_;
        ast::ColumnIdGenerator id_generator_;

    };
}
