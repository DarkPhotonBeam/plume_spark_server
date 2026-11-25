#pragma once
#include <plume_spark/ast/base.hpp>

#include "spark/connect/relations.pb.h"

namespace plume_spark {
    struct ResolvedDataSource {
        std::string name;
        std::string format;
        std::vector<ast::Attribute> columns;
    };

    class AstBuilder {
    public:
        ast::RelPtr build(const ::spark::connect::Relation &root);

    private:
        ast::RelPtr build_rel_(const ::spark::connect::Relation &rel);
        ast::ExprPtr build_expr_(const ::spark::connect::Expression &exp);
        ast::Literal build_lit_(const ::spark::connect::Expression_Literal &lit);

        absl::StatusOr<const ResolvedDataSource&> add_datasource_(const std::string &name, const std::string &format, const arrow::Schema& schema);
        std::optional<ast::Attribute> find_attribute(const std::string &name);

        std::vector<ResolvedDataSource> resolved_data_sources_;
        std::unordered_map<std::string, std::pair<size_t, size_t>> name_map_;
        ast::ColumnIdGenerator id_generator_;

    };
}
