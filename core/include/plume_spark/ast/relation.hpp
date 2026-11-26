#pragma once
#include <memory>
#include <variant>
#include <string>
#include <vector>

#include "plume_spark/ast/common.hpp"
#include "plume_spark/ast/expression.hpp"
#include "plume_spark/catalog/data_source.hpp"

namespace plume_spark::ast {

    struct NamedExpr {
        ColumnId id;
        std::string name;
        ExprPtr expr;
    };

    struct Relation;
    using RelPtr = std::unique_ptr<Relation>;

    struct Read {
        std::shared_ptr<const catalog::DataSource> data_source;
        std::vector<OutputColumn> output;
    };

    struct Filter {
        RelPtr input;
        ExprPtr condition;
    };

    struct Project {
        RelPtr input;
        std::vector<NamedExpr> columns;
    };

    struct Relation {
        std::variant<Read, Filter, Project> node;
    };



    std::vector<OutputColumn> output_of(const Relation &r);
}
