#include <plume_spark/ast/relation.hpp>

#include "plume_spark/util/casts.hpp"

namespace plume_spark::ast {
    std::vector<OutputColumn> output_of(const Relation &r) {
        auto visitor = plume_spark::util::overloaded{
            [](const Read &read) {
                return read.output;
            },
            [](const Project &proj) {
                std::vector<OutputColumn> cols;
                for (const auto &[id, name, expr] : proj.columns) {
                    cols.push_back(OutputColumn{
                        .id = id,
                        .name = name,
                        .type = expr->type,
                        .nullable = expr->nullable,
                    });
                }
                return cols;
            },
            [](const Filter &filt) {
                return output_of(*filt.input);
            }
        };

        return std::visit(visitor, r.node);
    }
}


