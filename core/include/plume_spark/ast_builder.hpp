#pragma once
#include <absl/status/statusor.h>
#include <plume_spark/ast/expression.hpp>
#include <plume_spark/ast/relation.hpp>

#include "spark/connect/relations.pb.h"


namespace plume_spark {
    struct Scope {
        std::span<const ast::OutputColumn> cols;

        absl::StatusOr<const ast::OutputColumn *> lookup(std::string_view name) const {
            const ast::OutputColumn *found = nullptr;
            for (const auto &c : cols) {
                if (!absl::EqualsIgnoreCase(c.name, name)) continue;
                if (found) return absl::InvalidArgumentError(absl::StrCat("ambiguous column '", name, "'"));
                found = &c;
            }
            if (!found) return absl::InvalidArgumentError(absl::StrCat("column '", name, "' not found"));
            return found;
        }
    };

    class AstBuilder {
    public:
        [[nodiscard]] absl::StatusOr<ast::RelPtr> build(const ::spark::connect::Relation &root);

    private:
        absl::StatusOr<ast::RelPtr> build_rel(const ::spark::connect::Relation &rel);

        absl::StatusOr<ast::ExprPtr> build_expr(const ::spark::connect::Expression &exp, const Scope &scope);
        static absl::StatusOr<ast::ExprPtr> build_lit(const ::spark::connect::Expression_Literal &lit);

    };
}
