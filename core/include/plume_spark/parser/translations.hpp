#pragma once
#include <absl/status/statusor.h>
#include <plume/expression/expression.hpp>

#include "plume_spark/ast/expression.hpp"

namespace plume_spark::parser {

    absl::StatusOr<plume::ColumnType> translate_col_type(const types::DataType &t);

    absl::StatusOr<plume::expr::ExprNode> translate_expr(const ast::Expression &expr, const std::map<ast::ColumnId, uint32_t> &pos);

}
