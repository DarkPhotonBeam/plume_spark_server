#include "plume_spark/parser/translations.hpp"

#include "plume_spark/util/casts.hpp"

namespace plume_spark::parser {
    namespace pe = plume::expr;

    absl::StatusOr<plume::ColumnType> translate_col_type(const types::DataType &t) {
        switch (t.id()) {
            case types::TypeId::Null:
                break;
            case types::TypeId::Boolean:
                return plume::ColumnType{plume::TypeId::BOOLEAN, 0, 0};
            case types::TypeId::Int8:
                return plume::ColumnType{plume::TypeId::INT8, 0, 0};
            case types::TypeId::Int16:
                return plume::ColumnType{plume::TypeId::INT16, 0, 0};
            case types::TypeId::Int32:
                return plume::ColumnType{plume::TypeId::INT32, 0, 0};
            case types::TypeId::Int64:
                return plume::ColumnType{plume::TypeId::INT64, 0, 0};
            case types::TypeId::Float32:
                return plume::ColumnType{plume::TypeId::FLOAT, 0, 0};
            case types::TypeId::Float64:
                return plume::ColumnType{plume::TypeId::DOUBLE, 0, 0};
            case types::TypeId::Decimal:
                return plume::ColumnType{plume::TypeId::DECIMAL, t.get_precision(), t.get_scale()};
            case types::TypeId::String:
                return plume::ColumnType{plume::TypeId::VARCHAR, 0, 0};
            case types::TypeId::Date:
                return plume::ColumnType{plume::TypeId::DATE, 0, 0};
            case types::TypeId::Timestamp:
                // todo: is this conversion correct? what is TIME?
                return plume::ColumnType{plume::TypeId::TIME, 0, 0};
            case types::TypeId::TimestampNtz:
                // todo: is this conversion correct? what is TIME?
                return plume::ColumnType{plume::TypeId::TIME, 0, 0};
            case types::TypeId::Binary:
                return absl::UnimplementedError("no translation for type Binary");
            case types::TypeId::List:
                return absl::UnimplementedError("no translation for type List");
            case types::TypeId::Struct:
                return absl::UnimplementedError("no translation for type Struct");
            case types::TypeId::Map:
                return absl::UnimplementedError("no translation for type Map");
        }

        return absl::InternalError("unhandled type case");
    }

    absl::StatusOr<duckdb::Value> translate_value(const ast::Value &value, const types::DataType &type) {
        auto visitor = util::overloaded{
            [](std::monostate) -> absl::StatusOr<duckdb::Value> { return duckdb::Value(); },
            [](const bool v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::BOOLEAN(v); },
            [](const int8_t v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::TINYINT(v); },
            [](const int16_t v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::SMALLINT(v); },
            [](const int32_t v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::INTEGER(v); },
            [](const int64_t v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::BIGINT(v); },
            [](const float v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::FLOAT(v); },
            [](const double v) -> absl::StatusOr<duckdb::Value> { return duckdb::Value::DOUBLE(v); },
            [type](const ast::Decimal &v) -> absl::StatusOr<duckdb::Value> {
                duckdb::hugeint_t h(absl::Int128High64(v.unscaled), absl::Int128Low64(v.unscaled));
                return duckdb::Value::DECIMAL(h, type.get_precision(), type.get_scale());
            },
            [](const std::string &s) -> absl::StatusOr<duckdb::Value> {
                duckdb::Value str(s);
                return str;
            },
            [](const ast::Bytes &b) -> absl::StatusOr<duckdb::Value> {
                return absl::UnimplementedError("bytes value translation not implemented");
            },
            [](const ast::Date &b) -> absl::StatusOr<duckdb::Value> {
                duckdb::date_t d(b.days);
                return duckdb::Value::DATE(d);
            },
            [](const ast::Timestamp &t) -> absl::StatusOr<duckdb::Value> {
                duckdb::timestamp_t tt(t.micros);
                return duckdb::Value::TIMESTAMP(tt);
            },
            [type](const ast::ListValue &lv) -> absl::StatusOr<duckdb::Value> {
                duckdb::vector<duckdb::Value> vals;
                for (auto &item : lv.items) {
                    auto conv = translate_value(item, type);
                    if (!conv.ok()) return conv.status();
                    vals.push_back(*conv);
                }
                // todo: call LIST(child_type, vals), needed for empty lists, needs translator to duckdb types
                return duckdb::Value::LIST(vals);
            },
        };
        return std::visit(visitor, value.v);
    }

    absl::StatusOr<duckdb::ExpressionType> translate_binop(ast::BinOp op) {
        switch (op) {
            case ast::BinOp::Eq:
                return duckdb::ExpressionType::COMPARE_EQUAL;
            case ast::BinOp::Lt:
                return duckdb::ExpressionType::COMPARE_LESSTHAN;
            case ast::BinOp::Lte:
                return duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
            case ast::BinOp::Gt:
                return duckdb::ExpressionType::COMPARE_GREATERTHAN;
            case ast::BinOp::Gte:
                return duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
            case ast::BinOp::NullSafeEq:
                // todo: find out if this has some quirks
                return duckdb::ExpressionType::COMPARE_EQUAL;
            case ast::BinOp::Add:
                return absl::UnimplementedError("binop Add not implemented");
            case ast::BinOp::Sub:
                return absl::UnimplementedError("binop Sub not implemented");
            case ast::BinOp::Mul:
                return absl::UnimplementedError("binop Mul not implemented");
            case ast::BinOp::Div:
                return absl::UnimplementedError("binop Div not implemented");
            case ast::BinOp::Mod:
                return absl::UnimplementedError("binop Mod not implemented");
            case ast::BinOp::And:
                return duckdb::ExpressionType::CONJUNCTION_AND;
            case ast::BinOp::Or:
                return duckdb::ExpressionType::CONJUNCTION_OR;
        }
    }

    absl::StatusOr<pe::ExprNode> translate_expr(const ast::Expression &expr, const std::map<ast::ColumnId, uint32_t> &pos) {
        auto t = translate_col_type(expr.type);
        if (!t.ok()) return t.status();

        auto visitor = util::overloaded{
            [&](const ast::Binary &b) -> absl::StatusOr<pe::ExprNode> {
                auto lhs = translate_expr(*b.lhs, pos);
                if (!lhs.ok()) return lhs.status();
                auto rhs = translate_expr(*b.rhs, pos);
                if (!rhs.ok()) return rhs.status();

                std::vector<pe::ExprNode> children{
                    *lhs,
                    *rhs,
                };


                return absl::UnimplementedError("expr binary not implemented");
            },
            [](const ast::Unary &) -> absl::StatusOr<pe::ExprNode> {
                return absl::UnimplementedError("expr unary not implemented");
            },
            [&](const ast::Cast &c) -> absl::StatusOr<pe::ExprNode> {
                auto child = translate_expr(*c.expr, pos);
                if (!child.ok()) return child.status();
                return pe::ExprNode::Cast(*child, *t, false);
            },
            [&](const ast::Reference &r) -> absl::StatusOr<pe::ExprNode> {
                if (auto it = pos.find(r.id); it != pos.end()) {
                    return pe::ExprNode::Reference(it->second, *t);
                }
                return absl::NotFoundError("didn't find mapping for ColumnId " + std::to_string(r.id.value));
            },
            [&](const ast::Literal &l) -> absl::StatusOr<pe::ExprNode> {
                auto val = translate_value(l.value, expr.type);
                if (!val.ok()) return val.status();
                return pe::ExprNode::Constant(*val, *t);
            },
        };
        return std::visit(visitor, expr.node);
    }
}
