#pragma once
#include <cstdint>
#include <memory>
#include <variant>
#include <vector>
#include <string>
#include <absl/numeric/int128.h>

#include "plume_spark/types/data_type.hpp"
#include "plume_spark/ast/common.hpp"

namespace plume_spark::ast {

    // literal values
    struct Date      { int32_t days; };       // days since 1970-01-01
    struct Timestamp { int64_t micros; };     // microseconds since the epoch, UTC for TIMESTAMP
    struct Decimal   { absl::int128 unscaled; };  // value = unscaled / 10^scale; the scale is in the DataType
    struct Bytes     { std::string data; };   // binary, kept distinct from text
    struct Value;
    struct ListValue { std::vector<Value> items; };   // std::vector of an incomplete type is fine since C++17

    struct Value {
        std::variant<std::monostate,
                     bool, int8_t, int16_t, int32_t, int64_t, float, double,
                     Decimal, std::string, Bytes, Date, Timestamp, ListValue> v;
    };

    struct Expression;
    using ExprPtr = std::unique_ptr<Expression>;

    enum class UnOp { Not, Negative, IsNull, IsNotNull };
    struct Unary {
        UnOp op;
        ExprPtr expr;
    };

    enum class BinOp { Eq, Lt, Lte, Gt, Gte, NullSafeEq, Add, Sub, Mul, Div, Mod, And, Or };
    struct Binary {
        BinOp op;
        ExprPtr lhs;
        ExprPtr rhs;
    };

    struct Reference {
        ColumnId id;
    };

    struct Cast {
        ExprPtr expr;
    };

    struct Literal {
        Value value;
        [[nodiscard]] bool is_null() const { return std::holds_alternative<std::monostate>(value.v); }
    };

    using ExprNodeType = std::variant<Unary, Binary, Reference, Literal, Cast>;

    struct Expression {
        plume_spark::types::DataType type;
        bool nullable;
        ExprNodeType node;
    };

    // factories:
    template <class T>
    ExprPtr expr(T t, types::DataType type, bool nullable = true) {
        return std::make_unique<Expression>(Expression{std::move(type), nullable, std::move(t)});
    }

    inline ExprPtr lit_i32(int32_t v) {
        return expr(Literal{Value{v}}, types::DataType::int32());
    }
    inline ExprPtr lit_i64(int64_t v) {
        return expr(Literal{Value{v}}, types::DataType::int64());
    }
    inline ExprPtr lit_string(std::string v) {
        return expr(Literal{Value{v}}, types::DataType::string());
    }
    inline ExprPtr lit_f64(double v) {
        return expr(Literal{Value{v}}, types::DataType::float64());
    }
    inline ExprPtr lit_null() {
        return expr(Literal{Value{}}, types::DataType::null());
    }
}
