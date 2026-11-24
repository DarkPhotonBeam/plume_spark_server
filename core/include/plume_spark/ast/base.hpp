#pragma once
#include <cstdint>
#include <memory>

#include "data_type.hpp"
#include "plume_spark/util/casts.hpp"
#include "spark/connect/types.pb.h"

namespace plume_spark::ast {


    struct Expr;
    using ExprPtr = std::unique_ptr<Expr>;

    struct Rel;
    using RelPtr = std::unique_ptr<Rel>;
    using WeakRelPtr = std::weak_ptr<Rel>;

    struct Date      { int32_t days; };       // days since 1970-01-01
    struct Timestamp { int64_t micros; };     // microseconds since the epoch, UTC for TIMESTAMP
    struct Decimal   { absl::int128 unscaled; };  // value = unscaled / 10^scale; the scale is in the DataType
    struct Bytes     { std::string data; };   // binary, kept distinct from text
    struct Value;
    struct ListValue { std::vector<Value> items; };   // std::vector of an incomplete type is fine since C++17

    struct Value {
        std::variant<std::monostate,          // NULL (the type is in Literal::type)
                     bool, int8_t, int16_t, int32_t, int64_t, float, double,
                     Decimal, std::string, Bytes, Date, Timestamp, ListValue> v;
    };

    struct Literal {
        DataType type;
        Value value;
        [[nodiscard]] bool is_null() const { return std::holds_alternative<std::monostate>(value.v); }
    };

    inline Literal int32_lit(int32_t v) { return {DataType::int32(), Value{v}}; }
    inline Literal int64_lit(int64_t v) { return {DataType::int64(), Value{v}}; }
    inline Literal float32_lit(float v) { return {DataType::float32(), Value{v}}; }
    inline Literal float64_lit(double v) { return {DataType::float64(), Value{v}}; }
    inline Literal string_lit(std::string v) { return {DataType::string(), Value{v}}; }
    inline Literal null_lit() { return {DataType::null(), Value{}}; }

    enum class BinOp { Add, Eq, And, Gte, Gt, Lte, Lt, Neq, Mul };

    constexpr std::string_view to_string(const BinOp op) {
        switch (op) {
            case BinOp::Add:
                return "Add";
            case BinOp::Eq:
                return "Eq";
            case BinOp::And:
                return "And";
        }
        return "?";
    }

    constexpr std::string_view format_as(const BinOp op) { return to_string(op); }

    struct Binary {
        BinOp op;
        ExprPtr lhs, rhs;
    };

    struct ColumnId {
        uint64_t value;

        auto operator<=>(const ColumnId &) const = default;
    };

    struct Attribute {
        ColumnId id;
        std::string name;
        spark::connect::DataType type;
        bool nullable;
    };

    struct UnresolvedAttribute {
        std::string name;
        bool is_metadata_col;
    };

    struct ColumnRef {
        Attribute attr;
    };

    struct Expr {
        std::variant<Literal, ColumnRef, Binary, UnresolvedAttribute> node;
    };

    struct NamedExpr {
        ExprPtr expr;
        Attribute out;
    };

    struct Filter {
        RelPtr input;
        ExprPtr condition;
    };

    struct Read {
        std::string format;
        std::vector<std::string> paths;
        std::vector<Attribute> output;
    };

    struct Project {
        RelPtr input;
        std::vector<NamedExpr> columns;
    };

    struct UnresolvedProject {
        RelPtr input;
        std::vector<ExprPtr> columns;
    };

    struct Rel {
        std::variant<Filter, Read, Project, UnresolvedProject> node;
    };

    class ColumnIdGenerator {
        uint64_t next_ = 0;
    public:
        ColumnId fresh() { return ColumnId{next_++}; }
    };
}
