#pragma once
#include <cstdint>
#include <memory>
#include <variant>
#include <vector>

namespace plume_spark::ast {

    enum class TypeId { Null, Boolean, Int8, Int16, Int32, Int64, Float32, Float64, Decimal, String, Binary, Date, Timestamp, TimestampNtz, List, Struct, Map };

    struct DecimalInfo { uint8_t precision, scale; };
    struct ListInfo;
    struct StructInfo;
    struct MapInfo;

    class DataType {
    public:
        static DataType int32() { return DataType(TypeId::Int32); }
        static DataType int64() { return DataType(TypeId::Int64); }
        static DataType int8() { return DataType(TypeId::Int8); }
        static DataType float32() { return DataType(TypeId::Float32); }
        static DataType float64() { return DataType(TypeId::Float64); }
        static DataType decimal(const uint8_t p, const uint8_t s) { return DataType(TypeId::Decimal, DecimalInfo{p, s}); }
        static DataType string() { return DataType(TypeId::String); }
        static DataType null() { return DataType(TypeId::Null); }
        static DataType boolean() { return DataType(TypeId::Boolean); }

        [[nodiscard]] TypeId id() const { return id_; }

    private:
        using Info = std::variant<
            std::monostate,
            DecimalInfo,
            std::shared_ptr<const ListInfo>,
            std::shared_ptr<const StructInfo>,
            std::shared_ptr<const MapInfo>
        >;
        explicit DataType(TypeId id, Info info = {}) : id_(id), info_(std::move(info)) {}
        TypeId id_;
        Info info_;
    };

    struct Field      { std::string name; DataType type; bool nullable = true; };
    struct ListInfo   { DataType element; bool contains_null; };
    struct StructInfo { std::vector<Field> fields; };
    struct MapInfo    { DataType key, value; bool value_contains_null; };

}
