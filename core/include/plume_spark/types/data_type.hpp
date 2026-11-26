#pragma once
#include <cstdint>
#include <memory>
#include <variant>
#include <vector>
#include <string>
#include <type_traits>
#include <arrow/type.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>

namespace plume_spark::types {

    enum class TypeId { Null, Boolean, Int8, Int16, Int32, Int64, Float32, Float64, Decimal, String, Binary, Date, Timestamp, TimestampNtz, List, Struct, Map };

    struct DecimalInfo {
        uint8_t precision, scale;
        bool operator==(const DecimalInfo &) const = default;
    };
    struct ListInfo;
    struct StructInfo;
    struct MapInfo;

    class DataType {
    public:
        static DataType int32() { return DataType(TypeId::Int32); }
        static DataType int64() { return DataType(TypeId::Int64); }
        static DataType int8() { return DataType(TypeId::Int8); }
        static DataType int16() { return DataType(TypeId::Int16); }
        static DataType float32() { return DataType(TypeId::Float32); }
        static DataType float64() { return DataType(TypeId::Float64); }
        static DataType decimal(const uint8_t p, const uint8_t s) { return DataType(TypeId::Decimal, DecimalInfo{p, s}); }
        static DataType string() { return DataType(TypeId::String); }
        static DataType null() { return DataType(TypeId::Null); }
        static DataType boolean() { return DataType(TypeId::Boolean); }
        static DataType binary() { return DataType(TypeId::Binary); }
        static DataType date() { return DataType(TypeId::Date); }
        static DataType timestamp() { return DataType(TypeId::Timestamp); }
        static DataType timestamp_ntz() { return DataType(TypeId::TimestampNtz); }

        [[nodiscard]] bool is_numeric() const {
            switch (id()) {
                case TypeId::String:
                case TypeId::Binary:
                case TypeId::Date:
                case TypeId::Timestamp:
                case TypeId::TimestampNtz:
                case TypeId::List:
                case TypeId::Struct:
                case TypeId::Map:
                case TypeId::Null:
                case TypeId::Boolean:
                    return false;
                case TypeId::Int8:
                case TypeId::Int16:
                case TypeId::Int32:
                case TypeId::Int64:
                case TypeId::Float32:
                case TypeId::Float64:
                case TypeId::Decimal:
                    return true;
                default:
                    return false;
            }
        }

        static absl::StatusOr<DataType> from_arrow(const arrow::DataType &t) {
            switch (t.id()) {
                case arrow::Type::BOOL:
                    return boolean();
                case arrow::Type::INT8:
                    return int8();
                case arrow::Type::INT16:
                    return int16();
                case arrow::Type::INT32:
                    return int32();
                case arrow::Type::INT64:
                    return int64();
                case arrow::Type::FLOAT:
                    return float32();
                case arrow::Type::DOUBLE:
                    return float64();
                case arrow::Type::STRING:
                    return string();
                case arrow::Type::BINARY:
                    return binary();
                case arrow::Type::DATE32:
                case arrow::Type::DATE64:
                    return date();
                case arrow::Type::TIMESTAMP:
                    return timestamp();
                case arrow::Type::DICTIONARY:
                    return from_arrow(*dynamic_cast<const arrow::DictionaryType &>(t).value_type());
                default:
                    return absl::UnimplementedError(absl::StrCat("type '", t.ToString(), "' not implemented"));
            }
            return absl::UnimplementedError(absl::StrCat("type '", t.ToString(), "' not implemented"));
        }

        [[nodiscard]] TypeId id() const { return id_; }

        bool operator==(const DataType &other) const;

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

    struct Field {
        std::string name; DataType type;
        bool nullable = true;
        bool operator==(const Field &) const = default;
    };
    struct ListInfo {
        DataType element;
        bool contains_null{};
        bool operator==(const ListInfo &) const = default;
    };
    struct StructInfo {
        std::vector<Field> fields;
        bool operator==(const StructInfo &) const = default;
    };
    struct MapInfo {
        DataType key, value;
        bool value_contains_null{};
        bool operator==(const MapInfo &) const = default;
    };

    inline bool DataType::operator==(const DataType &other) const {
        if (id_ != other.id_ || info_.index() != other.info_.index()) return false;
        return std::visit([&](const auto &a) -> bool {
            using T = std::decay_t<decltype(a)>;
            const auto &b = std::get<T>(other.info_);   // same index => same alternative
            if constexpr (std::is_same_v<T, std::monostate>) {
                return true;                            // plain types: id_ already matched
            } else if constexpr (std::is_same_v<T, DecimalInfo>) {
                return a == b;                          // precision and scale
            } else {                                    // shared_ptr<const XInfo>
                return a == b || (a && b && *a == *b);  // same object, or equal contents
            }
        }, info_);
    }

}
