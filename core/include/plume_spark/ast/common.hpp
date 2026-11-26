#pragma once
#include <cstdint>
#include <compare>
#include <functional>
#include <string>

#include "plume_spark/types/data_type.hpp"

namespace plume_spark::ast {
    struct ColumnId {
        uint64_t value;

        auto operator<=>(const ColumnId &) const = default;
    };

    struct OutputColumn {
        ColumnId id;
        std::string name;
        types::DataType type;
        bool nullable;
    };
}

template<>
struct std::hash<plume_spark::ast::ColumnId> {
    size_t operator()(const plume_spark::ast::ColumnId &c) const noexcept {
        return std::hash<uint64_t>{}(c.value);
    }
};
