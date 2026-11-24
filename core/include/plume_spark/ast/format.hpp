#pragma once
#include <string>

#include "plume_spark/ast/base.hpp"

namespace plume_spark::ast {
    std::string to_string(TypeId id);
    std::string to_string(const DataType &t);
    std::string to_string(const Value &v);
    std::string to_string(const Literal &l);
    std::string to_string(const Expr *e);   // one line, null-safe
    std::string to_string(const Rel *r);    // multi-line tree, null-safe

    inline std::string to_string(const ExprPtr &e) { return to_string(e.get()); }
    inline std::string to_string(const RelPtr &r) { return to_string(r.get()); }

    // Found by fmt (and therefore spdlog) through ADL: spdlog::debug("{}", *rel)
    inline std::string format_as(TypeId id) { return to_string(id); }
    inline std::string format_as(const DataType &t) { return to_string(t); }
    inline std::string format_as(const Literal &l) { return to_string(l); }
    inline std::string format_as(const Expr &e) { return to_string(&e); }
    inline std::string format_as(const Rel &r) { return to_string(&r); }
}
