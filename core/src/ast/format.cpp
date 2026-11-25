#include "plume_spark/ast/format.hpp"

#include <chrono>
#include <iterator>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>   // fmt::streamed, for absl::int128

namespace plume_spark::ast {
    namespace {
        using util::overloaded;

        template<class Range, class F>
        std::string join(const Range &r, F &&f, std::string_view sep = ", ") {
            std::string out;
            bool first = true;
            for (const auto &x : r) {
                if (!first) out += sep;
                first = false;
                out += f(x);
            }
            return out;
        }

        std::string_view symbol(BinOp op) {
            switch (op) {
                case BinOp::Add: return "+";
                case BinOp::Eq:  return "=";
                case BinOp::And: return "AND";
                case BinOp::Gt: return ">";
                case BinOp::Gte: return ">=";
                case BinOp::Lt: return "<";
                case BinOp::Lte: return "<=";
                case BinOp::Neq: return "!=";
                case BinOp::Mul: return "*";
            }
            return "?";
        }

        // "integer", "string", ... : the name of the set oneof field in spark.connect.DataType
        std::string spark_type_name(const spark::connect::DataType &t) {
            if (t.kind_case() == spark::connect::DataType::KIND_NOT_SET) return "?";
            const auto *f = t.GetDescriptor()->FindFieldByNumber(t.kind_case());
            return f ? std::string(f->name()) : "?";
        }

        std::string attr(const Attribute &a) { return fmt::format("{}#{}", a.name, a.id.value); }

        std::string named(const NamedExpr &n) {
            // A plain column passed through keeps its id: print just "a#0" instead of "a#0 AS a#0"
            if (n.expr) {
                if (const auto *c = std::get_if<ColumnRef>(&n.expr->node); c && c->attr.id == n.out.id)
                    return attr(n.out);
            }
            return fmt::format("{} AS {}", to_string(n.expr), attr(n.out));
        }

        struct NodeView {
            std::string line;
            std::vector<const Rel *> children;
        };

        NodeView describe(const Rel &r) {
            return std::visit(overloaded{
                [](const Filter &f) -> NodeView {
                    return {fmt::format("Filter {}", to_string(f.condition)), {f.input.get()}};
                },
                [](const Read &rd) -> NodeView {
                    auto cols = join(rd.output, [](const Attribute &a) {
                        return fmt::format("{}:{}", attr(a), a.type);
                    });
                    auto paths = join(rd.paths, [](const std::string &p) { return fmt::format("{:?}", p); });
                    return {fmt::format("Read {} [{}] paths=[{}]", rd.format, cols, paths), {}};
                },
                [](const Project &p) -> NodeView {
                    return {fmt::format("Project [{}]", join(p.columns, named)), {p.input.get()}};
                },
                [](const UnresolvedProject &p) -> NodeView {
                    auto cols = join(p.columns, [](const ExprPtr &e) { return to_string(e); });
                    return {fmt::format("'Project [{}]", cols), {p.input.get()}};
                },
            }, r.node);
        }

        // Spark explain() style:
        //   Project [a#0]
        //   +- Filter (a#0 = 1)
        //      +- Read parquet [...]
        void print(std::string &out, const Rel *r, const std::string &head, const std::string &indent) {
            out += head;
            if (!r) {
                out += "<null>\n";
                return;
            }
            auto [line, children] = describe(*r);
            out += line;
            out += '\n';
            for (std::size_t i = 0; i < children.size(); ++i) {
                const bool last = i + 1 == children.size();
                print(out, children[i], indent + "+- ", indent + (last ? "   " : ":  "));
            }
        }
    }

    std::string to_string(const TypeId id) {
        switch (id) {
            case TypeId::Null:         return "null";
            case TypeId::Boolean:      return "boolean";
            case TypeId::Int8:         return "int8";
            case TypeId::Int16:        return "int16";
            case TypeId::Int32:        return "int32";
            case TypeId::Int64:        return "int64";
            case TypeId::Float32:      return "float32";
            case TypeId::Float64:      return "float64";
            case TypeId::Decimal:      return "decimal";
            case TypeId::String:       return "string";
            case TypeId::Binary:       return "binary";
            case TypeId::Date:         return "date";
            case TypeId::Timestamp:    return "timestamp";
            case TypeId::TimestampNtz: return "timestamp_ntz";
            case TypeId::List:         return "list";
            case TypeId::Struct:       return "struct";
            case TypeId::Map:          return "map";
        }
        return "?";
    }

    // DataType keeps precision/scale, element types etc. private, so only the id is shown.
    std::string to_string(const DataType &t) { return to_string(t.id()); }

    std::string to_string(const Value &v) {
        return std::visit(overloaded{
            [](std::monostate) -> std::string { return "NULL"; },
            [](bool b) -> std::string { return b ? "true" : "false"; },
            [](const std::string &s) -> std::string { return fmt::format("{:?}", s); },  // quoted, escaped
            [](const Bytes &b) -> std::string {
                std::string out = "X'";
                for (unsigned char c : b.data) fmt::format_to(std::back_inserter(out), "{:02X}", c);
                return out + "'";
            },
            [](const Decimal &d) -> std::string { return fmt::format("{}", fmt::streamed(d.unscaled)); },  // unscaled
            [](const Date &d) -> std::string {
                return fmt::format("{:%F}", std::chrono::sys_days{std::chrono::days{d.days}});
            },
            [](const Timestamp &t) -> std::string {
                return fmt::format("{:%F %T}", std::chrono::sys_time<std::chrono::microseconds>{
                                                   std::chrono::microseconds{t.micros}});
            },
            [](const ListValue &l) -> std::string {
                return fmt::format("[{}]", join(l.items, [](const Value &x) { return to_string(x); }));
            },
            [](auto x) -> std::string { return fmt::format("{}", x); },  // remaining ints and floats
        }, v.v);
    }

    std::string to_string(const Literal &l) {
        return fmt::format("{}:{}", to_string(l.value), to_string(l.type));
    }

    std::string to_string(const Expr *e) {
        if (!e) return "<null>";
        return std::visit(overloaded{
            [](const Literal &l) { return to_string(l); },
            [](const ColumnRef &c) { return attr(c.attr); },
            [](const UnresolvedAttribute &u) { return fmt::format("'{}", u.name); },
            [](const Binary &b) {
                return fmt::format("({} {} {})", to_string(b.lhs), symbol(b.op), to_string(b.rhs));
            },
            [](const Attribute &a) { return fmt::format("{}#{}", a.name, a.id.value); },
        }, e->node);
    }

    std::string to_string(const Rel *r) {
        std::string out;
        print(out, r, "", "");
        if (!out.empty() && out.back() == '\n') out.pop_back();
        return out;
    }
}
