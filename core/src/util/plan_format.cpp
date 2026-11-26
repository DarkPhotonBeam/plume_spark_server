#include "plume_spark/util/plan_format.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <string_view>
#include <vector>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/text_format.h>

namespace plume_spark::util {
    namespace {
        namespace sc = ::spark::connect;
        namespace pb = ::google::protobuf;

        constexpr size_t kMaxFallbackLen = 160;
        constexpr size_t kMaxStringLitLen = 64;

        // ---------------------------------------------------------------- generic helpers

        template<class Range, class F>
        std::string join(const Range &r, F &&f, std::string_view sep = ", ") {
            std::string out;
            bool first = true;
            for (const auto &x: r) {
                if (!first) out += sep;
                first = false;
                out += f(x);
            }
            return out;
        }

        std::string truncate(std::string s, size_t max) {
            if (s.size() > max) {
                s.resize(max);
                s += "…";
            }
            return s;
        }

        std::string quote(std::string_view s) {
            std::string out = "'";
            for (char c: s) {
                if (c == '\'' || c == '\\') out += '\\';
                out += c;
            }
            out += '\'';
            return out;
        }

        // Name of the field set in oneof `oneof` of `m`, or "" if none is set.
        std::string set_oneof_field(const pb::Message &m, std::string_view oneof) {
            const auto *od = m.GetDescriptor()->FindOneofByName(oneof);
            if (!od) return "";
            const auto *f = m.GetReflection()->GetOneofFieldDescriptor(m, od);
            return f ? std::string(f->name()) : "";
        }

        // The message held in oneof `oneof` of `m`, or nullptr if none is set / it is a scalar.
        const pb::Message *set_oneof_message(const pb::Message &m, std::string_view oneof) {
            const auto *od = m.GetDescriptor()->FindOneofByName(oneof);
            if (!od) return nullptr;
            const auto *f = m.GetReflection()->GetOneofFieldDescriptor(m, od);
            if (!f || f->cpp_type() != pb::FieldDescriptor::CPPTYPE_MESSAGE) return nullptr;
            return &m.GetReflection()->GetMessage(m, f);
        }

        bool is_relation_field(const pb::FieldDescriptor *f) {
            return f->cpp_type() == pb::FieldDescriptor::CPPTYPE_MESSAGE &&
                   f->message_type() == sc::Relation::descriptor();
        }

        // All Relation-typed fields directly inside `m` (input, left/right, root/references, ...)
        std::vector<const sc::Relation *> child_relations(const pb::Message &m) {
            std::vector<const sc::Relation *> out;
            const auto *d = m.GetDescriptor();
            const auto *r = m.GetReflection();
            for (int i = 0; i < d->field_count(); ++i) {
                const auto *f = d->field(i);
                if (!is_relation_field(f)) continue;
                if (f->is_repeated()) {
                    for (int j = 0; j < r->FieldSize(m, f); ++j)
                        out.push_back(static_cast<const sc::Relation *>(&r->GetRepeatedMessage(m, f, j)));
                } else if (r->HasField(m, f)) {
                    out.push_back(static_cast<const sc::Relation *>(&r->GetMessage(m, f)));
                }
            }
            return out;
        }

        // Single-line text dump of `m`, without its child relations (those are printed as the tree).
        std::string fallback_dump(const pb::Message &m) {
            std::unique_ptr<pb::Message> copy(m.New());
            copy->CopyFrom(m);
            const auto *d = copy->GetDescriptor();
            for (int i = 0; i < d->field_count(); ++i)
                if (is_relation_field(d->field(i))) copy->GetReflection()->ClearField(copy.get(), d->field(i));

            std::string out;
            pb::TextFormat::Printer p;
            p.SetSingleLineMode(true);
            p.SetUseUtf8StringEscaping(true);
            if (!p.PrintToString(*copy, &out)) return "<unprintable>";
            while (!out.empty() && out.back() == ' ') out.pop_back();
            return truncate(std::move(out), kMaxFallbackLen);
        }

        std::string plan_id_suffix(bool has, int64_t id) {
            return has ? fmt::format(" @{}", id) : "";
        }

        std::string options(const pb::Map<std::string, std::string> &opts) {
            if (opts.empty()) return "";
            std::vector<std::pair<std::string, std::string>> kv(opts.begin(), opts.end());
            std::sort(kv.begin(), kv.end());
            return " {" + join(kv, [](const auto &p) { return p.first + "=" + p.second; }) + "}";
        }

        // ---------------------------------------------------------------- expressions

        std::string expr(const sc::Expression &e);

        std::string exprs(const pb::RepeatedPtrField<sc::Expression> &es) {
            return join(es, [](const sc::Expression &e) { return expr(e); });
        }

        std::string fmt_double(double d) {
            std::string s = fmt::format("{}", d);
            if (s.find_first_of(".eEni") == std::string::npos) s += ".0"; // 50 -> 50.0, keep nan/inf
            return s;
        }

        std::string literal(const sc::Expression_Literal &l) {
            using L = sc::Expression_Literal;
            switch (l.literal_type_case()) {
                case L::kNull: return "NULL";
                case L::kBinary: {
                    std::string hex;
                    for (unsigned char c: l.binary()) hex += fmt::format("{:02X}", c);
                    return "X'" + truncate(std::move(hex), kMaxStringLitLen) + "'";
                }
                case L::kBoolean: return l.boolean() ? "true" : "false";
                case L::kByte: return fmt::format("{}Y", l.byte());
                case L::kShort: return fmt::format("{}S", l.short_());
                case L::kInteger: return fmt::format("{}", l.integer());
                case L::kLong: return fmt::format("{}L", l.long_());
                case L::kFloat: return fmt_double(l.float_()) + "F";
                case L::kDouble: return fmt_double(l.double_());
                case L::kDecimal: return l.decimal().value() + "BD";
                case L::kString: return quote(truncate(l.string(), kMaxStringLitLen));
                case L::kDate: {
                    std::chrono::sys_days d{std::chrono::days{l.date()}};
                    return fmt::format("DATE '{:%F}'", d);
                }
                case L::kTimestamp: {
                    std::chrono::sys_time<std::chrono::microseconds> t{std::chrono::microseconds{l.timestamp()}};
                    return fmt::format("TIMESTAMP '{:%F %T}'", t);
                }
                case L::kTimestampNtz: {
                    std::chrono::sys_time<std::chrono::microseconds> t{std::chrono::microseconds{l.timestamp_ntz()}};
                    return fmt::format("TIMESTAMP_NTZ '{:%F %T}'", t);
                }
                case L::kArray:
                    return "array(" + join(l.array().elements(), literal) + ")";
                case L::kStruct:
                    return "struct(" + join(l.struct_().elements(), literal) + ")";
                case L::kMap: {
                    std::string out = "map(";
                    for (int i = 0; i < l.map().keys_size() && i < l.map().values_size(); ++i) {
                        if (i) out += ", ";
                        out += literal(l.map().keys(i)) + " -> " + literal(l.map().values(i));
                    }
                    return out + ")";
                }
                default:
                    return set_oneof_field(l, "literal_type") + "(" + fallback_dump(l) + ")";
            }
        }

        // Spark Connect encodes operators as UnresolvedFunction with these names
        std::string_view infix_op(std::string_view fn) {
            static constexpr std::pair<std::string_view, std::string_view> kOps[] = {
                {"==", "="}, {"!=", "!="}, {"<", "<"}, {"<=", "<="}, {">", ">"}, {">=", ">="},
                {"<=>", "<=>"}, {"+", "+"}, {"-", "-"}, {"*", "*"}, {"/", "/"}, {"%", "%"},
                {"and", "AND"}, {"or", "OR"}, {"&", "&"}, {"|", "|"}, {"^", "^"},
            };
            for (auto [k, v]: kOps) if (k == fn) return v;
            return {};
        }

        std::string sort_order(const sc::Expression_SortOrder &s) {
            using S = sc::Expression_SortOrder;
            std::string out = expr(s.child());
            if (s.direction() == S::SORT_DIRECTION_ASCENDING) out += " ASC";
            if (s.direction() == S::SORT_DIRECTION_DESCENDING) out += " DESC";
            if (s.null_ordering() == S::SORT_NULLS_FIRST) out += " NULLS FIRST";
            if (s.null_ordering() == S::SORT_NULLS_LAST) out += " NULLS LAST";
            return out;
        }

        std::string sort_orders(const pb::RepeatedPtrField<sc::Expression_SortOrder> &os) {
            return join(os, sort_order);
        }

        std::string function(std::string_view name, const pb::RepeatedPtrField<sc::Expression> &args,
                             bool distinct) {
            if (args.size() == 2 && !distinct) {
                if (auto op = infix_op(name); !op.empty())
                    return fmt::format("({} {} {})", expr(args[0]), op, expr(args[1]));
            }
            if (args.size() == 1 && !distinct) {
                if (name == "not") return "NOT " + expr(args[0]);
                if (name == "negative") return "-" + expr(args[0]);
            }
            return fmt::format("{}({}{})", name, distinct ? "DISTINCT " : "", exprs(args));
        }

        std::string expr(const sc::Expression &e) {
            using E = sc::Expression;
            switch (e.expr_type_case()) {
                case E::kLiteral:
                    return literal(e.literal());
                case E::kUnresolvedAttribute: {
                    const auto &a = e.unresolved_attribute();
                    return a.has_plan_id() ? fmt::format("{}@{}", a.unparsed_identifier(), a.plan_id())
                                           : a.unparsed_identifier();
                }
                case E::kUnresolvedFunction: {
                    const auto &f = e.unresolved_function();
                    return function(f.function_name(), f.arguments(), f.is_distinct());
                }
                case E::kCallFunction:
                    return function(e.call_function().function_name(), e.call_function().arguments(), false);
                case E::kExpressionString:
                    return "expr(" + quote(e.expression_string().expression()) + ")";
                case E::kUnresolvedStar:
                    return e.unresolved_star().has_unparsed_target() ? e.unresolved_star().unparsed_target() : "*";
                case E::kUnresolvedRegex:
                    return "regex(" + quote(e.unresolved_regex().col_name()) + ")";
                case E::kAlias: {
                    const auto &a = e.alias();
                    std::string names = a.name_size() == 1
                                            ? a.name(0)
                                            : "(" + join(a.name(), [](const std::string &s) { return s; }) + ")";
                    return expr(a.expr()) + " AS " + names;
                }
                case E::kCast: {
                    const auto &c = e.cast();
                    std::string type = c.has_type_str()
                                           ? c.type_str()
                                           : set_oneof_field(c.type(), "kind");
                    return fmt::format("CAST({} AS {})", expr(c.expr()), type);
                }
                case E::kSortOrder:
                    return sort_order(e.sort_order());
                case E::kUnresolvedExtractValue:
                    return fmt::format("{}[{}]", expr(e.unresolved_extract_value().child()),
                                       expr(e.unresolved_extract_value().extraction()));
                case E::kUnresolvedNamedLambdaVariable:
                    return join(e.unresolved_named_lambda_variable().name_parts(),
                                [](const std::string &s) { return s; }, ".");
                case E::kLambdaFunction: {
                    const auto &l = e.lambda_function();
                    std::string params = join(l.arguments(), [](const auto &v) {
                        return join(v.name_parts(), [](const std::string &s) { return s; }, ".");
                    });
                    return fmt::format("(({}) -> {})", params, expr(l.function()));
                }
                case E::kNamedArgumentExpression:
                    return e.named_argument_expression().key() + " => " + expr(e.named_argument_expression().value());
                case E::kWindow: {
                    const auto &w = e.window();
                    std::string spec;
                    if (w.partition_spec_size()) spec += "PARTITION BY " + exprs(w.partition_spec());
                    if (w.order_spec_size()) {
                        if (!spec.empty()) spec += " ";
                        spec += "ORDER BY " + sort_orders(w.order_spec());
                    }
                    if (w.has_frame_spec()) {
                        if (!spec.empty()) spec += " ";
                        spec += "frame(" + fallback_dump(w.frame_spec()) + ")";
                    }
                    return fmt::format("{} OVER ({})", expr(w.window_function()), spec);
                }
                case E::kSubqueryExpression:
                    return fmt::format("subquery(@{})", e.subquery_expression().plan_id());
                case E::EXPR_TYPE_NOT_SET:
                    return "<unset>";
                default: {
                    const auto *inner = set_oneof_message(e, "expr_type");
                    return set_oneof_field(e, "expr_type") + "(" + (inner ? fallback_dump(*inner) : "") + ")";
                }
            }
        }

        // ---------------------------------------------------------------- relations

        std::string names(const pb::RepeatedPtrField<std::string> &ns) {
            return join(ns, [](const std::string &s) { return s; });
        }

        std::string join_type(sc::Join_JoinType t) {
            switch (t) {
                case sc::Join::JOIN_TYPE_INNER: return "Inner";
                case sc::Join::JOIN_TYPE_FULL_OUTER: return "FullOuter";
                case sc::Join::JOIN_TYPE_LEFT_OUTER: return "LeftOuter";
                case sc::Join::JOIN_TYPE_RIGHT_OUTER: return "RightOuter";
                case sc::Join::JOIN_TYPE_LEFT_ANTI: return "LeftAnti";
                case sc::Join::JOIN_TYPE_LEFT_SEMI: return "LeftSemi";
                case sc::Join::JOIN_TYPE_CROSS: return "Cross";
                default: return "?";
            }
        }

        // The one-line description of a relation node (without children)
        std::string rel_line(const sc::Relation &rel) {
            using R = sc::Relation;
            switch (rel.rel_type_case()) {
                case R::kRead: {
                    const auto &r = rel.read();
                    std::string s = r.is_streaming() ? "ReadStream" : "Read";
                    if (r.has_named_table())
                        return s + " " + r.named_table().unparsed_identifier() + options(r.named_table().options());
                    const auto &ds = r.data_source();
                    s += " " + (ds.has_format() ? ds.format() : std::string("?"));
                    if (ds.paths_size()) s += " [" + names(ds.paths()) + "]";
                    if (ds.predicates_size()) s += " where [" + names(ds.predicates()) + "]";
                    if (ds.has_schema()) s += " schema=" + quote(truncate(ds.schema(), kMaxFallbackLen));
                    return s + options(ds.options());
                }
                case R::kProject:
                    return "Project [" + exprs(rel.project().expressions()) + "]";
                case R::kFilter:
                    return "Filter " + expr(rel.filter().condition());
                case R::kJoin: {
                    const auto &j = rel.join();
                    std::string s = "Join " + join_type(j.join_type());
                    if (j.using_columns_size()) s += " USING (" + names(j.using_columns()) + ")";
                    if (j.has_join_condition()) s += " ON " + expr(j.join_condition());
                    return s;
                }
                case R::kSetOp: {
                    const auto &o = rel.set_op();
                    std::string s;
                    switch (o.set_op_type()) {
                        case sc::SetOperation::SET_OP_TYPE_UNION: s = "Union"; break;
                        case sc::SetOperation::SET_OP_TYPE_INTERSECT: s = "Intersect"; break;
                        case sc::SetOperation::SET_OP_TYPE_EXCEPT: s = "Except"; break;
                        default: s = "SetOp?"; break;
                    }
                    if (o.is_all()) s += " ALL";
                    if (o.by_name()) s += " BY NAME";
                    if (o.allow_missing_columns()) s += " (allow missing)";
                    return s;
                }
                case R::kSort:
                    return std::string(rel.sort().has_is_global() && !rel.sort().is_global() ? "SortWithinPartitions" : "Sort")
                           + " [" + sort_orders(rel.sort().order()) + "]";
                case R::kLimit:
                    return fmt::format("Limit {}", rel.limit().limit());
                case R::kOffset:
                    return fmt::format("Offset {}", rel.offset().offset());
                case R::kTail:
                    return fmt::format("Tail {}", rel.tail().limit());
                case R::kAggregate: {
                    const auto &a = rel.aggregate();
                    std::string s = "Aggregate";
                    if (a.group_type() != sc::Aggregate::GROUP_TYPE_GROUPBY)
                        s += " " + std::string(sc::Aggregate::GroupType_Name(a.group_type())).substr(sizeof("GROUP_TYPE_") - 1);
                    s += " [" + exprs(a.grouping_expressions()) + "], [" + exprs(a.aggregate_expressions()) + "]";
                    if (a.has_pivot())
                        s += " pivot " + expr(a.pivot().col()) + " in (" + join(a.pivot().values(), literal) + ")";
                    for (const auto &gs: a.grouping_sets()) s += " (" + exprs(gs.grouping_set()) + ")";
                    return s;
                }
                case R::kSql:
                    return "SQL " + quote(rel.sql().query());
                case R::kLocalRelation: {
                    const auto &l = rel.local_relation();
                    std::string s = fmt::format("LocalRelation ({} bytes)", l.data().size());
                    if (l.has_schema()) s += " schema=" + quote(truncate(l.schema(), kMaxFallbackLen));
                    return s;
                }
                case R::kSample: {
                    const auto &sm = rel.sample();
                    std::string s = fmt::format("Sample [{}, {})", sm.lower_bound(), sm.upper_bound());
                    if (sm.with_replacement()) s += " with replacement";
                    if (sm.has_seed()) s += fmt::format(" seed={}", sm.seed());
                    return s;
                }
                case R::kDeduplicate: {
                    const auto &d = rel.deduplicate();
                    std::string s = d.within_watermark() ? "DeduplicateWithinWatermark" : "Deduplicate";
                    if (d.all_columns_as_keys()) return s + " [*]";
                    return s + " [" + names(d.column_names()) + "]";
                }
                case R::kRange: {
                    const auto &r = rel.range();
                    std::string s = fmt::format("Range ({}, {}, step={}", r.start(), r.end(), r.step());
                    if (r.has_num_partitions()) s += fmt::format(", splits={}", r.num_partitions());
                    return s + ")";
                }
                case R::kSubqueryAlias: {
                    const auto &a = rel.subquery_alias();
                    std::string q = a.qualifier_size() ? join(a.qualifier(), [](const std::string &s) { return s; }, ".") + "." : "";
                    return "SubqueryAlias " + q + a.alias();
                }
                case R::kRepartition: {
                    const auto &r = rel.repartition();
                    return fmt::format("Repartition {}{}", r.num_partitions(), r.shuffle() ? " shuffle" : "");
                }
                case R::kToDf:
                    return "ToDF [" + names(rel.to_df().column_names()) + "]";
                case R::kWithColumnsRenamed: {
                    const auto &w = rel.with_columns_renamed();
                    std::vector<std::string> rn;
                    for (const auto &r: w.renames()) rn.push_back(r.col_name() + " -> " + r.new_col_name());
                    return "WithColumnsRenamed [" + join(rn, [](const std::string &s) { return s; }) + "]";
                }
                case R::kShowString: {
                    const auto &s = rel.show_string();
                    return fmt::format("ShowString rows={} truncate={}{}", s.num_rows(), s.truncate(),
                                       s.vertical() ? " vertical" : "");
                }
                case R::kDrop: {
                    const auto &d = rel.drop();
                    std::string cols = exprs(d.columns());
                    if (d.column_names_size()) cols += (cols.empty() ? "" : ", ") + names(d.column_names());
                    return "Drop [" + cols + "]";
                }
                case R::kWithColumns: {
                    std::string s = join(rel.with_columns().aliases(), [](const sc::Expression_Alias &a) {
                        return expr(a.expr()) + " AS " + names(a.name());
                    });
                    return "WithColumns [" + s + "]";
                }
                case R::kHint: {
                    const auto &h = rel.hint();
                    return "Hint " + h.name() + (h.parameters_size() ? "(" + exprs(h.parameters()) + ")" : "");
                }
                case R::kWithWatermark:
                    return "WithWatermark " + rel.with_watermark().event_time() + ", " +
                           quote(rel.with_watermark().delay_threshold());
                case R::kCachedRemoteRelation:
                    return "CachedRemoteRelation " + rel.cached_remote_relation().relation_id();
                case R::kWithRelations:
                    return "WithRelations"; // children: root first, then the references
                case R::REL_TYPE_NOT_SET:
                    return "<unset>";
                default: {
                    const auto *inner = set_oneof_message(rel, "rel_type");
                    if (!inner) return set_oneof_field(rel, "rel_type");
                    std::string dump = fallback_dump(*inner);
                    return std::string(inner->GetDescriptor()->name()) + (dump.empty() ? "" : " " + dump);
                }
            }
        }

        void render(const sc::Relation &rel, const std::string &prefix, bool is_root, bool is_last, std::string &out);

        void render_children(const pb::Message &inner, const std::string &child_prefix, std::string &out) {
            auto children = child_relations(inner);
            for (size_t i = 0; i < children.size(); ++i)
                render(*children[i], child_prefix, false, i + 1 == children.size(), out);
        }

        void render(const sc::Relation &rel, const std::string &prefix, bool is_root, bool is_last, std::string &out) {
            out += prefix;
            if (!is_root) out += is_last ? "└─ " : "├─ ";
            out += rel_line(rel);
            out += plan_id_suffix(rel.has_common() && rel.common().has_plan_id(), rel.common().plan_id());
            out += '\n';

            const std::string child_prefix = is_root ? prefix : prefix + (is_last ? "   " : "│  ");
            if (const auto *inner = set_oneof_message(rel, "rel_type")) render_children(*inner, child_prefix, out);
        }
    }

    std::string format_relation(const sc::Relation &rel) {
        std::string out;
        render(rel, "", true, true, out);
        if (!out.empty() && out.back() == '\n') out.pop_back();
        return out;
    }

    std::string format_expression(const sc::Expression &e) {
        return expr(e);
    }

    std::string format_plan(const sc::Plan &plan) {
        if (plan.has_root()) return format_relation(plan.root());
        if (!plan.has_command()) return "<empty plan>";

        // Commands (write, createView, ...): print the command, then any relations it carries
        const auto *inner = set_oneof_message(plan.command(), "command_type");
        if (!inner) return "Command " + set_oneof_field(plan.command(), "command_type");

        std::string out = "Command " + std::string(inner->GetDescriptor()->name());
        if (std::string dump = fallback_dump(*inner); !dump.empty()) out += " " + dump;
        out += '\n';
        auto children = child_relations(*inner);
        for (size_t i = 0; i < children.size(); ++i)
            render(*children[i], "", false, i + 1 == children.size(), out);
        if (out.back() == '\n') out.pop_back();
        return out;
    }
}
