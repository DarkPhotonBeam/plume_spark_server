#include <plume_spark/ast_builder.hpp>
#include <spdlog/spdlog.h>
#include <plume/catalog/remote_resolver.hpp>
#include <plume/catalog/parquet.hpp>

#include "plume_spark/catalog/parquet.hpp"
#include <plume_spark/ast/expression.hpp>
#include <plume_spark/ast/relation.hpp>

namespace plume_spark {
    namespace sc = ::spark::connect;
    using namespace ast;

    absl::StatusOr<ExprPtr> AstBuilder::build_lit(const sc::Expression_Literal &lit) {
        switch (lit.literal_type_case()) {
            case spark::connect::Expression_Literal::kNull:
                break;
            case spark::connect::Expression_Literal::kBinary:
                break;
            case spark::connect::Expression_Literal::kBoolean:
                break;
            case spark::connect::Expression_Literal::kByte:
                break;
            case spark::connect::Expression_Literal::kShort:
                break;
            case spark::connect::Expression_Literal::kInteger:
                return lit_i32(lit.integer());
            case spark::connect::Expression_Literal::kLong:
                return lit_i64(lit.long_());
            case spark::connect::Expression_Literal::kFloat:
                break;
            case spark::connect::Expression_Literal::kDouble:
                return lit_f64(lit.double_());
            case spark::connect::Expression_Literal::kDecimal:
                break;
            case spark::connect::Expression_Literal::kString:
                return lit_string(lit.string());
            case spark::connect::Expression_Literal::kDate:
                break;
            case spark::connect::Expression_Literal::kTimestamp:
                break;
            case spark::connect::Expression_Literal::kTimestampNtz:
                break;
            case spark::connect::Expression_Literal::kCalendarInterval:
                break;
            case spark::connect::Expression_Literal::kYearMonthInterval:
                break;
            case spark::connect::Expression_Literal::kDayTimeInterval:
                break;
            case spark::connect::Expression_Literal::kArray:
                break;
            case spark::connect::Expression_Literal::kMap:
                break;
            case spark::connect::Expression_Literal::kStruct:
                break;
            case spark::connect::Expression_Literal::kSpecializedArray:
                break;
            case spark::connect::Expression_Literal::kTime:
                break;
            case spark::connect::Expression_Literal::kTimestampNtzNanos:
                break;
            case spark::connect::Expression_Literal::kTimestampLtzNanos:
                break;
            case spark::connect::Expression_Literal::LITERAL_TYPE_NOT_SET:
                break;
        }
        return absl::InternalError("unexpected literal case");
    }

    absl::StatusOr<ExprPtr> AstBuilder::build_expr(const sc::Expression &exp, const Scope &scope) {
        switch (exp.expr_type_case()) {
            case spark::connect::Expression::kLiteral:
                break;
            case spark::connect::Expression::kUnresolvedAttribute:
            {
                auto c = scope.lookup(exp.unresolved_attribute().unparsed_identifier());
                if (!c.ok()) return c.status();
                return expr(Reference{(*c)->id}, (*c)->type, (*c)->nullable);
            }
            case spark::connect::Expression::kUnresolvedFunction:
                break;
            case spark::connect::Expression::kExpressionString:
                break;
            case spark::connect::Expression::kUnresolvedStar:
                break;
            case spark::connect::Expression::kAlias:
                break;
            case spark::connect::Expression::kCast:
                break;
            case spark::connect::Expression::kUnresolvedRegex:
                break;
            case spark::connect::Expression::kSortOrder:
                break;
            case spark::connect::Expression::kLambdaFunction:
                break;
            case spark::connect::Expression::kWindow:
                break;
            case spark::connect::Expression::kUnresolvedExtractValue:
                break;
            case spark::connect::Expression::kUpdateFields:
                break;
            case spark::connect::Expression::kUnresolvedNamedLambdaVariable:
                break;
            case spark::connect::Expression::kCommonInlineUserDefinedFunction:
                break;
            case spark::connect::Expression::kCallFunction:
                break;
            case spark::connect::Expression::kNamedArgumentExpression:
                break;
            case spark::connect::Expression::kMergeAction:
                break;
            case spark::connect::Expression::kTypedAggregateExpression:
                break;
            case spark::connect::Expression::kSubqueryExpression:
                break;
            case spark::connect::Expression::kDirectShufflePartitionId:
                break;
            case spark::connect::Expression::kExtension:
                break;
            case spark::connect::Expression::EXPR_TYPE_NOT_SET:
                break;
        }
        return absl::UnimplementedError("build_expr not implemented");
    }

    absl::StatusOr<RelPtr> AstBuilder::build(const spark::connect::Relation &root) {
        return build_rel(root);
    }

    static absl::StatusOr<std::shared_ptr<const catalog::DataSource>> get_data_source(const sc::Read_DataSource &ds) {
        auto &format = ds.format();
        std::vector<std::string> paths;
        for (auto &p : ds.paths()) {
            paths.push_back(p);
        }
        if (format == "parquet") {
            return catalog::DataSource::parquet(std::move(paths));
        }
        return absl::UnimplementedError("the data source format " + format + " is not supported");
    }

    absl::StatusOr<std::vector<OutputColumn>> AstBuilder::generate_columns(const catalog::DataSource &src) {
        std::vector<OutputColumn> out;
        for (const auto &[name, type, nullable] : src.schema()) {
            out.push_back(OutputColumn{
                .id = fresh_id(),
                .name = name,
                .type = type,
                .nullable = nullable,
            });
        }
        return out;
    }

    static absl::StatusOr<OutputColumn> column_by_id(const std::vector<OutputColumn> &columns, ColumnId id) {
        for (auto col : columns) {
            if (id == col.id) {
                return col;
            }
        }
        return absl::NotFoundError("column not found");
    }

    absl::StatusOr<RelPtr> AstBuilder::build_rel(const sc::Relation &rel) {
        switch (rel.rel_type_case()) {
            case spark::connect::Relation::kRead:
            {
                auto ds = get_data_source(rel.read().data_source());
                if (!ds.ok()) return ds.status();

                auto cols = generate_columns(**ds);
                if (!cols.ok()) return cols.status();

                return std::make_unique<Relation>(Read{.data_source = *ds, .output = *cols});
            }
            case spark::connect::Relation::kProject:
            {
                auto input = build_rel(rel.project().input());
                if (!input.ok()) return input.status();

                auto input_cols = output_of(**input);

                const Scope scope{input_cols};

                std::vector<NamedExpr> cols;
                for (auto &e : rel.project().expressions()) {
                    switch (e.expr_type_case()) {
                        case sc::Expression::kAlias: // renamed expression
                        {
                            const auto &a = e.alias();
                            if (a.name_size() != 1) {
                                return absl::UnimplementedError("alias with multiple names");
                            }
                            auto built_expr = build_expr(a.expr(), scope);
                            if (!built_expr.ok()) return built_expr.status();
                            cols.push_back(NamedExpr{fresh_id(), a.name(0), *std::move(built_expr)});
                            break;
                        }
                        case sc::Expression::kUnresolvedStar:
                            if (e.unresolved_star().has_unparsed_target()) {
                                return absl::UnimplementedError("qualified star");
                            }
                            for (const auto &c : input_cols) {
                                cols.push_back({c.id, c.name, expr(Reference{c.id}, c.type, c.nullable)});
                            }
                            break;
                        default: {
                            auto built_expr = build_expr(e, scope);
                            if (!built_expr.ok()) return built_expr.status();
                            if (const auto *ref = std::get_if<Reference>(&(*built_expr)->node)) {
                                const auto in = column_by_id(input_cols, ref->id);
                                if (!in.ok()) return in.status();
                                cols.push_back({in->id, in->name, *std::move(built_expr)});
                            } else {
                                return absl::UnimplementedError("todo: unnamed projection expressions");
                            }
                            break;
                        }
                    }
                }

                return std::make_unique<Relation>(Project{.input = *std::move(input), .columns = std::move(cols)});
            }
            case spark::connect::Relation::kFilter: {
                auto input = build_rel(rel.filter().input());
                if (!input.ok()) return input.status();

                auto input_cols = output_of(**input);

                auto built_cond = build_expr(rel.filter().condition(), Scope{input_cols});
                if (!built_cond.ok()) return built_cond.status();
                if ((*built_cond)->type != DataType::boolean()) {
                    return absl::InvalidArgumentError("type error: filter condition is not a boolean");
                }

                return std::make_unique<Relation>(Filter{.input = *std::move(input), .condition = *std::move(built_cond)});
            }
            case spark::connect::Relation::kJoin:
                break;
            case spark::connect::Relation::kSetOp:
                break;
            case spark::connect::Relation::kSort:
                break;
            case spark::connect::Relation::kLimit:
                break;
            case spark::connect::Relation::kAggregate:
                break;
            case spark::connect::Relation::kShowString:
                break;
            case spark::connect::Relation::kUnknown:
                break;
            case spark::connect::Relation::REL_TYPE_NOT_SET:
                break;
            default:
                break;
        }
        return absl::UnimplementedError("this spark relation is not implemented");
    }
}
