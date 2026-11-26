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

    absl::StatusOr<std::shared_ptr<catalog::DataSource>> get_data_source(const sc::Read_DataSource &ds) {
        auto &format = ds.format();
        if (format == "parquet") {

        }
        return absl::UnimplementedError("the data source format " + format + " is not supported");
    }

    absl::StatusOr<std::vector<OutputColumn>> output_columns_from_data_source(const catalog::DataSource &src) {
        return absl::UnimplementedError("output_columns_from_data_source");
    }

    absl::StatusOr<RelPtr> AstBuilder::build_rel(const sc::Relation &rel) {
        switch (rel.rel_type_case()) {
            case spark::connect::Relation::kRead:
            {
                auto ds = get_data_source(rel.read().data_source());
                if (!ds.ok()) return ds.status();

                auto cols = output_columns_from_data_source(**ds);
                if (!cols.ok()) return cols.status();

                return std::make_unique<Relation>(Read{.data_source = *ds, .output = *cols});
            }
            case spark::connect::Relation::kProject:
            {
                auto input = build_rel(rel.project().input());
                if (!input.ok()) return input.status();

                auto input_cols = output_of(**input);

                auto &p = rel.project().input();
                return std::make_unique<Relation>(Project{});
            }
            case spark::connect::Relation::kFilter:
                break;
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
