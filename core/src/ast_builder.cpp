#include <plume_spark/ast_builder.hpp>
#include <spdlog/spdlog.h>

namespace plume_spark {
    namespace sc = ::spark::connect;

    namespace {
        ast::Literal build_literal(const sc::Expression_Literal &lit) {
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
                    return ast::int32_lit(lit.integer());
                case spark::connect::Expression_Literal::kLong:
                    return ast::int64_lit(lit.long_());
                case spark::connect::Expression_Literal::kFloat:
                    break;
                case spark::connect::Expression_Literal::kDouble:
                    return ast::float64_lit(lit.double_());
                case spark::connect::Expression_Literal::kDecimal:
                    break;
                case spark::connect::Expression_Literal::kString:
                    return ast::string_lit(lit.string());
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
            return ast::int32_lit(0); // todo: error handling instead of default return
        }

        ast::ExprPtr build_expr(const sc::Expression &exp) {
            switch (exp.expr_type_case()) {
                case spark::connect::Expression::kLiteral: {
                    auto &lit = exp.literal();
                    return std::make_unique<ast::Expr>(build_literal(lit));
                }
                case spark::connect::Expression::kUnresolvedAttribute: {
                    auto &ua = exp.unresolved_attribute();
                    return std::make_unique<ast::Expr>(ast::UnresolvedAttribute{
                        .name = ua.unparsed_identifier(),
                        .is_metadata_col = ua.is_metadata_column()
                    });
                }
                case spark::connect::Expression::kUnresolvedFunction: {
                    auto &uf = exp.unresolved_function();
                    auto &name = uf.function_name();
                    auto n_args = uf.arguments_size();
                    if (n_args == 2) {
                        // binary
                        auto lhs = build_expr(uf.arguments().Get(0));
                        auto rhs = build_expr(uf.arguments().Get(1));
                        ast::BinOp op = ast::BinOp::And;
                        if (name == "and") {
                            op = ast::BinOp::And;
                        } else if (name == "add") {
                            op = ast::BinOp::Add;
                        } else if (name == ">=") {
                            op = ast::BinOp::Gte;
                        } else {
                            spdlog::error("UNKNOWN FUNCTION NAME '{}'", name);
                        }
                        return std::make_unique<ast::Expr>(ast::Binary{
                            .op = op,
                            .lhs = std::move(lhs),
                            .rhs = std::move(rhs)
                        });
                    }
                }
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
            return nullptr;
        }
    }

    ast::RelPtr build_from_proto(const sc::Relation &rel) {

        switch (rel.rel_type_case()) {
            case sc::Relation::kRead: {
                auto &rd = rel.read();
                auto &ds = rd.data_source();
                auto &format = ds.format();
                auto schema = ds.schema();
                std::vector<std::string> paths{};
                for (auto &path : ds.paths()) {
                    paths.push_back(path);
                }
                // todo: resolve schema
                // if parquet -> get schema from parquet
                return std::make_unique<ast::Rel>(ast::Read{format, paths, std::vector<ast::Attribute>{}});
            }
            case sc::Relation::kProject: {
                auto input = build_from_proto(rel.project().input());
                std::vector<ast::ExprPtr> exprs{};
                for (auto &e : rel.project().expressions()) {
                    exprs.push_back(build_expr(e));
                }
                return std::make_unique<ast::Rel>(ast::UnresolvedProject{(std::move(input)), std::move(exprs)});
            }
            case sc::Relation::kFilter: {
                auto input = build_from_proto(rel.filter().input());
                auto cond = build_expr(rel.filter().condition());
                return std::make_unique<ast::Rel>(ast::Filter{std::move(input), (std::move(cond))});
            }
            case sc::Relation::kJoin:
                break;
            case sc::Relation::kSetOp:
                break;
            case sc::Relation::kSort:
                break;
            case sc::Relation::kLimit:
                break;
            case sc::Relation::kAggregate:
                break;
            case sc::Relation::kSql:
                break;
            case sc::Relation::kLocalRelation:
                break;
            case sc::Relation::kSample:
                break;
            case sc::Relation::kOffset:
                break;
            case sc::Relation::kDeduplicate:
                break;
            case sc::Relation::kRange:
                break;
            case sc::Relation::kSubqueryAlias:
                break;
            case sc::Relation::kRepartition:
                break;
            case sc::Relation::kToDf:
                break;
            case sc::Relation::kWithColumnsRenamed:
                break;
            case sc::Relation::kShowString: {
                auto tree = build_from_proto(rel.show_string().input());
                return tree;
            }
            case sc::Relation::kDrop:
                break;
            case sc::Relation::kTail:
                break;
            case sc::Relation::kWithColumns:
                break;
            case sc::Relation::kHint:
                break;
            case sc::Relation::kUnpivot:
                break;
            case sc::Relation::kToSchema:
                break;
            case sc::Relation::kRepartitionByExpression:
                break;
            case sc::Relation::kMapPartitions:
                break;
            case sc::Relation::kCollectMetrics:
                break;
            case sc::Relation::kParse:
                break;
            case sc::Relation::kGroupMap:
                break;
            case sc::Relation::kCoGroupMap:
                break;
            case sc::Relation::kWithWatermark:
                break;
            case sc::Relation::kApplyInPandasWithState:
                break;
            case sc::Relation::kHtmlString:
                break;
            case sc::Relation::kCachedLocalRelation:
                break;
            case sc::Relation::kCachedRemoteRelation:
                break;
            case sc::Relation::kCommonInlineUserDefinedTableFunction:
                break;
            case sc::Relation::kAsOfJoin:
                break;
            case sc::Relation::kCommonInlineUserDefinedDataSource:
                break;
            case sc::Relation::kWithRelations:
                break;
            case sc::Relation::kTranspose:
                break;
            case sc::Relation::kUnresolvedTableValuedFunction:
                break;
            case sc::Relation::kLateralJoin:
                break;
            case sc::Relation::kChunkedCachedLocalRelation:
                break;
            case sc::Relation::kRelationChanges:
                break;
            case sc::Relation::kNearestByJoin:
                break;
            case sc::Relation::kZip:
                break;
            case sc::Relation::kFillNa:
                break;
            case sc::Relation::kDropNa:
                break;
            case sc::Relation::kReplace:
                break;
            case sc::Relation::kSummary:
                break;
            case sc::Relation::kCrosstab:
                break;
            case sc::Relation::kDescribe:
                break;
            case sc::Relation::kCov:
                break;
            case sc::Relation::kCorr:
                break;
            case sc::Relation::kApproxQuantile:
                break;
            case sc::Relation::kFreqItems:
                break;
            case sc::Relation::kSampleBy:
                break;
            case sc::Relation::kCatalog:
                break;
            case sc::Relation::kMlRelation:
                break;
            case sc::Relation::kExtension:
                break;
            case sc::Relation::kUnknown:
                break;
            case sc::Relation::REL_TYPE_NOT_SET:
                break;
        }

        return nullptr;
    }
}
