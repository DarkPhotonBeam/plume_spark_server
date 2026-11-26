#pragma once
#include "plume_spark/ast_builder.hpp"
#include "plume_spark/ast/relation.hpp"
#include <plume/parser/converter.hpp>
#include <plume/parser/physical_plan.hpp>

namespace plume_spark {

    class PlumeBuilder {
    public:
        explicit PlumeBuilder(const ast::Relation &ast, plume::parser::ConverterConfig cfg) : ast_(std::move(ast)), cfg_(cfg), builder_(cfg.optimize_remote_fetching) {}

        absl::StatusOr<std::unique_ptr<plume::parser::PhysicalPlan>> convert();
    private:
        struct Built {
            plume::parser::PlanBuilder::SharedOpNode node;
            std::map<ast::ColumnId, uint32_t> pos;   // ColumnId -> index in node->output_schema
        };

        uint32_t split_count(uint64_t estimated_rows) const;

        absl::StatusOr<Built> build_read(const ast::Read &r);

        absl::StatusOr<Built> build(const ast::Relation &rel);

        const ast::Relation &ast_;
        plume::parser::ConverterConfig cfg_;
        plume::parser::PlanBuilder builder_;
    };
}
