#include "plume_spark/plume_builder.hpp"
#include "plume/parser/plan_builder.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/expression/expression.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/parser/converter.hpp"
#include "plume_spark/util/casts.hpp"

namespace plume_spark {
    namespace pp = plume::parser;
    namespace pe = plume::expr;

    uint32_t PlumeBuilder::split_count(uint64_t estimated_rows) const {
        if (cfg_.max_splits <= 1 || cfg_.target_rows_per_split == 0) {
            return cfg_.max_splits; // parallelism disabled, or "split as far as allowed"
        }
        uint64_t n = (estimated_rows + cfg_.target_rows_per_split - 1) / cfg_.target_rows_per_split;
        if (n < 1) {
            n = 1;
        }
        if (n > cfg_.max_splits) {
            n = cfg_.max_splits;
        }
        return static_cast<uint32_t>(n);
    }

    absl::StatusOr<PlumeBuilder::Built> PlumeBuilder::build_read(const ast::Read &r) {
        auto plume_src = r.data_source->plume_src();
        auto plume_src_schema = plume_src->schema;

        if (!plume_src_schema.has_value()) {
            return absl::UnimplementedError("building plume schemas from scratch is not implemented");
        }

        plume::Schema schema_full = std::move(plume_src_schema.value());

        if (cfg_.filter_pushdown) {
            return absl::UnimplementedError("filter_pushdown is not implemented yet");
        }

        plume::parser::PlanBuilder::SharedOpNode cur;

        plume::parser::LeafStage *leaf_stage;
        if (cfg_.projection_pushdown) {
            return absl::UnimplementedError("projection_pushdown is not implemented yet");
        } else {
            cur = builder_.Scan(plume_src, schema_full, nullptr, nullptr);
            leaf_stage = builder_.LeafStageAt(cur->stage_idx);

            auto proj = std::make_shared<plume::exec::ProjectionTemplate>();
            for (uint32_t s = 0; s < schema_full.columns.size(); s++) {
                const plume::Column &col = schema_full.columns[s];
                proj->projections.push_back(pe::ExprNode::Reference(s, col.type));
            }
            cur = builder_.After(std::move(cur), std::move(proj), schema_full);
        }
        leaf_stage->source_splits = split_count(plume_src->cardinality_total);

        return Built{
            .node = cur,
        };
    }

    absl::StatusOr<PlumeBuilder::Built> PlumeBuilder::build(const ast::Relation &rel) {
        using R = absl::StatusOr<Built>;
        auto visitor = util::overloaded{
            [this](const ast::Read &r) -> R { return build_read(r); },
            [](const ast::Project &) -> R { return absl::UnimplementedError("Project not implemented"); },
            [](const ast::Filter &) -> R { return absl::UnimplementedError("Filter not implemented"); },
        };
        return std::visit(visitor, rel.node);
    }


    absl::StatusOr<std::unique_ptr<plume::parser::PhysicalPlan>> PlumeBuilder::convert() {
        auto built = build(ast_);
        if (!built.ok()) return built.status();

        const auto root_node = built->node;

        return builder_.Export(root_node);
    }
}
