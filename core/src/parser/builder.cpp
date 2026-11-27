#include "plume_spark/parser/builder.hpp"
#include "plume/parser/plan_builder.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/expression/expression.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/parser/converter.hpp"
#include "plume_spark/util/casts.hpp"

namespace plume_spark::parser {
    namespace pp = plume::parser;
    namespace pe = plume::expr;
    namespace pexe = plume::exec;

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

        std::map<ast::ColumnId, uint32_t> pos;

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
                std::optional<ast::ColumnId> found = std::nullopt;
                for (auto c : r.output) {
                    if (c.name != col.name) continue;
                    if (found.has_value()) {
                        return absl::InternalError(absl::StrCat("ambiguous column name mapping for '", col.name, "'"));
                    }
                    found = c.id;
                }
                if (!found.has_value()) {
                    return absl::NotFoundError(absl::StrCat("no ColumnId found for '", col.name, "'"));
                }
                pos[found.value()] = s;
                proj->projections.push_back(pe::ExprNode::Reference(s, col.type));
            }
            cur = builder_.After(std::move(cur), std::move(proj), schema_full);
        }
        leaf_stage->source_splits = split_count(plume_src->cardinality_total);

        return Built{
            .node = cur,
            .pos = std::move(pos),
        };
    }

    absl::StatusOr<PlumeBuilder::Built> PlumeBuilder::build_project(const ast::Project &p) {
        auto prev = build(*p.input);
        if (!prev.ok()) return prev.status();

        auto t = std::make_shared<pexe::ProjectionTemplate>();
        t->projections.reserve(p.columns.size());
        for (auto &ne : p.columns) {

        }

        return absl::UnimplementedError("Project not implemented");
    }

    absl::StatusOr<PlumeBuilder::Built> PlumeBuilder::build_filter(const ast::Filter &f) {
        auto prev = build(*f.input);
        if (!prev.ok()) return prev.status();



        return absl::UnimplementedError("Filter not implemented");
    }

    absl::StatusOr<PlumeBuilder::Built> PlumeBuilder::build(const ast::Relation &rel) {
        using R = absl::StatusOr<Built>;
        auto visitor = util::overloaded{
            [this](const ast::Read &r) -> R { return build_read(r); },
            [this](const ast::Project &p) -> R { return build_project(p); },
            [this](const ast::Filter &f) -> R { return build_filter(f); },
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
