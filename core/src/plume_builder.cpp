#include <plume_spark/plume_builder.hpp>
#include "plume/parser/plan_builder.hpp"
#include "plume/execution/operators/projection.hpp"
#include "plume/expression/expression.hpp"
#include "plume/catalog/parquet.hpp"
#include "plume/parser/converter.hpp"

namespace plume_spark {
    namespace pp = plume::parser;

    struct Built {
        pp::PlanBuilder::SharedOpNode node;
        std::map<ast::ColumnId, uint32_t> pos;   // ColumnId -> index in node->output_schema
    };

    absl::StatusOr<Built> build_read(pp::PlanBuilder &b, const ast::Read &rd,
                                 std::shared_ptr<plume::catalog::RemoteParquetDataSource> src,
                                 const pp::ConverterConfig &cfg) {
        const plume::Schema &file = *src->schema;              // set by Resolve()
        auto proj = std::make_shared<std::vector<uint32_t>>();
        plume::Schema scanned;
        Built out;
        for (const auto &attr : rd.output) {
            auto it = std::find_if(file.columns.begin(), file.columns.end(),
                                   [&](const plume::Column &c) { return c.name == attr.name; });
            if (it == file.columns.end())
                return absl::NotFoundError("column not in parquet file: " + attr.name);
            proj->push_back(static_cast<uint32_t>(it - file.columns.begin()));
            out.pos[attr.id] = static_cast<uint32_t>(scanned.columns.size());
            scanned.columns.push_back(*it);
        }
        out.node = b.Scan(src, scanned, std::move(proj), /*pushed_filter=*/nullptr);
        b.StageAt(out.node->stage_idx)->output_schema = scanned;

        uint64_t n = (src->cardinality_total + cfg.target_rows_per_split - 1) / cfg.target_rows_per_split;
        b.LeafStageAt(out.node->stage_idx)->source_splits =
            static_cast<uint32_t>(std::clamp<uint64_t>(n, 1, cfg.max_splits));
        return out;
    }

    absl::StatusOr<Built> build_project(pp::PlanBuilder &b, Built in, const ast::Project &p) {
        auto t = std::make_shared<plume::exec::ProjectionTemplate>();
        plume::Schema schema;
        Built out;
        for (const auto &ne : p.columns) {
            const auto *ref = std::get_if<ast::ColumnRef>(&ne.expr->node);
            if (!ref) return absl::UnimplementedError("only column refs for now");
            uint32_t pos = in.pos.at(ref->attr.id);
            plume::Column col = in.node->output_schema.columns[pos];
            col.name = ne.out.name;                               // Spark's output name
            t->projections.push_back(plume::expr::ExprNode::Reference(pos, col.type));
            out.pos[ne.out.id] = static_cast<uint32_t>(schema.columns.size());
            schema.columns.push_back(std::move(col));
        }
        out.node = b.After(std::move(in.node), std::move(t), std::move(schema));
        return out;
    }

    namespace {
        // Builds `rel` bottom-up: inputs first, then the operator on top.
        absl::StatusOr<Built> build_rel(pp::PlanBuilder &b, const ast::Rel &rel,
                                        const std::vector<ResolvedDataSource> &srcs,
                                        const pp::ConverterConfig &cfg) {
            return std::visit(util::overloaded{
                [&](const ast::Read &r) -> absl::StatusOr<Built> {
                    if (srcs.empty() || !srcs[0].plume_src)
                        return absl::InternalError("read without a resolved data source");
                    return build_read(b, r, srcs[0].plume_src, cfg);   // TODO: pick the source belonging to r
                },
                [&](const ast::Project &p) -> absl::StatusOr<Built> {
                    auto in = build_rel(b, *p.input, srcs, cfg);       // build the input first
                    if (!in.ok()) return in.status();
                    return build_project(b, *std::move(in), p);
                },
                [](const ast::Filter &) -> absl::StatusOr<Built> {
                    return absl::UnimplementedError("Filter not supported yet");
                },
                [](const ast::UnresolvedProject &) -> absl::StatusOr<Built> {
                    return absl::InternalError("UnresolvedProject should have been resolved by AstBuilder");
                },
            }, rel.node);
        }
    }

    absl::StatusOr<std::unique_ptr<plume::parser::PhysicalPlan>> PlumeBuilder::build() {
        pp::PlanBuilder b{ccfg_.optimize_remote_fetching};
        auto root = build_rel(b, *ast_, srcs_, ccfg_);
        if (!root.ok()) return root.status();
        return b.Export(root->node);
    }
}
