#pragma once
#include "ast_builder.hpp"
#include "ast/base.hpp"
#include "plume/parser/converter.hpp"
#include "plume/parser/physical_plan.hpp"

namespace plume_spark {
    class PlumeBuilder {
    public:
        explicit PlumeBuilder(ast::RelPtr ast, const std::vector<ResolvedDataSource> &srcs, plume::parser::ConverterConfig ccfg) : ast_(std::move(ast)), srcs_(srcs), ccfg_(ccfg) {}
        absl::StatusOr<std::unique_ptr<plume::parser::PhysicalPlan>> build();
    private:
        ast::RelPtr ast_;
        const std::vector<ResolvedDataSource> &srcs_;
        plume::parser::ConverterConfig ccfg_;
    };
}
