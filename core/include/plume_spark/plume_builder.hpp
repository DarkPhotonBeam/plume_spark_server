#pragma once
#include "plume_spark/ast_builder.hpp"
#include "plume_spark/ast/relation.hpp"
#include <plume/parser/converter.hpp>
#include <plume/parser/physical_plan.hpp>

namespace plume_spark {
    class PlumeBuilder {
    public:
        explicit PlumeBuilder(ast::RelPtr ast, plume::parser::ConverterConfig ccfg) : ast_(std::move(ast)), ccfg_(ccfg) {}
        absl::StatusOr<std::unique_ptr<plume::parser::PhysicalPlan>> build();
    private:
        ast::RelPtr ast_;
        plume::parser::ConverterConfig ccfg_;
    };
}
