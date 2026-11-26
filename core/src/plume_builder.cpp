#include "plume_spark/plume_builder.hpp"
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


}
