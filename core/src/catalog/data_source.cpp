#include "plume_spark/catalog/data_source.hpp"

namespace plume_spark::catalog {
    absl::StatusOr<Schema> schema_from_arrow(const arrow::Schema &schema) {
        Schema out;
        for (auto &f : schema.fields()) {
            auto t = types::DataType::from_arrow(*f->type());
            if (!t.ok()) return t.status();
            out.push_back(types::Field{
                .name = f->name(),
                .type = *t,
                .nullable = f->nullable(),
            });
        }
        return out;
    }
}