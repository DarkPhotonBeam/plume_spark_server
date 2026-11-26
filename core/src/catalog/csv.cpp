#include "plume_spark/catalog/data_source.hpp"

namespace plume_spark::catalog {

    absl::StatusOr<std::shared_ptr<const DataSource>> DataSource::csv(std::string name, std::vector<std::string> urls) {
        return absl::UnimplementedError("csv data source not implemented");
    }

}