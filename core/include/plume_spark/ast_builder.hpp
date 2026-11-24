#pragma once
#include <plume_spark/ast/base.hpp>

#include "spark/connect/relations.pb.h"

namespace plume_spark {
    ast::RelPtr build_from_proto(const ::spark::connect::Relation &root);
}
