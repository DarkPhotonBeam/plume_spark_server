#pragma once
#include <string>

#include "spark/connect/base.pb.h"
#include "spark/connect/expressions.pb.h"
#include "spark/connect/relations.pb.h"

namespace plume_spark::util {
    // Compact, human-readable rendering of the unresolved Spark Connect plan, one node per line:
    //
    //   Project [order_id, amount, status] @2
    //   └─ Filter (is_priority AND (amount >= 50.0)) @1
    //      └─ Read parquet [/data/small.parquet] @0
    //
    // "@n" is the client-assigned plan_id (RelationCommon.plan_id / UnresolvedAttribute.plan_id).
    // Relation and expression kinds without a dedicated rendering fall back to their proto
    // type name plus a shortened text dump, so nothing is silently dropped.
    std::string format_plan(const ::spark::connect::Plan &plan);          // multi-line tree
    std::string format_relation(const ::spark::connect::Relation &rel);   // multi-line tree
    std::string format_expression(const ::spark::connect::Expression &e); // one line
}
