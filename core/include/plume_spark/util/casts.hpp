#pragma once
#include <string>
#include <google/protobuf/text_format.h>

namespace plume_spark::util {
    inline std::string msg_to_text(const google::protobuf::Message &m) {
        std::string out;
        google::protobuf::TextFormat::Printer p;
        p.SetUseUtf8StringEscaping(true);
        bool r = p.PrintToString(m, &out);
        return out;
    }

    inline grpc::Status to_grpc(const absl::Status& s) {
        return {static_cast<grpc::StatusCode>(s.code()), std::string(s.message())};
    }
}
