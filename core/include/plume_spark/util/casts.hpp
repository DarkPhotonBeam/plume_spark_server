#pragma once
#include <string>
#include <google/protobuf/text_format.h>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <grpcpp/support/status.h>
#include <absl/status/status.h>
#include <google/protobuf/message.h>

namespace plume_spark::util {
    template<class... Ts>
    struct overloaded : Ts... {
        using Ts::operator()...;
    };

    template <class... Ts>
    overloaded(Ts...) -> overloaded<Ts...>;

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

    inline arrow::Result<std::string> to_ipc_stream(const arrow::RecordBatch &batch) {
        ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
        ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, batch.schema()));
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(batch));
        ARROW_RETURN_NOT_OK(writer->Close());
        ARROW_ASSIGN_OR_RAISE(auto buf, sink->Finish());
        return buf->ToString();
    }

    inline arrow::Result<std::shared_ptr<arrow::RecordBatch>> make_range_batch(int64_t n) {
        arrow::Int64Builder b;
        ARROW_RETURN_NOT_OK(b.Reserve(n));
        for (int64_t i = 0; i < n; ++i) b.UnsafeAppend(i);
        ARROW_ASSIGN_OR_RAISE(auto arr, b.Finish());
        auto schema = arrow::schema({arrow::field("id", arrow::int64(), /*nullable=*/false)});
        return arrow::RecordBatch::Make(schema, n, {arr});
    }
}
