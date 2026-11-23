#include "object_handler.hh"
#include "api_router.hh"
#include "utils/hashers.hh"
#include "utils.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "service/client_state.hh"

namespace s3{

static logging::logger ologger("s3_object_handler");

future<s3_op_handle_result>
object_handler::operator()(s3_req_state&& req_state){
    switch (op_from_method(req_state.method.c_str())){
        case OP_PUT:
            co_return co_await handle_object_put(req_state.bucket, req_state.object, std::move(req_state.req));
        case OP_HEAD:
            co_return co_await handle_object_head(req_state.bucket, req_state.object, std::move(req_state.req));
        case OP_GET:
            if (req_state.req->query_parameters.contains("acl")){
                ologger.debug("get acl of object {} in bucket {}, ", req_state.object, req_state.bucket);
            }else{
                co_return co_await handle_object_get(req_state.bucket, req_state.object, std::move(req_state.req));
            }
            break;
        default:
            ologger.debug("{} {} not implemented ", req_state.method, req_state.req->get_url());
    }

    s3_op_handle_result res;
    res.status = reply::status_type::ok;
    co_return res;
}


future<object_handler::payload_info>
object_handler::read_and_hash_payload(input_stream<char>& stream, int32_t size) {
    auto reader = fragmented_temporary_buffer::reader();
    auto buf = co_await reader.read_exactly(stream, size);
    auto view = fragmented_temporary_buffer::view(buf);

    md5_hasher h;
    for (auto&& frag : view) {
        ologger.debug("frag size {}", frag.size());
        h.update(reinterpret_cast<const char*>(frag.data()), frag.size());
    }

    co_return payload_info{
        .buffer = std::move(buf),
        .md5sum = to_sstring(h.finalize())
    };
}

void object_handler::apply_live_cell(
    const schema& s,
    deletable_row& row,
    const bytes& col_name,
    api::timestamp_type now,
    auto&& value) {
    if (const column_definition* cdef = s.get_column_definition(col_name)) {
        row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, now, std::forward<decltype(value)>(value)));
    }else{
        ologger.debug("col {} didn't exist in {}", col_name, s.cf_name());
    }
}


future<s3_op_handle_result>
object_handler::handle_object_put(sstring& bucket, sstring& object, std::unique_ptr<request> req) {
    int32_t size = req->content_length;
    auto payload = co_await read_and_hash_payload(*req->content_stream, size);
    auto payload_view = fragmented_temporary_buffer::view(payload.buffer);
    ologger.debug("object {} in bucket {}, md5 hash: {}, size {}", object, bucket, payload.md5sum, payload_view.size_bytes());
    auto res = co_await mutate_object(
        bucket,
        object,
        object_version_clock::now(),
        [&] (const schema& s, deletable_row& row, api::timestamp_type now) {
            apply_live_cell(s, row, bytes("size"), now, int32_type->decompose(size));
            apply_live_cell(s, row, bytes("etag"), now, utf8_type->decompose(payload.md5sum));
            apply_live_cell(s, row, bytes("payload"), now, payload_view);
            apply_live_cell(s, row, bytes("inlined"), now, boolean_type->decompose(true));
        });

    if (res.status == reply::status_type::ok) {
        res.header["Etag"] = std::move(payload.md5sum);
    }
    co_return res;
}


future<s3_op_handle_result>
object_handler::handle_object_get(sstring& bucket, sstring& object, std::unique_ptr<request> req) {
    return query_object(bucket, object, {}/*wildcard*/,
                        [&bucket, &object, this] (const auto& row, const auto& selection, auto& res) {
        for_each_present_cell(row, selection, [&] (const std::string& column_name, bytes_view c) {
            if (column_name == "payload") {
                sstring payload = sstring(to_sstring_view(c));
                ologger.debug("object {} in bucket {}, payload: {}", object, bucket, payload);
                res.resp_body = std::move(payload);
            } else {
                populate_common_headers(bucket, object, column_name, c, res);
            }
        });
    } /* handle_row */);
}

future<s3_op_handle_result>
object_handler::handle_object_head(sstring& bucket, sstring& object, std::unique_ptr<request> req) {
    return query_object(bucket, object, {"size", "etag", "time"},
                        [&bucket, &object, this] (const auto& row, const auto& selection, auto& res) {
        for_each_present_cell(row, selection, [&] (const std::string& column_name, bytes_view c) {
            populate_common_headers(bucket, object, column_name, c, res);
        });
    });
}

}