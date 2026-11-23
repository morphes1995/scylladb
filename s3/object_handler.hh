#pragma once
#include "api_router.hh"
#include "base_handler.hh"
#include "log.hh"

namespace s3{

class object_handler : public base_handler{

public:
    struct payload_info {
        fragmented_temporary_buffer buffer;
        sstring md5sum;
    };
    object_handler(service::storage_proxy& proxy): base_handler(proxy){}
    future<s3_op_handle_result> operator()(s3_req_state&& req_state) override;

private:
    future<s3_op_handle_result> handle_object_put(sstring& bucket, sstring& object, std::unique_ptr<request> req);
    future<s3_op_handle_result> handle_object_get(sstring& bucket, sstring& object, std::unique_ptr<request> req);
    future<s3_op_handle_result> handle_object_head(sstring& bucket, sstring& object, std::unique_ptr<request> req);

    future<payload_info> read_and_hash_payload(input_stream<char>& stream, int32_t size);
    void apply_live_cell(
        const schema& s,
        deletable_row& row,
        const bytes& col_name,
        api::timestamp_type now,
        auto&& value);
};

} // namespace end