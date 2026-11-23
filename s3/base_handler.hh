#pragma once
#include "api_router.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "service/client_state.hh"
#include "log.hh"


namespace s3{

class base_handler{

// Type alias for the result handler callback
using row_result_handler = seastar::noncopyable_function<void(
    const std::vector<managed_bytes_opt>& row,
    const cql3::selection::selection& selection,
    s3_op_handle_result& res)>;

using row_mutation_handler = seastar::noncopyable_function<void(
    const schema& s,
    deletable_row& row,
    api::timestamp_type now)>;

public:
    base_handler(service::storage_proxy& proxy): _proxy(proxy){}

protected:
    service::storage_proxy& _proxy;
    virtual future<s3_op_handle_result> operator()(s3_req_state&& req_state) = 0;

    future<s3_op_handle_result> query_object(
        const sstring& bucket,
        const sstring& object,
        const std::vector<sstring>& column_names, // Empty = wildcard (all columns)
        row_result_handler handle_row);


    future<s3_op_handle_result> mutate_object(
        const sstring& bucket,
        const sstring& object,
        int64_t version,
        row_mutation_handler populate_row);


    template <typename Fn>
    void for_each_present_cell(
        const std::vector<managed_bytes_opt>& row,
        const cql3::selection::selection& selection,
        Fn&& fn){
        const auto& columns = selection.get_columns();
        auto column_it = columns.begin();
        for (const managed_bytes_opt& cell : row) {
            if (cell) {
                std::string column_name = (*column_it)->name_as_text();
                cell->with_linearized([&] (bytes_view c) {
                    fn(column_name, c);
                });
            }
            ++column_it;
        }
    }

    void populate_common_headers(
        const sstring& bucket,
        const sstring& object,
        const std::string& column_name,
        bytes_view c,
        s3_op_handle_result& res);

    schema_ptr get_object_metadata_schema() const;

    partition_key make_object_pk(const sstring& bucket, const sstring& object) const;
    clustering_key make_object_ck(const sstring& object, std::optional<int64_t> version) const;

};

}