/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "log.hh"
#include <fmt/ranges.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/short_streams.hh>
#include "seastarx.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "service/qos/service_level_controller.hh"
#include "service/client_state.hh"
#include "utils/assert.hh"
#include "utils/hashers.hh"
#include "utils/coarse_steady_clock.hh"
#include "timeout_config.hh"
#include <cctype>
#include <string_view>
#include <utility>
#include "service/storage_proxy.hh"
#include "utils/overloaded_functor.hh"
#include "server.hh"
#include "utils.hh"
#include "s3_op_handle_result.hh"

static logging::logger s3logger("s3-server");

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace s3 {

enum http_op {
  OP_GET,
  OP_PUT,
  OP_DELETE,
  OP_HEAD,
  OP_POST,
  OP_COPY,
  OP_OPTIONS,
  OP_UNKNOWN,
};
http_op op_from_method(const char *method)
{
  if (!method)
    return OP_UNKNOWN;
  if (strcmp(method, "GET") == 0)
    return OP_GET;
  if (strcmp(method, "PUT") == 0)
    return OP_PUT;
  if (strcmp(method, "DELETE") == 0)
    return OP_DELETE;
  if (strcmp(method, "HEAD") == 0)
    return OP_HEAD;
  if (strcmp(method, "POST") == 0)
    return OP_POST;
  if (strcmp(method, "COPY") == 0)
    return OP_COPY;
  if (strcmp(method, "OPTIONS") == 0)
    return OP_OPTIONS;

  return OP_UNKNOWN;
}

class s3_api_handler : public handler_base {
    service::storage_proxy& _proxy;
    [[maybe_unused]]qos::service_level_controller& _sl_controller;

private:
    future<s3_op_handle_result> do_handle_api_request(std::unique_ptr<request> req) {
        s3_op_handle_result res;

        sstring &origin_url = req.get()->_url;
        sstring &method = req.get()->_method;
        s3logger.debug("origin_url {} ", origin_url);
        s3logger.debug("method {} ", method);

        sstring bucket;
        sstring req_url = req->parse_query_param().c_str();
        req_url = req_url.substr(1);
        int pos = req_url.find('/');
        if (pos >= 0) {
            bucket = req_url.substr(0, pos);
        } else {
            bucket = req_url;
        }
        sstring object = req_url.substr(pos+1);

        if(!object.empty()){
            // object op
            s3logger.debug("{} object {} in bucket {}, ", method, object, bucket);

            switch (op_from_method(method.c_str())){
                case OP_PUT:
                    co_return co_await handle_object_put(bucket, object, std::move(req));
                    break;
                case OP_GET:
                case OP_HEAD:
                    co_return co_await handle_object_get(bucket, object, std::move(req));
                    break;
                default:
                    s3logger.debug("{} {} not implemented ", method, req_url);
            }

        }else{
            if(req->query_parameters.find("location") != req->query_parameters.end()){
                s3logger.debug("handle get bucket[{}] location request ", bucket);
                res.mime_type = mime_type_xml;
                res.resp_body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                                "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">default</LocationConstraint>\n";
            }
        }

        res.status = reply::status_type::ok;
        co_return res;
    }

    future<s3_op_handle_result> handle_object_put(sstring& bucket, sstring& object, std::unique_ptr<request> req){
        s3_op_handle_result res;

        int32_t size = req->content_length;
        s3logger.debug("object {} in bucket {}, size: {}", object, bucket, size);
        SCYLLA_ASSERT(req->content_stream);
        auto payload = co_await util::read_entire_stream_contiguous(*req->content_stream);
        md5_hasher h;
        h.update(payload.c_str(), payload.size());
        auto md5sum = to_sstring(h.finalize());
        s3logger.debug("object {} in bucket {}, md5 hash: {}, size {}", object, bucket, md5sum, payload.size());
        res.header["Etag"] = md5sum;

        auto payload_bytes = to_bytes(std::move(payload));
        schema_ptr schema;
        try{
           schema =  _proxy.data_dictionary().find_schema("s3", "object_metadata");
        }catch (data_dictionary::no_such_column_family){
            res.status = reply::status_type::internal_server_error;
            co_return res;
        }

        std::vector<bytes> raw_pk;
        raw_pk.push_back(to_bytes(bucket));
        raw_pk.push_back(int32_type->decompose(bucket_shard(object, 1024)));
        partition_key pk = partition_key::from_exploded(raw_pk);
        std::vector<bytes> raw_ck;
        raw_ck.push_back(to_bytes(object));
        raw_ck.push_back(long_type->decompose(object_version_clock::now()));
        clustering_key ck = clustering_key::from_exploded(raw_ck);

        mutation m(schema, pk);
        auto& row = m.partition().clustered_row(*schema, ck);
        api::timestamp_type now = api::new_timestamp();
        for (const column_definition& cdef : schema->regular_columns()) {
            if(cdef.name_as_text() == "size"){
                row.cells().apply(cdef, atomic_cell::make_live(*cdef.type, now, std::move(cdef.type->decompose(size))));
            }else if(cdef.name_as_text() == "payload"){
                row.cells().apply(cdef, atomic_cell::make_live(*cdef.type, now, std::move(payload_bytes)));
            }
        }

        co_await _proxy.mutate(std::vector<mutation>{std::move(m)},
                            db::consistency_level::LOCAL_QUORUM,
                            db::no_timeout, nullptr, empty_service_permit(),
                            db::allow_per_partition_rate_limit::yes);
        res.status  = reply::status_type::ok;
        co_return res;
    }

    future<s3_op_handle_result> handle_object_get(sstring& bucket, sstring& object, std::unique_ptr<request> req){
        s3_op_handle_result res;

        schema_ptr schema;
        try{
           schema =  _proxy.data_dictionary().find_schema("s3", "object_metadata");
        }catch (data_dictionary::no_such_column_family){
            res.status =reply::status_type::internal_server_error;
            co_return res;
        }

        // assemble pk and ck
        std::vector<bytes> raw_pk;
        raw_pk.push_back(to_bytes(bucket));
        raw_pk.push_back(int32_type->decompose(bucket_shard(object, 1024)));
        partition_key pk = partition_key::from_exploded(raw_pk);
        std::vector<bytes> raw_ck;
        raw_ck.push_back(to_bytes(object));
        clustering_key ck = clustering_key::from_exploded(raw_ck);

        dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*schema, pk))};
        std::vector<query::clustering_range> ck_bounds;
        // we get all versions of the object
        ck_bounds.push_back(query::clustering_range::make_starting_with(query::clustering_range::bound(ck)));

        auto regular_columns = boost::copy_range<query::column_id_vector>(
            schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
        auto selection = cql3::selection::selection::wildcard(schema);
        auto partition_slice = query::partition_slice(std::move(ck_bounds), {}, std::move(regular_columns), selection->get_query_options());
        auto command = ::make_lw_shared<query::read_command>(schema->id(),
                                                             schema->version(),
                                                             partition_slice,
                                                             _proxy.get_max_result_size(partition_slice),
                                                             query::tombstone_limit(_proxy.get_tombstone_limit()));

        co_return co_await _proxy.query(schema,
                              std::move(command),
                              std::move(partition_ranges),
                              db::consistency_level::LOCAL_QUORUM,
                              service::storage_proxy::coordinator_query_options(db::no_timeout, empty_service_permit(), service::client_state::for_internal_calls()))
                              .then([bucket, object, schema,
                                     partition_slice = std::move(partition_slice),
                                     selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) mutable {
                                        s3_op_handle_result res;
                                        // handle query result
                                        cql3::selection::result_set_builder builder(*selection, gc_clock::now());
                                        query::result_view::consume(*qr.query_result, partition_slice, cql3::selection::result_set_builder::visitor(builder, *schema, *selection));

                                        auto result_set = builder.build();
                                        if (result_set->empty()) {
                                            s3logger.debug("object {}/{} not found", bucket, object);
                                            res.status = reply::status_type::not_found;
                                            return make_ready_future<s3_op_handle_result>(std::move(res));
                                        }

                                        auto row_cnt = result_set->size();
                                        if(row_cnt > 1){
                                            s3logger.warn("object {} in bucket  {}  has {} versions to purge ", object, bucket, row_cnt -1);
                                        }

                                        auto& row = *result_set->rows().begin(); // we only use the latest obj version
                                        const auto& columns = selection->get_columns();
                                        auto column_it = columns.begin();
                                        for (const managed_bytes_opt& cell : row) {
                                            std::string column_name = (*column_it)->name_as_text();

                                            cell->with_linearized([&] (bytes_view c) {

                                                if(column_name == "payload"){
                                                    sstring payload = sstring(to_sstring_view(c)); // convert from hex view to string view
                                                    md5_hasher h;
                                                    h.update(payload.c_str(), payload.size());
                                                    auto md5sum = to_sstring(h.finalize());
                                                    res.header["Etag"] = "\"" + md5sum + "\"";
                                                    s3logger.debug("object {} in bucket {},  md5:{} ", object, bucket, res.header["Etag"]);
                                                    res.resp_body = std::move(payload);
                                                }else if(column_name == "time"){
                                                    int64_t nanoseconds = value_cast<int64_t>(long_type->deserialize(c));
                                                    res.header["Last-Modified"] = format_time_gmt(nanoseconds);
                                                }

                                            });

                                            column_it ++;
                                        }

                                        res.status = reply::status_type::ok;
                                        return make_ready_future<s3_op_handle_result>(std::move(res));
                                    });

    }

public:
    s3_api_handler(service::storage_proxy& proxy,
                   qos::service_level_controller& sl_controller)
    :_proxy(proxy), _sl_controller(sl_controller){}

    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
            sstring url = req->get_url();
             co_return co_await do_handle_api_request(std::move(req)).then_wrapped([url = std::move(url), rep = std::move(rep)](future<s3_op_handle_result> f) mutable {
               s3_op_handle_result res = f.get();
               s3logger.debug("s3 server handling {} finished, ret: {}  ", url, res.status);

               rep->_status = res.status;
               for(auto &item: res.header){
                   rep->add_header(item.first, item.second);
               }
               rep->write_body(res.mime_type, res.resp_body);
               rep->content_length = res.resp_body.size();

               rep->done();
               return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
         });
    }
};

void server::set_routes(routes& r) {
    s3_api_handler* req_handler = new s3_api_handler(_proxy, _sl_controller);
    // api_handler handle all PUT/GET/HEAD s3 requests
    r.add(new match_rule(req_handler),operation_type::PUT);
    r.add(new match_rule(req_handler),operation_type::GET);
    r.add(new match_rule(req_handler),operation_type::HEAD);
}
server::server(service::storage_proxy& proxy, qos::service_level_controller& sl_controller)
        : _http_server("http-s3")
        , _proxy(proxy)
        , _sl_controller(sl_controller)
        , _enabled_servers{}
        , _pending_requests{}
        {
        }

future<> server::init(net::inet_address addr, std::optional<uint16_t> port) {
    return seastar::async([this, addr, port] {
        set_routes(_http_server._routes);
        _http_server.set_content_length_limit(server::content_length_limit);
        _http_server.set_content_streaming(true);
        _http_server.listen(socket_address{addr, *port}).get();
        _enabled_servers.push_back(std::ref(_http_server));
    });
}

future<> server::stop() {
    return parallel_for_each(_enabled_servers, [] (http_server& server) {
        return server.stop();
    });
}

}

