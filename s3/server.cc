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
#include "service/qos/service_level_controller.hh"
#include "utils/assert.hh"
#include "timeout_config.hh"
#include <cctype>
#include <string_view>
#include <utility>
#include "service/storage_proxy.hh"
#include "gms/gossiper.hh"
#include "utils/overloaded_functor.hh"
#include "server.hh"

static logging::logger s3logger("s3-server");

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace s3 {

class api_handler : public handler_base {

private:
    future<int> do_handle_api_request(std::unique_ptr<request> req) {
        sstring &origin_url = req.get()->_url;
        sstring &method = req.get()->_method;
        s3logger.info("origin_url {} ", origin_url);
        s3logger.info("method {} ", method);



        return make_ready_future<int>(0);
    }
public:
    api_handler() = default;
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
            sstring url = req->get_url();
       co_return co_await do_handle_api_request(std::move(req)).then_wrapped([url = std::move(url), rep = std::move(rep)](future<int> f) mutable {
           int ret = f.get();
             if ( ret < 0 ) {
                 s3logger.info("s3 server error wile handling {}, ret: {} ", url, ret);
                 rep->_status = reply::status_type::internal_server_error;
             }else{
                 s3logger.info("s3 server handling {} success ", url);
             }

             rep->done();
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
         });
    }
};

void server::set_routes(routes& r) {
    api_handler* req_handler = new api_handler();
    // api_handler handle all PUT/GET s3 requests
    r.add(new match_rule(req_handler),operation_type::PUT);
    r.add(new match_rule(req_handler),operation_type::GET);
}
server::server(service::storage_proxy& proxy, gms::gossiper& gossiper, qos::service_level_controller& sl_controller)
        : _http_server("http-s3")
        , _proxy(proxy)
        , _gossiper(gossiper)
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

