/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include "server.hh"
#include "api_router.hh"

namespace s3 {

void server::set_routes(routes& r) {
    s3_api_router* router = new s3_api_router(_proxy, _sl_controller);
    // api_handler handle all PUT/GET/HEAD s3 requests
    r.add(new match_rule(router),operation_type::PUT);
    r.add(new match_rule(router),operation_type::GET);
    r.add(new match_rule(router),operation_type::HEAD);
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

