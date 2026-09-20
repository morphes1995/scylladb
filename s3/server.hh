/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/tls.hh>
#include <optional>
#include "service/qos/service_level_controller.hh"
#include "utils/small_vector.hh"
#include "utils/updateable_value.hh"
#include <seastar/core/units.hh>

namespace s3 {
class server {
    static constexpr size_t content_length_limit = 16*MB;
    httpd::http_server _http_server;
    [[maybe_unused]]service::storage_proxy& _proxy;
    [[maybe_unused]]gms::gossiper& _gossiper;
    [[maybe_unused]]qos::service_level_controller& _sl_controller;
    utils::small_vector<std::reference_wrapper<seastar::httpd::http_server>, 2> _enabled_servers;
    seastar::gate _pending_requests;

public:
    server(service::storage_proxy& proxy, gms::gossiper& gossiper, qos::service_level_controller& sl_controller);

    future<> init(net::inet_address addr, std::optional<uint16_t> port);
    future<> stop();
private:
    void set_routes(seastar::httpd::routes& r);
};

}

