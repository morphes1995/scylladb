/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/net/dns.hh>
#include "controller.hh"
#include "db/config.hh"
#include "service/qos/service_level_controller.hh"
#include "server.hh"

using namespace seastar;

namespace s3 {

static logging::logger logger("s3_controller");

controller::controller(
        sharded<gms::gossiper>& gossiper,
        sharded<service::storage_proxy>& proxy,
        sharded<qos::service_level_controller>& sl_controller,
        const db::config& config,
        seastar::scheduling_group sg)
    : protocol_server(sg)
    , _gossiper(gossiper)
    , _proxy(proxy)
    , _sl_controller(sl_controller)
    , _config(config)
{
}

sstring controller::name() const {
    return "s3_controller";
}

sstring controller::protocol() const {
    return "s3";
}

sstring controller::protocol_version() const {
    return "aws-s3";
}

std::vector<socket_address> controller::listen_addresses() const {
    return _listen_addresses;
}

future<> controller::start_server() {
    seastar::thread_attributes attr;
    attr.sched_group = _sched_group;
    return seastar::async(std::move(attr), [this] {
        _listen_addresses.clear();

        // Create an smp_service_group to be used for limiting the
        // concurrency when forwarding s3 request between shards - if necessary for LWT.
        smp_service_group_config c;
        c.max_nonlocal_requests = 5000;
        _ssg = create_smp_service_group(c).get();

        net::inet_address addr = utils::resolve(_config.s3_address, std::nullopt).get();

        _server.start(std::ref(_proxy), std::ref(_gossiper), std::ref(_sl_controller)).get();

        // Note: from this point on, if start_server() throws for any reason,
        // it must first call stop_server() to stop the executor and server
        // services we just started - or Scylla will cause an assertion
        // failure when the controller object is destroyed in the exception
        // unwinding.
        std::optional<uint16_t> s3_http_port;
        if (_config.s3_http_port()) {
            s3_http_port = _config.s3_http_port();
            _listen_addresses.push_back({addr, *s3_http_port});
        }

        _server.invoke_on_all(
                [addr, s3_http_port] (server& server) mutable {
            return server.init(addr, s3_http_port);
        }).handle_exception([this, addr, s3_http_port] (std::exception_ptr ep) {
            logger.error("Failed to set up s3 HTTP server on {} port {}, {}",
                    addr, s3_http_port, ep);
            return stop_server().then([ep = std::move(ep)] { return make_exception_future<>(ep); });
        }).then([addr, s3_http_port] {
            logger.info("s3 http server listening on {}, HTTP port {}",
                    addr, s3_http_port );
        }).get();
    });
}

future<> controller::stop_server() {
    return seastar::async([this] {
        if (!_ssg) {
            return;
        }
        _server.stop().get();
        _listen_addresses.clear();
        destroy_smp_service_group(_ssg.value()).get();
    });
}

future<> controller::request_stop_server() {
    return with_scheduling_group(_sched_group, [this] {
        return stop_server();
    });
}

}
