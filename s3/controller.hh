/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include "protocol_server.hh"

namespace service {
class storage_proxy;
}

namespace db {
class config;
}

namespace gms {
class gossiper;
}

namespace qos {
class service_level_controller;
}

namespace s3 {

using namespace seastar;

class server;

class controller : public protocol_server {
    sharded<gms::gossiper>& _gossiper;
    sharded<service::storage_proxy>& _proxy;
    sharded<qos::service_level_controller>& _sl_controller;
    const db::config& _config;
    std::vector<socket_address> _listen_addresses;
    sharded<server> _server;
    std::optional<smp_service_group> _ssg;

public:
    controller(
        sharded<gms::gossiper>& gossiper,
        sharded<service::storage_proxy>& proxy,
        sharded<qos::service_level_controller>& sl_controller,
        const db::config& config,
        seastar::scheduling_group sg);

    virtual sstring name() const override;
    virtual sstring protocol() const override;
    virtual sstring protocol_version() const override;
    virtual std::vector<socket_address> listen_addresses() const override;
    virtual future<> start_server() override;
    virtual future<> stop_server() override;
    virtual future<> request_stop_server() override;
};

}
