#pragma once

#include <seastar/http/httpd.hh>
#include <string_view>
#include "log.hh"
#include <seastar/http/short_streams.hh>
#include "s3_op_handle_result.hh"
#include "seastar/core/sstring.hh"
#include "service/qos/service_level_controller.hh"
#include "service/storage_proxy.hh"

using namespace httpd;
using request = http::request;
using reply = http::reply;

namespace s3{

struct s3_req_state{
    std::unique_ptr<request> req;
    sstring method;
    sstring bucket;
    sstring object;
};

class s3_api_router : public handler_base {
    service::storage_proxy& _proxy;
    [[maybe_unused]]qos::service_level_controller& _sl_controller;

private:
    s3_req_state init_req_state(std::unique_ptr<request> req);
    future<s3_op_handle_result> do_handle_api_request(std::unique_ptr<request> req);

public:
    s3_api_router(service::storage_proxy& proxy,
                   qos::service_level_controller& sl_controller)
    :_proxy(proxy), _sl_controller(sl_controller){}

    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
};
}

