# pragma once

#include "map"
#include "seastar/core/sstring.hh"
#include "seastar/http/reply.hh"

using namespace seastar;

namespace s3{


const sstring  mime_type_xml = "application/xml";

struct s3_op_handle_result{
    http::reply::status_type status;

    sstring mime_type;
    sstring resp_body;

    std::map<sstring, sstring> header;
};


}

