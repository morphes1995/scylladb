#pragma once

#include "api_router.hh"
#include "log.hh"


namespace s3{
class bucket_handler{
public:
    future<s3_op_handle_result> operator()(s3_req_state&& req_state);

};


}