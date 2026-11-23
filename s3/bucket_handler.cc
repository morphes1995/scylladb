#include "bucket_handler.hh"

namespace s3{

static logging::logger blogger("s3_bucket_handler");

future<s3_op_handle_result>
bucket_handler::operator()(s3_req_state&& req_state){
     s3_op_handle_result res;
     if(req_state.req->query_parameters.find("location") != req_state.req->query_parameters.end()){
        blogger.debug("handle get bucket[{}] location request ", req_state.bucket);
        res.mime_type = mime_type_xml;
        res.resp_body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                        "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">default</LocationConstraint>\n";
    }


     res.status = reply::status_type::ok;
     co_return res;
}


}