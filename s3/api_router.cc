#include "api_router.hh"
#include "object_handler.hh"
#include "bucket_handler.hh"

namespace s3 {

static logging::logger arlogger("s3_api_router");

future<std::unique_ptr<reply>>
s3_api_router::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    sstring url = req->get_url();
     co_return co_await do_handle_api_request(std::move(req)).then_wrapped([url = std::move(url), rep = std::move(rep)](future<s3_op_handle_result> f) mutable {
       s3_op_handle_result res = f.get();
       rep->_status = res.status;
       for(auto &item: res.header){
           rep->add_header(item.first, item.second);
       }
       rep->write_body(res.mime_type, res.resp_body);
       rep->content_length = res.resp_body.size();
       arlogger.debug("s3 server handling {} finished, ret: {} , body content_length {} ", url, res.status, rep->content_length);
       rep->done();
       return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
 });
}


s3_req_state
s3_api_router::init_req_state(std::unique_ptr<request> req){
    s3_req_state state;
    state.method = req.get()->_method;
    sstring req_url = req->parse_query_param().c_str();
    arlogger.debug("s3_api_router init req_state of  {} method {}", req_url, state.method);
    req_url = req_url.substr(1);
    int pos = req_url.find('/');
    if (pos >= 0) {
        state.bucket = req_url.substr(0, pos);
    } else {
        state.bucket = req_url;
    }
    state.object = req_url.substr(pos+1);
    state.req = std::move(req);
    return state;
}

future<s3_op_handle_result>
s3_api_router::do_handle_api_request(std::unique_ptr<request> req) {
    s3_op_handle_result res;
    s3_req_state state = init_req_state(std::move(req));
    if(!state.object.empty()){
        // object op
        object_handler handle_object(_proxy);
        co_return co_await handle_object(std::move(state));
    }else{
        // bucket op
        bucket_handler handle_bucket;
        co_return co_await handle_bucket(std::move(state));
    }

    res.status = reply::status_type::ok;
    co_return res;
}

}

