#include "blobstore.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include "utils/disk-error-handler.hh"
#include "checked-file-impl.hh"
#include "log.hh"

using namespace seastar;

static logging::logger blogger("blobstore");

const std::string db::blob_descriptor::SEPARATOR("-");
const std::string db::blob_descriptor::FILENAME_PREFIX("Blob" + SEPARATOR);
const std::string db::blob_descriptor::SUFFIX(".data");

db::blob_descriptor::blob_descriptor(const std::string& filename){
    _filename = filename;
}

db::blobstore_config db::blobstore_config::from_db_config(const db::config& cfg){
    db::blobstore_config c;
    c.blobstore_dir_location = cfg.blobstore_directory();
    c.blob_size_in_mb = 1024;

    return c;
}


future<db::blobstore> db::blobstore::create_blobstore(blobstore_config &cfg){
    blobstore bs(std::move(cfg));
    co_await bs.init();
    co_return bs;
}

db::blobstore::blobstore(blobstore_config &&cfg){
    _cfg = std::move(cfg);
}

future<> db::blobstore::init(){
    blogger.info("blobstore init finish.");
    return make_ready_future();
}

future<std::vector<db::blob_descriptor>>
db::blobstore::list_blobs(sstring &dirname) const {
    auto blob_dir = co_await open_checked_directory(commit_error_handler, dirname);
    std::vector<db::blob_descriptor> result;

    auto h = blob_dir.list_directory([&](directory_entry de) -> future<> {
        auto type = de.type;
        if (!type && !de.name.empty()) {
            type = co_await file_type(dirname + "/" + de.name);
        }
        if (type == directory_entry_type::regular && de.name[0] != '.') {
            try {
                result.emplace_back(de.name);
            } catch (std::domain_error& e) {
                blogger.warn("list blobs error: {}", e.what());
            }
        }
    });
    co_await h.done(); // wait for list finish
    co_return result;
}