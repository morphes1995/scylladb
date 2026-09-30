#include "blobstore.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include <boost/regex.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/reversed.hpp>

#include "utils/disk-error-handler.hh"
#include "utils/runtime.hh"
#include "checked-file-impl.hh"
#include "log.hh"

using namespace seastar;

static logging::logger bslogger("blobstore");

const std::string db::blob_descriptor::SEPARATOR("-");
const std::string db::blob_descriptor::FILENAME_PREFIX("Blob" + SEPARATOR);
const std::string db::blob_descriptor::SUFFIX(".data");

//Ex: Blob-12345.log
static const boost::regex filename_match(db::blob_descriptor::FILENAME_PREFIX + "(\\d+)" + db::blob_descriptor::SUFFIX);
db::blob_descriptor::blob_descriptor(const std::string& filename){
    boost::smatch m;
    auto cbegin = filename.cbegin();
    if (!boost::regex_match(cbegin, filename.cend(), m, filename_match)) {
        throw std::domain_error("Cannot parse the name of blob file: " + filename);
    }

    blob_id_type id = std::stoull(m[1].str());
    _id = id;
}

sstring db::blob_descriptor::filename() const {
    return FILENAME_PREFIX
           + std::to_string(_id)
           + SUFFIX;
}

db::blobstore_config db::blobstore_config::from_db_config(const db::config& cfg){
    db::blobstore_config c;
    c.blobstore_dir_location = cfg.blobstore_directory();
    c.blob_size_in_mb = cfg.blob_size_mb();
    return c;
}

db::blobstore::blobstore(blobstore_config &&cfg):
    _cfg(std::move(cfg))
    ,_blob_create_semaphore(1)
{

}

future<> db::blobstore::init(){
    auto descs = co_await list_blobs(_cfg.blobstore_dir_location);
    blob_id_type base_id = 0;
    for(auto& desc : descs) {
        // only open blobs belong to this shard
        // prerequisite: keep the shard number unchanged when scylla restart !, todo : blob data resharding when cpu count change ?
        if( (desc.id() >> max_base_bits) == this_shard_id() ) {
            auto f = co_await open_file_dma(_cfg.blobstore_dir_location + "/" + desc.filename(), open_flags::rw | open_flags::dsync);
            auto stat = co_await file_stat(_cfg.blobstore_dir_location + "/" + desc.filename(), follow_symlink::no);
            auto align = f.disk_overwrite_dma_alignment();
            blob_ptr blob_p= make_shared<blob>(this, std::move(f), desc, align, stat.size, stat.size);// old blob treated as sealed temporarily
            _blobs.emplace_back(blob_p);
            bslogger.info("init blob: {}, size {}, allocated_size {}, alignment {}", desc.filename(), stat.size, stat.allocated_size, align);

            base_id = std::max(base_id, desc.id() & base_mask);
        }
    }

    std::sort(_blobs.begin(), _blobs.end(), [](const blob_ptr& b1, const blob_ptr& b2) {
        return b1->blob_id() < b2->blob_id();
    });

    // only the latest blob can write
    if(_blobs.size() > 1){
        for(size_t i = 0; i < _blobs.size() - 1; i++){
            _blobs[i]->set_sealed();
        }
    }

    // base blob_id : [shard_id | ts_base_id]
    blob_id = ( blob_id_type(this_shard_id()) << max_base_bits ) | base_id;
    bslogger.info("blobstore init finish.  blob_id {}, ts_base_id {}", blob_id, base_id);
}


future<db::payload_pos>
db::blobstore::add_payload(bytes&& payload, size_t size){
    auto b = co_await writing_blob(size);
    auto pos = co_await b->append(std::move(payload), size);

    co_return pos;
}

future<db::blobstore::blob_ptr> db::blobstore::writing_blob(size_t size) {
    if (_blobs.empty() || !_blobs.back()->is_still_writing(size)) {
        return _blob_create_semaphore.wait().then([this, size]() {
            if (_blobs.empty() || !_blobs.back()->is_still_writing(size)) {
                return new_blob();
            }else{
               return make_ready_future<blob_ptr>(_blobs.back());
            }
        }).finally([this]() {
            _blob_create_semaphore.signal();
        });
    }
    return make_ready_future<blob_ptr>(_blobs.back());
}

future<db::blobstore::blob_ptr> db::blobstore::new_blob(){
    blob_descriptor d(next_blob_id());

    size_t max_size = _cfg.blob_size_in_mb << 20;
    //open without do_dsync for pre-allocation
    auto f = co_await open_file_dma(_cfg.blobstore_dir_location + "/" + d.filename(), open_flags::rw | open_flags::create);
    co_await f.allocate(0, max_size);
    size_t align = f.disk_write_dma_alignment();
    size_t buf_size = align_up<size_t>(128 * 1024, size_t(align));
    size_t zerofill_size = max_size;
    auto rem = zerofill_size;

    auto buf = temporary_buffer<char>::aligned(align, buf_size);
    std::fill(buf.get_write(), buf.get_write() + buf.size(), 0);
    while (rem != 0) {
        static constexpr size_t max_write = 4 * 1024 * 1024;
        auto n = std::min(max_write / buf_size, 1 + rem / buf_size);

        std::vector<iovec> v;
        v.reserve(n);
        size_t m = 0;
        while (m < rem && m < max_write) {
            auto s = std::min(rem - m, buf_size);
            v.emplace_back(iovec{ buf.get_write(), s});
            m += s;
        }
        auto s = co_await f.dma_write(max_size - rem, std::move(v));
        if (!s) [[unlikely]] {
            on_internal_error(bslogger, format("dma_write returned 0: max_size={} rem={} iovec.n={}", max_size, rem, n));
        }
        rem -= s;
    }

    // sync metadata (size/written)
    co_await f.flush();
    co_await f.close();
    // reopen with o_dsync
    f = co_await open_file_dma(_cfg.blobstore_dir_location + "/" + d.filename(), open_flags::rw | open_flags::dsync);
    auto size =co_await f.size();
    align = f.disk_overwrite_dma_alignment();

    blob_ptr blob_p= make_shared<blob>(this, std::move(f), d, align, size);
    _blobs.emplace_back(blob_p);
    bslogger.info("blob file {} created. alignment {}, size {}", d.filename(), align, size);
    co_return blob_p;
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
            result.emplace_back(de.name);
        }
    });
    co_await h.done(); // wait for list finish
    co_return result;
}

db::blobstore::buffer_type db::blobstore::acquire_buffer(size_t s) {
    auto i = buffers.begin();
    auto e = buffers.end();

    while (i != e) {
        if (i->size() >= s) {
            auto r = std::move(*i);
            buffers.erase(i);
            return r;
        }
        ++i;
    }
    auto a = ::memalign(blob::alignment, s);
    if (a == nullptr) {
        throw std::bad_alloc();
    }
    bslogger.debug("Allocated {} k buffer", s / 1024);
    return buffer_type(reinterpret_cast<char *>(a), s, make_free_deleter(a));
}

void db::blobstore::release_buffer(buffer_type&& b) {
    buffers.emplace_back(std::move(b));
    std::sort(buffers.begin(), buffers.end(), [](const buffer_type& b1, const buffer_type& b2) {
        return b1.size() < b2.size();
    });

    constexpr const size_t max_buffers = 8;

    if (buffers.size() > max_buffers) {
        bslogger.debug("Deleting {} buffers", buffers.size() - max_buffers);
        buffers.erase(buffers.begin() + max_buffers, buffers.end());
    }
}