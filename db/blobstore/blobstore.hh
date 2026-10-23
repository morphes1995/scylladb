#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/stream.hh>
#include <seastar/core/rwlock.hh>
#include "db/config.hh"
#include "utils/flush_queue.hh"

using namespace seastar;

namespace db{

using blob_id_type = uint64_t;
using blob_offset_type = uint32_t;

static const size_t max_cpu_bits = 10; // 1024 cpus. should be enough for anyone
static const size_t max_base_bits = 8 * sizeof(blob_id_type) - max_cpu_bits;
static const blob_id_type base_mask = (blob_id_type(1) << max_base_bits) - 1;
static const blob_id_type cpu_mask = ~base_mask;

struct blobstore_config{
    sstring blobstore_dir_location;
    uint64_t blob_size_in_mb;

    static blobstore_config from_db_config(const db::config& cfg);
};

struct blob_descriptor{
    private:
        blob_id_type _id;
    public:
        static const std::string SEPARATOR;
        static const std::string FILENAME_PREFIX;
        static const std::string SUFFIX;

        blob_descriptor(const blob_id_type  id): _id(id) {};
        blob_descriptor(const std::string& filename);
        sstring filename() const;
        blob_id_type id(){ return _id; };
};


struct payload_pos{
    blob_id_type blob_id;
    blob_offset_type offset;
};

class blob;
class blobstore{
public:
    using blob_ptr = ::shared_ptr<blob>;
    using buffer_type = fragmented_temporary_buffer;
    static constexpr size_t default_max_fragment_size = 128 * 1024;

    blobstore_config _cfg;
    blobstore(blobstore_config &&cfg);
    future<> init();

    future<payload_pos> add_payload(managed_bytes_view payload_view, size_t size);
    buffer_type acquire_buffer(size_t s, size_t alignment);

private:

    blob_id_type blob_id =0;
    std::vector<blob_ptr> _blobs;
    semaphore _blob_create_semaphore;

    std::vector<buffer_type> buffers;


    [[maybe_unused]]bool _shutdown = false;

    blob_id_type next_blob_id(){
        return ++ blob_id;
    }
    future<blob_ptr> new_blob();
    future<blob_ptr> writing_blob(size_t size);
    future<std::vector<blob_descriptor>> list_blobs(sstring &dirname) const;
};



/**
 * a blob corresponding to a append only file
 */
class blob : public enable_shared_from_this<blob>{
public:
    blob(blobstore *bs, file&& f, const blob_descriptor& d, size_t align, size_t size, size_t file_pos = 0):
         _bs(bs)
         , _f(f)
         , _desc(d)
         , _alignment(align)
         , _size(size)
         , _file_pos(file_pos)
         , _pending_ops(true)
    {
        if (_file_pos >= _size){
            _closed = true;
        }
    }

    bool is_still_writing(size_t size);
    blob_id_type blob_id(){
        return _desc.id();
    }
    void set_sealed() {
        _closed = true;
    };
    future<payload_pos> append(managed_bytes_view payload_view, size_t size);


private:
    blobstore *_bs;
    file _f;
    blob_descriptor _desc;
    size_t _alignment;
    [[maybe_unused]] size_t _size;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    utils::flush_queue<uint64_t, std::less<uint64_t>, lowres_clock> _pending_ops;

    rwlock _lock; // used as a barrier between write & flush
    bool _closed = false;

    future<> do_flush(uint64_t pos);
};

}


