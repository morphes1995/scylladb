#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/stream.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/queue.hh>
#include "db/config.hh"
#include "utils/flush_queue.hh"
#include "mutation/mutation_fragment.hh"

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
    // Max number of blob file to keep in pre-created reserve map.
    uint16_t max_reserve_blobs;

    static blobstore_config from_db_config(const db::config& cfg);
};

struct blob_descriptor{
    private:
        blob_id_type _id;
        // use O_DSYNC or not
        // extract payload when flush, we add the payload to blob file opening without O_DSYNC
        // extract payload before write CL, we add the payload to blob file opening with O_DSYNC
        bool _sync_write;
    public:
        static const std::string SEPARATOR;
        static const std::string FILENAME_PREFIX;
        static const std::string SUFFIX;
        static const std::string SYNC_SUFFIX;

        blob_descriptor(const blob_id_type  id, bool sync_write): _id(id), _sync_write(sync_write){};
        blob_descriptor(const std::string& filename);
        sstring filename() const;
        blob_id_type id(){ return _id; };
        bool sync_write(){ return _sync_write; };
};


struct payload_pos{
    blob_id_type blob_id;
    blob_offset_type offset;
};

struct object_meta_columns {
    const column_definition* inlined;
    const column_definition* payload;
    const column_definition* blob_id;
    const column_definition* offset;
    const column_definition* size;

    explicit object_meta_columns(const schema& s)
        : inlined(s.get_column_definition(bytes("inlined")))
        , payload(s.get_column_definition(bytes("payload")))
        , blob_id(s.get_column_definition(bytes("blob_id")))
        , offset(s.get_column_definition(bytes("offset")))
        , size(s.get_column_definition(bytes("size"))) {}
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

    buffer_type acquire_buffer(size_t s, size_t alignment);
    // return whether payload extract to blobstore or not
    future<std::optional<bool>> add_payload(const schema& schema, row& row, size_t object_payload_inline_threshold, bool sync);
    future<> read_payload(const schema& schema, row& row);
    future<> flush();
private:
    blob_id_type blob_id =0;

    semaphore _blob_create_semaphore;  // protect blob file creation without o_dsync
    std::map<blob_id_type, blob_ptr> _blobs; // blob file without o_dsync

    std::map<blob_id_type, blob_ptr> _sync_write_blobs; // blob file with o_dsync
    queue<blob_ptr> _reserved_sync_write_blobs; //  pre-created blob file with o_dsync
    // don't issue a new call of sync_write_blob creation until the old one has terminated
    std::optional<shared_future<with_clock<db::timeout_clock>>> _sync_write_blob_allocating;

    std::vector<buffer_type> buffers;


    bool _shutdown = false;

    blob_id_type next_blob_id(){
        return ++ blob_id;
    }

    future<blob_ptr> writing_blob(size_t size);
    future<blob_ptr> new_blob(bool sync_write);

    future<blob_ptr> writing_sync_write_blob(size_t size);
    future<> _sync_write_blobs_reserver;
    future<> reserve_sync_write_blobs();
    future<db::blobstore::blob_ptr> new_sync_write_blob();

    future<std::vector<blob_descriptor>> list_blobs(sstring &dirname) const;
    future<> init_blobs_status(std::map<blob_id_type, blob_ptr>& blobs, bool synced);
    future<blob_id_type> find_may_half_full_blob(const std::map<blob_id_type, blob_ptr>& blobs);

    // append payload to blob persistently
    future<payload_pos> append_payload(managed_bytes_view payload_view, size_t size, bool sync);
    future<fragmented_temporary_buffer> read_payload(blob_id_type blob_id, blob_offset_type offset, int64_t size);

    template <typename T>
    T read_cell_value(const row& r, const column_definition* cdef, const abstract_type& type);
    void remove_columns(row& r, std::initializer_list<column_id> ids_to_remove);
    void set_live_cell(row& r, const column_definition* cdef, api::timestamp_type ts, auto&& raw_bytes);
};



/**
 * a blob corresponding to a append only file
 */
class blob : public enable_shared_from_this<blob>{
public:

    static constexpr size_t blob_header_size = 4096;
    static constexpr uint32_t blob_header_magic = 0x827A7A0B;
    // uint32_t : payload size
    static constexpr size_t payload_overhead_size = sizeof(uint32_t);

    blob(blobstore *bs, file&& f, const blob_descriptor& d, size_t align, size_t max_size, size_t file_pos = 0):
         _bs(bs)
         , _f(f)
         , _desc(d)
         , _alignment(align)
         , _max_size(max_size)
         , _file_pos(file_pos)
         , _pending_ops(true)
    {
        if (_file_pos >= max_size){
            _sealed = true;
        }
    }

    bool is_still_writing(size_t size);
    blob_id_type blob_id(){
        return _desc.id();
    }
    sstring name(){
        return _desc.filename();
    }
    void set_sealed() { _sealed = true; };
    void set_file_pos(uint64_t file_pos) { _file_pos = file_pos; };
    size_t get_max_size(){ return _max_size; };
    uint64_t get_file_pos(){ return _file_pos; };
    bool sealed(){ return _sealed; };
    future<uint64_t> probe_actual_file_position();
    future<bool> has_valid_header();

    bool sync_write() { return _desc.sync_write(); };
    future<payload_pos> append(managed_bytes_view payload_view, size_t size, bool need_flush);
    future<fragmented_temporary_buffer> read(blob_offset_type offset, size_t size);
    future<> flush();

private:
    blobstore *_bs;
    file _f;
    blob_descriptor _desc;
    size_t _alignment;
    size_t _max_size;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    utils::flush_queue<uint64_t, std::less<uint64_t>, lowres_clock> _pending_ops;

    rwlock _lock; // used as a barrier between write & flush
    bool _sealed = false;
    future<> do_flush(uint64_t pos, bool force = false);
};

}


