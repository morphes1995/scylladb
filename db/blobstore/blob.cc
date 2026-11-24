#include "blobstore.hh"
#include "utils/flush_queue.hh"
#include "seastar/core/fstream.hh"
#include "errors.hh"

static logging::logger blogger("blob");

bool db::blob::is_still_writing(size_t size){
    if (_sealed){
        return false;
    }

    if(_file_pos + size + payload_overhead_size == _max_size){
        set_sealed();
        return true;
    }
    if(_file_pos + size + payload_overhead_size > _max_size){
        set_sealed();
        return false;
    }

    return true;
}

template<typename T, typename Input>
std::enable_if_t<std::is_fundamental<T>::value, T> read(Input& in) {
    return in.template read<T>();
}

future<uint64_t>
db::blob::probe_actual_file_position(){
    fragmented_temporary_buffer::reader frag_reader;
    input_stream<char> fin = make_file_input_stream(_f, 0);
    fragmented_temporary_buffer buf = co_await frag_reader.read_exactly(fin, blob_header_size);
    auto in = buf.get_istream();
    auto magic = read<uint32_t>(in);

    if(magic == 0){
        blogger.info("{} is a clean blob, set file pos to 0", _desc.filename());
        co_return 0;
    }
    if (magic != blob_header_magic) {
        blogger.info("{} is not a  blob file, {} != {}", _desc.filename(), magic, blob_header_magic);
        throw invalid_blob_format();
    }

    uint64_t pos = blob_header_size;
    while(pos + payload_overhead_size <= _max_size){
        buf = co_await frag_reader.read_exactly(fin, payload_overhead_size);
        in = buf.get_istream();
        auto payload_size = read<uint32_t>(in);
        if(payload_size == 0){
            blogger.info("blob {} is half-full, file pos probed at {}", _desc.filename(), pos);
            co_return pos;
        }
        pos += payload_overhead_size;

        auto aligned_payload_size = align_up(size_t(payload_overhead_size + payload_size), _alignment);
        co_await fin.skip(aligned_payload_size - payload_overhead_size);// jump to next payload
        pos += (aligned_payload_size - payload_overhead_size);
    }

    blogger.info("blob {} is full, set file pos {}", _desc.filename(), _max_size);
    co_return _max_size;
}

future<db::payload_pos>
db::blob::append(managed_bytes_view payload_view, size_t size, bool need_flush){
    SCYLLA_ASSERT(need_flush == _desc.sync_write());
    uint32_t payload_size = size;
    size_t overhead_size = payload_overhead_size;
    if(_file_pos == 0 ){
        overhead_size += blob_header_size;
    }
    size += overhead_size;

    // pos return the payload data offset
    payload_pos pos(_desc.id(), _file_pos + overhead_size);
    blogger.debug("writing to {}:{}", _desc.filename(), _file_pos);

    auto size_aligned = align_up(size, _alignment);
    auto padding = size_aligned - size;
    blobstore::buffer_type buffers = _bs->acquire_buffer(size_aligned, _alignment);
    auto out = buffers.get_ostream();

    if(_file_pos == 0){
        // blob file header info
        out.write(reinterpret_cast<const char*>(&blob_header_magic), sizeof(blob_header_magic));
        out.fill(0, blob_header_size - sizeof(blob_header_magic)); // reserved space
    }
    out.write(reinterpret_cast<char*>(&payload_size), sizeof(payload_size));
    // todo: add crc and payload object meta info
    for (bytes_view fragment : fragment_range(payload_view)) {
        out.write(reinterpret_cast<const char*>(fragment.data()), fragment.size());
    }
    out.fill(0, padding);
    SCYLLA_ASSERT(out.size() == buffers.size_bytes() - size_aligned);

    std::vector<iovec> iovecs;
    for(auto &buf: buffers){
        iovecs.emplace_back(iovec{ const_cast<char *>(buf.get()), buf.size() });
    }

    auto off = _file_pos;
    _file_pos += size_aligned;
    if(!need_flush &&  _file_pos >= _max_size ){
        // force flush when blob file is full
        need_flush = true;
    }
    auto flush_pos_tmp = _file_pos;
    // The write will be allowed to start now, but flush (below) must wait for not only this,
    // but all previous write/flush pairs.
    co_await _pending_ops.run_with_ordered_post_op(off, [&, frag_max_size = blobstore::default_max_fragment_size]() -> future<> {
            uint64_t written = 0;
             for(;;){
                try {
                    auto bytes = co_await _f.dma_write(off + written, iovecs);
                    if (written + bytes == size_aligned) {
                        blogger.debug("payload fully written to {}", _desc.filename());
                        break;
                    }

                    SCYLLA_ASSERT(bytes % _alignment == 0);
                    // handle partially write
                    auto c = bytes / frag_max_size;
                    auto r = bytes % frag_max_size;
                    // remove written iovec
                    iovecs.erase(iovecs.begin(), iovecs.begin() + c);
                    // update partially written iovec
                    char* iov_base = reinterpret_cast<char*>(iovecs.front().iov_base);
                    iovecs.front().iov_base = iov_base + r;
                    iovecs.front().iov_len -= r;

                    written += bytes;
                    blogger.warn("payload partial write of {} to {} at {}, rest {}/{} bytes ", bytes, _desc.filename(),  off + written, size_aligned - written, size_aligned);
                    continue;
                } catch (...) {
                    blogger.error("Failed to persist payload to blob for {}: {}", _desc.filename(), std::current_exception());
                    throw;
                }
            }
        }, [&]() -> future<> {
            if(need_flush){
                co_await do_flush(flush_pos_tmp);
            }
        });

    co_return pos;
}

future<> db::blob::do_flush(uint64_t pos, bool force) {
    if (!force && pos <= _flush_pos) {
        blogger.debug("blob {} already flushed! ({} <= {})", _desc.filename(), pos, _flush_pos);
        co_return;
    }
    blogger.debug("try flushing payload at {}", pos);
    try {
        co_await _f.flush();
        _flush_pos = std::max(pos, _flush_pos);
        blogger.debug("blob {} flushed to {}", _desc.filename(), _flush_pos);
    } catch (...) {
        blogger.error("Failed to flush blob {} to disk: {}", _desc.filename(), std::current_exception());
        throw;
    }
}

future<> db::blob::flush() {
    return do_flush(_file_pos, true);
}

future<fragmented_temporary_buffer>
db::blob::read(blob_offset_type offset, size_t size){
    auto aligned_down_offset = align_down(size_t(offset), _alignment);
    auto size_aligned = align_up(size + size_t(offset - aligned_down_offset), _alignment);
    auto suffix_to_discard = size_aligned - size - (offset - aligned_down_offset);
    blogger.debug("read payload, id {}, offset {}, aligned_down_offset{}, size {}, size_aligned {}, suffix_to_discard {}",
                 blob_id(), offset, aligned_down_offset, size, size_aligned, suffix_to_discard);

    blobstore::buffer_type buffers = _bs->acquire_buffer(size_aligned, _alignment);

    std::vector<iovec> iovecs;
    for(auto &buf: buffers){
        iovecs.emplace_back(iovec{ const_cast<char *>(buf.get()), buf.size() });
    }

    uint64_t total_read_bytes = 0;
    for(;;){
        try {
            auto read_bytes = co_await _f.dma_read(aligned_down_offset + total_read_bytes, iovecs);
            if (total_read_bytes + read_bytes == size_aligned) {
                blogger.debug("payload fully read from {}, size {}", _desc.filename(), size_aligned);
                break;
            }

            SCYLLA_ASSERT(read_bytes % _alignment == 0);
            // handle partially write
            iovecs.erase(iovecs.begin(), iovecs.begin() + read_bytes / blobstore::default_max_fragment_size);
            total_read_bytes += read_bytes;

            continue;
        } catch (...) {
            blogger.error("Failed to read payload to blob for {}: {}", _desc.filename(), std::current_exception());
            throw;
        }
    }

    // remove unnecessary data in the front and end, only keep actual payload data
    buffers.remove_prefix(offset - aligned_down_offset);
    buffers.remove_suffix(suffix_to_discard);
    co_return buffers;
}