#include "blobstore.hh"
#include "utils/flush_queue.hh"

static logging::logger blogger("blob");

bool db::blob::is_still_writing(size_t size){
    if (_closed){
        return false;
    }

    // todo payload entry overhead ?

    if(_file_pos + size == _max_size){
        set_sealed();
        return true;
    }

    if(_file_pos + size > _max_size){
        set_sealed();
        return false;
    }

    return true;
}

future<db::payload_pos>
db::blob::append(managed_bytes_view payload_view, size_t size, bool need_flush){

    SCYLLA_ASSERT(need_flush == _desc.sync_write());

    payload_pos pos(_desc.id(), _file_pos);
    blogger.debug("writing to {}:{}", _desc.filename(), _file_pos);

    auto size_aligned = align_up(size, _alignment);
    auto padding = size_aligned - size;
    blobstore::buffer_type buffers = _bs->acquire_buffer(size_aligned, _alignment);
    auto out = buffers.get_ostream();

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
                    _lock.read_unlock();
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