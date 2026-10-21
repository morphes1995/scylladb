#include "blobstore.hh"
#include "utils/flush_queue.hh"

static logging::logger blogger("blob");

bool db::blob::is_still_writing(size_t size){
    if (_closed){
        return false;
    }

    // todo payload entry overhead ?

    if(_file_pos + size == _size){
        set_sealed();
        return true;
    }

    if(_file_pos + size > _size){
        set_sealed();
        return false;
    }

    return true;
}

future<db::payload_pos>
db::blob::append(managed_bytes_view payload_view, size_t size){
    payload_pos pos(_desc.id(), _file_pos);
    blogger.debug("writing to {}:{}", _desc.filename(), _file_pos);

    auto size_aligned = align_up(size, _alignment);
    auto padding = size_aligned - size;
    blobstore::buffer_type buf = _bs->acquire_buffer(size_aligned, _alignment);
    auto out = simple_memory_output_stream(buf.get_write(), buf.size());

    // todo : add crc and payload object meta info
    for (bytes_view fragment : fragment_range(payload_view)) {
        out.write(reinterpret_cast<const char*>(fragment.data()), fragment.size());
    }
    out.fill(0, padding);
    SCYLLA_ASSERT(out.size() == buf.size() - size_aligned);

    auto off = _file_pos;
    _file_pos += size_aligned;
    auto flush_pos_tmp = _file_pos;
    auto p = buf.begin();
    auto written = make_lw_shared<size_t>(0);

    // The write will be allowed to start now, but flush (below) must wait for not only this,
    // but all previous write/flush pairs.
    co_await _pending_ops.run_with_ordered_post_op(off, [&]() -> future<> {
             for(;;){
                try {
                    auto bytes = co_await _f.dma_write(off + *written, p + *written, size_aligned - *written);
                    *written += bytes;
                    if (*written == size_aligned) {
                        blogger.debug("payload fully written to {}", _desc.filename());
                        break;
                    }
                    bytes = align_down(bytes, _alignment);
                    off += bytes;
                    blogger.debug("payload partial write of {} to {}: {}/{} bytes at at {}", bytes, _desc.filename(), size_aligned - *written, size_aligned, off - bytes);
                    continue;
                } catch (...) {
                    _lock.read_unlock();
                    blogger.error("Failed to persist payload to blob for {}: {}", _desc.filename(), std::current_exception());
                    throw;
                }
            }
            _bs->release_buffer(std::move(buf));
        }, [&]() -> future<> {
            co_await do_flush(flush_pos_tmp);
        });

    co_return pos;
}

future<> db::blob::do_flush(uint64_t pos) {
    if (pos <= _flush_pos) {
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