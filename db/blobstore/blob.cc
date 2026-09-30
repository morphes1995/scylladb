#include "blobstore.hh"

static logging::logger blogger("blob");

bool db::blob::is_still_writing(size_t size){
    if (_closed){
        return false;
    }

    // todo payload entry overhead ?
    if(_file_pos + size > _size){
        set_sealed();
        return false;
    }

    return true;
}

future<db::payload_pos>
db::blob::append(bytes&& payload, size_t size){
    payload_pos pos(_desc.id(), _file_pos);
    blogger.debug("writing to {}:{}", _desc.filename(), _file_pos);

    auto size_aligned = align_up(size, _alignment);
    auto padding = size_aligned - size;
    blobstore::buffer_type buf = _bs->acquire_buffer(size_aligned);
    auto out = simple_memory_output_stream(buf.get_write(), buf.size());
    // todo : add crc and payload object meta info
    out.write(reinterpret_cast<const char*>(payload.data()), payload.size());
    out.fill(0, padding);
    SCYLLA_ASSERT(out.size() == buf.size() - size_aligned);

    // acquire read lock, then write buffered data to file
    co_await _lock.read_lock();

    auto off = _file_pos;
    _file_pos += size_aligned;
    auto p = buf.begin();
    auto written = make_lw_shared<size_t>(0);
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

    SCYLLA_ASSERT( _file_pos <= _size );
    if(_file_pos == _size ){
        blogger.info("blob {} sealed, size {}mb", _desc.filename(), _file_pos >> 20);
        set_sealed();
    }
    _bs->release_buffer(std::move(buf));
    _lock.read_unlock();

    auto flush_pos_tmp = _file_pos;
    if (flush_pos_tmp <= _flush_pos) {
        blogger.debug("blob {} already flushed! ({} <= {})", _desc.filename(), flush_pos_tmp, _flush_pos);
        co_return pos;
    }

    // acquire write lock(make sure all file writing was done), then flush
    co_await _lock.write_lock();
    _lock.write_unlock(); // release immediately, so we can allow flushing and writing concurrent
    flush_pos_tmp = std::max(flush_pos_tmp, _file_pos);
    if (flush_pos_tmp <= _flush_pos) {
        blogger.debug("blob {} already flushed! ({} <= {})", _desc.filename(), flush_pos_tmp, _flush_pos);
        co_return pos;
    }

    try {
        co_await _f.flush();
        _flush_pos = std::max(flush_pos_tmp, _flush_pos);
        blogger.debug("blob {} flushed to {}", _desc.filename(), _flush_pos);
    } catch (...) {
        blogger.error("Failed to flush blob {} to disk: {}", _desc.filename(), std::current_exception());
        throw;
    }

    blogger.debug("appended payload of size {}, disk occupation {}", size, size_aligned);
    co_return pos;
}