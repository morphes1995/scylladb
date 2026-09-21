#include "utils.hh"

namespace s3{


int32_t bucket_shard(const seastar::sstring& key, int max_shards) {
    uint32_t hash = 0;
    const char *str = key.c_str();
    int length = key.size();
	while (length--) {
		unsigned char c = *str++;
		hash = (hash + (c << 4) + (c >> 4)) * 11;
	}
    hash = hash ^ ((hash & 0xFF) << 24);

    return hash % max_shards;
}

seastar::sstring format_time_gmt(int64_t nanoseconds){
    std::chrono::nanoseconds duration(nanoseconds);
    std::chrono::system_clock::time_point tp(
        std::chrono::duration_cast<std::chrono::system_clock::duration>(duration)
    );

    std::time_t time = std::chrono::system_clock::to_time_t(tp);
    std::tm* gmt_time = std::gmtime(&time);

    char buffer[100];
    std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S GMT", gmt_time);
    return seastar::sstring (buffer);
}


}

