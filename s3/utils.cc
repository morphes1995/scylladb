#include "utils.hh"

namespace s3{

http_op op_from_method(const char *method)
{
  if (!method)
    return OP_UNKNOWN;
  if (strcmp(method, "GET") == 0)
    return OP_GET;
  if (strcmp(method, "PUT") == 0)
    return OP_PUT;
  if (strcmp(method, "DELETE") == 0)
    return OP_DELETE;
  if (strcmp(method, "HEAD") == 0)
    return OP_HEAD;
  if (strcmp(method, "POST") == 0)
    return OP_POST;
  if (strcmp(method, "COPY") == 0)
    return OP_COPY;
  if (strcmp(method, "OPTIONS") == 0)
    return OP_OPTIONS;

  return OP_UNKNOWN;
}


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

