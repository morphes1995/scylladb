#pragma once

#include "stdint.h"
#include "seastar/core/sstring.hh"


namespace s3{


// Used for generating object version nano timestamps.
using timestamp_type = int64_t;
class object_version_clock final {
    using base = std::chrono::system_clock;
public:
    using rep = timestamp_type;
    using duration = std::chrono::nanoseconds;
    using period = typename duration::period;
    using time_point = std::chrono::time_point<object_version_clock, duration>;
    static timestamp_type now() {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch())).time_since_epoch().count();
    }
};

seastar::sstring format_time_gmt(int64_t nanoseconds);


int32_t bucket_shard(const seastar::sstring& key, int max_shards);

}
