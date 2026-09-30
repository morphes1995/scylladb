#pragma once

#include <seastar/core/future.hh>

#include "db/config.hh"

using namespace seastar;

using blob_id_type = uint64_t;

namespace db{

struct blobstore_config{
    sstring blobstore_dir_location;
    uint64_t blob_size_in_mb;

    static blobstore_config from_db_config(const db::config& cfg);
};

struct blob_descriptor{
    private:
        sstring _filename;
    public:
        static const std::string SEPARATOR;
        static const std::string FILENAME_PREFIX;
        static const std::string SUFFIX;

//        const blob_id_type id;

        blob_descriptor(const std::string& filename);
        sstring filename() const;
};

class blobstore{
public:
    blobstore_config _cfg;
   static future<blobstore> create_blobstore(blobstore_config &cfg);
   blobstore(blobstore_config &&cfg);

private:
    future<> init();
    future<std::vector<blob_descriptor>> list_blobs(sstring &dirname) const;
};

}


