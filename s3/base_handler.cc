#include "base_handler.hh"
#include "utils.hh"

namespace s3{

static logging::logger base_logger("s3_base_handler");

future<s3_op_handle_result>
base_handler::query_object(
    const sstring& bucket,
    const sstring& object,
    const std::vector<sstring>& column_names,
    row_result_handler handle_row) {

    s3_op_handle_result res;
    schema_ptr schema;
    try {
        schema = get_object_metadata_schema();
    } catch (const data_dictionary::no_such_column_family&) {
        res.status = reply::status_type::internal_server_error;
        co_return res;
    }

    // 1. Assemble partition and clustering keys
    partition_key pk = make_object_pk(bucket, object);
    clustering_key ck = make_object_ck(object, std::nullopt);

    dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*schema, pk))};
    std::vector<query::clustering_range> ck_bounds{
        query::clustering_range::make_starting_with(query::clustering_range::bound(ck))
    };

    // 2. Build column selection and partition slice
    shared_ptr<cql3::selection::selection> selection;
    query::column_id_vector column_ids;

    if (column_names.empty()) {
        column_ids = boost::copy_range<query::column_id_vector>(
            schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
        selection = cql3::selection::selection::wildcard(schema);
    } else {
        std::vector<const column_definition*> col_defs;
        col_defs.reserve(column_names.size());
        column_ids.reserve(column_names.size());
        for (const auto& name : column_names) {
            const column_definition* cdef = schema->get_column_definition(to_bytes(name));
            col_defs.push_back(cdef);
            column_ids.push_back(cdef->id);
        }
        selection = cql3::selection::selection::for_columns(schema, std::move(col_defs));
    }

    auto partition_slice = query::partition_slice(
        std::move(ck_bounds), {}, std::move(column_ids), selection->get_query_options());

    auto command = ::make_lw_shared<query::read_command>(
        schema->id(),
        schema->version(),
        partition_slice,
        _proxy.get_max_result_size(partition_slice),
        query::tombstone_limit(_proxy.get_tombstone_limit()),
        query::row_limit(1));

    // 3. Execute query
    auto qr = co_await _proxy.query(
        schema,
        std::move(command),
        std::move(partition_ranges),
        db::consistency_level::LOCAL_ONE,
        service::storage_proxy::coordinator_query_options(
            db::no_timeout, empty_service_permit(), service::client_state::for_internal_calls()));

    // 4. Build result set and check existence
    cql3::selection::result_set_builder builder(*selection, gc_clock::now());
    query::result_view::consume(
        *qr.query_result,
        partition_slice,
        cql3::selection::result_set_builder::visitor(builder, *schema, *selection));

    auto result_set = builder.build();
    if (result_set->empty()) {
        base_logger.debug("object {}/{} not found", bucket, object);
        res.status = reply::status_type::not_found;
        co_return res;
    }

    SCYLLA_ASSERT(result_set->size() == 1);

    // 5. Invoke caller-provided handler for row parsing
    handle_row(*result_set->rows().begin(), *selection, res);

    res.status = reply::status_type::ok;
    co_return res;
}


future<s3_op_handle_result> base_handler::mutate_object(
    const sstring& bucket,
    const sstring& object,
    int64_t version,
    row_mutation_handler populate_row) {
    s3_op_handle_result res;
    schema_ptr schema;
    try {
        schema = get_object_metadata_schema();
    } catch (const data_dictionary::no_such_column_family&) {
        res.status = reply::status_type::internal_server_error;
        co_return res;
    }

    partition_key pk = make_object_pk(bucket, object);
    clustering_key ck = make_object_ck(object, version);

    mutation m(schema, std::move(pk));
    auto& row = m.partition().clustered_row(*schema, std::move(ck));
    api::timestamp_type now = api::new_timestamp();

    populate_row(*schema, row, now);

    co_await _proxy.mutate(
        std::vector<mutation>{std::move(m)},
        db::consistency_level::ALL,
        db::no_timeout,
        nullptr,
        empty_service_permit(),
        db::allow_per_partition_rate_limit::yes);

    res.status = reply::status_type::ok;
    co_return res;
}

void base_handler::populate_common_headers(
    const sstring& bucket,
    const sstring& object,
    const std::string& column_name,
    bytes_view c,
    s3_op_handle_result& res) {
    if (column_name == "time") {
        int64_t nanoseconds = value_cast<int64_t>(long_type->deserialize(c));
        res.header["Last-Modified"] = format_time_gmt(nanoseconds);
        base_logger.debug("object {} in bucket {}, time: {}", object, bucket, nanoseconds);
    } else if (column_name == "size") {
        int32_t size = value_cast<int32_t>(int32_type->deserialize(c));
        base_logger.debug("object {} in bucket {}, size: {}", object, bucket, size);
        res.header["Content-Length"] = to_sstring(size);
    } else if (column_name == "etag") {
        sstring etag = value_cast<sstring>(utf8_type->deserialize(c));
        base_logger.debug("object {}/{} etag {}", bucket, object, etag);
        res.header["Etag"] = "\"" + etag + "\"";
    }
}

schema_ptr base_handler::get_object_metadata_schema() const {
    return _proxy.data_dictionary().find_schema("s3", "object_metadata");
}

partition_key base_handler::make_object_pk(const sstring& bucket, const sstring& object) const {
    std::vector<bytes> raw_pk;
    raw_pk.push_back(to_bytes(bucket));
    raw_pk.push_back(int32_type->decompose(bucket_shard(object, 1024)));
    return partition_key::from_exploded(raw_pk);
}

clustering_key base_handler::make_object_ck(const sstring& object, std::optional<int64_t> version) const {
    std::vector<bytes> raw_ck;
    raw_ck.push_back(to_bytes(object));
    if (version) {
        raw_ck.push_back(long_type->decompose(*version));
    }
    return clustering_key::from_exploded(raw_ck);
}

} // namespace
