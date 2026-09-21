add_library(s3 STATIC)
target_sources(s3
  PRIVATE
    controller.cc
    server.cc
    utils.cc
    ${cql_grammar_srcs})
target_include_directories(s3
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
  PRIVATE
    ${RAPIDJSON_INCLUDE_DIRS})
target_link_libraries(s3
  cql3
  idl
  Seastar::seastar
  xxHash::xxhash
  absl::headers)

check_headers(check-headers s3
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
