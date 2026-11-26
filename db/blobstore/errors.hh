#pragma once
#include <bits/exception.h>

class blob_error : public std::exception {};

class invalid_blob_format : public blob_error {
    static constexpr const char* _msg = "Not a blob file";
public:
    const char* what() const noexcept override {
        return _msg;
    }
};

class Unsupported_change_max_reserve_blobs : public blob_error {
    static constexpr const char* _msg = "change max_reserve_blobs option to a small number is not supported now";
public:
    const char* what() const noexcept override {
        return _msg;
    }
};