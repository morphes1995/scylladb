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