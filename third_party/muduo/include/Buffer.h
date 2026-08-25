#pragma once
#include <string>
#include <cstring>

class Buffer{
    private:std::string buf_;
    public:
    Buffer();
    ~Buffer();

    void append(const char* data,size_t size);
    void erase(size_t pot,size_t nn);
    size_t size();
    const char* data();
    void clear();
    bool pickmessage(std::string &ss);
};