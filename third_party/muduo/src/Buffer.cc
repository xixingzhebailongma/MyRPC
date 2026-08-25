#include "../include/Buffer.h"
Buffer::Buffer() = default;

Buffer::~Buffer() = default;

void Buffer::append(const char *data, size_t size) {
  buf_.append(data,size);
}

void Buffer::erase(size_t pos, size_t nn) { buf_.erase(pos, nn); }

size_t Buffer::size() { return buf_.size(); }

const char *Buffer::data() { return buf_.data(); }

void Buffer::clear() { buf_.clear(); }

bool Buffer::pickmessage(std::string &ss) {
  if (buf_.size() == 0)
    return false;
  size_t len;
  memcpy(&len, buf_.data(), 4);
  if (buf_.size() < len + 4)
    return false; // 如果buf_中的数据量小于报文头部，说明buf_中的报文内容不完整。

  ss = buf_.substr(4, len); // 从buf_中获取一个报文。
  buf_.erase(0, len + 4);
  return true;
}