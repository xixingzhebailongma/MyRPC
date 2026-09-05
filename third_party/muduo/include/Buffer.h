#pragma once
#include <cstddef>
#include <string>
#include <sys/types.h>
#include <vector>

// 仿 muduo 的读写双下标缓冲区，线程不安全，只在所属 loop 线程使用。
class Buffer {
public:
  static const size_t kCheapPrepend = 8;   // 预留头部空间
  static const size_t kInitialSize = 1024; // 初始容量

  Buffer();
  ~Buffer() = default;

  size_t readableBytes() const { return writerIndex_ - readerIndex_; }
  size_t writableBytes() const { return buffer_.size() - writerIndex_; }
  size_t prependableBytes() const { return readerIndex_; }

  const char *peek() const { return begin() + readerIndex_; }

  void retrieve(size_t len);
  void retrieveAll();
  std::string retrieveAsString(size_t len);

  void append(const char *data, size_t len);
  void ensureWritableBytes(size_t len);

  // 直接从 fd 读入可写区，返回读取字节数；<0 时 *savedErrno 置为 errno。
  ssize_t readFd(int fd, int *savedErrno);

private:
  char *begin() { return buffer_.data(); }
  const char *begin() const { return buffer_.data(); }
  char *beginWrite() { return begin() + writerIndex_; }
  const char *beginWrite() const { return begin() + writerIndex_; }
  void hasWritten(size_t len) { writerIndex_ += len; }
  void makeSpace(size_t len);

  std::vector<char> buffer_;
  size_t readerIndex_;
  size_t writerIndex_;
};