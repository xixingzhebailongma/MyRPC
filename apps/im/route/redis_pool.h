#pragma once
#include <condition_variable>
#include <cppconn/connection.h>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// 前向声明 hiredis C 类型，避免在头文件中暴露 hiredis.h
struct redisContext;

class RedisPool {
public:
  struct ContextDeleter {
    void operator()(redisContext *ctx) const; // redisFree
  };
  using ConnPtr = std::unique_ptr<redisContext, ContextDeleter>;

  RedisPool();
  ~RedisPool(); // 析构释放所有 idle 连接

  RedisPool(const RedisPool &) = delete;
  RedisPool &operator=(const RedisPool &) = delete;

  bool init(const std::string &ip, int port, size_t pool_size);
  size_t size() const;
  class Guard { //独占持有的checkout句柄
  public:
    Guard(RedisPool &pool, ConnPtr conn);
    ~Guard(); // 析构时归还/丢弃连接
    Guard(const Guard &) = delete;
    Guard &operator=(const Guard &) = delete;
    Guard(Guard &&o) noexcept; // 转移 conn_/broken_，源 disarm

    redisContext *get() const noexcept { return conn_.get(); }
    explicit operator bool() const noexcept { return static_cast<bool>(conn_); }
    void markBroken() noexcept { broken_ = true; }

  private:
    RedisPool &pool_;
    ConnPtr conn_;
    bool broken_ = false;
    bool armed_ = true;
  };
  Guard acquire();

  private:
  friend class Guard;
  void release(ConnPtr conn,bool broken);
  ConnPtr dial(); // redisConnect，失败返回 nullptr
  static bool ping(redisContext* ctx);  //借出前探活：PING->PONG
  std::string ip_;
  int port_ = 0;
  size_t pool_size_ = 4;
  std::vector<ConnPtr> idle_; //空闲连接
  size_t active_ = 0;       // 当前已 checkout（含拨号中）
  bool initialized_ = false;
  std::mutex mutex_;
  std::condition_variable cv_;
};