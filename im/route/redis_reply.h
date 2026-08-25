#pragma once
// 前向声明 hiredis C 类型，避免在头文件中暴露 hiredis.h
struct redisReply;

// RedisReply RAII 守卫：析构时自动 freeReplyObject，杜绝泄漏。
class RedisReply {
public:
  explicit RedisReply(redisReply *reply = nullptr);
  ~RedisReply();

  RedisReply(const RedisReply &) = delete;
  RedisReply &operator=(const RedisReply &) = delete;

  RedisReply(RedisReply &&o) noexcept; //窃取指针，源置空
  RedisReply &operator=(RedisReply &&o) noexcept;

  redisReply *get() const noexcept { return reply_; }
  explicit operator bool() const noexcept { return reply_ != nullptr; }
  redisReply *operator->() const noexcept { return reply_; }

private:
  redisReply *reply_ = nullptr;
};