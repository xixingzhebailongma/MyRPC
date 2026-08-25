#pragma once
#include "redis_pool.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
// 前向声明 hiredis C 类型，避免在头文件中暴露 hiredis.h
struct redisContext;

class RedisClient {
public:
  RedisClient();
  ~RedisClient();

  RedisClient(const RedisClient &) = delete;
  RedisClient &operator=(const RedisClient &) = delete;

  bool connect(const std::string &ip, int port);

  // String操作
  bool set(const std::string &key, const std::string &value);
  std::string get(const std::string &key);
  bool del(const std::string &key);
  bool exists(const std::string &key);

  // Hash操作
  bool hset(const std::string &key, const std::string &field,
            const std::string &value);
  std::string hget(const std::string &key, const std::string &field);

  //去重与过期
  bool setnx(const std::string &key, const std::string &value);
  bool expire(const std::string &key, int seconds);

  // List操作(离线消息队列)
  bool lpush(const std::string &key, const std::string &value);
  std::vector<std::string> lrange(const std::string &key, int start, int stop);

  //计数器
  int64_t incr(const std::string &key);

  // zset操作
  bool zadd(const std::string &key, double score, const std::string &member);
  std::vector<std::string> zrangebyscore(const std::string &key, double min,
                                         double max, int limit = 0);
  bool zrem(const std::string &key, const std::string &member);
  // Pub/Sub发布（一次性，复用连接池）
  bool publish(const std::string &channel, const std::string &msg);

private:
  static constexpr size_t kDefaultPoolSize = 4;
  RedisPool pool_;
};