#pragma once
#include "redis_client.h"
#include <string>

// 消息队列生产者：当前后端是 Redis Stream，接口对齐未来 RocketMQ。
// 调用方只关心「把一条 opaque payload 投进某个队列」，不感知底层字段名。
class StreamProducer {
public:
  // 连接 Redis；失败返回 false
  bool connect(const std::string &ip, int port);
  // 把 payload 投进 stream；成功返回 true 并把条目 id 写入 *out_id（可为空）
  bool produce(const std::string &stream, const std::string &payload,
               std::string *out_id);

private:
  RedisClient redis_;
};