#pragma once
#include "redis_client.h"
#include <cstdint>
#include <functional>
#include <string>

// 消息队列消费者：当前后端是 Redis Stream 消费组，接口对齐未来 RocketMQ。
// handler(entry_id, payload)：消费一条消息；是否 ACK 由调用方决定（见 ack()）。
class StreamConsumer {
public:
  using Handler = std::function<void(const std::string &entry_id,
                                     const std::string &payload)>;

  // 连接 Redis；失败返回 false
  bool connect(const std::string &ip, int port);
  // 幂等创建消费组（组已存在返回 true）
  bool ensureGroup(const std::string &stream, const std::string &group);
  // 从 stream 读一批新消息（">"），逐条交给 handler；返回处理条数
  int consume(const std::string &stream, const std::string &group,
              const std::string &consumer, int count, const Handler &handler);
  // XAUTOCLAIM 回收闲置超过 min_idle_ms 的消息，逐条交给 handler；返回处理条数
  int reclaim(const std::string &stream, const std::string &group,
              const std::string &consumer, int64_t min_idle_ms, int count,
              const Handler &handler);
  // 确认一条消息（从 PEL 移除），成功返回 true
  bool ack(const std::string &stream, const std::string &group,
           const std::string &id);

private:
  RedisClient redis_;
};