#pragma once
#include "redis_client.h"
#include <atomic>
#include <functional>
#include <string>
#include <thread>

// 自持的 Redis Streams 广播订阅器（替代 Pub/Sub 的 RedisSubscriber）：
// 每节点用一条专属消费组（group 含节点身份，需唯一且跨重启稳定），后台线程
// 阻塞读 XREADGROUP ">"，处理完逐条 XACK，并周期 XAUTOCLAIM 回收处理到一半
// 崩溃的滞留消息。相比 Pub/Sub 的「发后即忘」，消息落盘在 Stream，断线期间
// 不丢、重连后按组位置补齐。
class StreamSubscriber {
public:
  StreamSubscriber() = default;
  ~StreamSubscriber();

  StreamSubscriber(const StreamSubscriber &) = delete;
  StreamSubscriber &operator=(const StreamSubscriber &) = delete;

  // 订阅 stream；group 必须每节点唯一且跨重启稳定（如 "<stream>:<ip>:<port>"）。
  // 每收到一条消息调用 on_message(payload)（payload = 条目的 body 字段值）。
  void start(const std::string &ip, int port, const std::string &stream,
             const std::string &group,
             std::function<void(const std::string &payload)> on_message);
  void stop();

private:
  void run(std::string ip, int port, std::string stream, std::string group,
           std::function<void(const std::string &payload)> on_message);
  // 处理一批条目：取 body 字段 → on_message → xack；返回处理条数。
  int handleEntries(
      const std::string &stream, const std::string &group,
      std::vector<StreamEntry> &entries,
      const std::function<void(const std::string &)> &on_message);

  std::atomic<bool> stop_{false};
  std::thread thread_;
  RedisClient redis_; // 专用连接池，与业务 Redis 隔离
};
