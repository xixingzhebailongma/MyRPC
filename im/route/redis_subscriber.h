#pragma once
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

// 前向声明 hiredis C 类型，避免在头文件暴露 hiredis.h
struct redisContext;

// 自持的 Redis Pub/Sub 订阅器：
// 用一条专用连接（不走 RedisPool），后台线程阻塞收取消息并回调
// on_message(payload)。
class RedisSubscriber {
public:
  RedisSubscriber() = default;
  ~RedisSubscriber();

  RedisSubscriber(const RedisSubscriber &) = delete;
  RedisSubscriber &operator=(const RedisSubscriber &) = delete;

  // 订阅 channel；每收到一条消息调用 on_message(payload)
  void start(const std::string &ip, int port, const std::string &channel,
             std::function<void(const std::string &payload)> on_message);
  void stop();

private:
  void run(std::string ip, int port, std::string channel,
           std::function<void(const std::string &payload)> on_message);

  std::atomic<bool> stop_{false};
  std::thread thread_;
  std::mutex mutex_;
  redisContext *ctx_ = nullptr; // 由工作线程持有/释放，stop() 用 fd 解除阻塞
};