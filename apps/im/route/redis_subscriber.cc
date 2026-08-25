#include "redis_subscriber.h"
#include "Logger.h"
#include <chrono>
#include <hiredis/hiredis.h>
#include <sys/socket.h>
#include <thread>

RedisSubscriber::~RedisSubscriber() { stop(); }

void RedisSubscriber::start(
    const std::string &ip, int port, const std::string &channel,
    std::function<void(const std::string &payload)> on_message) {
  if (thread_.joinable())
    return;
  stop_ = false;
  thread_ = std::thread(&RedisSubscriber::run, this, ip, port, channel,
                        std::move(on_message));
}

void RedisSubscriber::stop() {
  stop_ = true;
  // 关闭底层 socket，使阻塞在 redisGetReply 的线程立即返回，线程再自行释放连接
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (ctx_ && ctx_->fd > 0) {
      shutdown(ctx_->fd, SHUT_RDWR);
    }
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}
/*外层 while (未停止)
  ├─ 连接 Redis
  ├─ 订阅频道
  └─ 内层 while (未停止)
       ├─ redisGetReply 阻塞读
       ├─ 出错 → break
  ↓
  清理 ctx / 释放连接
  ↓
  若未停止 → 睡眠 1 秒 → 回到外层循环开头*/
void RedisSubscriber::run(
    std::string ip, int port, std::string channel,
    std::function<void(const std::string &payload)> on_message) {
  while (!stop_.load()) {
    // 1. 建立专用连接
    redisContext *ctx = redisConnect(ip.c_str(), port);
    if (!ctx || ctx->err != 0) {
      if (ctx)
        redisFree(ctx);
      LOG_ERROR("RedisSubscriber: connect %s:%d failed", ip.c_str(), port);
      if (!stop_.load())
        std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    {
      std::lock_guard<std::mutex> lk(mutex_);
      ctx_ = ctx;
    }

    // 2. 订阅频道
    redisReply *reply = static_cast<redisReply *>(
        redisCommand(ctx, "SUBSCRIBE %b", channel.data(), channel.size()));
    if (!reply || ctx->err != 0) {
      if (reply)
        freeReplyObject(reply);
      LOG_ERROR("RedisSubscriber: SUBSCRIBE %s failed", channel.c_str());
    } else {
      freeReplyObject(reply); // 订阅确认，忽略
      // 3. 阻塞收取消息
      while (!stop_.load()) {
        reply = nullptr;
        if (redisGetReply(ctx, reinterpret_cast<void **>(&reply)) != REDIS_OK) {
          break; // 连接断开 / 被 stop() 关闭
        }
        if (!reply)
          continue;
        // 收到 message 时结构：[0]="message", [1]=频道名, [2]=负载
        if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3 &&
            reply->element[0]->type == REDIS_REPLY_STRING &&
            std::string(reply->element[0]->str, reply->element[0]->len) ==
                "message") {
          std::string payload(reply->element[2]->str, reply->element[2]->len);
          freeReplyObject(reply);
          on_message(payload);
        } else {
          freeReplyObject(reply); // 其它类型消息，忽略
        }
      }
    }

    // 4. 清理本连接，断线后自愈重连
    {
      std::lock_guard<std::mutex> lk(mutex_);
      if (ctx_ == ctx)
        ctx_ = nullptr;
    }
    redisFree(ctx);
    if (!stop_.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}