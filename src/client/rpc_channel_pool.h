#pragma once
#include "rpc_channel.h"
#include "rpc_client_config.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
// 到其它服务节点的 RpcChannel 连接池（多路复用共享模型）：
// 每个 key 维护一条 RpcChannel，getOrCreate() 内部加锁，调用方无需手动加锁。
// 返回 shared_ptr：池通过 removeExcept()/reapOnce() 剔除 key 时，被调用方持有
// 的 shared_ptr 仍保活对象，因此在途 Call 不会悬垂。
// RpcChannel 自身按 sequence_id 多路复用（后台收帧线程 + 响应 map），
// 故单条 channel 即可并发 Call、安全共享。
//
// 生命周期治理：内置后台 reaper 线程，周期扫空闲 channel（超过 idle_ttl_ms
// 未调用即主动 close + 剔除），避免连接池只增不减；节点下线经 removeExcept
// 剔除时同样主动 close，让在途调用快速失败、上层 failover。

class RpcChannelPool {
public:
  // idle_ttl_ms: 一条 channel 超过该时长未被调用即判空闲并回收（默认 60s）。
  // reap_interval: 后台扫描周期（默认 10s）。
  RpcChannelPool(uint64_t idle_ttl_ms = 60'000,
                 std::chrono::milliseconds reap_interval =
                     std::chrono::milliseconds(10'000))
      : idle_ttl_ms_(idle_ttl_ms), reap_interval_(reap_interval) {
    reaper_ = std::thread(&RpcChannelPool::reaperLoop, this);
  }

  // 从集中配置构造：空闲回收阈值 / 扫描周期 / channel 参数均来自
  // RpcClientConfig
  RpcChannelPool(const RpcClientConfig &cfg)
      : cfg_(cfg), idle_ttl_ms_(cfg.channel_idle_ttl_ms),
        reap_interval_(
            std::chrono::milliseconds(cfg.channel_reap_interval_ms)) {
    reaper_ = std::thread(&RpcChannelPool::reaperLoop, this);
  }

  RpcChannelPool(const RpcChannelPool &) = delete;
  RpcChannelPool &operator=(const RpcChannelPool &) = delete;

  ~RpcChannelPool() {
    {
      std::lock_guard<std::mutex> lock(reaper_mutex_);
      reaper_running_.store(false);
    }
    reaper_cv_.notify_all();
    if (reaper_.joinable())
      reaper_.join();
  }

  // 按 key 复用；不存在则创建到 server_ip:server_port 的 channel。
  // timeout_ms: 显式覆盖该 channel 的每次 Call 等待超时；<0 表示用配置值。
  std::shared_ptr<RpcChannel> getOrCreate(const std::string &key,
                                          const std::string &server_ip,
                                          uint16_t server_port,
                                          int timeout_ms = -1) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = channels_.find(key);
    if (it != channels_.end()) {
      return it->second;
    }
    auto ch = std::make_shared<RpcChannel>(server_ip, server_port, cfg_);
    if (timeout_ms >= 0) {
      ch->setTimeout(timeout_ms);
    }
    channels_.emplace(key, ch);
    return ch;
  }
  // 非创建式查找：key 不存在返回 nullptr（供熔断判定 / 选节点跳过用）。
  std::shared_ptr<RpcChannel> get(const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = channels_.find(key);
    return it == channels_.end() ? nullptr : it->second;
  }

  // 当前池中 channel 数（测试/观测用）。
  size_t size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return channels_.size();
  }

  // key 是否仍在池中（测试/观测用）。
  bool contains(const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return channels_.count(key) != 0;
  }

  // 移除 key 不在 keep 中的 channel，并主动 close() 唤醒在途 Call
  //（上层 LbRpcClient 自然 failover），而不是等 shared_ptr 归零析构。
  void removeExcept(const std::unordered_set<std::string> &keep) {
    std::vector<std::shared_ptr<RpcChannel>> to_close;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (auto it = channels_.begin(); it != channels_.end();) {
        if (!keep.count(it->first)) {
          to_close.push_back(it->second);
          it = channels_.erase(it);
        } else {
          ++it;
        }
      }
    }
    // 锁外 close：close() 内部会 join readLoop 线程，不能在持锁状态下阻塞。
    for (auto &ch : to_close)
      ch->close();
  }

private:
  // 后台回收循环：每 reap_interval_ 扫一次。
  // 用 condition_variable::wait_for 而非 sleep_for，析构时 notify 可立即唤醒，
  // 避免 ~RpcChannelPool 的 join 阻塞到当前整个扫描周期结束。
  void reaperLoop() {
    // 用独立的 reaper_mutex_ 做唤醒互斥，与保护 channels_ 的 mutex_ 解耦，
    // 避免 reaper 与 getOrCreate/removeExcept 在同一把锁上互相阻塞。
    std::unique_lock<std::mutex> lock(reaper_mutex_);
    while (reaper_running_.load()) {
      if (reaper_cv_.wait_for(lock, reap_interval_,
                              [this] { return !reaper_running_.load(); })) {
        break; // 析构唤醒，退出
      }
      reapOnce(); // 内部自己锁 mutex_（channels_ 守卫），与 reaper_mutex_ 无关
    }
  }

  // 单次回收：收集空闲超时的 channel，锁外主动 close。
  void reapOnce() {
    std::vector<std::shared_ptr<RpcChannel>> to_close;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      uint64_t now = nowMs();
      for (auto it = channels_.begin(); it != channels_.end();) {
        if (now - it->second->lastUsedMs() > idle_ttl_ms_) {
          to_close.push_back(it->second);
          it = channels_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (auto &ch : to_close)
      ch->close();
  }

  RpcClientConfig cfg_;
  uint64_t idle_ttl_ms_;
  std::chrono::milliseconds reap_interval_;
  std::atomic<bool> reaper_running_{true};
  std::thread reaper_;
  std::mutex reaper_mutex_;
  std::condition_variable reaper_cv_;
  std::unordered_map<std::string, std::shared_ptr<RpcChannel>> channels_;
  std::mutex mutex_;
};