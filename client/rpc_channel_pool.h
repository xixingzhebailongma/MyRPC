#pragma once
#include "rpc_channel.h"
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// 到其它服务节点的 RpcChannel 连接池（多路复用共享模型）：
// 每个 key 维护一条 RpcChannel，getOrCreate() 内部加锁，调用方无需手动加锁。
// 返回的裸指针在池的生命周期内稳定（channel 不回收）；
// RpcChannel 自身通过 call_mutex_ 串行化并发 Call，故可安全共享。

class RpcChannelPool {
public:
  RpcChannelPool() = default;
  RpcChannelPool(const RpcChannelPool &) = delete;
  RpcChannelPool &operator=(const RpcChannelPool &) = delete;

  // 按 key 复用；不存在则创建到 server_ip:server_port 的 channel。
  RpcChannel *getOrCreate(const std::string &key, const std::string &server_ip,
                          uint16_t server_port) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = channels_.find(key);
    if (it != channels_.end()) {
      return it->second.get();
    }
    auto ch = std::make_unique<RpcChannel>(server_ip, server_port);
    RpcChannel *raw = ch.get();
    channels_.emplace(key, std::move(ch));
    return raw;
  }

private:
  std::unordered_map<std::string, std::unique_ptr<RpcChannel>> channels_;
  std::mutex mutex_;
};