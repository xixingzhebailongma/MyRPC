#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <string>

// 框架级幂等键生成器：随机前缀 + 单调时间戳 + 进程内自增计数器。
// 同一逻辑调用在 failover 重试间必须复用同一个 key，服务端才能据此去重。
// 与 apps/im/client/request_id.h 的 generateRequestId 思路一致，但独立成
// 框架公共能力（不依赖 app 层）。
inline std::string generateUuid() {
  static std::atomic<uint64_t> counter{0};
  static uint32_t rand_prefix = [] {
    std::random_device rd;
    return rd();
  }();
  uint64_t ts = static_cast<uint64_t>(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
  return std::to_string(rand_prefix) + "-" + std::to_string(ts) + "-" +
         std::to_string(seq);
}