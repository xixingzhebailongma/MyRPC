#pragma once
#include <atomic>
#include <chrono>
#include <random>
#include <string>

// 生成全局唯一请求 ID，作为 SendMessage 的幂等键（client_request_id）。
// 规则：随机前缀 + 纳秒时间戳 + 进程内自增序号，不引入外部 UUID 库。
// 契约：客户端应用层超时重试时必须复用同一个 ID（同一业务操作同一键），
// 不要每次网络重发都换键，否则服务端幂等去重无法生效。
inline std::string generateRequestId() {
  static std::atomic<uint64_t> counter{0};
  static uint32_t rand_prefix = [] {
    std::random_device rd;
    return rd();
  }();
  uint64_t ts =
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
  return std::to_string(rand_prefix) + "-" + std::to_string(ts) + "-" +
         std::to_string(seq);
}