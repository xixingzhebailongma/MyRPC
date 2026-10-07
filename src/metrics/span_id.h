#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <unistd.h>

// span_id 生成器：独立于 trace_id 的 generateUuid。
// 64-bit = 进程前缀(getpid ^ 启动时间戳) << 32 | 原子自增计数，输出 16 位 hex。
// 前缀含 getpid + 启动时间，避免「多进程/重启后前缀相同」导致 span_id 碰撞。
inline std::string generateSpanId() {
  static uint32_t prefix = [] {
    uint64_t pid = static_cast<uint64_t>(::getpid());
    uint64_t ts = static_cast<uint64_t>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
    return static_cast<uint32_t>((pid << 16) ^ (ts & 0xffffu) ^ (ts >> 32));
  }();
  static std::atomic<uint32_t> counter{0};
  uint64_t v = (static_cast<uint64_t>(prefix) << 32) |
               counter.fetch_add(1, std::memory_order_relaxed);
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx",
                static_cast<unsigned long long>(v));
  return std::string(buf);
}
