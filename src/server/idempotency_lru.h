#pragma once
#include "idempotency_store.h"
#include <chrono>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

// 进程内线程安全 LRU 实现。完成态响应缓存 ttl_ms，in-flight 占位用
// in_flight_ttl_ms 作为防悬挂租约；renew() 可刷新该租约。每个 in-flight 项
// 带一个 fencing token（单进程单调自增），complete/abort 校验 token，防止
// 慢 worker 的迟到完成覆盖新请求的执行结果。
class IdempotencyLru final : public IdempotencyStore {
public:
  explicit IdempotencyLru(size_t max_entries = 10000,
                          uint64_t ttl_ms = 120000, // 对齐 IM 的 120s
                          uint64_t in_flight_ttl_ms = 10000);

  IdemResult claim(const std::string &key, std::string *cached_body,
                   IdemLease *lease) override;
  bool renew(const std::string &key, const IdemLease &lease) override;
  bool complete(const std::string &key, const IdemLease &lease,
                const std::string &body) override;
  void abort(const std::string &key, const IdemLease &lease) override;

private:
  enum class State { kInFlight, kDone };

  struct Entry {
    State state = State::kInFlight;
    std::string body;   // 仅 kDone 有效
    uint64_t token = 0; // fencing token，仅 kInFlight 有效
    std::chrono::steady_clock::time_point deadline;
  };

  using LruList = std::list<std::pair<std::string, Entry>>; // front = MRU

  void touch(LruList::iterator it); // move-to-front，须持锁
  void erase(LruList::iterator it); // 从 map_ + lru_ 移除，须持锁
  void evictIfNeeded();             // 超限时从尾部淘汰，须持锁

  std::mutex mutex_;
  std::unordered_map<std::string, LruList::iterator> map_;
  LruList lru_;
  size_t max_entries_;
  uint64_t ttl_ms_;
  uint64_t in_flight_ttl_ms_;
  uint64_t next_token_ = 0; // 单调自增，为每次 claim 发放唯一 token
};