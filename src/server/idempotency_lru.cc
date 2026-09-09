#include "idempotency_lru.h"
#include <iterator>

IdempotencyLru::IdempotencyLru(size_t max_entries, uint64_t ttl_ms,
                               uint64_t in_flight_ttl_ms)
    : max_entries_(max_entries), ttl_ms_(ttl_ms),
      in_flight_ttl_ms_(in_flight_ttl_ms) {}

void IdempotencyLru::touch(LruList::iterator it) {
  lru_.splice(lru_.begin(), lru_, it); // 移到最前（MRU）
}

void IdempotencyLru::erase(LruList::iterator it) {
  map_.erase(it->first);
  lru_.erase(it);
}

void IdempotencyLru::evictIfNeeded() {
  while (lru_.size() > max_entries_) {
    erase(std::prev(lru_.end())); // 淘汰尾部（LRU）
  }
}

IdemResult IdempotencyLru::claim(const std::string &key,
                                 std::string *cached_body, IdemLease *lease) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto now = std::chrono::steady_clock::now();
  auto it = map_.find(key);

  if (it != map_.end()) {
    Entry &e = it->second->second;
    if (e.state == State::kDone) {
      if (now < e.deadline) {
        touch(it->second);
        if (cached_body)
          *cached_body = e.body;
        return IdemResult::kReplay;
      }
      erase(it->second); // 已过期：惰性回收，落回插入
    } else {             // kInFlight
      if (now < e.deadline) {
        return IdemResult::kInFlight;
      }
      erase(it->second); // 悬挂租约已过期：回收占位，允许重新 claim
    }
  }

  // 插入 in-flight 占位，并发放 fencing token
  Entry entry;
  entry.state = State::kInFlight;
  entry.token = ++next_token_;
  entry.deadline = now + std::chrono::milliseconds(in_flight_ttl_ms_);
  lru_.push_front({key, std::move(entry)});
  map_[key] = lru_.begin();
  evictIfNeeded();
  if (lease)
    lease->token = next_token_;
  return IdemResult::kExecute;
}

bool IdempotencyLru::renew(const std::string &key, const IdemLease &lease) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end())
    return false; // 已被淘汰/过期
  Entry &e = it->second->second;
  if (e.state != State::kInFlight || e.token != lease.token)
    return false; // 已被新请求接管
  // 刷新 in-flight 租约
  e.deadline = std::chrono::steady_clock::now() +
               std::chrono::milliseconds(in_flight_ttl_ms_);
  return true;
}

bool IdempotencyLru::complete(const std::string &key, const IdemLease &lease,
                              const std::string &body) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end())
    return false; // 已被淘汰/过期
  Entry &e = it->second->second;
  if (e.state != State::kInFlight || e.token != lease.token)
    return false; // 已被新请求接管：丢弃本次结果
  e.state = State::kDone;
  e.body = body;
  e.deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms_);
  touch(it->second);
  return true;
}

void IdempotencyLru::abort(const std::string &key, const IdemLease &lease) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end())
    return;
  Entry &e = it->second->second;
  if (e.state == State::kInFlight && e.token == lease.token) {
    erase(it->second);
  }
}