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
                                 std::string *cached_body) {
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
      // 已过期：惰性回收，落回插入
      erase(it->second);
    } else { // kInFlight
      if (now < e.deadline) {
        return IdemResult::kInFlight;
      }
      // 悬挂租约已过期：回收占位，允许重新 claim
      erase(it->second);
    }
  }

  // 插入 in-flight 占位
  Entry entry;
  entry.state = State::kInFlight;
  entry.deadline = now + std::chrono::milliseconds(in_flight_ttl_ms_);
  lru_.push_front({key, std::move(entry)});
  map_[key] = lru_.begin();
  evictIfNeeded();
  return IdemResult::kExecute;
}

void IdempotencyLru::complete(const std::string &key, const std::string &body) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end())
    return; // 已被淘汰/过期，no-op
  Entry &e = it->second->second;
  if (e.state != State::kInFlight)
    return;
  e.state = State::kDone;
  e.body = body;
  e.deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms_);
  touch(it->second);
}

void IdempotencyLru::abort(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end())
    return;
  if (it->second->second.state == State::kInFlight) {
    erase(it->second);
  }
}