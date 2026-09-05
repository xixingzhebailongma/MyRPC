#include "route_cache.h"

bool RouteCache::get(const std::string &user_id, im::RouteQueryResponse *out) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = map_.find(user_id);
  if (it == map_.end())
    return false;
  auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - it->second.cached_at)
                 .count();
  if (age >= kTtlMs)
    return false; // 已过期，视为 miss
  out->set_found(true);
  out->set_server_id(it->second.server_id);
  out->set_server_ip(it->second.server_ip);
  out->set_server_port(it->second.server_port);
  return true;
}

void RouteCache::put(const std::string &user_id, const std::string &server_id,
                     const std::string &server_ip, int32_t server_port) {
  Entry e{server_id, server_ip, server_port, std::chrono::steady_clock::now()};
  std::lock_guard<std::mutex> lk(mutex_);
  map_[user_id] = e;
}

void RouteCache::invalidate(const std::string &user_id) {
  std::lock_guard<std::mutex> lk(mutex_);
  map_.erase(user_id);
}