#include "route_cache.h"
#include <algorithm>
#include <shared_mutex>
bool RouteCache::get(const std::string &user_id,
                     std::vector<im::RouteServer> *out) {
  std::shared_lock<std::shared_mutex> lk(mutex_);
  auto it = map_.find(user_id);
  if (it == map_.end())
    return false;
  auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - it->second.cached_at)
                 .count();
  if (age >= kTtlMs)
    return false; // 已过期，视为 miss
  *out = it->second.servers;
  return true;
}

void RouteCache::put(const std::string &user_id,
                     std::vector<im::RouteServer> servers) {
  Entry e;
  e.servers = std::move(servers);
  e.cached_at = std::chrono::steady_clock::now();
  std::lock_guard<std::shared_mutex> lk(mutex_);
  map_[user_id] = std::move(e);
}

void RouteCache::add(const std::string &user_id,
                     const im::RouteServer &server) {
  std::lock_guard<std::shared_mutex> lk(mutex_);
  auto now = std::chrono::steady_clock::now();
  auto it = map_.find(user_id);
  if (it == map_.end()) {
    Entry e;
    e.servers.push_back(server);
    e.cached_at = now;
    map_[user_id] = std::move(e);
    return;
  }
  auto &vec = it->second.servers;
  for (auto &s : vec) {
    if (s.server_id() == server.server_id() &&
        s.conn_id() == server.conn_id()) {
      s = server; // 同一连接重登：更新 ip/port，去重
      it->second.cached_at = now;
      return;
    }
  }
  vec.push_back(server);
  it->second.cached_at = now;
}

void RouteCache::remove(const std::string &user_id,
                        const std::string &server_id, uint64_t conn_id) {
  std::lock_guard<std::shared_mutex> lk(mutex_);
  auto it = map_.find(user_id);
  if (it == map_.end())
    return;
  auto &vec = it->second.servers;
  vec.erase(std::remove_if(vec.begin(), vec.end(),
                           [&](const im::RouteServer &s) {
                             return s.server_id() == server_id &&
                                    s.conn_id() == conn_id;
                           }),
            vec.end());
  if (vec.empty()) {
    map_.erase(it);
  } else {
    it->second.cached_at = std::chrono::steady_clock::now();
  }
}
