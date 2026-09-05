#pragma once
#include "im.pb.h"
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

// 本地路由缓存：user_id -> 目标节点(server_id/ip/port)。
// 热路径先查这里，miss 再回源 RouteServer；路由变更由 Redis Pub/Sub
// 推送即时更新。
class RouteCache {
public:
  // 兜底 TTL：即使推送丢失，缓存也不会永久陈旧
  static constexpr int kTtlMs = 60 * 1000;

  // 命中且未过期返回 true 并填充 out；否则返回 false
  bool get(const std::string &user_id, im::RouteQueryResponse *out);
  void put(const std::string &user_id, const std::string &server_id,
           const std::string &server_ip, int32_t server_port);
  void invalidate(const std::string &user_id);

private:
  struct Entry {
    std::string server_id;
    std::string server_ip;
    int32_t server_port = 0;
    std::chrono::steady_clock::time_point cached_at;
  };
  std::mutex mutex_;
  std::unordered_map<std::string, Entry> map_;
};