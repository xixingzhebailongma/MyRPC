#pragma once
#include "im.pb.h"
#include <chrono>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// 本地路由缓存：user_id -> 该用户所有在线节点列表(vector<RouteServer>)。
// 热路径先查这里，miss 再回源 RouteServer；路由变更由 Redis Pub/Sub
// 推送即时增量更新（上线 add / 下线 remove）。
class RouteCache {
public:
  // 兜底 TTL：即使推送丢失，缓存也不会永久陈旧
  static constexpr int kTtlMs = 60 * 1000;

  // 命中且未过期返回 true 并填充 out；否则返回 false
  bool get(const std::string &user_id, std::vector<im::RouteServer> *out);
  // 全量替换（回源 RouteServer 后）
  void put(const std::string &user_id, std::vector<im::RouteServer> servers);
  // 增量上线：按 (server_id, conn_id) 去重/更新，刷新 TTL
  void add(const std::string &user_id, const im::RouteServer &server);
  // 增量下线：移除指定 (server_id, conn_id) 连接，删空则整条 erase
  void remove(const std::string &user_id, const std::string &server_id,
              uint64_t conn_id);

private:
  struct Entry {
    std::vector<im::RouteServer> servers;
    std::chrono::steady_clock::time_point cached_at;
  };
  std::shared_mutex mutex_;
  std::unordered_map<std::string, Entry> map_;
};
