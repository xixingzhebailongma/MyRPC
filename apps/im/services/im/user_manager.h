#pragma once

#include <cstdint>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// 客户端连接的位置引用：接入层拆分后，IM 节点不再持有客户端 TCP 连接，
// 而是用 (gateway_id, conn_id) 定位到某个 Gateway 上的连接；回推时按
// gateway_rpc_ip:gateway_rpc_port 把帧发给那个 Gateway。
struct ClientConnRef {
  std::string gateway_id;
  uint64_t conn_id = 0;
  std::string gateway_rpc_ip;
  int gateway_rpc_port = 0;

  std::string key() const { return gateway_id + ":" + std::to_string(conn_id); }
};

// 多端会话管理：一个 user 可以有多条会话（session），每条会话对应一个客户端
// 连接（ClientConnRef）。身份从连接 key 反向推导（key -> sid -> meta）。
class UserManager {
public:
  struct SessionMeta {
    std::string session_id;
    std::string user_id;
    std::string username;
    std::string device_id;
    int device_type = 0;
    ClientConnRef conn;
    int64_t login_time = 0;
  };

  // 新会话上线（登录/重连握手成功后调用）
  void userOnline(const std::string &session_id, const std::string &user_id,
                  const std::string &username, const std::string &device_id,
                  int device_type, const ClientConnRef &conn);

  // 会话下线（按连接 key）。
  // 返回 true 表示这是该 user 在本节点的最后一条会话，调用方据此决定是否注销
  // Route。 out_uid 填下线的 user_id；key 未知时 out_uid 为空。
  bool userOfflineByConn(const std::string &conn_key,
                         std::string *out_uid = nullptr);

  // 任一会话在线即算在线
  bool isOnline(const std::string &user_id);

  // 某用户在本节点的全部连接（多端投递用）
  std::vector<ClientConnRef> getConnections(const std::string &user_id);

  // 按连接 (gateway_id, conn_id) 反向推导 user_id
  std::string getUserIdByConn(const std::string &gateway_id, uint64_t conn_id);

  // 统计接口（周期任务/监控用）：返回在线用户数与活跃会话数。
  // 只读，用 shared_lock，可安全地在任意线程调用。
  size_t onlineUserCount();
  size_t sessionCount();

private:
  std::string makeKey(const std::string &gateway_id, uint64_t conn_id) const {
    return gateway_id + ":" + std::to_string(conn_id);
  }

  std::shared_mutex mutex_;
  std::unordered_map<std::string, SessionMeta> sessions_; // sid -> meta
  std::unordered_map<std::string, std::string>
      conn_key_to_session_; // conn_key -> sid
  std::unordered_map<std::string, std::set<std::string>>
      user_to_sessions_; // uid -> {sid}
};