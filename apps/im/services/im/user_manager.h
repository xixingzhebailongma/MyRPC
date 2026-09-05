#pragma once

#include "Connection.h"
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// 多端会话管理：一个 user 可以有多条会话（session），每条会话对应一条 TCP
// 连接。 身份从 fd 反向推导（fd -> sid -> meta），热路径不查 Redis。
class UserManager {
public:
  struct SessionMeta {
    std::string session_id;
    std::string user_id;
    std::string username;
    std::string device_id;
    int device_type = 0;
    std::shared_ptr<Connection> conn;
    int64_t login_time = 0;
  };

  // 新会话上线（登录/重连握手成功后调用）
  void userOnline(const std::string &session_id, const std::string &user_id,
                  const std::string &username, const std::string &device_id,
                  int device_type, std::shared_ptr<Connection> conn);

  // 会话下线（按 fd）。
  // 返回 true 表示这是该 user 在本节点的最后一条会话，调用方据此决定是否注销
  // Route。 out_uid 填下线的 user_id；fd 未知时 out_uid 为空。
  bool userOfflineByFd(int fd, std::string *out_uid = nullptr);

  // 任一会话在线即算在线
  bool isOnline(const std::string &user_id);

  // 某用户在本节点的全部连接（多端投递用）
  std::vector<std::shared_ptr<Connection>>
  getConnections(const std::string &user_id);

  std::shared_ptr<Connection>
  getConnectionBySession(const std::string &session_id);

  std::string getUserIdByFd(int fd);
  std::string getSessionIdByFd(int fd);

private:
  std::mutex mutex_;
  std::unordered_map<std::string, SessionMeta> sessions_; // sid -> meta
  std::unordered_map<int, std::string> fd_to_session_;    // fd -> sid
  std::unordered_map<std::string, std::set<std::string>>
      user_to_sessions_; // uid -> {sid}
};