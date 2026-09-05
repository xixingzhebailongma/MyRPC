#include "user_manager.h"
#include <chrono>
#include <mutex>
#include <shared_mutex>
void UserManager::userOnline(const std::string &session_id,
                             const std::string &user_id,
                             const std::string &username,
                             const std::string &device_id, int device_type,
                             const ClientConnRef &conn) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  int64_t login_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
  SessionMeta meta{session_id,  user_id, username,  device_id,
                   device_type, conn,    login_time};
  sessions_[session_id] = std::move(meta);
  conn_key_to_session_[conn.key()] = session_id;
  user_to_sessions_[user_id].insert(session_id);
}

bool UserManager::userOfflineByConn(const std::string &conn_key,
                                    std::string *out_uid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto fit = conn_key_to_session_.find(conn_key);
  if (fit == conn_key_to_session_.end()) {
    if (out_uid)
      *out_uid = "";
    return false; // 未认证连接关闭：无会话可清
  }

  const std::string &sid = fit->second;
  std::string user_id;
  auto sit = sessions_.find(sid);
  if (sit != sessions_.end()) {
    user_id = sit->second.user_id;
  }
  conn_key_to_session_.erase(fit);
  if (sit != sessions_.end()) {
    sessions_.erase(sit);
  }
  if (out_uid)
    *out_uid = user_id;
  // 判断是否该 user 的最后一条会话
  bool was_last = false;
  auto uit = user_to_sessions_.find(user_id);
  if (uit != user_to_sessions_.end()) {
    uit->second.erase(sid);
    if (uit->second.empty()) {
      user_to_sessions_.erase(uit);
      was_last = true;
    }
  }
  return was_last;
}

bool UserManager::isOnline(const std::string &user_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = user_to_sessions_.find(user_id);
  return it != user_to_sessions_.end() && !it->second.empty();
}

std::vector<ClientConnRef>
UserManager::getConnections(const std::string &user_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<ClientConnRef> result;
  auto it = user_to_sessions_.find(user_id);
  if (it == user_to_sessions_.end())
    return result;
  for (const auto &sid : it->second) {
    auto sit = sessions_.find(sid);
    if (sit != sessions_.end()) {
      result.push_back(sit->second.conn);
    }
  }
  return result;
}

std::string UserManager::getUserIdByConn(const std::string &gateway_id,
                                         uint64_t conn_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto fit = conn_key_to_session_.find(makeKey(gateway_id, conn_id));
  if (fit == conn_key_to_session_.end())
    return "";
  auto sit = sessions_.find(fit->second);
  if (sit == sessions_.end())
    return "";
  return sit->second.user_id;
}

size_t UserManager::onlineUserCount() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return user_to_sessions_.size();
}

size_t UserManager::sessionCount() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return sessions_.size();
}