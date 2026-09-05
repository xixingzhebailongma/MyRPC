#include "user_manager.h"
#include "Connection.h"
#include <mutex>

void UserManager::userOnline(const std::string &session_id,
                             const std::string &user_id,
                             const std::string &username,
                             const std::string &device_id, int device_type,
                             std::shared_ptr<Connection> conn) {
  std::lock_guard<std::mutex> lock(mutex_);
  int64_t login_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
  SessionMeta meta{session_id,  user_id, username,  device_id,
                   device_type, conn,    login_time};
  sessions_[session_id] = std::move(meta);
  fd_to_session_[conn->fd()] = session_id;
  user_to_sessions_[user_id].insert(session_id);
}

bool UserManager::userOfflineByFd(int fd, std::string *out_uid) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto fit = fd_to_session_.find(fd);
  if (fit == fd_to_session_.end()) {
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
  fd_to_session_.erase(fit);
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
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = user_to_sessions_.find(user_id);
  return it != user_to_sessions_.end() && !it->second.empty();
}

std::vector<std::shared_ptr<Connection>>
UserManager::getConnections(const std::string &user_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::shared_ptr<Connection>> result;
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

std::shared_ptr<Connection>
UserManager::getConnectionBySession(const std::string &session_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = sessions_.find(session_id);
  if (it != sessions_.end())
    return it->second.conn;
  return nullptr;
}

std::string UserManager::getUserIdByFd(int fd) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto fit = fd_to_session_.find(fd);
  if (fit == fd_to_session_.end())
    return "";
  auto sit = sessions_.find(fit->second);
  if (sit == sessions_.end())
    return "";
  return sit->second.user_id;
}

std::string UserManager::getSessionIdByFd(int fd) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto fit = fd_to_session_.find(fd);
  if (fit == fd_to_session_.end())
    return "";
  return fit->second;
}