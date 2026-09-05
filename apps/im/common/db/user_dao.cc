#include "user_dao.h"
#include "Logger.h"
#include <cppconn/connection.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <random>
#include <string>

namespace {
// MySQL 连接断开类错误码：2006 server gone away / 2013 lost connection
bool isConnLost(int code) { return code == 2006 || code == 2013; }
} // namespace

UserDao::UserDao() = default;
UserDao::~UserDao() = default;

// ==================== init ====================
bool UserDao::init(const DbConfig &cfg) {
  return pool_.init(cfg.host, cfg.port, cfg.user, cfg.passwd, cfg.db,
                    cfg.pool_size);
}

// ==================== generateSalt ====================
std::string UserDao::generateSalt() {
  thread_local std::mt19937_64 gen{[] {
    std::random_device rd;
    return (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
  }()};
  static const char hex[] = "0123456789abcdef";
  std::uniform_int_distribution<uint64_t> dist;
  uint64_t a = dist(gen);
  uint64_t b = dist(gen);
  std::string salt;
  salt.reserve(32);
  for (uint64_t v : {a, b}) {
    for (int i = 7; i >= 0; --i) {
      unsigned char byte = static_cast<unsigned char>((v >> (i * 8)) & 0xff);
      salt.push_back(hex[byte >> 4]);
      salt.push_back(hex[byte & 0x0f]);
    }
  }
  return salt;
}

// ==================== registerUser ====================
bool UserDao::registerUser(const std::string &username,
                           const std::string &password, std::string *err) {
  auto guard = pool_.acquire();
  if (!guard) {
    if (err)
      *err = "db unavailable";
    return false;
  }
  sql::Connection *conn = guard.get();
  try {
    std::string salt = generateSalt();
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "INSERT INTO users (username, password_hash, salt) "
        "VALUES (?, SHA2(CONCAT(?, ?), 256), ?)"));
    pstmt->setString(1, username);
    pstmt->setString(2, password);
    pstmt->setString(3, salt);
    pstmt->setString(4, salt);
    pstmt->executeUpdate();
    return true;
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::registerUser failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    if (err) {
      *err =
          (e.getErrorCode() == 1062) ? "username already exists" : "db error";
    }
    return false;
  }
}

// ==================== verifyLogin ====================
bool UserDao::verifyLogin(const std::string &username,
                          const std::string &password) {
  auto guard = pool_.acquire();
  if (!guard)
    return false;
  sql::Connection *conn = guard.get();
  try {
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "SELECT username FROM users "
        "WHERE username = ? AND password_hash = SHA2(CONCAT(?, salt), 256)"));
    pstmt->setString(1, username);
    pstmt->setString(2, password);
    std::unique_ptr<sql::ResultSet> res(pstmt->executeQuery());
    return res->next(); // 有行 → 通过
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::verifyLogin failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    return false;
  }
}

// ==================== addFriend ====================
bool UserDao::addFriend(const std::string &user_id,
                        const std::string &friend_id, std::string *err) {
  if (user_id == friend_id) {
    if (err)
      *err = "cannot add self";
    return false;
  }
  auto guard = pool_.acquire();
  if (!guard) {
    if (err)
      *err = "db unavailable";
    return false;
  }
  sql::Connection *conn = guard.get();
  try {
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "INSERT INTO friends (user_id, friend_id) VALUES (?, ?), (?, ?)"));
    pstmt->setString(1, user_id);
    pstmt->setString(2, friend_id);
    pstmt->setString(3, friend_id);
    pstmt->setString(4, user_id);
    pstmt->executeUpdate();
    return true;
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::addFriend failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    if (err) {
      if (e.getErrorCode() == 1062)
        *err = "already friends";
      else if (e.getErrorCode() == 1452)
        *err = "user not found";
      else
        *err = "db error";
    }
    return false;
  }
}

// ==================== getFriendList ====================
std::vector<std::string> UserDao::getFriendList(const std::string &user_id) {
  auto guard = pool_.acquire();
  if (!guard)
    return {};
  sql::Connection *conn = guard.get();
  std::vector<std::string> friends;
  try {
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "SELECT friend_id FROM friends WHERE user_id = ?"));
    pstmt->setString(1, user_id);
    std::unique_ptr<sql::ResultSet> res(pstmt->executeQuery());
    while (res->next()) {
      friends.emplace_back(res->getString(1).c_str());
    }
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::getFriendList failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
  }
  return friends;
}

// ==================== publishUserEvent ====================
void UserDao::publishUserEvent(UserChangedEvent ev) {
  if (publisher_) {
    publisher_(ev);
  }
}

// ==================== updatePassword ====================
bool UserDao::updatePassword(const std::string &user_id,
                             const std::string &new_password,
                             std::string *err) {
  auto guard = pool_.acquire();
  if (!guard) {
    if (err)
      *err = "db unavailable";
    return false;
  }
  sql::Connection *conn = guard.get();
  try {
    std::string salt = generateSalt();
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "UPDATE users SET password_hash = SHA2(CONCAT(?, ?), 256), salt = ? "
        "WHERE username = ?"));
    pstmt->setString(1, new_password);
    pstmt->setString(2, salt);
    pstmt->setString(3, salt);
    pstmt->setString(4, user_id);
    int affected = pstmt->executeUpdate();
    if (affected <= 0) {
      if (err)
        *err = "user not found";
      return false;
    }
    publishUserEvent(UserChangedEvent{UserEventType::PasswordChanged, user_id,
                                      "", "password changed"});
    return true;
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::updatePassword failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    if (err)
      *err = "db error";
    return false;
  }
}

// ==================== deleteFriend ====================
bool UserDao::deleteFriend(const std::string &user_id,
                           const std::string &friend_id, std::string *err) {
  auto guard = pool_.acquire();
  if (!guard) {
    if (err)
      *err = "db unavailable";
    return false;
  }
  sql::Connection *conn = guard.get();
  try {
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "DELETE FROM friends WHERE (user_id = ? AND friend_id = ?)"
        "OR (user_id = ? AND friend_id = ?)"));
    pstmt->setString(1, user_id);
    pstmt->setString(2, friend_id);
    pstmt->setString(3, friend_id);
    pstmt->setString(4, user_id);
    pstmt->executeUpdate();
    publishUserEvent(UserChangedEvent{UserEventType::FriendRemoved, user_id,
                                      friend_id, "friend removed"});
    return true;
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::deleteFriend failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    if (err)
      *err = "db error";
    return false;
  }
}

// ==================== deleteUser ====================
// 注意：这里两次 executeUpdate，不是单事务（要严谨可用
// conn->setAutoCommit(false)+commit）。 当前 friends 表已设 ON DELETE
// CASCADE，删 users 会自动级联删 friends。
bool UserDao::deleteUser(const std::string &user_id, std::string *err) {
  auto guard = pool_.acquire();
  if (!guard) {
    if (err)
      *err = "db unavailable";
    return false;
  }
  sql::Connection *conn = guard.get();
  try {
    {
      std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
          "DELETE FROM friends WHERE user_id = ? OR friend_id = ?"));
      pstmt->setString(1, user_id);
      pstmt->setString(2, user_id);
      pstmt->executeUpdate();
    }
    std::unique_ptr<sql::PreparedStatement> pstmt(
        conn->prepareStatement("DELETE FROM users WHERE username = ?"));
    int affected = pstmt->executeUpdate();
    if (affected <= 0) {
      if (err)
        *err = "user not found";
      return false;
    }
    publishUserEvent(UserChangedEvent{UserEventType::UserDeleted, user_id, "",
                                      "user deleted"});
    return true;
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::deleteUser failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    if (err)
      *err = "db error";
    return false;
  }
}

// ==================== updateUserStatus（封禁/解封，需先加 status 列）
// ====================
bool UserDao::updateUserStatus(const std::string &user_id, int status,
                               std::string *err) {
  auto guard = pool_.acquire();
  if (!guard) {
    if (err)
      *err = "db unavailable";
    return false;
  }
  sql::Connection *conn = guard.get();
  try {
    std::unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
        "UPDATE users SET status = ? WHERE username = ?"));
    pstmt->setInt(1, status);
    pstmt->setString(2, user_id);
    int affected = pstmt->executeUpdate();
    if (affected <= 0) {
      if (err)
        *err = "user not found";
      return false;
    }
    if (status != 0) {
      publishUserEvent(UserChangedEvent{UserEventType::UserBanned, user_id, "",
                                        "user banned"});
    }
    return true;
  } catch (sql::SQLException &e) {
    LOG_ERROR("UserDao::updateUserStatus failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    if (isConnLost(e.getErrorCode()))
      guard.markBroken();
    if (err)
      *err = "db error";
    return false;
  }
}