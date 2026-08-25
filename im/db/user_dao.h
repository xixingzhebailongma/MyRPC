#pragma once

#include "mysql_pool.h"
#include <functional>
#include <string>
#include <vector>

struct DbConfig {
  std::string host = "127.0.0.1";
  unsigned int port = 3306;
  std::string user = "root";
  std::string passwd;
  std::string db = "myrpc_im";
  size_t pool_size = 4;
};

// ==================== 用户数据变更事件（纯 C++，DAO 不依赖 proto）
// ====================
enum class UserEventType {
  PasswordChanged,
  UserBanned,
  UserDeleted,
  FriendRemoved
};

struct UserChangedEvent {
  UserEventType type;
  std::string user_id;
  std::string target_id; // 可选
  std::string reason;    // 可选
};

using UserEventPublisher = std::function<void(const UserChangedEvent &)>;

class UserDao {
public:
  UserDao();
  ~UserDao();
  UserDao(const UserDao &) = delete;
  UserDao &operator=(const UserDao &) = delete;

  bool init(const DbConfig &cfg);

  // 注入事件发布器：SQL 写成功后调用。ImServer 启动时注入。
  void setPublisher(UserEventPublisher publisher) {
    publisher_ = std::move(publisher);
  }

  // 已有方法（不变）
  bool registerUser(const std::string &username, const std::string &password,
                    std::string *err = nullptr);
  bool verifyLogin(const std::string &username, const std::string &password);
  bool addFriend(const std::string &user_id, const std::string &friend_id,
                 std::string *err = nullptr);
  std::vector<std::string> getFriendList(const std::string &user_id);

  // 新增：UPDATE/DELETE（SQL 成功后发事件）
  bool updatePassword(const std::string &user_id,
                      const std::string &new_password,
                      std::string *err = nullptr);
  bool deleteFriend(const std::string &user_id, const std::string &friend_id,
                    std::string *err = nullptr);
  bool deleteUser(const std::string &user_id, std::string *err = nullptr);
  bool updateUserStatus(
      const std::string &user_id, int status,
      std::string *err = nullptr); // 0=正常 1=封禁（需加 status 列）

private:
  std::string generateSalt();
  void publishUserEvent(UserChangedEvent ev); // 空发布器时安全跳过
  MysqlPool pool_;
  UserEventPublisher publisher_;
};