#pragma once
  
  #include "Connection.h"
  #include <chrono>
  #include <memory>
  #include <mutex>
  #include <string>
  #include <unordered_map>
  #include <vector>

  class UserManager{
    public:
    void userOnline(const std::string& user_id,
                    const std::string& username,
                    std::shared_ptr<Connection>conn);

    void userOffline(const std::string& user_id);

    bool isOnline(const std::string& user_id);

    std::shared_ptr<Connection> getConnection(const std::string& user_id);

    std::vector<std::string>getAllOnlineUsers();
      std::string getUserIdByFd(int fd);
    private:
    struct UserInfo{
        std::string user_id;
        std::string username;
        std::shared_ptr<Connection>conn;
        int64_t login_time;
    };

    std::mutex mutex_;
    std::unordered_map<std::string,UserInfo>users_;
    std::unordered_map<int,std::string>fd_to_user_id_; //fd->user_id的反向查找
  };