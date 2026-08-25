#include "user_manager.h"
#include <mutex>


void UserManager::userOnline(const std::string &user_id, const std::string &username,
                std::shared_ptr<Connection> conn) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto now = std::chrono::system_clock::now();
  int64_t login_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now.time_since_epoch())
                           .count();
  users_[user_id] = UserInfo{user_id, username, conn, login_time};
  fd_to_user_id_[conn->fd()] = user_id;
}

void UserManager::userOffline(const std::string&user_id){
    std::lock_guard<std::mutex>lock(mutex_);
    //先从users_中找到该用户的连接，拿到fd并清理反向映射
    auto it = users_.find(user_id);
    if(it!=users_.end()){
        fd_to_user_id_.erase(it->second.conn->fd());
        users_.erase(user_id);
    }
    
}
std::string UserManager::getUserIdByFd(int fd){
    std::lock_guard<std::mutex>lock(mutex_);
    auto it = fd_to_user_id_.find(fd);
    if(it!=fd_to_user_id_.end()){
        return it->second;
    }
    return "";
}
bool UserManager::isOnline(const std::string& user_id){
    std::lock_guard<std::mutex>lock(mutex_);
    return users_.find(user_id)!=users_.end();
}

std::shared_ptr<Connection>UserManager::getConnection(const std::string& user_id){
    std::lock_guard<std::mutex>lock(mutex_);
    auto it = users_.find(user_id);
    if(it!=users_.end()){
        return it->second.conn;
    }
    return nullptr;
}

std::vector<std::string>UserManager::getAllOnlineUsers(){
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string>result;
    result.reserve(users_.size());
    for(const auto& pair:users_){
        result.push_back(pair.first);
    }
    return result;
}