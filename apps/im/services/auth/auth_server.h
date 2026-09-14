#pragma once

#include "rpc_server.h"
#include "session_store.h"
#include "user_dao.h" // DbConfig 定义在这里
#include <cstdint>
#include <string>

// 独立认证进程：注册 AuthService 到 etcd，无状态（状态全在 Redis），
// 可横向部署多个实例，由 LbRpcClient 跨实例 failover。
class AuthServer {
public:
  // db_cfg：MySQL 连接配置，用于密码校验（verifyLogin）。
  AuthServer(const std::string &ip, uint16_t port, const std::string &redis_ip,
             int redis_port, const std::string &etcd_endpoints,
             const std::string &service_name, const DbConfig &db_cfg,
             int thread_num = 4);
  void start();
  void stop();

private:
  // 非 conn-aware handler（签名：std::string(const std::string&)）
  std::string handleLogin(const std::string &request_body);
  std::string handleRegister(const std::string &request_body);
  std::string handleVerifyToken(const std::string &request_body);
  std::string handleRefresh(const std::string &request_body);
  std::string handleLogout(const std::string &request_body);
  std::string handleIssueTicket(const std::string &request_body);
  std::string handleResolveTicket(const std::string &request_body);
  std::string handleListSessions(const std::string &request_body);
  std::string handleKickSession(const std::string &request_body);
  std::string handleKickAllSessions(const std::string &request_body);

  RpcServer rpc_server_;
  SessionStore session_store_;
  UserDao user_dao_;

  // 节点身份，用于构建唯一且跨重启稳定的消费组名
  std::string ip_;
  int port_;
};
