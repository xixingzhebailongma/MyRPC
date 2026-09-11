#include "auth_server.h"
#include "Logger.h"
#include "auth.pb.h"
#include "im.pb.h"
#include "mq_constants.h"
#include "user_dao.h"
#include <functional>
#include <stdexcept>

AuthServer::AuthServer(const std::string &ip, uint16_t port,
                       const std::string &redis_ip, int redis_port,
                       const std::string &etcd_endpoints,
                       const std::string &service_name, const DbConfig &db_cfg,
                       int thread_num)
    : rpc_server_(ip, port, thread_num), ip_(ip), port_(port) {
  // 1.连接Redis(会话存储)
  if (!session_store_.connect(redis_ip, redis_port)) {
    throw std::runtime_error("Failed to connect Redis");
  }
  // 2.连接MySQL(密码校验)
  if (!user_dao_.init(db_cfg)) {
    throw std::runtime_error("Failed to connect MySQL");
  }
  // 3.注册4个非conn-aware handler
  rpc_server_.serviceManager().registerMethod(
      service_name, "Login",
      std::bind(&AuthServer::handleLogin, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "Register",
      std::bind(&AuthServer::handleRegister, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "VerifyToken",
      std::bind(&AuthServer::handleVerifyToken, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "Refresh",
      std::bind(&AuthServer::handleRefresh, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "Logout",
      std::bind(&AuthServer::handleLogout, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "IssueTicket",
      std::bind(&AuthServer::handleIssueTicket, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "ResolveTicket",
      std::bind(&AuthServer::handleResolveTicket, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "ListSessions",
      std::bind(&AuthServer::handleListSessions, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "KickSession",
      std::bind(&AuthServer::handleKickSession, this, std::placeholders::_1));
  rpc_server_.serviceManager().registerMethod(
      service_name, "KickAllSessions",
      std::bind(&AuthServer::handleKickAllSessions, this,
                std::placeholders::_1));
  // 4. 注册到 etcd
  rpc_server_.enableRegistry(etcd_endpoints, service_name, ip, port, 30);
  // 5. 订阅用户数据变更事件：改密码/封禁/删号 → 吊销该用户全部会话（Stream 广播）
  user_subscriber_.start(
      redis_ip, redis_port, immq::kUserEventsStream,
      "im:user:events:auth:" + ip_ + ":" + std::to_string(port_),
      [this](const std::string &payload) { this->onUserChanged(payload); });
}

void AuthServer::start() { rpc_server_.start(); }
void AuthServer::stop() {
  user_subscriber_.stop();
  rpc_server_.stop();
}

std::string AuthServer::handleLogin(const std::string &request_body) {
  auth::LoginRequest req;
  auth::LoginResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  // 1. 密码校验（SHA2+盐 重算比对，DB 侧）
  if (!user_dao_.verifyLogin(req.username(), req.password())) {
    resp.set_success(false);
    resp.set_message("invalid username or password");
    return resp.SerializeAsString();
  }
  // 2. 创建会话（签发 access/refresh，同端互踢）
  if (!session_store_.createSession(req, &resp)) {
    resp.set_success(false);
    resp.set_message("create session failed");
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleRegister(const std::string &request_body) {
  auth::RegisterRequest req;
  auth::RegisterResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  std::string err;
  if (user_dao_.registerUser(req.username(), req.password(), &err)) {
    resp.set_success(true);
    resp.set_message("register ok");
  } else {
    resp.set_success(false);
    resp.set_message(err.empty() ? "register failed" : err);
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleVerifyToken(const std::string &request_body) {
  auth::VerifyTokenRequest req;
  auth::VerifyTokenResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_valid(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  auth::SessionInfo session;
  if (session_store_.verifyAccessToken(req.token(), &session)) {
    resp.set_valid(true);
    *resp.mutable_session() = std::move(session);
  } else {
    resp.set_valid(false);
    resp.set_message("invalid token");
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleRefresh(const std::string &request_body) {
  auth::RefreshRequest req;
  auth::RefreshResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  if (!session_store_.refresh(req.refresh_token(), &resp)) {
    resp.set_success(false);
    resp.set_message("refresh failed");
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleLogout(const std::string &request_body) {
  auth::LogoutRequest req;
  auth::LogoutResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  // logout 幂等：内部找不到 token 也返回成功
  session_store_.logout(req.access_token());
  resp.set_success(true);
  return resp.SerializeAsString();
}

std::string AuthServer::handleIssueTicket(const std::string &request_body) {
  auth::IssueTicketRequest req;
  auth::IssueTicketResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  std::string tk;
  if (session_store_.issueTicket(req.access_token(), &tk)) {
    resp.set_success(true);
    resp.set_ticket(tk);
  } else {
    resp.set_success(false);
    resp.set_message("issue ticket failed");
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleResolveTicket(const std::string &request_body) {
  auth::ResolveTicketRequest req;
  auth::ResolveTicketResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_valid(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  auth::SessionInfo session;
  if (session_store_.resolveTicket(req.ticket(), req.gateway_id(),
                                   req.conn_id(), &session)) {
    resp.set_valid(true);
    *resp.mutable_session() = std::move(session);
  } else {
    resp.set_valid(false);
    resp.set_message("invalid ticket");
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleListSessions(const std::string &request_body) {
  auth::ListSessionsRequest req;
  auth::ListSessionsResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  auto sessions = session_store_.listSessions(req.user_id());
  resp.set_success(true);
  for (auto &s : sessions) {
    *resp.add_sessions() = std::move(s);
  }
  return resp.SerializeAsString();
}

std::string AuthServer::handleKickSession(const std::string &request_body) {
  auth::KickSessionRequest req;
  auth::KickSessionResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  bool ok = session_store_.kickSession(req.user_id(), req.session_id());
  resp.set_success(ok);
  resp.set_message(ok ? "session kicked" : "session not found");
  return resp.SerializeAsString();
}

std::string AuthServer::handleKickAllSessions(const std::string &request_body) {
  auth::KickAllSessionsRequest req;
  auth::KickAllSessionsResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  int kicked = session_store_.revokeAllSessions(req.user_id());
  resp.set_success(true);
  resp.set_kicked(kicked);
  return resp.SerializeAsString();
}

void AuthServer::onUserChanged(const std::string &payload) {
  im::UserChangedEvent ev;
  if (!ev.ParseFromString(payload)) {
    return;
  }

  // 只有这些事件会导致会话失效；FRIEND_REMOVED 等不吊销会话
  switch (ev.type()) {
  case im::UserChangedEvent::PASSWORD_CHANGED:
  case im::UserChangedEvent::USER_BANNED:
  case im::UserChangedEvent::USER_DELETED:
    break;
  default:
    return;
  }
  int kicked = session_store_.revokeAllSessions(ev.user_id());
  if (kicked > 0) {
    LOG_INFO("AuthServer: revoked %d sessions for user %s (type=%d)", kicked,
             ev.user_id().c_str(), ev.type());
  }
}
