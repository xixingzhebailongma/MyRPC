#include "im_server.h"
#include "Connection.h"
#include "EventLoop.h"
#include "Logger.h"
#include "auth.pb.h"
#include "im.pb.h"
#include "message_store.h"
#include "rpc_channel.h"
#include "user_dao.h"
#include <chrono>
#include <functional>
#include <google/protobuf/message.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>
ImServer::ImServer(const std::string &ip, int port,
                   const std::string &server_id,
                   const std::string &route_service,
                   const std::string &etcd_endpoints,
                   const std::string &redis_ip, int redis_port,
                   const DbConfig &db_cfg, const std::string &auth_service,
                   bool auth_enabled)
    : rpc_server_(ip, port), message_store_(server_id),
      route_client_(etcd_endpoints, route_service,
                    std::make_shared<ConsistentHashBalancer>(150)),
      auth_client_(etcd_endpoints, auth_service,
                   std::make_shared<RoundRobinBalancer>()),
      server_id_(server_id), ip_(ip), port_(port), redis_ip_(redis_ip),
      redis_port_(redis_port), auth_service_(auth_service),
      auth_enabled_(auth_enabled) {
  // 注册 4 个 conn-aware handler
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Login",
      std::bind(&ImServer::handleLogin, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "SendMessage",
      std::bind(&ImServer::handleSendMessage, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "GetFriendList",
      std::bind(&ImServer::handleGetFriendList, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "ForwardMessage",
      std::bind(&ImServer::handleForwardMessage, this, std::placeholders::_1,
                std::placeholders::_2));
  //连接Redis
  if (!message_store_.connect(redis_ip_, redis_port_)) {
    throw std::runtime_error("Failed to connect Redis");
  }
  // 连接 MySQL（注册/登录/好友）
  if (!user_dao_.init(db_cfg)) {
    throw std::runtime_error("Failed to connect MySQL");
  }
  // 注入事件发布器：DAO 写库成功后 -> 序列化为 proto -> Redis Pub/Sub 广播
  user_dao_.setPublisher([this](const ::UserChangedEvent &ev) {
    im::UserChangedEvent pev;
    switch (ev.type) {
    case UserEventType::PasswordChanged:
      pev.set_type(im::UserChangedEvent::PASSWORD_CHANGED);
      break;
    case UserEventType::UserBanned:
      pev.set_type(im::UserChangedEvent::USER_BANNED);
      break;
    case UserEventType::UserDeleted:
      pev.set_type(im::UserChangedEvent::USER_DELETED);
      break;
    case UserEventType::FriendRemoved:
      pev.set_type(im::UserChangedEvent::FRIEND_REMOVED);
      break;
    }
    pev.set_user_id(ev.user_id);
    pev.set_target_id(ev.target_id);
    pev.set_reason(ev.reason);
    std::string payload;
    pev.SerializeToString(&payload);
    message_store_.publish("im:user:events", payload);
  });
  //注册ACK确认&离线消息拉取（注意：proto类型是typo版PullOfflien...)
  rpc_server_.serviceManager().registerMethod(
      "ImService", "AckMessage",
      std::bind(&ImServer::handleAckMessage, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "PullOfflineMessages",
      std::bind(&ImServer::handlePullOfflineMessages, this,
                std::placeholders::_1, std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Register",
      std::bind(&ImServer::handleRegister, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "AddFriend",
      std::bind(&ImServer::handleAddFriend, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "ChangePassword",
      std::bind(&ImServer::handleChangePassword, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Connect",
      std::bind(&ImServer::handleConnect, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "IssueTicket",
      std::bind(&ImServer::handleIssueTicket, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Refresh",
      std::bind(&ImServer::handleRefresh, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Logout",
      std::bind(&ImServer::handleLogout, this, std::placeholders::_1,
                std::placeholders::_2));
  //注册 1s 周期定时器，用于消息重试检查（timerfd 定时器，与 IO
  //负载无关，不会被饿死）
  rpc_server_.setPeriodTimer(
      1.0, [this](EventLoop *loop) { this->onRetryCheck(loop); });
  // // 注册到 etcd，service name 用 server_id（如 "IM1", "IM2"）
  rpc_server_.enableRegistry(etcd_endpoints, server_id_, ip_, port_);
  // 连接关闭时：注销 Route 路由 + 标记用户离线
  rpc_server_.setCloseConnectionCallback([this](spConnection conn) {
    std::string user_id;
    bool was_last = user_manager_.userOfflineByFd(conn->fd(), &user_id);
    // 只有该用户在本节点的最后一条会话关闭时才注销 Route，
    // 否则会误伤用户其它仍在线设备的路由。
    if (was_last && !user_id.empty()) {
      unregisterUserFromRoute(user_id);
    }
  });
  // 连接建立：登记到未握手表
  rpc_server_.setNewConnectionCallback(
      [this](spConnection conn) { this->onNewConnection(conn); });
  // 握手超时扫描（独立 1s 定时器；底层 setPeriodicTimer
  // 每次注册独立定时器，安全）
  rpc_server_.setPeriodTimer(
      1.0, [this](EventLoop *loop) { this->sweepHandshakes(loop); });
  // 订阅路由变更事件：上线→预暖缓存，下线→失效缓存
  route_subscriber_.start(
      redis_ip_, redis_port_, "im:route:events",
      [this](const std::string &payload) { this->onRouteChange(payload); });
  // 订阅用户数据变更事件：跨节点踢下线
  user_subscriber_.start(
      redis_ip_, redis_port_, "im:user:events",
      [this](const std::string &payload) { this->onUserChanged(payload); });
}
void ImServer::start() { rpc_server_.start(); }
void ImServer::stop() {
  route_subscriber_.stop();
  user_subscriber_.stop();
  rpc_server_.stop();
}

//面向客户端的RPC接口
std::string ImServer::handleLogin(spConnection conn,
                                  const std::string &request_body) {
  im::LoginRequest req;
  im::LoginResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }

  // 约定：username 即 user_id
  std::string user_id = req.username();
  std::string session_id;

  if (auth_enabled_) {
    // 1. 委托 AuthServer：校验密码 + 签发 access/refresh
    auth::LoginRequest areq;
    areq.set_username(req.username());
    areq.set_password(req.password());
    areq.set_device_id(req.device_id());
    areq.set_device_type(static_cast<auth::DeviceType>(req.device_type()));
    areq.set_client_ip(conn->ip());

    std::string resp_body;
    int32_t err = 0;
    if (!auth_client_.Call("Login", areq.SerializeAsString(), resp_body, err)) {
      resp.set_success(false);
      resp.set_message("auth service unavailable");
      return resp.SerializeAsString();
    }
    auth::LoginResponse aresp;
    if (!aresp.ParseFromString(resp_body) || !aresp.success()) {
      resp.set_success(false);
      resp.set_message(aresp.success() ? "auth response parse error"
                                       : aresp.message());
      return resp.SerializeAsString();
    }
    resp.set_access_token(aresp.access_token());
    resp.set_refresh_token(aresp.refresh_token());
    resp.set_expires_in(aresp.expires_in());
    session_id = aresp.session().session_id();
  } else {
    // 旧路径：本地校验密码（--auth.enabled=false 的灰度回退）
    if (!user_dao_.verifyLogin(req.username(), req.password())) {
      resp.set_success(false);
      resp.set_message("invalid username or password");
      return resp.SerializeAsString();
    }
    resp.set_token(user_id + "_token");
    session_id = "legacy:" + std::to_string(conn->fd());
  }

  // 2. 本地记录上线（多端会话）
  user_manager_.userOnline(session_id, user_id, req.username(), req.device_id(),
                           req.device_type(), conn);
  // 登录成功即视为握手完成，把连接从未握手表移除（否则 10 秒后会被超时误杀）
  completeHandshake(conn->fd());
  // 3. 向 Route Server 注册路由
  bool ok = registerUserOnline(user_id);

  // 4. 用户上线后投递离线消息
  auto offline_msgs = message_store_.fetchOfflineMessages(user_id);
  for (auto &msg : offline_msgs) {
    deliverLocal(msg);
  }
  message_store_.clearOfflineMessages(user_id);

  resp.set_success(ok);
  resp.set_message(ok ? "login ok" : "route register failed");
  return resp.SerializeAsString();
}
std::string ImServer::handleRegister(spConnection conn,
                                     const std::string &request_body) {
  im::RegisterRequest req;
  im::RegisterResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  std::string err;
  bool ok = user_dao_.registerUser(req.username(), req.password(), &err);
  resp.set_success(ok);
  resp.set_message(ok ? "register ok" : err);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

std::string ImServer::handleAddFriend(spConnection conn,
                                      const std::string &request_body) {
  im::AddFriendRequest req;
  im::AddFriendResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份从连接推导，而非客户端自报的 user_id
  std::string uid = user_manager_.getUserIdByFd(conn->fd());
  if (uid.empty()) {
    resp.set_success(false);
    resp.set_message("unauthenticated");
    return resp.SerializeAsString();
  }
  std::string err;
  bool ok = user_dao_.addFriend(uid, req.friend_id(), &err);
  resp.set_success(ok);
  resp.set_message(ok ? "add friend ok" : err);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

std::string ImServer::handleChangePassword(spConnection conn,
                                           const std::string &request_body) {
  im::ChangePasswordRequest req;
  im::ChangePasswordResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：只允许改自己的密码，身份从连接推导
  std::string uid = user_manager_.getUserIdByFd(conn->fd());
  if (uid.empty()) {
    resp.set_success(false);
    resp.set_message("unauthenticated");
    return resp.SerializeAsString();
  }
  std::string err;
  bool ok = user_dao_.updatePassword(uid, req.new_password(), &err);
  resp.set_success(ok);
  resp.set_message(ok ? "password changed" : err);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}
std::string ImServer::handleSendMessage(spConnection conn,
                                        const std::string &request_body) {
  im::SendMessageRequest req;
  im::SendMessageResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：发送者身份从连接推导，强制覆盖客户端自报的 from_user_id
  std::string uid = user_manager_.getUserIdByFd(conn->fd());
  if (uid.empty()) {
    resp.set_success(false);
    resp.set_message("unauthenticated");
    return resp.SerializeAsString();
  }

  //复制为可变对象
  ChatMessage msg = req.msg();
  msg.set_from_user_id(uid); // 客户端自报的 from_user_id 一律忽略

  //真实 msg_id 始终由服务端生成（全局唯一：INCR 单调 + server_id 前缀）
  std::string real_msg_id = message_store_.generateMsgId();
  if (real_msg_id.empty()) {
    resp.set_success(false);
    resp.set_message("server error:failed to generate msg_id");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  msg.set_msg_id(real_msg_id);

  //设置状态和时间戳
  msg.set_status(im::MessageStatus::SENT);
  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

  //幂等去重：按 (发送者, client_request_id) 隔离，不再用全局 msg_id
  const std::string &client_request_id = req.client_request_id();
  if (!client_request_id.empty()) {
    std::string existing;
    if (!message_store_.tryClaimRequest(msg.from_user_id(), client_request_id,
                                        real_msg_id, &existing)) {
      //重复请求：返回之前已分配的 msg_id，保证响应幂等
      resp.set_success(true);
      resp.set_message("duplicate,ignored");
      resp.set_msg_id(existing.empty() ? real_msg_id : existing);
      std::string out;
      resp.SerializeToString(&out);
      return out;
    }
  }

  //路由投递
  bool ok = routeMessage(msg);

  //无论成功失败都返回msg_id
  resp.set_msg_id(msg.msg_id());
  if (ok) {
    // 成功 -> 状态置 SENT + 加入待确认队列（score = 首次重试到期时间）
    message_store_.markStatus(msg.msg_id(), im::MessageStatus::SENT);
    message_store_.addPending(msg, now_ms + RETRY_INTERVAL_MS);
    resp.set_success(true);
    resp.set_message("sent,waiting delivery ack");
  } else {
    // 失败 -> 状态置 SENT 保持一致，再存离线消息
    message_store_.markStatus(msg.msg_id(), im::MessageStatus::SENT);
    message_store_.storeOfflineMessage(msg.to_user_id(), msg);
    resp.set_success(true);
    resp.set_message("store offline");
  }
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

std::string ImServer::handleGetFriendList(spConnection conn,
                                          const std::string &request_body) {
  im::GetFriendListRequest req;
  im::GetFriendListResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份从连接推导
  std::string uid = user_manager_.getUserIdByFd(conn->fd());
  if (uid.empty()) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  resp.set_success(true);
  // 从 DB 查真实好友（username==user_id 约定不变）
  auto friends = user_dao_.getFriendList(uid);
  for (const auto &fid : friends) {
    auto *info = resp.add_friends();
    info->set_user_id(fid);
    info->set_username(fid);
    info->set_is_online(user_manager_.isOnline(fid));
  }
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

//面向其它IM server的接口
// IM2 收到 IM1 转发的消息 → 投递给 B）
std::string ImServer::handleForwardMessage(spConnection conn,
                                           const std::string &request_body) {
  im::ForwardMessageRequest req;
  im::ForwardMessageResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  bool ok = deliverLocal(req.msg());
  resp.set_success(ok);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}
//处理客户端发来的ACK确认
std::string ImServer::handleAckMessage(spConnection conn,
                                       const std::string &request_body) {
  //解析AckMessageRequest
  im::AckMessageRequest req;
  im::AckMessageResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  const MessageAck &ack = req.ack();

  // 1.更新Redis中的消息状态
  message_store_.markStatus(ack.msg_id(), ack.status());

  // 2. 从待确认队列移除（不再需要重试）
  message_store_.removePending(ack.msg_id());

  // 3. ACK 回投：本地优先 → 本地缓存 → 回源兜底
  if (user_manager_.isOnline(ack.to_user_id())) {
    deliverLocal(ack); // 目标在本节点 → 直接推
  } else {
    RouteQueryResponse route;
    if (route_cache_.get(ack.to_user_id(), &route)) {
      forwardAckToRemote(ack, route);
    } else {
      route = queryUserRoute(ack.to_user_id());
      if (route.found()) {
        route_cache_.put(ack.to_user_id(), route.server_id(), route.server_ip(),
                         route.server_port());
        forwardAckToRemote(ack, route);
      }
      // 查不到路由（发送方已下线）→ 丢弃 ACK，状态已落 Redis
    }
  }

  //返回成功
  resp.set_success(true);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

//处理客户端拉取离线消息
std::string
ImServer::handlePullOfflineMessages(spConnection conn,
                                    const std::string &request_body) {
  //解析PullOfflienMessagesRequest（注意：proto 里就是 typo 版 "Offlien"）
  im::PullOfflienMessagesRequest req;
  im::PullOfflienMessagesResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：只能拉自己的离线消息，身份从连接推导
  std::string uid = user_manager_.getUserIdByFd(conn->fd());
  if (uid.empty()) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  //从Redis拉取该用户的离线消息
  auto messages = message_store_.fetchOfflineMessages(uid);

  //拉完之后清除Redis 里的离线消息（已送达客户端）
  message_store_.clearOfflineMessages(uid);

  //填充响应
  resp.set_success(true);
  for (auto &msg : messages) {
    *resp.add_messages() = std::move(msg);
  }
  std::string out;
  resp.SerializeToString(&out);
  return out;
}
//消息路由核心
//消息路由核心：本地在线优先 → 本地缓存命中 → 回源 RouteServer 兜底
bool ImServer::routeMessage(const ChatMessage &msg) {
  if (user_manager_.isOnline(msg.to_user_id())) {
    //目标用户在本节点->直接投递
    return deliverLocal(msg);
  }
  RouteQueryResponse route;
  if (!route_cache_.get(msg.to_user_id(), &route)) {
    //缓存 miss -> 回源权威路由表
    route = queryUserRoute(msg.to_user_id());
    if (!route.found()) {
      return false; //对方不在线
    }
    route_cache_.put(msg.to_user_id(), route.server_id(), route.server_ip(),
                     route.server_port());
  }
  return forwardToRemote(msg, route);
}
//本地投递 → 把 ChatMessage 推给目标连接
bool ImServer::deliverLocal(const ChatMessage &msg) {
  auto conns = user_manager_.getConnections(msg.to_user_id());
  if (conns.empty()) {
    //用户不在线 → 存入离线队列，等用户上线后拉取
    return message_store_.storeOfflineMessage(msg.to_user_id(), msg);
  }
  ServerPushEnvelope envelope;
  envelope.set_type(ServerPushEnvelope::CHAT_MESSAGE);
  msg.SerializeToString(envelope.mutable_payload());
  // 打包成 [4字节LE长度][ChatMessage序列化] 帧
  std::string frame = packFrame(envelope);
  // 多端：每个在线设备都推一份
  for (auto &conn : conns) {
    conn->send(frame.data(), frame.size());
  }
  return true;
}
//重载：推送 ACK 通知给原始发送方
bool ImServer::deliverLocal(const MessageAck &ack) {
  auto conns = user_manager_.getConnections(ack.to_user_id());
  if (conns.empty()) {
    return false; //原始发送发不在线，无法推送
  }
  ServerPushEnvelope envelope;
  envelope.set_type(ServerPushEnvelope::DELIVERY_ACK);
  ack.SerializeToString(envelope.mutable_payload());
  std::string frame = packFrame(envelope);
  for (auto &conn : conns) {
    conn->send(frame.data(), frame.size());
  }
  return true;
}

//通用远程调用：复用连接池+RpcChannel::Call
bool ImServer::callRemote(const RouteQueryResponse &route,
                          const std::string &method,
                          const google::protobuf::Message &req,
                          google::protobuf::Message *resp) {
  auto server_id = route.server_id();
  // 1. 从连接池获取或创建到目标 IM Server 的 RpcChannel（池内部加锁）
  RpcChannel *channel = server_channels_.getOrCreate(
      server_id, route.server_ip(), static_cast<uint16_t>(route.server_port()));
  // 2. 序列化请求并调用
  std::string req_body;
  req.SerializeToString(&req_body);
  std::string resp_body;
  int32_t err = 0;
  bool ok = channel->Call("ImService", method, req_body, resp_body, err);
  if (ok && resp) {
    resp->ParseFromString(resp_body);
  }
  return ok;
}

//转发 ChatMessage（薄封装）
bool ImServer::forwardToRemote(const ChatMessage &msg,
                               const RouteQueryResponse &route) {
  im::ForwardMessageRequest req;
  *req.mutable_msg() = msg;
  im::ForwardMessageResponse resp;
  return callRemote(route, "ForwardMessage", req, &resp);
}
// 转发 ACK 回发送方节点（薄封装，复用已注册的 "AckMessage" 方法）
bool ImServer::forwardAckToRemote(const MessageAck &ack,
                                  const RouteQueryResponse &route) {
  im::AckMessageRequest req;
  *req.mutable_ack() = ack;
  im::AckMessageResponse resp;
  return callRemote(route, "AckMessage", req, &resp);
}
// Route Server交互
RouteQueryResponse ImServer::queryUserRoute(const std::string &user_id) {
  im::RouteQueryRequest req;
  req.set_user_id(user_id);
  std::string req_body = req.SerializeAsString();
  std::string resp_body;
  int error_code = 0;

  bool ok = route_client_.Call("RouteQuery", req_body, resp_body, error_code,
                               user_id);
  if (!ok || error_code != 0)
    return {};
  im::RouteQueryResponse resp;
  resp.ParseFromString(resp_body);
  return resp;
}
bool ImServer::registerUserOnline(const std::string &user_id) {
  im::RouteRegisterRequest req;
  req.set_user_id(user_id);
  req.set_server_id(server_id_);
  req.set_server_ip(ip_);
  req.set_server_port(port_);
  std::string req_body;
  req.SerializeToString(&req_body);

  std::string resp_body;
  int32_t err = 0;
  return route_client_.Call("RouteRegister", req_body, resp_body, err, user_id);
}
bool ImServer::unregisterUserFromRoute(const std::string &user_id) {
  im::RouteUnregisterRequest req;
  req.set_user_id(user_id);
  std::string req_body;
  req.SerializeToString(&req_body);

  std::string resp_body;
  int32_t err = 0;
  return route_client_.Call("RouteUnregister", req_body, resp_body, err,
                            user_id);
}
void ImServer::onRouteChange(const std::string &payload) {
  im::RouteChangeEvent ev;
  if (!ev.ParseFromString(payload)) {
    return;
  }
  if (ev.online()) {
    route_cache_.put(ev.user_id(), ev.server_id(), ev.server_ip(),
                     ev.server_port());
  } else {
    route_cache_.invalidate(ev.user_id());
  }
}

void ImServer::onUserChanged(const std::string &payload) {
  im::UserChangedEvent ev;
  if (!ev.ParseFromString(payload)) {
    return;
  }
  kickOffline(ev);
}
void ImServer::kickOffline(const im::UserChangedEvent &ev) {
  if (!user_manager_.isOnline(ev.user_id())) {
    return; // 不在本节点，忽略（其他节点会各自处理）
  }
  LOG_INFO("kick user offline: %s type=%d", ev.user_id().c_str(), ev.type());

  // 1. 发一条系统通知，客户端收到后提示重新登录
  im::SystemNotice notice;
  switch (ev.type()) {
  case im::UserChangedEvent::PASSWORD_CHANGED:
    notice.set_message("密码已修改，请重新登录");
    break;
  case im::UserChangedEvent::USER_BANNED:
    notice.set_message("账号已被封禁");
    break;
  case im::UserChangedEvent::USER_DELETED:
    notice.set_message("账号已被删除");
    break;
  case im::UserChangedEvent::FRIEND_REMOVED:
    notice.set_message("好友关系已变更");
    break;
  default:
    notice.set_message("账号状态已变更，请重新登录");
    break;
  }
  auto conns = user_manager_.getConnections(ev.user_id());
  if (conns.empty()) {
    return;
  }
  im::ServerPushEnvelope envelope;
  envelope.set_type(im::ServerPushEnvelope::SYSTEM_NOTICE);
  notice.SerializeToString(envelope.mutable_payload());
  std::string frame = packFrame(envelope);
  // 多端：每个在线设备都推一条系统通知，然后全部 forceClose
  for (auto &conn : conns) {
    conn->send(frame.data(), frame.size());
    conn->forceClose();
  }
}
std::string ImServer::packFrame(const google::protobuf::Message &msg) {
  std::string body;
  msg.SerializeToString(&body);
  uint32_t len = body.size();
  std::string frame;
  frame.append(reinterpret_cast<const char *>(&len), 4);
  frame.append(body);
  return frame;
}

void ImServer::onRetryCheck(EventLoop *loop) {
  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  // 拉取所有到期的待确认消息（score <= now_ms）
  auto due = message_store_.fetchDuePending(now_ms);

  for (auto &rec : due) {
    const std::string &id = rec.msg().msg_id();
    if (rec.retry_count() < MAX_RETRY) {
      if (routeMessage(rec.msg())) {
        //重试成功：延期 + 更新 retry_count
        message_store_.updatePending(rec.msg(), rec.retry_count() + 1,
                                     now_ms + RETRY_INTERVAL_MS);
      } else {
        // 重试投递失败（目标下线/网络失败）→ 转离线，不再无限重试
        message_store_.storeOfflineMessage(rec.msg().to_user_id(), rec.msg());
        message_store_.removePending(id);
      }
    } else {
      // 超过最大重试次数 → 标记 FAILED 并移除
      message_store_.markStatus(id, im::MessageStatus::FAILED);
      message_store_.removePending(id);
    }
  }
}

// ---------- 长连接握手管理 ----------
void ImServer::onNewConnection(spConnection conn) {
  std::lock_guard<std::mutex> lock(handshakes_mutex_);
  pending_handshakes_[conn->fd()] = std::make_pair(
      std::weak_ptr<Connection>(conn), std::chrono::steady_clock::now());
}

void ImServer::completeHandshake(int fd) {
  std::lock_guard<std::mutex> lock(handshakes_mutex_);
  pending_handshakes_.erase(fd);
}

void ImServer::sweepHandshakes(EventLoop *loop) {
  (void)loop;
  auto now = std::chrono::steady_clock::now();
  std::vector<std::shared_ptr<Connection>> to_close;
  {
    std::lock_guard<std::mutex> lock(handshakes_mutex_);
    for (auto it = pending_handshakes_.begin();
         it != pending_handshakes_.end();) {
      auto conn = it->second.first.lock();
      if (!conn) {
        it = pending_handshakes_.erase(it); // 连接已死，顺手清掉
        continue;
      }
      if (now - it->second.second >=
          std::chrono::seconds(kHandshakeTimeoutSec)) {
        to_close.push_back(conn);
        it = pending_handshakes_.erase(it);
      } else {
        ++it;
      }
    }
  }
  // 释放锁后再 forceClose（内部 queueinloop，不阻塞）
  for (auto &conn : to_close) {
    conn->forceClose();
  }
}

std::string ImServer::handleConnect(spConnection conn,
                                    const std::string &request_body) {
  im::ConnectRequest req;
  im::ConnectResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }

  // 委托 AuthServer 消费一次性票据
  auth::ResolveTicketRequest areq;
  areq.set_ticket(req.ticket());
  std::string resp_body;
  int32_t err = 0;
  if (!auth_client_.Call("ResolveTicket", areq.SerializeAsString(), resp_body,
                         err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::ResolveTicketResponse aresp;
  if (!aresp.ParseFromString(resp_body) || !aresp.valid()) {
    resp.set_success(false);
    resp.set_message(aresp.valid() ? "auth response parse error"
                                   : aresp.message());
    return resp.SerializeAsString();
  }
  const auth::SessionInfo &s = aresp.session();

  // 绑定会话
  user_manager_.userOnline(s.session_id(), s.user_id(), s.username(),
                           s.device_id(), static_cast<int>(s.device_type()),
                           conn);
  bool ok = registerUserOnline(s.user_id());

  // 投递离线消息
  auto offline = message_store_.fetchOfflineMessages(s.user_id());
  for (auto &m : offline) {
    deliverLocal(m);
  }
  message_store_.clearOfflineMessages(s.user_id());

  completeHandshake(conn->fd());

  resp.set_success(ok);
  resp.set_user_id(s.user_id());
  resp.set_message(ok ? "connect ok" : "route register failed");
  return resp.SerializeAsString();
}

std::string ImServer::handleIssueTicket(spConnection conn,
                                        const std::string &request_body) {
  (void)conn;
  im::IssueTicketRequest req;
  im::IssueTicketResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  auth::IssueTicketRequest areq;
  areq.set_access_token(req.access_token());
  std::string resp_body;
  int32_t err = 0;
  if (!auth_client_.Call("IssueTicket", areq.SerializeAsString(), resp_body,
                         err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::IssueTicketResponse aresp;
  if (!aresp.ParseFromString(resp_body)) {
    resp.set_success(false);
    resp.set_message("auth response parse error");
    return resp.SerializeAsString();
  }
  resp.set_success(aresp.success());
  resp.set_message(aresp.message());
  resp.set_ticket(aresp.ticket());
  return resp.SerializeAsString();
}

std::string ImServer::handleRefresh(spConnection conn,
                                    const std::string &request_body) {
  (void)conn;
  im::RefreshRequest req;
  im::RefreshResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  auth::RefreshRequest areq;
  areq.set_refresh_token(req.refresh_token());
  std::string resp_body;
  int32_t err = 0;
  if (!auth_client_.Call("Refresh", areq.SerializeAsString(), resp_body, err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::RefreshResponse aresp;
  if (!aresp.ParseFromString(resp_body)) {
    resp.set_success(false);
    resp.set_message("auth response parse error");
    return resp.SerializeAsString();
  }
  resp.set_success(aresp.success());
  resp.set_message(aresp.message());
  resp.set_access_token(aresp.access_token());
  resp.set_refresh_token(aresp.refresh_token());
  resp.set_expires_in(aresp.expires_in());
  return resp.SerializeAsString();
}

std::string ImServer::handleLogout(spConnection conn,
                                   const std::string &request_body) {
  im::LogoutRequest req;
  im::LogoutResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  auth::LogoutRequest areq;
  areq.set_access_token(req.access_token());
  std::string resp_body;
  int32_t err = 0;
  // 登出幂等：AuthServer 侧找不到 token 也返回成功
  auth_client_.Call("Logout", areq.SerializeAsString(), resp_body, err);

  // 本地解绑：这条连接从此不再是已认证状态，后续请求会被当作未认证拒绝。
  // 不主动 forceClose —— 让框架正常发回响应，连接留着但已无身份。
  std::string uid;
  bool was_last = user_manager_.userOfflineByFd(conn->fd(), &uid);
  if (was_last && !uid.empty()) {
    unregisterUserFromRoute(uid);
  }
  resp.set_success(true);
  return resp.SerializeAsString();
}