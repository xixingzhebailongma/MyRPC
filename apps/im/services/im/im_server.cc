#include "im_server.h"
#include "Connection.h"
#include "EventLoop.h"
#include "Logger.h"
#include "auth.pb.h"
#include "gateway_sign.h"
#include "idempotency_redis.h"
#include "im.pb.h"
#include "message_store.h"
#include "mq_constants.h"
#include "rpc_channel.h"
#include "rpc_protocol.h"
#include "user_dao.h"
#include <chrono>
#include <ctime>
#include <functional>
#include <google/protobuf/message.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>
ImServer::ImServer(const std::string &ip, int port,
                   const std::string &server_id, uint64_t worker_id,
                   const std::string &route_service,
                   const std::string &etcd_endpoints,
                   const std::string &redis_ip, int redis_port,
                   const DbConfig &db_cfg, const std::string &auth_service,
                   const std::string &shared_secret)
    : rpc_server_(ip, port), message_store_(server_id, worker_id),
      route_client_(etcd_endpoints, route_service,
                    std::make_shared<ConsistentHashBalancer>(150)),
      auth_client_(etcd_endpoints, auth_service,
                   std::make_shared<RoundRobinBalancer>()),
      server_id_(server_id), ip_(ip), port_(port), redis_ip_(redis_ip),
      redis_port_(redis_port), auth_service_(auth_service),
      shared_secret_(shared_secret) {
  // 注册 4 个 conn-aware handler
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Login",
      std::bind(&ImServer::handleLogin, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "SendMessage",
      std::bind(&ImServer::handleSendMessage, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "GetFriendList",
      std::bind(&ImServer::handleGetFriendList, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  //连接Redis
  if (!message_store_.connect(redis_ip_, redis_port_)) {
    throw std::runtime_error("Failed to connect Redis");
  }
  // 连接 Redis（消息队列生产者，投递+重试下沉到 deliver_server）
  if (!producer_.connect(redis_ip_, redis_port_)) {
    throw std::runtime_error("Failed to connect Redis (producer)");
  }
  // 注入共享 Redis 幂等存储：把 request_id 去重从进程内 LRU 换成跨节点共享，
  // 使 LbRpcClient failover 到其它节点时去重仍然生效。
  auto idem_store = std::make_unique<IdempotencyRedis>(server_id_);
  if (!idem_store->connect(redis_ip_, redis_port_)) {
    throw std::runtime_error("Failed to connect Redis (idempotency store)");
  }
  rpc_server_.setIdempotencyStore(std::move(idem_store));
  // 幂等 key 按认证用户隔离：不同用户即使撞 request_id 也不会串扰。
  // 注意：extractor 在 dispatch() 构造 key 时执行（早于 handler 内
  // verifyIdentity）， 依赖「IM
  // 只对网关开放」这一部署边界，见下方信任边界说明。
  rpc_server_.setNamespace("im");
  rpc_server_.setCallerIdExtractor(
      [](const RpcHeader &h) { return h.identity().user_id(); });
  // 连接 MySQL（注册/登录/好友）
  if (!user_dao_.init(db_cfg)) {
    throw std::runtime_error("Failed to connect MySQL");
  }
  // nonce 防重放存储（验签用）：SET NX EX 5
  if (!nonce_redis_.connect(redis_ip_, redis_port_)) {
    throw std::runtime_error("Failed to connect Redis (nonce store)");
  }
  //注册ACK确认&离线消息拉取（注意：proto类型是typo版PullOfflien...)
  // 两参 handler 一律用 lambda 而非 std::bind：bind
  // 表达式可吞掉多余实参，会同时匹配 conn-aware 和 with-context
  // 两个 registerMethod 重载而产生歧义。
  rpc_server_.serviceManager().registerMethod(
      "ImService", "AckMessage",
      [this](spConnection conn, const std::string &body) {
        return handleAckMessage(conn, body);
      });
  rpc_server_.serviceManager().registerMethod(
      "ImService", "PullOfflineMessages",
      std::bind(&ImServer::handlePullOfflineMessages, this,
                std::placeholders::_1, std::placeholders::_2,
                std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Register",
      [this](spConnection conn, const std::string &body) {
        return handleRegister(conn, body);
      });
  rpc_server_.serviceManager().registerMethod(
      "ImService", "AddFriend",
      std::bind(&ImServer::handleAddFriend, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "ChangePassword",
      std::bind(&ImServer::handleChangePassword, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Connect",
      std::bind(&ImServer::handleConnect, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "IssueTicket",
      [this](spConnection conn, const std::string &body) {
        return handleIssueTicket(conn, body);
      });
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Refresh",
      [this](spConnection conn, const std::string &body) {
        return handleRefresh(conn, body);
      });
  rpc_server_.serviceManager().registerMethod(
      "ImService", "Logout",
      std::bind(&ImServer::handleLogout, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "ListSessions",
      std::bind(&ImServer::handleListSessions, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "KickSession",
      std::bind(&ImServer::handleKickSession, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "KickAllSessions",
      std::bind(&ImServer::handleKickAllSessions, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3));
  rpc_server_.serviceManager().registerMethod(
      "ImService", "ClientDisconnect",
      [this](spConnection conn, const std::string &body) {
        return handleClientDisconnect(conn, body);
      });
  // 注册到 etcd：所有 IM 节点共享 "ImService" 服务名，供 Gateway 发现
  rpc_server_.enableRegistry(etcd_endpoints, "ImService", ip_, port_);
  // 连接断开 / 握手超时由 Gateway 负责，通过 ClientDisconnect 通知本节点清理。
  // 订阅路由变更事件：上线→预暖缓存，下线→失效缓存（Stream 广播，断线不丢）
  route_subscriber_.start(
      redis_ip_, redis_port_, immq::kRouteEventsStream,
      "im:route:events:im:" + ip_ + ":" + std::to_string(port_),
      [this](const std::string &payload) { this->onRouteChange(payload); });
}

void ImServer::start() { rpc_server_.start(); }

void ImServer::stop() {

  route_subscriber_.stop();
  rpc_server_.stop();
}

//面向客户端的RPC接口
std::string ImServer::handleLogin(spConnection conn,
                                  const std::string &request_body,
                                  const RpcHeader &hdr) {
  im::LoginRequest req;
  im::LoginResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }

  // 委托 AuthServer：校验密码 + 签发 access/refresh
  auth::LoginRequest areq;
  areq.set_username(req.username());
  areq.set_password(req.password());
  areq.set_device_id(req.device_id());
  areq.set_device_type(static_cast<auth::DeviceType>(req.device_type()));
  areq.set_client_ip(hdr.client_ip());

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

  resp.set_success(true);
  resp.set_message("login ok");
  return resp.SerializeAsString();
}
std::string ImServer::handleRegister(spConnection conn,
                                     const std::string &request_body) {
  (void)conn;
  im::RegisterRequest req;
  im::RegisterResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }

  // 委托 AuthServer：注册收口到身份 owner，和 Login/Refresh/Logout 对称
  auth::RegisterRequest areq;
  areq.set_username(req.username());
  areq.set_password(req.password());
  std::string resp_body;
  int32_t err = 0;
  if (!auth_client_.Call("Register", areq.SerializeAsString(), resp_body,
                         err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::RegisterResponse aresp;
  if (!aresp.ParseFromString(resp_body)) {
    resp.set_success(false);
    resp.set_message("auth response parse error");
    return resp.SerializeAsString();
  }
  resp.set_success(aresp.success());
  resp.set_message(aresp.message());
  return resp.SerializeAsString();
}

std::string ImServer::handleAddFriend(spConnection conn,
                                      const std::string &request_body,
                                      const RpcHeader &hdr) {

  im::AddFriendRequest req;
  im::AddFriendResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份由 Gateway 注入并签名
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  std::string uid = hdr.identity().user_id();
  std::string err;
  bool ok = user_dao_.addFriend(uid, req.friend_id(), &err);
  resp.set_success(ok);
  resp.set_message(ok ? "add friend ok" : err);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

std::string ImServer::handleChangePassword(spConnection conn,
                                           const std::string &request_body,
                                           const RpcHeader &hdr) {
  im::ChangePasswordRequest req;
  im::ChangePasswordResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份由 Gateway 注入并签名
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  std::string uid = hdr.identity().user_id();
  std::string err;
  bool ok = user_dao_.updatePassword(uid, req.new_password(), &err);
  if (ok) {
    // 改密成功后：吊销该用户全部会话（旧 token 失效）+ 踢下线。
    // 直连两步替代旧广播链：Auth 吊销会话 + 本节点踢连接。
    auth::KickAllSessionsRequest areq;
    areq.set_user_id(uid);
    std::string aresp_body;
    int32_t aerr = 0;
    auth_client_.Call("KickAllSessions", areq.SerializeAsString(), aresp_body,
                      aerr);
    kickUserOffline(uid, "密码已修改，请重新登录");
  }
  resp.set_success(ok);
  resp.set_message(ok ? "password changed" : err);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

std::string ImServer::handleSendMessage(spConnection conn,
                                        const std::string &request_body,
                                        const RpcHeader &hdr) {

  im::SendMessageRequest req;
  im::SendMessageResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份由 Gateway 注入并签名
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  std::string uid = hdr.identity().user_id();

  //复制为可变对象
  ChatMessage msg = req.msg();
  msg.set_from_user_id(uid); // 客户端自报的 from_user_id 一律忽略

  //真实 msg_id 始终由服务端生成（Snowflake，全局唯一）
  std::string real_msg_id = message_store_.generateMsgId();
  if (real_msg_id.empty()) {
    resp.set_success(false);
    resp.set_message("server error: snowflake unavailable (clock rollback)");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  msg.set_msg_id(real_msg_id);
  msg.set_status(im::MessageStatus::SENT);
  msg.set_trace_id(hdr.trace_id()); // 随消息经 Redis Stream 透传到 deliver

  //幂等去重：按 (发送者, client_request_id) 隔离
  const std::string &client_request_id = req.client_request_id();
  if (!client_request_id.empty()) {
    std::string existing;
    MessageStore::ClaimResult cr = message_store_.tryClaimRequest(
        msg.from_user_id(), client_request_id, real_msg_id, &existing);
    if (cr == MessageStore::ClaimResult::kDuplicate) {
      resp.set_success(true);
      resp.set_message("duplicate,ignored");
      resp.set_msg_id(existing.empty() ? real_msg_id : existing);
      std::string out;
      resp.SerializeToString(&out);
      return out;
    }
    if (cr == MessageStore::ClaimResult::kError) {
      // Redis 不可用：不去重、不入队，显式返回失败，客户端可稍后重试
      resp.set_success(false);
      resp.set_message("dedup store unavailable");
      std::string out;
      resp.SerializeToString(&out);
      return out;
    }
    // kFirst：继续入队
  }

  // 投递+重试下沉到 deliver_server：这里只「快入队、快返回」
  std::string payload;
  msg.SerializeToString(&payload);
  std::string entry_id;
  if (!producer_.produce(immq::kDeliveryStream, payload, &entry_id)) {
    // 入队失败 = 消息真正丢失：回滚去重键，允许客户端重试重投
    if (!client_request_id.empty()) {
      message_store_.releaseRequestClaim(uid, client_request_id);
      LOG_ERROR("handleSendMessage: enqueue failed, released claim for user=%s "
                "trace_id=%s",
                uid.c_str(),
                hdr.trace_id().empty() ? "-" : hdr.trace_id().c_str());
    }
    resp.set_success(false);
    resp.set_message("enqueue failed");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  message_store_.markStatus(msg.msg_id(), im::MessageStatus::SENT);

  resp.set_msg_id(msg.msg_id());
  resp.set_success(true);
  resp.set_message("sent");
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

std::string ImServer::handleGetFriendList(spConnection conn,
                                          const std::string &request_body,
                                          const RpcHeader &hdr) {
  im::GetFriendListRequest req;
  im::GetFriendListResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份由 Gateway 注入并签名
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  std::string uid = hdr.identity().user_id();
  resp.set_success(true);
  // 从 DB 查真实好友（username==user_id 约定不变）
  auto friends = user_dao_.getFriendList(uid);
  for (const auto &fid : friends) {
    auto *info = resp.add_friends();
    info->set_user_id(fid);
    info->set_username(fid);
    // 在线语义改为「连接在线」：Route 表里有该 user 的 field 即在线，
    // 不再依赖本地 UserManager（跨节点好友之前会误判离线）。
    info->set_is_online(!resolveRoutes(fid).empty());
  }
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
  const std::string &msg_id = ack.msg_id();

  // 节点无关的 ACK：只写全局状态键 msg:status:{msg_id}，不再跨节点删 pending。
  // 发送节点的 onRetryCheck 会在重试前查到 DELIVERED/READ
  // 并自行清理自己的队列， 因此这里无需（也不能）从 msg_id 反解发送节点。
  message_store_.markStatus(msg_id, ack.status());

  // ACK 回投：按路由表扇出到原始发送方所有在线连接（Gateway 回推）
  deliverLocal(ack);

  //返回成功
  resp.set_success(true);
  std::string out;
  resp.SerializeToString(&out);
  return out;
}

//处理客户端拉取离线消息
std::string ImServer::handlePullOfflineMessages(spConnection conn,
                                                const std::string &request_body,
                                                const RpcHeader &hdr) {
  //解析PullOfflienMessagesRequest（注意：proto 里就是 typo 版 "Offlien"）
  im::PullOfflienMessagesRequest req;
  im::PullOfflienMessagesResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }
  // 鉴权：身份由 Gateway 注入并签名
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }

  std::string uid = hdr.identity().user_id();
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

//本地投递 → 把 ChatMessage 推给目标用户所有在线连接（经 Gateway 回推）
bool ImServer::deliverLocal(const ChatMessage &msg, bool store_on_miss) {
  ServerPushEnvelope envelope;
  envelope.set_type(ServerPushEnvelope::CHAT_MESSAGE);
  msg.SerializeToString(envelope.mutable_payload());
  // 打包成 [4字节BE长度][ServerPushEnvelope序列化] 帧
  std::string frame = packFrame(envelope);

  std::vector<RouteServer> servers = resolveRoutes(msg.to_user_id());
  bool delivered = false;
  for (const auto &s : servers) {
    delivered = pushToGateway(s, frame) || delivered;
  }
  // 离线是否落库由调用方决策（store_on_miss）
  if (!delivered && store_on_miss) {
    message_store_.storeOfflineMessage(msg.to_user_id(), msg);
  }
  return delivered;
}
//重载：推送 ACK 通知给原始发送方（经 Gateway 回推）
bool ImServer::deliverLocal(const MessageAck &ack) {
  ServerPushEnvelope envelope;
  envelope.set_type(ServerPushEnvelope::DELIVERY_ACK);
  ack.SerializeToString(envelope.mutable_payload());
  std::string frame = packFrame(envelope);
  std::vector<RouteServer> servers = resolveRoutes(ack.to_user_id());
  bool delivered = false;
  for (const auto &s : servers) {
    delivered = pushToGateway(s, frame) || delivered;
  }
  return delivered;
}

// 回推到某个 Gateway 上的连接：把已打包的帧发过去，由 Gateway 写回对应 conn
bool ImServer::pushToGateway(const RouteServer &server,
                             const std::string &frame, bool force_close) {
  // 从连接池获取或创建到目标 Gateway 的 RpcChannel（池内部加锁）
  auto ch =
      server_channels_.getOrCreate(server.server_id(), server.server_ip(),
                                   static_cast<uint16_t>(server.server_port()));
  im::GatewayPushRequest req;
  req.set_conn_id(server.conn_id());
  req.set_frame(frame);
  req.set_force_close(force_close);
  std::string req_body = req.SerializeAsString();
  std::string resp_body;
  int32_t err = 0;
  return ch->Call("GatewayService", "Push", req_body, resp_body, err);
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
std::vector<RouteServer> ImServer::resolveRoutes(const std::string &user_id) {
  std::vector<RouteServer> servers;
  if (!route_cache_.get(user_id, &servers)) {
    RouteQueryResponse resp = queryUserRoute(user_id);
    for (const auto &s : resp.servers())
      servers.push_back(s);
    if (resp.found())
      route_cache_.put(user_id, servers);
  }
  return servers;
}
bool ImServer::registerUserOnline(const std::string &user_id,
                                  const ClientConnRef &ref,
                                  const std::string &session_id) {
  im::RouteRegisterRequest req;
  req.set_user_id(user_id);
  req.set_server_id(ref.gateway_id); // 现在注册的是 Gateway 位置
  req.set_server_ip(ref.gateway_rpc_ip);
  req.set_server_port(ref.gateway_rpc_port);
  req.set_conn_id(ref.conn_id);
  req.set_session_id(session_id);
  std::string req_body;
  req.SerializeToString(&req_body);

  std::string resp_body;
  int32_t err = 0;
  return route_client_.Call("RouteRegister", req_body, resp_body, err, user_id);
}
bool ImServer::unregisterUserFromRouteByConn(const std::string &gateway_id,
                                             uint64_t conn_id) {
  im::RouteUnregisterByConnRequest req;
  req.set_gateway_id(gateway_id);
  req.set_conn_id(conn_id);
  std::string req_body;
  req.SerializeToString(&req_body);

  std::string resp_body;
  int32_t err = 0;
  // 无 user_id 可用，按连接定位反查，节点无关
  return route_client_.Call("RouteUnregisterByConn", req_body, resp_body, err);
}
void ImServer::onRouteChange(const std::string &payload) {
  im::RouteChangeEvent ev;
  if (!ev.ParseFromString(payload)) {
    return;
  }
  if (ev.online()) {
    route_cache_.add(ev.user_id(), ev.server());
  } else {
    route_cache_.remove(ev.user_id(), ev.server().server_id(),
                        ev.server().conn_id());
  }
}

// 踢某用户全部在线连接（直连，不再广播）：resolveRoutes → 逐连接 push(force_close) + 注销路由
// 异步执行：避免在 handler 里同步 force_close 当前连接，导致响应来不及回传。
void ImServer::kickUserOffline(const std::string &uid,
                               const std::string &notice_text) {
  rpc_server_.submitTask([this, uid, notice_text]() {
    im::ServerPushEnvelope envelope;
    envelope.set_type(im::ServerPushEnvelope::SYSTEM_NOTICE);
    im::SystemNotice notice;
    notice.set_message(notice_text);
    notice.SerializeToString(envelope.mutable_payload());
    std::string frame = packFrame(envelope);
    for (const auto &s : resolveRoutes(uid)) {
      pushToGateway(s, frame, /*force_close=*/true);
      unregisterRoute(uid, s);
    }
  });
}

// 单会话踢：按 session_id 匹配要踢的那条连接（同样异步）
void ImServer::kickSessionOffline(const std::string &uid,
                                  const std::string &session_id,
                                  const std::string &notice_text) {
  rpc_server_.submitTask([this, uid, session_id, notice_text]() {
    im::ServerPushEnvelope envelope;
    envelope.set_type(im::ServerPushEnvelope::SYSTEM_NOTICE);
    im::SystemNotice notice;
    notice.set_message(notice_text);
    notice.SerializeToString(envelope.mutable_payload());
    std::string frame = packFrame(envelope);
    for (const auto &s : resolveRoutes(uid)) {
      if (s.session_id() == session_id) {
        pushToGateway(s, frame, /*force_close=*/true);
        unregisterRoute(uid, s);
      }
    }
  });
}

// 注销某连接的路由（RouteServer 版，直连踢人用）
bool ImServer::unregisterRoute(const std::string &user_id,
                               const RouteServer &server) {
  im::RouteUnregisterRequest req;
  req.set_user_id(user_id);
  req.set_server_id(server.server_id()); // gateway_id
  req.set_conn_id(server.conn_id());
  std::string req_body;
  req.SerializeToString(&req_body);
  std::string resp_body;
  int32_t err = 0;
  return route_client_.Call("RouteUnregister", req_body, resp_body, err,
                            user_id);
}

std::string ImServer::packFrame(const google::protobuf::Message &msg) {
  std::string body;
  msg.SerializeToString(&body);
  uint32_t len = body.size();
  std::string frame;
  char lb[kHeaderLen];
  writeLenBE(lb, len); // 大端长度前缀
  frame.append(lb, kHeaderLen);
  frame.append(body);
  return frame;
}

bool ImServer::verifyIdentity(const RpcHeader &hdr, std::string *err) {
  if (!hdr.has_identity() || hdr.signature().empty()) {
    if (err)
      *err = "missing identity";
    return false;
  }
  // 1. 时间戳新鲜度（±5s，秒级）
  int64_t now = static_cast<int64_t>(std::time(nullptr));
  int64_t ts = hdr.timestamp();
  if (ts < now - 5 || ts > now + 5) {
    return false;
  }
  // 2. nonce 防重放：SET NX EX 5，key 已存在 = 重放
  SetResult sr = nonce_redis_.setNxEx("sig:nonce:" + hdr.nonce(), "1", 5);
  if (sr == SetResult::kExists) {
    if (err)
      *err = "nonce replay";
    return false;
  }
  if (sr == SetResult::kError) {
    // Redis 不可用：nonce 防重放降级为跳过，但 HMAC 验签仍强制执行
    LOG_WARN("verifyIdentity: nonce store unavailable, skip replay check");
  }
  // 3. HMAC 验签
  if (!gateway_sign::verify(shared_secret_, hdr)) {
    if (err)
      *err = "signature mismatch";
    return false;
  }
  return true;
}

std::string ImServer::handleConnect(spConnection conn,
                                    const std::string &request_body,
                                    const RpcHeader &hdr) {
  (void)conn;
  im::ConnectRequest req;
  im::ConnectResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }

  // 身份由 Gateway 注入并签名：验签后直接信任，不再调 Auth ResolveTicket
  std::string err;
  if (!verifyIdentity(hdr, &err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + err);
    return resp.SerializeAsString();
  }
  const Identity &id = hdr.identity();
  // 连接引用 = 客户端所在的 Gateway 连接（无状态，仅用于注册路由）
  ClientConnRef ref;
  ref.gateway_id = hdr.gateway_id();
  ref.conn_id = hdr.conn_id();
  ref.gateway_rpc_ip = hdr.gateway_rpc_ip();
  ref.gateway_rpc_port = hdr.gateway_rpc_port();
  bool ok = registerUserOnline(id.user_id(), ref, id.session_id());

  // 投递离线消息
  auto offline = message_store_.fetchOfflineMessages(id.user_id());
  for (auto &m : offline) {
    deliverLocal(m);
  }
  message_store_.clearOfflineMessages(id.user_id());

  resp.set_success(ok);
  resp.set_user_id(id.user_id());
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
                                   const std::string &request_body,
                                   const RpcHeader &hdr) {
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
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  // 登出幂等：AuthServer 侧找不到 token 也返回成功
  auth_client_.Call("Logout", areq.SerializeAsString(), resp_body, err);

  // 直接注销这条连接的路由（反查索引，节点无关）
  unregisterUserFromRouteByConn(hdr.gateway_id(), hdr.conn_id());
  resp.set_success(true);
  return resp.SerializeAsString();
}

std::string ImServer::handleListSessions(spConnection conn,
                                         const std::string &request_body,
                                         const RpcHeader &hdr) {
  (void)conn;
  im::ListSessionsRequest req;
  im::ListSessionsResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  int32_t err = 0;
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  // 关键：user_id 从验签后的 Identity 派生，不信任请求体，堵住越权洞
  const std::string &uid = hdr.identity().user_id();

  auth::ListSessionsRequest areq;
  areq.set_user_id(uid);
  std::string resp_body;
  if (!auth_client_.Call("ListSessions", areq.SerializeAsString(), resp_body,
                         err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::ListSessionsResponse aresp;
  if (!aresp.ParseFromString(resp_body)) {
    resp.set_success(false);
    resp.set_message("auth response parse error");
    return resp.SerializeAsString();
  }
  resp.set_success(aresp.success());
  resp.set_message(aresp.message());
  for (const auto &s : aresp.sessions()) {
    im::SessionInfo *info = resp.add_sessions();
    info->set_session_id(s.session_id());
    info->set_user_id(s.user_id());
    info->set_username(s.username());
    info->set_device_id(s.device_id());
    info->set_device_type(static_cast<int32_t>(s.device_type()));
    info->set_client_ip(s.client_ip());
    info->set_created_at(s.created_at());
    info->set_refresh_expires_at(s.refresh_expires_at());
  }
  return resp.SerializeAsString();
}

std::string ImServer::handleKickSession(spConnection conn,
                                        const std::string &request_body,
                                        const RpcHeader &hdr) {
  (void)conn;
  im::KickSessionRequest req;
  im::KickSessionResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  int32_t err = 0;
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  const std::string &uid = hdr.identity().user_id();

  auth::KickSessionRequest areq;
  areq.set_user_id(uid);
  areq.set_session_id(req.session_id());
  std::string resp_body;
  if (!auth_client_.Call("KickSession", areq.SerializeAsString(), resp_body,
                         err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::KickSessionResponse aresp;
  if (!aresp.ParseFromString(resp_body)) {
    resp.set_success(false);
    resp.set_message("auth response parse error");
    return resp.SerializeAsString();
  }
  resp.set_success(aresp.success());
  resp.set_message(aresp.message());
  // 凭据吊销成功后才直连踢该会话
  if (aresp.success()) {
    kickSessionOffline(uid, req.session_id(), "该设备已被踢下线");
  }
  return resp.SerializeAsString();
}

std::string ImServer::handleKickAllSessions(spConnection conn,
                                            const std::string &request_body,
                                            const RpcHeader &hdr) {
  (void)conn;
  im::KickAllSessionsRequest req;
  im::KickAllSessionsResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    resp.set_message("parse error");
    return resp.SerializeAsString();
  }
  int32_t err = 0;
  std::string auth_err;
  if (!verifyIdentity(hdr, &auth_err)) {
    resp.set_success(false);
    resp.set_message(std::string("auth failed: ") + auth_err);
    return resp.SerializeAsString();
  }
  const std::string &uid = hdr.identity().user_id();

  auth::KickAllSessionsRequest areq;
  areq.set_user_id(uid);
  std::string resp_body;
  if (!auth_client_.Call("KickAllSessions", areq.SerializeAsString(), resp_body,
                         err)) {
    resp.set_success(false);
    resp.set_message("auth service unavailable");
    return resp.SerializeAsString();
  }
  auth::KickAllSessionsResponse aresp;
  if (!aresp.ParseFromString(resp_body)) {
    resp.set_success(false);
    resp.set_message("auth response parse error");
    return resp.SerializeAsString();
  }
  resp.set_success(aresp.success());
  resp.set_kicked(aresp.kicked());
  resp.set_message(aresp.message());
  if (aresp.success()) {
    kickUserOffline(uid, "账号已从所有设备下线，请重新登录");
  }
  return resp.SerializeAsString();
}

// Gateway -> IM：客户端连接断开，注销路由（走 RouteServer 反查索引，节点无关）。
std::string ImServer::handleClientDisconnect(spConnection conn,
                                             const std::string &request_body) {
  (void)conn;
  im::ClientDisconnectRequest req;
  im::ClientDisconnectResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  // 路由清理：按 (gateway_id, conn_id) 反查 user 后删除，任意节点可处理
  unregisterUserFromRouteByConn(req.gateway_id(), req.conn_id());
  resp.set_success(true);
  return resp.SerializeAsString();
}