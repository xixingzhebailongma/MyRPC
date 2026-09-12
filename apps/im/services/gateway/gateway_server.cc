#include "gateway_server.h"
#include "Logger.h"
#include "auth.pb.h"
#include "gateway_sign.h"
#include "im.pb.h"
#include "rpc_protocol.h"
#include <chrono>
#include <cstdint>
#include <ctime>
#include <string>
#include <thread>
#include <unordered_set>

GatewayServer::GatewayServer(const std::string &client_ip, uint16_t client_port,
                             const std::string &rpc_ip, uint16_t rpc_port,
                             const std::string &gateway_id,
                             const std::string &etcd_endpoints,
                             const std::string &im_service,
                             const std::string &auth_service,
                             const std::string &shared_secret)
    : client_server_(client_ip, client_port, 4), work_pool_(8, "GATEWAY"),
      rpc_server_(rpc_ip, rpc_port, 4),
      im_client_(etcd_endpoints, im_service,
                 std::make_shared<RoundRobinBalancer>()),
      auth_client_(etcd_endpoints, auth_service,
                   std::make_shared<RoundRobinBalancer>()),
      gateway_id_(gateway_id), rpc_ip_(rpc_ip), rpc_port_(rpc_port),
      shared_secret_(shared_secret) {
  // 客户端侧：接受长连接
  client_server_.setnewconnectioncb(
      [this](spConnection conn) { this->onNewConnection(conn); });
  client_server_.setcloseconnectioncb(
      [this](spConnection conn) { this->onConnectionClosed(conn); });
  client_server_.setonmessagecb([this](spConnection conn, Buffer &buf) {
    this->onClientMessage(conn, buf);
  });
  client_server_.setIdleTimeout(120.0); // 长时间无数据的连接直接关闭

  // IM 侧：接收回推
  // 用 lambda 而非 std::bind：bind 表达式可吞掉多余实参，会同时匹配
  // conn-aware 和 with-context 两个 registerMethod 重载而产生歧义。
  rpc_server_.serviceManager().registerMethod(
      "GatewayService", "Push",
      [this](spConnection conn, const std::string &body) {
        return handlePush(conn, body);
      });
}

void GatewayServer::enableClientTls(const std::string &cert,
                                    const std::string &key) {
  client_server_.enableTls(cert, key);
}

void GatewayServer::start() {
  // client_server_.start() 会阻塞在当前线程跑主循环（accept），
  // 所以 rpc_server_（接收 IM 回推）必须放到独立线程，否则永远不会 accept。
  rpc_thread_ = std::thread([this] { rpc_server_.start(); });
  client_server_.start();
}

void GatewayServer::stop() {
  client_server_.stop();
  rpc_server_.stop();
  if (rpc_thread_.joinable())
    rpc_thread_.join();
  work_pool_.stop();
}

void GatewayServer::onNewConnection(spConnection conn) {
  uint64_t conn_id = next_conn_id_.fetch_add(1);
  ConnState st;
  st.conn = conn;
  std::lock_guard<std::mutex> lk(mutex_);
  fd_to_conn_[conn->fd()] = conn_id;
  conns_[conn_id] = std::move(st);
}

void GatewayServer::onConnectionClosed(spConnection conn) {
  uint64_t conn_id = 0;
  std::string im_ip;
  uint16_t im_port = 0;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto fit = fd_to_conn_.find(conn->fd());
    if (fit == fd_to_conn_.end())
      return;
    conn_id = fit->second;
    fd_to_conn_.erase(fit);
    auto it = conns_.find(conn_id);
    if (it != conns_.end()) {
      im_ip = it->second.im_ip;
      im_port = it->second.im_port;
      conns_.erase(it);
    }
  }
  // 已固定到某 IM 节点：异步通知其清理会话（别阻塞 IO 线程）
  if (!im_ip.empty()) {
    if (!work_pool_.tryAdd([this, conn_id, im_ip, im_port] {
          notifyDisconnect(conn_id, im_ip, im_port);
        }))
      LOG_WARN("Gateway: work pool full, dropping disconnect notify conn=%llu",
               (unsigned long long)conn_id);
  }
}

void GatewayServer::onClientMessage(spConnection conn, Buffer &buf) {
  uint64_t conn_id = 0;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto fit = fd_to_conn_.find(conn->fd());
    if (fit == fd_to_conn_.end())
      return;
    conn_id = fit->second;
  }
  // 解帧循环：muduo 现在只给原始字节，这里逐帧剥头。
  while (true) {
    std::string payload;
    size_t frame_len = 0;
    FrameDecode r =
        tryDecodeFrame(buf.peek(), buf.readableBytes(), &frame_len, &payload);
    if (r == FrameDecode::kOk) {
      buf.retrieve(frame_len);
      // 非阻塞转发：满则丢弃，由 IM 离线消息/Ack 机制补偿
      if (!work_pool_.tryAdd(
              [this, conn_id, payload] { forwardToIm(conn_id, payload); }))
        LOG_WARN("Gateway: work pool full, dropping client msg conn=%llu",
                 (unsigned long long)conn_id);
    } else if (r == FrameDecode::kError) {
      conn->forceClose();
      return;
    } else { // kNeedMore
      break;
    }
  }
}

bool GatewayServer::isRetryableMethod(const std::string &method) const {
  static const std::unordered_set<std::string> kRetryable = {
      "SendMessage", "AckMessage", "GetFriendList", "PullOfflineMessages"};
  return kRetryable.count(method) != 0;
}

bool GatewayServer::isPreAuthMethod(const std::string &method) const {
  static const std::unordered_set<std::string> kPreAuth = {
      "Login", "Register", "IssueTicket", "Refresh"};
  return kPreAuth.count(method) != 0;
}

void GatewayServer::forwardToIm(uint64_t conn_id, std::string payload) {
  // payload 已由 onClientMessage 的解帧循环去掉 4 字节长度前缀
  RpcMessage req;
  if (!decodeMessage(payload, req) || !req.has_header()) {
    return;
  }
  const std::string method = req.header().method_name();

  std::shared_ptr<Connection> conn;
  std::string pinned_ip;
  uint16_t pinned_port = 0;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = conns_.find(conn_id);
    if (it == conns_.end())
      return;
    conn = it->second.conn.lock();
    if (!conn)
      return;
    pinned_ip = it->second.im_ip;
    pinned_port = it->second.im_port;
  }

  // 注入 header：网关身份 + 连接定位 + 客户端真实 IP + 本网关回推地址
  req.mutable_header()->set_gateway_id(gateway_id_);
  req.mutable_header()->set_conn_id(conn_id);
  req.mutable_header()->set_client_ip(conn->ip());
  req.mutable_header()->set_gateway_rpc_ip(rpc_ip_);
  req.mutable_header()->set_gateway_rpc_port(rpc_port_);
  uint64_t client_seq = req.header().sequence_id();

  // ===== 信任域入口：按连接认证状态分流 =====
  Identity id;
  bool authenticated = false;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = conns_.find(conn_id);
    if (it != conns_.end()) {
      authenticated = it->second.authenticated;
      if (authenticated)
        id = it->second.identity;
    }
  }
  if (!authenticated) {
    if (method == "Connect") {
      // 首帧：解析 ticket → 调 Auth ResolveTicket 完成身份绑定
      im::ConnectRequest creq;
      if (!creq.ParseFromString(req.body())) {
        return; // 非法 Connect，丢弃
      }
      if (!resolveTicket(conn_id, creq.ticket())) {
        im::ConnectResponse cresp;
        cresp.set_success(false);
        cresp.set_message("ticket invalid");
        RpcMessage resp =
            buildResponse(client_seq, 0, cresp.SerializeAsString());
        std::string frame = encodeMessage(resp);
        conn->send(frame.data(), frame.size());
        return;
      }
      {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = conns_.find(conn_id);
        if (it == conns_.end())
          return;
        id = it->second.identity;
        authenticated = true;
      }
    } else if (isPreAuthMethod(method)) {
      // 鉴权前方法（Login/Register/IssueTicket/Refresh）：无身份，原样透传
    } else {
      LOG_WARN("Gateway drop unauth request: conn=%llu method=%s",
               (unsigned long long)conn_id, method.c_str());
      return;
    }
  }
  if (authenticated) {
    signAndInject(req, id);
  }

  // 故障转移重试：首次用 pin 节点，传输层失败时对白名单方法换节点重试
  std::unordered_set<std::string> tried;
  std::string resp_body;
  int32_t err = 0;
  bool ok = false;
  std::string final_ip = pinned_ip;
  uint16_t final_port = pinned_port;

  for (int attempt = 0; attempt <= kForwardMaxRetries; ++attempt) {
    std::string ip = final_ip;
    uint16_t port = final_port;
    // pin 为空（首帧）或已失败，则选一个未尝试过的节点
    if (ip.empty() || tried.count(ip + ":" + std::to_string(port))) {
      auto node = im_client_.pickNodeExcept(tried);
      if (!node.has_value())
        break;
      ip = node->ip;
      port = node->port;
    }
    std::string addr = ip + ":" + std::to_string(port);
    tried.insert(addr);

    ok = getImChannel(ip, port)->CallMessage(req, resp_body, err);
    if (ok) {
      final_ip = ip;
      final_port = port;
      break;
    }

    LOG_WARN("Gateway forward fail: conn=%llu method=%s attempt=%d node=%s",
             (unsigned long long)conn_id, method.c_str(), attempt,
             addr.c_str());

    // 仅白名单方法在传输层失败时重试；重试前确认客户端连接还在
    if (!isRetryableMethod(method) || attempt >= kForwardMaxRetries ||
        conn->disconnected())
      break;
    std::this_thread::sleep_for(
        std::chrono::milliseconds(kRetryBackoffMs * (attempt + 1)));
  }

  // 成功且最终节点与 pin 不一致（含原本未 pin）时更新 pin
  if (ok && (final_ip != pinned_ip || final_port != pinned_port)) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = conns_.find(conn_id);
    if (it != conns_.end()) {
      it->second.im_ip = final_ip;
      it->second.im_port = final_port;
    }
  }

  // 用客户端原来的 sequence_id 回写响应
  // encodeMessage 已自带 4 字节 LE 长度前缀，不要再套一层
  RpcMessage resp = buildResponse(client_seq, ok ? err : -1, resp_body);
  std::string frame = encodeMessage(resp);
  conn->send(frame.data(), frame.size());
}

bool GatewayServer::resolveTicket(uint64_t conn_id, const std::string &ticket) {
  auth::ResolveTicketRequest areq;
  areq.set_ticket(ticket);
  areq.set_gateway_id(gateway_id_);
  areq.set_conn_id(conn_id);
  std::string resp_body;
  int32_t err = 0;
  if (!auth_client_.Call("ResolveTicket", areq.SerializeAsString(), resp_body,
                         err)) {
    LOG_WARN("Gateway resolve ticket RPC failed: conn=%llu",
             (unsigned long long)conn_id);
    return false;
  }
  auth::ResolveTicketResponse aresp;
  if (!aresp.ParseFromString(resp_body) || !aresp.valid()) {
    LOG_WARN("Gateway resolve ticket rejected: conn=%llu msg=%s",
             (unsigned long long)conn_id, aresp.message().c_str());
    return false;
  }
  const auth::SessionInfo &s = aresp.session();
  Identity id;
  id.set_user_id(s.user_id());
  id.set_session_id(s.session_id());
  id.set_device_id(s.device_id());
  id.set_username(s.username());
  id.set_device_type(static_cast<int32_t>(s.device_type()));
  id.set_authenticated_at(static_cast<int64_t>(std::time(nullptr)));

  std::lock_guard<std::mutex> lk(mutex_);
  auto it = conns_.find(conn_id);
  if (it == conns_.end()) {
    return false;
  }
  it->second.identity = std::move(id);
  it->second.authenticated = true;
  return true;
}

void GatewayServer::signAndInject(RpcMessage &req, const Identity &id) {
  RpcHeader *hdr = req.mutable_header();
  *hdr->mutable_identity() = id;
  hdr->set_timestamp(static_cast<int64_t>(std::time(nullptr)));
  hdr->set_nonce(gateway_id_ + ":" + std::to_string(next_nonce_.fetch_add(1)));
  hdr->set_signature(gateway_sign::sign(shared_secret_, *hdr));
}

std::shared_ptr<RpcChannel> GatewayServer::getImChannel(const std::string &ip,
                                                        uint16_t port) {
  return im_channels_.getOrCreate(ip + ":" + std::to_string(port), ip, port,
                                  kForwardTimeoutMs);
}

void GatewayServer::notifyDisconnect(uint64_t conn_id, const std::string &im_ip,
                                     uint16_t im_port) {
  im::ClientDisconnectRequest req;
  req.set_gateway_id(gateway_id_);
  req.set_conn_id(conn_id);
  auto ch = getImChannel(im_ip, im_port);
  std::string resp_body;
  int32_t err = 0;
  ch->Call("ImService", "ClientDisconnect", req.SerializeAsString(), resp_body,
           err);
}

std::string GatewayServer::handlePush(spConnection conn,
                                      const std::string &request_body) {
  (void)conn;
  im::GatewayPushRequest req;
  im::GatewayPushResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  std::shared_ptr<Connection> target;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = conns_.find(req.conn_id());
    if (it != conns_.end()) {
      target = it->second.conn.lock();
    }
  }
  if (!target) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  // frame 已是 IM 节点打包好的 [4字节LE长度][ServerPushEnvelope]，直写
  target->send(req.frame().data(), req.frame().size());
  if (req.force_close()) {
    target->forceClose();
  }
  resp.set_success(true);
  return resp.SerializeAsString();
}
