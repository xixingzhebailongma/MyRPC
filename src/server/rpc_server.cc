#include "rpc_server.h"
#include "Connection.h"
#include "Logger.h"
#include "TcpServer.h"
#include "idempotency_lru.h"
#include "idempotency_store.h"
#include "lease_renewer.h"
#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include "service_manager.h"
#include "service_registry.h"
#include <chrono>
#include <cstdint>
#include <string>
RpcServer::RpcServer(const std::string &ip, uint16_t port, int threadnum,
                     int workthreadnum)
    : server_(ip, port, threadnum), workPool_(workthreadnum, "WORK"),
      idemStore_(std::make_unique<IdempotencyLru>()) {
  //绑定消息回调
  server_.setonmessagecb(
      [this](spConnection conn, Buffer &buf) { this->onMessage(conn, buf); });
  //绑定新连接回调
  server_.setnewconnectioncb(
      [this](spConnection conn) { this->onConnection(conn); });
  //绑定连接关闭回调（仅日志：连接生命周期已上移到应用层 RPC，
  //见 ImService/ClientDisconnect）
  server_.setcloseconnectioncb([](spConnection conn) {
    LOG_INFO("RpcServer: connection closed %s:%d", conn->ip().c_str(),
             conn->port());
  });
}

RpcServer::~RpcServer() = default;

void RpcServer::start() {
  // 先创建租约续期器（setIdempotencyStore 须在 start() 前完成注入）
  renewer_ = std::make_unique<LeaseRenewer>(idemStore_.get());
  renewer_->start();

  // 先注册到 etcd，再启动阻塞的主事件循环。
  // 注意：server_.start() = mainloop_->run() 永不返回，注册必须放在它前面，
  // 否则服务永远不会出现在 etcd 里。
  if (registry_) {
    // 首轮失败不再意味着永久降级：EtcdClient 后台线程会持续重试注册
    if (!registry_->registerService()) {
      LOG_WARN("RpcServer: first etcd registration failed, "
               "retrying in background");
    }
  }

  server_.start(); // 阻塞：主事件循环（mainloop_->run()）
}

void RpcServer::stop() {
  if (registry_) {
    registry_->stop();
  }
  server_.stop(); // 先停止 IO（不再有新的 dispatch/add）
  workPool_.stop(); // 再排空并 join 工作线程（不再有 complete/remove）
  if (renewer_) {
    renewer_->stop(); // 最后停续租线程
  }
}

void RpcServer::submitTask(std::function<void()> task) {
  workPool_.addtask(std::move(task));
}
void RpcServer::setIdempotencyStore(std::unique_ptr<IdempotencyStore> store) {
  idemStore_ = std::move(store);
}
void RpcServer::onMessage(spConnection conn, Buffer &buf) {
  // muduo 现在是哑管道，这里用 protocol 的 tryDecodeFrame 逐帧剥头。
  while (true) {
    std::string payload;
    size_t frame_len = 0;
    FrameDecode r =
        tryDecodeFrame(buf.peek(), buf.readableBytes(), &frame_len, &payload);
    if (r == FrameDecode::kOk) {
      buf.retrieve(frame_len); // 消费掉这一帧
      dispatch(conn, std::move(payload));
    } else if (r == FrameDecode::kError) {
      LOG_WARN("RpcServer: illegal frame length from fd=%d, closing.",
               conn->fd());
      conn->forceClose();
      return;
    } else { // kNeedMore
      break;
    }
  }
}

void RpcServer::dispatch(spConnection conn, std::string payload) {
  RpcMessage request;
  if (!decodeMessage(payload, request)) {
    LOG_WARN("RpcServer: failed to decode RPC message from fd=%d", conn->fd());
    return;
  }
  const auto &header = request.header();

  std::string service_name = header.service_name();
  std::string method_name = header.method_name();
  uint64_t seq = header.sequence_id();
  std::string body = request.body();
  const std::string &trace_id = header.trace_id();

  LOG_DEBUG("RpcServer: request service=%s method=%s seq=%lu trace_id=%s",
            service_name.c_str(), method_name.c_str(), seq,
            trace_id.empty() ? "-" : trace_id.c_str());
  // INFO 级入口：DEBUG 在 release(NDEBUG) 下被剥离，观测/验证需要一条常开日志。
  LOG_INFO("RpcServer: request service=%s method=%s seq=%lu trace_id=%s",
           service_name.c_str(), method_name.c_str(), seq,
           trace_id.empty() ? "-" : trace_id.c_str());

  RpcHeader hdr = header; // 完整 header（含 gateway_id/conn_id/client_ip 等）

  // 应用层心跳：按 type 识别，走 workPool_ 应答 pong（业务线程池打满时心跳也
  // 答不上，客户端才能探测到“进程活着但服务卡死”），任务本身极小。
  if (header.type() == MessageType::MSG_HEARTBEAT) {
    if (!workPool_.tryAdd([conn, seq]() {
          conn->send(encodeMessage(buildHeartbeatAck(seq)));
        }))
      LOG_WARN("RpcServer: work pool full, dropping heartbeat seq=%lu", seq);
    return;
  }

  // 幂等去重：非心跳且带 request_id 的请求，先 claim 一次，短路后续 3 种
  // handler。 claim 成功（kExecute）后才执行 handler，worker 内 complete()；
  // 命中完成态（kReplay）直接重放缓存响应；命中 in-flight（kInFlight）丢弃，
  // 客户端超时后会用同一 request_id 重试，届时命中 kReplay。
  const std::string &request_id = header.request_id();
  const bool dedup_enabled = !request_id.empty();
  std::string cache_key;
  IdempotencyStore *idem = idemStore_.get(); // 拷贝裸指针，lambda 不捕获 this
  LeaseRenewer *renewer = renewer_.get(); // 同上；start() 后非空
  IdemLease lease;                        // claim 成功时填入 fencing token
  if (dedup_enabled) {
    // request_id 格式校验：非法直接拒绝，不 claim、不执行
    if (!isValidRequestId(request_id)) {
      LOG_WARN("RpcServer: invalid request_id rejected (len=%zu) seq=%lu",
               request_id.size(), seq);
      conn->send(encodeMessage(buildResponse(seq, kErrInvalidRequestId, "")));
      return;
    }
    cache_key = buildCacheKey(service_name, method_name, request_id, header);
    std::string cached_body;
    IdemResult r = idem->claim(cache_key, &cached_body, &lease);
    if (r == IdemResult::kReplay) {
      LOG_DEBUG("RpcServer: replay idempotent response %s seq=%lu",
                cache_key.c_str(), seq);
      conn->send(encodeMessage(buildResponse(seq, 0, cached_body)));
      return;
    }
    if (r == IdemResult::kInFlight) {
      LOG_WARN("RpcServer: duplicate in-flight %s seq=%lu, dropping",
               cache_key.c_str(), seq);
      return;
    }
    // kExecute：登记租约续期；软截止到期由 renewer 释放租约
    if (renewer) {
      auto deadline = std::chrono::steady_clock::time_point::max();
      if (execution_deadline_ms_ > 0)
        deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(execution_deadline_ms_);
      renewer->add(cache_key, lease, deadline);
    }
  }

  // 1) 优先查找带 context(header) 的 handler
  auto handlerWithContext =
      serviceMgr_.findMethodWithContext(service_name, method_name);
  if (handlerWithContext) {
    if (!workPool_.tryAdd([conn, handlerWithContext, seq, body, hdr,
                           dedup_enabled, cache_key, idem, lease, renewer]() {
          std::string resp = handlerWithContext(conn, body, hdr);
          if (dedup_enabled) {
            idem->complete(cache_key, lease, resp);
            if (renewer)
              renewer->remove(cache_key);
          }
          std::string wire = encodeMessage(buildResponse(seq, 0, resp));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem, lease,
                       renewer);
    return;
  }

  // 2) 其次查找需要 connection 的 handler
  auto handlerWithConn =
      serviceMgr_.findMethodWithConn(service_name, method_name);
  if (handlerWithConn) {
    if (!workPool_.tryAdd([conn, handlerWithConn, seq, body, dedup_enabled,
                           cache_key, idem, lease, renewer]() {
          std::string resp = handlerWithConn(conn, body);
          if (dedup_enabled) {
            idem->complete(cache_key, lease, resp);
            if (renewer)
              renewer->remove(cache_key);
          }
          std::string wire = encodeMessage(buildResponse(seq, 0, resp));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem, lease,
                       renewer);
    return;
  }

  // 3) 最后查找普通 handler
  auto handler = serviceMgr_.findMethod(service_name, method_name);
  if (handler) {
    if (!workPool_.tryAdd([conn, handler, seq, body, dedup_enabled, cache_key,
                           idem, lease, renewer]() {
          std::string resp = handler(body);
          if (dedup_enabled) {
            idem->complete(cache_key, lease, resp);
            if (renewer)
              renewer->remove(cache_key);
          }
          std::string wire = encodeMessage(buildResponse(seq, 0, resp));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem, lease,
                       renewer);
    return;
  }
  // 结果型 handler（带 error_code）：成功才缓存；失败 abort
  // 不缓存，允许重试重执行。
  auto handlerWithContextResult =
      serviceMgr_.findMethodWithContextResult(service_name, method_name);
  if (handlerWithContextResult) {
    if (!workPool_.tryAdd([conn, handlerWithContextResult, seq, body, hdr,
                           dedup_enabled, cache_key, idem, lease, renewer]() {
          RpcMethodResult result = handlerWithContextResult(conn, body, hdr);
          if (dedup_enabled) {
            if (result.error_code == 0)
              idem->complete(cache_key, lease, result.body);
            else
              idem->abort(cache_key,
                          lease); // 失败不缓存，客户端重试会真正重执行
            if (renewer)
              renewer->remove(cache_key);
          }
          std::string wire =
              encodeMessage(buildResponse(seq, result.error_code, result.body));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem, lease,
                       renewer);
    return;
  }

  auto handlerWithConnResult =
      serviceMgr_.findMethodWithConnResult(service_name, method_name);
  if (handlerWithConnResult) {
    if (!workPool_.tryAdd([conn, handlerWithConnResult, seq, body,
                           dedup_enabled, cache_key, idem, lease, renewer]() {
          RpcMethodResult result = handlerWithConnResult(conn, body);
          if (dedup_enabled) {
            if (result.error_code == 0)
              idem->complete(cache_key, lease, result.body);
            else
              idem->abort(cache_key, lease);
            if (renewer)
              renewer->remove(cache_key);
          }
          std::string wire =
              encodeMessage(buildResponse(seq, result.error_code, result.body));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem, lease,
                       renewer);
    return;
  }

  auto handlerResult = serviceMgr_.findMethodResult(service_name, method_name);
  if (handlerResult) {
    if (!workPool_.tryAdd([conn, handlerResult, seq, body, dedup_enabled,
                           cache_key, idem, lease, renewer]() {
          RpcMethodResult result = handlerResult(body);
          if (dedup_enabled) {
            if (result.error_code == 0)
              idem->complete(cache_key, lease, result.body);
            else
              idem->abort(cache_key, lease);
            if (renewer)
              renewer->remove(cache_key);
          }
          std::string wire =
              encodeMessage(buildResponse(seq, result.error_code, result.body));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem, lease,
                       renewer);
    return;
  }
  // 4) 找不到方法：释放已 claim 的占位，避免该 key 永久 in-flight。
  LOG_WARN("RpcServer: method not found: %s.%s", service_name.c_str(),
           method_name.c_str());
  if (dedup_enabled) {
    idem->abort(cache_key, lease);
    if (renewer)
      renewer->remove(cache_key);
  }
  std::string wire = encodeMessage(buildResponse(seq, -1, ""));
  conn->send(std::move(wire));
}

void RpcServer::rejectOverloaded(spConnection conn, uint64_t seq,
                                 bool dedup_enabled,
                                 const std::string &cache_key,
                                 IdempotencyStore *idem, const IdemLease &lease,
                                 LeaseRenewer *renewer) {
  if (dedup_enabled) {
    idem->abort(cache_key,
                lease); // 释放已 claim 的 in-flight 占位，让客户端可重试
    if (renewer)
      renewer->remove(cache_key);
  }
  LOG_WARN("RpcServer: work pool full, rejecting seq=%lu (overloaded)", seq);
  conn->send(encodeMessage(buildResponse(seq, kErrServerOverloaded, "")));
}

std::string RpcServer::buildCacheKey(const std::string &service_name,
                                     const std::string &method_name,
                                     const std::string &request_id,
                                     const RpcHeader &header) {
  // key = idem:{namespace}:{caller_id}:{service}/{method}:{request_id}
  // caller_id 为空时省略该段（不隔离，保持全局唯一押注在 request_id 上）
  std::string key = "idem:" + namespace_;
  std::string caller =
      caller_id_extractor_ ? caller_id_extractor_(header) : std::string();
  if (!caller.empty())
    key += ":" + caller;
  key += ":" + service_name + "/" + method_name + ":" + request_id;
  return key;
}

bool RpcServer::isValidRequestId(const std::string &request_id) {
  // 长度 ≤128，字符集 [0-9a-zA-Z-_.]（与客户端 generateUuid 输出兼容）。
  // 拒绝极短/超长/含特殊字符的 ID，降低碰撞与注入风险。
  if (request_id.empty() || request_id.size() > 128)
    return false;
  for (char c : request_id) {
    bool ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') ||
              (c >= 'A' && c <= 'Z') || c == '-' || c == '_' || c == '.';
    if (!ok)
      return false;
  }
  return true;
}

void RpcServer::onConnection(spConnection conn) {
  LOG_INFO("RpcServer: new connection from %s:%d", conn->ip().c_str(),
           conn->port());
}

void RpcServer::enableRegistry(const std::string &etcdEndpoints,
                               const std::string &serviceName,
                               const std::string &ip, uint16_t port,
                               int64_t tll) {
  registry_ = std::make_unique<ServiceRegistry>(etcdEndpoints, serviceName, ip,
                                                port, tll);
}

void RpcServer::setTimeoutCallback(std::function<void(EventLoop *)> cb) {
  server_.settimeoutcb(std::move(cb));
}
void RpcServer::setPeriodTimer(double interval,
                               std::function<void(EventLoop *)> cb) {
  server_.setPeriodicTimer(interval, std::move(cb));
}
void RpcServer::setIdleTimeout(double seconds) {
  server_.setIdleTimeout(seconds);
}