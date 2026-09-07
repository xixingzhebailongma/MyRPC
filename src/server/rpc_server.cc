#include "rpc_server.h"
#include "Connection.h"
#include "Logger.h"
#include "TcpServer.h"
#include "idempotency_lru.h"
#include "idempotency_store.h"
#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include "service_manager.h"
#include "service_registry.h"
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
  server_.start();

  if (registry_) {
    // 首轮失败不再意味着永久降级：EtcdClient 后台线程会持续重试注册
    if (!registry_->registerService()) {
      LOG_WARN("RpcServer: first etcd registration failed, "
               "retrying in background");
    }
  }
}

void RpcServer::stop() {
  if (registry_) {
    registry_->stop();
  }
  server_.stop(); // 先 join 全部 IO 线程，之后不再有新的 onMessage
  workPool_.stop(); // 再排空并 join 工作线程
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

  LOG_DEBUG("RpcServer: request service=%s method=%s seq=%lu",
            service_name.c_str(), method_name.c_str(), seq);

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
  if (dedup_enabled) {
    cache_key = service_name + "/" + method_name + ":" + request_id;
    std::string cached_body;
    IdemResult r = idem->claim(cache_key, &cached_body);
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
    // kExecute：继续执行；handler 未找到等失败路径会 abort() 释放占位。
  }

  // 1) 优先查找带 context(header) 的 handler
  auto handlerWithContext =
      serviceMgr_.findMethodWithContext(service_name, method_name);
  if (handlerWithContext) {
    if (!workPool_.tryAdd([conn, handlerWithContext, seq, body, hdr,
                           dedup_enabled, cache_key, idem]() {
          std::string resp = handlerWithContext(conn, body, hdr);
          if (dedup_enabled)
            idem->complete(cache_key, resp);
          std::string wire = encodeMessage(buildResponse(seq, 0, resp));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem);
    return;
  }

  // 2) 其次查找需要 connection 的 handler
  auto handlerWithConn =
      serviceMgr_.findMethodWithConn(service_name, method_name);
  if (handlerWithConn) {
    if (!workPool_.tryAdd([conn, handlerWithConn, seq, body, dedup_enabled,
                           cache_key, idem]() {
          std::string resp = handlerWithConn(conn, body);
          if (dedup_enabled)
            idem->complete(cache_key, resp);
          std::string wire = encodeMessage(buildResponse(seq, 0, resp));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem);
    return;
  }

  // 3) 最后查找普通 handler
  auto handler = serviceMgr_.findMethod(service_name, method_name);
  if (handler) {
    if (!workPool_.tryAdd(
            [conn, handler, seq, body, dedup_enabled, cache_key, idem]() {
              std::string resp = handler(body);
              if (dedup_enabled)
                idem->complete(cache_key, resp);
              std::string wire = encodeMessage(buildResponse(seq, 0, resp));
              conn->send(std::move(wire));
            }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem);
    return;
  }
  // 结果型 handler（带 error_code）：成功才缓存；失败 abort
  // 不缓存，允许重试重执行。
  auto handlerWithContextResult =
      serviceMgr_.findMethodWithContextResult(service_name, method_name);
  if (handlerWithContextResult) {
    if (!workPool_.tryAdd([conn, handlerWithContextResult, seq, body, hdr,
                           dedup_enabled, cache_key, idem]() {
          RpcMethodResult result = handlerWithContextResult(conn, body, hdr);
          if (dedup_enabled) {
            if (result.error_code == 0)
              idem->complete(cache_key, result.body);
            else
              idem->abort(cache_key); // 失败不缓存，客户端重试会真正重执行
          }
          std::string wire =
              encodeMessage(buildResponse(seq, result.error_code, result.body));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem);
    return;
  }

  auto handlerWithConnResult =
      serviceMgr_.findMethodWithConnResult(service_name, method_name);
  if (handlerWithConnResult) {
    if (!workPool_.tryAdd([conn, handlerWithConnResult, seq, body,
                           dedup_enabled, cache_key, idem]() {
          RpcMethodResult result = handlerWithConnResult(conn, body);
          if (dedup_enabled) {
            if (result.error_code == 0)
              idem->complete(cache_key, result.body);
            else
              idem->abort(cache_key);
          }
          std::string wire =
              encodeMessage(buildResponse(seq, result.error_code, result.body));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem);
    return;
  }

  auto handlerResult = serviceMgr_.findMethodResult(service_name, method_name);
  if (handlerResult) {
    if (!workPool_.tryAdd([conn, handlerResult, seq, body, dedup_enabled,
                           cache_key, idem]() {
          RpcMethodResult result = handlerResult(body);
          if (dedup_enabled) {
            if (result.error_code == 0)
              idem->complete(cache_key, result.body);
            else
              idem->abort(cache_key);
          }
          std::string wire =
              encodeMessage(buildResponse(seq, result.error_code, result.body));
          conn->send(std::move(wire));
        }))
      rejectOverloaded(conn, seq, dedup_enabled, cache_key, idem);
    return;
  }
  // 4) 找不到方法：释放已 claim 的占位，避免该 key 永久 in-flight。
  LOG_WARN("RpcServer: method not found: %s.%s", service_name.c_str(),
           method_name.c_str());
  if (dedup_enabled)
    idem->abort(cache_key);
  std::string wire = encodeMessage(buildResponse(seq, -1, ""));
  conn->send(std::move(wire));
}

void RpcServer::rejectOverloaded(spConnection conn, uint64_t seq,
                                 bool dedup_enabled,
                                 const std::string &cache_key,
                                 IdempotencyStore *idem) {
  if (dedup_enabled)
    idem->abort(cache_key); // 释放已 claim 的 in-flight 占位，让客户端可重试
  LOG_WARN("RpcServer: work pool full, rejecting seq=%lu (overloaded)", seq);
  conn->send(encodeMessage(buildResponse(seq, kErrServerOverloaded, "")));
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