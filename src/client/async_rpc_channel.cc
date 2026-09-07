#include "async_rpc_channel.h"
#include "Buffer.h"
#include "Connection.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Logger.h"
#include "TcpClient.h"
#include "rpc_error_code.h"
#include <chrono>

namespace {
uint64_t nowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
} // namespace

AsyncRpcChannel::AsyncRpcChannel(EventLoop *loop, const std::string &ip,
                                 uint16_t port, const RpcClientConfig &cfg)
    : loop_(loop), server_ip_(ip), server_port_(port), cfg_(cfg),
      failure_threshold_(cfg.circuit_failure_threshold),
      backoff_base_ms_(cfg.circuit_backoff_base_ms),
      backoff_max_ms_(cfg.circuit_backoff_max_ms) {
  client_.reset(new TcpClient(loop, InetAddress(ip, port), "AsyncRpcChannel"));
}

AsyncRpcChannel::~AsyncRpcChannel() { client_.reset(); }

void AsyncRpcChannel::connect() {
  if (closed_.load())
    return;
  auto self = shared_from_this();
  loop_->queueinloop([self] { self->startInLoop(); });
}

void AsyncRpcChannel::close() {
  bool expected = false;
  if (!closed_.compare_exchange_strong(expected, true))
    return;
  auto self = shared_from_this();
  loop_->queueinloop([self] {
    self->loop_->cancel(self->heartbeat_timer_);
    self->client_->stop();
    self->conn_.reset();
    self->connected_.store(false);
    self->failPending(static_cast<int32_t>(RpcError::CONNECTION_BROKEN));
  });
}

std::future<std::string> AsyncRpcChannel::Call(const std::string &service,
                                               const std::string &method,
                                               const std::string &request_body,
                                               int timeout_ms,
                                               const std::string &request_id) {
  auto promise = std::make_shared<std::promise<std::string>>();
  std::future<std::string> fut = promise->get_future();
  callImpl(service, method, request_body, timeout_ms, request_id, promise,
           nullptr);
  return fut;
}

void AsyncRpcChannel::Call(const std::string &service,
                           const std::string &method,
                           const std::string &request_body, ResponseCallback cb,
                           int timeout_ms, const std::string &request_id) {
  callImpl(service, method, request_body, timeout_ms, request_id, nullptr,
           std::move(cb));
}

void AsyncRpcChannel::callImpl(
    const std::string &service, const std::string &method,
    const std::string &body, int timeout_ms, const std::string &request_id,
    std::shared_ptr<std::promise<std::string>> promise, ResponseCallback cb) {
  auto self = shared_from_this();
  loop_->queueinloop([self, service, method, body, timeout_ms, request_id,
                      promise, cb] {
    if (self->closed_.load()) {
      PendingCall pc;
      pc.promise = promise;
      pc.callback = cb;
      self->deliver(pc, "", static_cast<int32_t>(RpcError::CONNECTION_BROKEN));
      return;
    }

    self->startInLoop();

    // 熔断期快速失败：不 connect、不记失败、也不计调用数
    if (self->isCircuitOpen()) {
      PendingCall pc;
      pc.promise = promise;
      pc.callback = cb;
      self->deliver(pc, "", static_cast<int32_t>(RpcError::CIRCUIT_OPEN));
      return;
    }

    self->total_calls_.fetch_add(1, std::memory_order_relaxed);
    int eff = timeout_ms < 0 ? self->cfg_.timeout_ms : timeout_ms;
    uint64_t seq = self->next_seq_id_++;

    RpcMessage req = buildRequest(service, method, seq, body);
    if (!request_id.empty())
      req.mutable_header()->set_request_id(request_id);
    std::string wire = encodeMessage(req);

    PendingCall pc;
    pc.promise = promise;
    pc.callback = cb;
    pc.timeout_timer = self->loop_->runAfter(
        eff / 1000.0, [self, seq] { self->onTimeout(seq); });
    self->pending_.emplace(seq, std::move(pc));

    if (self->conn_ && !self->conn_->disconnected()) {
      self->conn_->send(std::move(wire));
    } else {
      self->failCall(seq, static_cast<int32_t>(RpcError::CONNECTION_BROKEN));
    }
  });
}

void AsyncRpcChannel::startInLoop() {
  if (started_ || closed_.load())
    return;
  started_ = true;

  std::weak_ptr<AsyncRpcChannel> weak = weak_from_this();
  client_->setConnectionCallback([weak](std::shared_ptr<Connection> c) {
    if (auto s = weak.lock())
      s->onConnection(c);
  });
  client_->setCloseCallback([weak](std::shared_ptr<Connection> c) {
    if (auto s = weak.lock())
      s->onClose(c);
  });
  client_->setMessageCallback([weak](std::shared_ptr<Connection> c, Buffer &b) {
    if (auto s = weak.lock())
      s->onMessage(c, b);
  });
  client_->enableRetry();
  client_->connect();

  if (cfg_.heartbeat_interval_ms > 0) {
    double interval = cfg_.heartbeat_interval_ms / 1000.0;
    heartbeat_timer_ = loop_->runEvery(interval, [weak] {
      if (auto s = weak.lock())
        s->heartbeatTick();
    });
  }
}

void AsyncRpcChannel::onConnection(std::shared_ptr<Connection> conn) {
  conn_ = conn;
  connected_.store(true);
  last_active_ms_ = nowMs();
  heartbeat_miss_ = 0;
}

void AsyncRpcChannel::onClose(std::shared_ptr<Connection> conn) {
  connected_.store(false);
  if (conn_ == conn)
    conn_.reset();
  failPending(static_cast<int32_t>(RpcError::CONNECTION_BROKEN));
}

void AsyncRpcChannel::onMessage(std::shared_ptr<Connection> conn, Buffer &buf) {
  while (true) {
    std::string payload;
    size_t frame_len = 0;
    FrameDecode r =
        tryDecodeFrame(buf.peek(), buf.readableBytes(), &frame_len, &payload);
    if (r == FrameDecode::kOk) {
      buf.retrieve(frame_len);
      handleFrame(payload);
    } else if (r == FrameDecode::kError) {
      conn->forceClose();
      return;
    } else { // kNeedMore
      break;
    }
  }
}

void AsyncRpcChannel::handleFrame(const std::string &payload) {
  // 任何入站帧都视为对端存活（含心跳应答、服务端推送）
  last_active_ms_ = nowMs();
  heartbeat_miss_ = 0;

  RpcMessage msg;
  if (!decodeMessage(payload, msg))
    return;
  if (!msg.has_header()) {
    if (push_handler_)
      push_handler_(msg);
    return;
  }
  uint64_t seq = msg.header().sequence_id();
  auto it = pending_.find(seq);
  if (it == pending_.end())
    return; // 迟到/未知响应（含心跳应答）丢弃
  PendingCall pc = std::move(it->second);
  pending_.erase(it);
  loop_->cancel(pc.timeout_timer);
  int32_t ec = msg.header().error_code();
  std::string body = std::move(*msg.mutable_body());
  success_calls_.fetch_add(1, std::memory_order_relaxed);
  recordSuccess();
  deliver(pc, std::move(body), ec);
}

void AsyncRpcChannel::onTimeout(uint64_t seq) {
  failCall(seq, static_cast<int32_t>(RpcError::TIMEOUT));
}

void AsyncRpcChannel::failCall(uint64_t seq, int32_t code) {
  auto it = pending_.find(seq);
  if (it == pending_.end())
    return;
  PendingCall pc = std::move(it->second);
  pending_.erase(it);
  loop_->cancel(pc.timeout_timer);
  fail_calls_.fetch_add(1, std::memory_order_relaxed);
  recordFailure();
  deliver(pc, "", code);
}

void AsyncRpcChannel::failPending(int32_t code) {
  for (auto it = pending_.begin(); it != pending_.end();) {
    PendingCall pc = std::move(it->second);
    it = pending_.erase(it);
    loop_->cancel(pc.timeout_timer);
    fail_calls_.fetch_add(1, std::memory_order_relaxed);
    recordFailure();
    deliver(pc, "", code);
  }
}

void AsyncRpcChannel::deliver(PendingCall &pc, std::string body, int32_t code) {
  if (pc.promise) {
    if (code == 0)
      pc.promise->set_value(std::move(body));
    else
      pc.promise->set_exception(std::make_exception_ptr(AsyncRpcError(code)));
  } else if (pc.callback) {
    pc.callback(std::move(body), code);
  }
}

bool AsyncRpcChannel::isCircuitOpen() const {
  return state_.load() == State::kBroken && nowMs() < next_retry_ms_.load();
}

void AsyncRpcChannel::recordFailure() {
  int n = consecutive_failures_.fetch_add(1) + 1;
  state_.store(State::kBroken);
  if (n >= failure_threshold_) {
    int shift = n - failure_threshold_;
    if (shift > 5)
      shift = 5;
    uint64_t backoff = backoff_base_ms_ << shift;
    if (backoff > backoff_max_ms_)
      backoff = backoff_max_ms_;
    next_retry_ms_.store(nowMs() + backoff);
  }
}

void AsyncRpcChannel::recordSuccess() {
  consecutive_failures_.store(0);
  state_.store(State::kConnected);
}

void AsyncRpcChannel::heartbeatTick() {
  if (closed_.load())
    return;
  if (!conn_ || conn_->disconnected()) {
    heartbeat_miss_ = 0;
    return;
  }
  uint64_t now = nowMs();
  if (now - last_active_ms_ >=
      static_cast<uint64_t>(cfg_.heartbeat_interval_ms)) {
    if (++heartbeat_miss_ >= cfg_.heartbeat_miss_threshold) {
      LOG_WARN("AsyncRpcChannel: peer %s:%u heartbeat miss, closing",
               server_ip_.c_str(), server_port_);
      conn_->forceClose(); // 触发 onClose -> failPending + 重连
      return;
    }
    sendHeartbeatPing();
  } else {
    heartbeat_miss_ = 0;
  }
}

void AsyncRpcChannel::sendHeartbeatPing() {
  if (!conn_ || conn_->disconnected())
    return;
  uint64_t seq = next_seq_id_++;
  std::string wire = encodeMessage(buildHeartbeat(seq));
  conn_->send(std::move(wire));
}