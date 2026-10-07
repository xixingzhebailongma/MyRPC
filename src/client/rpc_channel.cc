#include "rpc_channel.h"
#include "Logger.h"
#include "rpc_error_code.h"
#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include "metrics_registry.h"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
RpcChannel::RpcChannel(const std::string &server_ip, uint16_t server_port,
                       int timeout_ms, int connect_timeout_ms)
    : server_ip_(server_ip), server_port_(server_port), timeout_ms_(timeout_ms),
      connect_timeout_ms_(connect_timeout_ms) {
  registerMetrics();
}

RpcChannel::RpcChannel(const std::string &server_ip, uint16_t server_port,
                       const RpcClientConfig &cfg)
    : failure_threshold_(cfg.circuit_failure_threshold),
      backoff_base_ms_(cfg.circuit_backoff_base_ms),
      backoff_max_ms_(cfg.circuit_backoff_max_ms), server_ip_(server_ip),
      server_port_(server_port), timeout_ms_(cfg.timeout_ms),
      connect_timeout_ms_(cfg.connect_timeout_ms),
      heartbeat_interval_ms_(cfg.heartbeat_interval_ms),
      heartbeat_miss_threshold_(cfg.heartbeat_miss_threshold) {
  registerMetrics();
}
RpcChannel::~RpcChannel() {
  unregisterMetrics();
  close();
}

void RpcChannel::registerMetrics() {
  metrics_token_ = MetricsRegistry::instance().registerClient(
      MetricsRegistry::ClientKind::kSync, [this]() {
        ClientCounters c;
        c.total = total_calls_.load(std::memory_order_relaxed);
        c.failed = fail_calls_.load(std::memory_order_relaxed);
        c.latency_sum_us =
            total_latency_us_.load(std::memory_order_relaxed);
        c.latency_max_us = max_latency_us_.load(std::memory_order_relaxed);
        return c;
      });
}

void RpcChannel::unregisterMetrics() {
  if (metrics_token_ != 0) {
    MetricsRegistry::instance().unregisterClient(metrics_token_);
    metrics_token_ = 0;
  }
}

bool RpcChannel::connect() {
  std::lock_guard<std::mutex> lock(connect_mutex_);
  if (sockfd_.load() >= 0)
    return true;

  // 清理上一次 readLoop 线程（若它已因断连退出，join 立即返回）
  if (thread_.joinable())
    thread_.join();
  state_.store(State::kConnecting);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    LOG_ERROR("RpcChannel: socket() failed");
    return false;
  }

  // 1. 设为非阻塞，配合 poll 实现 connect 超时
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(server_port_);
  inet_pton(AF_INET, server_ip_.c_str(), &addr.sin_addr);

  // 2. 非阻塞 connect：立即返回，EINPROGRESS 表示连接进行中
  int ret = ::connect(fd, (struct sockaddr *)&addr, sizeof(addr));
  if (ret < 0 && errno != EINPROGRESS) {
    LOG_ERROR("RpcChannel: connect() to %s:%d failed", server_ip_.c_str(),
              server_port_);
    ::close(fd);
    return false;
  }

  // 3. poll 等待连接建立（可写），超时放弃
  if (ret < 0) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLOUT;
    pfd.revents = 0;
    int pr = ::poll(&pfd, 1, connect_timeout_ms_.load());
    if (pr <= 0) {
      LOG_ERROR("RpcChannel: connect() to %s:%d timeout", server_ip_.c_str(),
                server_port_);
      ::close(fd);
      return false;
    }
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
    if (so_error != 0) {
      LOG_ERROR("RpcChannel: connect() to %s:%d failed: %s", server_ip_.c_str(),
                server_port_, strerror(so_error));
      ::close(fd);
      return false;
    }
  }

  // 4. 恢复阻塞模式。不再设 SO_RCVTIMEO/SO_SNDTIMEO——超时改由
  //    Call 里的 cv_.wait_for 按调用粒度控制。
  fcntl(fd, F_SETFL, flags);

  // 内核级半开连接探测：30s 空闲起、每 5s 探测、3 次失败判死。
  // 只能探测“对端主机没了 / 被 NAT 静默掐断”这类内核可感知的死连接，
  // 探测不到应用层挂死（对端进程挂了但 TCP 仍在，或被防火墙静默丢包），
  // 所以仍需应用层心跳。
  {
    int keepalive = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive));
    int idle = 30, intvl = 5, cnt = 3;
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
  }

  {
    std::lock_guard<std::mutex> l(mutex_);
    closed_ = false; // 支持 close()/断连后重连
  }
  sockfd_.store(fd);
  running_.store(true);
  thread_ = std::thread(&RpcChannel::readLoop, this);
  state_.store(State::kConnected);
  LOG_INFO("RpcChannel: connected to %s:%d", server_ip_.c_str(), server_port_);
  return true;
}

void RpcChannel::close() {
  std::lock_guard<std::mutex> lock(connect_mutex_);
  running_.store(false);
  int fd = sockfd_.load();
  // shutdown 但不 close：解除 readLoop 阻塞的 recv、中断在途 send，同时保持
  // fd 号有效，避免被复用后 readLoop 读到别人的 fd。真正的 close 由
  // readLoop 退出路径完成。
  if (fd >= 0)
    ::shutdown(fd, SHUT_RDWR);
  if (thread_.joinable())
    thread_.join();
}
bool RpcChannel::isCircuitOpen() const {
  return state_.load() == State::kBroken && nowMs() < next_retry_ms_.load();
}

void RpcChannel::recordFailure() {
  int n = consecutive_failures_.fetch_add(1) + 1;
  state_.store(State::kBroken);
  if (n >= failure_threshold_) {
    // 指数退避：1s、2s、4s、8s、16s、30s（封顶）
    int shift = n - failure_threshold_;
    if (shift > 5)
      shift = 5;
    uint64_t backoff = backoff_base_ms_ << shift;
    if (backoff > backoff_max_ms_)
      backoff = backoff_max_ms_;
    next_retry_ms_.store(nowMs() + backoff);
  }
}

void RpcChannel::recordSuccess() {
  consecutive_failures_.store(0);
  state_.store(State::kConnected);
}

void RpcChannel::recordOutcome(bool ok, uint64_t t0_us) {
  uint64_t lat = nowUs() - t0_us;
  total_latency_us_.fetch_add(lat, std::memory_order_relaxed);
  uint64_t prev = max_latency_us_.load(std::memory_order_relaxed);
  while (lat > prev && !max_latency_us_.compare_exchange_weak(
                           prev, lat, std::memory_order_relaxed))
    ;
  if (ok)
    success_calls_.fetch_add(1, std::memory_order_relaxed);
  else
    fail_calls_.fetch_add(1, std::memory_order_relaxed);
}

bool RpcChannel::sendAll(const char *data, size_t n) {
  // 快照 fd 安全：真正的 ::close 只在 send_mutex_ 保护下（readLoop 退出路径）
  // 发生，而本函数调用前已持 send_mutex_，故 fd 号不会被复用成新 fd。
  int fd = sockfd_.load();
  if (fd < 0)
    return false;
  size_t sent = 0;
  while (sent < n) {
    ssize_t r = ::send(fd, data + sent, n - sent, MSG_NOSIGNAL);
    if (r < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    if (r == 0)
      return false;
    sent += static_cast<size_t>(r);
  }
  return true;
}

bool RpcChannel::readFull(int fd, char *buf, size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::recv(fd, buf + got, n - got, 0);
    if (r < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    if (r == 0)
      return false;
    got += static_cast<size_t>(r);
  }
  return true;
}

bool RpcChannel::readFrame(int fd, std::string &payload) {
  // 读 4 字节大端长度前缀
  char hdr[kHeaderLen];
  if (!readFull(fd, hdr, kHeaderLen))
    return false;
  uint32_t len = readLenBE(hdr);
  // 长度校验：0 或超过 64MB 视为非法帧，防止坏帧触发巨量分配
  if (len == 0 || len > kMaxMessageLen) {
    LOG_ERROR("RpcChannel: invalid frame length %u", len);
    return false;
  }
  payload.resize(len);
  return readFull(fd, &payload[0], len);
}

bool RpcChannel::sendHeartbeat(int fd) {
  if (fd < 0)
    return false;
  // try_lock：不阻塞等 send_mutex_。否则若某在途 Call 持 send_mutex_ 阻塞在
  // ::send，readLoop 等锁就没人收帧，反而和对端死锁。拿不到就跳过本轮。
  std::unique_lock<std::mutex> lk(send_mutex_, std::try_to_lock);
  if (!lk.owns_lock())
    return false;
  RpcMessage ping = buildHeartbeat(next_seq_id_.fetch_add(1));
  std::string wire = encodeMessage(ping);
  return sendAll(wire.data(), wire.size());
}

void RpcChannel::readLoop() {
  int fd = sockfd_.load();
  int missed = 0;
  while (running_.load()) {
    // 空闲探测：heartbeat_interval 内无数据则视为空闲。
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    int pr = ::poll(&pfd, 1, heartbeat_interval_ms_.load());
    if (pr == 0) {
      // 空闲：连续 miss 达阈值判定死亡；否则发一帧心跳探测。
      if (++missed >= heartbeat_miss_threshold_.load())
        break;
      sendHeartbeat(fd);
      continue;
    }
    if (pr < 0) {
      if (errno == EINTR)
        continue;
      break;
    }
    // 有数据：任何入站帧都复位 miss 计数（心跳应答也会走到这里）。
    missed = 0;
    std::string payload;
    if (!readFrame(fd, payload))
      break;
    RpcMessage msg;
    if (msg.ParseFromString(payload) && msg.has_header()) {
      uint64_t seq = msg.header().sequence_id();
      std::lock_guard<std::mutex> lock(mutex_);
      // 只有仍在等待中的 seq 才存入；超时后的迟到响应直接丢弃，避免 map
      // 无界增长。
      if (pending_.count(seq)) {
        responses_[seq] = std::move(msg);
        cv_.notify_all();
      }
    }
    // 无 header 的帧（服务端推送）在 RpcChannel 场景忽略
  }
  // 收帧结束（对端断开 / close() / 坏帧）：回收 fd 并唤醒所有等待者，
  // 否则在途 Call 要白等满 timeout_ms。
  sockfd_.store(-1);
  if (fd >= 0) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    ::close(fd);
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    cv_.notify_all();
  }
}

bool RpcChannel::Call(const std::string &service_name,
                      const std::string &method_name,
                      const std::string &request_body,
                      std::string &response_body, int32_t &error_code,
                      int timeout_ms, const std::string &request_id,
                      const std::string &trace_id) {
  // seq 占位传 0，由 callImpl 统一覆盖
  RpcMessage request = buildRequest(service_name, method_name, 0, request_body);
  if (!request_id.empty()) {
    request.mutable_header()->set_request_id(request_id);
  }
  if (!trace_id.empty()) {
    request.mutable_header()->set_trace_id(trace_id);
  }
  return callImpl(request, response_body, error_code, timeout_ms);
}

bool RpcChannel::CallMessage(const RpcMessage &request,
                             std::string &response_body, int32_t &error_code,
                             int timeout_ms) {
  return callImpl(request, response_body, error_code, timeout_ms);
}

bool RpcChannel::callImpl(const RpcMessage &request, std::string &response_body,
                          int32_t &error_code, int timeout_ms) {
  // 熔断期快速失败：不 connect、不记失败、也不计为一次调用。
  if (state_.load() == State::kBroken && nowMs() < next_retry_ms_.load()) {
    error_code = static_cast<int32_t>(RpcError::CIRCUIT_OPEN);
    return false;
  }

  uint64_t t0 = nowUs();
  total_calls_.fetch_add(1, std::memory_order_relaxed);
  int effective_timeout = timeout_ms < 0 ? timeout_ms_.load() : timeout_ms;

  if (sockfd_.load() < 0 && !connect()) {
    recordFailure();
    recordOutcome(false, t0);
    error_code = static_cast<int32_t>(RpcError::CONNECTION_REFUSED);
    return false;
  }

  last_used_ms_.store(nowMs());
  uint64_t seq = next_seq_id_.fetch_add(1);
  RpcMessage req = request;
  req.mutable_header()->set_sequence_id(seq);
  std::string wire_data = encodeMessage(req);

  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.insert(seq);
  }
  {
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (!sendAll(wire_data.data(), wire_data.size())) {
      int fd = sockfd_.load();
      if (fd >= 0)
        ::shutdown(fd, SHUT_RDWR);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(seq);
      }
      recordFailure();
      recordOutcome(false, t0);
      error_code = static_cast<int32_t>(RpcError::CONNECTION_BROKEN);
      return false;
    }
  }

  std::unique_lock<std::mutex> lock(mutex_);
  bool ok = cv_.wait_for(lock, std::chrono::milliseconds(effective_timeout),
                         [&] { return closed_ || responses_.count(seq) != 0; });
  pending_.erase(seq);
  if (!ok) {
    recordFailure();
    recordOutcome(false, t0);
    error_code = static_cast<int32_t>(RpcError::TIMEOUT);
    return false;
  }

  auto it = responses_.find(seq);
  if (it == responses_.end()) {
    recordFailure();
    recordOutcome(false, t0);
    error_code = static_cast<int32_t>(RpcError::SERVER_ERROR);
    return false;
  }
  RpcMessage resp = std::move(it->second);
  responses_.erase(it);
  error_code = resp.header().error_code();
  response_body = std::move(resp.body());
  recordSuccess();
  recordOutcome(true, t0);
  return true;
}